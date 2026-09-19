from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import zipfile
from pathlib import Path

import pytest

from mypr_mcp.browser_tools import BrowserError, BrowserTools


class FakePage:
    def __init__(self) -> None:
        self.screenshots: list[str] = []

    async def screenshot(self, *, path: str, **options):
        self.screenshots.append(path)
        await asyncio.to_thread(Path(path).write_bytes, b"\x89PNG\r\n\x1a\n")


class FakeContext:
    def __init__(self, options):
        self.options = options
        self.pages = [FakePage()]
        self.closed = False
        self.close_count = 0
        self.listeners = {}

    def is_closed(self):
        return self.closed

    def on(self, event, callback):
        self.listeners.setdefault(event, []).append(callback)

    async def new_page(self):
        page = FakePage()
        self.pages.append(page)
        return page

    async def close(self):
        self.closed = True
        self.close_count += 1
        for callback in self.listeners.get("close", []):
            callback()

    async def storage_state(self, **options):
        return {"cookies": [{"name": "session", "value": "ok"}], **options}


class FakeBrowser:
    def __init__(self):
        self.contexts: list[FakeContext] = []
        self.closed = False
        self.close_count = 0

    def is_connected(self):
        return not self.closed

    async def new_context(self, **options):
        context = FakeContext(options)
        self.contexts.append(context)
        return context

    async def close(self):
        self.closed = True
        self.close_count += 1


class FakeBrowserType:
    def __init__(self):
        self.connections: list[tuple[str, dict]] = []
        self.browsers: list[FakeBrowser] = []

    async def connect(self, endpoint, *, headers=None, **options):
        self.connections.append((endpoint, dict(headers or {})))
        browser = FakeBrowser()
        self.browsers.append(browser)
        return browser

    async def connect_over_cdp(self, endpoint, **options):
        self.connections.append((endpoint, dict(options)))
        browser = FakeBrowser()
        self.browsers.append(browser)
        return browser


class FakeDriver:
    def __init__(self):
        self.chromium = FakeBrowserType()
        self.firefox = FakeBrowserType()
        self.webkit = FakeBrowserType()
        self.stopped = False

    async def stop(self):
        self.stopped = True


class FakeDriverFactory:
    def __init__(self):
        self.driver = FakeDriver()

    async def start(self):
        return self.driver


@pytest.fixture
def fake_playwright(monkeypatch):
    factory = FakeDriverFactory()
    package = types.ModuleType("playwright")
    module = types.ModuleType("playwright.async_api")
    module.async_playwright = lambda: factory
    monkeypatch.setitem(sys.modules, "playwright", package)
    monkeypatch.setitem(sys.modules, "playwright.async_api", module)
    return factory.driver


@pytest.mark.asyncio
async def test_managed_context_isolated_by_client_and_singleflight(tmp_path, fake_playwright):
    identity = {"value": "one"}
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        return {"endpoint": "ws://127.0.0.1:1234/pw"}

    tools = BrowserTools(tmp_path, lambda: {"client_id": identity["value"]}, rpc)
    first, second = await asyncio.gather(tools.context(), tools.context())
    assert first is second
    assert len([call for call in calls if call[0] == "browser_server"]) == 1
    identity["value"] = "two"
    other = await tools.context()
    assert other is not first
    assert len(fake_playwright.chromium.connections) == 2
    await tools.aclose()


@pytest.mark.asyncio
async def test_shared_context_uses_explicit_namespace_and_requires_close(tmp_path, fake_playwright):
    identity = {"value": "shared"}

    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: identity["value"], rpc)
    first = await tools.context("tab", shared=True, viewport={"width": 100, "height": 100})
    identity["value"] = "different"
    assert await tools.context("tab", shared=True, viewport={"width": 100, "height": 100}) is first
    with pytest.raises(RuntimeError, match="settings changed"):
        await tools.context("tab", shared=True, viewport={"width": 200, "height": 100})
    assert any(item["type"] == "context" and item["shared"] for item in tools.list())
    await tools.close("tab", shared=True)
    assert not tools.list()
    await tools.aclose()


@pytest.mark.asyncio
async def test_external_connection_is_disconnected_without_closing_remote_browser(
    tmp_path, fake_playwright
):
    async def rpc(op, **fields):
        raise AssertionError("external connections do not use manager RPC")

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    browser = await tools.connect("ws://external", name="remote")
    context = await tools.context(connection="remote")
    await tools.close("remote", connection=True)
    assert context.closed
    assert browser.closed
    await tools.aclose()
    assert browser.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_close", [False, True])
async def test_cancelled_external_connection_is_disconnected(tmp_path, fake_playwright, fail_close):
    started = asyncio.Event()
    release = asyncio.Event()
    connected = []
    original = fake_playwright.chromium.connect

    async def connect(endpoint, *, headers=None, **options):
        started.set()
        await release.wait()
        browser = await original(endpoint, headers=headers, **options)
        if fail_close:
            original_close = browser.close
            attempted = False

            async def close():
                nonlocal attempted
                if not attempted:
                    attempted = True
                    raise RuntimeError("temporary disconnect failure")
                await original_close()

            browser.close = close
        connected.append(browser)
        return browser

    fake_playwright.chromium.connect = connect

    async def rpc(op, **fields):
        raise AssertionError("external connections do not use manager RPC")

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    task = asyncio.create_task(tools.connect("ws://external"))
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert connected[0].closed is not fail_close
    assert bool(tools.list()) is fail_close
    await tools.aclose()
    assert connected[0].closed
    assert not tools.list()


@pytest.mark.asyncio
async def test_external_connection_race_with_close_is_disconnected(tmp_path, fake_playwright):
    started = asyncio.Event()
    release = asyncio.Event()
    connected = []
    original = fake_playwright.chromium.connect

    async def connect(endpoint, *, headers=None, **options):
        started.set()
        await release.wait()
        browser = await original(endpoint, headers=headers, **options)
        connected.append(browser)
        return browser

    fake_playwright.chromium.connect = connect

    async def rpc(op, **fields):
        raise AssertionError("external connections do not use manager RPC")

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    task = asyncio.create_task(tools.connect("ws://external"))
    await started.wait()
    cleanup = asyncio.create_task(tools.aclose())
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(BrowserError, match="closed"):
        await task
    await cleanup
    assert connected[0].closed
    assert not tools.list()


@pytest.mark.asyncio
async def test_state_is_private_and_screenshot_uses_workspace(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client/a", rpc)
    context = await tools.context()
    saved = await tools.save_state(context)
    target = tmp_path / saved["path"]
    assert target.exists()
    assert os.stat(target).st_mode & 0o777 == 0o600
    assert (await tools.load_state(path=saved["path"]))["cookies"]
    image = await tools.screenshot(context.pages[0], "artifacts/shot.png")
    assert image.format == "png"
    assert (tmp_path / "artifacts/shot.png").exists()
    await tools.aclose()


@pytest.mark.asyncio
async def test_state_namespace_hashes_client_ids_and_supports_external_path(
    tmp_path, fake_playwright
):
    identity = {"value": "shared"}

    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: identity["value"], rpc)
    first = await tools.context()
    first_state = await tools.save_state(first)
    identity["value"] = "a/b"
    second = await tools.context()
    second_state = await tools.save_state(second)
    assert first_state["path"] != second_state["path"]
    assert "/private/" in first_state["path"]
    assert "/private/" in second_state["path"]
    external = tmp_path.parent / "browser-state.json"
    saved = await tools.save_state(second, path=external)
    assert saved["path"] == str(external)
    assert external.exists()
    await tools.aclose()


@pytest.mark.asyncio
async def test_launch_options_header_uses_node_names(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    await tools.context(
        launch_options={"executable_path": "/bin/browser", "ignore_default_args": ["--x"]}
    )
    headers = fake_playwright.chromium.connections[0][1]
    assert json.loads(headers["x-playwright-launch-options"]) == {
        "executablePath": "/bin/browser",
        "ignoreDefaultArgs": ["--x"],
    }
    await tools.aclose()


@pytest.mark.asyncio
async def test_recording_and_storage_paths_are_workspace_relative(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    context = await tools.context(
        record_har_path="artifacts/recording.har",
        record_video_dir="artifacts/video",
        storage_state="state.json",
    )
    options = context.options
    assert options["record_har_path"] == str(tmp_path / "artifacts/recording.har")
    assert options["record_video_dir"] == str(tmp_path / "artifacts/video")
    assert options["storage_state"] == str(tmp_path / "state.json")
    assert (tmp_path / "artifacts/video").is_dir()
    await tools.aclose()


@pytest.mark.asyncio
async def test_remote_har_archive_is_exported_over_existing_file(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    await tools.context("har", record_har_path="recording.har")
    target = tmp_path / "recording.har"
    target.write_text('{"old": true}', encoding="utf-8")
    with zipfile.ZipFile(f"{target}.tmp", "w") as archive:
        archive.writestr("har.har", '{"new": true}')
    await tools.close("har")
    assert target.read_text(encoding="utf-8") == '{"new": true}'
    assert not await asyncio.to_thread(Path(f"{target}.tmp").exists)
    await tools.aclose()


@pytest.mark.asyncio
async def test_attached_har_exports_json_and_resources(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    await tools.context("har", record_har_path="recording.har", record_har_content="attach")
    target = tmp_path / "recording.har"
    with zipfile.ZipFile(f"{target}.tmp", "w") as archive:
        archive.writestr("har.har", '{"log": {"entries": []}}')
        archive.writestr("resource.dat", b"attached")
    await tools.close("har")
    assert json.loads(target.read_text(encoding="utf-8"))["log"]
    assert (tmp_path / "resource.dat").read_bytes() == b"attached"
    await tools.aclose()


@pytest.mark.asyncio
async def test_close_makes_late_creation_fail(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    await tools.aclose()
    with pytest.raises(BrowserError, match="closed"):
        await tools.context()


@pytest.mark.asyncio
async def test_dead_managed_browser_is_recreated(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    first = await tools.context()
    browser = fake_playwright.chromium.browsers[0]
    browser.closed = True
    second = await tools.context()
    assert second is not first
    assert len(fake_playwright.chromium.browsers) == 2
    await tools.aclose()


@pytest.mark.asyncio
async def test_native_context_close_is_removed_from_registry(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    first = await tools.context()
    await first.close()
    second = await tools.context()
    assert second is not first
    await tools.aclose()


@pytest.mark.asyncio
async def test_cancelled_context_creation_closes_native_context(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    await tools.context()
    browser = fake_playwright.chromium.browsers[0]
    started = asyncio.Event()
    release = asyncio.Event()
    original = browser.new_context

    async def slow_new_context(**options):
        started.set()
        await release.wait()
        return await original(**options)

    browser.new_context = slow_new_context
    task = asyncio.create_task(tools.context("slow"))
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert browser.contexts[-1].closed
    assert not any(item["name"] == "slow" for item in tools.list())
    await tools.aclose()


@pytest.mark.asyncio
async def test_close_failure_is_reported_and_registry_is_retained(tmp_path, fake_playwright):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    context = await tools.context()
    original_close = context.close

    async def fail_close():
        raise RuntimeError("HAR flush failed")

    context.close = fail_close
    with pytest.raises(BrowserError, match="HAR flush failed"):
        await tools.close("default")
    assert any(item["name"] == "default" for item in tools.list())
    context.close = original_close
    await tools.aclose()


def test_playwright_options_header_is_json(tmp_path):
    async def rpc(op, **fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: "client", rpc)
    assert json.loads(tools._json_signature({"headless": True})) == {"headless": True}
    with pytest.raises(TypeError):
        tools._json_signature({"bad": object()})
