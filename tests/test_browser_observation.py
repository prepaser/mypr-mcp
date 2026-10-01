from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

from mypr_mcp.browser_observation import _safe_url
from mypr_mcp.browser_tools import BrowserError, BrowserTools


class EventEmitter:
    def __init__(self):
        self.listeners = {}

    def on(self, name, callback):
        self.listeners.setdefault(name, []).append(callback)

    def remove_listener(self, name, callback):
        self.listeners[name].remove(callback)

    def emit(self, name, *args):
        for callback in tuple(self.listeners.get(name, ())):
            callback(*args)


class Page(EventEmitter):
    def __init__(self, context):
        super().__init__()
        self.context = context
        self.url = "https://user:password@example.test/?access_token=secret&next=home"
        self.snapshot_text = ""
        self.closed = False
        self.locators = []

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True
        self.emit("close")

    async def title(self):
        return "Example"

    def locator(self, selector):
        locator = Locator(self, selector)
        self.locators.append(locator)
        return locator


class Locator:
    def __init__(self, page, selector):
        self.page = page
        self.selector = selector
        self.options = None

    async def aria_snapshot(self, *, depth=None, mode=None, boxes=None):
        self.options = {"depth": depth, "mode": mode, "boxes": boxes}
        return self.page.snapshot_text


class LegacyLocator:
    async def aria_snapshot(self):
        return "legacy snapshot"


class Context(EventEmitter):
    def __init__(self, browser):
        super().__init__()
        self.browser = browser
        self.closed = False
        self.pages = [Page(self)]

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True
        for page in self.pages:
            await page.close()
        self.emit("close")


class Browser(EventEmitter):
    def __init__(self):
        super().__init__()
        self.contexts = []
        self.closed = False

    def is_connected(self):
        return not self.closed

    async def new_context(self, **_options):
        context = Context(self)
        self.contexts.append(context)
        return context

    async def close(self):
        self.closed = True
        self.emit("disconnected")


class BrowserType:
    def __init__(self):
        self.browsers = []

    async def connect(self, *_args, **_kwargs):
        browser = Browser()
        self.browsers.append(browser)
        return browser


class Driver:
    def __init__(self):
        self.chromium = BrowserType()
        self.firefox = BrowserType()
        self.webkit = BrowserType()

    async def stop(self):
        pass


class Request:
    method = "GET"
    resource_type = "fetch"
    url = "https://user:password@example.test/data?api_key=secret&safe=yes"
    failure = None

    def __init__(self, headers=None):
        self._headers = headers or {}

    async def all_headers(self):
        return self._headers


class Response:
    status = 200
    status_text = "OK"

    def __init__(self, request, headers, body=b"ok"):
        self.request = request
        self.url = request.url
        self._headers = headers
        self._body = body
        self.body_calls = 0

    async def all_headers(self):
        return self._headers

    async def body(self):
        self.body_calls += 1
        return self._body


@pytest.fixture
async def browser_tools(tmp_path, monkeypatch):
    driver = Driver()
    package = types.ModuleType("playwright")
    module = types.ModuleType("playwright.async_api")
    module.async_playwright = lambda: types.SimpleNamespace(start=lambda: _started(driver))
    monkeypatch.setitem(sys.modules, "playwright", package)
    monkeypatch.setitem(sys.modules, "playwright.async_api", module)
    identity = {"client_id": "alpha"}

    async def rpc(_op, **_fields):
        return "ws://127.0.0.1:1234/pw"

    tools = BrowserTools(tmp_path, lambda: identity, rpc)
    yield tools, identity
    await tools.aclose()


async def _started(driver):
    return driver


@pytest.mark.asyncio
async def test_observation_scopes_page_events_and_redacts_sensitive_data(browser_tools):
    tools, _identity = browser_tools
    context = await tools.context()
    page = context.pages[0]
    observation = await tools.observe(page)
    request = Request({"authorization": "Bearer private", "x-api-key": "private", "accept": "*/*"})
    response = Response(
        request,
        {"content-length": "2", "set-cookie": "session=private", "content-type": "text/plain"},
    )
    page.emit("request", request)
    page.emit("response", response)
    page.emit(
        "console",
        types.SimpleNamespace(
            type="warning",
            text="hello",
            location=lambda: {
                "url": "https://user:password@example.test/?access_token=secret&safe=yes",
                "lineNumber": 3,
            },
        ),
    )

    result = await observation.read()
    assert [event["type"] for event in result["events"]] == ["request", "response", "console"]
    request_event = result["events"][0]
    assert "secret" not in json.dumps(result)
    assert "password" not in json.dumps(result)
    assert result["events"][-1]["location"]["url"].startswith("https://[redacted]@example.test/")
    assert "safe=yes" in result["events"][-1]["location"]["url"]
    result["events"][-1]["location"]["lineNumber"] = "changed"
    replay = await observation.read()
    assert replay["events"][-1]["location"]["lineNumber"] == "3"
    details = await observation.request(request_event["request_id"])
    assert details["request"]["headers"]["authorization"] == "[redacted]"
    assert details["request"]["headers"]["x-api-key"] == "[redacted]"
    assert response.body_calls == 0
    with_body = await observation.request(request_event["request_id"], body=True)
    assert with_body["response"]["body"] == "ok"
    assert response.body_calls == 1
    explicit = await observation.request(
        request_event["request_id"], include_sensitive_headers=True
    )
    assert explicit["request"]["headers"]["authorization"] == "Bearer private"
    assert explicit["response"]["headers"]["set-cookie"] == "session=private"

    observation.close()
    assert not any(page.listeners.values())
    assert (await observation.read())["closed"]
    assert await observation.request(request_event["request_id"])


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@example.test/?access_token=secret&safe=yes#api%5Fkey=secret",
        "https://user:password@example.test:invalid/?access_token=secret&safe=yes#api%5Fkey=secret",
        "https://user:password@exa／mple.test/?access_token=secret&safe=yes",
        "//user:password@example.test:invalid/?access_token=secret&safe=yes",
    ],
)
def test_safe_url_masks_credentials_and_sensitive_values(url):
    result = _safe_url(url)

    assert "password" not in result
    assert "secret" not in result
    assert "[redacted]@" in result
    assert "safe=yes" in result
    assert result.endswith("#[redacted]") or "#" not in url


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "body", "read_body"),
    [
        ({"content-length": "300000"}, b"x", False),
        ({"content-length": "3", "content-encoding": "gzip"}, b"x", False),
        ({"content-length": "3"}, b"x" * (256 * 1024 + 1), True),
        ({}, b"x", False),
    ],
)
async def test_response_body_reads_are_opt_in_bounded_and_encoding_aware(
    browser_tools, headers, body, read_body
):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    observation = await tools.observe(page)
    request = Request()
    response = Response(request, headers, body)
    page.emit("request", request)
    page.emit("response", response)
    event = (await observation.read())["events"][0]

    result = await observation.request(event["request_id"], body=True)
    assert response.body_calls == int(read_body)
    if read_body:
        assert "body" not in result["response"]
        assert "body_error" in result["response"]
    else:
        assert "body_error" in result["response"]


@pytest.mark.asyncio
async def test_observation_handle_and_page_access_are_client_scoped(browser_tools):
    tools, identity = browser_tools
    first_context = await tools.context()
    first_page = first_context.pages[0]
    first = await tools.observe(first_page)

    identity["client_id"] = "beta"
    with pytest.raises(BrowserError, match="not owned"):
        await tools.observe(first_page)
    with pytest.raises(RuntimeError, match="different client"):
        await first.read()

    shared = await tools.context("shared", shared=True)
    shared_observer = await tools.observe(shared.pages[0])
    assert (await shared_observer.read())["events"] == []
    identity["client_id"] = "alpha"
    with pytest.raises(RuntimeError, match="different client"):
        await shared_observer.read()


@pytest.mark.asyncio
async def test_browser_disconnect_detaches_observers(browser_tools):
    tools, _ = browser_tools
    browser = await tools.connect("ws://external", name="external")
    context = await tools.context("external-context", connection="external")
    page = context.pages[0]
    observation = await tools.observe(page)
    await browser.close()
    assert observation._closed
    assert not any(page.listeners.values())


@pytest.mark.asyncio
async def test_client_event_ring_is_bounded_and_reads_only_its_page(browser_tools):
    tools, _ = browser_tools
    first_context = await tools.context()
    second_context = await tools.context("second")
    first_page, second_page = first_context.pages[0], second_context.pages[0]
    first = await tools.observe(first_page)
    second = await tools.observe(second_page)
    first_page.emit("pageerror", "first page")
    second_page.emit("pageerror", "second page")
    assert [event["text"] for event in (await first.read())["events"]] == ["first page"]
    assert [event["text"] for event in (await second.read())["events"]] == ["second page"]
    first_page.emit("pageerror", "x" * 5000)
    small = await first.read(cursor=1, limit=1, max_bytes=1024)
    assert small["truncated"]
    assert len(json.dumps(small).encode()) <= 1024

    for index in range(1100):
        first_page.emit("pageerror", f"error {index}")
    buffer = tools._observations._clients["alpha"]
    assert len(buffer.events) <= 1000
    assert buffer.bytes_used <= 4 * 1024 * 1024
    result = await first.read(cursor=0)
    assert result["dropped"]
    assert len(result["events"]) <= 100
    assert len(json.dumps(result).encode()) <= 32 * 1024


@pytest.mark.asyncio
async def test_observation_filters_wait_and_respects_compact_budget(browser_tools):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    observation = await tools.observe(page)
    for _ in range(1000):
        page.emit("pageerror", "")
    result = await observation.read(cursor=0, limit=1000)
    assert len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()) <= 32 * 1024

    request = Request()
    response = Response(request, {"content-length": "2"})
    waiting = asyncio.create_task(
        observation.read(types="response", url_contains="/data", status=200, wait_ms=500)
    )
    await asyncio.sleep(0)
    page.emit("request", request)
    page.emit("response", response)
    filtered = await waiting
    assert [event["type"] for event in filtered["events"]] == ["response"]


@pytest.mark.asyncio
async def test_observation_body_timeout_is_bounded(browser_tools):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    observation = await tools.observe(page)

    class HangingResponse(Response):
        async def body(self):
            await asyncio.sleep(10)
            return b"never"

    request = Request()
    page.emit("request", request)
    page.emit("response", HangingResponse(request, {"content-length": "5"}))
    event = (await observation.read())[
        "events"
    ][0]
    result = await observation.request(event["request_id"], body=True, body_timeout=0.01)
    assert "timed out" in result["response"]["body_error"]


@pytest.mark.asyncio
async def test_request_details_are_evicted_with_the_client_cap(browser_tools):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    observation = await tools.observe(page)
    first_id = None
    for _ in range(257):
        request = Request()
        page.emit("request", request)
        first_id = first_id or observation._requests[id(request)]
    with pytest.raises(KeyError, match="no longer retained"):
        await observation.request(first_id)
    assert len(tools._observations._clients["alpha"].requests) == 256


@pytest.mark.asyncio
async def test_snapshot_pagination_uses_one_immutable_capture(browser_tools):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    original = "first\n" + "界" * 5000 + "\nlast"
    page.snapshot_text = original
    first = await tools.snapshot(page, limit=2048, depth=5, mode="ai", boxes=True)
    assert first["has_more"]
    assert len(json.dumps(first, ensure_ascii=False).encode()) <= 2048
    assert page.locators[-1].options == {"depth": 5, "mode": "ai", "boxes": True}
    page.snapshot_text = "changed DOM"
    chunks = [first["text"]]
    cursor = first["next_cursor"]
    while cursor:
        result = await tools.snapshot(page, cursor=cursor, limit=2048)
        assert result["snapshot_id"] == first["snapshot_id"]
        chunks.append(result["text"])
        cursor = result["next_cursor"]
    assert "".join(chunks) == original
    assert first["url"].startswith("https://")
    assert "password" not in first["url"]


def test_snapshot_metadata_overflow_raises_instead_of_hanging_or_exceeding_limit():
    script = """
import asyncio
from mypr_mcp.browser_snapshots import BrowserSnapshots
class Locator:
    def __init__(self, value): self.value = value
    async def aria_snapshot(self): return self.value
class Page:
    url = ""
    def __init__(self, value): self.value = value
    def locator(self, _): return Locator(self.value)
    async def title(self): return "😀" * 512
async def main():
    for value in ("x", ""):
        try:
            await BrowserSnapshots().snapshot("owner", Page(value), limit=2048)
        except ValueError as exc:
            assert "requested output limit" in str(exc)
        else:
            raise AssertionError("oversized metadata was returned")
asyncio.run(main())
"""
    source = Path(__file__).resolve().parents[1] / "src"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(source), environment.get("PYTHONPATH", "")) if part
    )
    subprocess.run(
        [sys.executable, "-c", script],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=3,
    )


@pytest.mark.asyncio
async def test_snapshot_find_and_diff_are_paged_and_client_private(browser_tools):
    tools, identity = browser_tools
    context = await tools.context()
    page = context.pages[0]
    page.snapshot_text = "alpha\nbeta alpha\ngamma\nalpha"
    snapshot = await tools.snapshot(page)
    first = await tools.find(snapshot["snapshot_id"], "alpha", limit=2)
    assert [item["line"] for item in first["matches"]] == [1, 2]
    assert first["has_more"]
    rest = await tools.find(snapshot["snapshot_id"], "alpha", cursor=first["next_cursor"], limit=2)
    assert [item["line"] for item in rest["matches"]] == [4]
    page.snapshot_text = "alpha\nnoise"
    trailing = await tools.snapshot(page)
    first_trailing = await tools.find(trailing["snapshot_id"], "alpha", limit=1)
    assert not first_trailing["has_more"]
    assert first_trailing["next_cursor"] is None
    regex = await tools.find(snapshot["snapshot_id"], r"^alpha", regex=True)
    assert [item["line"] for item in regex["matches"]] == [1, 4]
    page.snapshot_text = "x ^alpha"
    negative = await tools.snapshot(page)
    assert not (await tools.find(negative["snapshot_id"], r"^alpha", regex=True))["matches"]

    page.snapshot_text = "\n".join("needle " + "x" * 4000 for _ in range(20))
    verbose = await tools.snapshot(page)
    matches = await tools.find(verbose["snapshot_id"], "needle", limit=100)
    assert matches["has_more"]
    assert len(json.dumps(matches, ensure_ascii=False).encode()) <= 32 * 1024

    page.snapshot_text = "new\n" + "n" * 5000
    after = await tools.snapshot(page)
    parts = []
    cursor = None
    while True:
        result = await tools.diff(
            snapshot["snapshot_id"], after["snapshot_id"], cursor=cursor, limit=2048
        )
        assert len(json.dumps(result, ensure_ascii=False).encode()) <= 2048
        parts.append(result["diff"])
        cursor = result["next_cursor"]
        if cursor is None:
            break
    assert "alpha" in "".join(parts)
    assert "new" in "".join(parts)

    identity["client_id"] = "other"
    with pytest.raises(KeyError, match="no longer retained"):
        await tools.find(snapshot["snapshot_id"], "alpha")


@pytest.mark.asyncio
async def test_snapshot_expiry_and_playwright_capability_errors(browser_tools):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    page.snapshot_text = "snapshot"
    first = await tools.snapshot(page)
    for _ in range(32):
        await tools.snapshot(page)
    with pytest.raises(KeyError, match="no longer retained"):
        await tools.find(first["snapshot_id"], "snapshot")

    class LegacyPage(Page):
        def locator(self, _selector):
            return LegacyLocator()

    with pytest.raises(ValueError, match="does not support"):
        await tools.snapshot(LegacyPage(context), depth=2)


@pytest.mark.asyncio
async def test_regex_timeout_isolated_from_kernel_and_worker_is_reaped(browser_tools, monkeypatch):
    import mypr_mcp.browser_snapshots as snapshot_module

    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    page.snapshot_text = "a" * 40_000 + "!"
    snapshot = await tools.snapshot(page)
    monkeypatch.setattr(snapshot_module, "_WORKER_TIMEOUT", 0.2)
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.01)

    beat = asyncio.create_task(heartbeat())
    try:
        with pytest.raises(TimeoutError, match="worker limit"):
            await tools.find(snapshot["snapshot_id"], r"(a+)+$", regex=True)
    finally:
        beat.cancel()
        with pytest.raises(asyncio.CancelledError):
            await beat
    assert ticks >= 5


@pytest.mark.asyncio
async def test_invalid_regex_and_reset_remove_observers(browser_tools):
    tools, _ = browser_tools
    context = await tools.context()
    page = context.pages[0]
    page.snapshot_text = "anything"
    snapshot = await tools.snapshot(page)
    with pytest.raises(ValueError, match="invalid regular expression"):
        await tools.find(snapshot["snapshot_id"], "[", regex=True)
    observation = await tools.observe(page)
    await tools.aclose()
    assert not any(page.listeners.values())
    assert (await observation.read())["closed"]
