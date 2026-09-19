import asyncio
from pathlib import Path

import pytest

from mypr_mcp.browser_service import BrowserService, _InstallLock


class _Process:
    pid = 12345
    returncode = None

    async def wait(self):
        return self.returncode


class _Shells:
    def __init__(self):
        self.started = []

    async def start(self, command, **kwargs):
        self.started.append((command, kwargs))
        return {"id": "installer"}

    async def wait(self, ident):
        assert ident == "installer"
        return {"state": "succeeded", "result": {"returncode": 0}}


@pytest.mark.asyncio
async def test_ensure_reuses_generation_server(tmp_path):
    service = BrowserService(tmp_path, Path("/usr/bin/python"), generation="g")
    calls = []

    async def start(browser, *, executable_path, channel):
        calls.append((browser, executable_path, channel))
        return {
            "endpoint": "ws://127.0.0.1:1234/token",
            "browser": browser,
            "generation": service.generation,
            "reused": False,
        }

    service._start_server = start
    first = await service.ensure("chromium", install=False, launch_options={"channel": "chrome"})
    service._server = type(
        "Server",
        (),
        {
            "endpoint": first["endpoint"],
            "process": _Process(),
            "stdout_task": asyncio.create_task(asyncio.sleep(0)),
            "stderr_task": asyncio.create_task(asyncio.sleep(0)),
        },
    )()
    second = await service.ensure("firefox", install=False)
    assert first["endpoint"] == second["endpoint"]
    assert second["reused"] is True
    assert calls == [("chromium", None, "chrome")]


@pytest.mark.asyncio
async def test_install_is_tracked_and_marked(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(tmp_path / "cache"))
    shells = _Shells()
    tracked = []
    service = BrowserService(
        tmp_path,
        Path("/usr/bin/python"),
        generation="g",
        shells=shells,
        track=lambda *args, **kwargs: tracked.append((args, kwargs)),
    )
    service._sdk_version = lambda: asyncio.sleep(0, result="1.0")
    service._installed = lambda browser: asyncio.sleep(0, result=True)
    await service._ensure_installed("chromium")
    assert shells.started[0][0][-3:] == ["playwright", "install", "chromium"]
    assert tracked[0][1]["purpose"] == "browser_install"
    assert (tmp_path / ".mypr/browser/chromium.json").exists()
    await service._ensure_installed("chromium")
    assert len(shells.started) == 1
    restarted = BrowserService(tmp_path, Path("/usr/bin/python"), shells=shells)
    restarted._sdk_version = service._sdk_version
    restarted._installed = service._installed
    await restarted._ensure_installed("chromium")
    assert len(shells.started) == 2


def test_install_lock_timeout(tmp_path):
    async def run():
        first = _InstallLock(tmp_path / "install.lock", 1)
        await first.__aenter__()
        try:
            with pytest.raises(TimeoutError):
                await _InstallLock(tmp_path / "install.lock", 0).__aenter__()
        finally:
            await first.__aexit__(None, None, None)

    asyncio.run(run())


@pytest.mark.parametrize("browser", ["", "chrome", "Chromium", None])
def test_invalid_browser(tmp_path, browser):
    service = BrowserService(tmp_path, Path("/usr/bin/python"))
    with pytest.raises(ValueError):
        service._validate_browser(browser)
