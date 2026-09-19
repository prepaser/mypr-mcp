from __future__ import annotations

import asyncio
import contextlib
import os
import sys
from pathlib import Path

import pytest

from mypr_mcp.browser_service import BrowserService


def _proc_children(root: int) -> set[int]:
    parents: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            fields = (entry / "stat").read_text(encoding="ascii").rsplit(") ", 1)[1].split()
            parents[int(entry.name)] = int(fields[1])
        except OSError, ValueError, IndexError:
            continue
    descendants: set[int] = set()
    queue = [root]
    while queue:
        parent = queue.pop()
        for pid, candidate_parent in parents.items():
            if candidate_parent == parent and pid not in descendants:
                descendants.add(pid)
                queue.append(pid)
    return descendants


def _running(pid: int) -> bool:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").rsplit(") ", 1)[1].split()
    except OSError, ValueError, IndexError:
        return False
    return fields[0] not in {"Z", "X"}


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform != "linux", reason="browser tree guard is Linux-only")
async def test_browser_tree_stops_when_watched_kernel_exits(tmp_path, monkeypatch):
    cache = Path(os.environ.get("MYPR_TEST_BROWSER_PATH", "/tmp/mypr-playwright-browsers"))
    if not await asyncio.to_thread(cache.exists):
        pytest.skip("Install Chromium into MYPR_TEST_BROWSER_PATH to run browser integration tests")
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(cache))

    from playwright.async_api import async_playwright

    watched = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(60)"
    )
    unrelated = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(60)"
    )
    service = BrowserService(
        tmp_path,
        Path(sys.executable),
        kernel_pid=watched.pid,
        generation="browser-guard-test",
        manager_pid=os.getpid(),
    )
    guard = None
    playwright = await async_playwright().start()
    browser = None
    context = None
    try:
        info = await service.ensure("chromium", install=False)
        guard = service._server.process
        browser = await playwright.chromium.connect(info["endpoint"])
        context = await browser.new_context()
        page = await context.new_page()
        await page.set_content("<title>guard test</title>")
        assert await page.title() == "guard test"
        owned = {guard.pid, *_proc_children(guard.pid)}
        assert len(owned) >= 2

        watched.kill()
        await asyncio.wait_for(watched.wait(), 5)
        await asyncio.wait_for(guard.wait(), 10)

        for _ in range(100):
            if not any(_running(pid) for pid in owned):
                break
            await asyncio.sleep(0.05)
        assert not any(_running(pid) for pid in owned)
        assert unrelated.returncode is None
    finally:
        if context is not None:
            with contextlib.suppress(Exception):
                await context.close()
        if browser is not None:
            with contextlib.suppress(Exception):
                await browser.close()
        await playwright.stop()
        with contextlib.suppress(Exception):
            await service.close()
        with contextlib.suppress(ProcessLookupError):
            unrelated.terminate()
        with contextlib.suppress(ProcessLookupError):
            watched.terminate()
        if unrelated.returncode is None:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(unrelated.wait(), 5)
        if watched.returncode is None:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(watched.wait(), 5)
