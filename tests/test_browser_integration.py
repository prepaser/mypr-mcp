from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
from conftest import execute, mcp_session, result_text


@pytest.fixture
async def browser_site(monkeypatch):
    cache = Path(os.environ.get("MYPR_TEST_BROWSER_PATH", "/tmp/mypr-playwright-browsers"))
    if not await asyncio.to_thread(cache.exists):
        pytest.skip("Install Chromium into MYPR_TEST_BROWSER_PATH to run browser integration tests")
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(cache))

    async def serve(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            content = b"""<html><body><input id="value">
<button onclick="document.querySelector('output').textContent=
 document.querySelector('input').value">Apply</button>
<output></output><iframe srcdoc="<p>frame content</p>"></iframe></body></html>"""
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n"
                + f"Content-Length: {len(content)}\r\n\r\n".encode()
                + content
            )
            await writer.drain()
        except ConnectionError, asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    finally:
        server.close()
        await server.wait_closed()


async def cell(session, code):
    result = await execute(session, "import json\n" + code)
    assert result["state"] == "succeeded", result.get("error") or result_text(result)
    return json.loads(result_text(result))


async def test_browser_contexts_reconnect_reset_and_saved_state(workspace, browser_site):
    async with mcp_session(workspace, client_id="browser-a") as first:
        info = await cell(
            first,
            "ws.local['ctx'] = await ws.browser.context()\n"
            "ws.local['page'] = await ws.local['ctx'].new_page()\n"
            f"await ws.local['page'].goto({browser_site!r})\n"
            "await ws.local['page'].locator('#value').fill('hello')\n"
            "await ws.local['page'].get_by_role('button', name='Apply').click()\n"
            "await ws.local['page'].evaluate(\"document.cookie='session=one'\")\n"
            "await ws.browser.save_state()\n"
            "await ws.browser.screenshot(ws.local['page'])\n"
            "print(json.dumps({'text': await ws.local['page'].locator('output').inner_text(), "
            "'frame': await ws.local['page'].frame_locator('iframe').locator('p').inner_text()}))",
        )
        assert info == {"text": "hello", "frame": "frame content"}
        async with mcp_session(workspace, client_id="browser-b") as second:
            isolated = await cell(
                second,
                "ctx = await ws.browser.context()\n"
                "page = await ctx.new_page()\n"
                f"await page.goto({browser_site!r})\n"
                "print(json.dumps(await page.evaluate('document.cookie')))",
            )
            assert isolated == ""
    assert list((workspace / ".mypr" / "artifacts").rglob("*.png"))
    async with mcp_session(workspace, client_id="browser-a") as resumed:
        retained = await cell(
            resumed,
            "print(json.dumps({'same': await ws.browser.context() is ws.local['ctx'], "
            "'text': await ws.local['page'].locator('output').inner_text()}))",
        )
        assert retained == {"same": True, "text": "hello"}
        reset = await execute(resumed, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded", reset
        fresh = await cell(
            resumed, "ctx = await ws.browser.context()\nprint(json.dumps(await ctx.cookies()))"
        )
        assert fresh == []
        restored = await cell(
            resumed,
            "state = await ws.browser.load_state()\n"
            "ctx = await ws.browser.context('restored', storage_state=state)\n"
            "print(json.dumps([cookie['value'] for cookie in await ctx.cookies()]))",
        )
        assert "one" in restored


async def test_external_cdp_browser_survives_reset(workspace, browser_site, tmp_path):
    from playwright.async_api import async_playwright

    profile = tmp_path / "external-profile"
    async with async_playwright() as playwright:
        external = await playwright.chromium.launch_persistent_context(
            str(profile),
            headless=True,
            args=["--remote-debugging-port=0", "--remote-debugging-address=127.0.0.1"],
        )
        try:
            outside = await external.new_page()
            await outside.set_content("<title>outside</title>")
            port = (
                await asyncio.to_thread((profile / "DevToolsActivePort").read_text)
            ).splitlines()[0]
            async with mcp_session(workspace) as session:
                connected = await cell(
                    session,
                    f"remote = await ws.browser.connect('http://127.0.0.1:{port}', "
                    "protocol='cdp', name='external')\n"
                    "ctx = await ws.browser.context('isolated', connection='external')\n"
                    "page = await ctx.new_page()\n"
                    "await page.set_content('<title>managed</title>')\n"
                    "print(json.dumps(await page.title()))",
                )
                assert connected == "managed"
                disconnected = await cell(
                    session,
                    "await ws.browser.close('external', connection=True)\n"
                    "print(json.dumps({'connected': remote.is_connected(), "
                    "'page_closed': page.is_closed()}))",
                )
                assert disconnected == {"connected": False, "page_closed": True}
                assert await outside.title() == "outside"
                await cell(
                    session,
                    f"await ws.browser.connect('http://127.0.0.1:{port}', "
                    "protocol='cdp', name='external')\n"
                    "await ws.browser.context('again', connection='external')\n"
                    "print(json.dumps(True))",
                )
                reset = await execute(session, "await ws.reset()", wait_ms=15000)
                assert reset["state"] == "succeeded", reset
            assert await outside.title() == "outside"
        finally:
            await external.close()


async def test_normal_reset_flushes_har_and_rejected_reset_keeps_browser(workspace, browser_site):
    async with mcp_session(workspace) as session:
        check = await cell(
            session,
            "import asyncio\n"
            "ctx = await ws.browser.context(record_har_path='recording.har')\n"
            "page = await ctx.new_page()\n"
            f"await page.goto({browser_site!r})\n"
            "blocker = ws.tasks.start(asyncio.sleep(10))\n"
            "try:\n    await ws.reset()\n"
            "except RuntimeError:\n    rejected = True\n"
            "else:\n    rejected = False\n"
            "await blocker.cancel()\n"
            "print(json.dumps({'rejected': rejected, 'alive': not page.is_closed()}))",
        )
        assert check == {"rejected": True, "alive": True}
        reset = await execute(session, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded", reset
    saved = json.loads((workspace / "recording.har").read_text())
    assert any(
        entry["request"]["url"].startswith(browser_site) for entry in saved["log"]["entries"]
    )
