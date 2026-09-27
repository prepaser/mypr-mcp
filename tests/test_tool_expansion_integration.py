import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import execute, mcp_session, result_text


def _repository(path):
    commands = [
        ["init", "-q"],
        ["add", "sample.py"],
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"],
    ]
    for args in commands:
        subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True)


async def test_history_and_local_diagnostics_work_through_workspace_rpc(workspace):
    (workspace / "sample.py").write_text("VALUE = 1\n")
    await asyncio.to_thread(_repository, workspace)
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import json, os\n"
            "ws.local['log'] = await ws.git.log(path='sample.py')\n"
            "ws.local['blame'] = await ws.git.blame('sample.py')\n"
            "ws.local['process'] = await ws.system.process(os.getpid(), sockets=True)\n"
            "ws.local['sockets'] = await ws.net.sockets(pid=os.getpid())\n"
            "print(json.dumps({'log': ws.local['log'], 'blame': ws.local['blame'], "
            "'process': ws.local['process'], 'sockets': ws.local['sockets']}))",
        )
        assert result["state"] == "succeeded", result
        payload = json.loads(result_text(result))
        assert len(payload["log"]["commits"]) == 1
        assert len(payload["blame"]["lines"]) == 1
        assert payload["process"]["scope"]["pid"] > 0
        assert "sockets" in payload["sockets"]


async def test_browser_observation_and_saved_snapshot_span_cells(workspace, monkeypatch):
    cache = Path(os.environ.get("MYPR_TEST_BROWSER_PATH", "/tmp/mypr-playwright-browsers"))
    if not await asyncio.to_thread(cache.exists):
        pytest.skip("Install Chromium into MYPR_TEST_BROWSER_PATH for browser integration")
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(cache))
    async with mcp_session(workspace) as session:
        first = await execute(
            session,
            "ws.local['ctx'] = await ws.browser.context()\n"
            "ws.local['page'] = await ws.local['ctx'].new_page()\n"
            "ws.local['observation'] = await ws.browser.observe(ws.local['page'])\n"
            "await ws.local['page'].set_content('<button>Original</button>')\n"
            "await ws.local['page'].evaluate(\"console.error('observed error')\")\n"
            "ws.local['snapshot'] = await ws.browser.snapshot(ws.local['page'])",
        )
        assert first["state"] == "succeeded", first
        second = await execute(
            session,
            "import json\n"
            "await ws.local['page'].set_content('<button>Changed</button>')\n"
            "print(json.dumps({'events': await ws.local['observation'].read(), "
            "'snapshot': ws.local['snapshot']}))",
        )
        assert second["state"] == "succeeded", second
        payload = json.loads(result_text(second))
        assert "observed error" in json.dumps(payload["events"])
        assert "Original" in json.dumps(payload["snapshot"])
        assert "Changed" not in json.dumps(payload["snapshot"])
        reset = await execute(session, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded", reset


@pytest.mark.skipif(shutil.which("ast-grep") is None, reason="ast-grep is optional")
async def test_ast_plan_applies_exact_preview_through_workspace_shell(workspace):
    source = workspace / "sample.py"
    original = b"logger.info('hello')\r\n"
    source.write_bytes(original)
    async with mcp_session(workspace) as session:
        preview = await execute(
            session,
            "import json\n"
            "ws.local['rewrite'] = await ws.fs.rewrite_ast('logger.info($X)', "
            "replacement='logger.debug($X)', lang='python', paths='sample.py')\n"
            "print(json.dumps({'applicable': ws.local['rewrite']['applicable']}))",
        )
        assert preview["state"] == "succeeded", preview
        assert json.loads(result_text(preview))["applicable"]
        assert source.read_bytes() == original
        applied = await execute(
            session, "await ws.fs.apply_rewrite(ws.local['rewrite']['plan_id'])"
        )
        assert applied["state"] == "succeeded", applied
        assert source.read_bytes() == b"logger.debug('hello')\r\n"
