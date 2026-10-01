from __future__ import annotations

import asyncio
import json
import sys

from conftest import execute, mcp_session, result_text

from mypr_mcp.config import ConfigStore


async def test_reload_from_python_preserves_state_and_global_inheritance(
    workspace, tmp_path, monkeypatch,
):
    global_path = tmp_path / "global" / "config.toml"
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(global_path))
    store = ConfigStore(None, global_path=global_path)
    store.set("limits.response_bytes", 49152, scope="global")
    store.set("lsp.servers.demo", {
        "command": ["global-lsp"], "languages": ["python"],
    }, scope="global")

    async with mcp_session(workspace) as session:
        result = await execute(session, "\n".join([
            "import json",
            "ws.local['marker'] = object()",
            "marker = ws.local['marker']",
            "generation = (await ws.status())['generation']",
            "assert await ws.config.get('limits.response_bytes') == 49152",
            "await ws.config.set('limits.response_bytes', 65536)",
            "assert (await ws.config.explain('limits.response_bytes'))['pending']",
            "await ws.config.set('lsp.servers.demo', "
            "{'command': ['local-lsp'], 'languages': ['python']})",
            "await ws.code.reload()",
            "explanation = await ws.config.explain('lsp.servers.demo')",
            "assert explanation['applied']['command'] == ['local-lsp'], explanation",
            "assert not explanation['pending'], explanation",
            "result = await ws.config.reload()",
            "assert result['errors'] == {} and result['deferred'] == {}, result",
            "assert ws.code._definitions['demo']['command'] == ['local-lsp']",
            "assert not (await ws.config.explain('limits.response_bytes'))['pending']",
            "await ws.config.unset('lsp.servers.demo')",
            "result = await ws.config.reload()",
            "assert result['errors'] == {} and result['deferred'] == {}, result",
            "assert ws.code._definitions['demo']['command'] == ['global-lsp']",
            "await ws.config.unset('limits.response_bytes')",
            "await ws.config.set('limits.response_bytes', 73728, scope='global')",
            "assert ws.local['marker'] is marker",
            "assert (await ws.status())['generation'] == generation",
            "print(json.dumps({'generation': generation}))",
        ]))
        assert result["state"] == "succeeded", result

        generation = json.loads(result_text(result))["generation"]

        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "mypr_mcp.cli", "config", "reload", "--all",
            cwd=workspace, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 35)
        assert process.returncode == 0, (stdout, stderr)
        result = await execute(session, "\n".join([
            "assert (await ws.status())['generation'] == " + repr(generation),
            "assert ws.local['marker'] is marker",
            "explanation = await ws.config.explain('limits.response_bytes')",
            "assert explanation['applied'] == 73728 and not explanation['pending']",
            "assert explanation['source'] == 'global'",
        ]))
        assert result["state"] == "succeeded", result

        store.set("lsp.servers.demo", {
            "command": ["reset-lsp"], "languages": ["python"],
        }, scope="global")
        reset = await execute(session, "await ws.reset(force=True)")
        assert reset["state"] == "succeeded", reset
        result = await execute(session, "\n".join([
            "assert ws.code._definitions['demo']['command'] == ['reset-lsp']",
            "explanation = await ws.config.explain('lsp.servers.demo.command')",
            "assert explanation['applied'] == ['reset-lsp']",
            "assert not explanation['pending']",
        ]))
        assert result["state"] == "succeeded", result
