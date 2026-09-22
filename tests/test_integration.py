from __future__ import annotations

import json
import sys
from pathlib import Path

from conftest import decode_result, execute, mcp_session, result_text, stop_manager

from mypr_mcp.instructions import INSTRUCTIONS


async def test_init_execute_and_poll_tools_and_expression_result(workspace: Path):
    async with mcp_session(workspace, initialize_client=False) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        assert set(tools) == {"init", "execute", "poll"}
        for tool in tools.values():
            assert tool.description
            assert all(prop.get("description") for prop in tool.input_schema["properties"].values())
        assert tools["execute"].input_schema["required"] == ["code"]
        assert tools["poll"].input_schema["required"] == ["exec_id"]
        assert tools["execute"].input_schema["properties"]["wait_ms"]["default"] == 1000
        assert tools["poll"].input_schema["properties"]["cursor"]["default"] is None

        initialized = decode_result(await session.call_tool("init", {}))
        assert "help" in initialized["runtime"]["capabilities"]
        assert initialized["runtime"]["instructions"] == INSTRUCTIONS
        help_result = await execute(
            session,
            'print(ws.help())\nprint(ws.help("fs"))',
        )
        assert help_result["state"] == "succeeded"
        assert "ws.fs.read" in result_text(help_result)
        assert "lifecycle" in result_text(help_result)
        for code, error in [
            ('ws.help("unknown-topic")', "ValueError"),
            ('ws.help(["fs"])', "TypeError"),
        ]:
            failed = await execute(session, code)
            assert failed["state"] == "failed"
            assert error in failed["error"]

        payload = await execute(session, "answer = 41\nanswer + 1")
        assert payload["state"] == "succeeded"
        assert result_text(payload).strip() == "42"


async def test_two_connections_share_namespace(workspace: Path):
    async with mcp_session(workspace) as first:
        created = await execute(first, "shared_value = 'from-first'\nshared_value")
        assert result_text(created).strip(" '\n") == "from-first"
        async with mcp_session(workspace) as second:
            observed = await execute(second, "shared_value")
            assert result_text(observed).strip(" '\n") == "from-first"
    async with mcp_session(workspace) as third:
        reconnected = await execute(third, "shared_value")
        assert result_text(reconnected).strip(" '\n") == "from-first"
        assert reconnected["generation"] == created["generation"]


async def test_workspaces_have_isolated_kernels(workspace: Path):
    other = workspace.parent / "other-workspace"
    other.mkdir()
    try:
        async with mcp_session(workspace) as left:
            await execute(left, "isolated_value = 'left'")
            async with mcp_session(other) as right:
                observed = await execute(right, "'isolated_value' in globals()")
                assert result_text(observed).strip() == "False"
    finally:
        await stop_manager(other)


async def test_shell_background_handle_is_controlled_by_next_cell(workspace: Path):
    async with mcp_session(workspace) as session:
        started = await execute(
            session,
            'job = await ws.shell.start("printf shell-ok; sleep 0.2")\njob.id',
        )
        job_id = result_text(started).strip(" '\n")
        assert len(job_id) == 32

        inspected = await execute(session, "await job\njob.status(), job.output()")
        text = result_text(inspected)
        assert "succeeded" in text
        assert "shell-ok" in text

        cancelled = await execute(session, 'job2 = await ws.shell.start("sleep 10")\njob2.id')
        assert len(result_text(cancelled).strip(" '\n")) == 32
        outcome = await execute(
            session,
            "await job2.cancel()\nawait __import__('asyncio').sleep(0.1)\njob2.status()",
        )
        assert "cancelled" in result_text(outcome)


async def test_async_tasks_result_and_cancel(workspace: Path):
    async with mcp_session(workspace) as session:
        started = await execute(
            session,
            "import asyncio\nfinished = ws.tasks.start(asyncio.sleep(0.1, result=7))\nfinished.id",
        )
        assert "task-" in result_text(started)
        result = await execute(session, "await finished")
        assert result_text(result).strip() == "7"

        background_output = await execute(
            session,
            "async def emit():\n    print('background-only')\n    return 8\n"
            "emitted = ws.tasks.start(emit())\nawait emitted\nemitted.result()",
        )
        assert result_text(background_output).strip() == "8"
        captured = await execute(session, "emitted.output()")
        assert "background-only" in result_text(captured)

        await execute(session, "pending = ws.tasks.start(asyncio.sleep(10))\npending.id")
        running = await execute(session, "pending.status()")
        assert "running" in result_text(running)
        cancelled = await execute(
            session,
            "await pending.cancel()\nawait asyncio.sleep(0)\npending.status()",
        )
        assert "cancelled" in result_text(cancelled)


async def test_reset_from_python_preserves_saved_assets_and_clears_memory(workspace: Path):
    async with mcp_session(workspace) as session:
        saved = await execute(
            session,
            "from pathlib import Path\n"
            "x = 123\n"
            "module = Path('.mypr/lib/ws_lib/persisted.py')\n"
            "module.write_text('VALUE = 9\\n')\n"
            "skill = Path('.mypr/skills/demo')\n"
            "skill.mkdir(parents=True, exist_ok=True)\n"
            "(skill / 'SKILL.md').write_text('# Demo\\n')\n"
            "import sys\n"
            "sys.path.insert(0, str(Path('.mypr/lib')))\n"
            "import ws_lib.persisted\n"
            "ws_lib.persisted.VALUE",
        )
        assert result_text(saved).strip() == "9"
        generation = saved["generation"]

        reset = await execute(session, "await ws.reset()", wait_ms=15_000)
        assert reset["state"] == "succeeded"
        assert "Workspace reset completed" in result_text(reset)
        assert reset["generation"] != generation

        state = await execute(session, "('x' in globals(), ws.skills.read('demo'))")
        assert "False" in result_text(state)
        assert "# Demo" in result_text(state)
        restored = await execute(session, "import ws_lib.persisted\nws_lib.persisted.VALUE")
        assert result_text(restored).strip() == "9"


async def test_skill_and_module_reload(workspace: Path):
    async with mcp_session(workspace) as session:
        payload = await execute(
            session,
            "from pathlib import Path\n"
            "import sys, importlib\n"
            "path = Path('.mypr/lib/ws_lib/reloadable.py')\n"
            "path.write_text('def value(): return 1\\n')\n"
            "sys.path.insert(0, str(Path('.mypr/lib')))\n"
            "import ws_lib.reloadable\n"
            "skill = Path('.mypr/skills/reload')\n"
            "skill.mkdir(parents=True, exist_ok=True)\n"
            "(skill / 'SKILL.md').write_text('---\\ndescription: test\\n---\\n# Reload\\n')\n"
            "(ws.skills.list(), ws.skills.read('reload'), ws_lib.reloadable.value())",
        )
        assert "reload" in result_text(payload)
        assert "1" in result_text(payload)
        changed = await execute(
            session,
            "path.write_text('def value(): return 22\\n')\n"
            "importlib.invalidate_caches()\n"
            "importlib.reload(ws_lib.reloadable)\n"
            "ws_lib.reloadable.value()",
        )
        assert result_text(changed).strip() == "22"


async def test_exception_and_output_limit(workspace: Path):
    (workspace / ".mypr").mkdir()
    (workspace / ".mypr" / "config.toml").write_text(
        "[limits]\noutput_bytes = 1024\nresponse_bytes = 1024\n"
    )
    async with mcp_session(workspace) as session:
        failed = await execute(session, "raise ValueError('expected failure')")
        assert failed["state"] == "failed"
        assert "expected failure" in (failed["error"] or "")

        large = await execute(session, "print('x' * 5000)")
        assert large["state"] == "succeeded"
        assert large["truncated"] is True
        assert len(result_text(large).encode()) <= 1024


async def test_burst_output_can_be_collected_across_poll_cursors(workspace: Path):
    async with mcp_session(workspace) as session:
        first = await execute(session, "print('z' * 5000)")
        chunks = [result_text(first)]
        cursor = first["cursor"]
        while first["has_more"]:
            first = decode_result(
                await session.call_tool("poll", {"exec_id": first["exec_id"], "cursor": cursor})
            )
            assert first["cursor"] > cursor
            chunks.append(result_text(first))
            cursor = first["cursor"]
        assert "z" * 5000 in "".join(chunks)


async def test_new_client_has_separate_request_scope_after_manager_restart(workspace: Path):
    code = (
        "from pathlib import Path\n"
        "counter = Path('counter.txt')\n"
        "value = int(counter.read_text()) if counter.exists() else 0\n"
        "counter.write_text(str(value + 1))\n"
        "value + 1"
    )
    async with mcp_session(workspace) as session:
        first = await execute(session, code, request_id="idempotent-side-effect")
        assert result_text(first).strip() == "1"
        await stop_manager(workspace)
    async with mcp_session(workspace) as session:
        repeated = await execute(session, code, request_id="idempotent-side-effect")
        assert result_text(repeated).strip() == "2"
        assert repeated["client_id"] != first["client_id"]
        again = await execute(session, code, request_id="idempotent-side-effect")
        assert again["exec_id"] == repeated["exec_id"]
        old = decode_result(await session.call_tool("poll", {"exec_id": first["exec_id"]}))
        assert result_text(old).strip() == "1"
    assert (workspace / "counter.txt").read_text() == "2"


async def test_external_stdio_mcp_is_available_inside_python(workspace: Path):
    config_dir = workspace / ".mypr"
    config_dir.mkdir()
    server = Path(__file__).with_name("echo_server.py")
    config = {
        "command": sys.executable,
        "args": [str(server)],
    }
    (config_dir / "config.toml").write_text(
        "[mcp.servers.echo]\n"
        f"command = {json.dumps(config['command'])}\n"
        f"args = [{json.dumps(str(server))}]\n"
    )
    async with mcp_session(workspace) as session:
        servers = await execute(session, "await ws.mcp.list_servers()")
        assert "echo" in result_text(servers)
        tools = await execute(session, "await ws.mcp.list_tools('echo')")
        assert "echo" in result_text(tools)
        called = await execute(
            session,
            "await ws.mcp.call_tool('echo', 'echo', {'text': 'hello from mcp'})",
        )
        assert "hello from mcp" in result_text(called)
