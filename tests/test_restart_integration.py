from __future__ import annotations

import ast
import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from conftest import execute, mcp_session, result_text


def json_value(payload: dict) -> object:
    value = ast.literal_eval(result_text(payload).strip())
    return json.loads(value) if isinstance(value, str) else value


async def identity(session) -> dict:
    payload = await execute(
        session,
        "import json\n"
        "json.dumps({'client_id': ws.client.id, "
        "'connection_id': ws.client.connection_id, "
        "'generation': (await ws.status())['generation']})",
    )
    value = json_value(payload)
    assert isinstance(value, dict)
    return value


async def run_cli(workspace, *args: str) -> asyncio.subprocess.Process:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "mypr_mcp.cli",
        *args,
        cwd=workspace,
        env=dict(os.environ),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return process


async def communicate_cli(process: asyncio.subprocess.Process) -> tuple[int, str, str]:
    stdout, stderr = await asyncio.wait_for(process.communicate(), 240)
    return (
        process.returncode,
        stdout.decode(errors="replace"),
        stderr.decode(errors="replace"),
    )


async def test_python_restart_returns_recorded_result_and_does_not_replay_code(workspace):
    async with mcp_session(workspace, client_id="restart-owner") as session:
        before = await identity(session)
        result = await execute(
            session,
            "from pathlib import Path\n"
            "restart_marker = 'before'\n"
            "Path('restart-survives.txt').write_text('kept')\n"
            "await ws.restart()\n"
            "Path('must-not-run.txt').write_text('bad')\n"
            "restart_marker = 'after'",
            wait_ms=0,
        )

        assert result["state"] == "succeeded"
        assert result["execution_generation"] == before["generation"]
        assert result["generation"] != before["generation"]
        assert "Workspace restart completed" in result_text(result)
        assert (workspace / "restart-survives.txt").read_text() == "kept"
        assert not (workspace / "must-not-run.txt").exists()

        after = await identity(session)
        memory = await execute(session, "'restart_marker' in globals()")

    assert after["client_id"] == before["client_id"]
    assert after["connection_id"] != before["connection_id"]
    assert after["generation"] != before["generation"]
    assert result_text(memory).strip() == "False"


async def test_restart_reconnects_all_open_mcp_sessions_and_preserves_client_ids(workspace):
    async with (
        mcp_session(workspace, client_id="first-agent") as first,
        mcp_session(workspace, client_id="second-agent") as second,
    ):
        first_before, second_before = await asyncio.gather(identity(first), identity(second))
        await execute(first, "shared_before_restart = 41")

        restarted = await execute(first, "await ws.restart()", wait_ms=0)
        assert restarted["state"] == "succeeded"

        first_after, second_after = await asyncio.gather(identity(first), identity(second))
        globals_after = await execute(second, "'shared_before_restart' in globals()")

    assert first_after["client_id"] == first_before["client_id"]
    assert second_after["client_id"] == second_before["client_id"]
    assert first_after["connection_id"] != first_before["connection_id"]
    assert second_after["connection_id"] != second_before["connection_id"]
    assert first_after["generation"] == second_after["generation"]
    assert first_after["generation"] != first_before["generation"]
    assert result_text(globals_after).strip() == "False"


@pytest.mark.parametrize("resource", ["python", "shell", "scan"])
async def test_restart_rejects_busy_work_and_force_cancels_it(workspace, resource):
    async with mcp_session(workspace, client_id=f"busy-{resource}") as session:
        if resource == "python":
            start = await execute(
                session,
                "import asyncio\nbusy = ws.tasks.start(asyncio.Event().wait())\nbusy.id",
            )
        elif resource == "shell":
            start = await execute(session, "busy = await ws.shell.start('sleep 30')\nbusy.id")
        else:
            start = await execute(
                session,
                "busy = await ws.net.scan('127.0.0.1', ports='1-65535', "
                "concurrency=1, rate=1, timeout=1)\n"
                "busy.id",
            )
        task_id = result_text(start).strip(" '\n")
        assert task_id

        rejected = await execute(
            session,
            "try:\n    await ws.restart()\nexcept RuntimeError as exc:\n    print(str(exc))",
        )
        assert "active" in result_text(rejected).lower() or "busy" in result_text(rejected).lower()

        forced = await execute(session, "await ws.restart(force=True)", wait_ms=0)
        assert forced["state"] == "succeeded"
        assert forced["generation"] != forced["execution_generation"]


async def test_cli_restart_rebinds_existing_sessions(workspace):
    async with (
        mcp_session(workspace, client_id="cli-first") as first,
        mcp_session(workspace, client_id="cli-second") as second,
    ):
        first_before, second_before = await asyncio.gather(identity(first), identity(second))
        process = await run_cli(workspace, "restart")
        returncode, stdout, stderr = await communicate_cli(process)
        assert returncode == 0, (stdout, stderr)

        first_after, second_after = await asyncio.gather(identity(first), identity(second))

    assert first_after["client_id"] == first_before["client_id"]
    assert second_after["client_id"] == second_before["client_id"]
    assert first_after["connection_id"] != first_before["connection_id"]
    assert second_after["connection_id"] != second_before["connection_id"]
    assert first_after["generation"] == second_after["generation"]
    assert first_after["generation"] != first_before["generation"]


async def test_concurrent_cli_restarts_create_one_replacement(workspace):
    async with mcp_session(workspace, client_id="concurrent-owner") as session:
        first, second = await asyncio.gather(
            run_cli(workspace, "restart"),
            run_cli(workspace, "restart"),
        )
        first_result, second_result = await asyncio.gather(
            communicate_cli(first), communicate_cli(second)
        )
        results = [first_result, second_result]
        assert sum(result[0] == 0 for result in results) == 1
        failed = next(result for result in results if result[0] != 0)
        assert "restart" in failed[2].lower() or "already" in failed[2].lower()

        current = await identity(session)
        assert current["client_id"] == "concurrent-owner"


async def test_concurrent_submission_bursts_keep_kernel_receiving(workspace):
    async with (
        mcp_session(workspace, client_id="burst-first") as first,
        mcp_session(workspace, client_id="burst-second") as second,
    ):
        for _ in range(32):
            states = await asyncio.gather(identity(first), identity(second))
            assert [state["client_id"] for state in states] == ["burst-first", "burst-second"]


async def test_new_installation_reuses_then_explicitly_replaces_old_manager(workspace, tmp_path):
    import shutil

    from conftest import decode_result
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from packaging.version import Version

    import mypr_mcp

    installed = tmp_path / "new-installation"
    package = installed / "mypr_mcp"
    await asyncio.to_thread(shutil.copytree, Path(mypr_mcp.__file__).parent, package)
    target_version = f"{Version(mypr_mcp.__version__).major + 1}.0.0"
    await asyncio.to_thread(
        (package / "__init__.py").write_text, f"__version__ = {target_version!r}\n"
    )
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mypr_mcp.cli", "serve"],
        cwd=str(workspace),
        env={**os.environ, "PYTHONPATH": str(installed)},
    )
    async with mcp_session(workspace, client_id="old-frontend") as old:
        before = await identity(old)
        await execute(old, "preserved_until_restart = 42")
        async with stdio_client(params) as (reader, writer):
            async with ClientSession(reader, writer) as new:
                await new.initialize()
                initialized = decode_result(
                    await new.call_tool("init", {"client_id": "new-frontend"})
                )
                assert initialized["runtime"]["bridge_version"] == target_version
                assert initialized["runtime"]["manager_version"] == mypr_mcp.__version__
                assert initialized["runtime"]["update_pending"] is True
                assert (await identity(new))["generation"] == before["generation"]
                assert result_text(await execute(new, "preserved_until_restart")).strip() == "42"
                replaced = await execute(new, "await ws.restart()")
                assert replaced["state"] == "succeeded", replaced
                after = await identity(old)
                assert after["generation"] != before["generation"]
                loaded = await execute(new, "__import__('mypr_mcp').__version__")
                assert result_text(loaded).strip(" '\n") == target_version
                rejected = await execute(old, "await ws.restart()")
                assert rejected["state"] == "failed"
                assert "older version" in rejected["error"]
                assert (await identity(new))["generation"] == after["generation"]


async def test_approved_restart_finishes_after_requester_disconnects(workspace):
    from conftest import decode_result

    from mypr_mcp.restart import wait_ticket

    async with mcp_session(workspace, client_id="departing") as session:
        before = await identity(session)
        result = decode_result(
            await session.call_tool("execute", {"code": "await ws.restart()", "wait_ms": 0})
        )
        async with asyncio.timeout(10):
            while "restart" not in result:
                result = decode_result(
                    await session.call_tool("poll", {"exec_id": result["exec_id"], "wait_ms": 0})
                )
                await asyncio.sleep(0.05)
        ident = result["restart"]["id"]
    ticket = await wait_ticket(workspace, ident, timeout=30)
    assert ticket["state"] == "succeeded", ticket
    async with mcp_session(workspace, client_id="departing") as resumed:
        after = await identity(resumed)
        assert after["client_id"] == before["client_id"]
        assert after["generation"] != before["generation"]


async def test_invalid_restart_target_preserves_live_kernel(workspace):
    from mypr_mcp.protocol import target_installation
    from mypr_mcp.restart import request_restart

    async with mcp_session(workspace) as session:
        before = await identity(session)
        await execute(session, "keep_after_failed_restart = 42")
        target = {**target_installation(), "python": str(workspace / "missing-python")}
        with pytest.raises(ValueError, match="executable"):
            await request_restart(workspace, target)
        assert (await identity(session))["generation"] == before["generation"]
        assert result_text(await execute(session, "keep_after_failed_restart")).strip() == "42"
