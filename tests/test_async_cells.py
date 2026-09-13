from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest
from conftest import decode_result, execute, mcp_session, poll_until_done, result_text

from mypr_mcp.transport import rpc, socket_path


async def submit(session, code: str) -> dict:
    return decode_result(await session.call_tool("execute", {"code": code, "wait_ms": 0}))


async def wait_until_running(session, exec_id: str) -> dict:
    for _ in range(40):
        payload = decode_result(
            await session.call_tool("poll", {"exec_id": exec_id, "cursor": 0, "wait_ms": 100})
        )
        if payload["state"] == "running":
            return payload
        if payload["state"] in {"failed", "cancelled", "lost"}:
            raise AssertionError(f"cell did not start: {payload}")
        await asyncio.sleep(0.05)
    raise AssertionError(f"cell did not start: {exec_id}")


async def test_cells_overlap_across_clients_and_keep_output_owner(workspace: Path):
    async with mcp_session(workspace) as first:
        setup = await execute(first, "import asyncio\ngate = asyncio.Event()")
        first_id = setup["client_id"]
        first_cell = await submit(
            first,
            "await gate.wait()\nprint('released-by:' + ws.client.id)\n'a-result'",
        )
        await wait_until_running(first, first_cell["exec_id"])

        async with mcp_session(workspace) as second:
            second_cell = await submit(
                second,
                "gate.set()\nprint('set-by:' + ws.client.id)\n'b-result'",
            )
            second_id = second_cell["client_id"]
            second_done = await poll_until_done(second, second_cell["exec_id"])

        first_done = await poll_until_done(first, first_cell["exec_id"])

    assert second_done["state"] == "succeeded"
    assert first_done["state"] == "succeeded"
    assert f"set-by:{second_id}" in result_text(second_done)
    assert f"released-by:{first_id}" not in result_text(second_done)
    assert f"released-by:{first_id}" in result_text(first_done)
    assert f"set-by:{second_id}" not in result_text(first_done)
    assert first_done["client_id"] == first_id
    assert second_done["client_id"] == second_id


async def test_same_client_cells_overlap_and_share_globals_and_functions(workspace: Path):
    async with mcp_session(workspace) as session:
        await execute(
            session,
            "import asyncio\n"
            "shared_log = []\n"
            "ready = asyncio.Event()\n"
            "release = asyncio.Event()\n"
            "def remembered(value):\n"
            "    shared_log.append(value)\n"
            "    return value.upper()\n",
        )
        first_cell = await submit(
            session,
            "ready.set()\nawait release.wait()\nshared_log.append('first')\n'first-done'",
        )
        await wait_until_running(session, first_cell["exec_id"])
        second_cell = await submit(
            session,
            "await ready.wait()\n"
            "shared_log.append('second')\n"
            "release.set()\n"
            "remembered('function')",
        )
        second_done = await poll_until_done(session, second_cell["exec_id"])
        first_done = await poll_until_done(session, first_cell["exec_id"])
        observed = await execute(session, "sorted(shared_log), remembered('persisted')")

    assert first_done["state"] == "succeeded"
    assert second_done["state"] == "succeeded"
    assert "FUNCTION" in result_text(second_done)
    assert "first" in result_text(observed)
    assert "second" in result_text(observed)
    assert "PERSISTED" in result_text(observed)


async def test_cell_handles_expose_result_and_cancel_finally(workspace: Path):
    async with mcp_session(workspace) as session:
        completed = await execute(session, "40 + 2")
        handle = await execute(
            session,
            f"handle = ws.tasks.get({completed['exec_id']!r})\n"
            "(handle.status()['kind'], handle.result(), "
            "any(item['id'] == handle.id and item['kind'] == 'cell' for item in ws.tasks.list()))",
        )
        assert "cell" in result_text(handle)
        assert "42" in result_text(handle)

        await execute(session, "import asyncio\ncancelled_in_finally = False")
        pending = await submit(
            session,
            "job_gate = asyncio.Event()\n"
            "async def detached():\n"
            "    await job_gate.wait()\n"
            "    return 9\n"
            "job = ws.tasks.start(detached())\n"
            "try:\n"
            "    await job\n"
            "finally:\n"
            "    cancelled_in_finally = True\n",
        )
        await wait_until_running(session, pending["exec_id"])
        cancel = await execute(
            session,
            f"handle = ws.tasks.get({pending['exec_id']!r})\nawait handle.cancel()\nTrue",
        )
        pending_done = await poll_until_done(session, pending["exec_id"])
        detached_result = await execute(
            session,
            "job_gate.set()\n(cancelled_in_finally, await job)",
        )

    assert cancel["state"] == "succeeded"
    assert pending_done["state"] == "cancelled"
    assert "True" in result_text(detached_result)
    assert "9" in result_text(detached_result)


async def test_cell_cannot_await_itself(workspace: Path):
    async with mcp_session(workspace) as session:
        pending = await submit(
            session,
            "active = (await ws.status())['active']\n"
            "exec_id = active[-1] if isinstance(active, list) else active\n"
            "await ws.tasks.get(exec_id)",
        )
        done = await poll_until_done(session, pending["exec_id"])

    assert done["state"] == "failed"
    assert any(word in (done["error"] or "").lower() for word in ("itself", "self", "cycle"))


async def test_reset_uses_live_tasks_instead_of_delayed_history(workspace: Path):
    async with mcp_session(workspace) as session:
        reset = await execute(
            session,
            "import asyncio\n"
            "import mypr_mcp.kernel_api as api\n"
            "original_rpc = api._rpc\n"
            "reported = asyncio.Event()\n"
            "async def delayed_report(op, **fields):\n"
            "    if op == 'task_event':\n"
            "        if fields['event']['state'] != 'running':\n"
            "            await asyncio.Event().wait()\n"
            "        result = await original_rpc(op, **fields)\n"
            "        reported.set()\n"
            "        return result\n"
            "    return await original_rpc(op, **fields)\n"
            "api._rpc = delayed_report\n"
            "job = ws.tasks.start(asyncio.sleep(0))\n"
            "await job\n"
            "await reported.wait()\n"
            "await ws.reset()",
            wait_ms=15_000,
        )
        assert reset["state"] == "succeeded", reset
        assert "Workspace reset completed" in result_text(reset)


async def test_reset_rejects_busy_and_force_resets_generation(workspace: Path):
    async with mcp_session(workspace) as session:
        rejected = await execute(
            session,
            "import asyncio\n"
            "task = asyncio.create_task(ws.reset(force=True))\n"
            "try:\n"
            "    await task\n"
            "except RuntimeError as exc:\n"
            "    print(str(exc))",
        )
        assert "foreground" in result_text(rejected).lower()

        pending = await submit(session, "import asyncio\nawait asyncio.Event().wait()")
        await wait_until_running(session, pending["exec_id"])
        old_generation = pending["generation"]

        with pytest.raises(RuntimeError, match="active|busy"):
            await rpc(socket_path(workspace), op="reset")

        python_reset = await execute(session, "await ws.reset(force=True)", wait_ms=15_000)
        cancelled = await poll_until_done(session, pending["exec_id"])
        assert python_reset["state"] == "succeeded"
        assert "Workspace reset completed" in result_text(python_reset)
        assert python_reset["generation"] != old_generation

        next_pending = await submit(session, "import asyncio\nawait asyncio.Event().wait()")
        await wait_until_running(session, next_pending["exec_id"])
        reset = await rpc(socket_path(workspace), op="reset", force=True)
        next_cancelled = await poll_until_done(session, next_pending["exec_id"])
        recovered = await execute(session, "6 * 7")

    assert reset["reset"] is True
    assert reset["generation"] != python_reset["generation"]
    assert cancelled["state"] == "cancelled"
    assert next_cancelled["state"] == "cancelled"
    assert recovered["state"] == "succeeded"
    assert recovered["generation"] == reset["generation"]
    assert result_text(recovered).strip() == "42"


async def test_rich_display_errors_and_history_preserve_cell_output(workspace: Path):
    async with mcp_session(workspace) as first:
        await execute(first, "import asyncio\ngate = asyncio.Event()")
        first_cell = await submit(
            first,
            "from IPython.display import HTML, display\n"
            "print('a-before')\n"
            "await gate.wait()\n"
            "display(HTML('<b>a-rich</b>'))\n"
            "raise ValueError('a-error')",
        )
        await wait_until_running(first, first_cell["exec_id"])
        async with mcp_session(workspace) as second:
            second_cell = await submit(
                second,
                "from IPython.display import HTML, display\n"
                "display(HTML('<b>b-rich</b>'))\n"
                "print('b-before')\n"
                "raise ValueError('b-error')",
            )
            second_done = await poll_until_done(second, second_cell["exec_id"])
        await execute(first, "gate.set()")
        first_done = await poll_until_done(first, first_cell["exec_id"])

        first_detail = await execute(first, f"await ws.history.get({first_cell['exec_id']!r})")
        second_detail = await execute(first, f"await ws.history.get({second_cell['exec_id']!r})")

    first_record = ast.literal_eval(result_text(first_detail).strip())
    second_record = ast.literal_eval(result_text(second_detail).strip())
    first_events = first_record["output"]
    second_events = second_record["output"]

    assert first_done["state"] == "failed"
    assert second_done["state"] == "failed"
    assert "a-error" in (first_done["error"] or "")
    assert "b-error" in (second_done["error"] or "")
    assert "b-before" not in result_text(first_done)
    assert "a-before" not in result_text(second_done)
    assert any(
        artifact["mime"] == "text/html"
        for event in first_done["output"]
        for artifact in event.get("artifacts", [])
    )
    assert any(
        artifact["mime"] == "text/html"
        for event in second_done["output"]
        for artifact in event.get("artifacts", [])
    )
    assert first_events[0]["text"] == "a-before\n"
    assert any("a-error" in event.get("text", "") for event in first_events)
    assert any("b-before" in event.get("text", "") for event in second_events)
    assert any("b-error" in event.get("text", "") for event in second_events)


async def test_ipython_magics_keep_workspace_and_output_semantics(workspace: Path):
    script = workspace / "magic_script.py"
    script.write_text("magic_value = 17\n")
    async with mcp_session(workspace) as session:
        pwd = await execute(session, "%pwd")
        timed = await execute(session, "%time 1 + 1")
        ran = await execute(session, "%run magic_script.py\nmagic_value")
        shell = await execute(session, "!printf shell-magic")
        captured = await execute(session, "%%capture captured_output\nprint('captured-magic')")
        captured_value = await execute(session, "captured_output.stdout")
        bash = await execute(session, "%%bash\nprintf bash-magic")
        suppressed = await execute(session, "'hidden-result';")
        future = await execute(
            session,
            "from __future__ import annotations\n"
            "class FutureExample:\n"
            "    value: MissingType\n"
            "FutureExample.__name__",
        )

    assert str(workspace) in result_text(pwd)
    assert "2" in result_text(timed)
    assert "CPU times" in result_text(timed) or "Wall time" in result_text(timed)
    assert "17" in result_text(ran)
    assert "shell-magic" in result_text(shell)
    assert captured["state"] == "succeeded"
    assert "captured-magic" in result_text(captured_value)
    assert "bash-magic" in result_text(bash)
    assert "hidden-result" not in result_text(suppressed)
    assert "FutureExample" in result_text(future)
