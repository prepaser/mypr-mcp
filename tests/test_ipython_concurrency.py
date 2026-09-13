import ast

from conftest import decode_result, execute, mcp_session, poll_until_done, result_text


async def test_interleaved_semicolons_and_nested_ipython_results(workspace):
    async with mcp_session(workspace) as session:
        await execute(session, "import asyncio\ngate = asyncio.Event()")
        pending = decode_result(
            await session.call_tool("execute", {"code": "await gate.wait()\n11", "wait_ms": 0})
        )
        suppressed = await execute(session, "gate.set()\n22;")
        resumed = await poll_until_done(session, pending["exec_id"])
        assert result_text(suppressed) == ""
        assert result_text(resumed).strip() == "11"
        values = await execute(
            session,
            f"(ws.tasks.get({pending['exec_id']!r}).result(), "
            f"await ws.tasks.get({suppressed['exec_id']!r}))",
        )
        assert ast.literal_eval(result_text(values)) == (11, 22)

        nested = await execute(session, "get_ipython().run_cell('3')\n42")
        observed = await execute(session, f"await ws.tasks.get({nested['exec_id']!r})")
        assert result_text(observed).strip() == "42"

        await execute(session, "gate.clear()")
        pending = decode_result(
            await session.call_tool("execute", {"code": "await gate.wait()\n33;", "wait_ms": 0})
        )
        shown = await execute(session, "gate.set()\n44")
        resumed = await poll_until_done(session, pending["exec_id"])
        assert result_text(shown).strip() == "44"
        assert result_text(resumed) == ""
