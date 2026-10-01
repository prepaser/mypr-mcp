from __future__ import annotations

import asyncio

from conftest import decode_result, execute, mcp_session, result_text, stop_manager


async def test_timer_capability_help_and_automatic_alert(workspace):
    async with mcp_session(workspace) as session:
        initial = decode_result(await session.call_tool("init", {}))
        assert "timers" in initial["runtime"]["capabilities"]

        help_result = await execute(session, 'ws.help("timers")')
        assert "ws.timers.start" in result_text(help_result)
        assert "automatically" in result_text(help_result)

        started = await execute(
            session,
            "timer = await ws.timers.start(seconds=0.05, label='integration')\n"
            "timer['id']",
        )
        timer_id = result_text(started).strip(" '\n")
        assert timer_id.startswith("timer-")

        await asyncio.sleep(0.1)
        response = decode_result(await session.call_tool("execute", {"code": "1", "wait_ms": 0}))
        assert response["timers"]["unacked"] == 1
        assert response["timers"]["items"][0]["id"] == timer_id

        acknowledged = await execute(
            session,
            f"await ws.timers.ack([{timer_id!r}])",
        )
        assert result_text(acknowledged).strip() == "1"

        clear = decode_result(await session.call_tool("execute", {"code": "1", "wait_ms": 0}))
        assert "timers" not in clear


async def test_timer_alert_survives_poll_and_is_client_isolated(workspace):
    async with (
        mcp_session(workspace, client_id="timer-a") as first,
        mcp_session(workspace, client_id="timer-b") as second,
    ):
        created = await execute(
            first,
            "timer = await ws.timers.start(seconds=0, label='a')\n"
            "timer['id']",
        )
        timer_id = result_text(created).strip(" '\n")
        await asyncio.sleep(0.02)

        first_response = decode_result(
            await first.call_tool("execute", {"code": "1", "wait_ms": 0})
        )
        second_response = decode_result(
            await second.call_tool("execute", {"code": "1", "wait_ms": 0})
        )
        assert first_response["timers"]["items"][0]["id"] == timer_id
        assert "timers" not in second_response

        repeated = decode_result(
            await first.call_tool("poll", {"exec_id": first_response["exec_id"], "wait_ms": 0})
        )
        assert repeated["timers"]["items"][0]["id"] == timer_id


async def test_unacknowledged_timer_survives_manager_restart(workspace):
    async with mcp_session(workspace, client_id="persistent-timer") as session:
        created = await execute(
            session,
            "timer = await ws.timers.start(seconds=0, label='persistent')\n"
            "timer['id']",
        )
        timer_id = result_text(created).strip(" '\n")
        reset = await execute(session, "await ws.reset()", wait_ms=15_000)
        assert reset["state"] == "succeeded"

    await stop_manager(workspace)
    async with mcp_session(workspace, client_id="persistent-timer") as session:
        initialized = decode_result(await session.call_tool("init", {}))
        assert initialized["timers"]["items"][0]["id"] == timer_id
