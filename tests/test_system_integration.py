from __future__ import annotations

import asyncio
import json

from conftest import decode_result, execute, mcp_session, poll_until_done, result_text


async def _json_cell(session, code: str) -> dict:
    payload = await execute(session, "import json\n" + code)
    text = result_text(payload).strip()
    assert text, payload
    return json.loads(text)


def _assert_envelope(payload: dict, workspace) -> None:
    assert {
        "collected_at",
        "duration_seconds",
        "scope",
        "sources",
        "warnings",
        "truncated",
    } <= payload.keys()
    assert payload["scope"]["pid"] > 0
    assert payload["scope"]["visibility"] == "kernel"
    assert payload["scope"]["workspace"] == str(workspace.resolve())
    assert isinstance(payload["sources"], dict)
    assert isinstance(payload["warnings"], list)


async def test_system_info_usage_processes_and_disks(workspace):
    async with mcp_session(workspace) as session:
        info = await _json_cell(session, "print(json.dumps(await ws.system.info()))")
        _assert_envelope(info, workspace)
        assert {
            "os",
            "python",
            "cpu",
            "memory",
            "storage",
            "limits",
        } <= info.keys()
        assert info["python"]["executable"]

        usage = await _json_cell(session, "print(json.dumps(await ws.system.usage(interval=0.1)))")
        _assert_envelope(usage, workspace)
        assert {"cpu", "memory", "swap", "network", "disk_io", "limits"} <= usage.keys()
        assert usage["duration_seconds"] >= 0

        own = await _json_cell(
            session,
            "import os\n"
            "print(json.dumps(await ws.system.processes("
            "pids=[os.getpid()], interval=0.1, cmdline=True)))",
        )
        _assert_envelope(own, workspace)
        assert own["processes"]
        process = own["processes"][0]
        assert process["pid"] == own["scope"]["pid"]
        assert isinstance(process["cmdline"], list)

        disks = await _json_cell(session, "print(json.dumps(await ws.system.disks(path='.')))")
        _assert_envelope(disks, workspace)
        assert disks["disks"]
        assert any(item["mountpoint"] == str(workspace.resolve()) for item in disks["disks"])


async def test_system_usage_does_not_block_another_cell(workspace):
    async with mcp_session(workspace) as session:
        pending = decode_result(
            await session.call_tool(
                "execute",
                {
                    "code": "system_usage = await ws.system.usage(interval=1.0, timeout=3)",
                    "wait_ms": 0,
                },
            )
        )
        assert pending["state"] in {"queued", "running"}
        await asyncio.sleep(0.1)

        quick = await execute(session, "40 + 2")
        assert quick["state"] == "succeeded"
        assert result_text(quick).strip() == "42"

        completed = await poll_until_done(session, pending["exec_id"], deadline_seconds=8)
        assert completed["state"] == "succeeded"


async def test_system_invalid_arguments_fail_before_starting_a_helper(workspace):
    async with mcp_session(workspace) as session:
        result = await _json_cell(
            session,
            """
old_probe = ws.system._probe
probe_calls = []
async def probe(*args, **kwargs):
    probe_calls.append(True)
    return {}
ws.system._probe = probe
try:
    try:
        await ws.system.usage(interval=0.05)
    except Exception as exc:
        error = type(exc).__name__
finally:
    ws.system._probe = old_probe
print(json.dumps({"error": error, "probe_calls": len(probe_calls)}))
            """,
        )
        assert result == {"error": "ValueError", "probe_calls": 0}


async def test_system_does_not_add_mcp_tools(workspace):
    async with mcp_session(workspace) as session:
        tools = await session.list_tools()
        assert {tool.name for tool in tools.tools} == {"init", "execute", "poll"}
