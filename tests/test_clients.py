from __future__ import annotations

import ast
import asyncio
import json
import os
import signal
import sys
from pathlib import Path

from conftest import execute, mcp_session, result_text


def json_output(payload: dict) -> object:
    """Decode the repr of a JSON string returned by IPython."""
    value = ast.literal_eval(result_text(payload).strip())
    return json.loads(value) if isinstance(value, str) else value


async def status(session) -> dict:
    payload = await execute(session, "await ws.status()")
    value = json_output(payload)
    assert isinstance(value, dict)
    return value


async def test_clients_have_logical_identity_and_isolated_local_state(workspace: Path):
    async with mcp_session(workspace, client_id="agent-a") as first:
        identity = await execute(
            first,
            "import json\n"
            "json.dumps({'id': ws.client.id, 'connection_id': ws.client.connection_id})",
        )
        identity = json_output(identity)
        assert identity["id"] == "agent-a"
        assert identity["connection_id"]

        await execute(
            first,
            "import asyncio\n"
            "ws.local['value'] = 'A'\n"
            "ws.local['fn'] = lambda value: f'A:{value}'\n"
            "async def emit_owner():\n"
            "    await asyncio.sleep(0.15)\n"
            "    print(ws.client.id)\n"
            "    return ws.client.id\n"
            "ws.local['job'] = ws.tasks.start(emit_owner())",
        )

        async with mcp_session(workspace, client_id="agent-b") as second:
            await execute(second, "shared_from_a = 'shared'\nws.local['value'] = 'B'")
            # Keep B's cell active while A's coroutine inherits A's execution context.
            await execute(second, "await asyncio.sleep(0.25)")
            local_b = await execute(
                second,
                "import json\n"
                "json.dumps({'value': ws.local['value'], 'has_fn': 'fn' in ws.local, "
                "'shared': shared_from_a})",
            )
            local_b = json_output(local_b)
            assert local_b == {"value": "B", "has_fn": False, "shared": "shared"}

        local_a = await execute(
            first,
            "json.dumps({'value': ws.local['value'], "
            "'fn': ws.local['fn']('ok'), 'result': ws.local['job'].result(), "
            "'status': ws.local['job'].status()['status']})",
        )
        assert json_output(local_a) == {
            "value": "A",
            "fn": "A:ok",
            "result": "agent-a",
            "status": "succeeded",
        }


async def test_reconnect_preserves_local_state_but_changes_connection(workspace: Path):
    async with mcp_session(workspace, client_id="stable-client") as first:
        created = await execute(
            first,
            "ws.local['persisted'] = 73\n"
            "import json\n"
            "json.dumps({'id': ws.client.id, 'connection_id': ws.client.connection_id})",
        )
        first_info = json_output(created)

    async with mcp_session(workspace, client_id="stable-client") as second:
        observed = await execute(
            second,
            "import json\n"
            "json.dumps({'id': ws.client.id, 'connection_id': ws.client.connection_id, "
            "'persisted': ws.local['persisted']})",
        )
        second_info = json_output(observed)

    assert second_info["id"] == first_info["id"] == "stable-client"
    assert second_info["connection_id"] != first_info["connection_id"]
    assert second_info["persisted"] == 73


def _find_client_process(client_id: str) -> int | None:
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError, UnicodeDecodeError:
            continue
        if "mypr_mcp.cli serve" in command and f"--client-id {client_id}" in command:
            return int(entry.name)
    return None


async def test_status_tracks_connections_unique_clients_and_abrupt_disconnect(workspace: Path):
    async with mcp_session(workspace, client_id="same-client") as first:
        async with mcp_session(workspace, client_id="other-client") as second:
            state = await status(second)
            assert state["connection_count"] == 2
            assert state["client_count"] == 2
            assert {item["client_id"] for item in state["connections"]} == {
                "same-client",
                "other-client",
            }
            for item in state["connections"]:
                assert item["connection_id"]
                assert item["connected_at"] <= item["last_activity"]
                assert item["active"] is None or isinstance(item["active"], str)
                assert isinstance(item["task_ids"], list)

        state = await status(first)
        assert state["connection_count"] == 1
        assert state["client_count"] == 1

        async with mcp_session(workspace, client_id="abrupt-client") as abrupt:
            await execute(abrupt, "1 + 1")
            pid = _find_client_process("abrupt-client")
            assert pid is not None
            os.kill(pid, signal.SIGKILL)

        for _ in range(40):
            state = await status(first)
            if state["connection_count"] == 1:
                break
            await asyncio.sleep(0.1)
        assert state["connection_count"] == 1
        assert state["client_count"] == 1


async def test_request_ids_are_scoped_to_logical_client(workspace: Path):
    async with mcp_session(workspace, client_id="client-a") as first:
        async with mcp_session(workspace, client_id="client-b") as second:
            left = await execute(first, "'left'", request_id="same-request")
            right = await execute(second, "'right'", request_id="same-request")
            assert result_text(left).strip(" '\n") == "left"
            assert result_text(right).strip(" '\n") == "right"

            duplicate = await execute(first, "'left'", request_id="same-request")
            assert duplicate["exec_id"] == left["exec_id"]
            assert result_text(duplicate).strip(" '\n") == "left"


async def test_history_lists_and_gets_execution_and_task_owners(workspace: Path):
    async with mcp_session(workspace, client_id="history-a") as first:
        execution = await execute(first, "print('history-execution')\n40 + 2")
        task = await execute(
            first,
            'job = await ws.shell.start("printf history-task")\njob.id',
        )
        task_id = result_text(task).strip(" '\n")
        await execute(first, "await job")
        await asyncio.sleep(0.4)

        records = await execute(
            first,
            "import json\njson.dumps(await ws.history.list(client_id='history-a'))",
        )
        records = json_output(records)
        items = records["items"] if isinstance(records, dict) else records
        assert any(
            item["id"] == execution["exec_id"] and item["kind"] == "execution" for item in items
        )
        assert any(item["id"] == task_id and item["kind"] in {"shell", "task"} for item in items)
        assert all(item["client_id"] == "history-a" for item in items)

        detail = await execute(
            first,
            f"import json\njson.dumps(await ws.history.get({execution['exec_id']!r}))",
        )
        detail = json_output(detail)
        assert detail["id"] == execution["exec_id"]
        assert detail["kind"] == "execution"
        assert detail["code"].endswith("40 + 2")
        assert detail["client_id"] == "history-a"
        assert any("history-execution" in event.get("text", "") for event in detail["output"])

        task_detail = await execute(
            first,
            f"import json\njson.dumps(await ws.history.get({task_id!r}))",
        )
        task_detail = json_output(task_detail)
        assert task_detail["id"] == task_id
        assert task_detail["client_id"] == "history-a"
        assert "history-task" in task_detail.get("output", "")


async def test_history_logs_cursor_is_stable_and_does_not_duplicate(workspace: Path):
    async with mcp_session(workspace, client_id="logs-client") as session:
        await execute(session, "'first-log-event'")
        first = await execute(
            session,
            "import json\njson.dumps(await ws.history.logs(client_id='logs-client', limit=100))",
        )
        first = json_output(first)
        assert isinstance(first, dict)
        cursor = first["cursor"]
        first_events = first["events"]

        await execute(session, "'second-log-event'")
        second = await execute(
            session,
            "import json\n"
            "json.dumps(await ws.history.logs(client_id='logs-client', "
            f"cursor={cursor!r}, limit=100))",
        )
        second = json_output(second)
        assert all(event not in first_events for event in second["events"])
        assert any("second-log-event" in json.dumps(event) for event in second["events"])
        assert second["cursor"] > cursor


async def test_cli_logs_follow_prints_new_events(workspace: Path):
    async with mcp_session(workspace, client_id="logs-cli") as session:
        await execute(session, "'follow-first-event'")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "mypr_mcp.cli",
            "logs",
            "--client-id",
            "logs-cli",
            "--limit",
            "1",
            "--follow",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(workspace),
            env=dict(os.environ),
        )
        try:
            assert process.stdout is not None
            await asyncio.wait_for(process.stdout.readline(), 5)
            await execute(session, "'follow-second-event'")
            lines = []
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                line = await asyncio.wait_for(process.stdout.readline(), 1)
                if not line:
                    break
                lines.append(line.decode())
                if "follow-second-event" in lines[-1]:
                    break
            assert any("follow-second-event" in line for line in lines)
        finally:
            process.terminate()
            await asyncio.wait_for(process.wait(), 5)


async def test_live_python_output_retains_owner_before_task_finishes(workspace):
    async with mcp_session(workspace, client_id="live-a") as first:
        await execute(
            first,
            "import asyncio\n"
            "async def live_output():\n"
            "    print('live-owner:' + ws.client.id)\n"
            "    await asyncio.sleep(20)\n"
            "ws.local['job'] = ws.tasks.start(live_output())",
        )
        async with mcp_session(workspace, client_id="live-b") as second:
            await execute(second, "await asyncio.sleep(0.4)")
            logs = json_output(
                await execute(second, "await ws.history.logs(client_id='live-a', limit=200)")
            )
            outputs = [
                event
                for event in logs["events"]
                if event["kind"] == "python" and event["event"] == "output"
            ]
            assert "".join(event["data"]["text"] for event in outputs) == "live-owner:live-a\n"
            assert all(event["client_id"] == "live-a" for event in outputs)
        state = json_output(await execute(first, "ws.local['job'].status()"))
        assert state["status"] == "running"
        await execute(first, "await ws.local['job'].cancel()")
