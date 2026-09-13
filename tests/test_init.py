from __future__ import annotations

import ast
import asyncio
import json
import sqlite3
from pathlib import Path

import pytest
from conftest import decode_result, execute, mcp_session, result_text, stop_manager

from mypr_mcp.transport import rpc, socket_path


def json_output(payload: dict) -> object:
    value = ast.literal_eval(result_text(payload).strip())
    return json.loads(value) if isinstance(value, str) else value


async def init(session, client_id: str | None = None) -> dict:
    arguments = {} if client_id is None else {"client_id": client_id}
    return decode_result(await session.call_tool("init", arguments))


async def test_execute_requires_init_without_creating_record(workspace: Path):
    async with mcp_session(workspace, initialize_client=False) as session:
        rejected = await session.call_tool("execute", {"code": "side_effect = True"})
        assert rejected.is_error
        assert "Call init before execute" in " ".join(block.text for block in rejected.content)

        records = await rpc(socket_path(workspace), op="history_list", limit=100)
        assert not any(item["kind"] == "execution" for item in records["items"])

        with pytest.raises(RuntimeError, match="init"):
            await rpc(
                socket_path(workspace),
                op="execute",
                code="side_effect = True",
                wait_ms=0,
                connection_id="uninitialized",
            )


async def test_poll_is_allowed_before_init(workspace: Path):
    async with mcp_session(workspace) as owner:
        completed = await execute(owner, "40 + 2")
        async with mcp_session(workspace, initialize_client=False) as uninitialized:
            polled = decode_result(
                await uninitialized.call_tool(
                    "poll", {"exec_id": completed["exec_id"], "wait_ms": 0}
                )
            )
            assert polled["state"] == "succeeded"
            assert result_text(polled).strip() == "42"


async def test_init_auto_id_is_readable_and_idempotent_under_concurrency(workspace: Path):
    async with mcp_session(workspace, initialize_client=False) as session:
        first, second = await asyncio.gather(init(session), init(session))
        assert first["client_id"]
        assert first["client_id"] == second["client_id"]
        repeated = await init(session)
        assert repeated == first
        adjective, animal = first["client_id"].split("-")
        assert adjective.islower() and animal.islower()


async def test_init_can_create_and_resume_a_named_client(workspace: Path):
    client_id = "resume-client"
    async with mcp_session(workspace, initialize_client=False) as first:
        created = await init(first, client_id)
        assert created["client_id"] == client_id
        await execute(
            first,
            "import asyncio\n"
            "ws.local['marker'] = 'persisted'\n"
            "ws.local['gate'] = asyncio.Event()\n"
            "ws.local['task'] = ws.tasks.start(ws.local['gate'].wait())\n"
            "await asyncio.sleep(0)",
        )
        counted = await execute(
            first,
            "from pathlib import Path\n"
            "counter = Path('resume-count.txt')\n"
            "value = int(counter.read_text()) if counter.exists() else 0\n"
            "counter.write_text(str(value + 1))\nvalue + 1",
            request_id="resume-request",
        )
        task_id = result_text(await execute(first, "ws.local['task'].id")).strip(" '\n")

    async with mcp_session(workspace, initialize_client=False) as second:
        resumed = await init(second, client_id)
        assert resumed["client_id"] == client_id
        state = await execute(
            second,
            "import json\n"
            f"json.dumps({{'marker': ws.local['marker'], "
            f"'task': ws.tasks.get({task_id!r}).status()['status']}})",
        )
        state_value = json_output(state)
        assert state_value["marker"] == "persisted"
        assert state_value["task"] == "running"
        request_code = (
            "from pathlib import Path\n"
            "counter = Path('resume-count.txt')\n"
            "value = int(counter.read_text()) if counter.exists() else 0\n"
            "counter.write_text(str(value + 1))\nvalue + 1"
        )
        duplicate = await execute(
            second,
            request_code,
            request_id="resume-request",
        )
        assert duplicate["exec_id"] == counted["exec_id"]
        assert result_text(duplicate).strip() == "1"


async def test_named_id_cannot_be_switched_and_invalid_ids_are_rejected(workspace: Path):
    async with mcp_session(workspace, initialize_client=False) as session:
        assert (await init(session, "fixed-client"))["client_id"] == "fixed-client"
        assert (await init(session, "fixed-client"))["client_id"] == "fixed-client"
        switched = await session.call_tool("init", {"client_id": "other-client"})
        assert switched.is_error
        assert "different client ID" in " ".join(block.text for block in switched.content)


@pytest.mark.parametrize("client_id", ["", "has space", "bad/id", "x" * 129])
async def test_init_rejects_invalid_client_ids(workspace: Path, client_id: str):
    async with mcp_session(workspace, initialize_client=False) as session:
        result = await session.call_tool("init", {"client_id": client_id})
        assert result.is_error
        assert "Client ID must be" in " ".join(block.text for block in result.content)


async def test_same_named_id_has_one_live_owner_and_loser_can_retry(workspace: Path):
    async with (
        mcp_session(workspace, initialize_client=False) as first,
        mcp_session(workspace, initialize_client=False) as second,
    ):
        results = await asyncio.gather(
            first.call_tool("init", {"client_id": "contested-client"}),
            second.call_tool("init", {"client_id": "contested-client"}),
        )
        successes = [result for result in results if not result.is_error]
        assert len(successes) == 1
        failure = next(result for result in results if result.is_error)
        assert "another connection" in " ".join(block.text for block in failure.content)
        loser = second if results[1].is_error else first
        retry = await init(loser, "loser-client")
        assert retry["client_id"] == "loser-client"


async def test_uninitialized_connection_does_not_count_as_client(workspace: Path):
    history_path = workspace / ".mypr" / "history.sqlite3"
    before = None
    during = None
    async with mcp_session(workspace, initialize_client=False):
        with sqlite3.connect(history_path) as database:
            before = database.execute("SELECT COUNT(*) FROM client_ids").fetchone()[0]
        async with mcp_session(workspace) as initialized:
            state = json_output(await execute(initialized, "await ws.status()"))
            assert state["connection_count"] == 2
            assert state["client_count"] == 1
            assert any(item.get("client_id") is None for item in state["connections"])
        with sqlite3.connect(history_path) as database:
            during = database.execute("SELECT COUNT(*) FROM client_ids").fetchone()[0]
    with sqlite3.connect(history_path) as database:
        after = database.execute("SELECT COUNT(*) FROM client_ids").fetchone()[0]
    assert during == before + 1
    assert after == during


async def test_resume_after_manager_restart_keeps_history_and_clears_memory(workspace: Path):
    client_id = "restart-client"
    async with mcp_session(workspace, initialize_client=False) as first:
        await init(first, client_id)
        saved = await execute(
            first, "memory_value = 42\nmemory_value", request_id="restart-request"
        )
        saved_id = saved["exec_id"]
    await stop_manager(workspace)

    async with mcp_session(workspace, initialize_client=False) as second:
        await init(second, client_id)
        memory = await execute(second, "'memory_value' in globals()")
        assert result_text(memory).strip() == "False"
        replayed = await execute(
            second, "memory_value = 42\nmemory_value", request_id="restart-request"
        )
        assert replayed["exec_id"] == saved_id
        records = json_output(
            await execute(second, f"await ws.history.list(client_id={client_id!r}, limit=100)")
        )
        assert any(
            item["id"] == saved_id and item["kind"] == "execution" for item in records["items"]
        )
