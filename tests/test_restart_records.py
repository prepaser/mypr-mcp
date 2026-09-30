import json

import pytest

from mypr_mcp.history import History
from mypr_mcp.restart_records import finalize_origin, poll_restart


@pytest.mark.parametrize("state", ["succeeded", "failed"])
def test_origin_result_is_durable_and_finalization_is_idempotent(tmp_path, monkeypatch, state):
    from mypr_mcp import restart

    ident = "a" * 32
    restart_id = "b" * 32
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    record = {
        "id": ident,
        "generation": "old",
        "state": "restarting",
        "restart_id": restart_id,
        "client_id": "same-client",
        "connection_id": "old-link",
        "created": 1,
        "truncated": False,
    }
    (root / f"{ident}.json").write_text(json.dumps(record))
    ticket = {
        "id": restart_id,
        "origin": {"exec_id": ident},
        "state": "starting",
        "new_generation": None,
    }
    monkeypatch.setattr(restart, "read_ticket", lambda workspace, key: ticket)
    assert poll_restart(tmp_path, ident)["state"] == "running"
    ticket.update(
        state=state,
        new_generation="new",
        new_version="1.0",
        error="failure" if state == "failed" else None,
    )
    finalize_origin(tmp_path, ticket)
    finalize_origin(tmp_path, ticket)
    result = poll_restart(tmp_path, ident)
    assert result["state"] == state
    assert result["generation"] == "new"
    assert result["execution_generation"] == "old"
    assert len(result["output"]) == 1
    assert poll_restart(tmp_path, ident, result["cursor"])["output"] == []
    history = History(tmp_path)
    try:
        assert history.get(ident)["state"] == state
        assert history.recover() == 0
    finally:
        history.close()


async def test_historical_restart_poll_reports_current_manager_generation(tmp_path, monkeypatch):
    from mypr_mcp import restart
    from mypr_mcp.runtime import Runtime

    runtime = Runtime(tmp_path)
    runtime.generation = "current"
    root = runtime.root / "runs"
    root.mkdir()
    ident = "c" * 32
    (root / f"{ident}.json").write_text(
        json.dumps(
            {
                "id": ident,
                "generation": "old",
                "state": "succeeded",
                "restart_id": "d" * 32,
                "restart_result": "completed earlier",
                "truncated": False,
            }
        )
    )
    ticket = {
        "id": "d" * 32,
        "state": "succeeded",
        "origin": {"exec_id": ident},
        "new_generation": "previous",
    }
    monkeypatch.setattr(restart, "read_ticket", lambda workspace, key: ticket)
    result = await runtime.poll(ident)
    assert result["generation"] == "current"
    assert result["execution_generation"] == "old"
    assert result["restart"]["new_generation"] == "previous"


async def test_disconnected_bridge_restart_poll_keeps_budget_and_expiry(tmp_path, monkeypatch):
    from mypr_mcp import restart
    from mypr_mcp.bridge import ConnectionBridge
    from mypr_mcp.diagnostics import RPCError
    from mypr_mcp.journal import append_events

    ident, ticket_id = "e" * 32, "f" * 32
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    record = {"id": ident, "kind": "execution", "generation": "old", "state": "succeeded",
              "restart_id": ticket_id, "restart_result": "completed", "truncated": False}
    (root / f"{ident}.json").write_text(json.dumps(record))
    append_events(root / f"{ident}.jsonl", [{"type": "stream", "text": "x" * 1500}])
    ticket = {"id": ticket_id, "state": "succeeded", "origin": {"exec_id": ident},
              "new_generation": "new"}
    monkeypatch.setattr(restart, "read_ticket", lambda *args: ticket)
    bridge = ConnectionBridge(tmp_path)
    with pytest.raises(RPCError, match="cursor unchanged"):
        await bridge.request("poll", exec_id=ident, cursor=0, max_bytes=1024)
    page = await bridge.request("poll", exec_id=ident, cursor=0, max_bytes=4096)
    assert page["cursor"] == 2
    assert not page["has_more"]
    history = History(tmp_path)
    try:
        history.record("execution", record)
        history.mark_storage_evicted([f".mypr/runs/{ident}.jsonl"])
        expired = await bridge.request("poll", exec_id=ident, cursor=page["cursor"], max_bytes=1024)
        assert expired["output"] == []
        assert expired["output_evicted"]
        assert expired["warnings"][0]["code"] == "output_expired"
        assert expired["cursor"] == page["cursor"]
    finally:
        history.close()
