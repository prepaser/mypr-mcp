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
