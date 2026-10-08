from __future__ import annotations

import json
from pathlib import Path

import pytest

import mypr_mcp.restart as restart
from mypr_mcp.history import History
from mypr_mcp.restart_records import finalize_origin
from mypr_mcp.transport import workspace_id


def _ticket(workspace: Path, ident: str, *, state: str = "starting") -> dict:
    return {
        "id": ident,
        "state": state,
        "workspace_id": workspace_id(workspace),
        "created_at": 1.0,
        "updated_at": 1.0,
        "coordinator_pid": 999_999_999,
        "coordinator_starttime": None,
        "target": {"python": "/usr/bin/python", "package_root": "/tmp", "version": "1"},
        "origin": None,
        "force": False,
        "error": None,
        "new_generation": None,
        "new_version": None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_recovery_prefers_terminal_current_ticket_over_stale_archive(
    tmp_path: Path, explicit: bool
):
    ident = "a" * 32
    ticket = _ticket(tmp_path, ident)
    restart._write_ticket(tmp_path, {**ticket, "state": "succeeded"})
    restart._atomic_write(restart._ticket_path(tmp_path, ident), ticket)

    result = await restart.recover_ticket(tmp_path, ident if explicit else None)

    assert result["state"] == "succeeded"
    assert restart.read_ticket(tmp_path)["state"] == "succeeded"
    assert restart.read_ticket(tmp_path, ident)["state"] == "succeeded"


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_recovery_reconciles_a_newer_terminal_archive(tmp_path: Path, explicit: bool):
    ident = "a" * 32
    current = _ticket(tmp_path, ident)
    restart._write_ticket(tmp_path, current)
    current = restart.read_ticket(tmp_path)
    archived = {**current, "state": "succeeded", "updated_at": current["updated_at"] + 1}
    restart._atomic_write(restart._ticket_path(tmp_path, ident), archived)

    result = await restart.recover_ticket(tmp_path, ident if explicit else None)

    assert result["state"] == "succeeded"
    assert restart.read_ticket(tmp_path)["state"] == "succeeded"
    assert restart.read_ticket(tmp_path, ident)["state"] == "succeeded"


@pytest.mark.asyncio
async def test_recovery_of_archived_old_ticket_does_not_replace_current_ticket(
    tmp_path: Path,
):
    old_id = "a" * 32
    current_id = "b" * 32
    restart._write_ticket(tmp_path, {**_ticket(tmp_path, current_id), "state": "succeeded"})
    restart._atomic_write(restart._ticket_path(tmp_path, old_id), _ticket(tmp_path, old_id))

    result = await restart.recover_ticket(tmp_path, old_id)

    assert result["state"] == "failed"
    assert restart.read_ticket(tmp_path)["id"] == current_id
    assert restart.read_ticket(tmp_path)["state"] == "succeeded"
    assert restart.read_ticket(tmp_path, old_id)["state"] == "failed"


@pytest.mark.parametrize("state", ["succeeded", "failed"])
@pytest.mark.parametrize("committed", [False, True])
async def test_terminal_ticket_retries_origin_history_without_duplicate_event(
    tmp_path, monkeypatch, state, committed
):
    ident, restart_id = "c" * 32, "d" * 32
    runs = tmp_path / ".mypr/runs"
    runs.mkdir(parents=True)
    path = runs / f"{ident}.json"
    record = {"id": ident, "kind": "execution", "generation": "old", "state": "restarting"}
    path.write_text(json.dumps(record))
    history = History(tmp_path)
    try:
        history.record("execution", record)
    finally:
        history.close()
    ticket = {
        **_ticket(tmp_path, restart_id, state=state),
        "origin": {"exec_id": ident},
        "error": "restart failed" if state == "failed" else None,
        "new_generation": "new",
    }
    restart._write_ticket(tmp_path, ticket)
    original = History.record

    def fail_record(self, *args, **kwargs):
        if committed:
            original(self, *args, **kwargs)
        raise OSError("transient history failure")

    monkeypatch.setattr(History, "record", fail_record)
    with pytest.raises(OSError, match="transient history failure"):
        finalize_origin(tmp_path, ticket)
    finalized = json.loads(path.read_text())
    monkeypatch.setattr(History, "record", original)

    assert (await restart.recover_ticket(tmp_path, restart_id))["state"] == state
    await restart.recover_ticket(tmp_path, restart_id)

    assert json.loads(path.read_text()) == finalized
    history = History(tmp_path)
    try:
        saved = history.get(ident)
        assert saved["state"] == state
        assert saved["restart_finalized"] == restart_id
        assert saved["finished"] == finalized["finished"]
        assert history._db.execute(
            "SELECT COUNT(*) FROM events WHERE id = ? AND event = ?", (ident, state)
        ).fetchone()[0] == 1
        assert history._db.execute(
            "SELECT updated FROM entities WHERE id = ?", (ident,)
        ).fetchone()[0] == finalized["finished"]
    finally:
        history.close()
