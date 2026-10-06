import json
import sqlite3
import threading

import pytest

import mypr_mcp.restart as restart
from mypr_mcp.history import History
from mypr_mcp.restart_records import _output_evicted, finalize_origin, poll_restart


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
    monkeypatch.setattr(restart, "read_ticket", lambda workspace, key=None: ticket)
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
    monkeypatch.setattr(restart, "read_ticket", lambda workspace, key=None: ticket)
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


async def test_abandoned_restart_is_recovered_before_polling_origin(tmp_path):
    from mypr_mcp import restart
    from mypr_mcp.restart_records import poll_restart
    from mypr_mcp.transport import workspace_id

    ident = "1" * 32
    ticket_id = "2" * 32
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    record = {
        "id": ident,
        "generation": "old",
        "state": "restarting",
        "restart_id": ticket_id,
        "client_id": "client",
        "connection_id": "connection",
        "created": 1,
    }
    (root / f"{ident}.json").write_text(json.dumps(record))
    ticket = {
        "id": ticket_id,
        "state": "starting",
        "workspace_id": workspace_id(tmp_path),
        "target": {"python": "/usr/bin/python", "package_root": "/tmp", "version": "1"},
        "coordinator_pid": 999999,
        "coordinator_starttime": None,
        "created_at": 1,
        "updated_at": 1,
        "origin": {"exec_id": ident},
    }
    restart._write_ticket(tmp_path, ticket)
    recovered = await restart.recover_ticket(tmp_path)
    assert recovered["state"] == "failed"
    assert poll_restart(tmp_path, ident)["state"] == "failed"


async def test_bridge_poll_recovers_archived_ticket_without_clobbering_current(tmp_path):
    from mypr_mcp import restart
    from mypr_mcp.bridge import ConnectionBridge
    from mypr_mcp.transport import workspace_id

    ident = "3" * 32
    abandoned_id = "4" * 32
    current_id = "5" * 32
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    (root / f"{ident}.json").write_text(
        json.dumps({"id": ident, "generation": "old", "state": "restarting",
                    "restart_id": abandoned_id})
    )
    base = {
        "workspace_id": workspace_id(tmp_path),
        "target": {"python": "/usr/bin/python", "package_root": "/tmp", "version": "1"},
        "coordinator_pid": 999999,
        "coordinator_starttime": None,
        "created_at": 1,
        "updated_at": 1,
    }
    restart._write_ticket(tmp_path, {
        **base, "id": abandoned_id, "state": "starting", "origin": {"exec_id": ident},
    })
    restart._write_ticket(tmp_path, {
        **base, "id": current_id, "state": "succeeded", "origin": None,
    })
    bridge = ConnectionBridge(tmp_path)
    result = await bridge._poll_restart(ident, 0, 0)
    assert result["state"] == "failed"
    assert restart.read_ticket(tmp_path)["id"] == current_id
    assert restart.read_ticket(tmp_path, abandoned_id)["state"] == "failed"


def test_restart_output_lookup_waits_for_temporary_database_lock(tmp_path):
    root = tmp_path / ".mypr"
    root.mkdir()
    connection = sqlite3.connect(root / "history.sqlite3", check_same_thread=False)
    connection.execute("CREATE TABLE entities (id TEXT, data TEXT)")
    connection.execute(
        "INSERT INTO entities VALUES (?, ?)", ("execution", '{"output_evicted":true}')
    )
    connection.commit()
    connection.execute("BEGIN EXCLUSIVE")
    release = threading.Timer(1.2, connection.commit)
    release.start()
    try:
        assert _output_evicted(tmp_path, "execution") is True
    finally:
        release.join()
        connection.close()


@pytest.mark.parametrize("override,unstable_config", [(None, False), (0, False), (None, True)])
async def test_offline_restart_poll_uses_configured_default(
    tmp_path, monkeypatch, override, unstable_config
):
    from mypr_mcp.bridge import ConnectionBridge
    from mypr_mcp.config import ConfigStore

    ConfigStore(tmp_path).set("limits.poll_wait_ms", 0 if override is None else 30000)
    bridge = ConnectionBridge(tmp_path)
    calls = []

    if unstable_config:
        def load(_self):
            raise RuntimeError("configuration changed while being read; retry")

        monkeypatch.setattr(ConfigStore, "load", load)

    async def recover(_ident):
        pass

    def result(*_args):
        calls.append(True)
        return {"state": "failed" if unstable_config else "running", "output": []}, None

    monkeypatch.setattr(bridge, "_recover_restart", recover)
    monkeypatch.setattr(bridge, "_restart_result", result)
    page = await bridge._poll_restart("a" * 32, 0, override)
    assert page["state"] == ("failed" if unstable_config else "running")
    assert calls == [True]


async def test_offline_restart_poll_uses_configured_response_budget(tmp_path, monkeypatch):
    from mypr_mcp.bridge import ConnectionBridge
    from mypr_mcp.config import ConfigSnapshot, ConfigStore
    from mypr_mcp.journal import append_events
    from mypr_mcp.transport import workspace_id

    snapshot = ConfigSnapshot(
        values={"limits": {"response_bytes": 1024, "poll_wait_ms": 0}},
        revision=None,
    )
    monkeypatch.setattr(ConfigStore, "load", lambda _self: snapshot)
    ident = "6" * 32
    ticket_id = "7" * 32
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    (root / f"{ident}.json").write_text(
        json.dumps(
            {
                "id": ident,
                "generation": "old",
                "state": "succeeded",
                "restart_id": ticket_id,
            }
        )
    )
    append_events(
        root / f"{ident}.jsonl",
        [{"type": "stream", "text": "x" * 100} for _ in range(20)],
    )
    restart._write_ticket(
        tmp_path,
        {
            "id": ticket_id,
            "state": "succeeded",
            "workspace_id": workspace_id(tmp_path),
            "target": {"python": "/usr/bin/python", "package_root": "/tmp", "version": "1"},
            "origin": {"exec_id": ident},
            "new_generation": "new",
        },
    )
    bridge = ConnectionBridge(tmp_path)
    async def recover(_ident):
        pass

    monkeypatch.setattr(bridge, "_recover_restart", recover)

    page = await bridge._poll_restart(ident, 0, 0)

    assert len(page["output"]) == 7
    assert page["cursor"] == 7
    assert page["has_more"]


def test_restart_poll_rejects_cursor_past_logical_end(tmp_path, monkeypatch):
    from mypr_mcp import restart
    from mypr_mcp.transport import workspace_id

    ident = "8" * 32
    ticket_id = "9" * 32
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    (root / f"{ident}.json").write_text(
        json.dumps(
            {
                "id": ident,
                "generation": "old",
                "state": "succeeded",
                "restart_id": ticket_id,
            }
        )
    )
    ticket = {
        "id": ticket_id,
        "state": "succeeded",
        "workspace_id": workspace_id(tmp_path),
        "target": {"python": "/usr/bin/python", "package_root": "/tmp", "version": "1"},
        "origin": {"exec_id": ident},
        "new_generation": "new",
    }
    monkeypatch.setattr(restart, "read_ticket", lambda *_args: ticket)

    with pytest.raises(ValueError, match="Invalid output cursor"):
        poll_restart(tmp_path, ident, cursor=1)
