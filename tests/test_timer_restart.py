from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path

import pytest

import mypr_mcp.bridge as bridge_module
import mypr_mcp.restart as restart
import mypr_mcp.timers as timers_module
from mypr_mcp.bridge import ConnectionBridge
from mypr_mcp.diagnostics import RPCError
from mypr_mcp.history import History
from mypr_mcp.messages import MessageStore
from mypr_mcp.protocol import descriptor, target_installation
from mypr_mcp.timers import TimerStore
from mypr_mcp.transport import workspace_id


@pytest.fixture
def timer_workspace(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    store = TimerStore(workspace)
    try:
        yield workspace, store
    finally:
        store.close()


def _restart_fixture(
    workspace: Path,
    *,
    state: str = "succeeded",
    client_id: str = "alice",
    connection_id: str = "connection",
    request_id: str | None = None,
    code: str | None = None,
    exec_id: str = "a" * 32,
    ticket_id: str = "b" * 32,
):
    target = target_installation()
    ticket = {
        "id": ticket_id,
        "state": state,
        "workspace_id": workspace_id(workspace),
        "created_at": time.time(),
        "updated_at": time.time(),
        "coordinator_pid": None,
        "coordinator_starttime": None,
        "old_pid": None,
        "old_generation": "old",
        "target": target,
        "origin": {
            "exec_id": exec_id,
            "client_id": client_id,
            "connection_id": connection_id,
            **({"request_id": request_id} if request_id is not None else {}),
        },
        "force": False,
        "error": None,
        "new_generation": "new" if state == "succeeded" else None,
        "new_version": target["version"] if state == "succeeded" else None,
    }
    restart._write_ticket(workspace, ticket)
    runs = workspace / ".mypr" / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    record = {
        "id": exec_id,
        "kind": "execution",
        "generation": "old",
        "state": "restarting" if state not in {"succeeded", "failed"} else state,
        "restart_id": ticket_id,
        "client_id": client_id,
        "connection_id": connection_id,
        "created": time.time(),
        "truncated": False,
        "restart_result": "Workspace restart completed" if state == "succeeded" else None,
        **({"code": code} if code is not None else {}),
    }
    (runs / f"{exec_id}.json").write_text(json.dumps(record))
    return exec_id, ticket


def _old_bridge_state():
    return {**descriptor(), "generation": "old"}


async def _noop(*args, **kwargs):
    return None


async def test_disconnected_initial_poll_returns_only_client_timer(timer_workspace, monkeypatch):
    workspace, store = timer_workspace
    alice_timer = store.start("alice", seconds=0, label="alice")
    store.start("bob", seconds=0, label="bob")
    exec_id, _ = _restart_fixture(workspace)
    store.close()

    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert result["state"] == "succeeded"
    assert [item["id"] for item in result["timers"]["items"]] == [alice_timer["id"]]


@pytest.mark.parametrize("state", ["succeeded", "failed"])
async def test_restart_poll_includes_offline_message_preview(timer_workspace, state, monkeypatch):
    workspace, store = timer_workspace
    messages = MessageStore(workspace)
    sent = messages.send("bob", "alice", f"restart {state}")
    messages.close()
    exec_id, _ = _restart_fixture(workspace, state=state)
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    monkeypatch.setattr(bridge, "_recover_restart", _noop)

    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert result["state"] == state
    assert result["inbox"]["unacked"] == 1
    assert result["inbox"]["messages"][0]["id"] == sent["id"]


async def test_running_restart_poll_wakes_for_offline_message(timer_workspace, monkeypatch):
    workspace, store = timer_workspace
    messages = MessageStore(workspace)
    exec_id, _ = _restart_fixture(workspace, state="starting")
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    monkeypatch.setattr(bridge, "_recover_restart", _noop)

    async def deliver():
        await asyncio.sleep(0.1)
        messages.send("bob", "alice", "arrived")
        messages.close()

    delivery = asyncio.create_task(deliver())
    try:
        result = await asyncio.wait_for(bridge._poll_restart(exec_id, 0, 3000), 2)
    finally:
        await delivery
        messages.close()
    assert result["state"] == "running"
    assert result["inbox"]["messages"][0]["text"] == "arrived"


async def test_restart_poll_does_not_leak_timer_before_init(timer_workspace, monkeypatch):
    workspace, store = timer_workspace
    store.start("alice", seconds=0, label="alice")
    exec_id, _ = _restart_fixture(workspace, client_id="alice")
    store.close()

    bridge = ConnectionBridge(workspace)
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert result["state"] == "succeeded"
    assert "timers" not in result


@pytest.mark.parametrize("kind", ["before_deadline", "acknowledged", "cancelled"])
async def test_restart_poll_omits_non_pending_timers(timer_workspace, kind, monkeypatch):
    workspace, store = timer_workspace
    if kind == "before_deadline":
        store.start("alice", seconds=3600, label=kind)
    elif kind == "acknowledged":
        timer = store.start("alice", seconds=0, label=kind)
        assert store.ack("alice", [timer["id"]]) == 1
    else:
        timer = store.start("alice", seconds=3600, label=kind)
        assert store.cancel("alice", timer["id"])["state"] == "cancelled"
    exec_id, _ = _restart_fixture(workspace)
    store.close()

    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert "timers" not in result


async def test_running_restart_fallback_waits_for_timer_and_stays_running(
    timer_workspace, monkeypatch
):
    workspace, store = timer_workspace
    timer = store.start("alice", seconds=0.1, label="wake")
    exec_id, _ = _restart_fixture(workspace, state="starting")
    store.close()
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    monkeypatch.setattr(bridge, "_recover_restart", _noop)

    result = await asyncio.wait_for(bridge._poll_restart(exec_id, 0, 5000), 3)

    assert result["state"] == "running"
    assert result["timers"]["items"][0]["id"] == timer["id"]


def test_snapshot_timer_expiry_latches_when_clock_moves_back(tmp_path: Path, monkeypatch):
    workspace = tmp_path / "clock"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.close()
    now = [100.0]
    monkeypatch.setattr(timers_module.time, "time", lambda: now[0])
    store = TimerStore(workspace)
    try:
        timer = store.start("alice", seconds=10, label="latched")
        store.close()
        now[0] = 111.0
        preview, _ = TimerStore.snapshot_existing(workspace, "alice")
        assert preview["items"][0]["id"] == timer["id"]
        now[0] = 100.0
        preview, _ = TimerStore.snapshot_existing(workspace, "alice")
        assert preview["items"][0]["id"] == timer["id"]
    finally:
        store.close()


async def test_execute_connection_failure_recovers_matching_origin(
    timer_workspace, monkeypatch
):
    workspace, store = timer_workspace
    timer = store.start("alice", seconds=0, label="execute")
    store.close()
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    bridge._state = _old_bridge_state()
    bridge._ready.set()
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    request_id = "execute-request"
    exec_id, _ = _restart_fixture(
        workspace,
        connection_id=bridge.connection_id,
        request_id=request_id,
        code="42",
    )
    monkeypatch.setattr(bridge, "wait_ready", _noop)

    async def disconnected(*args, **kwargs):
        raise ConnectionError("manager disconnected")

    monkeypatch.setattr(bridge_module, "rpc", disconnected)
    result = await bridge.request(
        "execute", code="42", request_id=request_id, wait_ms=0
    )

    assert result["exec_id"] == exec_id
    assert result["state"] == "succeeded"
    assert result["timers"]["items"][0]["id"] == timer["id"]


async def test_execute_connection_failure_does_not_recover_conflicting_code(
    timer_workspace, monkeypatch
):
    workspace, store = timer_workspace
    store.close()
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    bridge._state = _old_bridge_state()
    bridge._ready.set()
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    request_id = "execute-request"
    _restart_fixture(
        workspace,
        connection_id=bridge.connection_id,
        request_id=request_id,
        code="42",
    )
    monkeypatch.setattr(bridge, "wait_ready", _noop)

    async def disconnected(*args, **kwargs):
        raise ConnectionError("manager disconnected")

    monkeypatch.setattr(bridge_module, "rpc", disconnected)
    with pytest.raises(ConnectionError, match="manager disconnected"):
        await bridge.request(
            "execute", code="different", request_id=request_id, wait_ms=0
        )


async def test_execute_application_error_is_not_treated_as_restart_disconnect(
    timer_workspace, monkeypatch
):
    workspace, store = timer_workspace
    store.close()
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    bridge._state = _old_bridge_state()
    bridge._ready.set()
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    request_id = "execute-request"
    _restart_fixture(
        workspace,
        connection_id=bridge.connection_id,
        request_id=request_id,
        code="42",
    )
    monkeypatch.setattr(bridge, "wait_ready", _noop)

    async def rejected(*args, **kwargs):
        raise RPCError("request_id already used for different code")

    monkeypatch.setattr(bridge_module, "rpc", rejected)
    with pytest.raises(RPCError, match="different code"):
        await bridge.request(
            "execute", code="different", request_id=request_id, wait_ms=0
        )


async def test_poll_connection_failure_recovers_matching_ticket(timer_workspace, monkeypatch):
    workspace, store = timer_workspace
    timer = store.start("alice", seconds=0, label="poll")
    store.close()
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"
    bridge._state = _old_bridge_state()
    bridge._ready.set()
    monkeypatch.setattr(bridge, "_recover_restart", _noop)
    exec_id, _ = _restart_fixture(workspace, connection_id=bridge.connection_id)
    monkeypatch.setattr(bridge, "wait_ready", _noop)

    async def disconnected(*args, **kwargs):
        raise ConnectionError("manager disconnected")

    monkeypatch.setattr(bridge_module, "rpc", disconnected)
    result = await bridge.request("poll", exec_id=exec_id, cursor=0, wait_ms=0)

    assert result["exec_id"] == exec_id
    assert result["state"] == "succeeded"
    assert result["timers"]["items"][0]["id"] == timer["id"]


def test_snapshot_existing_is_compatible_without_timer_schema(tmp_path: Path):
    workspace = tmp_path / "empty"
    workspace.mkdir()

    assert TimerStore.snapshot_existing(workspace, "alice") == (None, None)
    assert not (workspace / ".mypr" / "history.sqlite3").exists()

    history = History(workspace)
    history.reserve_client_id("alice")
    history.close()
    assert TimerStore.snapshot_existing(workspace, "alice") == (None, None)
    connection = sqlite3.connect(workspace / ".mypr" / "history.sqlite3")
    try:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='timers'"
        ).fetchone() is None
    finally:
        connection.close()
