import os
from pathlib import Path

import pytest

import mypr_mcp.restart as restart
from mypr_mcp.protocol import target_installation
from mypr_mcp.transport import workspace_id


def _target() -> dict:
    return target_installation()


def _state(workspace: Path, *, generation: str = "old") -> dict:
    return {
        "healthy": True,
        "version": target_installation()["version"],
        "protocol_version": 1,
        "capabilities": ["restart"],
        "workspace_id": workspace_id(workspace),
        "generation": generation,
        "pid": os.getpid(),
    }


class _Process:
    pid = os.getpid()


@pytest.mark.asyncio
async def test_target_descriptor_reads_the_candidate_installation():
    descriptor = await restart.target_descriptor(_target())
    assert descriptor["version"] == _target()["version"]
    assert descriptor["protocol_version"] == 1


@pytest.mark.asyncio
async def test_request_writes_latest_and_archived_ticket_and_rejects_duplicate(
    tmp_path: Path, monkeypatch
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    async def descriptor(_target):
        return _descriptor()

    monkeypatch.setattr(restart, "target_descriptor", descriptor)

    async def found(_workspace):
        return Path("manager.sock"), _state(workspace)

    monkeypatch.setattr(restart, "find_runtime", found)
    monkeypatch.setattr(restart.subprocess, "Popen", lambda *args, **kwargs: _Process())

    ticket = await restart.request_restart(
        workspace,
        _target(),
        origin={"exec_id": "exec", "client_id": "client"},
    )
    assert ticket["state"] == "preparing"
    assert restart.read_ticket(workspace)["id"] == ticket["id"]
    assert restart.read_ticket(workspace, ticket["id"])["target"]["version"] == _target()["version"]
    with pytest.raises(restart.RestartInProgress) as error:
        await restart.request_restart(workspace, _target())
    assert error.value.ticket["id"] == ticket["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "stop", "start"])
async def test_coordinator_replaces_modern_manager_and_records_generation(
    tmp_path: Path, monkeypatch, failure
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    target = _target()
    ticket = {
        "id": "a" * 32,
        "state": "preparing",
        "workspace_id": workspace_id(workspace),
        "created_at": 1.0,
        "updated_at": 1.0,
        "coordinator_pid": None,
        "coordinator_starttime": None,
        "old_pid": os.getpid(),
        "old_generation": "old",
        "target": {
            "python": target["python"],
            "package_root": str(Path(target["package_root"]).resolve()),  # noqa: ASYNC240
            "version": target["version"],
            "protocol": {"protocol_version": 1},
        },
        "origin": None,
        "force": False,
        "error": None,
        "new_generation": None,
        "new_version": None,
    }
    restart._write_ticket(workspace, ticket)
    calls = []

    async def found(_workspace):
        if calls and calls[-1] == "stopped":
            return Path("new.sock"), {**_state(workspace, generation="new"), "version": "0.9.0"}
        return Path("old.sock"), _state(workspace)

    async def prepare(path, **fields):
        calls.append(fields["op"] if "op" in fields else "prepared")
        return {}

    async def fake_rpc(path, **fields):
        if fields.get("op") == "restart_prepare":
            calls.append("prepared")
            return {}
        calls.append("status")
        return {
            "healthy": True,
            "workspace_id": workspace_id(workspace),
            "generation": "new",
            "version": target["version"],
        }

    async def fake_stop(*args, **kwargs):
        calls.append("stopped")
        if failure == "stop":
            raise RuntimeError("stop failed")
        return {"stopped": True}

    async def fake_ensure(_workspace, *, locked=False):
        assert locked
        if failure == "start":
            raise RuntimeError("start failed")
        assert os.environ["PYTHONPATH"].split(os.pathsep)[0] == target["package_root"]
        calls.append("ensured")
        return Path("new.sock")

    monkeypatch.setattr(restart, "find_runtime", found)
    monkeypatch.setattr(restart, "rpc", fake_rpc)
    monkeypatch.setattr("mypr_mcp.cli.stop_runtime", fake_stop)
    monkeypatch.setattr("mypr_mcp.cli.ensure", fake_ensure)
    await restart._coordinate(workspace, ticket["id"])
    result = restart.read_ticket(workspace, ticket["id"])
    if failure:
        assert result["state"] == "failed"
        assert result["error"] == f"{failure} failed"
    else:
        assert result["state"] == "succeeded"
        assert result["new_generation"] == "new"
        assert calls[:3] == ["prepared", "stopped", "ensured"]


def _descriptor():
    return {"version": target_installation()["version"], "protocol_version": 1, "capabilities": []}
