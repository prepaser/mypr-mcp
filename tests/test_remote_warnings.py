import pytest

import mypr_mcp.kernel_api as api


async def test_shell_warnings_reach_handle_status_and_output(monkeypatch):
    warning = {"code": "output_write_error", "text": "Output could not be saved"}

    async def rpc(op, **kwargs):
        assert op == "shell_poll"
        return {
            "state": "succeeded",
            "output": [{"text": "retained"}],
            "cursor": 1,
            "result": {"returncode": 0},
            "warnings": [warning],
            "truncated": True,
        }

    monkeypatch.setattr(api, "_rpc", rpc)
    handle = api.RemoteTask("a" * 32, api.TaskManager())
    assert await handle == {"returncode": 0}
    assert handle.status()["status"] == "succeeded"
    assert handle.status()["returncode"] == 0
    assert handle.status()["warnings"] == [warning]
    assert handle.output() == "retained"
    assert handle.output(cursor=0) == {
        "output": "retained",
        "cursor": 8,
        "truncated": True,
        "warnings": [warning],
    }


@pytest.mark.parametrize(
    "attached,kind,state,returncode,error",
    [
        (False, "shell", "failed", 7, None),
        (True, "shell", "failed", 7, None),
        (True, "package", "failed", 7, None),
        (False, "shell", "failed", None, None),
        (False, "shell", "lost", None, "connection lost"),
        (False, "shell", "failed", -1, "spawn failed"),
    ],
)
async def test_remote_failure_exposes_known_exit_code(
    monkeypatch, attached, kind, state, returncode, error
):
    ident = "b" * 32

    async def rpc(op, **fields):
        if op == "history_get":
            return {"id": ident, "kind": kind, "generation": "old"}
        assert op == "shell_read"
        return {
            "state": state,
            "error": error,
            "output": [],
            "cursor": "end",
            "has_more": False,
            "result": {"returncode": returncode} if returncode is not None else None,
        }

    monkeypatch.setattr(api, "_rpc", rpc)
    manager = api.TaskManager()
    handle = await manager.attach(ident) if attached else api.RemoteTask(ident, manager)
    with pytest.raises(api.RPCError) as caught:
        await handle

    expected = {"id": ident, "state": state}
    status = handle.status()
    assert status["status"] == state
    if returncode is None:
        assert "returncode" not in status
    else:
        expected["returncode"] = returncode
        assert status["returncode"] == returncode
    assert caught.value.code == "rpc_error"
    assert caught.value.details == expected
    if error is not None:
        assert str(caught.value) == error
    elif returncode is not None:
        assert str(caught.value) == f"task {ident} failed (exit {returncode})"
