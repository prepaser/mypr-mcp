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
    assert handle.status()["warnings"] == [warning]
    assert handle.output() == "retained"
    assert handle.output(cursor=0) == {
        "output": "retained",
        "cursor": 8,
        "truncated": True,
        "warnings": [warning],
    }
