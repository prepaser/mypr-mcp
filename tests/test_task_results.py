import asyncio
import math
from contextlib import asynccontextmanager

import pytest

import mypr_mcp.kernel_api as api
from mypr_mcp.persistence import PersistenceWorker
from mypr_mcp.runtime import Runtime, _close_stores, _open_stores
from mypr_mcp.services import MCPBridge
from mypr_mcp.task_results import encode_result, load_result, store_result


@asynccontextmanager
async def manager(workspace):
    runtime = Runtime(workspace)
    runtime.persistence = PersistenceWorker()
    runtime.history, runtime.messages = await runtime.io(_open_stores, workspace)
    runtime.mcp = MCPBridge(workspace)
    try:
        yield runtime
    finally:
        await runtime.mcp.close()
        await runtime.io(_close_stores, runtime.history, runtime.messages)
        await runtime.persistence.close()


def test_json_result_limits_and_integrity(tmp_path):
    value = {"unicode": "한국어", "value": [None, True, 42]}
    reference = store_result(tmp_path, "task", "generation", encode_result(value))
    assert load_result(tmp_path, reference) == value
    for invalid in (b"bytes", {1: "value"}, math.nan, "x" * (256 * 1024)):
        with pytest.raises((TypeError, ValueError)):
            encode_result(invalid)
    (tmp_path / reference["path"]).write_text('{"changed":true}')
    with pytest.raises(ValueError, match="size|hash"):
        load_result(tmp_path, reference)


async def test_fast_task_result_survives_terminal_event_and_attach(tmp_path, monkeypatch):
    async with manager(tmp_path) as runtime:
        monkeypatch.setenv("MYPR_GENERATION", runtime.generation)

        async def rpc(op, **fields):
            return await runtime.dispatch({
                "op": op, "client_id": "reader", "generation": runtime.generation, **fields
            })

        monkeypatch.setattr(api, "_rpc", rpc)
        tasks = api.TaskManager()

        async def fast():
            return {"answer": 42}

        with api.execution_context({"client_id": "reader", "exec_id": "cell"}):
            handle = tasks.start(fast(), task_id="fast", persist_result=True)
        assert await handle == {"answer": 42}
        await asyncio.gather(*tasks._reporters)
        assert runtime.task_records["fast"]["state"] == "succeeded"
        history_id = f"python:{runtime.generation}:fast"
        record = await runtime.io(runtime.history.get, history_id)
        assert record["result_persisted"]
        assert record["state"] == "succeeded"
        attached = await api.TaskManager().attach(history_id)
        assert attached.result() == {"answer": 42}
        assert attached.status()["result_persisted"]


async def test_saved_lsp_config_cas_and_managed_mcp_write(tmp_path):
    async with manager(tmp_path) as runtime:
        await runtime.mcp.configure("sample", {"command": "example"})
        definitions = {"python": {"command": ["server"], "languages": ["python"]}}
        saved = await runtime.code_config({
            "method": "set_lsp", "definitions": definitions, "expected_servers": {}
        })
        snapshot = await runtime.code_config({"method": "get_lsp"})
        assert snapshot["revision"] == saved["revision"]
        assert snapshot["servers"]["python"]["timeout"] == 10.0
        await runtime.mcp.configure("second", {"command": "example"})
        await runtime.code_config({
            "method": "set_lsp", "definitions": {},
            "expected_revision": saved["revision"], "expected_servers": snapshot["servers"],
        })
        assert set(runtime.mcp.config) == {"sample", "second"}
        runtime.mcp.store.path.write_text("[lsp.servers]\n")
        with pytest.raises(RuntimeError, match="changed on disk"):
            await runtime.code_config({"method": "set_lsp", "definitions": {},
                                       "expected_servers": {}})


async def test_offline_client_directory_keeps_identity_and_unacked_count(tmp_path):
    async with manager(tmp_path) as runtime:
        await runtime.io(runtime.history.reserve_client_id, "reader")
        await runtime.io(runtime.history.reserve_client_id, "writer")
        await runtime.io(runtime.history.touch_client, "reader", 123)
        await runtime.io(runtime.messages.send, "writer", "reader", "hello")
        runtime.clients["attached"] = {"client_id": "writer"}
        result = await runtime.dispatch({"op": "message_clients", "connected": False})
        assert result["clients"][0]["id"] == "reader"
        assert result["clients"][0]["last_seen"] == 123
        assert result["clients"][0]["unacked"] == 1
        assert result["clients"][0]["connected"] is False
