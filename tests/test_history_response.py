import asyncio
import json

from mypr_mcp.history import History
from mypr_mcp.kernel_api import History as KernelHistory
from mypr_mcp.messages import MessageStore
from mypr_mcp.runtime import Runtime
from mypr_mcp.transport import MAX_MESSAGE, attachment, rpc


async def test_history_get_reads_an_accepted_near_limit_record(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    runtime.messages = MessageStore(tmp_path)
    runtime.healthy = True
    (runtime.root / "runs").mkdir()
    sock = tmp_path / "history.sock"
    monkeypatch.setenv("MYPR_SOCKET", str(sock))
    monkeypatch.setenv("MYPR_GENERATION", runtime.generation)
    server = await asyncio.start_unix_server(runtime.connection, path=str(sock), limit=MAX_MESSAGE)
    try:
        async with attachment(sock, "history-reader"):
            await rpc(sock, op="init", connection_id="history-reader", client_id="history-test")
            request = {
                "op": "execute",
                "connection_id": "history-reader",
                "code": "pass\n#",
                "wait_ms": 0,
            }
            request["code"] += "x" * (MAX_MESSAGE - 1 - len(json.dumps(request).encode()))
            accepted = await rpc(sock, **request)
            rec = runtime.execs[accepted["exec_id"]]
            await runtime.finish(rec, "succeeded")
            runtime.execs.pop(rec["id"], None)
            for result in (
                await rpc(sock, op="history_get", id=rec["id"]),
                await KernelHistory().get(rec["id"]),
            ):
                assert result["state"] == "succeeded"
                assert result["code"] == request["code"]
    finally:
        server.close()
        server.close_clients()
        await server.wait_closed()
        await runtime.shells.close()
        runtime.messages.close()
        runtime.history.close()
