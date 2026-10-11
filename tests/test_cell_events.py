import asyncio
from types import SimpleNamespace

import pytest

from mypr_mcp.history import History
from mypr_mcp.runtime import Runtime


async def test_cell_events_ignore_idle_stale_generations_and_late_completion(workspace):
    runtime = Runtime(workspace)
    runtime.history = History(workspace)
    (runtime.root / "runs").mkdir()
    rec = dict(
        id="a" * 32,
        generation=runtime.generation,
        client_id="a",
        connection_id="connection-a",
        msg_id="message-a",
        state="queued",
        events=[],
        bytes=0,
        truncated=False,
        done=asyncio.Event(),
        idle=asyncio.Event(),
    )
    runtime.execs[rec["id"]] = rec
    runtime.by_msg[rec["msg_id"]] = rec
    messages = asyncio.Queue()
    drained = asyncio.Event()

    async def receive():
        if messages.empty():
            drained.set()
        return await messages.get()

    runtime.kc = SimpleNamespace(get_iopub_msg=receive)

    def send(kind, **content):
        messages.put_nowait(
            {"parent_header": {"msg_id": rec["msg_id"]}, "msg_type": kind, "content": content}
        )

    def event(state, generation=None):
        send(
            "mypr_cell",
            exec_id=rec["id"],
            generation=generation or runtime.generation,
            state=state,
        )

    event("running")
    send("status", execution_state="idle")
    event("succeeded", generation="old-generation")
    reader = asyncio.create_task(runtime.read_output())
    try:
        await asyncio.wait_for(drained.wait(), 5)
        assert rec["state"] == "running"
        assert list(runtime.active) == [rec["id"]]
        assert not rec["idle"].is_set()
        drained.clear()
        send("stream", name="stdout", text="last output")
        event("succeeded")
        event("failed")
        send("stream", name="stdout", text="late output")
        await asyncio.wait_for(drained.wait(), 5)
        assert rec["state"] == "succeeded"
        assert rec["done"].is_set()
        assert runtime.active == {}
        assert "".join(item["text"] for item in rec["events"]) == "last output"
        assert rec["truncated"] is False
        assert not rec.get("warnings")
        records = runtime.history.logs(cursor=0)["events"]
        assert records[-1]["event"] == "succeeded"
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        runtime.history.close()


@pytest.mark.parametrize("state", ["succeeded", "failed", "cancelled"])
async def test_terminal_fallback_persists_incomplete_output_warning(tmp_path, state):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    runtime.healthy = True
    (runtime.root / "runs").mkdir()
    try:
        rec = await runtime.admit_execution("client", None, {"code": "print('output')"})
        rec["warnings"] = [{"code": f"prior_{n}", "text": "prior warning"} for n in range(4)]
        await runtime._dispatch({
            "op": "cell_terminal", "exec_id": rec["id"],
            "generation": runtime.generation, "state": state,
        })
        warm = await runtime.poll(rec["id"], wait_ms=0)
        runtime.execs.pop(rec["id"], None)
        cold = await runtime.poll(rec["id"], wait_ms=0)
        for result in (warm, cold):
            assert result["state"] == state
            assert result["truncated"] is True
            assert result["warnings"][0]["code"] == "output_incomplete"
            assert len(result["warnings"]) == 4
            assert result["warnings_truncated"] is True
        assert runtime.history.get(rec["id"])["warnings"][0]["code"] == "output_incomplete"
    finally:
        await runtime.shells.close()
        runtime.history.close()
