import asyncio
import json
import sys
from types import SimpleNamespace

from conftest import execute, mcp_session, result_text

import mypr_mcp.cli as cli
from mypr_mcp.cli import ensure, stop_runtime, tool_result
from mypr_mcp.history import History
from mypr_mcp.runtime import Runtime
from mypr_mcp.transport import rpc, socket_path


async def test_invalid_display_does_not_break_next_cell(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "from IPython.display import display\ndisplay({'image/png': 'a'}, raw=True)\n42",
        )
        assert result["state"] == "succeeded"
        assert result["warnings"][0]["code"] == "artifact_error"
        assert result_text(result).strip() == "42"
        assert result_text(await execute(session, "6 * 7")).strip() == "42"
        assert (await rpc(socket_path(workspace), op="status"))["healthy"]


def test_missing_image_preserves_result_and_inbox(tmp_path):
    result = tool_result(
        {
            "state": "succeeded",
            "cursor": 1,
            "inbox": {"unacked": 1},
            "output": [
                {
                    "text": "saved",
                    "artifacts": [{"mime": "image/png", "path": str(tmp_path / "missing")}],
                }
            ],
        }
    )
    assert not result.is_error
    assert result.structured_content["cursor"] == 1
    assert result.structured_content["inbox"]["unacked"] == 1
    assert result.structured_content["warnings"][0]["code"] == "artifact_unavailable"


async def test_small_cache_preserves_poll_and_request_identity(workspace):
    root = workspace / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text(
        "[limits]\ncompleted_tasks=2\ncompleted_records=1\ncache_bytes=4096\n"
    )
    async with mcp_session(workspace) as session:
        first = await execute(
            session, "counter = globals().get('counter', 0) + 1\ncounter", request_id=""
        )
        await execute(session, f"held = ws.tasks.get({first['exec_id']!r})")
        for _ in range(5):
            await execute(session, "42")
        missing = await execute(session, f"ws.tasks.get({first['exec_id']!r})")
        assert missing["state"] == "failed"
        assert result_text(await execute(session, "held.result()")).strip() == "1"
        assert (await rpc(socket_path(workspace), op="poll", exec_id=first["exec_id"]))[
            "state"
        ] == "succeeded"
        replay = await execute(
            session, "counter = globals().get('counter', 0) + 1\ncounter", request_id=""
        )
        assert replay["exec_id"] == first["exec_id"]
        assert result_text(await execute(session, "counter")).strip() == "1"


async def test_error_summary_is_bounded(workspace):
    root = workspace / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text("[limits]\noutput_bytes=1024\nresponse_bytes=1024\n")
    async with mcp_session(workspace) as session:
        result = await execute(session, "raise ValueError('x' * 100000)")
        assert result["state"] == "failed"
        assert len(result["error"].encode()) <= 256
        assert result["error_truncated"]


async def test_stop_waits_for_exit_before_reconnect(workspace):
    path = await ensure(workspace)
    before = await rpc(path, op="status")
    assert await stop_runtime(path) == {"stopped": True}
    after = await rpc(await ensure(workspace), op="status")
    assert before["pid"] != after["pid"]
    assert before["generation"] != after["generation"]


async def test_stop_legacy_manager_uses_matching_metadata(tmp_path, monkeypatch):
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(30)"
    )
    root = tmp_path / ".mypr"
    root.mkdir()
    path = tmp_path / "manager.sock"
    (root / "runtime.json").write_text(
        json.dumps(
            {
                "pid": process.pid,
                "generation": "legacy",
                "socket": str(path),
            }
        )
    )

    async def request(_path, **fields):
        if fields["op"] == "status":
            return {"generation": "legacy"}
        assert fields["manager_pid"] == process.pid
        process.terminate()
        return {"stopped": True}

    monkeypatch.setattr(cli, "rpc", request)
    try:
        assert await stop_runtime(path, workspace=tmp_path) == {"stopped": True}
        await process.wait()
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()


async def test_base_exception_job_reports_failure_and_does_not_block_stop(workspace):
    path = socket_path(workspace)
    async with mcp_session(workspace) as session:
        started = await execute(
            session,
            "import asyncio\n"
            "class Fatal(BaseException): pass\n"
            "async def crash(): raise Fatal('done')\n"
            "ws.local['job'] = ws.tasks.start(crash())\nws.local['job'].id",
        )
        ident = result_text(started).strip(" '\n")
        async with asyncio.timeout(5):
            while True:
                records = await rpc(path, op="history_list")
                if any(
                    item["id"] == ident and item["state"] == "failed" for item in records["items"]
                ):
                    break
                await asyncio.sleep(0.01)
    assert await stop_runtime(path) == {"stopped": True}


async def test_failed_worker_marks_runtime_and_pending_cell_lost(workspace):
    runtime = Runtime(workspace)
    runtime.history = History(workspace)
    (runtime.root / "runs").mkdir()
    rec = dict(
        id="a" * 32,
        state="running",
        client_id="owner",
        events=[],
        bytes=0,
        truncated=False,
        done=asyncio.Event(),
        idle=asyncio.Event(),
    )
    runtime.execs[rec["id"]] = rec
    runtime.healthy = True

    async def broken():
        raise ValueError("reader failed")

    task = asyncio.create_task(broken(), name="mypr:iopub")
    task.add_done_callback(runtime.critical_done)
    try:
        await asyncio.gather(task, return_exceptions=True)
        assert not runtime.healthy
        assert "reader failed" in runtime.health_error
        assert rec["state"] == "lost"
        assert rec["done"].is_set()
    finally:
        runtime.history.close()


async def test_large_output_yields_before_terminal_status(workspace):
    runtime = Runtime(workspace)
    runtime.history = History(workspace)
    (runtime.root / "runs").mkdir()
    rec = dict(
        id="b" * 32,
        generation=runtime.generation,
        state="running",
        client_id="owner",
        events=[],
        bytes=0,
        truncated=False,
        done=asyncio.Event(),
        idle=asyncio.Event(),
    )
    runtime.execs[rec["id"]] = rec
    runtime.by_msg["source"] = rec
    messages = asyncio.Queue()
    text = "x" * (1024 * 1024)
    for kind, content in (
        ("stream", {"name": "stdout", "text": text}),
        (
            "mypr_cell",
            {"exec_id": rec["id"], "generation": runtime.generation, "state": "succeeded"},
        ),
    ):
        messages.put_nowait(
            {"msg_type": kind, "content": content, "parent_header": {"msg_id": "source"}}
        )
    runtime.kc = SimpleNamespace(get_iopub_msg=messages.get)
    reader = asyncio.create_task(runtime.read_output())
    try:
        await asyncio.sleep(0)
        await runtime.dispatch({"op": "status"})
        assert 0 < rec["bytes"] < len(text)
        await asyncio.wait_for(rec["done"].wait(), 5)
        assert rec["bytes"] == len(text)
        assert "".join(item["text"] for item in rec["events"]) == text
        events = runtime.history.logs(limit=1)["events"]
        assert events[0]["event"] == "succeeded"
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        runtime.history.close()
