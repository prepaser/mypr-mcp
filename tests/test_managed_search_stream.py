import asyncio
import sys
import time
from pathlib import Path

import pytest

from mypr_mcp.managed_commands import ManagedCommands
from mypr_mcp.services import Shells


class Runtime:
    workspace = "/workspace"

    def __init__(self, shells):
        self.shells = shells
        self.tracked = []

    def track_shell(self, *args, **kwargs):
        self.tracked.append((args, kwargs))


class InfiniteOutput:
    def __init__(self):
        self.cancelled = []

    async def start(self, command, cwd, env, *, input=None):
        return {"id": "stream-job"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        if self.cancelled:
            return {
                "id": ident,
                "state": "cancelled",
                "cursor": "done",
                "has_more": False,
                "result": {"returncode": -15},
            }
        return {
            "id": ident,
            "state": "running",
            "cursor": "next",
            "has_more": True,
            "output": [{"stream": "stdout", "text": "0123456789"}],
        }

    async def cancel(self, ident):
        self.cancelled.append(ident)
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_stream_stops_at_scan_byte_limit_without_accumulating_stdout():
    shells = InfiniteOutput()
    seen = []

    async def consume(text):
        seen.append(text)

    result = await ManagedCommands(Runtime(shells), "c", "conn", "exec").stream(
        ["emit"], max_bytes=4, on_stdout=consume
    )

    assert result["stdout"] == ""
    assert result["stop_reason"] == "scan_bytes"
    assert result["truncated"] is True
    assert shells.cancelled == ["stream-job"]
    assert seen == ["0123"]


class SplitOutput:
    def __init__(self):
        self.reads = 0

    async def start(self, command, cwd, env, *, input=None):
        return {"id": "split-job"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        self.reads += 1
        if self.reads == 1:
            return {
                "id": ident,
                "state": "running",
                "cursor": "one",
                "has_more": True,
                "output": [{"stream": "stdout", "text": "ab"}],
            }
        return {
            "id": ident,
            "state": "succeeded",
            "cursor": "done",
            "has_more": False,
            "output": [
                {"stream": "stdout", "text": "cd"},
                {"stream": "stderr", "text": "warning"},
            ],
            "result": {"returncode": 0},
        }

    async def cancel(self, ident):
        raise AssertionError("completed stream must not be cancelled")


@pytest.mark.asyncio
async def test_stream_forwards_each_stdout_chunk_and_keeps_stderr():
    chunks = []

    async def consume(text):
        chunks.append(text)

    result = await ManagedCommands(Runtime(SplitOutput()), "c", "conn", "exec").stream(
        ["emit"], on_stdout=consume
    )

    assert chunks == ["ab", "cd"]
    assert result["stdout"] == ""
    assert result["stderr"] == "warning"
    assert result["state"] == "succeeded"


class CallbackStop:
    def __init__(self):
        self.cancelled = []
        self.calls = 0

    async def start(self, command, cwd, env, *, input=None):
        return {"id": "matched-job"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        self.calls += 1
        if self.cancelled:
            return {
                "id": ident,
                "state": "cancelled",
                "cursor": "done",
                "has_more": False,
                "result": {"returncode": -15},
            }
        return {
            "id": ident,
            "state": "running",
            "cursor": "one",
            "has_more": True,
            "output": [{"stream": "stdout", "text": "match"}],
        }

    async def cancel(self, ident):
        self.cancelled.append(ident)
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_stream_callback_reason_cancels_job_and_is_preserved():
    shells = CallbackStop()

    async def consume(text):
        assert text == "match"
        return "matched"

    result = await ManagedCommands(Runtime(shells), "c", "conn", "exec").stream(
        ["emit"], on_stdout=consume
    )

    assert result["stop_reason"] == "matched"
    assert result["state"] == "cancelled"
    assert shells.cancelled == ["matched-job"]
    assert shells.calls == 2


class SlowOutput:
    def __init__(self):
        self.cancelled = False
        self.drain = False

    async def start(self, command, cwd, env, *, input=None):
        return {"id": "slow-job"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        if not self.drain:
            await asyncio.sleep(1)
        return {
            "id": ident,
            "state": "cancelled" if self.cancelled else "succeeded",
            "cursor": "done",
            "has_more": False,
            "output": [{"stream": "stdout", "text": "late"}] if self.drain else [],
            "result": {"returncode": -15 if self.cancelled else 0},
        }

    async def cancel(self, ident):
        self.cancelled = True
        self.drain = True
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_stream_timeout_cancels_job_and_returns_reason():
    shells = SlowOutput()
    seen = []

    async def consume(text):
        seen.append(text)

    result = await ManagedCommands(Runtime(shells), "c", "conn", "exec").stream(
        [sys.executable, "-c", "pass"], timeout=0.01, on_stdout=consume
    )

    assert result["timed_out"] is True
    assert result["stop_reason"] == "timeout"
    assert result["state"] == "cancelled"
    assert shells.cancelled is True
    assert seen == ["late"]


class DelayedStart:
    def __init__(self):
        self.started = asyncio.Event()
        self.cancelled = []

    async def start(self, command, cwd, env, *, input=None):
        self.started.set()
        await asyncio.sleep(1)
        return {"id": "late-job"}

    async def cancel(self, ident):
        self.cancelled.append(ident)
        return {"id": ident, "state": "cancelled"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        return {
            "id": ident,
            "state": "cancelled",
            "cursor": "done",
            "has_more": False,
            "result": {"returncode": -15},
        }


@pytest.mark.asyncio
async def test_stream_caller_cancellation_during_launch_cleans_up_job():
    shells = DelayedStart()
    task = asyncio.create_task(
        ManagedCommands(Runtime(shells), "c", "conn", "exec").stream(
            ["sleep"], timeout=10, on_stdout=lambda _: None
        )
    )
    await shells.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert shells.cancelled == ["late-job"]


@pytest.mark.asyncio
async def test_stream_timeout_budget_includes_launch():
    shells = DelayedStart()
    started = time.monotonic()
    result = await ManagedCommands(Runtime(shells), "c", "conn", "exec").stream(
        ["sleep"], timeout=0.01, on_stdout=lambda _: None
    )

    assert time.monotonic() - started >= 1
    assert result["timed_out"] is True
    assert result["stop_reason"] == "timeout"
    assert shells.cancelled == ["late-job"]


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform != "linux", reason="process_guard requires Linux")
async def test_stream_timeout_kills_detached_descendant(tmp_path: Path):
    shells = Shells(tmp_path)
    runtime = Runtime(shells)
    runtime.workspace = str(tmp_path)
    child_pid: list[int] = []
    command_code = (
        "import os,signal,time\n"
        "child=os.fork()\n"
        "if child == 0:\n"
        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "    time.sleep(60)\n"
        "    os._exit(0)\n"
        "print(child, flush=True)\n"
        "os._exit(0)\n"
    )

    async def consume(text):
        child_pid.append(int(text.strip()))

    try:
        result = await ManagedCommands(runtime, "c", "conn", "exec").stream(
            [sys.executable, "-c", command_code], timeout=2, on_stdout=consume
        )
    finally:
        await shells.close()

    assert result["stop_reason"] == "timeout"
    assert child_pid
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        proc = Path(f"/proc/{child_pid[0]}")
        if not await asyncio.to_thread(proc.exists):
            break
        try:
            stat = await asyncio.to_thread((proc / "stat").read_text, encoding="ascii")
            state = stat.split()[2]
        except FileNotFoundError, IndexError:
            break
        if state in {"Z", "X"}:
            break
        await asyncio.sleep(0.05)
    assert not await asyncio.to_thread(Path(f"/proc/{child_pid[0]}").exists)
