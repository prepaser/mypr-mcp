import asyncio

import pytest

from mypr_mcp.managed_commands import ManagedCommands


class FakeRuntime:
    def __init__(self, shells):
        self.shells = shells
        self.workspace = "/workspace"
        self.tracked = []

    def track_shell(self, *args, **kwargs):
        self.tracked.append((args, kwargs))


class FakeShell:
    def __init__(self):
        self.started = asyncio.Event()
        self.release_start = asyncio.Event()
        self.cancelled = []
        self.read_calls = 0

    async def start(self, command, cwd, env, *, input=None):
        self.started.set()
        await self.release_start.wait()
        return {"id": "job-1"}

    async def cancel(self, ident):
        self.cancelled.append(ident)
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_run_cancellation_after_start_waits_for_id_and_cancels_job():
    shells = FakeShell()
    runtime = FakeRuntime(shells)
    task = asyncio.create_task(ManagedCommands(runtime, "c", "conn", "exec").run(["sleep", "1"]))
    await shells.started.wait()
    task.cancel()
    shells.release_start.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert shells.cancelled == ["job-1"]
    assert len(runtime.tracked) == 1


class OutputShell:
    def __init__(self):
        self.cancelled = False

    async def start(self, command, cwd, env, *, input=None):
        return {"id": "job-2"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        events = [{"stream": "stdout", "text": "x" * 64} for _ in range(10_000)]
        return {
            "id": ident,
            "state": "succeeded",
            "output": events,
            "cursor": "done",
            "has_more": False,
            "result": {"returncode": 0},
            "warnings": [{"code": str(index)} for index in range(10)],
        }

    async def cancel(self, ident):
        self.cancelled = True
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_run_bounds_output_and_warning_memory():
    shells = OutputShell()
    result = await ManagedCommands(FakeRuntime(shells), "c", "conn", "exec").run(
        ["emit"], max_bytes=100
    )
    assert len(result["stdout"].encode()) == 100
    assert result["truncated"] is True
    assert len(result["warnings"]) == 4
    assert result["warnings_truncated"] is True


class TimeoutShell:
    async def start(self, command, cwd, env, *, input=None):
        return {"id": "job-3"}

    async def read(self, ident, *, cursor, max_bytes, wait_ms):
        if wait_ms:
            await asyncio.sleep(1)
        return {
            "id": ident,
            "state": "cancelled",
            "output": [{"stream": "stdout", "text": "done"}],
            "cursor": "end",
            "has_more": False,
            "result": {"returncode": -15},
        }

    async def cancel(self, ident):
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_run_timeout_cancels_and_returns_bounded_terminal_result():
    result = await ManagedCommands(FakeRuntime(TimeoutShell()), "c", "conn", "exec").run(
        ["hang"], timeout=0.01
    )
    assert result["timed_out"] is True
    assert result["state"] == "cancelled"
    assert result["stdout"] == "done"


async def test_quiet_running_pages_do_not_finish_command():
    class QuietShell(OutputShell):
        def __init__(self):
            super().__init__()
            self.read_calls = 0

        async def read(self, ident, *, cursor, max_bytes, wait_ms):
            self.read_calls += 1
            if self.read_calls < 4:
                return {"state": "running", "cursor": "unchanged", "output": [], "has_more": False}
            return {
                "state": "succeeded",
                "cursor": "end",
                "output": [{"text": "done"}],
                "result": {"returncode": 0},
                "has_more": False,
            }

    result = await ManagedCommands(FakeRuntime(QuietShell()), "c", "conn", "exec").run(["quiet"])
    assert result["state"] == "succeeded"
    assert result["stdout"] == "done"
