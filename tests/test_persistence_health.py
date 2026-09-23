import asyncio
import contextlib

import pytest

import mypr_mcp.runtime as runtime_module
from mypr_mcp.runtime import Runtime


async def test_storage_worker_failure_remains_fatal_during_reset(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    python = runtime.root / "venv/bin/python"
    python.parent.mkdir(parents=True)
    python.touch()

    async def dependencies_already_present(*args):
        pass

    monkeypatch.setattr(runtime_module, "ensure_runtime", dependencies_already_present)
    await runtime.prepare()
    runtime.healthy = True
    runtime.resetting = True
    try:
        runtime.persistence._task.cancel()
        await asyncio.gather(runtime.persistence._task, return_exceptions=True)
        assert not runtime.healthy
        assert "persistence" in runtime.health_error.lower()

        def no_kernel_for_dead_storage(**kwargs):
            pytest.fail("kernel startup must reject a dead persistence worker")

        monkeypatch.setattr(runtime_module, "AsyncKernelManager", no_kernel_for_dead_storage)
        with pytest.raises(RuntimeError, match="[Pp]ersistence"):
            await runtime.start_kernel()
        with pytest.raises(RuntimeError, match="[Pp]ersistence"):
            await runtime.io(runtime.history.find_request, "client", "request")
    finally:
        runtime.stopping.set()
        await runtime.drain_background()
        with contextlib.suppress(asyncio.CancelledError):
            await runtime.persistence.close()
        await asyncio.to_thread(runtime.messages.close)
        await asyncio.to_thread(runtime.history.close)
        await runtime.close_shells()
        await runtime.mcp.close()


async def test_failure_marking_uses_replaced_task_record(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.task_records = {"first": {"id": "first", "kind": "python", "state": "running"}}
    gate = ObservedLock()
    await gate.acquire()
    runtime._task_locks["first"] = gate
    marking = asyncio.create_task(runtime.mark_persistence_failure("storage failed"))
    try:
        await asyncio.wait_for(gate.waiting.wait(), timeout=1)
        replacement = {"id": "first", "kind": "python", "state": "running", "output": "new"}
        runtime.task_records["first"] = replacement
        gate.release()
        await marking
        assert replacement["state"] == "lost"
        assert runtime.task_records["first"] is replacement
    finally:
        if gate.locked():
            gate.release()
        marking.cancel()
        await asyncio.gather(marking, return_exceptions=True)


async def test_lose_python_tasks_persists_replacement_record(tmp_path):
    runtime = Runtime(tmp_path)

    class History:
        records = []

        def record(self, kind, record, *, event=None, entity_id=None):
            self.records.append((kind, dict(record), event, entity_id))

    history = History()
    runtime.history = history
    record = {"id": "task", "kind": "python", "state": "running"}
    runtime.task_records[record["id"]] = record
    gate = ObservedLock()
    await gate.acquire()
    runtime._task_locks[record["id"]] = gate
    marking = asyncio.create_task(runtime.lose_python_tasks("Workspace reset", state="cancelled"))
    try:
        await asyncio.wait_for(gate.waiting.wait(), timeout=1)
        replacement = {**record, "output": "latest"}
        runtime.task_records[record["id"]] = replacement
        gate.release()
        await marking
        assert replacement["state"] == "cancelled"
        assert replacement["output"] == "latest"
        assert history.records[-1][1] == replacement
        assert history.records[-1][2] == "cancelled"
    finally:
        if gate.locked():
            gate.release()
        marking.cancel()
        await asyncio.gather(marking, return_exceptions=True)


class ObservedLock(asyncio.Lock):
    def __init__(self):
        super().__init__()
        self.waiting = asyncio.Event()

    async def acquire(self):
        if self.locked():
            self.waiting.set()
        return await super().acquire()
