import asyncio
import threading
import time

import pytest

from mypr_mcp.history import History
from mypr_mcp.persistence import PersistenceWorker
from mypr_mcp.runtime import Runtime


@pytest.fixture
async def shell_history(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    runtime.persistence = PersistenceWorker()
    record = {
        "id": "a" * 32, "kind": "shell", "state": "running",
        "generation": runtime.generation, "client_id": "client",
    }
    runtime.task_records[record["id"]] = record

    async def poll(ident, cursor=0):
        assert ident == record["id"]
        return {
            "state": "succeeded", "output": [], "cursor": 0,
            "result": {"returncode": 0},
        }

    monkeypatch.setattr(runtime.shells, "poll", poll)
    await runtime.io(runtime.history.record, "shell", record, event="running")
    try:
        yield runtime, record
    finally:
        runtime.stopping.set()
        await runtime.shells.close()
        await runtime.persistence.close()
        runtime.history.close()


async def test_concurrent_shell_history_reads_and_watcher_publish_one_terminal_event(shell_history):
    runtime, record = shell_history
    results = await asyncio.gather(
        *(
            runtime._dispatch_history("history_get", {"id": record["id"]})
            for _ in range(8)
        ),
        runtime.watch_shell(runtime.shells, record),
    )
    assert all(result["state"] == "succeeded" for result in results[:-1])
    assert [event["event"] for event in runtime.history.logs()["events"]] == [
        "running", "succeeded",
    ]


async def test_cancelled_shell_refresh_finishes_the_shared_transition(shell_history, monkeypatch):
    runtime, record = shell_history
    loop = asyncio.get_running_loop()
    committed = asyncio.Event()
    release = threading.Event()
    original = runtime.history.record

    def write(kind, candidate, **kwargs):
        result = original(kind, candidate, **kwargs)
        loop.call_soon_threadsafe(committed.set)
        assert release.wait(5)
        return result

    monkeypatch.setattr(runtime.history, "record", write)
    refresh = asyncio.create_task(runtime.refresh_shell(runtime.shells, record))
    try:
        await asyncio.wait_for(committed.wait(), 5)
        refresh.cancel()
        await asyncio.sleep(0)
        assert not refresh.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await refresh
        assert record["state"] == "succeeded"
        await runtime.refresh_shell(runtime.shells, record)
        assert [event["event"] for event in runtime.history.logs()["events"]] == [
            "running", "succeeded",
        ]
    finally:
        release.set()
        await asyncio.gather(refresh, return_exceptions=True)


async def test_terminal_shell_history_read_preserves_retention_age(shell_history):
    runtime, record = shell_history
    await runtime.refresh_shell(runtime.shells, record)
    old = time.time() - 90 * 86400
    record["finished"] = old
    await runtime.io(runtime.history.record, "shell", record, updated_at=old)
    before = await runtime.io(runtime.history.storage_history_snapshot, retention_days=30)

    await runtime._dispatch_history("history_get", {"id": record["id"]})

    after = await runtime.io(runtime.history.storage_history_snapshot, retention_days=30)
    assert [item["id"] for item in before["entities"]] == [record["id"]]
    assert [item["id"] for item in after["entities"]] == [record["id"]]
    updated = await runtime.io(
        lambda: runtime.history._db.execute(
            "SELECT updated FROM entities WHERE id=?", (record["id"],)
        ).fetchone()[0]
    )
    assert updated == old
    assert record["finished"] == old
