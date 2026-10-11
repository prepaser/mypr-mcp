from types import SimpleNamespace

import pytest

from mypr_mcp.json_utils import json_bytes
from mypr_mcp.runtime import Runtime


@pytest.mark.parametrize("text,count", [("line", 130), ("x" * 20000, 30), ("x" * 270000, 1)])
async def test_shell_history_batches_preserve_order_and_limits(tmp_path, text, count):
    runtime = Runtime(tmp_path)
    saved = []
    batches = []
    events = [{"stream": "stdout", "text": f"{i}:{text}"} for i in range(count)]
    record = {
        "id": "job",
        "kind": "shell",
        "client_id": "client",
        "connection_id": "conn",
        "exec_id": "exec",
        "state": "running",
    }

    def append_many(kind, event, items):
        assert (kind, event) == ("shell", "output")
        batches.append(items)
        saved.extend(items)

    async def poll(ident, cursor):
        assert (ident, cursor) == ("job", 0)
        return {"output": events, "cursor": count, "state": "succeeded"}

    async def refresh(shells, rec):
        assert len(saved) == count
        rec["state"] = "succeeded"

    runtime.history = SimpleNamespace(append_many=append_many)
    runtime.refresh_shell = refresh
    await runtime.watch_shell(SimpleNamespace(poll=poll), record)

    assert [item["text"] for item in saved] == [item["text"] for item in events]
    assert all(item["client_id"] == "client" and item["exec_id"] == "exec" for item in saved)
    assert record["state"] == "succeeded"
    assert len(batches) < count or count == 1
    assert all(len(batch) <= 64 for batch in batches)
    assert all(
        sum(len(json_bytes(item)) for item in batch) <= 256 * 1024 or len(batch) == 1
        for batch in batches
    )


async def test_shell_history_failure_does_not_publish_completion_or_retry(tmp_path):
    runtime = Runtime(tmp_path)
    record = {
        "id": "job",
        "kind": "shell",
        "client_id": "client",
        "connection_id": "conn",
        "exec_id": "exec",
        "state": "running",
    }
    calls = 0
    failures = []
    saved = []

    def append_many(kind, event, items):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("unconfirmed write")
        saved.extend(items)

    async def poll(ident, cursor):
        assert cursor == 0
        return {
            "output": [{"stream": "stdout", "text": str(i)} for i in range(130)],
            "cursor": 130,
            "state": "succeeded",
        }

    async def refresh(*args):
        pytest.fail("completion must wait for all output batches")

    runtime.history = SimpleNamespace(append_many=append_many)
    runtime.persistence_failed = failures.append
    runtime.refresh_shell = refresh
    with pytest.raises(OSError, match="unconfirmed write"):
        await runtime.watch_shell(SimpleNamespace(poll=poll), record)
    assert calls == 2
    assert len(saved) == 64
    assert record["state"] == "running"
    assert len(failures) == 1
