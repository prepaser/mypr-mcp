import asyncio

import mypr_mcp.kernel_api as api


async def test_concurrent_attach_reuses_remote_handle_and_original_owner(monkeypatch):
    ready = asyncio.Event()
    calls = 0
    ident = "a" * 32

    async def rpc(op, **fields):
        nonlocal calls
        if op == "history_get":
            calls += 1
            if calls == 2:
                ready.set()
            await ready.wait()
            return {
                "id": ident,
                "kind": "shell",
                "state": "succeeded",
                "client_id": "owner",
                "connection_id": "original",
                "exec_id": "original-exec",
                "generation": "old",
            }
        assert op == "shell_read"
        return {
            "state": "succeeded",
            "output": [],
            "cursor": "end",
            "has_more": False,
            "result": {"returncode": 0},
        }

    monkeypatch.setattr(api, "_rpc", rpc)
    manager = api.TaskManager()
    first, second = await asyncio.gather(manager.attach(ident), manager.attach(ident))
    assert first is second
    assert first.status()["client_id"] == "owner"
    assert first.status()["exec_id"] == "original-exec"
    await first


async def test_historical_attach_does_not_accumulate_completed_handle_cache(monkeypatch):
    async def rpc(op, **fields):
        assert op == "history_get"
        return {
            "id": fields["id"],
            "kind": "python",
            "state": "succeeded",
            "generation": "old",
            "output": "saved",
            "has_more": False,
        }

    monkeypatch.setattr(api, "_rpc", rpc)
    manager = api.TaskManager()
    for i in range(10):
        assert isinstance(await manager.attach(f"old-{i}"), api.HistoricalTask)
    assert not manager.list()
    assert not manager.active()


async def test_legacy_python_history_preserves_prefix_without_journal(tmp_path):
    from mypr_mcp.history import History
    from mypr_mcp.runtime import Runtime

    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    record = {
        "id": "legacy",
        "generation": "a" * 32,
        "kind": "python",
        "state": "succeeded",
        "output": "saved prefix",
        "output_truncated": True,
    }
    try:
        history_id = runtime.task_history_id(record)
        runtime.history.record("python", record, entity_id=history_id)
        found = await runtime.dispatch({"op": "history_get", "id": history_id})
        assert found["output"] == "saved prefix"
        task = api.HistoricalTask(found)
        page = await task.read()
        assert page["output"] == "saved prefix"
        assert page["truncated"]
    finally:
        runtime.history.close()


def test_distinct_custom_task_names_have_distinct_journals(tmp_path):
    from mypr_mcp.runtime import Runtime

    runtime = Runtime(tmp_path)
    paths = [
        runtime.task_journal_path({"id": ident, "generation": "b" * 32, "kind": "python"})
        for ident in ("a:b", "a_b", "a/b", "a?b")
    ]
    assert len(set(paths)) == 4
    assert all(path.parent == runtime.root / "runs" for path in paths)
