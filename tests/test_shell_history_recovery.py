import asyncio
import json
import sqlite3
import sys

import pytest

from mypr_mcp.history import History
from mypr_mcp.runtime import Runtime, _open_stores


@pytest.mark.parametrize("state", ["succeeded", "failed", "cancelled"])
async def test_restart_recovers_shell_completion_after_history_output_failure(
    tmp_path, monkeypatch, state
):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    runtime.healthy = True
    replacement = None
    messages = None

    def failed_append(*args, **kwargs):
        raise OSError("history output publication failed")

    monkeypatch.setattr(runtime.history, "append_many", failed_append)
    code = 'import sys; print("retained output", flush=True)'
    if state == "failed":
        code += "; sys.exit(7)"
    elif state == "cancelled":
        code += "; input()"
    try:
        started = await runtime.shells.start(
            [sys.executable, "-c", code], stdin=state == "cancelled"
        )
        ident = started["id"]
        runtime.track_shell(ident, "review-client", "review-connection", "review-execution")
        watcher = next(iter(runtime.shell_watchers))
        if state == "cancelled":
            failures = await asyncio.wait_for(
                asyncio.gather(watcher, return_exceptions=True), 5
            )
            await asyncio.wait_for(runtime.shells.cancel(ident), 5)
        else:
            await asyncio.wait_for(runtime.shells.wait(ident), 5)
            failures = await asyncio.wait_for(
                asyncio.gather(watcher, return_exceptions=True), 5
            )
        assert isinstance(failures[0], OSError)
        assert runtime.history.get(ident)["state"] == "running"
        terminal = await runtime.shells.poll(ident)
        assert terminal["state"] == state
        journal = tmp_path / ".mypr" / "jobs" / f"{ident}.jsonl"
        saved_output = journal.read_bytes()
        runtime.stopping.set()
        await runtime.shells.close()
        await runtime.drain_background()
        runtime.history.close()

        history, messages = _open_stores(tmp_path)
        replacement = Runtime(tmp_path)
        replacement.history = history
        recovered = await replacement._dispatch_history("history_get", {"id": ident})
        assert replacement.task_records == {}
        assert recovered["state"] == state
        assert recovered["result"] == terminal["result"]
        assert recovered["generation"] == runtime.generation
        assert recovered["client_id"] == "review-client"
        assert recovered["connection_id"] == "review-connection"
        assert recovered["exec_id"] == "review-execution"
        assert recovered["warnings"][0]["code"] == "history_output_incomplete"
        assert journal.read_bytes() == saved_output
        assert history.recover() == 0
        assert [event["event"] for event in history.logs()["events"]] == ["running", state]
    finally:
        runtime.stopping.set()
        await runtime.shells.close()
        await runtime.drain_background()
        runtime.history.close()
        if replacement is not None:
            await replacement.shells.close()
            replacement.history.close()
        if messages is not None:
            messages.close()


def pending_shell(tmp_path, *, state="running", **fields):
    history = History(tmp_path)
    ident = "a" * 32
    history.record("shell", {"id": ident, "state": state, **fields}, event=state)
    path = tmp_path / ".mypr" / "jobs" / f"{ident}.json"
    path.parent.mkdir(exist_ok=True)
    metadata = {
        "id": ident, "state": "succeeded", "result": {"returncode": 0},
        "error": None, "finished_at": 42.0,
    }
    return history, ident, path, metadata


@pytest.mark.parametrize(
    "changes",
    [
        None, [], {"id": "b" * 32}, {"result": None}, {"result": {"returncode": True}},
        {"result": {"returncode": 7}}, {"finished_at": None},
        {"error": "inconsistent success"}, {"state": "failed"},
        {"finished_at": float("nan")}, {"finished_at": 10 ** 400},
        {"error": []}, {"state": "running"},
    ],
)
def test_recovery_keeps_unknown_shell_outcomes_lost(tmp_path, changes):
    history, ident, path, metadata = pending_shell(tmp_path)
    try:
        if isinstance(changes, dict):
            path.write_text(json.dumps({**metadata, **changes}))
        elif changes is not None:
            path.write_text(json.dumps(changes))
        assert history.recover() == 1
        assert history.get(ident)["state"] == "lost"
        assert history.recover() == 0
    finally:
        history.close()


def test_recovery_preserves_terminal_history_and_other_job_kinds(tmp_path):
    history, ident, path, metadata = pending_shell(tmp_path, state="failed", error="known failure")
    path.write_text(json.dumps(metadata))
    try:
        for kind, job_id in (("scan", "b" * 32), ("package", "c" * 32)):
            history.record(kind, {"id": job_id, "state": "running"})
            path.with_name(f"{job_id}.json").write_text(json.dumps({**metadata, "id": job_id}))
        assert history.recover() == 2
        assert history.get(ident)["state"] == "failed"
        assert history.get(ident)["error"] == "known failure"
        assert history.get("b" * 32)["state"] == "lost"
        assert history.get("c" * 32)["state"] == "lost"
    finally:
        history.close()


def test_recovery_bounds_warnings_and_keeps_output_retention(tmp_path):
    previous = [{"code": str(index), "text": "existing warning"} for index in range(4)]
    history, ident, path, metadata = pending_shell(
        tmp_path, generation="old", warnings=previous,
        output_evicted=True, body_evicted=True, body_evicted_at=1,
    )
    path.write_text(json.dumps({**metadata, "generation": "untrusted", "output": "ignored"}))
    try:
        assert history.recover() == 1
        record = history.get(ident)
        assert record["generation"] == "old"
        assert record["finished"] == 42
        assert record["warnings"][0]["code"] == "history_output_incomplete"
        assert record["warnings"][1:] == previous[:3]
        assert record["warnings_truncated"] is True
        assert record["output_evicted"] is True
        assert record["body_evicted"] is True
        assert record["body_evicted_at"] == 1
        assert "output" not in record
    finally:
        history.close()


def test_recovery_propagates_failed_history_commit_without_publishing_completion(tmp_path):
    history, ident, path, metadata = pending_shell(tmp_path)
    path.write_text(json.dumps(metadata))
    history._db.execute("""
        CREATE TEMP TRIGGER fail_recovery BEFORE UPDATE OF data ON entities
        WHEN json_extract(NEW.data, '$.state') = 'succeeded'
        BEGIN SELECT RAISE(ABORT, 'recovery write failed'); END
    """)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="recovery write failed"):
            history.recover()
        assert history.get(ident)["state"] == "running"
        assert [event["event"] for event in history.logs()["events"]] == ["running"]
        history._db.execute("DROP TRIGGER fail_recovery")
        assert history.recover() == 1
        assert history.get(ident)["state"] == "succeeded"
    finally:
        history.close()
