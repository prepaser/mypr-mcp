from pathlib import Path

import pytest

from mypr_mcp.history import History


def test_batch_order_and_filtered_cursor_skip_unrelated_events(tmp_path):
    history = History(tmp_path)
    try:
        events = history.append_many(
            "execution",
            "output",
            [
                {
                    "id": str(index),
                    "client_id": "alice" if index % 3 == 0 else "bob",
                    "text": str(index),
                }
                for index in range(20)
            ],
        )
        assert [event["data"]["text"] for event in events] == [str(index) for index in range(20)]
        cursor, found = 0, []
        while cursor < events[-1]["seq"]:
            page = history.logs(cursor=cursor, client_id="alice", limit=2)
            assert page["cursor"] > cursor
            cursor = page["cursor"]
            found.extend(page["events"])
        assert [event["data"]["text"] for event in found] == [
            str(index) for index in range(0, 20, 3)
        ]
        assert history.logs(cursor=cursor, client_id="alice")["events"] == []
    finally:
        history.close()


def test_legacy_database_gains_request_index_without_losing_records(tmp_path):
    import json
    import sqlite3

    root = tmp_path / ".mypr"
    root.mkdir()
    old = {"id": "legacy", "client": "owner", "request_id": "", "code": "42", "state": "succeeded"}
    with sqlite3.connect(root / "history.sqlite3") as database:
        database.execute(
            "CREATE TABLE entities (entity_seq INTEGER PRIMARY KEY AUTOINCREMENT, "
            "id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, created REAL NOT NULL, "
            "updated REAL NOT NULL, data TEXT NOT NULL)"
        )
        database.execute(
            "INSERT INTO entities(id,kind,created,updated,data) VALUES(?,?,?,?,?)",
            ("legacy", "execution", 1, 1, json.dumps(old)),
        )
    history = History(tmp_path)
    try:
        assert history.find_request("owner", "") == old
        assert history.get("legacy") == old
        assert history.find_request("other", "") is None
        with sqlite3.connect(root / "history.sqlite3") as database:
            assert database.execute(
                "SELECT 1 FROM sqlite_master WHERE name='execution_request_idx'"
            ).fetchone()
    finally:
        history.close()


def test_record_merge_summary_and_get(tmp_path: Path):
    history = History(tmp_path)
    try:
        history.record(
            "execution",
            {"id": "a", "client_id": "c", "state": "running", "code": "x", "output": "y"},
        )
        history.record("execution", {"id": "a", "state": "succeeded", "result": 1})
        assert history.get("a") == {
            "id": "a",
            "client_id": "c",
            "state": "succeeded",
            "code": "x",
            "output": "y",
            "result": 1,
            "kind": "execution",
        }
        summary = history.list()["items"]
        assert summary == [
            {"id": "a", "client_id": "c", "state": "succeeded", "result": 1, "kind": "execution"}
        ]
    finally:
        history.close()


def test_logs_cursor_filter_and_recovery(tmp_path: Path):
    history = History(tmp_path)
    try:
        history.record("python", {"id": "a", "client_id": "one", "state": "running"}, "started")
        history.record("python", {"id": "b", "client_id": "two", "state": "succeeded"}, "done")
        history.append("python", "output", {"id": "a", "client_id": "one", "text": "hello"})
        tail = history.logs(client_id="one")
        assert [event["event"] for event in tail["events"]] == ["started", "output"]
        assert tail["events"] == sorted(tail["events"], key=lambda event: event["seq"])
        history.record("shell", {"id": "c", "client_id": "one", "state": "running"}, "running")
        forward = history.logs(cursor=tail["cursor"], client_id="one")
        assert [event["id"] for event in forward["events"]] == ["c"]
        assert history.recover() == 2
        assert history.get("a")["state"] == "lost"
    finally:
        history.close()


def test_list_and_log_validation(tmp_path: Path):
    history = History(tmp_path)
    try:
        with pytest.raises(ValueError):
            history.list(limit=0)
        with pytest.raises(ValueError):
            history.logs(cursor="bad")
        with pytest.raises(ValueError):
            history.record("unknown", {"id": "x"})
    finally:
        history.close()


def test_empty_log_cursor_and_filtered_pages_survive_restart(tmp_path):
    history = History(tmp_path)
    cursor = history.logs(client_id="a")["cursor"]
    assert cursor == 0
    for index in range(5):
        history.append("connection", "connected", {"client_id": "a", "id": str(index)})
        history.append("connection", "connected", {"client_id": "b", "id": str(index)})
    page = history.logs(cursor=cursor, client_id="a", limit=2)
    assert [event["id"] for event in page["events"]] == ["0", "1"]
    history.close()
    history = History(tmp_path)
    try:
        next_page = history.logs(cursor=page["cursor"], client_id="a", limit=2)
        assert [event["id"] for event in next_page["events"]] == ["2", "3"]
        last = history.logs(cursor=next_page["cursor"], client_id="a", limit=2)
        assert [event["id"] for event in last["events"]] == ["4"]
        assert history.logs(cursor=last["cursor"], client_id="a")["events"] == []
    finally:
        history.close()
