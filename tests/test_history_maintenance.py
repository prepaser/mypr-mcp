from __future__ import annotations

import hashlib
import sqlite3
import time

from mypr_mcp import history_maintenance
from mypr_mcp.history import History
from mypr_mcp.history_maintenance import vacuum_if_worthwhile
from mypr_mcp.mail_store import MailStore


def _age_database(root, *, entity_id: str, event_seq: int, age: float = 90 * 86400) -> None:
    cutoff = time.time() - age
    with sqlite3.connect(root / ".mypr" / "history.sqlite3") as database:
        database.execute("UPDATE entities SET updated=? WHERE id=?", (cutoff, entity_id))
        database.execute("UPDATE events SET time=? WHERE seq=?", (cutoff, event_seq))
        database.commit()


def test_history_gc_preview_apply_compacts_body_and_reports_watermark(tmp_path):
    history = History(tmp_path)
    history.record(
        "execution",
        {
            "id": "old",
            "state": "succeeded",
            "generation": "g1",
            "client_id": "client",
            "result_ref": {"path": ".mypr/task-results/old.json"},
            "code": "print('old')",
            "output": "x" * 1000,
            "events": [{"text": "event"}],
        },
    )
    event = history.append("execution", "output", {"id": "old", "text": "body"})
    _age_database(tmp_path, entity_id="old", event_seq=event["seq"])
    before = history.get("old")

    plan = history.storage_history_snapshot(retention_days=30)
    assert plan["entities"][0]["id"] == "old"
    assert plan["events"][0]["seq"] == event["seq"]
    assert history.get("old") == before

    result = history.storage_history_apply(plan)
    assert result["entities"] == 1
    assert result["events"] == 1
    assert result["pruned_through_seq"] == event["seq"]
    compacted = history.get("old")
    assert compacted["id"] == "old"
    assert compacted["state"] == "succeeded"
    assert compacted["generation"] == "g1"
    assert compacted["result_ref"] == {"path": ".mypr/task-results/old.json"}
    assert compacted["code_sha256"] == hashlib.sha256(b"print('old')").hexdigest()
    assert compacted["body_evicted"] is True
    assert "code" not in compacted
    assert "output" not in compacted
    assert "events" not in compacted

    current = history.append("execution", "output", {"id": "new", "text": "new"})
    logs = history.logs(cursor=0)
    assert [item["seq"] for item in logs["events"]] == [current["seq"]]
    assert logs["history_truncated"] is True
    assert logs["pruned_through_seq"] == event["seq"]
    history.close()


def test_history_gc_revalidates_and_protects_active_or_corrupt_rows(tmp_path):
    history = History(tmp_path)
    history.record("execution", {"id": "active", "state": "running", "code": "x"})
    history.record("execution", {"id": "old", "state": "succeeded", "code": "x"})
    with sqlite3.connect(tmp_path / ".mypr" / "history.sqlite3") as database:
        cutoff = time.time() - 90 * 86400
        database.execute("UPDATE entities SET updated=?", (cutoff,))
        database.commit()
    plan = history.storage_history_snapshot(retention_days=30)
    assert [item["id"] for item in plan["entities"]] == ["old"]
    history.record("execution", {"id": "old", "state": "succeeded", "code": "changed"})
    result = history.storage_history_apply(plan)
    assert result["entities"] == 0
    assert history.get("old")["code"] == "changed"
    history.close()


def test_history_gc_revalidates_event_owner_state(tmp_path):
    history = History(tmp_path)
    history.record("execution", {"id": "job", "state": "succeeded", "code": "x"})
    event = history.append("execution", "output", {"id": "job", "text": "old"})
    _age_database(tmp_path, entity_id="job", event_seq=event["seq"])
    plan = history.storage_history_snapshot(retention_days=30)

    history.record("execution", {"id": "job", "state": "running", "code": "x"})
    result = history.storage_history_apply(plan)
    assert result["events"] == 0
    assert history.logs(cursor=0)["events"][0]["seq"] == event["seq"]
    history.close()


def test_history_logs_keeps_protected_event_below_prune_watermark(tmp_path):
    history = History(tmp_path)
    history.record("execution", {"id": "active", "state": "running"})
    active_event = history.append("execution", "running", {"id": "active"})
    history.record("execution", {"id": "terminal", "state": "succeeded", "code": "x"})
    terminal_event = history.append("execution", "succeeded", {"id": "terminal"})
    with sqlite3.connect(tmp_path / ".mypr" / "history.sqlite3") as database:
        cutoff = time.time() - 90 * 86400
        database.execute("UPDATE entities SET updated=? WHERE id='terminal'", (cutoff,))
        database.execute("UPDATE events SET time=?", (cutoff,))
        database.commit()
    plan = history.storage_history_snapshot(retention_days=30)
    result = history.storage_history_apply(plan)
    assert result["pruned_through_seq"] == terminal_event["seq"]
    page = history.logs(cursor=0)
    assert [item["seq"] for item in page["events"]] == [active_event["seq"]]
    assert page["history_truncated"] is True
    history.close()


def test_history_gc_candidate_limit_skips_old_protected_rows(tmp_path, monkeypatch):
    monkeypatch.setattr("mypr_mcp.history._MAX_HISTORY_GC_ITEMS", 1)
    history = History(tmp_path)
    history.record(
        "execution", {"id": "evicted-1", "state": "succeeded", "body_evicted": True}
    )
    history.record(
        "execution", {"id": "evicted-2", "state": "succeeded", "body_evicted": True}
    )
    history.record("execution", {"id": "eligible", "state": "succeeded", "code": "x"})
    history.record("execution", {"id": "active", "state": "running"})
    history.append("execution", "running", {"id": "active"})
    eligible_event = history.append("execution", "succeeded", {"id": "eligible"})
    with sqlite3.connect(tmp_path / ".mypr" / "history.sqlite3") as database:
        cutoff = time.time() - 90 * 86400
        database.execute(
            "UPDATE entities SET updated=? WHERE id IN ('evicted-1','evicted-2','eligible')",
            (cutoff,),
        )
        database.execute("UPDATE events SET time=?", (cutoff,))
        database.commit()
    plan = history.storage_history_snapshot(retention_days=30)
    assert [item["id"] for item in plan["entities"]] == ["eligible"]
    assert [item["seq"] for item in plan["events"]] == [eligible_event["seq"]]
    history.close()


def test_history_gc_candidate_limit_ignores_malformed_ids(tmp_path, monkeypatch):
    monkeypatch.setattr("mypr_mcp.history._MAX_HISTORY_GC_ITEMS", 1)
    history = History(tmp_path)
    history.record("execution", {"id": "eligible", "state": "succeeded", "code": "x"})
    with sqlite3.connect(tmp_path / ".mypr" / "history.sqlite3") as database:
        cutoff = time.time() - 90 * 86400
        database.execute(
            "INSERT INTO entities(id,kind,created,updated,data) "
            "VALUES (?,?,?,?,?)",
            (
                "malformed",
                "execution",
                cutoff,
                cutoff - 1,
                '{"id":42,"state":"succeeded","code":"x"}',
            ),
        )
        database.execute("UPDATE entities SET updated=? WHERE id='eligible'", (cutoff,))
        database.commit()
    plan = history.storage_history_snapshot(retention_days=30)
    assert [item["id"] for item in plan["entities"]] == ["eligible"]
    history.close()


def test_mail_send_seq_migration_preserves_legacy_cursor(tmp_path):
    path = tmp_path / ".mypr" / "history.sqlite3"
    path.parent.mkdir()
    with sqlite3.connect(path) as database:
        database.executescript(
            """
            CREATE TABLE mail_sends (
                id TEXT PRIMARY KEY NOT NULL,
                draft_id TEXT NOT NULL,
                client_id TEXT NOT NULL,
                request_id TEXT,
                state TEXT NOT NULL,
                accepted TEXT NOT NULL DEFAULT '[]',
                rejected TEXT NOT NULL DEFAULT '[]',
                rejected_details TEXT NOT NULL DEFAULT '[]',
                stage TEXT,
                error TEXT,
                warning TEXT,
                created REAL NOT NULL,
                updated REAL NOT NULL,
                UNIQUE(draft_id, request_id)
            );
            INSERT INTO mail_sends(
                id,draft_id,client_id,request_id,state,created,updated
            ) VALUES ('send-1','draft-1','client',NULL,'queued',1,1);
            """
        )
        database.commit()
    history = History(tmp_path)
    history.close()
    with sqlite3.connect(path) as database:
        columns = {row[1] for row in database.execute("PRAGMA table_info(mail_sends)")}
        assert "send_seq" in columns
        assert database.execute("SELECT send_seq,id FROM mail_sends").fetchone() == (1, "send-1")
        assert database.execute(
            "SELECT value FROM history_meta WHERE key='mail_send_seq_migrated'"
        ).fetchone() == ("1",)
    store = MailStore(tmp_path)
    try:
        assert store.sends("client")["items"][0]["id"] == "send-1"
    finally:
        store.close()


def test_vacuum_skips_when_filesystem_space_is_insufficient(tmp_path, monkeypatch):
    path = tmp_path / "history.sqlite3"
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE values_table(value TEXT)")
        database.commit()

        class Stats:
            f_bavail = path.stat().st_size * 3 // 2
            f_frsize = 1

        monkeypatch.setattr("mypr_mcp.history_maintenance.os.statvfs", lambda _: Stats())
        result = vacuum_if_worthwhile(database, path, last_vacuum=None)
    assert result["vacuumed"] is False
    assert result["reason"] == "insufficient_filesystem_space"


def test_vacuum_reports_space_reclaimed_after_post_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "history.sqlite3"
    with sqlite3.connect(path) as database:
        database.execute("PRAGMA journal_mode=WAL")
        database.execute("CREATE TABLE values_table(value BLOB)")
        database.executemany(
            "INSERT INTO values_table(value) VALUES (?)",
            [(b"x" * 4096,) for _ in range(4096)],
        )
        database.commit()
        database.execute("DELETE FROM values_table")
        database.commit()
        monkeypatch.setattr(history_maintenance, "VACUUM_MIN_DB_BYTES", 1)
        monkeypatch.setattr(history_maintenance, "VACUUM_MIN_FREE_BYTES", 1)
        monkeypatch.setattr(history_maintenance, "VACUUM_MIN_FREE_RATIO", 0)
        result = vacuum_if_worthwhile(database, path, last_vacuum=None)
        assert result["vacuumed"] is True
        assert result["post_checkpoint"] == [0, 0, 0]
        assert result["reclaimed_bytes"] > 0
        assert path.stat().st_size < 16 * 1024 * 1024


def test_vacuum_rate_limit_retries_busy_post_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "history.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE values_table(value BLOB)")
        connection.executemany(
            "INSERT INTO values_table(value) VALUES (?)",
            [(b"x" * 4096,) for _ in range(1024)],
        )
        connection.commit()
        connection.execute("DELETE FROM values_table")
        connection.commit()
        monkeypatch.setattr(history_maintenance, "VACUUM_MIN_DB_BYTES", 1)
        monkeypatch.setattr(history_maintenance, "VACUUM_MIN_FREE_BYTES", 1)
        monkeypatch.setattr(history_maintenance, "VACUUM_MIN_FREE_RATIO", 0)

        class BusyPostCheckpoint:
            def __init__(self, wrapped):
                self.wrapped = wrapped
                self.checkpoints = 0

            def execute(self, statement, *parameters):
                result = self.wrapped.execute(statement, *parameters)
                if statement == "PRAGMA wal_checkpoint(TRUNCATE)":
                    self.checkpoints += 1
                    if self.checkpoints == 2:
                        return _BusyResult()
                return result

        class _BusyResult:
            def fetchone(self):
                return (1, 0, 0)

        database = BusyPostCheckpoint(connection)
        saved: list[float] = []
        first = vacuum_if_worthwhile(
            database,
            path,
            last_vacuum=None,
            now=1000,
            save_last_vacuum=saved.append,
        )
        assert first["vacuumed"] is True
        assert first["reason"] == "busy_or_unavailable:post_checkpoint"
        assert saved == [1000]
        second = vacuum_if_worthwhile(
            database,
            path,
            last_vacuum=saved[-1],
            now=1001,
            save_last_vacuum=saved.append,
        )
        assert second["reason"] == "rate_limited"
        assert database.checkpoints == 3
        assert saved == [1000]
