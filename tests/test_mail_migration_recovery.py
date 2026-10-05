from __future__ import annotations

import sqlite3

import pytest

from mypr_mcp.mail_store import MailStore


def _seed(root):
    store = MailStore(root)
    ref = store.message_ref("work", "INBOX", 7, 3, "endpoint-a")
    watch = store.create_watch(
        "client", "work", "INBOX", uidvalidity=7, endpoint_identity="endpoint-a"
    )
    notification = store.add_notification(
        client_id="client",
        watch_id=watch["id"],
        account="work",
        mailbox="INBOX",
        endpoint_identity="endpoint-a",
        uidvalidity=7,
        uid=3,
        kind="new_mail",
        payload={"message_id": ref},
    )
    store.close()
    return ref, notification["id"]


def test_interrupted_rebuild_copy_is_discarded_without_changing_rows(tmp_path):
    ref, notification = _seed(tmp_path)
    db_path = tmp_path / ".mypr" / "history.sqlite3"
    db = sqlite3.connect(db_path)
    try:
        for table in ("mail_refs", "mail_notifications"):
            db.execute(f"CREATE TABLE {table}_new AS SELECT * FROM {table}")
        db.commit()
    finally:
        db.close()

    store = MailStore(tmp_path)
    try:
        assert store.resolve_ref(ref)["endpoint_identity"] == "endpoint-a"
        assert store.notifications("client")["items"][0]["id"] == notification
    finally:
        store.close()

    db = sqlite3.connect(db_path)
    try:
        assert (
            db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_new'"
            ).fetchall()
            == []
        )
    finally:
        db.close()


def test_interrupted_drop_recovers_nonempty_replacement(tmp_path):
    ref, _ = _seed(tmp_path)
    db_path = tmp_path / ".mypr" / "history.sqlite3"
    db = sqlite3.connect(db_path)
    try:
        db.execute("ALTER TABLE mail_refs RENAME TO mail_refs_new")
        db.commit()
    finally:
        db.close()

    store = MailStore(tmp_path)
    try:
        assert store.resolve_ref(ref)["endpoint_identity"] == "endpoint-a"
    finally:
        store.close()


def test_empty_replacement_after_drop_is_recovered(tmp_path):
    _seed(tmp_path)
    db_path = tmp_path / ".mypr" / "history.sqlite3"
    db = sqlite3.connect(db_path)
    try:
        db.execute("ALTER TABLE mail_notifications RENAME TO mail_notifications_new")
        db.execute("DELETE FROM mail_notifications_new")
        db.commit()
    finally:
        db.close()

    store = MailStore(tmp_path)
    try:
        assert store.notifications("client")["items"] == []
    finally:
        store.close()


def test_nonempty_replacement_wins_over_empty_original(tmp_path):
    _seed(tmp_path)
    db_path = tmp_path / ".mypr" / "history.sqlite3"
    db = sqlite3.connect(db_path)
    try:
        db.execute("DELETE FROM mail_refs")
        db.execute("CREATE TABLE mail_refs_new AS SELECT * FROM mail_refs")
        db.execute(
            "INSERT INTO mail_refs_new "
            "(id,account,mailbox,uidvalidity,uid,endpoint_identity,created) "
            "VALUES(?,?,?,?,?,?,?)",
            ("recovered", "work", "INBOX", 7, 3, "endpoint-a", 1.0),
        )
        db.commit()
    finally:
        db.close()

    store = MailStore(tmp_path)
    try:
        assert store.resolve_ref("recovered")["endpoint_identity"] == "endpoint-a"
    finally:
        store.close()


def test_different_replacement_data_is_preserved_and_refused(tmp_path):
    _seed(tmp_path)
    db_path = tmp_path / ".mypr" / "history.sqlite3"
    db = sqlite3.connect(db_path)
    try:
        db.execute("CREATE TABLE mail_refs_new AS SELECT * FROM mail_refs")
        db.execute(
            "INSERT INTO mail_refs_new "
            "(id,account,mailbox,uidvalidity,uid,endpoint_identity,created) "
            "VALUES(?,?,?,?,?,?,?)",
            ("extra", "work", "INBOX", 7, 4, "endpoint-a", 1.0),
        )
        db.commit()
    finally:
        db.close()

    with pytest.raises(sqlite3.DatabaseError, match="different data"):
        MailStore(tmp_path)

    db = sqlite3.connect(db_path)
    try:
        assert db.execute("SELECT COUNT(*) FROM mail_refs").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM mail_refs_new").fetchone()[0] == 2
    finally:
        db.close()


def test_legacy_namespace_indexes_rebuild_transactionally(tmp_path):
    db_path = tmp_path / ".mypr" / "history.sqlite3"
    db_path.parent.mkdir(parents=True)
    db = sqlite3.connect(db_path)
    try:
        db.executescript(
            """
            CREATE TABLE mail_notifications (
                notification_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                client_id TEXT NOT NULL,
                watch_id TEXT NOT NULL,
                account TEXT NOT NULL,
                mailbox TEXT NOT NULL,
                uidvalidity INTEGER,
                uid INTEGER,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                created REAL NOT NULL,
                acknowledged REAL,
                UNIQUE(client_id, watch_id, uidvalidity, uid, kind)
            );
            CREATE INDEX mail_notifications_client_idx
                ON mail_notifications(client_id, acknowledged, notification_seq);
            INSERT INTO mail_notifications
                (id,client_id,watch_id,account,mailbox,uidvalidity,uid,kind,payload,created)
                VALUES ('mail-1','client','watch-1','work','INBOX',7,3,'new_mail','{}',1.0);
            UPDATE sqlite_sequence SET seq=500 WHERE name='mail_notifications';
            CREATE TABLE mail_refs (
                id TEXT PRIMARY KEY NOT NULL,
                account TEXT NOT NULL,
                mailbox TEXT NOT NULL,
                uidvalidity INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                created REAL NOT NULL,
                UNIQUE(account, mailbox, uidvalidity, uid)
            );
            CREATE INDEX mail_refs_namespace_idx
                ON mail_refs(account, mailbox, uidvalidity, uid);
            INSERT INTO mail_refs VALUES ('msg-1','work','INBOX',7,3,1.0);
            """
        )
        db.commit()
    finally:
        db.close()

    store = MailStore(tmp_path)
    try:
        assert store.resolve_ref("msg-1")["endpoint_identity"] == ""
        assert store.notifications("client")["items"][0]["id"] == "mail-1"
        ref_indexes = store._db.execute("PRAGMA index_list(mail_refs)").fetchall()
        assert any(row[2] for row in ref_indexes)
        store.add_notification(
            client_id="client",
            watch_id="watch-1",
            account="work",
            mailbox="INBOX",
            uidvalidity=7,
            uid=4,
            kind="new_mail",
            payload={},
        )
        assert (
            store._db.execute("SELECT MAX(notification_seq) FROM mail_notifications").fetchone()[0]
            == 501
        )
    finally:
        store.close()


@pytest.mark.parametrize("state", ["original_empty", "replacement_only", "replacement_empty"])
def test_interrupted_notification_rebuild_preserves_sequence_highwater(tmp_path, state):
    _seed(tmp_path)
    path = tmp_path / ".mypr" / "history.sqlite3"
    db = sqlite3.connect(path)
    try:
        schema = db.execute(
            "SELECT sql FROM sqlite_master WHERE name='mail_notifications'"
        ).fetchone()[0]
        db.execute(schema.replace("mail_notifications", "mail_notifications_new", 1))
        db.execute("INSERT INTO mail_notifications_new SELECT * FROM mail_notifications")
        db.execute("UPDATE sqlite_sequence SET seq=500 WHERE name='mail_notifications'")
        if state == "original_empty":
            db.execute("DELETE FROM mail_notifications")
        elif state == "replacement_only":
            db.execute("UPDATE sqlite_sequence SET seq=500 WHERE name='mail_notifications_new'")
            db.execute("DROP TABLE mail_notifications")
        else:
            db.execute("DELETE FROM mail_notifications_new")
        db.commit()
    finally:
        db.close()
    store = MailStore(tmp_path)
    try:
        store.add_notification(
            client_id="client",
            watch_id="new-watch",
            account="work",
            mailbox="INBOX",
            uidvalidity=7,
            uid=4,
            kind="new_mail",
            payload={},
        )
        assert (
            store._db.execute("SELECT MAX(notification_seq) FROM mail_notifications").fetchone()[0]
            == 501
        )
    finally:
        store.close()


def test_legacy_dropped_sequence_rejects_stale_notification_cursor(tmp_path):
    _seed(tmp_path)
    path = tmp_path / ".mypr" / "history.sqlite3"
    with sqlite3.connect(path) as db:
        schema = db.execute(
            "SELECT sql FROM sqlite_master WHERE name='mail_notifications'"
        ).fetchone()[0]
        db.execute(schema.replace("mail_notifications", "mail_notifications_new", 1))
        db.execute("INSERT INTO mail_notifications_new SELECT * FROM mail_notifications")
        db.execute("UPDATE sqlite_sequence SET seq=500 WHERE name='mail_notifications'")
        db.execute("DROP TABLE mail_notifications")
    store = MailStore(tmp_path)
    try:
        with pytest.raises(ValueError, match="retry without cursor"):
            store.notifications("client", cursor=500)
        assert len(store.notifications("client")["items"]) == 1
        with store._tx():
            store._db.execute("UPDATE sqlite_sequence SET seq=500 WHERE name='mail_notifications'")
            store._db.execute("DELETE FROM mail_notifications")
        assert store.notifications("client", cursor=500)["items"] == []
    finally:
        store.close()
