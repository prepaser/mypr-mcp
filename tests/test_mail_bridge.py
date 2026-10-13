from __future__ import annotations

import sqlite3

import pytest
from test_timer_restart import _restart_fixture

from mypr_mcp.bridge import ConnectionBridge
from mypr_mcp.history import History
from mypr_mcp.mail_store import MailStore


async def test_restart_fallback_skips_legacy_mail_preview_without_migration(
    tmp_path, monkeypatch
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.close()
    store = MailStore(workspace)
    store.close()

    database = workspace / ".mypr" / "history.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE mail_watches RENAME TO mail_watches_current")
        connection.execute(
            """
            CREATE TABLE mail_watches (
                id TEXT PRIMARY KEY NOT NULL,
                client_id TEXT NOT NULL,
                account TEXT NOT NULL,
                mailbox TEXT NOT NULL,
                uidvalidity INTEGER,
                last_uid INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                created REAL NOT NULL,
                updated REAL NOT NULL,
                UNIQUE(client_id, account, mailbox)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO mail_watches(
                id,client_id,account,mailbox,uidvalidity,last_uid,state,error,created,updated
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            ("legacy-watch", "alice", "work", "INBOX", 7, 3, "watching", None, 1, 1),
        )
        connection.execute("DROP TABLE mail_watches_current")
        connection.commit()

    exec_id, _ = _restart_fixture(workspace)
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"

    async def no_network(*args):
        return None

    monkeypatch.setattr(bridge, "_recover_restart", no_network)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert result["state"] == "succeeded"
    assert result["exec_id"] == exec_id
    assert "mail" not in result
    with sqlite3.connect(database) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(mail_watches)")}
        row = connection.execute(
            "SELECT id,client_id,account,mailbox,last_uid FROM mail_watches"
        ).fetchone()
    assert "endpoint_identity" not in columns
    assert row == ("legacy-watch", "alice", "work", "INBOX", 3)

    migrated = MailStore(workspace)
    try:
        watch = migrated.get_watch("legacy-watch", "alice")
    finally:
        migrated.close()
    assert watch is not None
    assert watch["endpoint_identity"] == ""
    assert watch["last_uid"] == 3


@pytest.mark.parametrize("initialized", [False, True])
async def test_restart_fallback_returns_only_owned_cached_mail(tmp_path, monkeypatch, initialized):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    store = MailStore(workspace)
    watch = store.create_watch("alice", "work", "INBOX", uidvalidity=41)
    notice = store.add_notification(
        client_id="alice",
        watch_id=watch["id"],
        account="work",
        mailbox="INBOX",
        uidvalidity=41,
        uid=1,
        kind="new_mail",
        payload={"message_id": "ref", "subject": "cached"},
    )
    store.add_notification(
        client_id="bob",
        watch_id="bob-watch",
        account="work",
        mailbox="INBOX",
        uidvalidity=41,
        uid=2,
        kind="new_mail",
        payload={"message_id": "other"},
    )
    store.close()
    exec_id, _ = _restart_fixture(workspace)
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice" if initialized else None

    async def no_network(*args):
        return None

    monkeypatch.setattr(bridge, "_recover_restart", no_network)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)
    if initialized:
        assert [item["id"] for item in result["mail"]["items"]] == [notice["id"]]
        assert result["mail"]["watches"][0]["state"] == "offline"
    else:
        assert "mail" not in result


@pytest.mark.parametrize("payload", ["{", "[]"])
async def test_restart_fallback_survives_corrupt_cached_mail(tmp_path, monkeypatch, payload):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.close()
    store = MailStore(workspace)
    watch = store.create_watch("alice", "work", "INBOX", uidvalidity=41)
    notice = store.add_notification(
        client_id="alice",
        watch_id=watch["id"],
        account="work",
        mailbox="INBOX",
        uidvalidity=41,
        uid=1,
        kind="new_mail",
        payload={"message_id": "ref"},
    )
    store._db.execute(
        "UPDATE mail_notifications SET payload=? WHERE id=?", (payload, notice["id"])
    )
    store.close()
    exec_id, _ = _restart_fixture(workspace)
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"

    async def no_network(*args):
        return None

    monkeypatch.setattr(bridge, "_recover_restart", no_network)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert result["state"] == "succeeded"
    assert result["mail"]["items"][0]["id"] == notice["id"]
    assert result["mail"]["items"][0]["payload_corrupt"] is True
    assert result["mail"]["warnings"][0]["code"] == "mail_notification_payload_corrupt"


async def test_restart_fallback_survives_corrupt_send_metadata(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.close()
    store = MailStore(workspace)
    value = store.persist_draft(
        "alice", "work", b"From: sender@example.test\r\n\r\nbody\r\n", {}
    )
    send = store.create_send("alice", value["id"], "request")
    with store._tx():
        store._db.execute(
            "UPDATE mail_sends SET state='unknown', accepted=? WHERE id=?",
            ("{", send["id"]),
        )
    store.close()
    exec_id, _ = _restart_fixture(workspace)
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice"

    async def no_network(*args):
        return None

    monkeypatch.setattr(bridge, "_recover_restart", no_network)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)

    assert result["state"] == "succeeded"
    assert result["mail"]["sends"][0]["id"] == send["id"]
    assert result["mail"]["sends"][0]["outcome_uncertain"] is True
    assert result["mail"]["warnings"][0]["code"] == "mail_send_metadata_corrupt"
