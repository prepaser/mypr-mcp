from __future__ import annotations

import json
import os
import time

from mypr_mcp.mail_store import MailStore


def draft(store: MailStore, client: str = "client") -> dict:
    return store.persist_draft(
        client,
        "work",
        b"From: sender@example.test\r\n\r\nbody\r\n",
        {"recipients": ["to@example.test"]},
    )


def age_send(store: MailStore, send_id: str, draft_id: str) -> None:
    old = time.time() - 31 * 24 * 60 * 60
    with store._tx():
        store._db.execute(
            "UPDATE mail_sends SET updated=?,state='accepted' WHERE id=?",
            (old, send_id),
        )
        store._db.execute(
            "UPDATE mail_drafts SET created=? WHERE id=?",
            (old, draft_id),
        )


def test_default_send_request_id_is_bound_to_draft(tmp_path):
    store = MailStore(tmp_path)
    try:
        value = draft(store)
        first = store.create_send("client", value["id"], None)
        second = store.create_send("client", value["id"], None)
        assert first["id"] == second["id"]
        assert first["request_id"] == f"draft:{value['id']}"
        assert len(store.sends("client")["items"]) == 1
    finally:
        store.close()


def test_watch_batch_commits_reference_notifications_and_cursor_once(tmp_path):
    store = MailStore(tmp_path)
    try:
        watch = store.create_watch(
            "client", "work", "INBOX", uidvalidity=7, endpoint_identity="ep-a"
        )
        first = store.record_watch_batch(
            watch["id"], uidvalidity=7, uids=[1, 2], endpoint_identity="ep-a"
        )
        second = store.record_watch_batch(
            watch["id"], uidvalidity=7, uids=[1, 2], endpoint_identity="ep-a"
        )
        assert [item["message_id"] for item in first] == [item["message_id"] for item in second]
        assert store.get_watch(watch["id"])["last_uid"] == 2
        assert len(store.notifications("client")["items"]) == 2
        assert len(store._db.execute("SELECT id FROM mail_refs").fetchall()) == 2
    finally:
        store.close()


def test_commit_watch_accepts_precreated_refs_and_clears_error_atomically(tmp_path):
    store = MailStore(tmp_path)
    try:
        watch = store.create_watch(
            "client", "work", "INBOX", uidvalidity=7, endpoint_identity="ep-a"
        )
        store.update_watch(watch["id"], state="error", error="offline")
        message_id = store.message_ref("work", "INBOX", 7, 3, "ep-a")
        notifications = store.commit_watch(
            watch["id"],
            uidvalidity=7,
            last_uid=3,
            endpoint_identity="ep-a",
            notifications=[{"uid": 3, "kind": "new_mail", "payload": {"message_id": message_id}}],
        )
        assert notifications[0]["message_id"] == message_id
        assert store.get_watch(watch["id"])["error"] is None
    finally:
        store.close()


def test_watch_error_can_be_cleared_and_namespace_reset_generates_new_reference(tmp_path):
    store = MailStore(tmp_path)
    try:
        watch = store.create_watch(
            "client", "work", "INBOX", uidvalidity=7, endpoint_identity="ep-a"
        )
        store.update_watch(watch["id"], state="error", error="offline")
        store.update_watch(watch["id"], state="watching", error=None)
        assert store.get_watch(watch["id"])["error"] is None
        old_ref = store.message_ref("work", "INBOX", 7, 1, "ep-a")
        notification = store.reset_watch_namespace(
            watch["id"], uidvalidity=8, last_uid=0, endpoint_identity="ep-b"
        )
        new_ref = store.message_ref("work", "INBOX", 8, 1, "ep-b")
        assert notification["kind"] == "namespace_changed"
        assert new_ref != old_ref
        assert store.resolve_ref(old_ref)["endpoint_identity"] == "ep-a"
        assert store.get_watch(watch["id"])["endpoint_identity"] == "ep-b"
    finally:
        store.close()


def test_notification_dedup_includes_endpoint_identity_for_same_uid_namespace(tmp_path):
    store = MailStore(tmp_path)
    try:
        watch = store.create_watch(
            "client", "work", "INBOX", uidvalidity=41, endpoint_identity="server-a"
        )
        first = store.add_notification(
            client_id="client",
            watch_id=watch["id"],
            account="work",
            mailbox="INBOX",
            endpoint_identity="server-a",
            uidvalidity=41,
            uid=1,
            kind="new_mail",
            payload={"message_id": "server-a-message"},
        )
        second = store.add_notification(
            client_id="client",
            watch_id=watch["id"],
            account="work",
            mailbox="INBOX",
            endpoint_identity="server-b",
            uidvalidity=41,
            uid=1,
            kind="new_mail",
            payload={"message_id": "server-b-message"},
        )
        assert first["id"] != second["id"]
        assert second["message_id"] == "server-b-message"
        assert len(store.notifications("client")["items"]) == 2
    finally:
        store.close()


def test_snapshot_existing_offline_view_does_not_mutate_watch_state(tmp_path):
    store = MailStore(tmp_path)
    try:
        watch = store.create_watch("client", "work", "INBOX", uidvalidity=1)
        store.close()
        preview = MailStore.snapshot_existing(tmp_path, "client", offline=True)
        assert preview["watches"][0]["state"] == "offline"
        fresh = MailStore(tmp_path, initialize=False)
        try:
            assert fresh.get_watch(watch["id"])["state"] == "pending"
        finally:
            fresh.close()
    finally:
        if store.available:
            store.close()


def test_snapshot_is_bounded_and_detects_pending_send_beyond_preview_page(tmp_path):
    store = MailStore(tmp_path)
    try:
        for index in range(6):
            value = draft(store)
            send = store.create_send("client", value["id"], f"request-{index}")
            if index < 5:
                store.update_send(send["id"], state="failed", error="refused")
        for index in range(40):
            store.create_watch("client", f"account-{index}", "INBOX", uidvalidity=1)
        preview = store.snapshot("client")
        assert preview is not None
        assert [send["state"] for send in preview["sends"]] == ["queued"]
        assert preview["sends_more"] is False
        assert len(json.dumps(preview, ensure_ascii=False, separators=(",", ":")).encode()) <= 4096
        assert len(preview["watches"]) <= 6
        assert len(preview["sends"]) <= 3
    finally:
        store.close()


def test_gc_returns_relative_terminal_mime_and_cleans_rows_after_delete(tmp_path):
    store = MailStore(tmp_path)
    try:
        value = draft(store)
        send = store.create_send("client", value["id"], "request")
        age_send(store, send["id"], value["id"])
        notification = store.add_notification(
            client_id="client",
            watch_id="watch",
            account="work",
            mailbox="INBOX",
            uidvalidity=1,
            uid=1,
            kind="new_mail",
            payload={"message_id": "msg"},
        )
        store.ack_notifications("client", [notification["id"]])
        old = time.time() - 31 * 24 * 60 * 60
        with store._tx():
            store._db.execute("UPDATE mail_notifications SET acknowledged=?", (old,))
        snapshot = store.storage_gc_snapshot()
        assert snapshot["candidates"]
        item = snapshot["candidates"][0]
        assert item["path"].startswith(".mypr/mail/")
        assert item["requires_tombstone"] is True
        assert store.storage_gc_before_delete([item]) == [item["path"]]
        store.mail_root.joinpath("drafts", f"{value['id']}.eml").unlink()
        result = store.storage_gc_after_delete([item])
        assert result["drafts"] == 1
        assert result["sends"] == 1
        assert result["notifications"] == 1
        assert store.get_draft(value["id"]) is None
        assert store.get_send(send["id"]) is None
    finally:
        store.close()


def test_gc_protects_unsent_and_inflight_drafts(tmp_path):
    store = MailStore(tmp_path)
    try:
        unsent = draft(store, "unsent")
        inflight = draft(store, "inflight")
        send = store.create_send("inflight", inflight["id"], "request")
        store.update_send(send["id"], state="unknown")
        snapshot = store.storage_gc_snapshot()
        paths = set(snapshot["protected_paths"])
        assert f".mypr/mail/drafts/{unsent['id']}.eml" in paths
        assert f".mypr/mail/drafts/{inflight['id']}.eml" in paths
        assert snapshot["candidates"] == []
    finally:
        store.close()


def test_failed_begin_releases_store_lock_for_other_threads(tmp_path):
    import sqlite3
    import threading

    import pytest

    store = MailStore(tmp_path)
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    store._db.execute("PRAGMA busy_timeout=0")
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError):
            store.create_watch("client", "work", "INBOX")
    finally:
        blocker.rollback()
        blocker.close()
    accessed = threading.Event()

    def query():
        store.watches()
        accessed.set()

    thread = threading.Thread(target=query, daemon=True)
    thread.start()
    try:
        assert accessed.wait(2)
    finally:
        if not accessed.is_set():
            store._lock.release()
        thread.join(timeout=2)
        store.close()


def test_draft_mime_remains_usable_after_workspace_move(tmp_path):
    import hashlib
    from pathlib import Path

    original = tmp_path / "original"
    relocated = tmp_path / "relocated"
    store = MailStore(original)
    value = draft(store)
    mime = Path(value["mime_path"]).read_bytes()
    assert Path(value["mime_path"]).stat().st_mode & 0o777 == 0o600
    assert value["mime_sha256"] == hashlib.sha256(mime).hexdigest()
    store.close()
    original.rename(relocated)
    fresh = MailStore(relocated)
    try:
        recovered = fresh.get_draft(value["id"], "client")
        assert Path(recovered["mime_path"]).is_relative_to(relocated)
        assert Path(recovered["mime_path"]).read_bytes() == mime
        assert fresh.create_send("client", value["id"], None)["state"] == "queued"
    finally:
        fresh.close()


def test_recover_inflight_removes_unindexed_draft_mime(tmp_path):
    store = MailStore(tmp_path)
    orphan = store.mail_root / "drafts" / ("draft-" + "a" * 21 + "A.eml")
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"From: sender@example.test\r\n\r\nbody\r\n")
    manual = orphan.with_name("draft-important.eml")
    manual.write_bytes(b"manual file")
    unknown = orphan.with_name("draft-" + "é" * 22 + ".eml")
    unknown.write_bytes(b"unknown file")
    try:
        assert store.recover_inflight() == 1
        assert not orphan.exists()
        assert manual.read_bytes() == b"manual file"
        assert unknown.read_bytes() == b"unknown file"
    finally:
        store.close()


def test_storage_gc_exposes_only_old_unindexed_draft_mime(tmp_path):
    store = MailStore(tmp_path)
    orphan = store.mail_root / "drafts" / ("draft-" + "a" * 21 + "A.eml")
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"orphan")
    old = time.time() - 31 * 24 * 60 * 60
    os.utime(orphan, (old, old))
    relative = orphan.relative_to(tmp_path).as_posix()
    try:
        snapshot = store.storage_gc_snapshot()
        assert snapshot["candidates"] == [
            {
                "path": relative,
                "reason": "orphan_draft",
                "group": f"orphan:{relative}",
                "requires_tombstone": False,
            }
        ]
        assert snapshot["protected_paths"] == []
        assert store.storage_gc_before_delete(snapshot["candidates"]) == [
            relative
        ]
    finally:
        store.close()
