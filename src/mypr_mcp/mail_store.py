"""Durable local state for the workspace mail service.

The store contains cursors, references and outbox metadata only.  Mail bodies
remain on the provider; draft MIME is kept as an immutable local file.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .storage_lock import StorageLock

MAIL_RETENTION_DAYS = 30
_DAY = 24 * 60 * 60
_UNSET = object()


class MailStore:
    def __init__(self, root: Path, *, initialize: bool = True, read_only: bool = False) -> None:
        if initialize and read_only:
            raise ValueError("read_only cannot be used while initializing MailStore")
        self.root = Path(root).expanduser().resolve()
        self.db_path = self.root / ".mypr" / "history.sqlite3"
        self.mail_root = self.root / ".mypr" / "mail"
        if initialize:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.mail_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._read_only = read_only
        self._db = sqlite3.connect(
            self.db_path
            if initialize
            else self.db_path.as_uri() + f"?mode={'ro' if read_only else 'rw'}",
            uri=not initialize,
            isolation_level=None,
            check_same_thread=False,
            timeout=30,
        )
        try:
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA busy_timeout=30000")
            if initialize:
                self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("PRAGMA synchronous=NORMAL")
                self._create_schema()
        except BaseException:
            self._db.close()
            self._closed = True
            raise

    def _create_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS mail_watches (
                    id TEXT PRIMARY KEY NOT NULL,
                    client_id TEXT NOT NULL,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    endpoint_identity TEXT NOT NULL DEFAULT '',
                    uidvalidity INTEGER,
                    last_uid INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'pending',
                    error TEXT,
                    created REAL NOT NULL,
                    updated REAL NOT NULL,
                    UNIQUE(client_id, account, mailbox)
                );
                CREATE INDEX IF NOT EXISTS mail_watches_target_idx
                    ON mail_watches(account, mailbox, state);
                CREATE TABLE IF NOT EXISTS mail_notifications (
                    notification_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL,
                    watch_id TEXT NOT NULL,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    endpoint_identity TEXT NOT NULL DEFAULT '',
                    uidvalidity INTEGER,
                    uid INTEGER,
                    kind TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created REAL NOT NULL,
                    acknowledged REAL,
                    UNIQUE(client_id, watch_id, endpoint_identity, uidvalidity, uid, kind)
                );
                CREATE INDEX IF NOT EXISTS mail_notifications_client_idx
                    ON mail_notifications(client_id, acknowledged, notification_seq);
                CREATE TABLE IF NOT EXISTS mail_refs (
                    id TEXT PRIMARY KEY NOT NULL,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    uidvalidity INTEGER NOT NULL,
                    uid INTEGER NOT NULL,
                    endpoint_identity TEXT NOT NULL DEFAULT '',
                    created REAL NOT NULL,
                    UNIQUE(account, mailbox, uidvalidity, uid, endpoint_identity)
                );
                CREATE INDEX IF NOT EXISTS mail_refs_namespace_idx
                    ON mail_refs(account, mailbox, uidvalidity, uid);
                CREATE TABLE IF NOT EXISTS mail_drafts (
                    id TEXT PRIMARY KEY NOT NULL,
                    client_id TEXT NOT NULL,
                    account TEXT NOT NULL,
                    mime_path TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS mail_drafts_client_idx
                    ON mail_drafts(client_id, created);
                CREATE TABLE IF NOT EXISTS mail_sends (
                    send_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
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
                CREATE INDEX IF NOT EXISTS mail_sends_client_idx
                    ON mail_sends(client_id, created);
                """
            )
            send_columns = {
                row[1] for row in self._db.execute("PRAGMA table_info(mail_sends)").fetchall()
            }
            if "warning" not in send_columns:
                self._db.execute("ALTER TABLE mail_sends ADD COLUMN warning TEXT")
            if "error" not in send_columns:
                self._db.execute("ALTER TABLE mail_sends ADD COLUMN error TEXT")
            if "accepted" not in send_columns:
                self._db.execute(
                    "ALTER TABLE mail_sends ADD COLUMN accepted TEXT NOT NULL DEFAULT '[]'"
                )
            if "rejected" not in send_columns:
                self._db.execute(
                    "ALTER TABLE mail_sends ADD COLUMN rejected TEXT NOT NULL DEFAULT '[]'"
                )
            if "rejected_details" not in send_columns:
                self._db.execute(
                    "ALTER TABLE mail_sends ADD COLUMN rejected_details TEXT NOT NULL DEFAULT '[]'"
                )
            if "stage" not in send_columns:
                self._db.execute("ALTER TABLE mail_sends ADD COLUMN stage TEXT")
            send_columns = {
                row[1] for row in self._db.execute("PRAGMA table_info(mail_sends)").fetchall()
            }
            if "send_seq" not in send_columns:
                self._migrate_send_seq()
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS history_meta "
                "(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)"
            )
            self._db.execute(
                "INSERT INTO history_meta(key,value) VALUES ('mail_send_seq_migrated','1') "
                "ON CONFLICT(key) DO UPDATE SET value='1'"
            )
            columns = {
                row[1] for row in self._db.execute("PRAGMA table_info(mail_refs)").fetchall()
            }
            if "endpoint_identity" not in columns:
                self._db.execute(
                    "ALTER TABLE mail_refs ADD COLUMN endpoint_identity TEXT NOT NULL DEFAULT ''"
                )

            watch_columns = {
                row[1] for row in self._db.execute("PRAGMA table_info(mail_watches)").fetchall()
            }
            if "endpoint_identity" not in watch_columns:
                self._db.execute(
                    "ALTER TABLE mail_watches ADD COLUMN endpoint_identity TEXT NOT NULL DEFAULT ''"
                )
            notification_columns = {
                row[1]
                for row in self._db.execute("PRAGMA table_info(mail_notifications)").fetchall()
            }
            if "endpoint_identity" not in notification_columns:
                self._db.execute(
                    "ALTER TABLE mail_notifications ADD COLUMN endpoint_identity "
                    "TEXT NOT NULL DEFAULT "
                    "''"
                )
            indexes = self._db.execute("PRAGMA index_list(mail_notifications)").fetchall()
            notification_unique = False
            for index in indexes:
                if not index[2]:
                    continue
                names = [
                    item[2]
                    for item in self._db.execute(f"PRAGMA index_info({index[1]!r})").fetchall()
                ]
                if names == [
                    "client_id",
                    "watch_id",
                    "endpoint_identity",
                    "uidvalidity",
                    "uid",
                    "kind",
                ]:
                    notification_unique = True
                    break
            if not notification_unique:
                self._db.executescript(
                    """
                    CREATE TABLE mail_notifications_new (
                        notification_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                        id TEXT NOT NULL UNIQUE,
                        client_id TEXT NOT NULL,
                        watch_id TEXT NOT NULL,
                        account TEXT NOT NULL,
                        mailbox TEXT NOT NULL,
                        endpoint_identity TEXT NOT NULL DEFAULT '',
                        uidvalidity INTEGER,
                        uid INTEGER,
                        kind TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        created REAL NOT NULL,
                        acknowledged REAL,
                        UNIQUE(client_id, watch_id, endpoint_identity, uidvalidity, uid, kind)
                    );
                    INSERT INTO mail_notifications_new
                        (notification_seq,id,client_id,watch_id,account,mailbox,endpoint_identity,uidvalidity,uid,kind,payload,created,acknowledged)
                    SELECT notification_seq,id,client_id,watch_id,account,mailbox,
                        COALESCE(endpoint_identity,''),uidvalidity,uid,kind,payload,
                        created,acknowledged
                    FROM mail_notifications;
                    DROP TABLE mail_notifications;
                    ALTER TABLE mail_notifications_new RENAME TO mail_notifications;
                    CREATE INDEX IF NOT EXISTS mail_notifications_client_idx
                        ON mail_notifications(client_id, acknowledged, notification_seq);
                    """
                )
            indexes = self._db.execute("PRAGMA index_list(mail_refs)").fetchall()
            legacy_unique = False
            for index in indexes:
                if not index[2]:
                    continue
                names = [
                    item[2]
                    for item in self._db.execute(f"PRAGMA index_info({index[1]!r})").fetchall()
                ]
                if names == ["account", "mailbox", "uidvalidity", "uid"]:
                    legacy_unique = True
                    break
            if legacy_unique:
                self._db.executescript(
                    """
                    CREATE TABLE mail_refs_new (
                        id TEXT PRIMARY KEY NOT NULL,
                        account TEXT NOT NULL,
                        mailbox TEXT NOT NULL,
                        uidvalidity INTEGER NOT NULL,
                        uid INTEGER NOT NULL,
                        endpoint_identity TEXT NOT NULL DEFAULT '',
                        created REAL NOT NULL,
                        UNIQUE(account, mailbox, uidvalidity, uid, endpoint_identity)
                    );
                    INSERT INTO mail_refs_new
                        SELECT id,account,mailbox,uidvalidity,uid,endpoint_identity,created
                        FROM mail_refs;
                    DROP TABLE mail_refs;
                    ALTER TABLE mail_refs_new RENAME TO mail_refs;
                    CREATE INDEX IF NOT EXISTS mail_refs_namespace_idx
                        ON mail_refs(account, mailbox, uidvalidity, uid);
                    """
                )

    def _migrate_send_seq(self) -> None:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.execute(
                """
                CREATE TABLE mail_sends_new (
                    send_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
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
                )
                """
            )
            self._db.execute(
                """
                INSERT INTO mail_sends_new(
                    send_seq,id,draft_id,client_id,request_id,state,accepted,rejected,
                    rejected_details,stage,error,warning,created,updated
                )
                SELECT rowid,id,draft_id,client_id,request_id,state,accepted,rejected,
                    rejected_details,stage,error,warning,created,updated
                FROM mail_sends ORDER BY rowid
                """
            )
            self._db.execute("DROP TABLE mail_sends")
            self._db.execute("ALTER TABLE mail_sends_new RENAME TO mail_sends")
            self._db.execute(
                "CREATE INDEX IF NOT EXISTS mail_sends_client_idx "
                "ON mail_sends(client_id, created)"
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    @property
    def available(self) -> bool:
        return not self._closed

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True

    def create_watch(
        self,
        client_id: str,
        account: str,
        mailbox: str,
        *,
        uidvalidity: int | None = None,
        last_uid: int = 0,
        endpoint_identity: str = "",
    ) -> dict[str, Any]:
        now = time.time()
        with self._tx():
            row = self._db.execute(
                "SELECT * FROM mail_watches WHERE client_id=? AND account=? AND mailbox=?",
                (client_id, account, mailbox),
            ).fetchone()
            if row is None:
                ident = "watch-" + secrets.token_urlsafe(12)
                self._db.execute(
                    (
                        "INSERT INTO "
                        "mail_watches(id,client_id,account,mailbox,endpoint_identity,"
                        "uidvalidity,last_uid,state,created,updated) "
                        "VALUES(?,?,?,?,?,?,?, 'pending', ?, "
                        "?)"
                    ),
                    (
                        ident,
                        client_id,
                        account,
                        mailbox,
                        endpoint_identity,
                        uidvalidity,
                        max(0, int(last_uid)),
                        now,
                        now,
                    ),
                )
            else:
                ident = row["id"]
                if uidvalidity is not None:
                    self._db.execute(
                        (
                            "UPDATE mail_watches SET "
                            "endpoint_identity=?,uidvalidity=?,last_uid=?,state='pending'"
                            ",error=NULL,updated=? WHERE "
                            "id=?"
                        ),
                        (
                            endpoint_identity,
                            uidvalidity,
                            max(0, int(last_uid)),
                            now,
                            ident,
                        ),
                    )
            return self._watch_row(
                self._db.execute("SELECT * FROM mail_watches WHERE id=?", (ident,)).fetchone()
            )

    def get_watch(self, watch_id: str, client_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM mail_watches WHERE id=?", (watch_id,)).fetchone()
        if row is None or client_id is not None and row["client_id"] != client_id:
            return None
        return self._watch_row(row)

    def watches(self, client_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM mail_watches"
        values: tuple[Any, ...] = ()
        if client_id is not None:
            query += " WHERE client_id=?"
            values = (client_id,)
        query += " ORDER BY created, id"
        with self._lock:
            rows = self._db.execute(query, values).fetchall()
        return [self._watch_row(row) for row in rows]

    def delete_watch(self, watch_id: str, client_id: str) -> dict[str, Any]:
        with self._tx():
            row = self._db.execute(
                "SELECT * FROM mail_watches WHERE id=? AND client_id=?", (watch_id, client_id)
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown watch ID: {watch_id}")
            self._db.execute("DELETE FROM mail_watches WHERE id=?", (watch_id,))
            return self._watch_row(row)

    def update_watch(
        self,
        watch_id: str,
        *,
        uidvalidity: int | None = None,
        last_uid: int | None = None,
        state: str | None = None,
        error: str | None | object = _UNSET,
        endpoint_identity: str | None = None,
    ) -> None:
        fields: list[str] = ["updated=?"]
        values: list[Any] = [time.time()]
        for key, value in (
            ("endpoint_identity", endpoint_identity),
            ("uidvalidity", uidvalidity),
            ("last_uid", last_uid),
            ("state", state),
            ("error", error),
        ):
            if value is _UNSET:
                continue
            if value is not None:
                fields.append(f"{key}=?")
                values.append(value)
            elif key == "error":
                fields.append("error=NULL")
        values.append(watch_id)
        with self._lock:
            self._db.execute(f"UPDATE mail_watches SET {','.join(fields)} WHERE id=?", values)

    def add_notification(
        self,
        *,
        client_id: str,
        watch_id: str,
        account: str,
        mailbox: str,
        endpoint_identity: str = "",
        uidvalidity: int | None,
        uid: int | None,
        kind: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        with self._tx():
            return self._add_notification_tx(
                client_id=client_id,
                watch_id=watch_id,
                account=account,
                mailbox=mailbox,
                endpoint_identity=endpoint_identity,
                uidvalidity=uidvalidity,
                uid=uid,
                kind=kind,
                payload=payload,
            )

    def record_watch_batch(
        self,
        watch_id: str,
        *,
        uidvalidity: int,
        uids: list[int],
        last_uid: int | None = None,
        endpoint_identity: str = "",
    ) -> list[dict[str, Any]]:
        """Commit references, notifications and a watch cursor atomically."""
        values = sorted({int(uid) for uid in uids})
        if any(uid < 1 for uid in values):
            raise ValueError("uids must be positive")
        events = [{"uid": uid, "kind": "new_mail", "payload": {}} for uid in values]
        return self.commit_watch(
            watch_id,
            uidvalidity=uidvalidity,
            last_uid=max(int(last_uid or 0), max(values, default=0)),
            endpoint_identity=endpoint_identity,
            notifications=events,
        )

    def commit_watch(
        self,
        watch_id: str,
        *,
        uidvalidity: int,
        last_uid: int,
        endpoint_identity: str,
        notifications: list[Mapping[str, Any]],
        state: str = "watching",
        error: str | None = None,
    ) -> list[dict[str, Any]]:
        if type(uidvalidity) is not int or uidvalidity < 1:
            raise ValueError("uidvalidity must be a positive integer")
        if type(last_uid) is not int or last_uid < 0:
            raise ValueError("last_uid must be non-negative")
        if not isinstance(notifications, list):
            raise TypeError("notifications must be a list")
        with self._tx():
            watch = self._db.execute(
                "SELECT * FROM mail_watches WHERE id=?", (watch_id,)
            ).fetchone()
            if watch is None:
                raise ValueError(f"unknown watch ID: {watch_id}")
            current_namespace = watch["uidvalidity"]
            if current_namespace is not None and int(current_namespace) != int(uidvalidity):
                raise ValueError("mailbox namespace changed")
            if (
                watch["endpoint_identity"]
                and endpoint_identity
                and watch["endpoint_identity"] != endpoint_identity
            ):
                raise ValueError("mail watch belongs to an outdated account endpoint")
            effective_last = max(int(watch["last_uid"]), last_uid)
            committed: list[dict[str, Any]] = []
            for event in notifications:
                if not isinstance(event, Mapping):
                    raise TypeError("watch notifications must contain objects")
                uid = event.get("uid")
                if uid is not None:
                    if type(uid) is not int or uid < 1:
                        raise ValueError("notification uid must be positive")
                    effective_last = max(effective_last, uid)
                kind = event.get("kind", "new_mail")
                payload = event.get("payload", {})
                if not isinstance(kind, str) or not kind:
                    raise ValueError("notification kind must be a non-empty string")
                if not isinstance(payload, Mapping):
                    raise TypeError("notification payload must be an object")
                payload = dict(payload)
                if uid is not None and "message_id" not in payload:
                    payload["message_id"] = self._message_ref_tx(
                        watch["account"],
                        watch["mailbox"],
                        int(uidvalidity),
                        uid,
                        endpoint_identity,
                    )
                committed.append(
                    self._add_notification_tx(
                        client_id=watch["client_id"],
                        watch_id=watch_id,
                        account=watch["account"],
                        mailbox=watch["mailbox"],
                        endpoint_identity=endpoint_identity,
                        uidvalidity=int(uidvalidity),
                        uid=uid,
                        kind=kind,
                        payload=payload,
                    )
                )
            now = time.time()
            self._db.execute(
                (
                    "UPDATE mail_watches SET "
                    "endpoint_identity=?,uidvalidity=?,last_uid=?,state=?,error=?"
                    ",updated=? WHERE "
                    "id=?"
                ),
                (
                    endpoint_identity,
                    int(uidvalidity),
                    effective_last,
                    state,
                    error,
                    now,
                    watch_id,
                ),
            )
            return committed

    def reset_watch_namespace(
        self,
        watch_id: str,
        *,
        uidvalidity: int,
        last_uid: int,
        endpoint_identity: str = "",
        message: str = "mailbox UID namespace changed; previous message references are stale",
    ) -> dict[str, Any]:
        with self._tx():
            watch = self._db.execute(
                "SELECT * FROM mail_watches WHERE id=?", (watch_id,)
            ).fetchone()
            if watch is None:
                raise ValueError(f"unknown watch ID: {watch_id}")
            now = time.time()
            self._db.execute(
                (
                    "UPDATE mail_watches SET "
                    "endpoint_identity=?,uidvalidity=?,last_uid=?,state='syncing'"
                    ",error=?,updated=? WHERE "
                    "id=?"
                ),
                (
                    endpoint_identity,
                    int(uidvalidity),
                    max(0, int(last_uid)),
                    message[:2048],
                    now,
                    watch_id,
                ),
            )
            return self._add_notification_tx(
                client_id=watch["client_id"],
                watch_id=watch_id,
                account=watch["account"],
                mailbox=watch["mailbox"],
                endpoint_identity=endpoint_identity,
                uidvalidity=int(uidvalidity),
                uid=None,
                kind="namespace_changed",
                payload={"message": message[:2048]},
            )

    def _message_ref_tx(
        self,
        account: str,
        mailbox: str,
        uidvalidity: int,
        uid: int,
        endpoint_identity: str,
    ) -> str:
        rows = self._db.execute(
            (
                "SELECT id,endpoint_identity FROM mail_refs WHERE account=? "
                "AND mailbox=? AND uidvalidity=? AND uid=? ORDER BY "
                "created"
            ),
            (account, mailbox, uidvalidity, uid),
        ).fetchall()
        for row in rows:
            if row["endpoint_identity"] == endpoint_identity:
                return str(row["id"])
        if not endpoint_identity:
            for row in rows:
                if not row["endpoint_identity"]:
                    return str(row["id"])
        ident = "msg-" + secrets.token_urlsafe(16)
        self._db.execute(
            (
                "INSERT INTO "
                "mail_refs(id,account,mailbox,uidvalidity,uid,endpoint_identi"
                "ty,created) "
                "VALUES(?,?,?,?,?,?,?)"
            ),
            (ident, account, mailbox, uidvalidity, uid, endpoint_identity, time.time()),
        )
        return ident

    def _add_notification_tx(
        self,
        *,
        client_id: str,
        watch_id: str,
        account: str,
        mailbox: str,
        endpoint_identity: str,
        uidvalidity: int | None,
        uid: int | None,
        kind: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        ident = "mail-" + secrets.token_urlsafe(12)
        encoded = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":"))
        if uid is None:
            existing = self._db.execute(
                (
                    "SELECT * FROM mail_notifications WHERE client_id=? AND "
                    "watch_id=? AND endpoint_identity=? AND uidvalidity IS ? AND "
                    "uid IS NULL AND "
                    "kind=?"
                ),
                (client_id, watch_id, endpoint_identity, uidvalidity, kind),
            ).fetchone()
            if existing is not None:
                return self._notification_row(existing)
        self._db.execute(
            (
                "INSERT OR IGNORE INTO "
                "mail_notifications(id,client_id,watch_id,account,mailbox,end"
                "point_identity,uidvalidity,uid,kind,payload,created) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)"
            ),
            (
                ident,
                client_id,
                watch_id,
                account,
                mailbox,
                endpoint_identity,
                uidvalidity,
                uid,
                kind,
                encoded,
                time.time(),
            ),
        )
        row = self._db.execute(
            (
                "SELECT * FROM mail_notifications WHERE client_id=? AND "
                "watch_id=? AND endpoint_identity=? AND uidvalidity IS ? AND "
                "uid IS ? AND "
                "kind=?"
            ),
            (client_id, watch_id, endpoint_identity, uidvalidity, uid, kind),
        ).fetchone()
        return self._notification_row(row)

    def notifications(
        self,
        client_id: str,
        *,
        limit: int = 20,
        cursor: int | None = None,
        include_acknowledged: bool = False,
    ) -> dict[str, Any]:
        limit = _limit(limit)
        clauses = ["client_id=?"]
        values: list[Any] = [client_id]
        if not include_acknowledged:
            clauses.append("acknowledged IS NULL")
        if cursor is not None:
            clauses.append("notification_seq > ?")
            values.append(int(cursor))
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM mail_notifications WHERE "
                + " AND ".join(clauses)
                + " ORDER BY notification_seq LIMIT ?",
                (*values, limit + 1),
            ).fetchall()
        items = [self._notification_row(row) for row in rows[:limit]]
        return {
            "items": items,
            "has_more": len(rows) > limit,
            "next_cursor": int(rows[limit - 1]["notification_seq"])
            if len(rows) > limit and items
            else None,
        }

    def ack_notifications(self, client_id: str, ids: list[str]) -> int:
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self._tx():
            rows = self._db.execute(
                f"SELECT id,client_id FROM mail_notifications WHERE id IN ({placeholders})",
                ids,
            ).fetchall()
            found = {row["id"]: row for row in rows}
            missing = next((ident for ident in ids if ident not in found), None)
            if missing:
                raise ValueError(f"unknown notification ID: {missing}")
            if any(row["client_id"] != client_id for row in rows):
                raise ValueError("notification belongs to another client")
            stamp = time.time()
            cursor = self._db.execute(
                "UPDATE mail_notifications SET acknowledged=? WHERE "
                "client_id=? AND acknowledged IS NULL "
                f"AND id IN ({placeholders})",
                (stamp, client_id, *ids),
            )
            return cursor.rowcount

    def message_ref(
        self, account: str, mailbox: str, uidvalidity: int, uid: int, endpoint_identity: str = ""
    ) -> str:
        with self._tx():
            return self._message_ref_tx(
                account, mailbox, int(uidvalidity), int(uid), endpoint_identity
            )

    def resolve_ref(self, message_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM mail_refs WHERE id=?", (message_id,)).fetchone()
        return dict(row) if row is not None else None

    def create_draft(
        self, client_id: str, account: str, mime_path: str, metadata: Mapping[str, Any]
    ) -> dict[str, Any]:
        ident = "draft-" + secrets.token_urlsafe(16)
        now = time.time()
        with StorageLock(self.root / ".mypr" / "storage.lock"):
            with self._tx():
                self._db.execute(
                    (
                        "INSERT INTO "
                        "mail_drafts(id,client_id,account,mime_path,metadata,created)"
                        " "
                        "VALUES(?,?,?,?,?,?)"
                    ),
                    (
                        ident,
                        client_id,
                        account,
                        mime_path,
                        json.dumps(dict(metadata), ensure_ascii=False),
                        now,
                    ),
                )
        return self.get_draft(ident, client_id)

    def persist_draft(
        self, client_id: str, account: str, mime: bytes, metadata: Mapping[str, Any]
    ) -> dict[str, Any]:
        ident = "draft-" + secrets.token_urlsafe(16)
        now = time.time()
        directory = self.mail_root / "drafts"
        directory.mkdir(parents=True, exist_ok=True)
        with StorageLock(self.root / ".mypr" / "storage.lock"):
            path = directory / f"{ident}.eml"
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(mime)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                with self._tx():
                    self._db.execute(
                        (
                            "INSERT INTO "
                            "mail_drafts(id,client_id,account,mime_path,metadata,created)"
                            " "
                            "VALUES(?,?,?,?,?,?)"
                        ),
                        (
                            ident,
                            client_id,
                            account,
                            path.relative_to(self.root).as_posix(),
                            json.dumps(
                                {**metadata, "mime_sha256": hashlib.sha256(mime).hexdigest()},
                                ensure_ascii=False,
                            ),
                            now,
                        ),
                    )
            except BaseException:
                try:
                    path.unlink()
                except OSError:
                    pass
                raise
        return self.get_draft(ident, client_id)

    def get_draft(self, draft_id: str, client_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM mail_drafts WHERE id=?", (draft_id,)).fetchone()
        if row is None or client_id is not None and row["client_id"] != client_id:
            return None
        return self._draft_row(row)

    def create_send(self, client_id: str, draft_id: str, request_id: str | None) -> dict[str, Any]:
        ident = "send-" + secrets.token_urlsafe(16)
        now = time.time()
        with StorageLock(self.root / ".mypr" / "storage.lock"):
            with self._tx():
                draft = self._db.execute(
                    "SELECT id,client_id,mime_path FROM mail_drafts WHERE id=?",
                    (draft_id,),
                ).fetchone()
                if draft is None:
                    raise ValueError(f"unknown draft ID: {draft_id}")
                if draft["client_id"] != client_id:
                    raise ValueError("draft belongs to another client")
                if not self._draft_mime_exists(draft["mime_path"]):
                    raise ValueError("draft MIME is unavailable")
                if request_id is None:
                    request_id = f"draft:{draft_id}"
                if request_id is not None:
                    old = self._db.execute(
                        "SELECT * FROM mail_sends WHERE client_id=? AND request_id=?",
                        (client_id, request_id),
                    ).fetchone()
                    if old is not None:
                        if old["draft_id"] != draft_id:
                            raise ValueError("request_id is already associated with another draft")
                        return self._send_row(old)
                existing = self._db.execute(
                    (
                        "SELECT * FROM mail_sends WHERE draft_id=? AND state IN "
                        "('queued','sending','accepted','partial','unknown') ORDER "
                        "BY created DESC LIMIT "
                        "1"
                    ),
                    (draft_id,),
                ).fetchone()
                if existing is not None:
                    raise ValueError("draft has already been submitted")
                self._db.execute(
                    (
                        "INSERT INTO "
                        "mail_sends(id,draft_id,client_id,request_id,state,created,up"
                        "dated) VALUES(?,?,?,?, 'queued', ?, "
                        "?)"
                    ),
                    (ident, draft_id, client_id, request_id, now, now),
                )
                return self._send_row(
                    self._db.execute("SELECT * FROM mail_sends WHERE id=?", (ident,)).fetchone()
                )

    def get_send(self, send_id: str, client_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM mail_sends WHERE id=?", (send_id,)).fetchone()
        if row is None or client_id is not None and row["client_id"] != client_id:
            return None
        return self._send_row(row)

    def update_send(
        self,
        send_id: str,
        *,
        state: str,
        accepted: list[str] | None = None,
        rejected: list[str] | None = None,
        rejected_details: list[Mapping[str, Any]] | None = None,
        stage: str | None = None,
        error: str | None = None,
        warning: str | None = None,
    ) -> dict[str, Any]:
        if state not in {"queued", "sending", "accepted", "partial", "failed", "unknown"}:
            raise ValueError(f"invalid send state: {state}")
        fields = ["state=?", "updated=?"]
        values: list[Any] = [state, time.time()]
        if accepted is not None:
            fields.append("accepted=?")
            values.append(json.dumps(accepted))
        if rejected is not None:
            fields.append("rejected=?")
            values.append(json.dumps(rejected))
        if rejected_details is not None:
            fields.append("rejected_details=?")
            values.append(json.dumps(rejected_details, ensure_ascii=False))
        if stage is not None:
            fields.append("stage=?")
            values.append(stage[:64])
        if error is not None:
            fields.append("error=?")
            values.append(error[:2048])
        if warning is not None:
            fields.append("warning=?")
            values.append(warning[:2048])
        values.append(send_id)
        with self._tx():
            current = self._db.execute(
                "SELECT state FROM mail_sends WHERE id=?", (send_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown send ID: {send_id}")
            if (
                current["state"] in {"accepted", "partial", "failed", "unknown"}
                and state != current["state"]
            ):
                raise ValueError(
                    f"send {send_id} is already terminal with state {current['state']}"
                )
            self._db.execute(f"UPDATE mail_sends SET {','.join(fields)} WHERE id=?", values)
            row = self._db.execute("SELECT * FROM mail_sends WHERE id=?", (send_id,)).fetchone()
        if row is None:
            raise ValueError(f"unknown send ID: {send_id}")
        return self._send_row(row)

    def sends(
        self, client_id: str, *, limit: int = 20, cursor: int | None = None
    ) -> dict[str, Any]:
        limit = _limit(limit)
        with self._lock:
            rows = self._db.execute(
                (
                    "SELECT send_seq AS seq,* FROM mail_sends WHERE client_id=? AND "
                    "(? IS NULL OR send_seq>?) ORDER BY send_seq LIMIT "
                    "?"
                ),
                (client_id, cursor, cursor, limit + 1),
            ).fetchall()
        items = [self._send_row(row) for row in rows[:limit]]
        return {
            "items": items,
            "has_more": len(rows) > limit,
            "next_cursor": int(rows[limit - 1]["seq"]) if len(rows) > limit and items else None,
        }

    def sends_for_status(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM mail_sends WHERE state IN "
                "('queued','sending','partial','unknown') ORDER BY updated "
                "DESC LIMIT "
                "100"
            ).fetchall()
        return [self._send_row(row) for row in rows]

    def queued_sends(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM mail_sends WHERE state='queued' ORDER BY created LIMIT 100"
            ).fetchall()
        return [self._send_row(row) for row in rows]

    def storage_gc_snapshot(self, *, retention_days: int = MAIL_RETENTION_DAYS) -> dict[str, Any]:
        """Return a bounded, relative-path snapshot for workspace storage GC.

        This method opens a separate connection because Storage invokes it from
        its worker thread while the service's connection belongs to the
        persistence worker.
        """
        _retention_days(retention_days)
        try:
            fresh = type(self)(self.root, initialize=False, read_only=True)
        except (OSError, sqlite3.Error) as exc:
            return {
                "protected_paths": self._mail_file_paths(),
                "candidates": [],
                "error": f"mail store unavailable: {type(exc).__name__}",
            }
        try:
            return fresh._storage_gc_snapshot_local(retention_days)
        finally:
            fresh.close()

    def storage_gc_before_delete(self, candidates: list[Mapping[str, Any]]) -> list[str]:
        """Revalidate eligible MIME paths while StorageLock is held."""
        try:
            fresh = type(self)(self.root, initialize=False, read_only=True)
        except OSError, sqlite3.Error:
            return []
        try:
            eligible = fresh._eligible_draft_paths(MAIL_RETENTION_DAYS)
            return [
                path
                for item in candidates
                if isinstance(item, Mapping)
                and isinstance(path := item.get("path"), str)
                and path in eligible
            ]
        finally:
            fresh.close()

    def storage_gc_after_delete(
        self, deleted: list[Mapping[str, Any]] | None = None
    ) -> dict[str, int]:
        """Remove terminal DB rows after MIME deletion, with a fresh connection."""
        deleted_paths = {
            item["path"]
            for item in deleted or ()
            if isinstance(item, Mapping) and isinstance(item.get("path"), str)
        }
        try:
            fresh = type(self)(self.root, initialize=False)
        except OSError, sqlite3.Error:
            return {"drafts": 0, "sends": 0, "notifications": 0}
        try:
            with StorageLock(self.root / ".mypr" / "storage.lock"):
                return fresh._storage_gc_cleanup(deleted_paths, MAIL_RETENTION_DAYS)
        finally:
            fresh.close()

    def prune(self, retention_days: int = MAIL_RETENTION_DAYS) -> dict[str, int]:
        """Prune eligible terminal records and acknowledged notifications."""
        _retention_days(retention_days)
        try:
            fresh = type(self)(self.root, initialize=False)
        except OSError, sqlite3.Error:
            return {"drafts": 0, "sends": 0, "notifications": 0}
        try:
            with StorageLock(self.root / ".mypr" / "storage.lock"):
                return fresh._storage_gc_cleanup(set(), retention_days)
        finally:
            fresh.close()

    def _storage_gc_snapshot_local(self, retention_days: int) -> dict[str, Any]:
        files = self._mail_file_paths()
        protected = set(files)
        candidates: list[dict[str, Any]] = []
        try:
            rows = self._db.execute(
                "SELECT id,mime_path,created FROM mail_drafts ORDER BY created LIMIT 4097"
            ).fetchall()
            sends = self._db.execute(
                "SELECT draft_id,state,updated FROM mail_sends ORDER BY updated LIMIT 16385"
            ).fetchall()
        except sqlite3.Error as exc:
            return {
                "protected_paths": files,
                "candidates": [],
                "error": f"mail snapshot query failed: {type(exc).__name__}",
            }
        if len(rows) > 4096 or len(sends) > 16384:
            return {
                "protected_paths": files,
                "candidates": [],
                "truncated": True,
                "error": "mail snapshot was truncated",
            }
        by_draft: dict[str, list[tuple[str, float]]] = {}
        for row in sends:
            by_draft.setdefault(str(row["draft_id"]), []).append(
                (str(row["state"]), float(row["updated"] or 0))
            )
        cutoff = time.time() - retention_days * _DAY
        for row in rows:
            path = self._mail_relative_path(row["mime_path"])
            if path is None or path not in protected:
                continue
            history = by_draft.get(str(row["id"]), [])
            if not history or any(
                state not in {"accepted", "failed", "partial"} for state, _ in history
            ):
                continue
            latest = max(float(row["created"] or 0), *(stamp for _, stamp in history))
            if latest > cutoff:
                continue
            protected.remove(path)
            candidates.append(
                {
                    "path": path,
                    "reason": "terminal_send",
                    "group": f"draft:{row['id']}",
                    "requires_tombstone": True,
                }
            )
        return {"protected_paths": sorted(protected), "candidates": candidates}

    def _eligible_draft_paths(self, retention_days: int) -> set[str]:
        snapshot = self._storage_gc_snapshot_local(retention_days)
        return {
            item["path"]
            for item in snapshot.get("candidates", ())
            if isinstance(item, Mapping) and isinstance(item.get("path"), str)
        }

    def _storage_gc_cleanup(self, deleted_paths: set[str], retention_days: int) -> dict[str, int]:
        cutoff = time.time() - retention_days * _DAY
        drafts = sends = notifications = 0
        with self._tx():
            if deleted_paths:
                rows = self._db.execute("SELECT id,mime_path,created FROM mail_drafts").fetchall()
                for row in rows:
                    path = self._mail_relative_path(row["mime_path"])
                    if path not in deleted_paths:
                        continue
                    send_rows = self._db.execute(
                        "SELECT id,state,updated FROM mail_sends WHERE draft_id=?",
                        (row["id"],),
                    ).fetchall()
                    if not send_rows or any(
                        item["state"] not in {"accepted", "failed", "partial"} for item in send_rows
                    ):
                        continue
                    latest = max(
                        float(row["created"] or 0),
                        *(float(item["updated"] or 0) for item in send_rows),
                    )
                    if latest > cutoff:
                        continue
                    cursor = self._db.execute(
                        "DELETE FROM mail_sends WHERE draft_id=?",
                        (row["id"],),
                    )
                    sends += cursor.rowcount
                    cursor = self._db.execute(
                        "DELETE FROM mail_drafts WHERE id=?",
                        (row["id"],),
                    )
                    drafts += cursor.rowcount
            cursor = self._db.execute(
                (
                    "DELETE FROM mail_notifications WHERE acknowledged IS NOT "
                    "NULL AND acknowledged <= "
                    "?"
                ),
                (cutoff,),
            )
            notifications = cursor.rowcount
        return {"drafts": drafts, "sends": sends, "notifications": notifications}

    def _mail_file_paths(self) -> list[str]:
        if not self.mail_root.is_dir():
            return []
        paths: list[str] = []
        for directory, dirnames, filenames in os.walk(self.mail_root, followlinks=False):
            dirnames[:] = [name for name in dirnames if not (Path(directory) / name).is_symlink()]
            for name in filenames:
                path = Path(directory) / name
                try:
                    path.lstat()
                except OSError:
                    continue
                if not path.is_file() or path.is_symlink():
                    continue
                relative = self._mail_relative_path(path)
                if relative is not None:
                    paths.append(relative)
        return sorted(paths)

    def _mail_relative_path(self, value: Any) -> str | None:
        if not isinstance(value, (str, os.PathLike)):
            return None
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = self.root / path
        try:
            resolved = path.resolve(strict=False)
            relative = resolved.relative_to(self.root).as_posix()
            resolved.relative_to(self.mail_root)
        except OSError, ValueError:
            return None
        return relative if relative.startswith(".mypr/mail/") else None

    def _draft_mime_exists(self, value: Any) -> bool:
        if self._mail_relative_path(value) is None:
            return False
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = self.root / path
        try:
            info = path.lstat()
        except OSError:
            return False
        return path.is_file() and not path.is_symlink() and info.st_size <= 25 * 1024 * 1024

    def recover_inflight(self) -> int:
        with self._tx():
            cursor = self._db.execute(
                (
                    "UPDATE mail_sends SET state='unknown',error='mail service "
                    "restarted during send',updated=? WHERE "
                    "state='sending'"
                ),
                (time.time(),),
            )
            unknown = cursor.rowcount
            cursor = self._db.execute(
                (
                    "UPDATE mail_sends SET state='failed',error='mail service "
                    "restarted before send began',updated=? WHERE "
                    "state='queued'"
                ),
                (time.time(),),
            )
            return unknown + cursor.rowcount

    def snapshot(self, client_id: str, *, offline: bool = False) -> dict[str, Any] | None:
        if self._closed:
            return None
        notifications = self.notifications(client_id, limit=5)
        with self._lock:
            unacked = int(
                self._db.execute(
                    (
                        "SELECT COUNT(*) FROM mail_notifications WHERE client_id=? "
                        "AND acknowledged IS "
                        "NULL"
                    ),
                    (client_id,),
                ).fetchone()[0]
            )
            if offline:
                watch_rows = self._db.execute(
                    "SELECT * FROM mail_watches WHERE client_id=? ORDER BY updated DESC LIMIT 6",
                    (client_id,),
                ).fetchall()
            else:
                watch_rows = self._db.execute(
                    (
                        "SELECT * FROM mail_watches WHERE client_id=? AND state IN "
                        "('error','offline','syncing','pending') ORDER BY updated "
                        "DESC LIMIT "
                        "6"
                    ),
                    (client_id,),
                ).fetchall()
            send_rows = self._db.execute(
                (
                    "SELECT * FROM mail_sends WHERE client_id=? AND state IN "
                    "('queued','sending','unknown','partial') ORDER BY updated "
                    "DESC LIMIT "
                    "4"
                ),
                (client_id,),
            ).fetchall()
        watches = []
        for row in watch_rows:
            watch = self._watch_row(row)
            if offline:
                watch["state"] = "offline"
                watch["error"] = watch["error"] or "mail service is offline"
            watches.append(watch)
        sends = [self._send_row(row) for row in send_rows]
        issue = bool(watches)
        pending_send = bool(sends)
        pending = bool(unacked or pending_send)
        if not issue and not pending:
            return None
        preview = {
            "unacked": unacked,
            "items": notifications["items"][:5],
            "has_more": notifications["has_more"],
            "watches": [_compact_watch(watch) for watch in watches[:5]],
            "sends": [_compact_send(send) for send in sends[:3]],
            "watches_more": len(watches) > 5,
            "sends_more": len(sends) > 3,
        }
        while len(_json_bytes(preview)) > 4096 and preview["items"]:
            preview["items"].pop()
            preview["has_more"] = True
        while len(_json_bytes(preview)) > 4096 and preview["sends"]:
            preview["sends"].pop()
            preview["sends_more"] = True
        while len(_json_bytes(preview)) > 4096 and preview["watches"]:
            preview["watches"].pop()
            preview["watches_more"] = True
        if len(_json_bytes(preview)) > 4096:
            preview = {
                "unacked": unacked,
                "has_more": True,
                "watches": [],
                "sends": [],
                "watches_more": len(watches) > 0,
                "sends_more": len(sends) > 0,
            }
        return preview

    @classmethod
    def snapshot_existing(
        cls, root: Path, client_id: str, *, offline: bool = True
    ) -> dict[str, Any] | None:
        path = Path(root).expanduser() / ".mypr" / "history.sqlite3"
        if not path.is_file():
            return None
        try:
            store = cls(root, initialize=False, read_only=True)
        except OSError, sqlite3.Error:
            return None
        try:
            exists = store._db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mail_watches'"
            ).fetchone()
            if exists is None:
                return None
            snapshot = store.snapshot(client_id, offline=offline)
            return snapshot
        finally:
            store.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("mail store is closed")

    def _tx(self):
        return _Transaction(self)

    @staticmethod
    def _watch_row(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            raise ValueError("watch no longer exists")
        return {
            "id": row["id"],
            "client_id": row["client_id"],
            "account": row["account"],
            "mailbox": row["mailbox"],
            "endpoint_identity": row["endpoint_identity"],
            "uidvalidity": row["uidvalidity"],
            "last_uid": row["last_uid"],
            "state": row["state"],
            "error": row["error"],
            "created_at": row["created"],
            "updated_at": row["updated"],
        }

    @staticmethod
    def _notification_row(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            raise ValueError("notification no longer exists")
        payload = json.loads(row["payload"])
        return {
            "id": row["id"],
            "client_id": row["client_id"],
            "watch_id": row["watch_id"],
            "account": row["account"],
            "mailbox": row["mailbox"],
            "endpoint_identity": row["endpoint_identity"],
            "uidvalidity": row["uidvalidity"],
            "uid": row["uid"],
            "kind": row["kind"],
            **payload,
            "created_at": row["created"],
            "acknowledged": row["acknowledged"] is not None,
        }

    def _draft_row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        path = Path(row["mime_path"])
        if not path.is_absolute():
            path = self.root / path
        return {
            "id": row["id"],
            "client_id": row["client_id"],
            "account": row["account"],
            "mime_path": str(path),
            **json.loads(row["metadata"]),
            "created_at": row["created"],
        }

    @staticmethod
    def _send_row(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            raise ValueError("send record no longer exists")
        return {
            "id": row["id"],
            "draft_id": row["draft_id"],
            "client_id": row["client_id"],
            "request_id": row["request_id"],
            "state": row["state"],
            "stage": row["stage"],
            "accepted": json.loads(row["accepted"]),
            "rejected": json.loads(row["rejected"]),
            "rejected_details": json.loads(row["rejected_details"]),
            "error": row["error"],
            "warning": row["warning"],
            "created_at": row["created"],
            "updated_at": row["updated"],
        }


class _Transaction:
    def __init__(self, store: MailStore) -> None:
        self.store = store

    def __enter__(self):
        self.store._ensure_open()
        self.store._lock.acquire()
        try:
            self.store._db.execute("BEGIN IMMEDIATE")
        except BaseException:
            self.store._lock.release()
            raise
        return self.store

    def __exit__(self, typ, value, tb):
        try:
            try:
                self.store._db.execute("ROLLBACK" if typ else "COMMIT")
            except BaseException:
                if self.store._db.in_transaction:
                    self.store._db.rollback()
                raise
        finally:
            self.store._lock.release()
        return False


def _limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("limit must be a positive integer")
    return min(value, 100)


def _compact_watch(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: (str(item)[:512] if key == "error" and item is not None else item)
        for key, item in value.items()
        if key in {"id", "account", "mailbox", "state", "error"}
    }


def _compact_send(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value[key]
        for key in ("id", "draft_id", "state", "error", "warning", "created_at", "updated_at")
        if key in value
    }


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _retention_days(value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("retention_days must be a positive integer")
    return value
