"""Persistent, workspace-local messages between initialized clients."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_MAX_TEXT_BYTES = 16 * 1024
_MAX_READ_BYTES = 32 * 1024
_MAX_INBOX_BYTES = 4 * 1024
_MAX_INBOX_MESSAGES = 5


class MessageStore:
    """Store and deliver messages using the workspace history database."""

    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self.db_path = self.root / ".mypr" / "history.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._db = sqlite3.connect(
            self.db_path,
            isolation_level=None,
            check_same_thread=False,
            timeout=30,
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA busy_timeout=30000")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created REAL NOT NULL,
                    acknowledged REAL
                );
                CREATE INDEX IF NOT EXISTS messages_recipient_idx
                    ON messages(recipient, acknowledged, id);
                """
            )

    def send(self, sender: str, to: str, text: str) -> dict[str, Any]:
        """Send a message to a registered client."""
        self._ensure_open()
        sender = self._validate_client(sender, "sender")
        recipient = self._validate_client(to, "recipient")
        text = self._validate_text(text)
        created = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    "INSERT INTO messages(sender, recipient, text, created) VALUES (?, ?, ?, ?)",
                    (sender, recipient, text, created),
                )
                message = {
                    "id": int(cursor.lastrowid),
                    "from": sender,
                    "to": recipient,
                    "text": text,
                    "created_at": created,
                }
                if _page_size([message]) > _MAX_READ_BYTES:
                    raise ValueError("message is too large to return")
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return message

    def read(
        self,
        recipient: str,
        limit: int = 20,
        after: int | None = None,
    ) -> dict[str, Any]:
        """Return unacknowledged messages in ascending ID order."""
        self._ensure_open()
        recipient = self._validate_client(recipient, "recipient")
        limit = _validate_limit(limit)
        after = _validate_after(after)
        params: list[Any] = [recipient]
        where = "recipient = ? AND acknowledged IS NULL"
        if after is not None:
            where += " AND id > ?"
            params.append(after)
        with self._lock:
            rows = self._db.execute(
                f"SELECT id, sender, recipient, text, created FROM messages "
                f"WHERE {where} ORDER BY id ASC LIMIT ?",
                (*params, limit + 1),
            ).fetchall()

        messages: list[dict[str, Any]] = []
        cap_reached = False
        for row in rows[:limit]:
            message = _message(row)
            candidate = [*messages, message]
            if _page_size(candidate) > _MAX_READ_BYTES:
                cap_reached = True
                break
            messages.append(message)
        has_more = cap_reached or len(rows) > len(messages)
        return _page(messages, has_more)

    def ack(self, recipient: str, ids: list[int]) -> int:
        """Acknowledge the supplied messages atomically."""
        self._ensure_open()
        recipient = self._validate_client(recipient, "recipient")
        values = _validate_ids(ids)
        if not values:
            return 0
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                rows = _fetch_ids(self._db, values)
                found = {int(row["id"]): row for row in rows}
                missing = [ident for ident in values if ident not in found]
                if missing:
                    raise ValueError(f"unknown message ID: {missing[0]}")
                foreign = next(
                    (row for row in found.values() if row["recipient"] != recipient), None
                )
                if foreign is not None:
                    raise ValueError(f"message {foreign['id']} is addressed to another client")
                acknowledged = time.time()
                changed = 0
                for start in range(0, len(values), 900):
                    chunk = values[start : start + 900]
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = self._db.execute(
                        f"UPDATE messages SET acknowledged = ? "
                        f"WHERE id IN ({placeholders}) AND acknowledged IS NULL",
                        (acknowledged, *chunk),
                    )
                    changed += cursor.rowcount
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return changed

    def inbox(self, recipient: str) -> dict[str, Any]:
        """Return a bounded preview of a client's unacknowledged messages."""
        self._ensure_open()
        recipient = self._validate_client(recipient, "recipient")
        with self._lock:
            count = int(
                self._db.execute(
                    "SELECT COUNT(*) FROM messages WHERE recipient = ? AND acknowledged IS NULL",
                    (recipient,),
                ).fetchone()[0]
            )
            rows = self._db.execute(
                "SELECT id, sender, text FROM messages "
                "WHERE recipient = ? AND acknowledged IS NULL ORDER BY id ASC LIMIT ?",
                (recipient, _MAX_INBOX_MESSAGES),
            ).fetchall()

        messages: list[dict[str, Any]] = []
        for row in rows:
            full = {
                "id": int(row["id"]),
                "from": row["sender"],
                "text": row["text"],
                "truncated": False,
            }
            if _json_size([*messages, full]) <= _MAX_INBOX_BYTES:
                messages.append(full)
                continue
            truncated = _fit_inbox_message(messages, full)
            if truncated is not None:
                messages.append(truncated)
            break
        return {"unacked": count, "messages": messages, "has_more": count > len(messages)}

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True

    def _validate_client(self, value: str, role: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{role} must be a non-empty string")
        with self._lock:
            row = self._db.execute("SELECT 1 FROM client_ids WHERE id = ?", (value,)).fetchone()
        if row is None:
            raise ValueError(f"unknown {role} client: {value!r}")
        return value

    @staticmethod
    def _validate_text(value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("text must be a string")
        if not value:
            raise ValueError("text must be a non-empty string")
        if len(value.encode("utf-8")) > _MAX_TEXT_BYTES:
            raise ValueError("text must be at most 16 KiB in UTF-8")
        return value

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("message store is closed")


def _validate_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    return value


def _validate_after(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("after must be a positive integer or None")
    return value


def _validate_ids(value: list[int]) -> list[int]:
    if not isinstance(value, list):
        raise TypeError("ids must be a list of positive integers")
    result: list[int] = []
    seen: set[int] = set()
    for ident in value:
        if isinstance(ident, bool) or not isinstance(ident, int) or ident <= 0:
            raise ValueError("ids must contain only positive integers")
        if ident not in seen:
            seen.add(ident)
            result.append(ident)
    return result


def _fetch_ids(db: sqlite3.Connection, ids: list[int]) -> list[sqlite3.Row]:
    rows: list[sqlite3.Row] = []
    for start in range(0, len(ids), 900):
        chunk = ids[start : start + 900]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            db.execute(f"SELECT id, recipient FROM messages WHERE id IN ({placeholders})", chunk)
        )
    return rows


def _message(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "from": row["sender"],
        "to": row["recipient"],
        "text": row["text"],
        "created_at": row["created"],
    }


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


def _page(messages: list[dict[str, Any]], has_more: bool) -> dict[str, Any]:
    return {
        "messages": messages,
        "next_cursor": messages[-1]["id"] if has_more and messages else None,
        "has_more": has_more,
    }


def _page_size(messages: list[dict[str, Any]]) -> int:
    return max(_json_size(_page(messages, more)) for more in (False, True))


def _fit_inbox_message(
    existing: list[dict[str, Any]], full: dict[str, Any]
) -> dict[str, Any] | None:
    text = full["text"]
    low, high = 0, len(text)
    best: dict[str, Any] | None = None
    while low <= high:
        middle = (low + high) // 2
        candidate = {**full, "text": text[:middle], "truncated": True}
        if _json_size([*existing, candidate]) <= _MAX_INBOX_BYTES:
            best = candidate
            low = middle + 1
        else:
            high = middle - 1
    return best
