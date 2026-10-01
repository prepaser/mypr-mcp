"""Persistent, workspace-local timers."""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from numbers import Real
from pathlib import Path
from typing import Any

_STATES = {"scheduled", "expired", "cancelled"}
_MAX_LABEL_BYTES = 256
_MAX_NOTIFICATION_BYTES = 4 * 1024
_MAX_NOTIFICATION_ITEMS = 5


class TimerStore:
    """Store timers in the workspace history database."""

    def __init__(
        self, root: Path, *, clock: Callable[[], float] | None = None, initialize: bool = True,
    ):
        self.root = Path(root).resolve()
        self.db_path = self.root / ".mypr" / "history.sqlite3"
        if initialize:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or time.time
        self._lock = threading.RLock()
        self._closed = False
        self._db = sqlite3.connect(
            self.db_path if initialize else self.db_path.as_uri() + "?mode=rw",
            uri=not initialize,
            isolation_level=None,
            check_same_thread=False,
            timeout=30,
        )
        try:
            self._db.row_factory = sqlite3.Row
            if initialize:
                self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.execute("PRAGMA busy_timeout=30000")
            if initialize:
                self._create_schema()
        except BaseException:
            self._db.close()
            self._closed = True
            raise

    def _create_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS timers (
                    timer_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    created REAL NOT NULL,
                    due REAL NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('scheduled', 'expired', 'cancelled')),
                    acknowledged REAL
                );
                CREATE INDEX IF NOT EXISTS timers_client_seq_idx
                    ON timers(client_id, timer_seq);
                CREATE INDEX IF NOT EXISTS timers_pending_idx
                    ON timers(client_id, state, acknowledged, due, id);
                """
            )

    def start(
        self,
        client: str,
        seconds: Real | None = None,
        *,
        at: datetime | str | None = None,
        label: str = "",
    ) -> dict[str, Any]:
        """Create a one-shot timer."""
        self._ensure_open()
        client = self._validate_client(client)
        label = _validate_label(label)
        created = self._now()
        due = _due_time(seconds, at, created)
        timer_id = f"timer-{uuid.uuid4().hex}"
        state = "scheduled" if due > created else "expired"
        record = {
            "id": timer_id,
            "client_id": client,
            "label": label,
            "created_at": created,
            "due_at": due,
            "state": state,
            "remaining_seconds": max(0.0, due - created),
            "acknowledged": False,
        }
        with self._transaction():
            self._db.execute(
                "INSERT INTO timers(id, client_id, label, created, due, state) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (timer_id, client, label, created, due, state),
            )
            self._touch_activity(client, created)
        return record

    def check(self, client: str, timer_id: str) -> dict[str, Any]:
        """Return one timer and lazily update its expiry state."""
        self._ensure_open()
        client = self._validate_client(client)
        timer_id = _validate_id(timer_id)
        with self._client_transaction(client) as now:
            row = self._get_owned(timer_id, client)
            if row is None:
                raise ValueError(f"unknown timer ID: {timer_id}")
            self._touch_activity(client, now)
            return _record(row, now)

    def list(
        self,
        client: str,
        state: str | None = None,
        limit: int = 50,
        cursor: int | str | None = None,
    ) -> dict[str, Any]:
        """Return a page of a client's timers in creation order."""
        self._ensure_open()
        client = self._validate_client(client)
        state = _validate_state(state)
        limit = _validate_limit(limit)
        cursor = _validate_cursor(cursor)
        with self._transaction():
            now = self._now()
            self._expire_due(now, client)
            clauses = ["client_id = ?"]
            values: list[Any] = [client]
            if state is not None:
                clauses.append("state = ?")
                values.append(state)
            if cursor is not None:
                clauses.append("timer_seq > ?")
                values.append(cursor)
            rows = self._db.execute(
                "SELECT timer_seq, id, client_id, label, created, due, state, acknowledged "
                "FROM timers WHERE "
                + " AND ".join(clauses)
                + " ORDER BY timer_seq ASC LIMIT ?",
                (*values, limit + 1),
            ).fetchall()
            self._touch_activity(client, now)
        visible = rows[:limit]
        has_more = len(rows) > limit
        items = [_record(row, now) for row in visible]
        return {
            "items": items,
            "has_more": has_more,
            "next_cursor": int(visible[-1]["timer_seq"]) if has_more else None,
        }

    def cancel(self, client: str, timer_id: str) -> dict[str, Any]:
        """Cancel a scheduled timer and return its resulting record."""
        self._ensure_open()
        client = self._validate_client(client)
        timer_id = _validate_id(timer_id)
        with self._client_transaction(client) as now:
            row = self._get_owned(timer_id, client)
            if row is None:
                raise ValueError(f"unknown timer ID: {timer_id}")
            if row["state"] == "expired":
                raise ValueError("expired timers cannot be cancelled; acknowledge them instead")
            if row["state"] == "cancelled":
                return _record(row, now)
            self._db.execute("UPDATE timers SET state = 'cancelled' WHERE id = ?", (timer_id,))
            row = self._get_owned(timer_id, client)
            assert row is not None
            self._touch_activity(client, now)
            return _record(row, now)

    def ack(self, client: str, ids: list[str]) -> int:
        """Acknowledge expired timers atomically."""
        self._ensure_open()
        client = self._validate_client(client)
        ids = _validate_ids(ids)
        if not ids:
            return 0
        with self._client_transaction(client) as now:
            rows = self._fetch_ids(ids)
            found = {row["id"]: row for row in rows}
            missing = next((timer_id for timer_id in ids if timer_id not in found), None)
            if missing is not None:
                raise ValueError(f"unknown timer ID: {missing}")
            foreign = next((row for row in rows if row["client_id"] != client), None)
            if foreign is not None:
                raise ValueError(f"timer {foreign['id']} is owned by another client")
            pending = next(
                (row for row in rows if row["state"] != "expired" and row["acknowledged"] is None),
                None,
            )
            if pending is not None:
                raise ValueError(f"timer {pending['id']} is not expired")
            timestamp = now
            changed = 0
            for start in range(0, len(ids), 900):
                chunk = ids[start : start + 900]
                placeholders = ",".join("?" for _ in chunk)
                result = self._db.execute(
                    f"UPDATE timers SET acknowledged = ? WHERE id IN ({placeholders}) "
                    "AND acknowledged IS NULL",
                    (timestamp, *chunk),
                )
                changed += result.rowcount
            self._touch_activity(client, now)
        return changed

    def notifications(self, client: str) -> dict[str, Any] | None:
        """Return a bounded preview of a client's unacknowledged expiries."""
        self._ensure_open()
        client = self._validate_client(client)
        with self._transaction():
            now = self._now()
            self._expire_due(now, client)
            count = int(
                self._db.execute(
                    "SELECT COUNT(*) FROM timers WHERE client_id = ? AND state = 'expired' "
                    "AND acknowledged IS NULL",
                    (client,),
                ).fetchone()[0]
            )
            if count == 0:
                return None
            rows = self._db.execute(
                "SELECT id, label, due FROM timers WHERE client_id = ? AND state = 'expired' "
                "AND acknowledged IS NULL ORDER BY due ASC, id ASC LIMIT ?",
                (client, _MAX_NOTIFICATION_ITEMS),
            ).fetchall()
            self._touch_activity(client, now)
        items: list[dict[str, Any]] = []
        for row in rows:
            item = {"id": row["id"], "label": row["label"], "due_at": row["due"]}
            candidate = {
                "unacked": count,
                "items": [*items, item],
                "has_more": count > len(items) + 1,
            }
            if _json_size(candidate) > _MAX_NOTIFICATION_BYTES:
                break
            items.append(item)
        return {
            "unacked": count,
            "items": items,
            "has_more": count > len(items),
        }

    def next_deadline(self, client: str) -> float | None:
        """Return the next scheduled or unacknowledged expired deadline."""
        self._ensure_open()
        client = self._validate_client(client)
        with self._transaction():
            now = self._now()
            self._expire_due(now, client)
            scheduled = self._db.execute(
                "SELECT MIN(due) FROM timers WHERE client_id = ? AND state = 'scheduled'",
                (client,),
            ).fetchone()
            expired = self._db.execute(
                "SELECT MIN(due) FROM timers WHERE client_id = ? AND state = 'expired' "
                "AND acknowledged IS NULL",
                (client,),
            ).fetchone()
            self._touch_activity(client, now)
        deadlines: list[float] = []
        if scheduled[0] is not None:
            deadlines.append(float(scheduled[0]))
        if expired[0] is not None:
            deadlines.append(min(now, float(expired[0])))
        return min(deadlines) if deadlines else None

    @classmethod
    def snapshot_existing(cls, root: Path, client: str, *, include_deadline: bool = False):
        if not (Path(root) / ".mypr/history.sqlite3").is_file():
            return None, None
        store = cls(root, initialize=False)
        try:
            if not store._db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='timers'"
            ).fetchone():
                return None, None
            preview = store.notifications(client)
            due_at = store.next_deadline(client) if preview is None and include_deadline else None
            return preview, due_at
        finally:
            store.close()

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True

    def _get_owned(self, timer_id: str, client: str) -> sqlite3.Row | None:
        row = self._db.execute(
            "SELECT timer_seq, id, client_id, label, created, due, state, acknowledged "
            "FROM timers WHERE id = ? AND client_id = ?",
            (timer_id, client),
        ).fetchone()
        if row is not None:
            return row
        exists = self._db.execute("SELECT 1 FROM timers WHERE id = ?", (timer_id,)).fetchone()
        if exists is not None:
            raise ValueError(f"timer {timer_id} is owned by another client")
        return None

    def _fetch_ids(self, ids: list[str]) -> list[sqlite3.Row]:
        rows: list[sqlite3.Row] = []
        for start in range(0, len(ids), 900):
            chunk = ids[start : start + 900]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(
                self._db.execute(
                    "SELECT id, client_id, state, acknowledged FROM timers "
                    f"WHERE id IN ({placeholders})",
                    chunk,
                ).fetchall()
            )
        return rows

    def _expire_due(self, now: float, client: str) -> None:
        self._db.execute(
            "UPDATE timers SET state = 'expired' WHERE client_id = ? AND state = 'scheduled' "
            "AND due <= ?",
            (client, now),
        )

    def _validate_client(self, value: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError("client must be a non-empty string")
        with self._lock:
            row = self._db.execute("SELECT 1 FROM client_ids WHERE id = ?", (value,)).fetchone()
        if row is None:
            raise ValueError(f"unknown client: {value!r}")
        return value

    def _touch_activity(self, client_id: str, timestamp: float) -> None:
        self._db.execute(
            "UPDATE client_ids SET last_seen = MAX(COALESCE(last_seen, 0), ?) WHERE id = ?",
            (timestamp, client_id),
        )

    @contextmanager
    def _transaction(self):
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                try:
                    self._db.execute("COMMIT")
                except BaseException:
                    self._db.execute("ROLLBACK")
                    raise

    @contextmanager
    def _client_transaction(self, client: str):
        """Expire due timers before an operation that may fail validation."""
        with self._lock:
            with self._transaction():
                now = self._now()
                self._expire_due(now, client)
            with self._transaction():
                yield now

    def _now(self) -> float:
        now = float(self._clock())
        if not math.isfinite(now):
            raise ValueError("clock returned a non-finite timestamp")
        return now

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("timer store is closed")


def _validate_label(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("label must be a string")
    if len(value.encode("utf-8")) > _MAX_LABEL_BYTES:
        raise ValueError("label must be at most 256 bytes in UTF-8")
    return value


def _due_time(seconds: Real | None, at: datetime | str | None, now: float) -> float:
    if (seconds is None) == (at is None):
        raise ValueError("specify exactly one of seconds or at")
    if seconds is not None:
        if isinstance(seconds, bool) or not isinstance(seconds, Real):
            raise TypeError("seconds must be a finite non-negative number")
        try:
            seconds_value = float(seconds)
        except (OverflowError, ValueError) as exc:
            raise ValueError("seconds must be a finite non-negative number") from exc
        if not math.isfinite(seconds_value) or seconds_value < 0:
            raise ValueError("seconds must be a finite non-negative number")
        try:
            due = now + seconds_value
        except OverflowError as exc:
            raise ValueError("seconds must produce a finite deadline") from exc
        if not math.isfinite(due):
            raise ValueError("seconds must produce a finite deadline")
        return due
    parsed = _parse_at(at)
    return parsed.timestamp()


def _parse_at(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("at must be an ISO 8601 datetime with a timezone") from exc
    if not isinstance(value, datetime):
        raise TypeError("at must be a timezone-aware datetime or ISO 8601 string")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("at must include a timezone")
    return value.astimezone(UTC)


def _validate_id(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("timer ID must be a non-empty string")
    return value


def _validate_ids(value: list[str]) -> list[str]:
    if not isinstance(value, list):
        raise TypeError("ids must be a list of timer IDs")
    result: list[str] = []
    seen: set[str] = set()
    for ident in value:
        ident = _validate_id(ident)
        if ident not in seen:
            seen.add(ident)
            result.append(ident)
    return result


def _validate_state(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in _STATES:
        raise ValueError(f"state must be one of {sorted(_STATES)} or None")
    return value


def _validate_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    return value


def _validate_cursor(value: int | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("cursor must be a non-negative integer")
    if not isinstance(value, (int, str)):
        raise ValueError("cursor must be a non-negative integer")
    if isinstance(value, str) and (not value or not value.isdecimal()):
        raise ValueError("cursor must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("cursor must be a non-negative integer") from exc
    if not 0 <= parsed <= (1 << 63) - 1:
        raise ValueError("cursor must be a non-negative integer")
    return parsed


def _record(row: sqlite3.Row, now: float) -> dict[str, Any]:
    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "label": row["label"],
        "created_at": float(row["created"]),
        "due_at": float(row["due"]),
        "state": row["state"],
        "remaining_seconds": max(0.0, float(row["due"]) - now)
        if row["state"] == "scheduled"
        else 0.0,
        "acknowledged": row["acknowledged"] is not None,
    }


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
