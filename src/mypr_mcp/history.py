"""Persistent workspace execution and connection history.

The manager is asynchronous, but history operations are intentionally small,
synchronous SQLite transactions.  A single manager owns a History instance,
so the connection is kept simple while the database remains usable by a
second process during recovery or inspection.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .client_ids import ADJECTIVES, ANIMALS

_KINDS = {"execution", "python", "shell", "package", "scan"}
_ACTIVE_STATES = {"queued", "running", "cancelling"}
_MAX_PAYLOAD_BYTES = 64 * 1024


def _python_history_id(record: Mapping[str, Any]) -> str | None:
    if record.get("kind") == "python" and record.get("generation") and record.get("id"):
        return str(record.get("history_id") or f"python:{record['generation']}:{record['id']}")
    return record.get("history_id")


def _python_history_key(value: str) -> tuple[str, str] | None:
    parts = value.split(":", 2)
    if len(parts) == 3 and parts[0] == "python" and all(parts[1:]):
        return parts[1], parts[2]
    return None


class History:
    """Store workspace entities and an append-only event log in SQLite."""

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
                CREATE TABLE IF NOT EXISTS client_ids (
                    id TEXT PRIMARY KEY NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entities (
                    entity_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    created REAL NOT NULL,
                    updated REAL NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS entities_kind_idx
                    ON entities(kind, entity_seq DESC);
                CREATE INDEX IF NOT EXISTS entities_public_id_idx ON entities(
                    json_extract(data, '$.id'), entity_seq DESC
                );
                CREATE INDEX IF NOT EXISTS execution_request_idx ON entities(
                    COALESCE(json_extract(data, '$.client_id'), json_extract(data, '$.client')),
                    json_extract(data, '$.request_id'), entity_seq
                ) WHERE kind = 'execution';
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    time REAL NOT NULL,
                    event TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    id TEXT,
                    client_id TEXT,
                    connection_id TEXT,
                    exec_id TEXT,
                    state TEXT,
                    error TEXT,
                    data TEXT
                );
                CREATE INDEX IF NOT EXISTS events_client_idx
                    ON events(client_id, seq);
                INSERT OR IGNORE INTO client_ids(id)
                    SELECT client_id FROM events WHERE client_id IS NOT NULL;
                INSERT OR IGNORE INTO client_ids(id)
                    SELECT json_extract(data, '$.client_id') FROM entities
                    WHERE json_extract(data, '$.client_id') IS NOT NULL;
                INSERT OR IGNORE INTO client_ids(id)
                    SELECT json_extract(data, '$.client') FROM entities
                    WHERE json_extract(data, '$.client') IS NOT NULL;
                """
            )

    def allocate_client_id(self) -> str:
        """Reserve a readable ID permanently, including connections that do no work."""
        self._ensure_open()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                used = {row[0] for row in self._db.execute("SELECT id FROM client_ids")}
                for _ in range(64):
                    client_id = f"{secrets.choice(ADJECTIVES)}-{secrets.choice(ANIMALS)}"
                    if client_id not in used:
                        break
                else:
                    available = [
                        name
                        for adjective in ADJECTIVES
                        for animal in ANIMALS
                        if (name := f"{adjective}-{animal}") not in used
                    ]
                    if not available:
                        raise RuntimeError("No unused client IDs remain in this workspace")
                    client_id = secrets.choice(available)
                self._db.execute("INSERT INTO client_ids(id) VALUES (?)", (client_id,))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return client_id

    def reserve_client_id(self, client_id: str) -> None:
        """Reserve a caller-selected ID, or retain its existing reservation."""
        self._ensure_open()
        with self._lock:
            self._remember_client(client_id)

    def _remember_client(self, value: Any) -> None:
        if value is not None:
            self._db.execute("INSERT OR IGNORE INTO client_ids(id) VALUES (?)", (str(value),))

    def record(
        self,
        kind: str,
        record: dict[str, Any],
        event: str | None = None,
        *,
        entity_id: str | None = None,
    ) -> dict[str, Any]:
        """Merge an entity update and optionally append its lifecycle event."""
        self._ensure_open()
        self._validate_kind(kind)
        if not isinstance(record, dict):
            raise TypeError("record must be a dict")
        ident = record.get("id")
        if not isinstance(ident, str) or not ident:
            raise ValueError("record.id must be a non-empty string")
        supplied_kind = record.get("kind")
        if supplied_kind is not None and supplied_kind != kind:
            raise ValueError(f"record kind {supplied_kind!r} does not match {kind!r}")
        storage_id = ident if entity_id is None else entity_id
        if not isinstance(storage_id, str) or not storage_id:
            raise ValueError("entity_id must be a non-empty string")
        if storage_id != ident:
            record = {**record, "history_id": storage_id}
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    "SELECT entity_seq, kind, data FROM entities WHERE id = ?", (storage_id,)
                ).fetchone()
                if row is None:
                    merged = dict(record)
                    merged.setdefault("id", ident)
                    merged.setdefault("kind", kind)
                    self._db.execute(
                        "INSERT INTO entities(id, kind, created, updated, data) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (storage_id, kind, now, now, _dump(merged)),
                    )
                else:
                    if row["kind"] != kind:
                        raise ValueError(f"entity {storage_id!r} already has kind {row['kind']!r}")
                    previous = _load(row["data"])
                    merged = {**previous, **record}
                    merged["id"] = ident
                    merged.setdefault("kind", kind)
                    self._db.execute(
                        "UPDATE entities SET updated = ?, data = ? WHERE id = ?",
                        (now, _dump(merged), storage_id),
                    )
                self._remember_client(merged.get("client_id"))
                self._remember_client(merged.get("client"))
                result = dict(merged)
                if event is not None:
                    history_id = _python_history_id(result)
                    data = (
                        {"history_id": history_id, "generation": result.get("generation")}
                        if history_id is not None
                        else None
                    )
                    self._insert_event(event, kind, result, now=now, data=data)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return result

    def append(self, kind: str, event: str, data: Any) -> dict[str, Any]:
        """Append a bounded event payload and return its materialized event."""
        return self.append_many(kind, event, [data])[0]

    def append_many(self, kind: str, event: str, items: list[Any]) -> list[dict[str, Any]]:
        """Commit an ordered batch of bounded events in one transaction."""
        self._ensure_open()
        if not isinstance(kind, str) or not kind:
            raise ValueError("kind must be a non-empty string")
        if not isinstance(event, str) or not event:
            raise ValueError("event must be a non-empty string")
        rows = []
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for data in items:
                    metadata = data if isinstance(data, Mapping) else {}
                    seq = self._insert_event(
                        event, kind, metadata, now=time.time(), data=_bounded(data)
                    )
                    rows.append(
                        self._db.execute("SELECT * FROM events WHERE seq = ?", (seq,)).fetchone()
                    )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return [_event(row) for row in rows]

    def find_request(self, client_id: str, request_id: str) -> dict[str, Any] | None:
        self._ensure_open()
        with self._lock:
            row = self._db.execute(
                "SELECT data FROM entities WHERE kind = 'execution' AND "
                "COALESCE(json_extract(data, '$.client_id'), json_extract(data, '$.client')) = ? "
                "AND json_extract(data, '$.request_id') = ? ORDER BY entity_seq LIMIT 1",
                (client_id, request_id),
            ).fetchone()
        return _load(row["data"]) if row else None

    def list(
        self,
        client_id: str | None = None,
        limit: int = 20,
        cursor: int | str | None = None,
    ) -> dict[str, Any]:
        """Return newest-first entity summaries with row-sequence pagination."""
        self._ensure_open()
        limit = _limit(limit)
        cursor_value = _cursor(cursor, allow_zero=True)
        params: list[Any] = []
        clauses: list[str] = []
        if client_id is not None:
            if not isinstance(client_id, str):
                raise TypeError("client_id must be a string or None")
            clauses.append(
                "(json_extract(data, '$.client_id') = ? OR json_extract(data, '$.client') = ?)"
            )
            params.append(client_id)
            params.append(client_id)
        if cursor_value is not None:
            clauses.append("entity_seq < ?")
            params.append(cursor_value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._db.execute(
                f"SELECT entity_seq, data FROM entities{where} ORDER BY entity_seq DESC LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = []
        for row in visible:
            item = _load(row["data"])
            item.pop("code", None)
            item.pop("output", None)
            item.pop("events", None)
            history_id = _python_history_id(item)
            if history_id is not None:
                item["history_id"] = history_id
            items.append(item)
        next_cursor = int(visible[-1]["entity_seq"]) if has_more and visible else None
        return {"items": items, "next_cursor": next_cursor}

    def get(self, ident: str) -> dict[str, Any] | None:
        """Return a complete entity, including code and output when present."""
        self._ensure_open()
        if not isinstance(ident, str) or not ident:
            raise ValueError("id must be a non-empty string")
        with self._lock:
            python_key = _python_history_key(ident)
            if python_key is not None:
                generation, task_id = python_key
                row = self._db.execute(
                    "SELECT data FROM entities "
                    "WHERE kind = 'python' "
                    "AND json_extract(data, '$.id') = ? "
                    "AND json_extract(data, '$.generation') = ? "
                    "ORDER BY entity_seq DESC LIMIT 1",
                    (task_id, generation),
                ).fetchone()
                if row is not None:
                    result = _load(row["data"])
                    result["history_id"] = _python_history_id(result) or ident
                    return result
            row = self._db.execute("SELECT data FROM entities WHERE id = ?", (ident,)).fetchone()
            if row is not None:
                exact = _load(row["data"])
                if exact.get("history_id") == ident:
                    return exact
            row = self._db.execute(
                "SELECT data FROM entities "
                "WHERE json_extract(data, '$.id') = ? "
                "ORDER BY entity_seq DESC LIMIT 1",
                (ident,),
            ).fetchone()
        if row is None:
            return None
        result = _load(row["data"])
        history_id = _python_history_id(result)
        if history_id is not None:
            result["history_id"] = history_id
        return result

    def logs(
        self,
        cursor: int | str | None = None,
        client_id: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Read the event log.

        A missing cursor returns the tail in chronological order.  A supplied
        cursor streams forward and advances over scanned non-matching events,
        which makes client-filtered polling safe when the workspace is busy.
        """
        self._ensure_open()
        limit = _limit(limit)
        cursor_value = _cursor(cursor, allow_zero=True)
        if client_id is not None and not isinstance(client_id, str):
            raise TypeError("client_id must be a string or None")
        with self._lock:
            high = int(self._db.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()[0])
            if cursor_value is None:
                if client_id is None:
                    rows = self._db.execute(
                        "SELECT * FROM events WHERE seq <= ? ORDER BY seq DESC LIMIT ?",
                        (high, limit),
                    ).fetchall()
                else:
                    rows = self._db.execute(
                        "SELECT * FROM events WHERE client_id = ? AND seq <= ? "
                        "ORDER BY seq DESC LIMIT ?",
                        (client_id, high, limit),
                    ).fetchall()
                rows.reverse()
                next_cursor = high
            else:
                where = "seq > ? AND seq <= ?"
                params = [cursor_value, high]
                if client_id is not None:
                    where += " AND client_id = ?"
                    params.append(client_id)
                rows = self._db.execute(
                    f"SELECT * FROM events WHERE {where} ORDER BY seq LIMIT ?", (*params, limit)
                ).fetchall()
                next_cursor = (
                    int(rows[-1]["seq"]) if len(rows) == limit else max(cursor_value, high)
                )
        return {"events": [_event(row) for row in rows], "cursor": next_cursor}

    def recover(self) -> int:
        """Mark unfinished entities lost after manager startup."""
        return self.mark_active(error="Manager stopped before completion")

    def mark_active(
        self,
        kind: str | None = None,
        state: str = "lost",
        error: str | None = None,
    ) -> int:
        """Transition active entities and append one event for each update."""
        self._ensure_open()
        if kind is not None:
            self._validate_kind(kind)
        if not isinstance(state, str) or not state:
            raise ValueError("state must be a non-empty string")
        with self._lock:
            params: list[Any] = [*sorted(_ACTIVE_STATES)]
            where = "json_extract(data, '$.state') IN (?, ?, ?)"
            if kind is not None:
                where += " AND kind = ?"
                params.append(kind)
            rows = self._db.execute(
                f"SELECT id, kind, data FROM entities WHERE {where}", params
            ).fetchall()
            count = 0
            for row in rows:
                record = _load(row["data"])
                record.update(state=state, finished=time.time())
                if error is None:
                    record.pop("error", None)
                else:
                    record["error"] = error
                self.record(row["kind"], record, event=state, entity_id=row["id"])
                count += 1
        return count

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True

    def _insert_event(
        self,
        event: str,
        kind: str,
        record: Mapping[str, Any],
        *,
        now: float,
        data: Any = None,
    ) -> int:
        self._remember_client(record.get("client_id"))
        ident = _optional_string(record.get("id"))
        exec_id = _optional_string(record.get("exec_id")) or ident
        values = (
            now,
            event,
            kind,
            ident,
            _optional_string(record.get("client_id")),
            _optional_string(record.get("connection_id")),
            exec_id,
            _optional_string(record.get("state")),
            _optional_string(record.get("error")),
            _dump(data) if data is not None else None,
        )
        cursor = self._db.execute(
            """
            INSERT INTO events(
                time, event, kind, id, client_id, connection_id,
                exec_id, state, error, data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        return int(cursor.lastrowid)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("history is closed")

    @staticmethod
    def _validate_kind(kind: str) -> None:
        if not isinstance(kind, str) or kind not in _KINDS:
            raise ValueError(f"kind must be one of {sorted(_KINDS)}")


def _limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 200:
        raise ValueError("limit must be an integer between 1 and 200")
    return value


def _cursor(value: int | str | None, *, allow_zero: bool) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("cursor must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("cursor must be a non-negative integer") from exc
    if parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError("cursor must be a positive integer")
    return parsed


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _load(value: str) -> dict[str, Any]:
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("history entity data must be an object")
    return loaded


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _bounded(value: Any) -> Any:
    """Keep event payloads bounded while retaining useful raw output text."""
    safe = json.loads(_dump(value))
    if len(_dump(safe).encode()) <= _MAX_PAYLOAD_BYTES:
        return safe
    if isinstance(safe, dict):
        result = dict(safe)
        result["truncated"] = True
        for key in ("output", "text", "stdout", "stderr", "raw"):
            value = result.get(key)
            if not isinstance(value, str):
                continue
            without_value = {k: v for k, v in result.items() if k != key}
            room = max(0, _MAX_PAYLOAD_BYTES - len(_dump(without_value).encode()) - 64)
            encoded = value.encode("utf-8")[:room]
            result[key] = encoded.decode("utf-8", "ignore")
            if len(_dump(result).encode()) <= _MAX_PAYLOAD_BYTES:
                return result
        return {"truncated": True}
    return {"truncated": True}


def _event(row: sqlite3.Row) -> dict[str, Any]:
    result: dict[str, Any] = {
        "seq": int(row["seq"]),
        "time": row["time"],
        "event": row["event"],
        "kind": row["kind"],
        "id": row["id"],
        "client_id": row["client_id"],
        "connection_id": row["connection_id"],
        "exec_id": row["exec_id"],
        "state": row["state"],
    }
    if row["error"] is not None:
        result["error"] = row["error"]
    if row["data"] is not None:
        result["data"] = json.loads(row["data"])
    return result
