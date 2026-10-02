"""Persistent workspace execution and connection history.

The manager is asynchronous, but history operations are intentionally small,
synchronous SQLite transactions.  A single manager owns a History instance,
so the connection is kept simple while the database remains usable by a
second process during recovery or inspection.
"""

from __future__ import annotations

import hashlib
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
_MAX_HISTORY_WARNINGS = 8
_MAX_WARNING_TEXT = 256


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
                    CASE WHEN json_valid(data) THEN json_extract(data, '$.id') END,
                    entity_seq DESC
                );
                CREATE INDEX IF NOT EXISTS execution_request_idx ON entities(
                    CASE WHEN json_valid(data) THEN
                        COALESCE(json_extract(data, '$.client_id'), json_extract(data, '$.client'))
                    END,
                    CASE WHEN json_valid(data) THEN json_extract(data, '$.request_id') END,
                    entity_seq
                ) WHERE kind = 'execution' AND json_valid(data);
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
                    WHERE json_valid(data)
                    AND json_extract(data, '$.client_id') IS NOT NULL;
                INSERT OR IGNORE INTO client_ids(id)
                    SELECT json_extract(data, '$.client') FROM entities
                    WHERE json_valid(data)
                    AND json_extract(data, '$.client') IS NOT NULL;
                """
            )
            columns = {row[1] for row in self._db.execute("PRAGMA table_info(client_ids)")}
            for column in ("created", "last_seen"):
                if column not in columns:
                    self._db.execute(f"ALTER TABLE client_ids ADD COLUMN {column} REAL")
            self._repair_json_indexes()

    def _repair_json_indexes(self) -> None:
        indexes = {
            row["name"]: row["sql"] or ""
            for row in self._db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'index' "
                "AND name IN ('entities_public_id_idx', 'execution_request_idx')"
            )
        }
        if "json_valid(data)" not in indexes.get("entities_public_id_idx", ""):
            self._db.execute("DROP INDEX IF EXISTS entities_public_id_idx")
            self._db.execute(
                "CREATE INDEX entities_public_id_idx ON entities("
                "CASE WHEN json_valid(data) THEN json_extract(data, '$.id') END, "
                "entity_seq DESC)"
            )
        if "json_valid(data)" not in indexes.get("execution_request_idx", ""):
            self._db.execute("DROP INDEX IF EXISTS execution_request_idx")
            self._db.execute(
                "CREATE INDEX execution_request_idx ON entities("
                "CASE WHEN json_valid(data) THEN "
                "COALESCE(json_extract(data, '$.client_id'), json_extract(data, '$.client')) END, "
                "CASE WHEN json_valid(data) THEN json_extract(data, '$.request_id') END, "
                "entity_seq) WHERE kind = 'execution' AND json_valid(data)"
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
            self._db.execute(
                "INSERT OR IGNORE INTO client_ids(id, created) VALUES (?, ?)",
                (str(value), time.time()),
            )

    def touch_client(self, client_id: str, timestamp: float | None = None) -> None:
        self._ensure_open()
        timestamp = time.time() if timestamp is None else timestamp
        with self._lock:
            self._remember_client(client_id)
            self._db.execute(
                "UPDATE client_ids SET last_seen = MAX(COALESCE(last_seen, 0), ?) WHERE id = ?",
                (timestamp, client_id),
            )

    def clients(
        self, *, prefix: str | None = None, cursor: str | None = None,
        limit: int = 50, connected: bool | None = None, active_ids=(),
    ):
        self._ensure_open()
        limit = _limit(limit)
        if prefix is not None and not isinstance(prefix, str):
            raise TypeError("prefix must be a string or None")
        if cursor is not None and not isinstance(cursor, str):
            raise TypeError("cursor must be a string or None")
        if connected is not None and type(connected) is not bool:
            raise TypeError("connected must be a boolean or None")
        clauses, values = [], []
        if prefix is not None:
            clauses.append("substr(id, 1, ?) = ?")
            values.extend((len(prefix), prefix))
        if cursor is not None:
            clauses.append("id > ?")
            values.append(cursor)
        if connected is not None:
            operator = "IN" if connected else "NOT IN"
            clauses.append(f"id {operator} (SELECT value FROM json_each(?))")
            values.append(json.dumps(sorted(active_ids)))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            rows = self._db.execute(
                "SELECT id, created, last_seen, "
                "(SELECT COUNT(*) FROM messages WHERE recipient=client_ids.id "
                "AND acknowledged IS NULL) AS unacked FROM client_ids"
                + where + " ORDER BY id LIMIT ?",
                (*values, limit + 1),
            ).fetchall()
        items = [dict(row) for row in rows[:limit]]
        more = len(rows) > limit
        return {
            "clients": items,
            "has_more": more,
            "next_cursor": items[-1]["id"] if more and items else None,
        }

    def storage_records(self) -> list[dict[str, Any]]:
        self._ensure_open()
        fields = (
            "id", "kind", "state", "generation", "history_id", "exec_id",
            "client_id", "connection_id", "result_ref", "output_evicted",
            "result_evicted", "scan_output_evicted", "created", "finished", "artifacts",
        )
        projected = [
            "id AS row_id", "kind AS row_kind",
            "created AS row_created", "updated AS row_updated",
            "CASE WHEN json_valid(data) THEN 1 ELSE 0 END AS data_valid",
            "CASE WHEN json_valid(data) THEN json_type(data) END AS data_type",
        ]
        projected.extend(
            "CASE WHEN json_valid(data) THEN "
            "CASE WHEN json_type(data) = 'object' "
            f"THEN json_extract(data, '$.{field}') END END AS data_{field}"
            for field in ("id", "kind")
        )
        projected.extend(
            "CASE WHEN json_valid(data) THEN "
            "CASE WHEN json_type(data) = 'object' "
            f"THEN json_extract(data, '$.{field}') END END AS {field}"
            for field in fields
            if field not in {"id", "kind"}
        )
        with self._lock:
            rows = self._db.execute(f"SELECT {', '.join(projected)} FROM entities").fetchall()
        records = []
        for row in rows:
            if not row["data_valid"]:
                records.append(
                    _corrupt_entity(
                        row["row_id"], row["row_kind"], "invalid_json",
                        created=row["row_created"], updated=row["row_updated"],
                    )
                )
                continue
            if row["data_type"] != "object":
                records.append(
                    _corrupt_entity(
                        row["row_id"], row["row_kind"], "invalid_object",
                        created=row["row_created"], updated=row["row_updated"],
                    )
                )
                continue
            record = {
                field: row[f"data_{field}"] if field in {"id", "kind"} else row[field]
                for field in fields
            }
            for field in ("result_ref", "artifacts"):
                if isinstance(record[field], str):
                    try:
                        record[field] = json.loads(record[field])
                    except (TypeError, ValueError, UnicodeError):
                        record = _corrupt_entity(
                            row["row_id"], row["row_kind"], "invalid_nested_json",
                            created=row["row_created"], updated=row["row_updated"],
                        )
                        break
            records.append(record)
        return records

    def _relative_storage_path(self, value: Any) -> str | None:
        if not isinstance(value, str) or not value:
            return None
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = self.root / candidate
        try:
            relative = candidate.resolve(strict=False).relative_to(self.root)
        except (OSError, ValueError):
            return None
        normalized = relative.as_posix()
        return normalized if normalized.startswith(".mypr/") else None

    def _artifact_paths(self, record: Mapping[str, Any]) -> set[str]:
        paths: set[str] = set()
        for item in record.get("artifacts") or ():
            if isinstance(item, Mapping):
                path = self._relative_storage_path(item.get("path"))
                if path is not None:
                    paths.add(path)
        owner = record.get("exec_id") or record.get("id")
        if not isinstance(owner, str) or not owner:
            return paths
        directory = self.root / ".mypr" / "artifacts" / owner
        try:
            children = list(directory.iterdir()) if directory.is_dir() else []
        except OSError:
            children = []
        for child in children[:4096]:
            if child.is_file() and not child.is_symlink():
                path = self._relative_storage_path(str(child))
                if path is not None:
                    paths.add(path)
        return paths

    def _scan_paths(self, ident: str) -> set[str]:
        metadata = self.root / ".mypr" / "scans" / f"{ident}.json"
        paths: set[str] = set()
        try:
            payload = json.loads(metadata.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, Mapping):
            for key in ("result_path", "summary_path", "artifact", "artifact_path", "config"):
                path = self._relative_storage_path(payload.get(key))
                if path is not None:
                    paths.add(path)
        if not paths:
            prefix = f".mypr/scans/{ident}"
            paths.update(
                f"{prefix}{suffix}"
                for suffix in (".jsonl", ".summary.json", ".xml", ".request.json")
            )
        return paths

    def storage_gc_snapshot(self) -> dict[str, Any]:
        active, protected, references, tombstones = set(), set(), {}, {}
        warnings: list[dict[str, str]] = []
        uncertain = False
        warnings_truncated = False
        for record in self.storage_records():
            if record.get("corrupt"):
                uncertain = True
                warnings_truncated |= _append_warning(warnings, record.get("warning"))
                continue
            ident, kind = record.get("id"), record.get("kind")
            if not isinstance(ident, str):
                continue
            if kind == "python":
                history_id = _python_history_id(record)
                digest = hashlib.sha256(str(history_id).encode()).hexdigest()
                paths = [f".mypr/runs/task-{digest}.jsonl"]
            elif kind == "execution":
                paths = [f".mypr/runs/{ident}.jsonl"]
            elif kind == "scan":
                paths = sorted(self._scan_paths(ident))
            else:
                paths = [f".mypr/jobs/{ident}.jsonl"]
            reference = record.get("result_ref")
            if isinstance(reference, dict):
                path = self._relative_storage_path(reference.get("path"))
                if path is not None:
                    paths.append(path)
            paths.extend(self._artifact_paths(record))
            live = record.get("state") in _ACTIVE_STATES
            if live:
                active.add(ident)
                protected.update(paths)
            for path in paths:
                references.setdefault(path, []).append(ident)
                result_path = (
                    self._relative_storage_path(reference.get("path"))
                    if isinstance(reference, dict)
                    else None
                )
                is_result = result_path is not None and path == result_path
                if record.get("result_evicted" if is_result else (
                    "scan_output_evicted" if kind == "scan" else "output_evicted"
                )):
                    tombstones[path] = {"id": ident}
        return {
            "active_ids": active, "protected_paths": protected,
            "references": references, "tombstones": tombstones,
            "uncertain": uncertain, "warnings": warnings,
            "warnings_truncated": warnings_truncated,
        }

    def storage_gc_before_delete(self, candidates) -> list[str]:
        snapshot = self.storage_gc_snapshot()
        if snapshot.get("uncertain"):
            return []
        protected = set(snapshot["protected_paths"])
        paths = [item["path"] for item in candidates if item["path"] not in protected]
        return self.mark_storage_evicted(paths)

    def mark_storage_evicted(self, paths: list[str]) -> list[str]:
        """Preserve entity identity and deduplication while expiring owned data."""
        self._ensure_open()
        if any(record.get("corrupt") for record in self.storage_records()):
            return []
        selected = set(paths)
        marked: set[str] = set()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for row in self._db.execute("SELECT id, data FROM entities").fetchall():
                    record, warning = _decode_entity(row)
                    if warning is not None:
                        continue
                    if record.get("state") not in {"succeeded", "failed", "cancelled", "lost"}:
                        continue
                    kind = record.get("kind")
                    ident = record.get("id")
                    if kind == "python":
                        key = _python_history_id(record)
                        digest = hashlib.sha256(str(key).encode()).hexdigest()
                        output_paths = {f".mypr/runs/task-{digest}.jsonl"}
                    elif kind == "execution":
                        output_paths = {f".mypr/runs/{ident}.jsonl"}
                    elif kind == "scan":
                        output_paths = self._scan_paths(ident)
                    else:
                        output_paths = {f".mypr/jobs/{ident}.jsonl"}
                    updated = False
                    if output_paths & selected:
                        if kind == "scan":
                            record.update(
                                scan_output_evicted=True,
                                output_evicted=True,
                                output_evicted_at=time.time(),
                            )
                        else:
                            record.update(output_evicted=True, output_evicted_at=time.time())
                        record.pop("output", None)
                        record.pop("events", None)
                        updated = True
                    reference = record.get("result_ref")
                    result_path = (
                        self._relative_storage_path(reference.get("path"))
                        if isinstance(reference, dict)
                        else None
                    )
                    if result_path in selected:
                        record.update(result_evicted=True, result_evicted_at=time.time())
                        updated = True
                    artifact_paths = self._artifact_paths(record)
                    if artifact_paths & selected:
                        record.update(
                            output_evicted=True,
                            artifact_evicted=True,
                            output_evicted_at=time.time(),
                        )
                        record.pop("output", None)
                        record.pop("events", None)
                        updated = True
                    if updated:
                        self._db.execute(
                            "UPDATE entities SET data=? WHERE id=?", (_dump(record), row["id"])
                        )
                        marked.update(
                            path
                            for path in selected
                            if path in output_paths or path == result_path or path in artifact_paths
                        )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return sorted(marked)

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
                    try:
                        previous = _load(row["data"])
                    except (TypeError, ValueError, UnicodeError) as exc:
                        raise ValueError(
                            f"cannot update corrupt history entity {storage_id!r}"
                        ) from exc
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
        return [_event(row)[0] for row in rows]

    def find_request(self, client_id: str, request_id: str) -> dict[str, Any] | None:
        self._ensure_open()
        with self._lock:
            corrupt = self._db.execute(
                "SELECT 1 FROM entities WHERE kind = 'execution' AND "
                "CASE WHEN json_valid(data) THEN json_type(data) != 'object' ELSE 1 END "
                "LIMIT 1"
            ).fetchone()
            if corrupt is not None:
                raise RuntimeError(
                    "cannot verify request deduplication: persisted execution history is corrupt"
                )
            row = self._db.execute(
                "SELECT id, kind, created, updated, data FROM entities "
                "WHERE kind = 'execution' AND "
                "json_valid(data) AND "
                "COALESCE(json_extract(data, '$.client_id'), json_extract(data, '$.client')) = ? "
                "AND json_extract(data, '$.request_id') = ? ORDER BY entity_seq LIMIT 1",
                (client_id, request_id),
            ).fetchone()
        if row is None:
            return None
        record, warning = _decode_entity(row)
        return None if warning is not None else record

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
                "json_valid(data) AND "
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
                f"SELECT entity_seq, id, kind, created, updated, data FROM entities{where} "
                "ORDER BY entity_seq DESC LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = []
        warnings: list[dict[str, str]] = []
        warnings_truncated = False
        for row in visible:
            item, warning = _decode_entity(row)
            warnings_truncated |= _append_warning(warnings, warning)
            item.pop("code", None)
            item.pop("output", None)
            item.pop("events", None)
            if not item.get("corrupt"):
                history_id = _python_history_id(item)
                if history_id is not None:
                    item["history_id"] = history_id
            items.append(item)
        next_cursor = int(visible[-1]["entity_seq"]) if has_more and visible else None
        result = {"items": items, "next_cursor": next_cursor}
        _attach_warnings(result, warnings, warnings_truncated)
        return result

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
                    "SELECT id, kind, created, updated, data FROM entities "
                    "WHERE kind = 'python' "
                    "AND json_valid(data) "
                    "AND json_extract(data, '$.id') = ? "
                    "AND json_extract(data, '$.generation') = ? "
                    "ORDER BY entity_seq DESC LIMIT 1",
                    (task_id, generation),
                ).fetchone()
                if row is not None:
                    result, warning = _decode_entity(row)
                    if warning is not None:
                        return result
                    result["history_id"] = _python_history_id(result) or ident
                    return result
            row = self._db.execute(
                "SELECT id, kind, created, updated, data FROM entities WHERE id = ?",
                (ident,),
            ).fetchone()
            if row is not None:
                exact, warning = _decode_entity(row)
                if warning is not None:
                    return exact
                if exact.get("history_id") == ident:
                    return exact
            row = self._db.execute(
                "SELECT id, kind, created, updated, data FROM entities "
                "WHERE json_valid(data) AND json_extract(data, '$.id') = ? "
                "ORDER BY entity_seq DESC LIMIT 1",
                (ident,),
            ).fetchone()
        if row is None:
            return None
        result, warning = _decode_entity(row)
        if warning is not None:
            return result
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
        events: list[dict[str, Any]] = []
        warnings: list[dict[str, str]] = []
        warnings_truncated = False
        for row in rows:
            event, warning = _event(row)
            events.append(event)
            warnings_truncated |= _append_warning(warnings, warning)
        result = {"events": events, "cursor": next_cursor}
        _attach_warnings(result, warnings, warnings_truncated)
        return result

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
            where = "json_valid(data) AND json_extract(data, '$.state') IN (?, ?, ?)"
            if kind is not None:
                where += " AND kind = ?"
                params.append(kind)
            rows = self._db.execute(
                f"SELECT id, kind, created, updated, data FROM entities WHERE {where}", params
            ).fetchall()
            count = 0
            for row in rows:
                record, warning = _decode_entity(row)
                if warning is not None:
                    continue
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


def _row_value(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _corrupt_entity(
    ident: Any,
    kind: Any,
    reason: str,
    *,
    created: Any = None,
    updated: Any = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": ident,
        "kind": kind,
        "corrupt": True,
        "corruption": reason,
    }
    if created is not None:
        record["created"] = created
    if updated is not None:
        record["updated"] = updated
    record["warning"] = {
        "code": "history_entity_corrupt",
        "text": f"History entity {ident!r} data is unavailable ({reason})."[:_MAX_WARNING_TEXT],
    }
    return record


def _decode_entity(row: sqlite3.Row) -> tuple[dict[str, Any], dict[str, str] | None]:
    ident = _row_value(row, "id", "unknown")
    kind = _row_value(row, "kind", "unknown")
    try:
        loaded = json.loads(row["data"])
    except (TypeError, ValueError, UnicodeError):
        record = _corrupt_entity(
            ident,
            kind,
            "invalid_json",
            created=_row_value(row, "created"),
            updated=_row_value(row, "updated"),
        )
    else:
        if not isinstance(loaded, dict):
            record = _corrupt_entity(
                ident,
                kind,
                "invalid_object",
                created=_row_value(row, "created"),
                updated=_row_value(row, "updated"),
            )
        else:
            return loaded, None
    return record, record["warning"]


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


def _event(row: sqlite3.Row) -> tuple[dict[str, Any], dict[str, str] | None]:
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
        try:
            result["data"] = json.loads(row["data"])
        except (TypeError, ValueError, UnicodeError):
            result["data_corrupt"] = True
            return result, {
                "code": "history_event_corrupt",
                "text": (
                    f"History event {result['seq']} data is invalid and was omitted."
                    [: _MAX_WARNING_TEXT]
                ),
            }
    return result, None


def _append_warning(
    warnings: list[dict[str, str]], warning: dict[str, str] | None
) -> bool:
    if warning is None:
        return False
    if len(warnings) >= _MAX_HISTORY_WARNINGS:
        return True
    warnings.append(warning)
    return False


def _attach_warnings(
    result: dict[str, Any],
    warnings: list[dict[str, str]],
    truncated: bool = False,
) -> None:
    if warnings:
        result["warnings"] = warnings
    if truncated:
        result["warnings_truncated"] = True
