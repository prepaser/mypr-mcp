"""Persistent workspace execution and connection history.

The manager is asynchronous, but history operations are intentionally small,
synchronous SQLite transactions.  A single manager owns a History instance,
so the connection is kept simple while the database remains usable by a
second process during recovery or inspection.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import secrets
import sqlite3
import threading
import time
from collections.abc import Iterable, Iterator, Mapping
from itertools import islice
from pathlib import Path
from typing import Any

from .client_ids import ADJECTIVES, ANIMALS
from .file_io import read_bytes
from .history_maintenance import vacuum_if_worthwhile
from .json_utils import SOURCE_HASH_ENCODING, json_text, sha256_text, source_sha256

_KINDS = {"execution", "python", "shell", "package", "scan"}
_ACTIVE_STATES = {"queued", "running", "cancelling"}
_MAX_PAYLOAD_BYTES = 64 * 1024
_MAX_HISTORY_WARNINGS = 8
_MAX_WARNING_TEXT = 256
_MAX_HISTORY_GC_ITEMS = 1000
_MAX_HISTORY_JSON_RETRIES = 64
_MAX_EXECUTION_RECORD_BYTES = 16 * 1024 * 1024
_MAX_SCAN_METADATA_BYTES = 1 * 1024 * 1024
_DAY = 24 * 60 * 60
_TERMINAL_STATES = {"succeeded", "failed", "cancelled", "lost", "reset", "complete", "completed"}
_BULKY_ENTITY_FIELDS = {"code", "output", "events"}
_TELEMETRY_KINDS = {"web", "mcp", "dependency", "connection", "config", "runtime"}


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
                CREATE TABLE IF NOT EXISTS history_meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL
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
            self._ensure_mail_send_seq()

    def _meta_value(self, key: str) -> str | None:
        row = self._db.execute(
            "SELECT value FROM history_meta WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])

    def _set_meta(self, key: str, value: Any) -> None:
        self._db.execute(
            "INSERT INTO history_meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def _ensure_mail_send_seq(self) -> None:
        """Give mail sends a stable cursor independent of SQLite's hidden rowid."""
        table = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mail_sends'"
        ).fetchone()
        if table is None:
            self._set_meta("mail_send_seq_migrated", 1)
            return
        columns = {
            row[1] for row in self._db.execute("PRAGMA table_info(mail_sends)").fetchall()
        }
        if "send_seq" in columns:
            self._set_meta("mail_send_seq_migrated", 1)
            return
        required = {
            "id", "draft_id", "client_id", "request_id", "state", "accepted",
            "rejected", "rejected_details", "stage", "error", "warning", "created",
            "updated",
        }
        missing = required - columns
        if missing:
            self._set_meta("mail_send_seq_migrated", 0)
            return
        try:
            self._db.execute("BEGIN IMMEDIATE")
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
            self._set_meta("mail_send_seq_migrated", 1)
            self._db.execute("COMMIT")
        except BaseException:
            try:
                self._db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._set_meta("mail_send_seq_migrated", 0)

    def _mail_send_seq_migrated(self) -> bool:
        return self._meta_value("mail_send_seq_migrated") == "1"

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

    def _storage_record_rows(self) -> Iterator[dict[str, Any]]:
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
            rows = self._db.execute(f"SELECT {', '.join(projected)} FROM entities")
            for row in rows:
                yield self._storage_record_from_row(row, fields)

    @staticmethod
    def _storage_record_from_row(
        row: sqlite3.Row, fields: tuple[str, ...]
    ) -> dict[str, Any]:
        if not row["data_valid"]:
            return _corrupt_entity(
                row["row_id"], row["row_kind"], "invalid_json",
                created=row["row_created"], updated=row["row_updated"],
            )
        if row["data_type"] != "object":
            return _corrupt_entity(
                row["row_id"], row["row_kind"], "invalid_object",
                created=row["row_created"], updated=row["row_updated"],
            )
        record = {
            field: row[f"data_{field}"] if field in {"id", "kind"} else row[field]
            for field in fields
        }
        for field in ("result_ref", "artifacts"):
            if isinstance(record[field], str):
                try:
                    record[field] = json.loads(record[field])
                except (TypeError, ValueError, UnicodeError):
                    return _corrupt_entity(
                        row["row_id"], row["row_kind"], "invalid_nested_json",
                        created=row["row_created"], updated=row["row_updated"],
                    )
        return record

    def storage_records(self) -> list[dict[str, Any]]:
        return list(self._storage_record_rows())

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
            children = islice(directory.iterdir(), 4096) if directory.is_dir() else ()
            for child in children:
                if child.is_file() and not child.is_symlink():
                    path = self._relative_storage_path(str(child))
                    if path is not None:
                        paths.add(path)
        except OSError:
            pass
        return paths

    def _scan_paths_with_status(
        self, ident: str, *, metadata_optional: bool = False
    ) -> tuple[set[str], dict[str, str] | None]:
        metadata = self.root / ".mypr" / "scans" / f"{ident}.json"
        paths: set[str] = set()
        warning: dict[str, str] | None = None
        loaded = False
        try:
            payload = json.loads(
                read_bytes(
                    metadata,
                    max_bytes=_MAX_SCAN_METADATA_BYTES,
                    follow_symlinks=False,
                ).decode("utf-8")
            )
            loaded = True
        except FileNotFoundError:
            payload = None
            if not metadata_optional:
                warning = {
                    "code": "scan_metadata_missing",
                    "text": (
                        f"Scan {ident} metadata is missing while its paths are still referenced."
                    )[:_MAX_WARNING_TEXT],
                }
        except (OSError, UnicodeError, ValueError) as exc:
            payload = None
            warning = {
                "code": "scan_metadata_unreadable",
                "text": (
                    f"Scan {ident} metadata could not be read safely: {type(exc).__name__}."
                )[:_MAX_WARNING_TEXT],
            }
        if isinstance(payload, Mapping):
            found_field = False
            for key in ("result_path", "summary_path", "artifact", "artifact_path", "config"):
                if key not in payload or payload[key] is None:
                    continue
                found_field = True
                path = self._relative_storage_path(payload[key])
                if path is None:
                    warning = {
                        "code": "scan_metadata_invalid_path",
                        "text": (
                            f"Scan {ident} metadata contains an invalid {key} path."
                        )[:_MAX_WARNING_TEXT],
                    }
                else:
                    paths.add(path)
            if not found_field:
                warning = {
                    "code": "scan_metadata_incomplete",
                    "text": f"Scan {ident} metadata contains no output paths."[:_MAX_WARNING_TEXT],
                }
        elif loaded:
            warning = {
                "code": "scan_metadata_invalid_object",
                "text": f"Scan {ident} metadata is not a JSON object."[:_MAX_WARNING_TEXT],
            }
        if not paths:
            prefix = f".mypr/scans/{ident}"
            paths.update(
                f"{prefix}{suffix}"
                for suffix in (".jsonl", ".summary.json", ".xml", ".request.json")
            )
        return paths, warning

    def _scan_paths(self, ident: str) -> set[str]:
        return self._scan_paths_with_status(ident)[0]

    def storage_gc_snapshot(
        self,
        *,
        retention_days: int = 30,
        paths: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        if type(retention_days) is not int or retention_days < 1:
            raise ValueError("retention_days must be a positive integer")
        retained_paths = None
        if paths is not None:
            retained_paths = {
                path
                for value in paths
                if (path := self._relative_storage_path(value)) is not None
            }
        active, protected, known_owners, references, tombstones = set(), set(), set(), {}, {}
        warnings: list[dict[str, str]] = []
        uncertain = False
        warnings_truncated = False
        for record in self._storage_record_rows():
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
                paths, warning = self._scan_paths_with_status(
                    ident,
                    metadata_optional=(
                        bool(record.get("scan_output_evicted"))
                        or record.get("state") in {"failed", "cancelled", "lost", "reset"}
                    ),
                )
                if warning is not None:
                    uncertain = True
                    warnings_truncated |= _append_warning(warnings, warning)
                paths = sorted(paths)
            else:
                paths = [f".mypr/jobs/{ident}.jsonl"]
            reference = record.get("result_ref")
            if isinstance(reference, dict):
                path = self._relative_storage_path(reference.get("path"))
                if path is not None:
                    paths.append(path)
            paths.extend(self._artifact_paths(record))
            if retained_paths is not None:
                scoped = []
                for path in paths:
                    if path in retained_paths:
                        scoped.append(path)
                    elif path.endswith(".jsonl"):
                        sidecar = path.removesuffix(".jsonl") + ".idx"
                        if sidecar in retained_paths:
                            scoped.append(sidecar)
                paths = scoped
            state = record.get("state")
            if paths and state in _TERMINAL_STATES:
                known_owners.add(ident)
            live = state in _ACTIVE_STATES
            if live:
                active.add(ident)
                protected.update(paths)
            elif paths and state not in _TERMINAL_STATES:
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
            "known_owner_ids": known_owners,
            "references": references, "tombstones": tombstones,
            "paths_scoped": retained_paths is not None,
            "uncertain": uncertain, "warnings": warnings,
            "warnings_truncated": warnings_truncated,
            "history": self.storage_history_snapshot(retention_days=retention_days),
        }

    def storage_history_snapshot(self, *, retention_days: int = 30) -> dict[str, Any]:
        """Build an immutable, bounded plan for compacting old history bodies."""
        self._ensure_open()
        if type(retention_days) is not int or retention_days < 1:
            raise ValueError("retention_days must be a positive integer")
        try:
            cutoff = time.time() - retention_days * _DAY
        except OverflowError as exc:
            raise ValueError("history retention cutoff is not finite") from exc
        if not _finite_number(cutoff):
            raise ValueError("history retention cutoff is not finite")
        entities: list[dict[str, Any]] = []
        body_bytes = 0
        terminal_marks = ",".join("?" for _ in _TERMINAL_STATES)
        terminal_values = tuple(sorted(_TERMINAL_STATES))
        with self._lock:
            entity_sql = (
                "SELECT entity_seq,id,kind,updated,data FROM entities "
                "WHERE json_valid(data) AND json_type(data)='object' "
                "AND json_type(data,'$.id')='text' "
                f"AND json_extract(data,'$.state') IN ({terminal_marks}) "
                "AND updated <= ? "
            )
            retry_limit = (
                min(_MAX_HISTORY_JSON_RETRIES, max(1, _MAX_HISTORY_GC_ITEMS // 10))
                if _MAX_HISTORY_GC_ITEMS > 1
                else 0
            )
            normal_limit = _MAX_HISTORY_GC_ITEMS - retry_limit
            normal_rows = self._db.execute(
                entity_sql
                + "AND COALESCE(json_extract(data,'$.body_evicted'),0) != 1 "
                "AND (json_type(data,'$.code') IS NOT NULL "
                "OR json_type(data,'$.output') IS NOT NULL "
                "OR json_type(data,'$.events') IS NOT NULL) "
                "ORDER BY updated,entity_seq LIMIT ?",
                (*terminal_values, cutoff, normal_limit),
            ).fetchall()
            retry_rows = []
            if retry_limit or not normal_rows:
                retry_rows = self._db.execute(
                    entity_sql
                    + "AND kind = 'execution' "
                    "AND json_extract(data,'$.json_compaction_pending') = 1 "
                    "ORDER BY CASE "
                    "WHEN json_type(data,'$.json_compaction_attempted_at') IN ('integer','real') "
                    "THEN json_extract(data,'$.json_compaction_attempted_at') "
                    "ELSE 0 END, updated,entity_seq LIMIT ?",
                    (
                        *terminal_values,
                        cutoff,
                        retry_limit or _MAX_HISTORY_GC_ITEMS,
                    ),
                ).fetchall()
            rows = [*normal_rows, *retry_rows]
            for row in rows:
                try:
                    record = _load(row["data"])
                except (TypeError, ValueError, UnicodeError):
                    continue
                state = record.get("state")
                ident = record.get("id")
                if not isinstance(ident, str) or not isinstance(state, str):
                    continue
                terminal = state in _TERMINAL_STATES
                retry_json = (
                    str(row["kind"]) == "execution"
                    and record.get("json_compaction_pending") is True
                )
                if (
                    not terminal
                    or float(row["updated"] or 0) > cutoff
                    or (record.get("body_evicted") is True and not retry_json)
                    or (not retry_json and not _BULKY_ENTITY_FIELDS.intersection(record))
                ):
                    continue
                entities.append(
                    {
                        "id": str(row["id"]),
                        "entity_seq": int(row["entity_seq"]),
                        "kind": str(row["kind"]),
                        "updated": float(row["updated"]),
                        "data_sha256": _sha256_text(row["data"]),
                    }
                )
                body_bytes += len(str(row["data"]).encode("utf-8"))
                if len(entities) >= _MAX_HISTORY_GC_ITEMS:
                    break
            events: list[dict[str, Any]] = []
            owner_sql = (
                "CASE WHEN json_valid(e.data) AND json_type(e.data)='object' "
                "AND json_type(e.data,'$.history_id')='text' "
                "THEN json_extract(e.data,'$.history_id') "
                "WHEN e.id IS NOT NULL THEN e.id ELSE e.exec_id END"
            )
            event_rows = self._db.execute(
                "SELECT e.seq,e.time,e.id,e.exec_id,e.kind,e.data," + owner_sql + " AS owner "
                "FROM events e JOIN entities owner ON owner.id=" + owner_sql + " "
                "WHERE e.time <= ? AND (e.data IS NULL OR json_valid(e.data)) "
                "AND json_valid(owner.data) AND json_type(owner.data)='object' "
                f"AND json_extract(owner.data,'$.state') IN ({terminal_marks}) "
                "AND owner.updated <= ? ORDER BY e.seq LIMIT ?",
                (cutoff, *terminal_values, cutoff, _MAX_HISTORY_GC_ITEMS),
            ).fetchall()
            for row in event_rows:
                owner = _event_owner(row)
                if owner is None or owner != row["owner"]:
                    continue
                events.append(
                    {
                        "seq": int(row["seq"]),
                        "time": float(row["time"]),
                        "id": row["id"],
                        "kind": row["kind"],
                        "owner": owner,
                        "data_sha256": _sha256_optional_text(row["data"]),
                    }
                )
                body_bytes += len(str(row["data"] or "").encode("utf-8"))
                if len(events) >= _MAX_HISTORY_GC_ITEMS:
                    break
            remaining_events = _MAX_HISTORY_GC_ITEMS - len(events)
            if remaining_events:
                telemetry_marks = ",".join("?" for _ in _TELEMETRY_KINDS)
                telemetry_rows = self._db.execute(
                    "SELECT e.seq,e.time,e.id,e.exec_id,e.kind,e.data," + owner_sql + " AS owner "
                    "FROM events e "
                    "WHERE e.time <= ? AND e.kind IN (" + telemetry_marks + ") "
                    "AND (e.data IS NULL OR json_valid(e.data)) "
                    "AND NOT EXISTS (SELECT 1 FROM entities owner WHERE owner.id="
                    + owner_sql
                    + ") ORDER BY e.seq LIMIT ?",
                    (cutoff, *sorted(_TELEMETRY_KINDS), remaining_events),
                ).fetchall()
                for row in telemetry_rows:
                    owner = _event_owner(row)
                    if owner is not None and row["owner"] is not None and owner != row["owner"]:
                        continue
                    events.append(
                        {
                            "seq": int(row["seq"]),
                            "time": float(row["time"]),
                            "id": row["id"],
                            "kind": row["kind"],
                            "owner": owner,
                            "data_sha256": _sha256_optional_text(row["data"]),
                        }
                    )
                    body_bytes += len(str(row["data"] or "").encode("utf-8"))
                    if len(events) >= _MAX_HISTORY_GC_ITEMS:
                        break
            watermark = int(self._meta_value("pruned_through_seq") or 0)
            last_vacuum = self._meta_value("last_vacuum_at")
        return {
            "retention_days": retention_days,
            "cutoff": cutoff,
            "entities": entities,
            "events": events,
            "pruned_through_seq": watermark,
            "history_truncated": watermark > 0,
            "last_vacuum_at": float(last_vacuum) if last_vacuum else None,
            "mail_send_seq_migrated": self._mail_send_seq_migrated(),
            "bytes": body_bytes,
        }

    def storage_history_apply(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        """Apply an unchanged history plan, compacting at most 1000 rows per kind."""
        self._ensure_open()
        entities = plan.get("entities", ())
        events = plan.get("events", ())
        if isinstance(entities, (str, bytes)) or not isinstance(entities, list):
            raise TypeError("history plan entities must be a list")
        if isinstance(events, (str, bytes)) or not isinstance(events, list):
            raise TypeError("history plan events must be a list")
        if len(entities) > _MAX_HISTORY_GC_ITEMS or len(events) > _MAX_HISTORY_GC_ITEMS:
            raise ValueError("history GC batches are limited to 1000 entities and events")
        now = time.time()
        cutoff = plan.get("cutoff")
        if cutoff is None:
            retention_days = plan.get("retention_days")
            if type(retention_days) is not int or retention_days < 1:
                raise ValueError("history plan retention_days is invalid")
            try:
                cutoff = now - retention_days * _DAY
            except OverflowError as exc:
                raise ValueError("history plan cutoff is invalid") from exc
        if (
            not isinstance(cutoff, (int, float))
            or isinstance(cutoff, bool)
            or not _finite_number(cutoff)
        ):
            raise ValueError("history plan cutoff is invalid")
        compacted = 0
        deleted_events = 0
        pruned_bytes = 0
        skipped: list[dict[str, Any]] = []
        max_seq = 0
        compacted_json: list[str] = []
        compacted_json_timestamps: dict[str, Any] = {}
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for item in entities:
                    if not isinstance(item, Mapping):
                        skipped.append({"reason": "invalid_entity_plan"})
                        continue
                    ident = item.get("id")
                    if not isinstance(ident, str):
                        skipped.append({"id": ident, "reason": "invalid_entity_id"})
                        continue
                    row = self._db.execute(
                        "SELECT entity_seq,kind,updated,data FROM entities WHERE id=?", (ident,)
                    ).fetchone()
                    if (
                        row is None
                        or float(row["updated"]) > float(cutoff)
                        or not _history_entity_matches(row, item)
                    ):
                        skipped.append({"id": ident, "reason": "changed_since_plan"})
                        continue
                    try:
                        record = _load(row["data"])
                    except (TypeError, ValueError, UnicodeError):
                        skipped.append({"id": ident, "reason": "corrupt"})
                        continue
                    if record.get("state") not in _TERMINAL_STATES:
                        skipped.append({"id": ident, "reason": "active_or_ambiguous"})
                        continue
                    retry_json = (
                        row["kind"] == "execution"
                        and record.get("json_compaction_pending") is True
                    )
                    if retry_json:
                        compacted_json.append(ident)
                        compacted_json_timestamps[ident] = record.get("body_evicted_at")
                        continue
                    compact = {
                        key: value for key, value in record.items()
                        if key not in _BULKY_ENTITY_FIELDS
                    }
                    code = record.get("code")
                    if code is not None:
                        compact["code_sha256"] = source_sha256(
                            code if isinstance(code, str) else _dump(code)
                        )
                        compact["code_sha256_encoding"] = SOURCE_HASH_ENCODING
                    compact["body_evicted"] = True
                    compact["body_evicted_at"] = now
                    if row["kind"] == "execution":
                        compact["json_compaction_pending"] = True
                        compacted_json_timestamps[ident] = now
                    old_size = len(str(row["data"]).encode("utf-8"))
                    new_data = _dump(compact)
                    self._db.execute(
                        "UPDATE entities SET data=? WHERE id=?", (new_data, ident)
                    )
                    compacted += 1
                    if row["kind"] == "execution":
                        compacted_json.append(ident)
                    pruned_bytes += max(0, old_size - len(new_data.encode("utf-8")))
                for item in events:
                    if not isinstance(item, Mapping) or not isinstance(item.get("seq"), int):
                        skipped.append({"reason": "invalid_event_plan"})
                        continue
                    seq = int(item["seq"])
                    row = self._db.execute(
                        "SELECT seq,time,id,exec_id,kind,data FROM events WHERE seq=?", (seq,)
                    ).fetchone()
                    if row is not None and row["data"] is not None:
                        try:
                            json.loads(row["data"])
                        except (TypeError, ValueError, UnicodeError):
                            skipped.append({"seq": seq, "reason": "corrupt"})
                            continue
                    if (
                        row is None
                        or float(row["time"]) > float(cutoff)
                        or not _history_event_matches(row, item)
                    ):
                        skipped.append({"seq": seq, "reason": "changed_since_plan"})
                        continue
                    owner = _event_owner(row)
                    if owner is None and row["kind"] not in _TELEMETRY_KINDS:
                        skipped.append({"seq": seq, "reason": "active_or_ambiguous"})
                        continue
                    if item.get("kind") is not None and row["kind"] != item.get("kind"):
                        skipped.append({"seq": seq, "reason": "changed_since_plan"})
                        continue
                    if owner is not None and owner != item.get("owner"):
                        skipped.append({"seq": seq, "reason": "active_or_ambiguous"})
                        continue
                    if owner is not None:
                        owner_row = self._db.execute(
                            "SELECT kind,updated,data FROM entities WHERE id=?", (owner,)
                        ).fetchone()
                        if owner_row is None:
                            if row["kind"] not in _TELEMETRY_KINDS:
                                skipped.append({"seq": seq, "reason": "active_or_ambiguous"})
                                continue
                        else:
                            try:
                                owner_record = _load(owner_row["data"])
                            except (TypeError, ValueError, UnicodeError):
                                skipped.append({"seq": seq, "reason": "active_or_ambiguous"})
                                continue
                            if (
                                owner_record.get("state") not in _TERMINAL_STATES
                                or float(owner_row["updated"]) > float(cutoff)
                            ):
                                skipped.append({"seq": seq, "reason": "active_or_ambiguous"})
                                continue
                    self._db.execute("DELETE FROM events WHERE seq=?", (seq,))
                    deleted_events += 1
                    pruned_bytes += len(str(row["data"] or "").encode("utf-8"))
                    max_seq = max(max_seq, seq)
                watermark = int(self._meta_value("pruned_through_seq") or 0)
                if max_seq:
                    watermark = max(watermark, max_seq)
                    self._set_meta("pruned_through_seq", watermark)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            json_compacted, json_errors, failed_json = self._compact_execution_records(
                compacted_json, now, timestamps=compacted_json_timestamps
            )
            self._clear_json_compaction_pending(
                ident for ident in compacted_json if ident not in failed_json
            )
            self._mark_json_compaction_failed(failed_json, now)
            vacuum = self._vacuum_after_history_commit(now)
        watermark = int(self._meta_value("pruned_through_seq") or 0)
        return {
            "entities": compacted,
            "json_compacted": json_compacted,
            **({"json_compaction_errors": json_errors} if json_errors else {}),
            "events": deleted_events,
            "pruned_bytes": pruned_bytes,
            "reclaimed_bytes": int(vacuum.get("reclaimed_bytes", 0)),
            "checkpoint": vacuum.get("checkpoint"),
            "skipped": skipped,
            "history_truncated": watermark > 0,
            "pruned_through_seq": watermark,
            "vacuum": vacuum,
        }

    def _compact_execution_records(
        self,
        records: list[str],
        timestamp: float,
        *,
        timestamps: Mapping[str, Any] | None = None,
    ) -> tuple[int, list[dict[str, str]], set[str]]:
        compacted = 0
        errors: list[dict[str, str]] = []
        failed: set[str] = set()
        timestamps = timestamps or {}

        def mark_failed(ident: str, error: str) -> None:
            failed.add(ident)
            if len(errors) < _MAX_HISTORY_WARNINGS:
                errors.append({"id": ident, "error": error[:_MAX_WARNING_TEXT]})

        for ident in records:
            if not ident or Path(ident).name != ident or ident in {".", ".."}:
                if isinstance(ident, str) and ident:
                    mark_failed(ident, "execution ID cannot be used as a run filename")
                continue
            path = self.root / ".mypr" / "runs" / f"{ident}.json"
            try:
                payload = json.loads(
                    read_bytes(
                        path,
                        max_bytes=_MAX_EXECUTION_RECORD_BYTES,
                        follow_symlinks=False,
                    ).decode("utf-8")
                )
                if not isinstance(payload, dict):
                    mark_failed(ident, "execution JSON is not an object")
                    continue
                if payload.get("id", ident) != ident:
                    mark_failed(ident, "execution JSON ID does not match its history ID")
                    continue
                if payload.get("state") not in _TERMINAL_STATES:
                    mark_failed(ident, "execution JSON is not terminal")
                    continue
                if not any(field in payload for field in _BULKY_ENTITY_FIELDS):
                    continue
                code = payload.get("code")
                compact = {
                    key: value
                    for key, value in payload.items()
                    if key not in _BULKY_ENTITY_FIELDS
                }
                if isinstance(code, str):
                    compact["code_sha256"] = source_sha256(code)
                    compact["code_sha256_encoding"] = SOURCE_HASH_ENCODING
                compact["body_evicted"] = True
                persisted_at = timestamps.get(ident, timestamp)
                if (
                    isinstance(persisted_at, bool)
                    or not isinstance(persisted_at, (int, float))
                    or not math.isfinite(persisted_at)
                ):
                    persisted_at = timestamp
                compact["body_evicted_at"] = persisted_at
                temporary = path.with_suffix(path.suffix + ".tmp")
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                try:
                    with os.fdopen(descriptor, "wb") as stream:
                        descriptor = -1
                        stream.write(_dump(compact).encode("utf-8"))
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, path)
                    directory = os.open(
                        path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                    )
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    if descriptor != -1:
                        os.close(descriptor)
                    temporary.unlink(missing_ok=True)
                compacted += 1
            except FileNotFoundError:
                continue
            except (OSError, TypeError, ValueError, UnicodeError) as exc:
                mark_failed(ident, f"{type(exc).__name__}: {exc}")
        return compacted, errors, failed

    def _clear_json_compaction_pending(self, records) -> None:
        values = [(ident,) for ident in records if isinstance(ident, str)]
        if not values:
            return
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.executemany(
                "UPDATE entities SET data=json_remove("
                "data, '$.json_compaction_pending', '$.json_compaction_attempted_at') "
                "WHERE id=? AND json_valid(data) "
                "AND json_extract(data, '$.json_compaction_pending') = 1",
                values,
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def _mark_json_compaction_failed(self, records, timestamp: float) -> None:
        values = [(timestamp, ident) for ident in records if isinstance(ident, str)]
        if not values:
            return
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.executemany(
                "UPDATE entities SET data=json_set("
                "data, '$.json_compaction_attempted_at', ?) "
                "WHERE id=? AND json_valid(data) "
                "AND json_extract(data, '$.json_compaction_pending') = 1",
                values,
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def _vacuum_after_history_commit(self, now: float) -> dict[str, Any]:
        last = self._meta_value("last_vacuum_at")
        return vacuum_if_worthwhile(
            self._db,
            self.db_path,
            last_vacuum=float(last) if last else None,
            now=now,
            mail_migrated=self._mail_send_seq_migrated,
            save_last_vacuum=self._save_vacuum_time,
        )

    def _save_vacuum_time(self, value: float) -> None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._set_meta("last_vacuum_at", value)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def storage_gc_before_delete(self, candidates) -> list[str]:
        paths = [item["path"] for item in candidates]
        snapshot = self.storage_gc_snapshot(paths=paths)
        if snapshot.get("uncertain"):
            return []
        protected = set(snapshot["protected_paths"])
        paths = [item["path"] for item in candidates if item["path"] not in protected]
        return self.mark_storage_evicted(paths)

    def mark_storage_evicted(self, paths: list[str]) -> list[str]:
        """Preserve entity identity and deduplication while expiring owned data."""
        self._ensure_open()
        selected = set(paths)
        marked: set[str] = set()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for row in self._db.execute(
                    "SELECT id, data FROM entities ORDER BY entity_seq"
                ):
                    record, warning = _decode_entity(row)
                    if warning is not None:
                        self._db.execute("ROLLBACK")
                        return []
                    for field in ("result_ref", "artifacts"):
                        if isinstance(record.get(field), str):
                            try:
                                record[field] = json.loads(record[field])
                            except (TypeError, ValueError, UnicodeError):
                                self._db.execute("ROLLBACK")
                                return []
                    if record.get("state") not in _TERMINAL_STATES:
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
                        output_paths, warning = self._scan_paths_with_status(
                            ident,
                            metadata_optional=(
                                bool(record.get("scan_output_evicted"))
                                or record.get("state") in {"failed", "cancelled", "lost", "reset"}
                            ),
                        )
                        if warning is not None:
                            self._db.execute("ROLLBACK")
                            return []
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
                            artifact_evicted=True,
                            artifact_evicted_at=time.time(),
                        )
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
                        marked.update(
                            path
                            for path in selected
                            if path.endswith(".idx")
                            and path.removesuffix(".idx") + ".jsonl" in output_paths
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
        updated_at: float | None = None,
        preserve_updated: bool = False,
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
        if type(preserve_updated) is not bool:
            raise TypeError("preserve_updated must be a boolean")
        if storage_id != ident:
            record = {**record, "history_id": storage_id}
        now = time.time() if updated_at is None else updated_at
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(now)
        ):
            raise TypeError("updated_at must be a number or None")
        now = float(now)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    "SELECT entity_seq, kind, updated, data FROM entities WHERE id = ?",
                    (storage_id,),
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
                    if preserve_updated:
                        now = float(row["updated"])
                    if row["kind"] != kind:
                        raise ValueError(f"entity {storage_id!r} already has kind {row['kind']!r}")
                    try:
                        previous = _load(row["data"])
                    except (TypeError, ValueError, UnicodeError) as exc:
                        raise ValueError(
                            f"cannot update corrupt history entity {storage_id!r}"
                        ) from exc
                    merged = {**previous, **record}
                    if previous.get("body_evicted") is True:
                        merged = {
                            key: value
                            for key, value in merged.items()
                            if key not in _BULKY_ENTITY_FIELDS
                        }
                        merged["body_evicted"] = True
                        if "body_evicted_at" in previous:
                            merged["body_evicted_at"] = previous["body_evicted_at"]
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
            watermark = int(self._meta_value("pruned_through_seq") or 0)
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
        result["history_truncated"] = watermark > 0
        result["pruned_through_seq"] = watermark
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
    return json_text(value, separators=(",", ":"), default=str)


def _sha256_text(value: str) -> str:
    return sha256_text(value)


def _sha256_optional_text(value: Any) -> str | None:
    return None if value is None else _sha256_text(str(value))


def _finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (OverflowError, TypeError, ValueError):
        return False


def _event_owner(row: sqlite3.Row) -> str | None:
    data = row["data"]
    if data is not None:
        try:
            payload = json.loads(data)
        except (TypeError, ValueError, UnicodeError):
            return None
        if isinstance(payload, Mapping):
            history_id = payload.get("history_id")
            if isinstance(history_id, str) and history_id:
                return history_id
    ident = row["id"]
    if ident is not None:
        return str(ident)
    exec_id = _row_value(row, "exec_id")
    if exec_id is not None:
        return str(exec_id)
    if data is not None:
        try:
            payload = json.loads(data)
        except (TypeError, ValueError, UnicodeError):
            return None
        if isinstance(payload, Mapping):
            payload_exec_id = payload.get("exec_id")
            if isinstance(payload_exec_id, str) and payload_exec_id:
                return payload_exec_id
    return None


def _history_entity_matches(row: sqlite3.Row, item: Mapping[str, Any]) -> bool:
    return (
        int(row["entity_seq"]) == item.get("entity_seq")
        and str(row["kind"]) == item.get("kind")
        and float(row["updated"]) == float(item.get("updated"))
        and _sha256_text(str(row["data"])) == item.get("data_sha256")
    )


def _history_event_matches(row: sqlite3.Row, item: Mapping[str, Any]) -> bool:
    return (
        float(row["time"]) == float(item.get("time"))
        and row["id"] == item.get("id")
        and (item.get("kind") is None or row["kind"] == item.get("kind"))
        and _sha256_optional_text(row["data"]) == item.get("data_sha256")
    )


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
            encoded = value.encode("utf-8", "backslashreplace")[:room]
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
