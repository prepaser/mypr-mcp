"""Bounded, reference-aware maintenance for workspace runtime storage.

The manager owns the lifecycle that decides when to call this service.  This
module only discovers candidates, builds revalidated plans, and applies safe
deletions.  Output records that need a history tombstone are never deleted
until the caller explicitly acknowledges that marker.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import stat
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any

from .storage_lock import StorageLock

DEFAULT_AGE_DAYS = 30
DEFAULT_MAX_BYTES = 1024 * 1024 * 1024
DEFAULT_REVISION_KEEP = 50
DAY = 24 * 60 * 60
_MAX_FILES = 100_000
_MAX_HASH_BYTES = 64 * 1024 * 1024
_MAX_JSON_BYTES = 16 * 1024 * 1024
_MAX_PLAN_COUNT = 16
_MAX_PLAN_AGE = 60 * 60
_MAX_PLAN_CANDIDATES = 4096
_MAX_PUBLIC_ITEMS = 1024
_TERMINAL = {"succeeded", "failed", "cancelled", "lost", "reset", "complete", "completed"}
_REVISION = set("0123456789abcdef")
_PROTECTED_TOP_LEVEL = {
    "venv",
    "lib",
    "skills",
    "config.toml",
    "requirements.txt",
    "runtime.json",
    "history.sqlite3",
    "history.sqlite3-shm",
    "history.sqlite3-wal",
    "manager.lock",
    "startup.lock",
    "storage.lock",
}
_CATEGORIES = {
    "runs": "runs",
    "jobs": "jobs",
    "scans": "scans",
    "searches": "snapshots",
    "git": "snapshots",
    "git-history": "snapshots",
    "document-results": "document_results",
    "documents": "document_results",
    "html-results": "document_results",
    "change-plans": "change_plans",
    "rewrites": "change_plans",
    "task-results": "task_results",
    "results": "task_results",
    "artifacts": "artifacts",
    "revisions": "revisions",
}


@dataclass(frozen=True, slots=True)
class _Entry:
    path: Path
    relative: str
    category: str
    size: int
    mtime_ns: int
    inode: int
    digest: str | None


@dataclass(frozen=True, slots=True)
class _Record:
    ident: str
    state: str | None
    active: bool
    paths: frozenset[Path]


class Storage:
    """Plan and apply bounded cleanup below one workspace's ``.mypr`` root.

    An optional history adapter may provide ``storage_gc_snapshot()`` with
    ``active_ids``, ``protected_paths``, ``references`` and ``tombstones``.
    Before deleting output files, ``storage_gc_before_delete(candidates)`` may
    persist eviction metadata and return the paths whose markers were stored.
    The callback must finish its durable write before returning.
    """

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        *,
        history: Any = None,
        active_ids: Callable[[], Iterable[str]] | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.root = (self.workspace / ".mypr").resolve(strict=False)
        try:
            self.root.relative_to(self.workspace)
        except ValueError as exc:
            raise ValueError("workspace storage escapes the workspace") from exc
        self.lock_path = self.root / "storage.lock"
        self.history = history
        self.active_ids = active_ids
        self._now = now or time.time
        self._plans: dict[str, dict[str, Any]] = {}
        self._reference_scan_truncated = False
        self._active_ids_truncated = False

    async def usage(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._usage_sync)

    async def gc(
        self,
        *,
        dry_run: bool = True,
        older_than_days: int = DEFAULT_AGE_DAYS,
        max_bytes: int | None = DEFAULT_MAX_BYTES,
        revision_keep: int = DEFAULT_REVISION_KEEP,
    ) -> dict[str, Any]:
        """Build a bounded cleanup plan, optionally applying safe candidates.

        ``dry_run=False`` still leaves output candidates in place until the
        caller supplies their history tombstones to :meth:`gc_apply`.
        """

        if type(dry_run) is not bool:
            raise TypeError("dry_run must be a boolean")
        options = self._validate_options(older_than_days, max_bytes, revision_keep)
        active = self._active_ids()
        plan = await asyncio.to_thread(self._plan_sync, options, active)
        self._remember_plan(plan)
        if dry_run:
            return self._public_plan(plan)
        return await self.gc_apply(plan["plan_id"])

    async def gc_apply(
        self,
        plan_id: str,
        *,
        tombstones: Iterable[str] = (),
    ) -> dict[str, Any]:
        if not isinstance(plan_id, str) or not plan_id:
            raise ValueError("plan_id must be a non-empty string")
        plan = self._plans.get(plan_id)
        if plan is None:
            raise ValueError("storage GC plan has expired or does not exist")
        if self._now() - plan["created_at"] > _MAX_PLAN_AGE:
            self._plans.pop(plan_id, None)
            raise ValueError("storage GC plan has expired or does not exist")
        acknowledged = self._validate_tombstones(tombstones)
        allowed = {item["path"] for item in plan["tombstones"]}
        unknown = acknowledged - allowed
        if unknown:
            raise ValueError(f"tombstone is not part of this GC plan: {sorted(unknown)[0]}")
        result = await asyncio.to_thread(self._apply_sync, plan, acknowledged)
        self._plans.pop(plan_id, None)
        return result

    def _active_ids(self) -> set[str]:
        if self.active_ids is None:
            return set()
        values = self.active_ids()
        if not isinstance(values, Iterable):
            return set()
        selected = list(islice(values, _MAX_PLAN_CANDIDATES + 1))
        self._active_ids_truncated = len(selected) > _MAX_PLAN_CANDIDATES
        return {
            value
            for value in selected[:_MAX_PLAN_CANDIDATES]
            if isinstance(value, str) and value
        }

    def _remember_plan(self, plan: dict[str, Any]) -> None:
        now = self._now()
        for ident, old in tuple(self._plans.items()):
            if now - old["created_at"] > _MAX_PLAN_AGE:
                self._plans.pop(ident, None)
        self._plans[plan["plan_id"]] = plan
        while len(self._plans) > _MAX_PLAN_COUNT:
            oldest = min(self._plans, key=lambda ident: self._plans[ident]["created_at"])
            self._plans.pop(oldest, None)

    def _workspace_identity(self) -> str:
        info = self.workspace.stat()
        if not stat.S_ISDIR(info.st_mode):
            raise NotADirectoryError(self.workspace)
        return f"{info.st_dev:x}:{info.st_ino:x}"

    def _history_snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "active_ids": set(),
            "protected_paths": set(),
            "references": {},
            "references_truncated": False,
            "tombstones": {},
            "error": None,
        }
        method = getattr(self.history, "storage_gc_snapshot", None)
        if not callable(method):
            return result
        try:
            raw = method()
        except Exception as exc:
            result["error"] = f"history snapshot failed: {type(exc).__name__}"
            return result
        if not isinstance(raw, Mapping):
            result["error"] = "history snapshot was not an object"
            return result
        active_source = raw.get("active_ids", ())
        if not isinstance(active_source, Iterable):
            active_source = ()
        active_values = list(islice(active_source, _MAX_PLAN_CANDIDATES + 1))
        if len(active_values) > _MAX_PLAN_CANDIDATES:
            result["references_truncated"] = True
        result["active_ids"] = {
            value
            for value in active_values[:_MAX_PLAN_CANDIDATES]
            if isinstance(value, str) and value
        }
        protected_source = raw.get("protected_paths", ())
        if not isinstance(protected_source, Iterable):
            protected_source = ()
        protected_values = list(islice(protected_source, _MAX_PLAN_CANDIDATES + 1))
        if len(protected_values) > _MAX_PLAN_CANDIDATES:
            result["references_truncated"] = True
        result["protected_paths"] = self._safe_relative_set(protected_values[:_MAX_PLAN_CANDIDATES])
        references = raw.get("references", {})
        if isinstance(references, Mapping):
            if len(references) > _MAX_PLAN_CANDIDATES:
                result["references_truncated"] = True
            items = list(islice(references.items(), _MAX_PLAN_CANDIDATES))
            for path, owners in items:
                if not isinstance(path, str) or not isinstance(owners, Iterable):
                    continue
                owner_values = list(islice(owners, 65))
                if len(owner_values) > 64:
                    result["references_truncated"] = True
                result["references"][path] = [
                    str(owner) for owner in owner_values[:64] if owner is not None
                ]
        tombstones = raw.get("tombstones", {})
        if isinstance(tombstones, Mapping):
            result["tombstones"] = {
                path: dict(value)
                for path, value in tombstones.items()
                if isinstance(path, str) and isinstance(value, Mapping)
            }
        return result

    @staticmethod
    def _safe_relative_set(values: Iterable[Any]) -> set[str]:
        result = set()
        for value in values:
            if not isinstance(value, str):
                continue
            path = Path(value)
            if path.is_absolute() or ".." in path.parts or "\\" in value:
                continue
            result.add(path.as_posix())
        return result

    @staticmethod
    def _validate_options(older_than_days, max_bytes, revision_keep) -> dict[str, Any]:
        if type(older_than_days) is not int or older_than_days < 1:
            raise ValueError("older_than_days must be a positive integer")
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
            raise ValueError("max_bytes must be a non-negative integer or None")
        if type(revision_keep) is not int or revision_keep < 1:
            raise ValueError("revision_keep must be a positive integer")
        return {
            "older_than_days": older_than_days,
            "max_bytes": max_bytes,
            "revision_keep": revision_keep,
        }

    def _usage_sync(self) -> dict[str, Any]:
        with StorageLock(self.lock_path):
            entries, truncated = self._entries()
        return self._usage(entries, truncated)

    def _plan_sync(self, options: dict[str, Any], active_ids: set[str]) -> dict[str, Any]:
        with StorageLock(self.lock_path):
            entries, truncated = self._entries()
            history_state = self._history_snapshot()
            active = active_ids | set(history_state["active_ids"])
            records = self._records(entries, active)
            protected = self._protected(entries, records, history_state)
            candidates = self._candidates(entries, records, protected, options, history_state)
            usage = self._usage(entries, truncated)
            selected = self._select(candidates, usage["total_bytes"], options["max_bytes"])
            revision_prunable = self._revision_prune_preview(entries, options["revision_keep"])
            selected, candidate_truncated = self._limit_candidates(selected)
            selected = self._hydrate_candidates(selected)
            projected = usage["total_bytes"] - sum(item["size"] for item in selected)
            plan_id = secrets.token_hex(16)
            tombstones = [
                {
                    "path": item["path"],
                    "category": item["category"],
                    "reason": item["reason"],
                }
                for item in selected
                if item["requires_tombstone"]
            ]
            return {
                "plan_id": plan_id,
                "created_at": self._now(),
                "workspace_id": self._workspace_identity(),
                "options": options,
                "usage": usage,
                "scan_truncated": truncated,
                "candidate_truncated": candidate_truncated,
                "revision_prunable": revision_prunable,
                "quota": {
                    "max_bytes": options["max_bytes"],
                    "projected_bytes": projected,
                    "over_budget": (
                        None
                        if truncated or candidate_truncated
                        else options["max_bytes"] is not None
                        and projected > options["max_bytes"]
                    ),
                },
                "candidates": selected,
                "tombstones": tombstones,
                "protected": protected,
            }

    def _apply_sync(self, plan: dict[str, Any], acknowledged: set[str]) -> dict[str, Any]:
        deleted: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        with StorageLock(self.lock_path):
            if plan.get("workspace_id") != self._workspace_identity():
                return {
                    "plan_id": plan["plan_id"],
                    "deleted": [],
                    "skipped": [{"path": "", "reason": "workspace_identity_changed"}],
                    "deleted_bytes": 0,
                    "tombstones_required": plan["tombstones"],
                }
            try:
                revision_pruned = self._prune_revision_indexes(plan["options"]["revision_keep"])
            except Exception as exc:
                return {
                    "plan_id": plan["plan_id"],
                    "deleted": [],
                    "skipped": [
                        {"path": ".mypr/revisions", "reason": "revision_prune_failed"}
                    ],
                    "deleted_bytes": 0,
                    "remaining_bytes": None,
                    "over_budget": None,
                    "revision_pruned": [],
                    "error": f"{type(exc).__name__}: {exc}",
                    "tombstones_required": plan["tombstones"],
                }
            entries, scan_truncated = self._entries()
            history_state = self._history_snapshot()
            active = self._active_ids() | set(history_state["active_ids"])
            records = self._records(entries, active)
            protected = self._protected(entries, records, history_state)
            current_candidates = {
                item["path"]: item
                for item in self._candidates(
                    entries, records, protected, plan["options"], history_state
                )
            }
            apply_items = {item["path"]: item for item in plan["candidates"]}
            prevalidated: dict[str, tuple[dict[str, Any], _Entry, dict[str, Any]]] = {}
            planned_groups: dict[str, set[str]] = defaultdict(set)
            for item in apply_items.values():
                planned_groups[item["group"]].add(item["path"])
                path = self.workspace / item["path"]
                current_plan = current_candidates.get(item["path"])
                if current_plan is None:
                    skipped.append({**item, "reason": "protected_or_expired"})
                    continue
                if not self._inside_workspace(path):
                    skipped.append({**item, "reason": "workspace_identity_changed"})
                    continue
                current = self._entry(
                    path,
                    item["category"],
                    with_digest=item.get("digest") is not None,
                )
                if current is None:
                    skipped.append({**item, "reason": "already_absent"})
                    continue
                if not self._same_entry(current, item):
                    skipped.append({**item, "reason": "changed_since_plan"})
                    continue
                prevalidated[item["path"]] = (item, current, current_plan)
            failed_paths = set(apply_items) - set(prevalidated)
            current_groups: dict[str, set[str]] = defaultdict(set)
            for item in current_candidates.values():
                current_groups[item["group"]].add(item["path"])
            invalid_groups = {
                group
                for group, paths in planned_groups.items()
                if (
                    len(paths) > 1
                    and (
                        paths & failed_paths
                        or current_groups.get(group, set()) != paths
                    )
                )
            }
            for group in invalid_groups:
                for path in planned_groups[group]:
                    prevalidated.pop(path, None)
                    if path not in failed_paths:
                        item = apply_items[path]
                        skipped.append({**item, "reason": "paired_file_invalid"})
            marker_candidates = [
                current_plan
                for item, _, current_plan in prevalidated.values()
                if current_plan["requires_tombstone"]
            ]
            marker = getattr(self.history, "storage_gc_before_delete", None)
            marker_failed = False
            if marker_candidates and callable(marker):
                try:
                    acknowledged.update(self._safe_relative_set(marker(marker_candidates)))
                except Exception:
                    marker_failed = True
            for item, _, current_plan in prevalidated.values():
                if (
                    current_plan["requires_tombstone"]
                    and item["path"] not in acknowledged
                    and (marker_failed or not callable(marker))
                ):
                    skipped.append(
                        {
                            **item,
                            "reason": (
                                "history_marker_failed"
                                if marker_failed
                                else "tombstone_required"
                            ),
                        }
                    )
                    continue
                if current_plan["requires_tombstone"] and item["path"] not in acknowledged:
                    skipped.append({**item, "reason": "tombstone_required"})
                    continue
                path = self.workspace / item["path"]
                try:
                    current = self._entry(
                        path,
                        item["category"],
                        with_digest=item.get("digest") is not None,
                    )
                except OSError as exc:
                    skipped.append({**item, "reason": f"stat_failed: {type(exc).__name__}"})
                    continue
                if current is None:
                    skipped.append({**item, "reason": "already_absent"})
                    continue
                if not self._inside_workspace(path):
                    skipped.append({**item, "reason": "workspace_identity_changed"})
                    continue
                if not self._same_entry(current, item):
                    skipped.append({**item, "reason": "changed_since_plan"})
                    continue
                try:
                    path.unlink()
                except OSError as exc:
                    skipped.append({**item, "reason": f"delete_failed: {type(exc).__name__}"})
                else:
                    deleted.append(item)
        remaining_usage = self._usage(*self._entries())
        return {
            "plan_id": plan["plan_id"],
            "deleted": deleted,
            "skipped": skipped,
            "deleted_bytes": sum(item["size"] for item in deleted),
            "tombstones_required": plan["tombstones"],
            "scan_truncated": scan_truncated,
            "revision_pruned": revision_pruned,
            "remaining_bytes": remaining_usage["total_bytes"],
            "over_budget": (
                None
                if scan_truncated or remaining_usage["truncated"]
                else plan["options"]["max_bytes"] is not None
                and remaining_usage["total_bytes"] > plan["options"]["max_bytes"]
            ),
        }

    def _entries(self) -> tuple[list[_Entry], bool]:
        if not self.root.is_dir():
            return [], False
        entries: list[_Entry] = []
        truncated = False
        for directory, dirnames, filenames in os.walk(self.root, followlinks=False):
            dirnames[:] = [name for name in dirnames if name not in {"venv", "lib", "skills"}]
            for name in filenames:
                if len(entries) >= _MAX_FILES:
                    truncated = True
                    break
                path = Path(directory) / name
                try:
                    relative = self._relative(path)
                except ValueError:
                    continue
                category = self._category(relative)
                if category is None:
                    continue
                entry = self._entry(path, category)
                if entry is not None:
                    entries.append(entry)
            if truncated:
                break
        return entries, truncated

    def _entry(
        self,
        path: Path,
        category: str | None = None,
        *,
        with_digest: bool = False,
    ) -> _Entry | None:
        try:
            info = path.lstat()
        except OSError:
            return None
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            return None
        try:
            relative = self._relative(path)
        except ValueError:
            return None
        digest = self._digest(path, info.st_size) if with_digest else None
        if with_digest and digest is not None:
            try:
                current = path.lstat()
            except OSError:
                return None
            if (
                current.st_ino != info.st_ino
                or current.st_size != info.st_size
                or current.st_mtime_ns != info.st_mtime_ns
            ):
                return None
        return _Entry(
            path,
            relative,
            category or self._category(relative) or "other",
            info.st_size,
            info.st_mtime_ns,
            info.st_ino,
            digest,
        )

    def _relative(self, path: Path) -> str:
        return path.resolve(strict=False).relative_to(self.workspace).as_posix()

    def _inside_workspace(self, path: Path) -> bool:
        try:
            path.resolve(strict=False).relative_to(self.workspace)
        except ValueError:
            return False
        return True

    @staticmethod
    def _category(relative: str) -> str | None:
        parts = Path(relative).parts
        if len(parts) < 2 or parts[0] != ".mypr":
            return None
        if parts[1] in _PROTECTED_TOP_LEVEL:
            return None
        return _CATEGORIES.get(parts[1])

    @staticmethod
    def _digest(path: Path, size: int) -> str | None:
        if size > _MAX_HASH_BYTES:
            return None
        digest = hashlib.sha256()
        read = 0
        try:
            with path.open("rb") as stream:
                while read < _MAX_HASH_BYTES and (
                    chunk := stream.read(min(1024 * 1024, _MAX_HASH_BYTES - read))
                ):
                    read += len(chunk)
                    digest.update(chunk)
        except OSError:
            return None
        return digest.hexdigest()

    def _records(self, entries: list[_Entry], active_ids: set[str]) -> list[_Record]:
        records: list[_Record] = []
        self._reference_scan_truncated = False
        for entry in entries:
            if entry.category not in {"runs", "jobs", "scans"} or not entry.relative.endswith(
                ".json"
            ):
                continue
            if entry.relative.endswith((".summary.json", ".request.json")):
                continue
            payload = self._read_json(entry.path)
            if not isinstance(payload, Mapping):
                continue
            ident = str(payload.get("id") or entry.path.stem)
            state = payload.get("state")
            state = str(state) if state is not None else None
            paths = frozenset(self._referenced_paths(payload))
            active = ident in active_ids or state not in _TERMINAL
            records.append(_Record(ident, state, active, paths))
        return records

    def _protected(
        self,
        entries: list[_Entry],
        records: list[_Record],
        history_state: Mapping[str, Any],
    ) -> dict[str, Any]:
        protected_paths = {
            entry.relative for entry in entries if entry.relative.endswith(".lock")
        }
        protected_paths.update(history_state.get("protected_paths", set()))
        active_ids = {record.ident for record in records if record.active}
        references: dict[str, set[str]] = defaultdict(set)
        active_references: set[str] = set()
        for record in records:
            for path in record.paths:
                relative = self._relative(path)
                references[relative].add(record.ident)
                if record.active:
                    active_references.add(relative)
        for path, owners in history_state.get("references", {}).items():
            if not self._safe_relative_set((path,)):
                continue
            references[path].update(owners)
            if any(owner in active_ids for owner in owners):
                active_references.add(path)
        if history_state.get("error"):
            for entry in entries:
                if entry.category in {"runs", "jobs", "scans", "artifacts", "task_results"}:
                    protected_paths.add(entry.relative)
        if history_state.get("references_truncated"):
            for entry in entries:
                if entry.category in {"artifacts", "task_results"}:
                    protected_paths.add(entry.relative)
        if self._reference_scan_truncated:
            for entry in entries:
                if entry.category in {"artifacts", "task_results"}:
                    protected_paths.add(entry.relative)
        if self._active_ids_truncated:
            for entry in entries:
                if entry.category in {"runs", "jobs", "scans", "artifacts", "task_results"}:
                    protected_paths.add(entry.relative)
        for entry in entries:
            if entry.category in {"runs", "jobs", "scans"} and entry.relative.endswith(".json"):
                if not entry.relative.endswith((".summary.json", ".request.json")):
                    protected_paths.add(entry.relative)
            if entry.category in {"runs", "jobs", "scans"}:
                if any(
                    record.active
                    and entry.path.stem == record.ident
                    and entry.category == self._category(f".mypr/{entry.category}")
                    for record in records
                ):
                    active_references.add(entry.relative)
        return {
            "paths": sorted(protected_paths | active_references),
            "active_ids": sorted(active_ids),
            "references": {key: sorted(value) for key, value in references.items()},
            "tombstones": dict(history_state.get("tombstones", {})),
        }

    def _candidates(
        self,
        entries: list[_Entry],
        records: list[_Record],
        protected: dict[str, Any],
        options: dict[str, Any],
        history_state: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        protected_paths = set(protected["paths"])
        references = protected["references"]
        record_by_id = {record.ident: record for record in records}
        revision_refs = self._revision_refs(entries, options["revision_keep"])
        revision_protect_all = revision_refs is None
        if revision_protect_all:
            revision_refs = set()
        cutoff = self._now() - options["older_than_days"] * DAY
        entries_by_path = {entry.relative: entry for entry in entries}
        candidates: list[dict[str, Any]] = []
        for entry in entries:
            group = entry.relative
            if entry.relative in protected_paths or entry.path.name.endswith(".lock"):
                continue
            if entry.size > _MAX_HASH_BYTES:
                continue
            expired = entry.mtime_ns / 1_000_000_000 <= cutoff
            if entry.category == "revisions" and entry.path.parent.name == "objects":
                if revision_protect_all or entry.path.name in revision_refs:
                    continue
                reason = "unreferenced_revision_object"
                requires_tombstone = False
            elif entry.category in {"runs", "jobs"} and entry.relative.endswith(".jsonl"):
                reason = "completed_output"
                requires_tombstone = True
            elif entry.category == "scans" and entry.relative.endswith((".jsonl", ".xml")):
                reason = "completed_scan_output"
                requires_tombstone = True
            elif entry.category == "artifacts":
                owners = references.get(entry.relative, [])
                if not owners or any(
                    record_by_id.get(owner, _Record("", None, True, frozenset())).active
                    for owner in owners
                ):
                    continue
                reason = "generated_artifact"
                requires_tombstone = True
            elif entry.category == "task_results":
                owners = references.get(entry.relative, [])
                if not owners and not expired:
                    continue
                reason = "persisted_task_result" if owners else "orphan_task_result"
                requires_tombstone = bool(owners)
            elif entry.category in {
                "snapshots",
                "document_results",
                "change_plans",
            }:
                group = entry.relative
                if entry.category == "document_results":
                    if entry.relative.endswith(".resume"):
                        partner = entry.relative.removesuffix(".resume") + ".json"
                    elif entry.relative.endswith(".json"):
                        partner = entry.relative.removesuffix(".json") + ".resume"
                    else:
                        continue
                    partner_entry = entries_by_path.get(partner)
                    if (
                        partner_entry is None
                        or partner in protected_paths
                    ):
                        continue
                    group = min(entry.relative, partner)
                reason = f"expired_{entry.category}"
                requires_tombstone = False
            else:
                continue
            candidates.append(
                {
                    "path": entry.relative,
                    "category": entry.category,
                    "size": entry.size,
                    "mtime_ns": entry.mtime_ns,
                    "inode": entry.inode,
                    "digest": None,
                    "reason": reason,
                    "requires_tombstone": requires_tombstone
                    and entry.relative not in history_state.get("tombstones", {}),
                    "expired": expired,
                    "group": group,
                }
            )
        return sorted(
            candidates,
            key=lambda item: (not item["expired"], item["mtime_ns"], item["path"]),
        )

    @staticmethod
    def _limit_candidates(
        candidates: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        grouped: list[list[dict[str, Any]]] = []
        for item in candidates:
            if not grouped or grouped[-1][0]["group"] != item["group"]:
                grouped.append([])
            grouped[-1].append(item)
        limited: list[dict[str, Any]] = []
        for group in grouped:
            if len(limited) + len(group) > _MAX_PLAN_CANDIDATES:
                continue
            limited.extend(group)
        return limited, len(limited) != len(candidates)

    def _hydrate_candidates(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        hydrated: list[dict[str, Any]] = []
        invalid_groups: set[str] = set()
        for item in candidates:
            path = self.workspace / item["path"]
            entry = self._entry(path, item["category"], with_digest=True)
            if entry is None or not self._same_entry(entry, item):
                invalid_groups.add(item["group"])
                continue
            if entry.digest is None:
                invalid_groups.add(item["group"])
                continue
            hydrated.append({**item, "digest": entry.digest})
        return [item for item in hydrated if item["group"] not in invalid_groups]

    def _revision_prune_preview(self, entries: list[_Entry], keep: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for entry in entries:
            if entry.category != "revisions" or not entry.relative.startswith(
                ".mypr/revisions/index/"
            ):
                continue
            payload = self._read_json(entry.path, max_bytes=16 * 1024 * 1024)
            if not isinstance(payload, Mapping) or payload.get("version") != 2:
                continue
            records = payload.get("revisions")
            if not isinstance(records, list) or len(records) <= keep:
                continue
            result.append(
                {
                    "path": entry.relative,
                    "remove_count": len(records) - keep,
                    "high_water": payload.get("next_sequence"),
                }
            )
        return result[:_MAX_PUBLIC_ITEMS]

    def _prune_revision_indexes(self, keep: int) -> list[dict[str, Any]]:
        pruned: list[dict[str, Any]] = []
        for path in self._revision_index_paths():
            payload = self._read_json(path, max_bytes=16 * 1024 * 1024)
            if not isinstance(payload, Mapping):
                continue
            migrated = False
            if payload.get("version") == 1:
                if not self._valid_v1_index(payload):
                    continue
                payload = self._upgrade_v1_index(payload)
                migrated = True
            if payload.get("version") != 2:
                continue
            records = payload.get("revisions")
            if not isinstance(records, list):
                continue
            if not self._valid_v2_index(payload):
                continue
            resource = payload.get("resource")
            keep_sequences = {item["sequence"] for item in records[-keep:]}
            state, current = self._current_state(resource)
            if state == "unknown":
                continue
            if state == "present" and current is not None:
                matches = [item for item in records if item["revision"] == current]
                if matches:
                    keep_sequences.add(max(matches, key=lambda item: item["sequence"])["sequence"])
            elif state == "absent":
                matches = [item for item in records if item.get("absent") is True]
                if matches:
                    keep_sequences.add(max(matches, key=lambda item: item["sequence"])["sequence"])
            retained = [item for item in records if item["sequence"] in keep_sequences]
            removed = [item for item in records if item["sequence"] not in keep_sequences]
            if not removed and not migrated:
                continue
            updated = dict(payload)
            updated["revisions"] = retained
            updated["count"] = len(retained)
            first_sequence = min(item["sequence"] for item in retained)
            removed_before = [
                item["sequence"] for item in removed if item["sequence"] < first_sequence
            ]
            updated["pruned_before"] = min(
                max(
                int(payload.get("pruned_before", 0)),
                max(removed_before, default=0),
                ),
                first_sequence - 1,
            )
            self._atomic_json(path, updated)
            pruned.append(
                {
                    "path": self._relative(path),
                    "removed": len(removed),
                    "high_water": updated.get("next_sequence"),
                    "migrated": migrated,
                }
            )
        return pruned

    @staticmethod
    def _upgrade_v1_index(payload: Mapping[str, Any]) -> dict[str, Any]:
        updated = dict(payload)
        updated["version"] = 2
        records = [dict(item) for item in payload["revisions"]]
        updated["revisions"] = records
        updated["count"] = len(records)
        updated["next_sequence"] = records[-1]["sequence"] + 1 if records else 1
        updated["pruned_before"] = 0
        return updated

    def _revision_index_paths(self) -> list[Path]:
        root = self.root / "revisions" / "index"
        if not root.is_dir():
            return []
        paths: list[Path] = []
        for directory, _, filenames in os.walk(root, followlinks=False):
            for name in filenames:
                path = Path(directory) / name
                if path.suffix == ".json" and not path.is_symlink():
                    paths.append(path)
        return paths

    def _valid_v2_index(self, payload: Mapping[str, Any]) -> bool:
        next_sequence = payload.get("next_sequence")
        if type(next_sequence) is not int or next_sequence < 1:
            return False
        previous = 0
        for item in payload["revisions"]:
            revision = item.get("revision") if isinstance(item, Mapping) else None
            absent = isinstance(item, Mapping) and item.get("absent") is True
            if (
                not isinstance(item, Mapping)
                or type(item.get("sequence")) is not int
                or item["sequence"] <= previous
                or item["sequence"] >= next_sequence
                or not isinstance(revision, str)
                or type(item.get("size")) is not int
                or item["size"] < 0
                or not isinstance(item.get("created_at"), str)
                or (
                    absent
                    and (revision != "absent" or item.get("size") != 0)
                )
                or (
                    not absent
                    and (len(revision) != 64 or any(char not in _REVISION for char in revision))
                )
            ):
                return False
            previous = item["sequence"]
        return type(payload.get("count")) is int and payload["count"] == len(
            payload["revisions"]
        )

    def _current_state(self, resource: Any) -> tuple[str, str | None]:
        if not isinstance(resource, str):
            return "unknown", None
        path = (self.workspace / resource).resolve(strict=False)
        if not self._inside_workspace(path):
            return "unknown", None
        entry = self._entry(path, with_digest=True)
        if entry is None:
            if not path.exists():
                return "absent", None
            return "unknown", None
        if entry.digest is None:
            return "unknown", None
        return "present", entry.digest

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > 16 * 1024 * 1024:
            raise ValueError("revision index exceeds its size limit")
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary_path = Path(temporary)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary_path.unlink(missing_ok=True)

    def _revision_refs(self, entries: list[_Entry], keep: int) -> set[str] | None:
        refs: set[str] = set()
        indexes = [
            entry
            for entry in entries
            if entry.category == "revisions" and entry.relative.startswith(".mypr/revisions/index/")
        ]
        for entry in indexes:
            payload = self._read_json(entry.path, max_bytes=16 * 1024 * 1024)
            if not isinstance(payload, Mapping):
                return None
            version = payload.get("version")
            if version == 2 and not self._valid_v2_index(payload):
                return None
            if version == 1 and not self._valid_v1_index(payload):
                return None
            if version not in {1, 2}:
                return None
            revisions = payload.get("revisions")
            if not isinstance(revisions, list):
                return None
            valid = [
                item
                for item in revisions
                if (
                    isinstance(item, Mapping)
                    and isinstance(item.get("revision"), str)
                    and type(item.get("sequence")) is int
                    and item["sequence"] > 0
                )
            ]
            if len(valid) != len(revisions):
                return None
            for item in sorted(valid, key=lambda item: item["sequence"], reverse=True)[:keep]:
                if item["revision"] != "absent":
                    refs.add(item["revision"])
            resource = payload.get("resource")
            if isinstance(resource, str):
                state, current = self._current_state(resource)
                if state == "unknown":
                    return None
                if state == "present" and current is not None:
                    refs.add(current)
        return refs

    @staticmethod
    def _valid_v1_index(payload: Mapping[str, Any]) -> bool:
        records = payload.get("revisions")
        if not isinstance(records, list):
            return False
        if type(payload.get("count")) is not int or payload["count"] != len(records):
            return False
        for sequence, item in enumerate(records, 1):
            if (
                not isinstance(item, Mapping)
                or item.get("sequence") != sequence
                or not isinstance(item.get("revision"), str)
                or type(item.get("size")) is not int
                or item["size"] < 0
                or not isinstance(item.get("created_at"), str)
                or (
                    item.get("absent") is True
                    and (item["revision"] != "absent" or item.get("size") != 0)
                )
                or (
                    item.get("absent") is not True
                    and (
                        len(item["revision"]) != 64
                        or any(char not in _REVISION for char in item["revision"])
                    )
                )
            ):
                return False
        return True

    def _select(self, candidates, total_bytes: int, max_bytes: int | None):
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for candidate in candidates:
            groups[candidate["group"]].append(candidate)
        units = [
            {
                "items": items,
                "size": sum(item["size"] for item in items),
                "expired": all(item["expired"] for item in items),
                "mtime_ns": min(item["mtime_ns"] for item in items),
            }
            for items in groups.values()
        ]
        units.sort(key=lambda unit: (not unit["expired"], unit["mtime_ns"]))
        expired = [unit for unit in units if unit["expired"]]
        recent = [unit for unit in units if not unit["expired"]]
        if max_bytes is None:
            return [item for unit in expired for item in unit["items"]]
        selected = list(expired)
        remaining = total_bytes - sum(unit["size"] for unit in selected)
        for candidate in recent:
            if remaining <= max_bytes:
                break
            selected.append(candidate)
            remaining -= candidate["size"]
        return [item for unit in selected for item in unit["items"]]

    def _usage(self, entries: list[_Entry], truncated: bool) -> dict[str, Any]:
        categories: dict[str, dict[str, int]] = defaultdict(lambda: {"files": 0, "bytes": 0})
        for entry in entries:
            item = categories[entry.category]
            item["files"] += 1
            item["bytes"] += entry.size
        return {
            "total_files": len(entries),
            "total_bytes": sum(item["bytes"] for item in categories.values()),
            "truncated": truncated,
            "categories": dict(categories),
        }

    @staticmethod
    def _read_json(path: Path, *, max_bytes: int = _MAX_JSON_BYTES) -> Any:
        try:
            if path.stat().st_size > max_bytes:
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None

    def _referenced_paths(self, value: Any) -> set[Path]:
        found: set[Path] = set()
        queue: list[tuple[Any, int]] = [(value, 0)]
        visited = 0
        while queue and visited < 10_000:
            current, depth = queue.pop()
            visited += 1
            if depth > 16:
                continue
            if isinstance(current, Mapping):
                queue.extend((child, depth + 1) for child in islice(current.values(), 10_000))
            elif isinstance(current, list):
                queue.extend((child, depth + 1) for child in islice(current, 10_000))
            elif isinstance(current, str) and len(current) <= 4096:
                candidate = Path(current).expanduser()
                if not candidate.is_absolute():
                    candidate = self.workspace / candidate
                try:
                    resolved = candidate.resolve(strict=False)
                    resolved.relative_to(self.workspace)
                except (OSError, ValueError):
                    continue
                if resolved.is_file() and ".mypr" in resolved.parts:
                    found.add(resolved)
        if queue or visited >= 10_000:
            self._reference_scan_truncated = True
        return found

    @staticmethod
    def _same_entry(current: _Entry, planned: Mapping[str, Any]) -> bool:
        return (
            current.relative == planned["path"]
            and current.size == planned["size"]
            and current.mtime_ns == planned["mtime_ns"]
            and current.inode == planned["inode"]
            and (planned.get("digest") is None or current.digest == planned["digest"])
        )

    @staticmethod
    def _validate_tombstones(values: Iterable[str]) -> set[str]:
        if isinstance(values, (str, bytes)):
            raise TypeError("tombstones must be an iterable of relative paths")
        result = set()
        for value in values:
            if not isinstance(value, str) or not value:
                raise ValueError("tombstones must contain non-empty paths")
            path = Path(value)
            if path.is_absolute() or ".." in path.parts or "\\" in value:
                raise ValueError("tombstone path must stay inside the workspace")
            result.add(path.as_posix())
        return result

    @staticmethod
    def _public_plan(plan: dict[str, Any]) -> dict[str, Any]:
        result = {key: value for key, value in plan.items() if key not in {"created_at"}}
        candidates = list(result.get("candidates", ()))
        tombstones = list(result.get("tombstones", ()))
        protected = dict(result.get("protected", {}))
        paths = list(protected.get("paths", ()))
        references = dict(protected.get("references", {}))
        result["candidates"] = candidates[:_MAX_PUBLIC_ITEMS]
        result["tombstones"] = tombstones[:_MAX_PUBLIC_ITEMS]
        protected["paths"] = paths[:_MAX_PUBLIC_ITEMS]
        protected["references"] = dict(list(references.items())[:_MAX_PUBLIC_ITEMS])
        result["protected"] = protected
        result["public_truncated"] = any(
            (
                len(candidates) > _MAX_PUBLIC_ITEMS,
                len(tombstones) > _MAX_PUBLIC_ITEMS,
                len(paths) > _MAX_PUBLIC_ITEMS,
                len(references) > _MAX_PUBLIC_ITEMS,
            )
        )
        return result
