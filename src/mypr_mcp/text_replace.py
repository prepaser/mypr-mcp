"""Bounded preview/apply text replacements for workspace files."""

from __future__ import annotations

import asyncio
import base64
import difflib
import json
import stat
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from .change_plans import MAX_FILES, MAX_INPUT_OUTPUT_BYTES, ChangePlanError, ChangePlanStore
from .patching import _commit, _plan, _read_state, _reject_symlink_alias, _State

MAX_DIFF_BYTES = 32 * 1024
MAX_MATCHES = 10_000


async def replace(
    fs: Any,
    pattern: str,
    replacement: str,
    *,
    paths: str | list[str] | None = None,
    glob: str | list[str] | None = None,
    fixed: bool = True,
    ignore_case: bool = False,
    hidden: bool = False,
    no_ignore: bool = False,
    max_files: int = MAX_FILES,
    max_bytes: int = MAX_INPUT_OUTPUT_BYTES,
    timeout: float = 30,  # noqa: ASYNC109
    history: bool = True,
) -> dict[str, Any]:
    if not isinstance(pattern, str) or not pattern or "\x00" in pattern:
        raise ValueError("pattern must be a non-empty string without NUL")
    if not isinstance(replacement, str) or "\x00" in replacement:
        raise ValueError("replacement must be a string without NUL")
    if (
        type(fixed) is not bool
        or type(ignore_case) is not bool
        or type(hidden) is not bool
        or type(no_ignore) is not bool
        or type(history) is not bool
    ):
        raise TypeError("fixed, ignore_case, hidden, no_ignore, and history must be booleans")
    if type(max_files) is not int or not 1 <= max_files <= MAX_FILES:
        raise ValueError(f"max_files must be between 1 and {MAX_FILES}")
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_INPUT_OUTPUT_BYTES:
        raise ValueError(f"max_bytes must be between 1 and {MAX_INPUT_OUTPUT_BYTES}")
    if type(timeout) not in (int, float) or not 0 < timeout <= 300:
        raise ValueError("timeout must be between 0 and 300 seconds")
    if fs._shell is None:
        raise RuntimeError("text replacement requires the workspace shell")
    deadline = asyncio.get_running_loop().time() + float(timeout)
    async with asyncio.timeout(timeout):
        page = await fs.search(
            None,
            paths=paths,
            glob=glob,
            hidden=hidden,
            no_ignore=no_ignore,
            mode="files",
            max_matches=MAX_MATCHES,
            max_bytes=max_bytes,
            timeout=min(float(timeout), 30),
        )
        candidates = list(page.get("files", []))
        while page.get("has_more"):
            cursor = page.get("next_cursor")
            if not cursor:
                return _incomplete("missing search cursor")
            page = await fs.search(cursor=cursor, max_matches=MAX_MATCHES, max_bytes=max_bytes)
            candidates.extend(page.get("files", []))
        if not page.get("complete", False) or page.get("scan_truncated", False):
            return _incomplete(page.get("stop_reason") or "search_incomplete")
        if len(candidates) > MAX_MATCHES:
            return _incomplete("file_limit")
        unique = list(dict.fromkeys(value for value in candidates if isinstance(value, str)))
        request = {
            "root": str(fs.workspace),
            "paths": unique,
            "pattern": pattern,
            "replacement": replacement,
            "fixed": fixed,
            "ignore_case": ignore_case,
            "max_files": max_files,
            "max_bytes": max_bytes,
        }
        encoded = base64.b64encode(json.dumps(request, ensure_ascii=True).encode()).decode()
        worker = Path(__file__).with_name("text_replace_worker.py")
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return _incomplete("timeout")
        result = await fs._shell.run(
            [sys.executable, str(worker)],
            cwd=fs.workspace,
            input=encoded,
            timeout=remaining,
            check=False,
            max_bytes=64 * 1024 * 1024,
        )
        if result.get("timed_out") or result.get("truncated") or result.get("returncode") != 0:
            return _incomplete("timeout" if result.get("timed_out") else "worker_failed")
        try:
            worker_result = json.loads(result.get("stdout", ""))
        except (TypeError, json.JSONDecodeError):
            return _incomplete("worker_invalid_output")
        if not worker_result.get("complete", False):
            return _incomplete(worker_result.get("reason", "worker_incomplete"))
        operations: list[dict[str, Any]] = []
        total = 0
        worker_operations = worker_result.get("operations", [])
        if not isinstance(worker_operations, list):
            return _incomplete("worker_invalid_output")
        for item in worker_operations:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                return _incomplete("worker_invalid_output")
            supplied = item["path"]
            resolved, display = fs._path(supplied)
            try:
                _reject_symlink_alias(fs, supplied, resolved)
            except ValueError:
                return _incomplete("invalid_path")
            if fs._history_resource(resolved) is None or resolved.is_symlink():
                return _incomplete("invalid_path")
            try:
                old = base64.b64decode(item["old"], validate=True)
                new = base64.b64decode(item["new"], validate=True)
            except (KeyError, ValueError, TypeError):
                return _incomplete("worker_invalid_output")
            total += len(old) + len(new)
            if total > max_bytes or len(operations) >= max_files:
                return _incomplete("byte_limit" if total > max_bytes else "file_limit")
            operations.append(
                {
                    "path": resolved,
                    "input": supplied,
                    "display": display,
                    "old": old,
                    "new": new,
                    "matches": item.get("matches", 0),
                }
            )
        if not operations:
            return {
                "plan_id": None,
                "applicable": False,
                "complete": True,
                "reason": "no_changes",
                "changes": [],
                "diff": "",
                "diff_truncated": False,
                "changed_files": 0,
            }
        store = ChangePlanStore(
            fs.workspace,
            "replace",
            max_files=max_files,
            max_input_output_bytes=max_bytes,
        )
        plan_id = store.create(
            {
                "pattern": pattern,
                "replacement": replacement,
                "fixed": fixed,
                "ignore_case": ignore_case,
                "history": history,
                "operations": operations,
            }
        )
        changes, diff, diff_truncated = _preview(operations)
        return {
            "plan_id": plan_id,
            "applicable": True,
            "complete": True,
            "reason": None,
            "changes": changes,
            "diff": diff,
            "diff_truncated": diff_truncated,
            "changed_files": len(operations),
            "original_bytes": sum(len(item["old"]) for item in operations),
            "planned_bytes": sum(len(item["new"]) for item in operations),
        }


async def apply_replace(fs: Any, plan_id: str) -> dict[str, Any]:
    store = ChangePlanStore(fs.workspace, "replace")
    try:
        payload = store.load(plan_id)
    except ChangePlanError:
        raise
    entries = payload.get("operations")
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_FILES:
        raise ChangePlanError("invalid replacement plan")
    resolved: list[tuple[dict[str, Any], Path, str, bytes, _State]] = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("input"), str):
            raise ChangePlanError("invalid replacement operation")
        supplied = entry["input"]
        path, actual_display = fs._path(supplied)
        try:
            _reject_symlink_alias(fs, supplied, path)
        except ValueError as exc:
            raise ChangePlanError("replacement path is a symlink alias") from exc
        display = entry.get("display")
        if (
            not isinstance(display, str)
            or fs._history_resource(path) is None
            or path.is_symlink()
        ):
            raise ChangePlanError("replacement path is outside the workspace")
        old = entry.get("old")
        new = entry.get("new")
        if not isinstance(old, bytes) or not isinstance(new, bytes):
            raise ChangePlanError("replacement operation contains invalid bytes")
        if path in {item[1] for item in resolved}:
            raise ChangePlanError("replacement plan contains duplicate paths")
        if actual_display != display:
            raise ChangePlanError("replacement path changed since preview")
        state = await asyncio.to_thread(_read_state, path, display)
        if not state.exists or state.data != old:
            raise ChangePlanError(f"replacement source changed: {display}")
        resolved.append((entry, path, display, new, state))
    locks = [fs._lock(item[1]) for item in sorted(resolved, key=lambda item: str(item[1]))]
    history_store = fs._history_store() if payload.get("history", True) else None
    resources = sorted(
        resource
        for _, path, _, _, _ in resolved
        if (resource := fs._history_resource(path)) is not None
    )
    async with AsyncExitStack() as stack:
        if history_store is not None:
            for resource in resources:
                await stack.enter_async_context(history_store.transaction(resource))
        for lock in locks:
            await lock.acquire()
            stack.callback(lock.release)
        plans = [
            _plan(
                "update",
                path,
                display,
                state.data,
                new,
                state.info,
                stat.S_IMODE(state.info.st_mode),
            )
            for _, path, display, new, state in resolved
        ]
        for _, (_, path, display, _, state) in zip(plans, resolved, strict=True):
            current = await asyncio.to_thread(_read_state, path, display)
            if current.data != state.data:
                raise ChangePlanError(f"replacement source changed: {display}")
        if history_store is not None:
            history_store.prepare_changes_sync(plans)
        result = await asyncio.to_thread(
            _commit, plans, {path: state for _, path, _, _, state in resolved}, MAX_DIFF_BYTES,
            history_store=history_store,
        )
        result.update(
            {
                "plan_id": plan_id,
                "applied": True,
                "history_recorded": history_store is not None,
            }
        )
    try:
        store.remove(plan_id)
    except OSError as exc:
        result["warnings"] = [f"replacement plan cleanup failed: {exc}"]
    return result


def _preview(operations: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str, bool]:
    changes = []
    chunks: list[str] = []
    total = 0
    truncated = False
    for item in operations:
        old, new = item["old"], item["new"]
        display = item["display"]
        changes.append(
            {
                "path": display,
                "matches": item["matches"],
                "old_revision": _sha(old),
                "revision": _sha(new),
                "size": len(new),
            }
        )
        for line in difflib.unified_diff(
            old.decode("utf-8").splitlines(keepends=True),
            new.decode("utf-8").splitlines(keepends=True),
            fromfile=display,
            tofile=display,
        ):
            encoded = line.encode("utf-8")
            if total + len(encoded) > MAX_DIFF_BYTES:
                truncated = True
                break
            chunks.append(line)
            total += len(encoded)
        if truncated:
            break
    return changes, "".join(chunks), truncated


def _incomplete(reason: str) -> dict[str, Any]:
    return {
        "plan_id": None,
        "applicable": False,
        "complete": False,
        "reason": reason,
        "changes": [],
        "diff": "",
        "diff_truncated": False,
        "changed_files": 0,
    }


def _sha(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


__all__ = ["apply_replace", "replace"]
