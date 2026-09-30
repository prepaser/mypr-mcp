"""Preview and transactionally apply bounded ast-grep rewrites."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import difflib
import io
import itertools
import json
import os
import re
import secrets
import shutil
import stat
import tempfile
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .filesystem import _diff_lines, _require_regular, _sha256, _to_thread_uncancelled
from .patching import _commit, _plan, _reject_symlink_alias, _signature, _State
from .search_backends import _inline_rule, _verify_sg

_MAX_FILES = 100
_MAX_BYTES = 16 * 1024 * 1024
_TIMEOUT = 30.0
_MAX_DIFF = 32_768
_STORE_BYTES = 64 * 1024 * 1024
_STORE_PLANS = 16
_PLAN_TTL = 60 * 60


async def rewrite_ast(
    fs: Any,
    *,
    pattern: str | None,
    rule: Mapping[str, Any] | None,
    replacement: str,
    lang: str,
    paths: str | list[str] | None,
    glob: str | list[str] | None,
    history: bool = True,
) -> dict[str, Any]:
    if type(history) is not bool:
        raise TypeError("history must be a boolean")
    _validate(pattern, rule, replacement, lang, paths, glob)
    if fs._shell is None:
        raise RuntimeError("AST rewrites require the workspace shell")
    executable = shutil.which("ast-grep") or shutil.which("sg")
    if executable is None:
        raise RuntimeError("ast-grep is required for ws.fs.rewrite_ast; install ast-grep")
    if Path(executable).name == "sg":
        _verify_sg(executable)

    root = fs.workspace / ".mypr" / "rewrites"
    config = root / "sgconfig.yml"
    store = _PlanStore(root)
    await _to_thread_uncancelled(_write_once, config, "ruleDirs: []\n")
    command = _discovery_command(executable, config, pattern, rule, replacement, lang, paths, glob)
    deadline = time.monotonic() + _TIMEOUT
    output_bytes = 0
    try:
        async with asyncio.timeout(_TIMEOUT):
            found = await fs._shell.run(
                command,
                cwd=fs.workspace,
                timeout=_TIMEOUT,
                check=False,
                max_bytes=_MAX_BYTES,
            )
            output_bytes += _output_size(found)
            _raise_command_error(found)
            if _incomplete(found):
                return _incomplete_result(_reason(found, "discovery_incomplete"))
            candidate_paths: dict[Path, str] = {}
            for record in _records(found.get("stdout", "")):
                raw = record.get("file")
                if not isinstance(raw, str) or not raw or raw == "STDIN":
                    raise RuntimeError("ast-grep returned an invalid file path")
                _reject_ast_symlink_alias(fs, raw)
                path, display = fs._path(raw)
                _reject_symlink_alias(fs, raw, path)
                if path.is_relative_to(root.resolve()):
                    continue
                candidate_paths.setdefault(path, display)
                if len(candidate_paths) > _MAX_FILES:
                    return _incomplete_result("file_limit")

            if not candidate_paths:
                return {
                    "plan_id": None,
                    "applicable": False,
                    "complete": True,
                    "reason": "no_matches",
                    "scanned_files": 0,
                    "changed_files": 0,
                    "original_bytes": 0,
                    "planned_bytes": 0,
                    "changes": [],
                    "diff": "",
                    "diff_truncated": False,
                }

            inputs: list[tuple[Path, str, bytes, os.stat_result, bytes]] = []
            original_bytes = 0
            planned_bytes = 0
            for path, display in candidate_paths.items():
                remaining_input = _MAX_BYTES - original_bytes
                try:
                    state = await _to_thread_uncancelled(
                        _read_bounded_state, path, display, remaining_input
                    )
                except _InputLimit:
                    return _incomplete_result("byte_limit")
                if not state.exists:
                    return _incomplete_result("file_changed_during_scan")
                _require_regular(state.info, display)
                original_bytes += len(state.data)
                if original_bytes > _MAX_BYTES:
                    return _incomplete_result("byte_limit")
                try:
                    source = state.data.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ValueError(f"AST rewrites require UTF-8 source files: {display}") from exc
                per_file = _stdin_command(
                    executable, config, pattern, rule, replacement, lang
                )
                remaining = max(0.001, deadline - time.monotonic())
                result = await fs._shell.run(
                    per_file,
                    cwd=fs.workspace,
                    input=source,
                    timeout=remaining,
                    check=False,
                    max_bytes=max(1, _MAX_BYTES - output_bytes),
                )
                output_bytes += _output_size(result)
                _raise_command_error(result)
                if _incomplete(result):
                    return _incomplete_result(_reason(result, "rewrite_incomplete"))
                matches = _records(result.get("stdout", ""))
                first_match = next(matches, None)
                if first_match is None:
                    return _incomplete_result("file_changed_during_scan")
                changed = _apply_matches(
                    state.data, itertools.chain((first_match,), matches), display
                )
                if changed != state.data:
                    planned_bytes += len(changed)
                    if planned_bytes > _MAX_BYTES:
                        return _incomplete_result("byte_limit")
                    inputs.append((path, display, state.data, state.info, changed))
            preview = _preview(inputs, original_bytes, planned_bytes)
            if not inputs:
                return {
                    **preview,
                    "plan_id": None,
                    "applicable": False,
                    "complete": True,
                    "reason": "no_changes",
                    "scanned_files": len(candidate_paths),
                }
            payload = {
                "created": time.time(),
                "workspace": _workspace_identity(fs.workspace),
                "files": [
                    {
                        "path": str(path),
                        "display": display,
                        "old_hash": _sha256(old),
                        "old_size": len(old),
                        "new": base64.b64encode(new).decode("ascii"),
                    }
                    for path, display, old, info, new in inputs
                ],
                "preview": preview,
                "original_bytes": original_bytes,
                "planned_bytes": planned_bytes,
                "history": history,
            }
            plan_id = await _to_thread_uncancelled(store.create, payload)
            return {
                **preview,
                "plan_id": plan_id,
                "applicable": True,
                "complete": True,
                "scanned_files": len(candidate_paths),
                "original_bytes": original_bytes,
                "planned_bytes": planned_bytes,
            }
    except TimeoutError:
        return _incomplete_result("timeout")


async def apply_rewrite(fs: Any, plan_id: str) -> dict[str, Any]:
    if not isinstance(plan_id, str) or len(plan_id) != 32 or any(
        char not in "0123456789abcdef" for char in plan_id
    ):
        raise ValueError("invalid rewrite plan ID")
    store = _PlanStore(fs.workspace / ".mypr" / "rewrites")
    payload = await _to_thread_uncancelled(store.load, plan_id)
    if payload.get("workspace") != _workspace_identity(fs.workspace):
        raise ValueError("rewrite plan belongs to a different or moved workspace")
    entries = payload.get("files")
    if not isinstance(entries, list) or not 1 <= len(entries) <= _MAX_FILES:
        raise RuntimeError("invalid rewrite plan")
    resolved: list[tuple[Path, str, str, bytes, int]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError("invalid rewrite plan")
        raw_path, display = entry.get("path"), entry.get("display")
        old_hash, old_size = entry.get("old_hash"), entry.get("old_size")
        encoded = entry.get("new")
        if (
            not isinstance(raw_path, str)
            or not isinstance(display, str)
            or not isinstance(old_hash, str)
            or len(old_hash) != 64
            or type(old_size) is not int
            or not 0 <= old_size <= _MAX_BYTES
            or not isinstance(encoded, str)
            or len(encoded) > 4 * ((_MAX_BYTES + 2) // 3)
        ):
            raise RuntimeError("invalid rewrite plan")
        _reject_ast_symlink_alias(fs, raw_path)
        path, actual_display = fs._path(raw_path)
        _reject_symlink_alias(fs, raw_path, path)
        if path in {item[0] for item in resolved}:
            raise RuntimeError("invalid rewrite plan with duplicate paths")
        try:
            new = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise RuntimeError("invalid rewrite plan data") from exc
        if sum(len(item[3]) for item in resolved) + len(new) > _MAX_BYTES:
            raise RuntimeError("rewrite plan exceeds the input limit")
        resolved.append((path, actual_display or display, old_hash, new, old_size))

    history_store = fs._history_store() if payload.get("history", True) else None
    resources = sorted({
        resource
        for path, _, _, _, _ in resolved
        if (resource := history_store._resource_for_path(path)) is not None
    }) if history_store is not None else []
    locks = [fs._lock(item[0]) for item in sorted(resolved, key=lambda item: str(item[0]))]
    async with contextlib.AsyncExitStack() as stack:
        if history_store is not None:
            for resource in resources:
                await stack.enter_async_context(history_store.transaction(resource))
        for lock in locks:
            await lock.acquire()
            stack.callback(lock.release)
        states = {}
        plans = []
        for path, display, expected_hash, new, old_size in resolved:
            try:
                state = await _to_thread_uncancelled(
                    _read_bounded_state, path, display, old_size
                )
            except _InputLimit as exc:
                raise RuntimeError(f"rewrite source changed since preview: {display}") from exc
            if (
                not state.exists
                or len(state.data) != old_size
                or _sha256(state.data) != expected_hash
            ):
                raise RuntimeError(f"rewrite source changed since preview: {display}")
            _require_regular(state.info, display)
            states[path] = state
            plans.append(
                _plan(
                    "update",
                    path,
                    display,
                    state.data,
                    new,
                    state.info,
                    stat.S_IMODE(state.info.st_mode),
                )
            )
        if history_store is not None:
            await _to_thread_uncancelled(history_store.prepare_changes_sync, plans)
        result = await _to_thread_uncancelled(
            _commit, plans, states, _MAX_DIFF, history_store=history_store
        )
        result.update(
            {
                "plan_id": plan_id,
                "applied": True,
                "original_bytes": payload.get("original_bytes"),
                "planned_bytes": payload.get("planned_bytes"),
                "history_recorded": history_store is not None,
            }
        )
        try:
            await _to_thread_uncancelled(store.remove, plan_id)
        except OSError as exc:
            result["warnings"] = [f"rewrite plan cleanup failed: {exc}"]
        return result


def _validate(pattern, rule, replacement, lang, paths, glob) -> None:
    if (pattern is None) == (rule is None):
        raise ValueError("provide exactly one of pattern or rule")
    if pattern is not None and (not isinstance(pattern, str) or not pattern):
        raise ValueError("pattern must be a non-empty string")
    if isinstance(pattern, str) and "\0" in pattern:
        raise ValueError("pattern must not contain NUL")
    if rule is not None and not isinstance(rule, Mapping):
        raise TypeError("rule must be a matcher mapping")
    if rule is not None:
        _reject_nul(rule)
    if not isinstance(replacement, str) or "\0" in replacement:
        raise ValueError("replacement must be a string without NUL")
    if not isinstance(lang, str) or not lang.strip() or "\0" in lang:
        raise ValueError("lang must be a non-empty string")
    for name, value in (("paths", paths), ("glob", glob)):
        values = [value] if isinstance(value, str) else value
        if values is not None and (
            not isinstance(values, list)
            or not values
            or not all(isinstance(item, str) and item and "\0" not in item for item in values)
        ):
            raise ValueError(f"{name} must be a string or list of non-empty strings")


def _discovery_command(executable, config, pattern, rule, replacement, lang, paths, globs):
    if rule is None:
        command = [
            executable,
            "run",
            "--config",
            str(config),
            "--pattern",
            pattern,
            "--lang",
            lang,
            "--rewrite",
            replacement,
        ]
    else:
        inline = _fix_rule(rule, lang, replacement)
        command = [executable, "scan", "--config", str(config), "--inline-rules", inline]
    command.extend(["--json=stream", "--threads", "2", "--color", "never"])
    command.extend(_glob_args(globs))
    raw_paths = [paths] if isinstance(paths, str) else paths
    command.extend(["--", *(raw_paths if raw_paths is not None else ["."])])
    return command


def _stdin_command(executable, config, pattern, rule, replacement, lang):
    if rule is None:
        return [
            executable,
            "run",
            "--config",
            str(config),
            "--pattern",
            pattern,
            "--lang",
            lang,
            "--rewrite",
            replacement,
            "--stdin",
            "--json=stream",
            "--threads",
            "2",
            "--color",
            "never",
        ]
    return [
        executable,
        "scan",
        "--config",
        str(config),
        "--inline-rules",
        _fix_rule(rule, lang, replacement),
        "--stdin",
        "--json=stream",
        "--threads",
        "2",
        "--color",
        "never",
    ]


def _fix_rule(rule, lang, replacement):
    inline = json.loads(_inline_rule(rule, lang, {}))
    inline["fix"] = replacement
    return json.dumps(inline, ensure_ascii=False, separators=(",", ":"))


def _glob_args(glob):
    values = [glob] if isinstance(glob, str) else glob or []
    result = []
    for value in values:
        result.extend(["--globs", value])
    return result


def _records(output):
    if not isinstance(output, str):
        raise RuntimeError("ast-grep returned non-text output")
    for line in io.StringIO(output):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError("ast-grep returned invalid JSON") from exc
        if not isinstance(record, dict):
            raise RuntimeError("ast-grep returned an invalid match record")
        yield record


def _reject_nul(value):
    if isinstance(value, str) and "\0" in value:
        raise ValueError("AST rule strings must not contain NUL")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_nul(key)
            _reject_nul(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_nul(child)


def _reject_ast_symlink_alias(fs: Any, supplied: str) -> None:
    candidate = Path(supplied).expanduser()
    current = Path(candidate.anchor) if candidate.is_absolute() else fs.workspace
    parts = candidate.parts[1:] if candidate.is_absolute() else candidate.parts
    for part in parts:
        if part in ("", "."):
            continue
        if part == "..":
            current = current.parent
            continue
        current /= part
        if current.is_symlink():
            raise ValueError(f"symlink paths are not supported in AST rewrites: {supplied}")


class _InputLimit(Exception):
    pass


def _read_bounded_state(path: Path, display: str, limit: int) -> _State:
    try:
        before = path.stat()
    except FileNotFoundError:
        return _State(path, display, False, None, None)
    _require_regular(before, display)
    if before.st_size > limit:
        raise _InputLimit
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    except FileNotFoundError:
        return _State(path, display, False, None, None)
    try:
        opened = os.fstat(fd)
        _require_regular(opened, display)
        if opened.st_size > limit or _signature(before) != _signature(opened):
            raise _InputLimit
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise _InputLimit
        try:
            current = path.stat()
        except FileNotFoundError as exc:
            raise RuntimeError(f"File changed while reading: {display}") from exc
        if _signature(opened) != _signature(after) or _signature(after) != _signature(current):
            raise RuntimeError(f"File changed while reading: {display}")
        return _State(path, display, True, data, after)
    finally:
        if fd >= 0:
            os.close(fd)


def _apply_matches(data: bytes, records: Iterable[Mapping[str, Any]], display: str) -> bytes:
    output = bytearray()
    cursor = 0
    previous_start = -1
    found = False
    for record in records:
        offsets = record.get("replacementOffsets")
        replacement = record.get("replacement")
        if (
            not isinstance(offsets, dict)
            or type(offsets.get("start")) is not int
            or type(offsets.get("end")) is not int
            or not isinstance(replacement, str)
        ):
            raise RuntimeError(f"ast-grep did not return rewrite offsets for {display}")
        start, end = offsets["start"], offsets["end"]
        if not 0 <= start <= end <= len(data):
            raise RuntimeError(f"ast-grep returned out-of-range rewrite offsets for {display}")
        if start < cursor or start == previous_start:
            raise ValueError(f"overlapping AST matches in {display}")
        try:
            data[:start].decode("utf-8")
            data[start:end].decode("utf-8")
            data[end:].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"ast-grep returned invalid UTF-8 offsets for {display}") from exc
        replacement = _normalize_newlines(replacement, data, start)
        output.extend(data[cursor:start])
        output.extend(replacement.encode("utf-8"))
        cursor = end
        previous_start = start
        found = True
    if not found:
        return data
    output.extend(data[cursor:])
    return bytes(output)


def _normalize_newlines(replacement: str, data: bytes, offset: int) -> str:
    if "\n" not in replacement and "\r" not in replacement:
        return replacement
    ending = _nearby_newline(data, offset)
    return re.sub(r"\r\n|\r|\n", lambda _: ending, replacement)


def _nearby_newline(data: bytes, offset: int) -> str:
    before = max(data.rfind(b"\n", 0, offset), data.rfind(b"\r", 0, offset))
    after_candidates = [
        value
        for value in (data.find(b"\n", offset), data.find(b"\r", offset))
        if value >= 0
    ]
    if before >= 0:
        if data[before : before + 2] == b"\r\n" or (
            data[before : before + 1] == b"\n" and before > 0 and data[before - 1 : before] == b"\r"
        ):
            return "\r\n"
        return data[before : before + 1].decode("ascii")
    if after_candidates:
        position = min(after_candidates)
        if data[position : position + 2] == b"\r\n":
            return "\r\n"
        return data[position : position + 1].decode("ascii")
    return "\n"


def _preview(inputs, original_bytes, planned_bytes):
    changes = []
    for _path, display, old, _info, new in inputs:
        changes.append(
            {
                "path": display,
                "old_revision": _sha256(old),
                "revision": _sha256(new),
                "size": len(new),
            }
        )
    pieces = []
    used = 0
    truncated = False
    for _path, display, old, _info, new in inputs:
        for line in difflib.unified_diff(
            _diff_lines(old.decode("utf-8")),
            _diff_lines(new.decode("utf-8")),
            fromfile=display,
            tofile=display,
        ):
            if not line.endswith("\n"):
                line += "\n\\ No newline at end of file\n"
            encoded = line.encode("utf-8")
            if used + len(encoded) > _MAX_DIFF:
                truncated = True
                break
            pieces.append(line)
            used += len(encoded)
        if truncated:
            break
    return {
        "changes": changes,
        "diff": "".join(pieces),
        "diff_truncated": truncated,
        "changed_files": len(inputs),
        "original_bytes": original_bytes,
        "planned_bytes": planned_bytes,
    }


def _workspace_identity(workspace: Path) -> dict[str, Any]:
    root = Path(workspace).resolve()
    info = root.stat()
    return {"path": str(root), "device": info.st_dev, "inode": info.st_ino}


def _incomplete_result(reason):
    return {
        "plan_id": None,
        "applicable": False,
        "complete": False,
        "reason": reason,
        "scanned_files": None,
        "changed_files": 0,
        "original_bytes": 0,
        "planned_bytes": 0,
        "changes": [],
        "diff": "",
        "diff_truncated": False,
    }


def _incomplete(result):
    return bool(result.get("timed_out") or result.get("truncated"))


def _raise_command_error(result):
    if result.get("error") or result.get("returncode") not in (0, 1):
        detail = str(result.get("stderr") or result.get("error") or result.get("returncode"))
        raise RuntimeError(f"ast-grep failed: {detail[:2048]}")


def _reason(result, fallback):
    if result.get("timed_out"):
        return "timeout"
    if result.get("truncated"):
        return "output_limit"
    return fallback


def _output_size(result):
    return len(str(result.get("stdout", "")).encode("utf-8")) + len(
        str(result.get("stderr", "")).encode("utf-8")
    )


def _write_once(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)


class _PlanStore:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / ".lock"

    def create(self, payload: dict[str, Any]) -> str:
        ident = secrets.token_hex(16)
        data = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("ascii")
        if len(data) > _STORE_BYTES:
            raise RuntimeError("rewrite plan exceeds the bounded plan store")
        with self._lock():
            files = sorted(
                self.root.glob("[0-9a-f]" * 32 + ".json"),
                key=lambda path: path.stat().st_mtime,
            )
            now = time.time()
            expired = [path for path in files if now - path.stat().st_mtime > _PLAN_TTL]
            valid = [path for path in files if path not in expired]
            sizes = {path: path.stat().st_size for path in valid}
            total = sum(sizes.values())
            victims = []
            while valid and (len(valid) + 1 > _STORE_PLANS or total + len(data) > _STORE_BYTES):
                oldest = valid.pop(0)
                victims.append(oldest)
                total -= sizes[oldest]
            if total + len(data) > _STORE_BYTES:
                raise RuntimeError("rewrite plan exceeds the bounded plan store")
            target = self.root / f"{ident}.json"
            fd, temporary = tempfile.mkstemp(prefix=f".{ident}.", suffix=".tmp", dir=self.root)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
                _fsync_directory(self.root)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(temporary)
            for old in [*expired, *victims]:
                old.unlink(missing_ok=True)
        return ident

    def load(self, ident: str) -> dict[str, Any]:
        target = self.root / f"{ident}.json"
        try:
            if time.time() - target.stat().st_mtime > _PLAN_TTL:
                target.unlink(missing_ok=True)
                raise ValueError("rewrite plan has expired")
            if target.stat().st_size > _STORE_BYTES:
                raise RuntimeError("invalid rewrite plan size")
            payload = json.loads(target.read_text(encoding="ascii"))
        except FileNotFoundError as exc:
            raise ValueError("rewrite plan has expired or does not exist") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("files"), list):
            raise RuntimeError("invalid rewrite plan")
        return payload

    def remove(self, ident: str) -> None:
        with self._lock():
            (self.root / f"{ident}.json").unlink(missing_ok=True)

    @contextlib.contextmanager
    def _lock(self):
        import fcntl

        with self.lock_path.open("a+b") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
