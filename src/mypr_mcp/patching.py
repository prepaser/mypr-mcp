"""Transactional multi-file patches for the workspace filesystem API.

The parser intentionally accepts the small, strict patch language emitted by
agent ``apply_patch`` implementations.  Context is matched byte-for-byte
after UTF-8 decoding; no whitespace or fuzzy matching is performed.
"""

from __future__ import annotations

import difflib
import hashlib
import os
import re
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .filesystem import (
    _check_expected,
    _diff_lines,
    _read_regular,
    _require_regular,
    _signature,
    _to_thread_uncancelled,
)

_FILE_HEADER = re.compile(r"^\*\*\* (Add|Update|Delete) File: (.+)$")
_MOVE_HEADER = re.compile(r"^\*\*\* Move to: (.+)$")
_HUNK_HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$")


@dataclass(frozen=True)
class _Hunk:
    old_start: int | None
    old_count: int | None
    new_start: int | None
    new_count: int | None
    lines: tuple[tuple[str, str], ...]
    eof: bool = False
    anchor: str | None = None


@dataclass(frozen=True)
class _Change:
    operation: str
    source: str
    destination: str | None = None
    lines: tuple[str, ...] = ()
    hunks: tuple[_Hunk, ...] = ()
    eof: bool = False


@dataclass
class _State:
    path: Path
    display: str
    exists: bool
    data: bytes | None
    info: os.stat_result | None

    @property
    def revision(self) -> str | None:
        return None if self.data is None else _sha256(self.data)


def parse_patch(patch: str) -> tuple[_Change, ...]:
    """Parse a strict ``*** Begin Patch`` / ``*** End Patch`` document."""
    if not isinstance(patch, str):
        raise TypeError("patch must be a string")
    lines = patch.replace("\r\n", "\n").split("\n")
    if lines and not lines[-1]:
        lines.pop()
    if not lines or lines[0] != "*** Begin Patch":
        raise ValueError("patch must start with *** Begin Patch")
    if lines[-1] != "*** End Patch":
        raise ValueError("patch must end with *** End Patch")
    changes: list[_Change] = []
    index = 1
    while index < len(lines) - 1:
        if not lines[index]:
            index += 1
            continue
        match = _FILE_HEADER.fullmatch(lines[index])
        if not match:
            raise ValueError(f"unexpected patch line: {lines[index]!r}")
        operation, source = match.groups()
        if not source or Path(source).name == "":
            raise ValueError("patch file path must not be empty")
        index += 1
        destination: str | None = None
        if index < len(lines) - 1:
            move = _MOVE_HEADER.fullmatch(lines[index])
            if move:
                if operation != "Update":
                    raise ValueError("Move to is only valid after Update File")
                destination = move.group(1)
                if not destination:
                    raise ValueError("patch destination must not be empty")
                index += 1
        if operation == "Add":
            content, eof, index = _parse_add(lines, index)
            changes.append(_Change("add", source, lines=tuple(content), eof=eof))
            continue
        if operation == "Delete":
            if index < len(lines) - 1 and not lines[index].startswith("*** "):
                raise ValueError("Delete File cannot contain content")
            changes.append(_Change("delete", source))
            continue
        hunks, eof, index = _parse_update(lines, index)
        if not hunks and destination is None:
            raise ValueError("Update File requires at least one @@ hunk")
        changes.append(
            _Change("move" if destination else "update", source, destination, hunks=hunks, eof=eof)
        )
    if not changes:
        raise ValueError("patch contains no file changes")
    return tuple(changes)


def _parse_add(lines: list[str], index: int) -> tuple[list[str], bool, int]:
    content: list[str] = []
    eof = False
    while index < len(lines) - 1 and (
        not lines[index].startswith("*** ") or lines[index] == "*** End of File"
    ):
        line = lines[index]
        if line == "*** End of File":
            if eof:
                raise ValueError("duplicate End of File marker")
            eof = True
        elif line.startswith("+"):
            if eof:
                raise ValueError("content follows End of File")
            content.append(line[1:])
        else:
            raise ValueError(f"invalid Add File line: {line!r}")
        index += 1
    return content, eof, index


def _parse_update(lines: list[str], index: int) -> tuple[list[_Hunk], bool, int]:
    hunks: list[_Hunk] = []
    eof = False
    while index < len(lines) - 1 and not lines[index].startswith("*** "):
        header = lines[index]
        match = _HUNK_HEADER.fullmatch(header)
        anchor = None
        if match:
            old_start, old_count, new_start, new_count = match.groups()
        elif header == "@@" or header.startswith("@@ "):
            old_start = old_count = new_start = new_count = None
            anchor = header[3:] if header != "@@" else None
        else:
            raise ValueError(f"expected @@ hunk, got {header!r}")
        index += 1
        hunk_lines: list[tuple[str, str]] = []
        hunk_eof = False
        while index < len(lines) - 1 and (
            not lines[index].startswith("@@")
            and (not lines[index].startswith("*** ") or lines[index] == "*** End of File")
        ):
            line = lines[index]
            if line == "*** End of File":
                if hunk_eof:
                    raise ValueError("duplicate End of File marker")
                hunk_eof = True
            elif line.startswith((" ", "+", "-")):
                if hunk_eof:
                    raise ValueError("content follows End of File")
                hunk_lines.append((line[0], line[1:]))
            else:
                raise ValueError(f"invalid hunk line: {line!r}")
            index += 1
        old_lines = sum(mark in (" ", "-") for mark, _ in hunk_lines)
        new_lines = sum(mark in (" ", "+") for mark, _ in hunk_lines)
        parsed_old = int(old_count or "1") if old_start is not None else None
        parsed_new = int(new_count or "1") if new_start is not None else None
        if parsed_old is not None and parsed_old != old_lines:
            raise ValueError(f"hunk old line count is {parsed_old}, found {old_lines}")
        if parsed_new is not None and parsed_new != new_lines:
            raise ValueError(f"hunk new line count is {parsed_new}, found {new_lines}")
        if hunk_eof:
            if eof or index < len(lines) - 1 and lines[index].startswith("@@"):
                raise ValueError("End of File must be the final hunk marker")
            eof = True
        hunks.append(
            _Hunk(
                None if old_start is None else int(old_start),
                parsed_old,
                None if new_start is None else int(new_start),
                parsed_new,
                tuple(hunk_lines),
                hunk_eof,
                anchor,
            )
        )
    return hunks, eof, index


async def apply_patch(
    fs: Any,
    patch: str,
    *,
    expected_hashes: Mapping[str, str | None] | None = None,
    dry_run: bool = False,
    max_diff_bytes: int = 32_768,
) -> dict[str, Any]:
    """Validate and transactionally apply a multi-file agent patch.

    ``expected_hashes`` maps paths to SHA-256 revisions.  A value of ``None``
    asserts that the path is absent; omitted paths have no CAS assertion.
    """
    if type(dry_run) is not bool:
        raise TypeError("dry_run must be a boolean")
    if (
        not isinstance(max_diff_bytes, int)
        or isinstance(max_diff_bytes, bool)
        or max_diff_bytes < 1
    ):
        raise ValueError("max_diff_bytes must be a positive integer")
    changes = parse_patch(patch)
    resolved = _resolve_changes(fs, changes)
    _check_conflicts(resolved)
    expected = _resolve_expected_hashes(fs, expected_hashes)
    locks = [fs._lock(path) for path in sorted(_lock_paths(resolved, expected), key=str)]
    acquired: list[Any] = []
    try:
        for lock in locks:
            await lock.acquire()
            acquired.append(lock)
        return await _to_thread_uncancelled(
            _apply_locked,
            fs,
            resolved,
            expected,
            dry_run,
            max_diff_bytes,
        )
    finally:
        for lock in reversed(acquired):
            lock.release()


def _resolve_changes(
    fs: Any, changes: tuple[_Change, ...]
) -> list[tuple[_Change, Path, str, Path | None, str | None]]:
    resolved: list[tuple[_Change, Path, str, Path | None, str | None]] = []
    for change in changes:
        source, source_display = fs._path(change.source)
        destination = destination_display = None
        if change.destination is not None:
            destination, destination_display = fs._path(change.destination)
        _reject_symlink_alias(fs, change.source, source)
        if change.destination is not None:
            _reject_symlink_alias(fs, change.destination, destination)
        resolved.append((change, source, source_display, destination, destination_display))
    return resolved


def _reject_symlink_alias(fs: Any, supplied: str, resolved: Path) -> None:
    candidate = Path(supplied).expanduser()
    if not candidate.is_absolute():
        candidate = fs.workspace / candidate
    candidate = Path(os.path.normpath(str(candidate.absolute())))
    current = candidate
    while current != current.parent:
        if current.is_symlink():
            raise ValueError(f"symlink paths are not supported in patches: {supplied}")
        current = current.parent


def _lock_paths(
    resolved: list[tuple[_Change, Path, str, Path | None, str | None]],
    expected: Mapping[Path, str | None] | None = None,
) -> set[Path]:
    paths = {
        path
        for _, source, _, destination, _ in resolved
        for path in (source, destination)
        if path is not None
    }
    paths.update(expected or ())
    return paths


def _check_conflicts(resolved: list[tuple[_Change, Path, str, Path | None, str | None]]) -> None:
    roles: dict[Path, str] = {}
    for _change, source, _, destination, _ in resolved:
        for path, role in ((source, "source"), (destination, "destination")):
            if path is None:
                continue
            previous = roles.get(path)
            if previous is not None:
                raise ValueError(f"conflicting patch operations for {path}")
            roles[path] = role
        if destination is not None and destination == source:
            raise ValueError(f"move source and destination are identical: {source}")
    existing: dict[tuple[int, int], Path] = {}
    for path in roles:
        try:
            info = path.stat()
        except FileNotFoundError:
            continue
        key = (info.st_dev, info.st_ino)
        other = existing.get(key)
        if other is not None and other != path:
            raise ValueError(f"patch paths alias the same file: {other} and {path}")
        existing[key] = path
    paths = tuple(roles)
    for index, path in enumerate(paths):
        for other in paths[index + 1 :]:
            if path in other.parents or other in path.parents:
                raise ValueError(f"conflicting parent and child patch paths: {path} and {other}")


def _apply_locked(
    fs: Any,
    resolved: list[tuple[_Change, Path, str, Path | None, str | None]],
    expected_hashes: Mapping[Path, str | None],
    dry_run: bool,
    max_diff_bytes: int,
) -> dict[str, Any]:
    states: dict[Path, _State] = {}
    for _, source, source_display, destination, destination_display in resolved:
        for path, display in ((source, source_display), (destination, destination_display)):
            if path is not None and path not in states:
                states[path] = _read_state(path, display)
    for path, wanted in expected_hashes.items():
        if path not in states:
            states[path] = _read_state(path, str(path))
        if wanted is None:
            if states[path].exists:
                raise ValueError(f"Revision mismatch: expected absent, got {states[path].revision}")
        else:
            _check_expected(states[path].data, wanted)
    plans: list[dict[str, Any]] = []
    for change, source, source_display, destination, destination_display in resolved:
        source_state = states[source]
        if change.operation == "add":
            if source_state.exists:
                raise FileExistsError(f"File already exists: {source_display}")
            new_data = _join_lines(change.lines, bool(change.lines))
            plans.append(_plan("add", source, source_display, None, new_data, None, 0o600))
        elif change.operation == "delete":
            if not source_state.exists:
                raise FileNotFoundError(source_display)
            _require_regular(source_state.info, source_display)
            plans.append(
                _plan(
                    "delete",
                    source,
                    source_display,
                    source_state.data,
                    None,
                    source_state.info,
                    None,
                )
            )
        else:
            if not source_state.exists:
                raise FileNotFoundError(source_display)
            _require_regular(source_state.info, source_display)
            new_data = source_state.data
            if change.hunks:
                old_text = source_state.data.decode("utf-8")
                new_data = _apply_hunks(old_text, change.hunks).encode("utf-8")
            target = destination or source
            target_display = destination_display or source_display
            target_state = states[target]
            if destination is not None and target_state.exists:
                raise FileExistsError(f"File already exists: {target_display}")
            plans.append(
                _plan(
                    "move" if destination is not None else "update",
                    target,
                    target_display,
                    None if destination is not None else source_state.data,
                    new_data,
                    target_state.info if destination is not None else source_state.info,
                    stat.S_IMODE(source_state.info.st_mode),
                    source=source if destination is not None else None,
                    source_display=source_display if destination is not None else None,
                    source_old=source_state.data if destination is not None else None,
                    source_old_info=source_state.info if destination is not None else None,
                )
            )
    if dry_run:
        return _result(plans, True, max_diff_bytes)
    return _commit(plans, states, max_diff_bytes)


def _resolve_expected_hashes(
    fs: Any, expected_hashes: Mapping[str, str | None] | None
) -> dict[Path, str | None]:
    if expected_hashes is None:
        return {}
    if not isinstance(expected_hashes, Mapping):
        raise TypeError("expected_hashes must be a mapping")
    output: dict[Path, str | None] = {}
    for supplied, value in expected_hashes.items():
        if not isinstance(supplied, (str, os.PathLike)):
            raise TypeError("expected_hashes paths must be strings")
        if value is not None and not isinstance(value, str):
            raise TypeError("expected_hashes values must be strings or None")
        path, _ = fs._path(supplied)
        _reject_symlink_alias(fs, supplied, path)
        if path in output:
            raise ValueError(f"duplicate expected hash path: {supplied}")
        output[path] = value
    return output


def _read_state(path: Path, display: str) -> _State:
    try:
        info = path.stat()
    except FileNotFoundError:
        return _State(path, display, False, None, None)
    _require_regular(info, display)
    data, opened = _read_regular(path, display)
    after = path.stat()
    if _signature(info) != _signature(opened) or _signature(info) != _signature(after):
        raise RuntimeError(f"File changed while reading: {display}")
    return _State(path, display, True, data, after)


def _apply_hunks(text: str, hunks: tuple[_Hunk, ...]) -> str:
    lines = _split_source_lines(text)
    default_ending = next((ending for _, ending in lines if ending), "\n")
    cursor = 0
    delta = 0
    for hunk in hunks:
        if not hunk.lines:
            raise ValueError("hunk must contain context or changes")
        if hunk.anchor is not None:
            anchors = [i for i in range(cursor, len(lines)) if lines[i][0] == hunk.anchor]
            if len(anchors) != 1:
                raise ValueError("hunk heading is missing or ambiguous")
            cursor = anchors[0] + 1
        old_lines = [line for mark, line in hunk.lines if mark in (" ", "-")]
        candidates = [
            start
            for start in range(cursor, len(lines) - len(old_lines) + 1)
            if [line for line, _ in lines[start : start + len(old_lines)]] == old_lines
        ]
        if not old_lines:
            if hunk.old_start is not None:
                start = hunk.old_start + delta
            elif hunk.eof:
                start = len(lines)
            elif hunk.anchor is not None or not lines:
                start = cursor
            else:
                raise ValueError("insertion hunk requires context or a line range")
            if start < cursor or start > len(lines):
                raise ValueError("insertion hunk line is out of range")
            if hunk.eof and start != len(lines):
                raise ValueError("End of File hunk is not anchored at the end")
        else:
            if hunk.eof:
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate + len(old_lines) == len(lines)
                ]
            if len(candidates) != 1:
                raise ValueError(
                    "hunk context is ambiguous" if len(candidates) > 1 else "hunk context not found"
                )
            start = candidates[0]
        old_segment = lines[start : start + len(old_lines)]
        replacement: list[tuple[str, str]] = []
        at_eof_without_newline = bool(lines and lines[-1][1] == "")
        old_index = 0
        for mark, line in hunk.lines:
            if mark == " ":
                replacement.append(old_segment[old_index])
                old_index += 1
            elif mark == "-":
                old_index += 1
            else:
                nearby = (
                    old_segment[min(old_index, len(old_segment) - 1)]
                    if old_segment
                    else lines[min(start, len(lines) - 1)]
                    if lines
                    else ("", default_ending)
                )
                replacement.append((line, nearby[1] or default_ending))
        if start == len(lines) and lines and lines[-1][1] == "" and replacement:
            lines[-1] = (lines[-1][0], default_ending)
        if replacement:
            for i, (line, ending) in enumerate(replacement[:-1]):
                if not ending:
                    replacement[i] = (line, default_ending)
            if start + len(old_segment) == len(lines) and at_eof_without_newline:
                replacement[-1] = (replacement[-1][0], "")
        lines[start : start + len(old_lines)] = replacement
        cursor = start + len(replacement)
        delta += len(replacement) - len(old_segment)
    return "".join(line + ending for line, ending in lines)


def _split_source_lines(text: str) -> list[tuple[str, str]]:
    lines: list[tuple[str, str]] = []
    start = 0
    for match in re.finditer(r"\r\n|\r|\n", text):
        lines.append((text[start : match.start()], match.group()))
        start = match.end()
    if start < len(text):
        lines.append((text[start:], ""))
    return lines


def _join_lines(lines: tuple[str, ...], final_newline: bool) -> bytes:
    text = "\n".join(lines)
    if final_newline and lines:
        text += "\n"
    return text.encode("utf-8")


def _plan(
    operation: str,
    path: Path,
    display: str,
    old: bytes | None,
    new: bytes | None,
    old_info: os.stat_result | None,
    mode: int | None,
    *,
    source: Path | None = None,
    source_display: str | None = None,
    source_old: bytes | None = None,
    source_old_info: os.stat_result | None = None,
) -> dict[str, Any]:
    return {
        "operation": operation,
        "path": path,
        "display": display,
        "source": source,
        "source_display": source_display,
        "source_old": source_old,
        "source_old_info": source_old_info,
        "old": old,
        "new": new,
        "old_info": old_info,
        "mode": mode,
    }


def _result(plans: list[dict[str, Any]], dry_run: bool, limit: int) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    diff_parts: list[str] = []
    total = 0
    truncated = False
    for plan in plans:
        old_value = plan["source_old"] if plan["operation"] == "move" else plan["old"]
        old = old_value or b""
        new = plan["new"] or b""
        changed = old != new or plan["operation"] in {"add", "delete", "move"}
        entry = {
            "operation": plan["operation"],
            "path": plan["display"],
            "changed": changed,
            "old_revision": _sha256(old) if old_value is not None else None,
            "revision": _sha256(new) if plan["new"] is not None else None,
            "size": len(new) if plan["new"] is not None else 0,
        }
        if plan["source_display"] is not None:
            entry["source"] = plan["source_display"]
        output.append(entry)
        if not truncated:
            for line in difflib.unified_diff(
                _diff_lines(old.decode("utf-8", "replace")),
                _diff_lines(new.decode("utf-8", "replace")),
                fromfile=(plan["source_display"] or plan["display"])
                if old_value is not None
                else "/dev/null",
                tofile=plan["display"] if plan["new"] is not None else "/dev/null",
            ):
                if not line.endswith("\n"):
                    line += "\n\\ No newline at end of file\n"
                encoded = line.encode("utf-8")
                if total + len(encoded) > limit:
                    truncated = True
                    break
                diff_parts.append(line)
                total += len(encoded)
    return {
        "changes": output,
        "dry_run": dry_run,
        "diff": "".join(diff_parts),
        "diff_truncated": truncated,
    }


def _commit(plans: list[dict[str, Any]], states: dict[Path, _State], limit: int) -> dict[str, Any]:
    temporaries: dict[Path, Path] = {}
    backups: dict[Path, Path] = {}
    created_dirs: list[Path] = []
    committed: list[
        tuple[Path, bytes | None, os.stat_result | None, bytes | None, os.stat_result | None]
    ] = []
    preserve_backups = False
    primary: BaseException | None = None
    wrapped: BaseException | None = None
    completed = False
    active_plans = [
        plan for plan in plans if not (plan["operation"] == "update" and plan["old"] == plan["new"])
    ]
    try:
        for plan in active_plans:
            if plan["new"] is None:
                continue
            parent = plan["path"].parent
            if not parent.exists():
                missing: list[Path] = []
                current = parent
                while not current.exists():
                    missing.append(current)
                    current = current.parent
                for directory in reversed(missing):
                    directory.mkdir()
                    created_dirs.append(directory)
            fd, name = tempfile.mkstemp(prefix=f".{plan['path'].name}.", dir=parent)
            temp = Path(name)
            temporaries[plan["path"]] = temp
            with os.fdopen(fd, "wb") as stream:
                if plan["mode"] is not None:
                    os.fchmod(stream.fileno(), plan["mode"])
                stream.write(plan["new"])
                stream.flush()
                os.fsync(stream.fileno())
        for plan in active_plans:
            backup_path = plan["source"] or plan["path"]
            state = states[backup_path]
            if not state.exists or backup_path in backups:
                continue
            fd, name = tempfile.mkstemp(
                prefix=f".{backup_path.name}.mypr-backup.", dir=backup_path.parent
            )
            backup = Path(name)
            backups[backup_path] = backup
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), stat.S_IMODE(state.info.st_mode))
                stream.write(state.data)
                stream.flush()
                os.fsync(stream.fileno())
        for plan in active_plans:
            path = plan["path"]
            state = states.get(path)
            current = _read_state(path, plan["display"])
            if state is None:
                state = _State(path, plan["display"], False, None, None)
            _assert_unchanged(state, current)
            if plan["operation"] == "delete":
                path.unlink()
                committed.append((path, plan["old"], plan["old_info"], None, None))
            elif state.exists:
                os.replace(temporaries[path], path)
                committed.append((path, plan["old"], plan["old_info"], plan["new"], None))
            else:
                os.link(temporaries[path], path)
                committed.append((path, plan["old"], plan["old_info"], plan["new"], None))
                temporaries[path].unlink(missing_ok=True)
            if plan["operation"] != "delete":
                post_info = path.stat()
                committed[-1] = (
                    path,
                    plan["old"],
                    plan["old_info"],
                    plan["new"],
                    post_info,
                )
            if plan["source"] is not None:
                source = plan["source"]
                source_state = states[source]
                current_source = _read_state(source, plan["source_display"] or str(source))
                _assert_unchanged(source_state, current_source)
                source.unlink()
                committed.append((source, source_state.data, source_state.info, None, None))
        result = _result(plans, False, limit)
        completed = True
        return result
    except BaseException as exc:
        primary = exc
        recovery: list[str] = []
        for path, old, old_info, expected_new, expected_info in reversed(committed):
            try:
                current = _read_state(path, str(path))
                if expected_new is None and current.exists:
                    raise RuntimeError("path changed outside this patch")
                if expected_new is not None and (
                    not current.exists or current.data != expected_new
                ):
                    raise RuntimeError("path changed outside this patch")
                if expected_info is not None and (
                    current.info is None or _signature(current.info) != _signature(expected_info)
                ):
                    raise RuntimeError("path metadata changed outside this patch")
                if old is None:
                    if current.exists:
                        path.unlink()
                else:
                    _restore(path, old, old_info)
            except BaseException as restore_exc:
                recovery.append(f"{path}: {restore_exc}")
        if recovery:
            preserve_backups = True
            wrapped = RuntimeError(
                "patch failed and rollback was incomplete: "
                f"{'; '.join(recovery)}; backups preserved at "
                f"{', '.join(str(path) for path in backups.values())}"
            )
            raise wrapped from exc
        raise
    finally:
        cleanup_errors: list[str] = []
        for temporary in temporaries.values():
            try:
                temporary.unlink(missing_ok=True)
            except BaseException as cleanup_exc:
                cleanup_errors.append(f"{temporary}: {cleanup_exc}")
        if not preserve_backups:
            for backup in backups.values():
                try:
                    backup.unlink(missing_ok=True)
                except BaseException as cleanup_exc:
                    cleanup_errors.append(f"{backup}: {cleanup_exc}")
        if not completed:
            for directory in reversed(created_dirs):
                try:
                    directory.rmdir()
                except BaseException as cleanup_exc:
                    cleanup_errors.append(f"{directory}: {cleanup_exc}")
        if cleanup_errors:
            preserve_backups = True
            note = "patch cleanup failed; artifacts may remain at: " + "; ".join(cleanup_errors)
            target = wrapped or primary
            if target is not None:
                target.add_note(note)
            elif completed:
                result["warnings"] = [note]
            else:
                raise RuntimeError(note)


def _assert_unchanged(expected: _State, current: _State) -> None:
    if (
        expected.exists != current.exists
        or expected.data != current.data
        or expected.info is not None
        and current.info is not None
        and _signature(expected.info) != _signature(current.info)
    ):
        raise RuntimeError(f"File changed while applying patch: {expected.display}")


def _restore(path: Path, data: bytes, info: os.stat_result | None) -> None:
    mode = stat.S_IMODE(info.st_mode) if info is not None else 0o600
    parent = path.parent
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.rollback.", dir=parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
