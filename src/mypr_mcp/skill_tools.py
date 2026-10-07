"""Validation and revision-aware writes for workspace skills."""

from __future__ import annotations

import asyncio
import datetime as _datetime
import difflib
import hashlib
import os
import re
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .json_utils import json_bytes
from .persistence import await_completion
from .revisions import RevisionStore

_MAX_DISCOVERY_ERRORS = 16


def _bounded_text(value: Any, limit: int = 512) -> str:
    try:
        text = str(value)
        try:
            raw = text.encode("utf-8", "surrogateescape")
        except UnicodeEncodeError:
            raw = text.encode("utf-8", "backslashreplace")
        return raw[:limit].decode("utf-8", "surrogateescape")
    except Exception:
        return type(value).__name__


def _discovery_error(state: dict[str, Any], path: Path, error: OSError) -> None:
    state["error_count"] += 1
    errors = state.setdefault("errors", [])
    if len(errors) >= _MAX_DISCOVERY_ERRORS:
        return
    errors.append(
        {
            "code": "discovery_error",
            "message": _bounded_text(f"Unable to inspect {path}: {error}"),
        }
    )


def _iter_skill_paths(root: Path, state: dict[str, Any]):
    logical_root = Path(root)
    try:
        root_stat = logical_root.stat()
    except FileNotFoundError:
        return
    except OSError as exc:
        _discovery_error(state, logical_root, exc)
        return
    if not stat.S_ISDIR(root_stat.st_mode):
        return
    try:
        resolved_root = logical_root.resolve()
    except OSError as exc:
        _discovery_error(state, logical_root, exc)
        return
    pending = [(resolved_root, logical_root, frozenset({resolved_root}))]
    while pending and not state["scan_truncated"]:
        directory, logical_directory, ancestors = pending.pop()
        try:
            with os.scandir(directory) as iterator:
                entries = []
                for entry in iterator:
                    state["scanned"] += 1
                    if state["scanned"] > state["scan_limit"]:
                        state["scan_truncated"] = True
                        break
                    entries.append(entry)
            entries.sort(key=lambda entry: entry.name)
        except OSError as exc:
            _discovery_error(state, logical_directory, exc)
            continue
        directories = []
        for entry in entries:
            candidate = logical_directory / entry.name
            try:
                resolved = candidate.resolve()
                resolved.relative_to(resolved_root)
                entry_stat = entry.stat(follow_symlinks=True)
                entry_is_file = stat.S_ISREG(entry_stat.st_mode)
                entry_is_dir = stat.S_ISDIR(entry_stat.st_mode)
                if directory != resolved_root and entry.name == "SKILL.md" and entry_is_file:
                    yield candidate
                elif entry_is_dir and resolved not in ancestors:
                    if entry.is_symlink():
                        skill = candidate / "SKILL.md"
                        skill.resolve().relative_to(resolved_root)
                        if stat.S_ISREG(skill.stat().st_mode):
                            yield skill
                    else:
                        directories.append(
                            (resolved, candidate, ancestors | {resolved})
                        )
            except ValueError:
                continue
            except OSError as exc:
                _discovery_error(state, candidate, exc)
                continue
        pending.extend(reversed(directories))


def skill_paths_page(
    root: str | os.PathLike[str],
    *,
    offset: int = 0,
    limit: int = 100,
    scan_limit: int = 10_000,
) -> dict[str, Any]:
    """Return a bounded skill path page without collecting the whole tree."""

    if type(offset) is not int or offset < 0:
        raise ValueError("offset must be a non-negative integer")
    if type(limit) is not int or limit < 1:
        raise ValueError("limit must be a positive integer")
    if type(scan_limit) is not int or scan_limit < 1:
        raise ValueError("scan_limit must be a positive integer")
    if offset > scan_limit:
        raise ValueError("offset must not exceed scan_limit")
    paths: list[Path] = []
    seen = 0
    has_more = False
    state = {
        "scanned": 0,
        "scan_limit": scan_limit,
        "scan_truncated": False,
        "errors": [],
        "error_count": 0,
    }
    for path in _iter_skill_paths(Path(root), state):
        if seen < offset:
            seen += 1
            continue
        if len(paths) >= limit:
            has_more = True
            break
        paths.append(path)
        seen += 1
    has_more |= bool(state["scan_truncated"])
    return {
        "paths": paths,
        "next_offset": offset + len(paths) if has_more else None,
        "has_more": has_more,
        "scanned": state["scanned"],
        "scan_truncated": state["scan_truncated"],
        "errors": list(state["errors"]),
        "complete": not state["scan_truncated"] and not state["errors"],
        "warnings_truncated": state["error_count"] > len(state["errors"]),
        "error_count": state["error_count"],
    }


class SkillsWriting:
    """Mixin for the existing ``Skills`` API.

    The host class supplies ``root``, ``_path`` and ``_metadata``.  It also
    supplies ``_fs`` (a :class:`~mypr_mcp.filesystem.Filesystem`) when the
    mixin is initialized.
    """

    async def validate(self, name: str, text: str | None = None) -> dict[str, Any]:
        path = self._skill_path(name)
        if text is None:
            if self._fs is not None:
                page = await self._fs.read(self._relative(path), max_bytes=16 * 1024 * 1024)
                if page.get("truncated"):
                    raise ValueError("skill text exceeds max_bytes; pass the full text explicitly")
                text = page["text"]
            else:
                text = path.read_text(encoding="utf-8")
        if not isinstance(text, str):
            raise TypeError("text must be a string or None")
        await self._ensure_yaml(text)
        return _validate_skill(name, path, text, self.root)

    async def _ensure_yaml(self, text: str) -> None:
        """Prepare PyYAML only when *text* contains a YAML front matter body."""

        body = _front_matter_body(text)
        if body is None or not body.strip() or self._fs is None:
            return
        ensure = getattr(self._fs, "_ensure", None)
        if callable(ensure):
            await ensure("pyyaml")

    async def write(
        self,
        name: str,
        text: str,
        *,
        expected_hash: str | None = None,
        dry_run: bool = False,
        max_diff_bytes: int = 32 * 1024,
    ) -> dict[str, Any]:
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        if self._fs is None:
            raise RuntimeError("skill writes require a workspace filesystem")
        path = self._skill_path(name)
        validation = await self.validate(name, text)
        if not validation["valid"]:
            details = "; ".join(validation["errors"])
            raise ValueError(f"Invalid skill: {details}")
        relative = self._relative(path)
        revisions = self._revision_store()
        task = asyncio.create_task(
            self._write_transaction(
                name,
                relative,
                text,
                expected_hash,
                dry_run,
                max_diff_bytes,
                validation,
                revisions,
            )
        )
        return await await_completion(task)

    async def _write_transaction(
        self,
        name: str,
        relative: str,
        text: str,
        expected_hash: str | None,
        dry_run: bool,
        max_diff_bytes: int,
        validation: dict[str, Any],
        revisions: RevisionStore,
    ) -> dict[str, Any]:
        async with revisions.transaction(relative):
            try:
                old_result = await self._fs.read(relative, max_bytes=16 * 1024 * 1024)
                if old_result.get("truncated"):
                    raise ValueError("existing skill exceeds max_bytes; refusing partial CAS")
                old = old_result["text"]
                old_revision = old_result["revision"]
            except FileNotFoundError:
                old = None
                old_revision = None
            _check_hash(old, expected_hash, old_revision)
            if dry_run:
                diff, diff_truncated = _diff(relative, old or "", text, max_diff_bytes)
                return {
                    "name": name,
                    "path": relative,
                    "changed": old != text,
                    "dry_run": True,
                    "old_revision": old_revision,
                    "revision": _sha256(text),
                    "diff": diff,
                    "diff_truncated": diff_truncated,
                    "warnings": validation["warnings"],
                }
            result = await revisions.commit(
                relative,
                old,
                text,
                expected_hash=expected_hash,
            )
            return {
                "name": name,
                "path": relative,
                "changed": old != text,
                "dry_run": False,
                "old_revision": old_revision,
                "warnings": validation["warnings"],
                **result,
            }

    async def history(
        self, name: str, *, limit: int = 20, cursor: int | None = None
    ) -> dict[str, Any]:
        relative = self._relative(self._skill_path(name))
        revisions = self._revision_store()
        async with revisions.transaction(relative):
            return {"name": name, **await revisions.history(relative, limit=limit, cursor=cursor)}

    async def read_revision(
        self,
        name: str,
        revision: str,
        *,
        start_byte: int = 0,
        max_bytes: int = 32_768,
    ) -> dict[str, Any]:
        relative = self._relative(self._skill_path(name))
        revisions = self._revision_store()
        async with revisions.transaction(relative):
            return {
                "name": name,
                **await revisions.read_revision(
                    relative, revision, start_byte=start_byte, max_bytes=max_bytes
                ),
            }

    async def restore(
        self, name: str, revision: str, *, expected_hash: str | None = None
    ) -> dict[str, Any]:
        if self._fs is None:
            raise RuntimeError("skill writes require a workspace filesystem")
        path = self._skill_path(name)
        relative = self._relative(path)
        revisions = self._revision_store()
        task = asyncio.create_task(
            self._restore_transaction(name, relative, revision, expected_hash, revisions)
        )
        return await await_completion(task)

    async def _restore_transaction(
        self,
        name: str,
        relative: str,
        revision: str,
        expected_hash: str | None,
        revisions: RevisionStore,
    ) -> dict[str, Any]:
        async with revisions.transaction(relative):
            text = await revisions.restore_content(relative, revision)
            validation = await self.validate(name, text)
            if not validation["valid"]:
                details = "; ".join(validation["errors"])
                raise ValueError(f"Invalid skill revision: {details}")
            try:
                current = await self._fs.read(relative, max_bytes=64 * 1024 * 1024)
                if current.get("truncated"):
                    raise ValueError("existing skill exceeds max_bytes; refusing partial CAS")
                old = current["text"]
                old_revision = current["revision"]
            except FileNotFoundError:
                old = None
                old_revision = None
            if old is not None and expected_hash is None:
                raise FileExistsError("Skill already exists; provide expected_hash to restore")
            if expected_hash is not None and old_revision != expected_hash:
                raise ValueError(f"Revision mismatch: expected {expected_hash}, got {old_revision}")
            if old == text:
                return {
                    "name": name,
                    "path": relative,
                    "changed": False,
                    "revision": revision,
                    "old_revision": old_revision,
                    "activated": False,
                    "warnings": validation["warnings"],
                }
            result = await revisions.commit(
                relative,
                old,
                text,
                expected_hash=expected_hash,
            )
            return {
                "name": name,
                "path": relative,
                "changed": True,
                "old_revision": old_revision,
                "activated": False,
                "warnings": validation["warnings"],
                **result,
            }

    def _revision_store(self) -> RevisionStore:
        if self._fs is None:
            raise RuntimeError("skill revision history requires a workspace filesystem")
        return RevisionStore(self._fs.workspace, self._fs, "skills")

    def _skill_path(self, name: str) -> Path:
        _validate_skill_name(name)
        return self._path(name)

    def _relative(self, path: Path) -> str:
        workspace = getattr(self._fs, "workspace", None)
        if workspace is None:
            # The regular Skills root is below the workspace's .mypr folder.
            workspace = self.root.parent.parent
        return str(path.resolve().relative_to(Path(workspace).resolve()))


class _MetadataLimitError(ValueError):
    pass


def _read_metadata_prefix(prefix: bytes) -> str | None:
    if not prefix.startswith(b"---"):
        return None
    lines = prefix.splitlines(keepends=True)
    for index, line in enumerate(lines[1:], start=1):
        if line.rstrip(b"\r\n") == b"---":
            return b"".join(lines[: index + 1]).decode("utf-8")
    return prefix.decode("utf-8", errors="replace")


def _read_prefix(path: Path, max_bytes: int) -> tuple[bytes, bool]:
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError(f"skill path is not a regular file: {path}")
        data = os.read(descriptor, max_bytes + 1)
    finally:
        os.close(descriptor)
    return data[:max_bytes], len(data) > max_bytes


def _bounded_metadata(value: Any) -> tuple[Any, bool]:
    active: set[int] = set()
    truncated = [False]
    result = _bounded_metadata_value(value, active=active, depth=0, nodes=[0], truncated=truncated)
    return result, truncated[0]


def _bounded_metadata_value(
    value: Any,
    *,
    active: set[int],
    depth: int,
    nodes: list[int],
    truncated: list[bool],
) -> Any:
    nodes[0] += 1
    if nodes[0] > 256:
        raise _MetadataLimitError("skill metadata exceeds the item limit")
    if depth > 16:
        raise _MetadataLimitError("skill metadata exceeds the nesting limit")
    if isinstance(value, str):
        if len(value) > 4096:
            truncated[0] = True
        return value[:4096]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, (_datetime.date, _datetime.datetime, _datetime.time)):
        return value.isoformat()
    identity = id(value)
    if identity in active:
        raise _MetadataLimitError("skill metadata contains a recursive YAML alias")
    active.add(identity)
    try:
        if isinstance(value, Mapping):
            result = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= 256:
                    raise _MetadataLimitError("skill metadata exceeds the item limit")
                bounded_key = _bounded_metadata_value(
                    key,
                    active=active,
                    depth=depth + 1,
                    nodes=nodes,
                    truncated=truncated,
                )
                if not isinstance(bounded_key, str):
                    bounded_key = str(bounded_key)
                result[bounded_key] = _bounded_metadata_value(
                    item,
                    active=active,
                    depth=depth + 1,
                    nodes=nodes,
                    truncated=truncated,
                )
            return result
        if isinstance(value, (list, tuple, set, frozenset)):
            if len(value) > 256:
                raise _MetadataLimitError("skill metadata exceeds the item limit")
            return [
                _bounded_metadata_value(
                    item,
                    active=active,
                    depth=depth + 1,
                    nodes=nodes,
                    truncated=truncated,
                )
                for item in value
            ]
        text = str(value)
        if len(text) > 4096:
            truncated[0] = True
        return text[:4096]
    finally:
        active.remove(identity)


def _json_bytes(value: Any) -> bytes:
    return json_bytes(value, separators=(",", ":"))


def _fit_skill_item(item: dict[str, Any], max_bytes: int) -> dict[str, Any]:
    try:
        if len(_json_bytes([item])) <= max_bytes:
            return item
    except (TypeError, ValueError, RecursionError):
        pass
    name = str(item.get("name", ""))
    path = str(item.get("path", ""))
    description = item.get("description")
    reduced = {
        "name": name,
        "path": path,
        "error": "skill metadata exceeds the response byte budget",
        "metadata_truncated": True,
    }
    if isinstance(description, str):
        reduced["description"] = description[:4096]
    while len(_json_bytes([reduced])) > max_bytes:
        if reduced.get("description"):
            reduced["description"] = reduced["description"][: len(reduced["description"]) // 2]
        elif "error" in reduced:
            reduced.pop("error")
        else:
            break
    return reduced


def _mark_skill_list_truncated(
    items: list[dict[str, Any]], omitted: int, max_bytes: int
) -> None:
    if not items:
        return
    item = items[-1]
    item["list_truncated"] = True
    item["omitted"] = max(0, omitted)
    reserved = {
        "name",
        "path",
        "description",
        "error",
        "metadata_truncated",
        "list_truncated",
        "omitted",
        "omitted_at_least",
        "next_offset",
        "scan_truncated",
    }
    while len(_json_bytes(items)) > max_bytes:
        candidates = [key for key in item if key not in reserved]
        if candidates:
            key = max(candidates, key=lambda candidate: len(_json_bytes(item[candidate])))
            item.pop(key)
            item["metadata_truncated"] = True
        elif item.get("description"):
            item["description"] = item["description"][: len(item["description"]) // 2]
            item["metadata_truncated"] = True
        elif len(items) > 1:
            items.pop()
            omitted += 1
            item = items[-1]
            item["list_truncated"] = True
            item["omitted"] = max(0, omitted)
        else:
            break


def _validate_skill(name: str, path: Path, text: str, root: Path) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    try:
        _validate_skill_name(name)
    except ValueError as exc:
        errors.append(str(exc))
    front = _front_matter(text)
    if front is None:
        warnings.append("legacy skill without YAML front matter")
    elif isinstance(front, str):
        errors.append(front)
    else:
        metadata = front
        errors.extend(_metadata_errors(metadata))
        if "name" not in metadata:
            warnings.append("front matter name is missing")
        if "description" not in metadata:
            warnings.append("front matter description is missing")
    warnings.extend(_markdown_diagnostics(path, text, root))
    return {
        "valid": not errors,
        "name": name,
        "path": str(path),
        "metadata": metadata,
        "warnings": warnings,
        "errors": errors,
    }


def _validate_skill_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("skill name must be a non-empty path")
    if "\x00" in name or "\\" in name or name.startswith("/"):
        raise ValueError("skill name must be a relative path")
    parts = name.split("/")
    if any(not part or part in {".", ".."} for part in parts):
        raise ValueError("skill name contains an invalid path component")


def _metadata_errors(metadata: Mapping[str, Any]) -> list[str]:
    return [
        f"front matter {key} must be a string"
        for key in ("name", "description")
        if key in metadata and not isinstance(metadata[key], str)
    ]


def _front_matter(text: str) -> dict[str, Any] | str | None:
    body = _front_matter_body(text)
    if body is None:
        if not text.startswith("---"):
            return None
        lines = text.splitlines(keepends=True)
        if not lines or lines[0].rstrip("\r\n") != "---":
            return "YAML front matter must start with ---"
        return "YAML front matter is missing its closing ---"
    if not body.strip():
        return {}
    try:
        import yaml
    except ImportError:
        return "PyYAML is required to parse YAML front matter"
    try:
        parsed = yaml.safe_load(body)
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        return f"Invalid YAML front matter: {detail}"
    if parsed is None:
        return {}
    if not isinstance(parsed, Mapping):
        return "YAML front matter root must be a mapping"
    return dict(parsed)


def _front_matter_body(text: str) -> str | None:
    if not text.startswith("---"):
        return None
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != "---":
        return None
    for index, line in enumerate(lines[1:], start=1):
        if line.rstrip("\r\n") == "---":
            return "".join(lines[1:index])
    return None


_MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+[^)]*)?\)")


def _markdown_diagnostics(path: Path, text: str, root: Path) -> list[str]:
    warnings: list[str] = []
    skill_root = path.parent.resolve()
    workspace_root = root.resolve()
    for target in _MARKDOWN_LINK.findall(text):
        if target.startswith(("#", "/", "\\")) or "://" in target:
            continue
        target_path = (skill_root / target.split("#", 1)[0]).resolve()
        try:
            target_path.relative_to(workspace_root)
        except ValueError:
            warnings.append(f"local Markdown reference escapes skills root: {target}")
            continue
        if not target_path.exists():
            warnings.append(f"local Markdown reference is missing: {target}")
    return warnings


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _check_hash(old: str | None, expected: str | None, actual_revision: str | None = None) -> None:
    if expected is None:
        if old is not None:
            raise FileExistsError("Skill already exists; provide expected_hash")
        return
    actual = actual_revision if old is not None else None
    if actual != expected:
        raise ValueError(f"Revision mismatch: expected {expected}, got {actual}")


def _diff(path: str, old: str, new: str, limit: int) -> tuple[str, bool]:
    if type(limit) is not int or limit < 1:
        raise ValueError("max_diff_bytes must be a positive integer")
    output = bytearray()
    truncated = False
    for line in difflib.unified_diff(
        old.splitlines(keepends=True), new.splitlines(keepends=True), fromfile=path, tofile=path
    ):
        encoded = line.encode()
        if len(output) + len(encoded) > limit:
            truncated = True
            break
        output.extend(encoded)
    return output.decode("utf-8", "replace"), truncated
