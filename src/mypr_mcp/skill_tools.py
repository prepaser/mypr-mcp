"""Validation and revision-aware writes for workspace skills."""

from __future__ import annotations

import asyncio
import difflib
import hashlib
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .persistence import await_completion
from .revisions import RevisionStore


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
        return _validate_skill(name, path, text, self.root)

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
        if "name" in metadata and not isinstance(metadata["name"], str):
            errors.append("front matter name must be a string")
        if "description" in metadata and not isinstance(metadata["description"], str):
            errors.append("front matter description must be a string")
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


def _front_matter(text: str) -> dict[str, Any] | str | None:
    if not text.startswith("---"):
        return None
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != "---":
        return "YAML front matter must start with ---"
    closing = None
    for index, line in enumerate(lines[1:], start=1):
        if line.rstrip("\r\n") == "---":
            closing = index
            break
    if closing is None:
        return "YAML front matter is missing its closing ---"
    body = "".join(lines[1:closing])
    try:
        import yaml

        parsed = yaml.safe_load(body)
    except ImportError:
        parsed = _minimal_yaml(body)
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        return f"Invalid YAML front matter: {detail}"
    if parsed is None:
        return {}
    if not isinstance(parsed, Mapping):
        return "YAML front matter root must be a mapping"
    return dict(parsed)


def _minimal_yaml(body: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in body.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if ":" not in line:
            raise ValueError("expected key: value")
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


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
