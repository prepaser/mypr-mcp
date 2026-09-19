"""Read-only, structured Git views for the workspace API."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from .snapshots import SnapshotStore

_MAX_COMMAND_BYTES = 16 * 1024 * 1024
_DEFAULT_RESPONSE_BYTES = 32 * 1024


class Git:
    def __init__(self, workspace: str | os.PathLike[str], shell: Any):
        self.workspace = Path(workspace).expanduser().resolve()
        self.shell = shell
        self.snapshots = SnapshotStore(self.workspace / ".mypr", name="git")

    async def status(
        self,
        *,
        cursor: str | None = None,
        max_entries: int = 200,
        max_bytes: int = _DEFAULT_RESPONSE_BYTES,
    ) -> dict[str, Any]:
        self._validate_limit(max_bytes)
        self._validate_entries(max_entries)
        if cursor is not None:
            snapshot, offset = await asyncio.to_thread(
                self.snapshots.decode, cursor, expected_kind="status"
            )
            return await asyncio.to_thread(
                self._status_page, snapshot, offset, max_entries, max_bytes
            )
        root = await self._repo_root()
        result = await self._run(
            [
                "status",
                "--porcelain=v2",
                "--branch",
                "-z",
                "--untracked-files=all",
            ]
        )
        return await asyncio.to_thread(self._status_snapshot, result, max_entries, max_bytes, root)

    def _status_snapshot(
        self, result: dict[str, Any], max_entries: int, max_bytes: int, root: Path
    ) -> dict[str, Any]:
        records = self._split_nul(result["stdout"])
        branch: dict[str, Any] = {}
        files: list[dict[str, Any]] = []
        index = 0
        while index < len(records):
            record = records[index]
            if record.startswith("# "):
                self._branch_record(branch, record[2:])
                index += 1
                continue
            original = None
            if record.startswith("2 ") and index + 1 < len(records):
                original = records[index + 1]
                index += 1
            parsed = self._status_record(record, original)
            if parsed is not None:
                files.append(parsed)
            index += 1
        ident = self.snapshots.create(
            {},
            files,
            kind="status",
            branch=branch,
            root=str(root),
            truncated=bool(result.get("truncated")),
            warnings=list(result.get("warnings", [])),
        )
        page = self._status_page(self.snapshots.load(ident), 0, max_entries, max_bytes)
        page.update(snapshot_id=ident)
        return page

    def _status_page(
        self, snapshot: dict[str, Any], offset: int, max_entries: int, max_bytes: int
    ) -> dict[str, Any]:
        items, index = self._items_page(snapshot["items"], offset, max_entries, max_bytes)
        more = index < len(snapshot["items"])
        next_cursor = self.snapshots.cursor(snapshot["id"], index, "status") if more else None
        return {
            "root": snapshot.get("root", str(self.workspace)),
            "workspace": str(self.workspace),
            "branch": snapshot.get("branch", {}),
            "files": items,
            "cursor": next_cursor,
            "next_cursor": next_cursor,
            "has_more": more,
            "truncated": bool(snapshot.get("truncated")) or more,
            "scan_truncated": bool(snapshot.get("truncated")),
            "warnings": list(snapshot.get("warnings", [])),
        }

    async def diff(
        self,
        *,
        staged: bool = False,
        rev: str | None = None,
        paths: str | list[str] | None = None,
        cursor: str | None = None,
        max_bytes: int = _DEFAULT_RESPONSE_BYTES,
    ) -> dict[str, Any]:
        self._validate_limit(max_bytes)
        if cursor is not None:
            snapshot, offset = await asyncio.to_thread(
                self.snapshots.decode, cursor, expected_kind="diff"
            )
            return await asyncio.to_thread(self._text_page, snapshot, offset, max_bytes)
        root = await self._repo_root()
        common = ["diff", "--no-ext-diff", "--no-textconv", "--no-color"]
        if staged:
            common.append("--cached")
        if rev is not None:
            if not isinstance(rev, str) or not rev or rev.startswith("-"):
                raise ValueError("rev must be a non-empty string")
            common.append(rev)
        selected = self._paths(paths, root)
        metadata = await self._run([*common, "--name-status", "-z", "--", *selected])
        patch = await self._run(
            [*common, "--binary", "--full-index", "--", *selected], max_bytes=_MAX_COMMAND_BYTES
        )
        return await asyncio.to_thread(
            self._diff_snapshot, metadata, patch, staged, rev, paths, max_bytes, root
        )

    def _diff_snapshot(
        self,
        metadata: dict[str, Any],
        patch: dict[str, Any],
        staged: bool,
        rev: str | None,
        paths: str | list[str] | None,
        max_bytes: int,
        root: Path,
    ) -> dict[str, Any]:
        files = self._diff_files(metadata["stdout"])
        lines = [{"file": item} for item in files]
        lines.extend({"text": text} for text in self._chunk_text(patch["stdout"]))
        ident = self.snapshots.create(
            {"staged": staged, "rev": rev, "paths": paths},
            lines,
            kind="diff",
            truncated=bool(patch.get("truncated") or metadata.get("truncated")),
            warnings=list(metadata.get("warnings", [])) + list(patch.get("warnings", [])),
            root=str(root),
        )
        page = self._text_page(self.snapshots.load(ident), 0, max_bytes)
        page.update(snapshot_id=ident)
        return page

    def _text_page(self, snapshot: dict[str, Any], offset: int, max_bytes: int) -> dict[str, Any]:
        items, index = self._items_page(snapshot["items"], offset, max_bytes=max_bytes)
        more = index < len(snapshot["items"])
        next_cursor = (
            self.snapshots.cursor(snapshot["id"], index, snapshot.get("kind")) if more else None
        )
        text = (
            "".join(item.get("text", "") for item in items)
            if snapshot["kind"] == "diff"
            else "".join(items)
        )
        result = {
            "root": snapshot.get("root", str(self.workspace)),
            "workspace": str(self.workspace),
            "patch" if snapshot.get("kind") == "diff" else "text": text,
            "cursor": next_cursor,
            "next_cursor": next_cursor,
            "has_more": more,
            "truncated": bool(snapshot.get("truncated")) or more,
            "scan_truncated": bool(snapshot.get("truncated")),
            "warnings": list(snapshot.get("warnings", [])),
        }
        if snapshot.get("kind") == "diff":
            result["files"] = [item["file"] for item in items if "file" in item]
        else:
            result.update(ref=snapshot.get("ref"), path=snapshot.get("path"))
        return result

    async def show(
        self,
        ref: str = "HEAD",
        *,
        path: str | None = None,
        cursor: str | None = None,
        max_bytes: int = _DEFAULT_RESPONSE_BYTES,
    ) -> dict[str, Any]:
        self._validate_limit(max_bytes)
        if cursor is not None:
            snapshot, offset = await asyncio.to_thread(
                self.snapshots.decode, cursor, expected_kind="show"
            )
            return await asyncio.to_thread(self._text_page, snapshot, offset, max_bytes)
        if not isinstance(ref, str) or not ref or ref.startswith("-"):
            raise ValueError("ref must be a non-empty string")
        if path is not None and not isinstance(path, str):
            raise TypeError("path must be a string or None")
        root = await self._repo_root()
        target = ref if path is None else f"{ref}:{self._path(path, root)}"
        result = await self._run(
            ["show", "--no-ext-diff", "--no-textconv", "--no-color", "--end-of-options", target],
            max_bytes=_MAX_COMMAND_BYTES,
        )
        return await asyncio.to_thread(self._show_snapshot, result, ref, path, max_bytes, root)

    def _show_snapshot(
        self, result: dict[str, Any], ref: str, path: str | None, max_bytes: int, root: Path
    ) -> dict[str, Any]:
        ident = self.snapshots.create(
            {"ref": ref, "path": path},
            self._chunk_text(result["stdout"]),
            kind="show",
            ref=ref,
            path=path,
            truncated=bool(result.get("truncated")),
            warnings=list(result.get("warnings", [])),
            root=str(root),
        )
        page = self._text_page(self.snapshots.load(ident), 0, max_bytes)
        page.update(snapshot_id=ident)
        return page

    async def _run(self, args: list[str], *, max_bytes: int = _MAX_COMMAND_BYTES) -> dict[str, Any]:
        result = await self.shell.run(
            [
                "git",
                "--no-pager",
                "--no-optional-locks",
                "-c",
                "color.ui=false",
                "-c",
                "diff.relative=false",
                "-C",
                str(getattr(self, "repo", self.workspace)),
                *args,
            ],
            cwd=self.workspace,
            check=False,
            max_bytes=max_bytes,
            env={**os.environ, "GIT_PAGER": "cat", "GIT_OPTIONAL_LOCKS": "0"},
        )
        if result.get("error"):
            raise RuntimeError(f"git failed: {result['error']}")
        if result.get("returncode") != 0:
            detail = str(result.get("stderr", "")).strip() or "git command failed"
            raise RuntimeError(detail)
        return result

    async def _repo_root(self) -> Path:
        result = await self._run(["rev-parse", "--show-toplevel"], max_bytes=4096)
        value = str(result.get("stdout", "")).removesuffix("\n")
        if not value:
            raise RuntimeError("git did not report a repository root")
        self.repo = Path(value)
        return self.repo

    def _path(self, value: str, root: Path | None = None) -> str:
        base = root or self.workspace
        path = Path(value)
        candidate = path if path.is_absolute() else self.workspace / path
        candidate = Path(os.path.normpath(str(candidate.absolute())))
        try:
            relative = candidate.relative_to(base)
        except ValueError as exc:
            raise ValueError("path must remain below the repository") from exc
        return relative.as_posix()

    def _paths(self, paths: str | list[str] | None, root: Path | None = None) -> list[str]:
        if paths is None:
            return []
        values = [paths] if isinstance(paths, str) else paths
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            raise TypeError("paths must be a string, list of strings, or None")
        return [":(top,literal)" + self._path(item, root) for item in values]

    @staticmethod
    def _validate_limit(value: int) -> None:
        if type(value) is not int or not 1024 <= value <= _MAX_COMMAND_BYTES:
            raise ValueError(f"max_bytes must be between 1024 and {_MAX_COMMAND_BYTES}")

    @staticmethod
    def _validate_entries(value: int) -> None:
        if type(value) is not int or not 1 <= value <= 100_000:
            raise ValueError("max_entries must be between 1 and 100000")

    @staticmethod
    def _items_page(
        items: list[Any],
        offset: int,
        max_entries: int | None = None,
        max_bytes: int = _DEFAULT_RESPONSE_BYTES,
    ) -> tuple[list[Any], int]:
        page: list[Any] = []
        used = 0
        index = offset
        while index < len(items):
            item = items[index]
            if isinstance(item, str):
                cost = len(item.encode("utf-8"))
            else:
                cost = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
            if not page and cost > max_bytes:
                raise ValueError("max_bytes is too small for a Git record; increase the budget")
            if page and used + cost > max_bytes:
                break
            page.append(item)
            used += cost
            index += 1
            if max_entries is not None and len(page) >= max_entries:
                break
        return page, index

    @staticmethod
    def _chunk_text(text: str, chunk_bytes: int = 128) -> list[str]:
        raw = text.encode("utf-8")
        chunks: list[str] = []
        start = 0
        while start < len(raw):
            end = min(start + chunk_bytes, len(raw))
            while end > start and end < len(raw) and raw[end] & 0xC0 == 0x80:
                end -= 1
            if end == start:
                end = min(start + chunk_bytes, len(raw))
            chunks.append(raw[start:end].decode("utf-8"))
            start = end
        return chunks

    @staticmethod
    def _split_nul(text: str) -> list[str]:
        return [item for item in text.split("\0") if item]

    @staticmethod
    def _branch_record(branch: dict[str, Any], record: str) -> None:
        key, _, value = record.partition(" ")
        if key == "branch.oid":
            branch["oid"] = value
        elif key == "branch.head":
            branch["head"] = value
        elif key == "branch.upstream":
            branch["upstream"] = value
        elif key == "branch.ab":
            parts = value.split()
            if len(parts) == 2:
                branch["ahead"] = _signed_int(parts[0])
                branch["behind"] = _signed_int(parts[1])

    @staticmethod
    def _status_record(record: str, original_path: str | None = None) -> dict[str, Any] | None:
        kind = record[:1]
        if kind in {"?", "!"}:
            return {"index": kind, "worktree": kind, "path": record[2:]}
        count = {"1": 8, "2": 9, "u": 10}.get(kind)
        if count is None:
            return None
        fields = record.split(" ", count)
        if len(fields) != count + 1 or len(fields[1]) != 2:
            return None
        result = {
            "index": fields[1][0],
            "worktree": fields[1][1],
            "submodule": fields[2],
            "path": fields[-1],
        }
        if kind == "2":
            result.update(original_path=original_path, score=fields[8])
        if kind == "u":
            result["unmerged"] = True
        return result

    @staticmethod
    def _diff_files(text: str) -> list[dict[str, Any]]:
        records = Git._split_nul(text)
        output: list[dict[str, Any]] = []
        index = 0
        while index < len(records):
            status = records[index]
            if index + 1 < len(records):
                item: dict[str, Any] = {"status": status, "path": records[index + 1]}
                index += 1
                if status.startswith("R") or status.startswith("C"):
                    if index + 1 < len(records):
                        item["original_path"] = item["path"]
                        item["path"] = records[index + 1]
                        index += 1
                output.append(item)
            index += 1
        return output


def _signed_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None
