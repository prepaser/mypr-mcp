"""Read-only, structured Git views for the workspace API."""

from __future__ import annotations

import asyncio
import json
import os
import threading
from pathlib import Path
from typing import Any

from .snapshots import SnapshotStore

_MAX_COMMAND_BYTES = 16 * 1024 * 1024
_MAX_HISTORY_SCAN_BYTES = 2 * 1024 * 1024
_MAX_HISTORY_SNAPSHOTS = 32
_MAX_HISTORY_SNAPSHOT_BYTES = 16 * 1024 * 1024
_DEFAULT_RESPONSE_BYTES = 32 * 1024
_HISTORY_SNAPSHOT_LOCK = threading.Lock()


class Git:
    def __init__(self, workspace: str | os.PathLike[str], shell: Any):
        self.workspace = Path(workspace).expanduser().resolve()
        self.shell = shell
        self.snapshots = SnapshotStore(self.workspace / ".mypr", name="git")
        self.history_snapshots = SnapshotStore(self.workspace / ".mypr", name="git-history")

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

    async def log(
        self,
        ref: str = "HEAD",
        *,
        path: str | None = None,
        author: str | None = None,
        since: str | None = None,
        until: str | None = None,
        cursor: str | None = None,
        max_entries: int = 50,
        max_bytes: int = _DEFAULT_RESPONSE_BYTES,
    ) -> dict[str, Any]:
        self._validate_limit(max_bytes)
        self._validate_entries(max_entries)
        if cursor is not None:
            snapshot, offset = await asyncio.to_thread(
                self.history_snapshots.decode, cursor, expected_kind="log"
            )
            return await asyncio.to_thread(
                self._history_page, snapshot, offset, max_entries, max_bytes
            )
        self._validate_ref(ref)
        self._validate_filter("path", path)
        self._validate_filter("author", author)
        self._validate_filter("since", since)
        self._validate_filter("until", until)
        root = await self._repo_root()
        commit = await self._resolve_commit(ref)
        query: dict[str, Any] = {
            "ref": ref,
            "path": path,
            "author": author,
            "since": since,
            "until": until,
        }
        args = ["log", "-z", "--format=%H%x00%an%x00%ae%x00%aI%x00%s"]
        if author is not None:
            args.append(f"--author={author}")
        if since is not None:
            args.append(f"--since={since}")
        if until is not None:
            args.append(f"--until={until}")
        args.append(commit)
        if path is not None:
            args.extend(["--", ":(top,literal)" + self._path(path, root)])
        result = await self._run(args, max_bytes=_MAX_HISTORY_SCAN_BYTES)
        items = self._parse_log(result["stdout"], truncated=bool(result.get("truncated")))
        ident, snapshot = await asyncio.to_thread(
            self._create_history_snapshot,
            query,
            items,
            kind="log",
            root=str(root),
            ref=commit,
            truncated=bool(result.get("truncated")),
            warnings=list(result.get("warnings", [])),
        )
        page = await asyncio.to_thread(self._history_page, snapshot, 0, max_entries, max_bytes)
        page["snapshot_id"] = ident
        return page

    async def blame(
        self,
        path: str | None = None,
        ref: str = "HEAD",
        *,
        start_line: int | None = None,
        end_line: int | None = None,
        cursor: str | None = None,
        max_entries: int = 100,
        max_bytes: int = _DEFAULT_RESPONSE_BYTES,
    ) -> dict[str, Any]:
        self._validate_limit(max_bytes)
        self._validate_entries(max_entries)
        if cursor is not None:
            snapshot, offset = await asyncio.to_thread(
                self.history_snapshots.decode, cursor, expected_kind="blame"
            )
            return await asyncio.to_thread(
                self._history_page, snapshot, offset, max_entries, max_bytes
            )
        if not isinstance(path, str) or not path:
            raise ValueError("path must be a non-empty string")
        self._validate_ref(ref)
        if (start_line is None) != (end_line is None):
            raise ValueError("start_line and end_line must be provided together")
        if start_line is not None and (
            type(start_line) is not int
            or type(end_line) is not int
            or start_line < 1
            or end_line < start_line
        ):
            raise ValueError("line range must use positive lines with end_line >= start_line")
        root = await self._repo_root()
        relative_path = self._path(path, root)
        commit = await self._resolve_commit(ref)
        query = {"path": path, "ref": ref, "start_line": start_line, "end_line": end_line}
        args = ["--literal-pathspecs", "blame", "--line-porcelain"]
        if start_line is not None:
            args.extend(["-L", f"{start_line},{end_line}"])
        args.extend([commit, "--", relative_path])
        result = await self._run(args, max_bytes=_MAX_HISTORY_SCAN_BYTES)
        items = self._parse_blame(result["stdout"], truncated=bool(result.get("truncated")))
        ident, snapshot = await asyncio.to_thread(
            self._create_history_snapshot,
            query,
            items,
            kind="blame",
            root=str(root),
            ref=commit,
            path=relative_path,
            truncated=bool(result.get("truncated")),
            warnings=list(result.get("warnings", [])),
        )
        page = await asyncio.to_thread(self._history_page, snapshot, 0, max_entries, max_bytes)
        page["snapshot_id"] = ident
        return page

    async def _resolve_commit(self, ref: str) -> str:
        result = await self._run(
            ["rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"],
            max_bytes=4096,
        )
        commit = str(result.get("stdout", "")).strip()
        if not commit or any(char not in "0123456789abcdefABCDEF" for char in commit):
            raise RuntimeError("git did not resolve ref to a commit")
        return commit.lower()

    def _create_history_snapshot(
        self,
        query: dict[str, Any],
        items: list[dict[str, Any]],
        *,
        kind: str,
        root: str,
        ref: str,
        truncated: bool,
        warnings: list[str],
        path: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        with _HISTORY_SNAPSHOT_LOCK:
            ident = self.history_snapshots.create(
                query,
                items,
                kind=kind,
                root=root,
                ref=ref,
                path=path,
                truncated=truncated,
                warnings=warnings,
            )
            files = []
            for item in self.history_snapshots.root.glob("*.json"):
                try:
                    stat = item.stat()
                except FileNotFoundError:
                    continue
                files.append((item, stat.st_mtime_ns, stat.st_size))
            files.sort(key=lambda item: (item[1], item[0].name))
            current = next((item for item in files if item[0].stem == ident), None)
            if current is None or current[2] > _MAX_HISTORY_SNAPSHOT_BYTES:
                (self.history_snapshots.root / f"{ident}.json").unlink(missing_ok=True)
                raise RuntimeError("Git history snapshot exceeds its storage limit")
            total = sum(item[2] for item in files)
            while len(files) > _MAX_HISTORY_SNAPSHOTS or total > _MAX_HISTORY_SNAPSHOT_BYTES:
                victim_index = next(
                    (index for index, item in enumerate(files) if item[0].stem != ident), None
                )
                if victim_index is None:
                    (self.history_snapshots.root / f"{ident}.json").unlink(missing_ok=True)
                    raise RuntimeError("Git history snapshot exceeds its storage limit")
                victim, _, size = files.pop(victim_index)
                try:
                    victim.unlink()
                except FileNotFoundError:
                    pass
                total -= size
            return ident, {
                "id": ident,
                "query": query,
                "items": items,
                "kind": kind,
                "root": root,
                "ref": ref,
                "path": path,
                "truncated": truncated,
                "warnings": warnings,
            }

    def _history_page(
        self,
        snapshot: dict[str, Any],
        offset: int,
        max_entries: int,
        max_bytes: int,
    ) -> dict[str, Any]:
        items, index = self._items_page(snapshot["items"], offset, max_entries, max_bytes)
        more = index < len(snapshot["items"])
        kind = snapshot["kind"]
        cursor = self.history_snapshots.cursor(snapshot["id"], index, kind) if more else None
        result: dict[str, Any] = {
            "root": snapshot.get("root", str(self.workspace)),
            "workspace": str(self.workspace),
            "ref": snapshot.get("ref"),
            "query": snapshot.get("query", {}),
            "commits" if kind == "log" else "lines": items,
            "cursor": cursor,
            "next_cursor": cursor,
            "has_more": more,
            "truncated": bool(snapshot.get("truncated")) or more,
            "scan_truncated": bool(snapshot.get("truncated")),
            "warnings": list(snapshot.get("warnings", [])),
        }
        if kind == "blame":
            result["path"] = snapshot.get("path")
        return result

    @staticmethod
    def _parse_log(text: str, *, truncated: bool) -> list[dict[str, Any]]:
        fields = text.split("\0")
        if fields and fields[-1] == "":
            fields.pop()
        count, remainder = divmod(len(fields), 5)
        if remainder and not truncated:
            raise RuntimeError("git returned malformed log output")
        commits = []
        for index in range(count):
            oid, author, email, authored_at, subject = fields[index * 5 : index * 5 + 5]
            if not oid or any(char not in "0123456789abcdefABCDEF" for char in oid):
                if truncated and index == count - 1:
                    break
                raise RuntimeError("git returned malformed log output")
            commits.append(
                {
                    "commit": oid.lower(),
                    "author": author,
                    "email": email.removeprefix("<").removesuffix(">"),
                    "authored_at": authored_at,
                    "subject": subject,
                }
            )
        return commits

    @staticmethod
    def _parse_blame(text: str, *, truncated: bool) -> list[dict[str, Any]]:
        if truncated and text and not text.endswith("\n"):
            text = text.rpartition("\n")[0]
        records = text.split("\n")
        lines: list[dict[str, Any]] = []
        index = 0
        while index < len(records):
            if index == len(records) - 1 and not records[index]:
                break
            header = records[index].split()
            if len(header) not in {3, 4} or any(not value.isdigit() for value in header[1:]):
                if truncated:
                    break
                raise RuntimeError("git returned malformed blame output")
            commit, original_line, final_line = header[:3]
            object_id = commit.removeprefix("^")
            if not object_id or any(char not in "0123456789abcdefABCDEF" for char in object_id):
                if truncated:
                    break
                raise RuntimeError("git returned malformed blame output")
            index += 1
            metadata: dict[str, str] = {}
            content: str | None = None
            while index < len(records):
                record = records[index]
                index += 1
                if record.startswith("\t"):
                    content = record[1:]
                    break
                key, separator, value = record.partition(" ")
                if separator:
                    metadata[key] = value
            if content is None:
                if truncated:
                    break
                raise RuntimeError("git returned incomplete blame output")
            email = metadata.get("author-mail", "")
            lines.append(
                {
                    "line": int(final_line),
                    "original_line": int(original_line),
                    "commit": object_id.lower(),
                    "author": metadata.get("author", ""),
                    "email": email.removeprefix("<").removesuffix(">"),
                    "author_time": int(metadata["author-time"])
                    if metadata.get("author-time", "").lstrip("-").isdigit()
                    else None,
                    "summary": metadata.get("summary", ""),
                    "text": content,
                }
            )
        return lines

    @staticmethod
    def _validate_ref(ref: str) -> None:
        if not isinstance(ref, str) or not ref or ref.startswith("-") or "\0" in ref:
            raise ValueError("ref must be a non-empty Git ref")

    @staticmethod
    def _validate_filter(name: str, value: str | None) -> None:
        if value is not None and (not isinstance(value, str) or not value or "\0" in value):
            raise ValueError(f"{name} must be a non-empty string or None")

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
