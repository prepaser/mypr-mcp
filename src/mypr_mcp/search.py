"""Bounded, asynchronous workspace search backed by ripgrep."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
import shutil
from pathlib import Path
from typing import Any

from .snapshots import SnapshotStore

_DEFAULT_MAX_MATCHES = 100
_DEFAULT_MAX_BYTES = 32 * 1024
_MAX_CONTEXT = 100
_MAX_MATCHES = 10_000
_MAX_BYTES = 16 * 1024 * 1024
_QUERY_BYTES = 16 * 1024 * 1024


class Search:
    """Search files below a workspace through the managed shell API."""

    def __init__(self, workspace: Path, shell: Any):
        self.workspace = Path(workspace).expanduser().resolve()
        self.shell = shell
        self.snapshots = SnapshotStore(self.workspace / ".mypr", name="searches")

    async def search(
        self,
        pattern: str | None = None,
        *,
        paths: str | list[str] | None = None,
        glob: str | list[str] | None = None,
        fixed: bool = False,
        ignore_case: bool = False,
        hidden: bool = False,
        no_ignore: bool = False,
        context: int = 0,
        max_matches: int = _DEFAULT_MAX_MATCHES,
        max_bytes: int = _DEFAULT_MAX_BYTES,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Run a bounded regex or fixed-string search.

        With ``pattern=None``, this lists files. Paths are passed as argv
        elements and may be absolute or relative to the workspace.
        """

        if cursor is not None:
            snapshot, offset = await asyncio.to_thread(self.snapshots.decode, cursor)
            self._validate_options(context, max_matches, max_bytes)
            return await asyncio.to_thread(self._page, snapshot, offset, max_bytes, max_matches)
        if pattern is not None and not isinstance(pattern, str):
            raise TypeError("pattern must be a string or None")
        self._validate_options(context, max_matches, max_bytes)
        if shutil.which("rg") is None:
            raise RuntimeError("ripgrep (rg) is required for ws.fs.search; install ripgrep")
        command = self._command(
            pattern,
            paths=paths,
            glob=glob,
            fixed=fixed,
            ignore_case=ignore_case,
            hidden=hidden,
            no_ignore=no_ignore,
            context=context,
        )
        scan_bytes = _QUERY_BYTES
        run = await self.shell.run(
            command,
            cwd=self.workspace,
            check=False,
            max_bytes=scan_bytes,
        )
        returncode = run.get("returncode")
        stderr = str(run.get("stderr", ""))
        if run.get("error"):
            raise RuntimeError(f"ripgrep failed: {run['error']}")
        if returncode not in (0, 1):
            detail = stderr.strip() or f"exit status {returncode}"
            raise RuntimeError(f"ripgrep failed: {detail}")
        stdout = str(run.get("stdout", ""))
        truncated = bool(run.get("truncated") or run.get("timed_out"))
        if pattern is None:
            parse = self._parse_files
            kind = "files"
        else:
            parse = self._parse_matches
            kind = "matches"
        query = {
            "pattern": pattern,
            "paths": paths,
            "glob": glob,
            "fixed": fixed,
            "ignore_case": ignore_case,
            "hidden": hidden,
            "no_ignore": no_ignore,
            "context": context,
        }
        ident, page = await asyncio.to_thread(
            self._persist_page,
            parse,
            stdout,
            query,
            kind,
            truncated,
            run.get("id"),
            max_bytes,
            max_matches,
            list(run.get("warnings", [])),
        )
        page.update(
            id=run.get("id"),
            snapshot_id=ident,
            warnings=list(run.get("warnings", [])),
        )
        return page

    def _persist_page(
        self,
        parser: Any,
        stdout: str,
        query: dict[str, Any],
        kind: str,
        truncated: bool,
        run_id: str | None,
        max_bytes: int,
        max_matches: int,
        warnings: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        items, parsed_truncated = parser(stdout, None, _QUERY_BYTES)
        ident = self.snapshots.create(
            query,
            items,
            kind=kind,
            truncated=truncated or parsed_truncated,
            run_id=run_id,
            warnings=warnings,
        )
        snapshot = self.snapshots.load(ident)
        return ident, self._page(snapshot, 0, max_bytes, max_matches)

    def _page(
        self, snapshot: dict[str, Any], offset: int, max_bytes: int, max_matches: int
    ) -> dict[str, Any]:
        items = snapshot["items"]
        kind = snapshot.get("kind", "matches")
        page: list[Any] = []
        used = 0
        index = offset
        match_count = 0
        clipped_any = False
        while index < len(items):
            item = items[index]
            clipped = False
            is_match = kind == "matches" and item.get("kind") == "match"
            if is_match and match_count >= max_matches:
                break
            if kind == "matches":
                full_cost = self._json_cost(item)
                if page and used + full_cost > max_bytes:
                    break
                if not page and full_cost > max_bytes:
                    item, cost, clipped = self._bounded_item(item, max_bytes)
                    if item is None:
                        item = dict(items[index])
                        item["text"] = ""
                        item["text_truncated"] = True
                        cost = self._json_cost(item)
                        clipped = True
                else:
                    cost = full_cost
                clipped_any |= clipped
            else:
                cost = self._json_cost(item)
            if not page and cost > max_bytes:
                raise ValueError("max_bytes is too small for search metadata; increase the budget")
            if page and used + cost > max_bytes:
                break
            page.append(item)
            used += cost
            index += 1
            if is_match:
                match_count += 1
        more = index < len(items)
        next_cursor = self.snapshots.cursor(snapshot["id"], index, kind) if more else None
        result: dict[str, Any] = {
            "matches": page if kind == "matches" else [],
            "files": page if kind == "files" else [],
            "cursor": next_cursor,
            "next_cursor": next_cursor,
            "has_more": more,
            "truncated": bool(snapshot.get("truncated")) or more,
            "scan_truncated": bool(snapshot.get("truncated")),
            "id": snapshot.get("run_id"),
            "snapshot_id": snapshot["id"],
            "warnings": list(snapshot.get("warnings", [])),
        }
        if clipped_any:
            result["truncated"] = True
        return result

    def _command(
        self,
        pattern: str | None,
        *,
        paths: str | list[str] | None,
        glob: str | list[str] | None,
        fixed: bool,
        ignore_case: bool,
        hidden: bool,
        no_ignore: bool,
        context: int,
    ) -> list[str]:
        command = ["rg", "--no-config"]
        if pattern is None:
            command.extend(["--files", "--null"])
        else:
            command.append("--json")
            if fixed:
                command.append("--fixed-strings")
            if context:
                command.extend(["--context", str(context)])
        if ignore_case:
            command.append("--ignore-case")
        if hidden:
            command.append("--hidden")
        if no_ignore:
            command.append("--no-ignore")
        for item in self._many(glob):
            if not item:
                raise ValueError("glob must not be empty")
            command.extend(["--glob", item])
        command.append("--")
        if pattern is not None:
            command.append(pattern)
        command.extend(self._paths(paths))
        return command

    @staticmethod
    def _paths(paths: str | list[str] | None) -> list[str]:
        values = Search._many(paths)
        if not values:
            return ["."]
        for value in values:
            if not value:
                raise ValueError("paths must contain non-empty strings")
        return values

    @staticmethod
    def _many(value: str | list[str] | None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
        raise ValueError("value must be a string or list of strings")

    @staticmethod
    def _validate_options(context: int, max_matches: int, max_bytes: int) -> None:
        if type(context) is not int or not 0 <= context <= _MAX_CONTEXT:
            raise ValueError(f"context must be between 0 and {_MAX_CONTEXT}")
        if type(max_matches) is not int or not 1 <= max_matches <= _MAX_MATCHES:
            raise ValueError(f"max_matches must be between 1 and {_MAX_MATCHES}")
        if type(max_bytes) is not int or not 1 <= max_bytes <= _MAX_BYTES:
            raise ValueError(f"max_bytes must be between 1 and {_MAX_BYTES}")

    @classmethod
    def _parse_files(
        cls, text: str, max_files: int | None, max_bytes: int
    ) -> tuple[list[str], bool]:
        files: list[str] = []
        used = 0
        truncated = not text.endswith("\0") and bool(text)
        paths = text.split("\0")
        if truncated:
            paths.pop()
        for path in paths:
            if not path:
                continue
            if max_files is not None and len(files) >= max_files:
                return files, True
            cost = cls._json_cost(path)
            if used + cost > max_bytes:
                return files, True
            files.append(path)
            used += cost
        return files, truncated

    @classmethod
    def _parse_matches(
        cls, text: str, max_matches: int | None, max_bytes: int
    ) -> tuple[list[dict[str, Any]], bool]:
        matches: list[dict[str, Any]] = []
        used = 0
        truncated = not text.endswith("\n") and bool(text)
        for line in text.split("\n"):
            try:
                item = json.loads(line)
            except ValueError, UnicodeError:
                continue
            if not isinstance(item, dict) or item.get("type") not in {"match", "context"}:
                continue
            candidate = cls._match_item(item)
            if candidate is None:
                continue
            if (
                candidate["kind"] == "match"
                and max_matches is not None
                and sum(result["kind"] == "match" for result in matches) >= max_matches
            ):
                return matches, True
            added, cost, item_truncated = cls._bounded_item(candidate, max_bytes - used)
            if added is None:
                return matches, True
            matches.append(added)
            used += cost
            truncated |= item_truncated
        return matches, truncated

    @classmethod
    def _match_item(cls, item: dict[str, Any]) -> dict[str, Any] | None:
        data = item.get("data")
        if not isinstance(data, dict):
            return None
        path = cls._decode(data.get("path"))
        line = cls._decode(data.get("lines"))
        if path is None or line is None:
            return None
        column = None
        submatches = data.get("submatches")
        if item["type"] == "match" and isinstance(submatches, list) and submatches:
            first = submatches[0]
            if isinstance(first, dict) and type(first.get("start")) is int:
                column = first["start"] + 1
        return {
            "kind": item["type"],
            "path": path,
            "line": data.get("line_number"),
            "column": column,
            "text": line.rstrip("\r\n"),
        }

    @staticmethod
    def _bounded_item(item: dict[str, Any], budget: int) -> tuple[dict[str, Any] | None, int, bool]:
        cost = Search._json_cost(item)
        if cost <= budget:
            return item, cost, False
        shortened = dict(item)
        shortened["text_truncated"] = True
        original = str(item.get("text", ""))
        left, right = 0, len(original)
        best: dict[str, Any] | None = None
        best_cost = 0
        while left <= right:
            middle = (left + right) // 2
            candidate = dict(shortened)
            candidate["text"] = original[:middle]
            candidate_cost = Search._json_cost(candidate)
            if candidate_cost <= budget:
                best, best_cost = candidate, candidate_cost
                left = middle + 1
            else:
                right = middle - 1
        if best is None:
            return None, 0, True
        return best, best_cost, True

    @staticmethod
    def _json_cost(value: Any) -> int:
        return len(json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode())

    @staticmethod
    def _decode(value: Any) -> str | None:
        if not isinstance(value, dict):
            return None
        text = value.get("text")
        if isinstance(text, str):
            return text
        encoded = value.get("bytes")
        if not isinstance(encoded, str):
            return None
        try:
            return os.fsdecode(base64.b64decode(encoded, validate=True))
        except ValueError, binascii.Error:
            return None


async def search(workspace: Path, shell: Any, pattern: str | None = None, **kwargs: Any):
    """Convenience wrapper around :class:`Search`."""

    return await Search(workspace, shell).search(pattern, **kwargs)
