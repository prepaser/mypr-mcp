"""Bounded workspace text, document, and structural searches."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import math
import os
import shutil
import tempfile
import time
from bisect import bisect_left
from pathlib import Path
from typing import Any

from .managed_commands import _uncancelled
from .search_results import Results
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
        self._slots = asyncio.Semaphore(2)
        self.snapshots = SnapshotStore(self.workspace / ".mypr", name="searches")

    async def search(
        self,
        pattern=None,
        *,
        backend="rg",
        mode=None,
        paths=None,
        glob=None,
        fixed=False,
        ignore_case=False,
        hidden=False,
        no_ignore=False,
        context=0,
        word=False,
        line=False,
        multiline=False,
        dotall=False,
        before=None,
        after=None,
        regex_engine="default",
        timeout=30,  # noqa: ASYNC109
        scan_bytes=_QUERY_BYTES,
        scan_limit=None,
        max_matches=_DEFAULT_MAX_MATCHES,
        max_bytes=_DEFAULT_MAX_BYTES,
        cursor=None,
        adapters=None,
        accurate=False,
        cache=True,
        archive_depth=5,
        lang=None,
        rule=None,
        constraints=None,
        utils=None,
        strictness="smart",
    ):  # noqa: ASYNC109
        if backend == "info":
            from .search_backends import inspect_backends

            slots = getattr(getattr(self.shell, "runtime", None), "search_slots", self._slots)
            async with asyncio.timeout(30):
                async with slots:
                    return await inspect_backends(self.workspace, self.shell)
        if backend not in {"rg", "rga", "ast"}:
            raise ValueError("unknown search backend")
        self._validate_options(context, max_matches, max_bytes)
        if cursor is not None:
            changed = (
                pattern is not None
                or rule is not None
                or paths is not None
                or glob is not None
                or any(
                    (
                        fixed,
                        ignore_case,
                        hidden,
                        no_ignore,
                        context,
                        word,
                        line,
                        multiline,
                        dotall,
                        accurate,
                    )
                )
                or before is not None
                or after is not None
                or regex_engine != "default"
                or timeout != 30
                or scan_bytes != _QUERY_BYTES
                or scan_limit is not None
                or adapters is not None
                or cache is not True
                or archive_depth != 5
                or lang is not None
                or constraints is not None
                or utils is not None
                or strictness != "smart"
            )
            if changed:
                raise ValueError("cursor accepts only page budgets and its existing mode")
            snapshot, offset = await asyncio.to_thread(self.snapshots.decode, cursor)
            if snapshot.get("backend", "rg") != backend:
                raise ValueError("cursor belongs to a different search backend")
            if mode is not None and mode != snapshot.get("kind", "matches"):
                raise ValueError("cursor cannot change the search mode")
            return await asyncio.to_thread(self._page, snapshot, offset, max_bytes, max_matches)
        if mode is None:
            mode = "files" if pattern is None and backend == "rg" else "matches"
        if mode not in {"matches", "files", "counts", "exists"}:
            raise ValueError("mode must be matches, files, counts, or exists")
        if pattern is None and backend != "ast" and not (backend == "rg" and mode == "files"):
            raise ValueError("this search requires a pattern")
        if pattern is not None:
            patterns = self._many(pattern)
            if not patterns or any("\0" in value for value in patterns):
                raise ValueError("pattern must be a string or nonempty list without NUL")
        for key, value in {
            "fixed": fixed,
            "ignore_case": ignore_case,
            "hidden": hidden,
            "no_ignore": no_ignore,
            "word": word,
            "line": line,
            "multiline": multiline,
            "dotall": dotall,
            "accurate": accurate,
            "cache": cache,
        }.items():
            if type(value) is not bool:
                raise TypeError(f"{key} must be a boolean")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        if type(scan_bytes) is not int or not 1 <= scan_bytes <= _QUERY_BYTES:
            raise ValueError(f"scan_bytes must be between 1 and {_QUERY_BYTES}")
        if scan_limit is not None and (type(scan_limit) is not int or scan_limit < 1):
            raise ValueError("scan_limit must be a positive integer")
        for value in (before, after):
            if value is not None and (type(value) is not int or not 0 <= value <= _MAX_CONTEXT):
                raise ValueError("before and after must be between 0 and 100")
        if context and (before is not None or after is not None):
            raise ValueError("context cannot be combined with before or after")
        if word and line:
            raise ValueError("word and line cannot both be enabled")
        if dotall and not multiline:
            raise ValueError("dotall requires multiline=True")
        if regex_engine not in {"default", "pcre2"}:
            raise ValueError("regex_engine must be default or pcre2")
        if backend == "ast" and any(
            (
                fixed,
                ignore_case,
                word,
                line,
                multiline,
                dotall,
                context,
                before is not None,
                after is not None,
                regex_engine != "default",
            )
        ):
            raise ValueError("text matching and context options cannot be used for AST searches")
        if backend != "ast" and strictness != "smart":
            raise ValueError("strictness requires search_ast")
        if backend != "ast" and any(
            value is not None for value in (lang, rule, constraints, utils)
        ):
            raise ValueError("AST options require search_ast")
        if backend != "rga" and (
            adapters is not None or accurate or not cache or archive_depth != 5
        ):
            raise ValueError("document options require search_docs")
        paths = self._paths(paths)
        globs = self._many(glob)
        if any(not value or "\0" in value for value in [*paths, *globs]):
            raise ValueError("paths and globs must be nonempty strings without NUL")
        generated = self.workspace / ".mypr" / "searches"
        explicit = any(
            (self.workspace / value).absolute().is_relative_to(generated) for value in paths
        )
        if not explicit:
            globs = [*globs, "!.mypr/searches/**", "!**/.mypr/searches/**"]
        options = dict(
            pattern=pattern,
            mode=mode,
            paths=paths,
            glob=globs,
            fixed=fixed,
            ignore_case=ignore_case,
            hidden=hidden,
            no_ignore=no_ignore,
            context=context,
            word=word,
            line=line,
            multiline=multiline,
            dotall=dotall,
            before=before,
            after=after,
            regex_engine=regex_engine,
            adapters=adapters,
            accurate=accurate,
            cache=cache,
            archive_depth=archive_depth,
            lang=lang,
            rule=rule,
            constraints=constraints,
            utils=utils,
            strictness=strictness,
            scan_limit=scan_limit,
        )
        collector = Results(backend, mode, parse_match=self._match_item, limit=scan_limit)
        deadline = time.monotonic() + timeout
        runtime = getattr(self.shell, "runtime", None)
        if runtime is not None:
            slots = runtime.search_slots
        else:
            slots = self._slots
        run = {}
        acquired = False
        temporary_cache = None
        try:
            async with asyncio.timeout(timeout):
                await slots.acquire()
                acquired = True
                prepare = asyncio.create_task(asyncio.to_thread(self._prepare, backend, options))
                try:
                    command, temporary_cache = await asyncio.shield(prepare)
                except asyncio.CancelledError:
                    command, temporary_cache = await _uncancelled(prepare, propagate=False)
                    raise
            remaining = max(0.001, deadline - time.monotonic())

            async def consume(chunk):
                parse = asyncio.create_task(asyncio.to_thread(collector.feed, chunk))
                try:
                    return await asyncio.shield(parse)
                except asyncio.CancelledError:
                    await _uncancelled(parse, propagate=False)
                    raise

            if hasattr(self.shell, "stream"):
                run = await self.shell.stream(
                    command,
                    cwd=self.workspace,
                    timeout=remaining,
                    max_bytes=scan_bytes,
                    on_stdout=consume,
                )
            else:
                run = await self.shell.run(
                    command,
                    cwd=self.workspace,
                    check=False,
                    timeout=remaining,
                    max_bytes=scan_bytes,
                )
                await consume(str(run.get("stdout", "")))
        except TimeoutError:
            run = {**run, "timed_out": True, "stop_reason": "timeout"}
        finally:
            try:
                if temporary_cache is not None:
                    cleanup = asyncio.create_task(
                        asyncio.to_thread(shutil.rmtree, temporary_cache, True)
                    )
                    await _uncancelled(cleanup)
            finally:
                if acquired:
                    slots.release()
        items = await asyncio.to_thread(collector.finish)
        reason = collector.stopped or run.get("stop_reason")
        if run.get("timed_out"):
            reason = "timeout"
        elif not reason and (run.get("truncated") or collector.invalid):
            reason = "scan_bytes" if run.get("truncated") else "invalid_output"
        rc = run.get("returncode")
        stderr = str(run.get("stderr", "")).strip()
        warnings = list(run.get("warnings", []))
        if stderr:
            warnings.append({"code": "backend_diagnostic", "message": stderr[:4096]})
        if not reason and (run.get("error") or rc not in (0, 1)):
            query_error = any(
                token in stderr.lower()
                for token in (
                    "regex parse error",
                    "pcre2:",
                    "pcre2 is not available",
                    "unrecognized flag",
                    "unexpected argument",
                    "requires pcre2",
                    "not compiled with pcre2",
                )
            )
            if run.get("error") or (not items and (backend != "rga" or query_error)):
                label = "ripgrep" if backend == "rg" else backend
                raise RuntimeError(f"{label} failed: {stderr or run.get('error') or rc}")
            reason = "backend_error"
        complete = reason in (None, "matched")
        if stderr and not reason:
            complete = False
            reason = "backend_warning"
        found = None
        if mode == "exists":
            positive = collector.stopped == "matched" or (backend != "ast" and rc == 0)
            found = True if positive else False if complete else None
            if positive:
                complete, reason = True, "matched"
        ident = await asyncio.to_thread(
            self.snapshots.create,
            options,
            items,
            kind=mode,
            backend=backend,
            version=2,
            complete=complete,
            stop_reason=reason,
            truncated=not complete,
            exists=found,
            run_id=run.get("id"),
            warnings=warnings[:8],
        )
        snapshot = await asyncio.to_thread(self.snapshots.load, ident)
        return await asyncio.to_thread(self._page, snapshot, 0, max_bytes, max_matches)

    def _prepare(self, backend, options):
        temporary = None
        if backend == "rga" and not options["cache"]:
            temporary = tempfile.mkdtemp(prefix=".rga-", dir=self.snapshots.root)
            options = {**options, "_cache_path": temporary}
        try:
            return self._build(backend, options), temporary
        except BaseException:
            if temporary:
                shutil.rmtree(temporary, ignore_errors=True)
            raise

    def _build(self, backend, options):
        if backend != "rg":
            from .search_backends import build

            if backend == "rga" and shutil.which("rga") is None:
                raise RuntimeError(
                    "ripgrep-all (rga) is required for ws.fs.search_docs; install ripgrep-all"
                )
            if backend == "ast":
                executable = shutil.which("ast-grep") or shutil.which("sg")
                if executable is None:
                    raise RuntimeError(
                        "ast-grep is required for ws.fs.search_ast; install ast-grep"
                    )
                options = {**options, "executable": executable}
            return build(self.workspace, backend, options)
        if shutil.which("rg") is None:
            raise RuntimeError("ripgrep (rg) is required for ws.fs.search; install ripgrep")
        pattern, mode = options["pattern"], options["mode"]
        command = ["rg", "--no-config", "--threads", "2"]
        if pattern is None:
            command += ["--files", "--null"]
        else:
            output_mode = "matches" if mode == "counts" and options.get("scan_limit") else mode
            command += {
                "matches": ["--json"],
                "files": ["--files-with-matches", "--null"],
                "counts": ["--count-matches", "--with-filename", "--null"],
                "exists": ["--quiet"],
            }[output_mode]
            for name, flag in {
                "fixed": "--fixed-strings",
                "word": "--word-regexp",
                "line": "--line-regexp",
                "multiline": "--multiline",
                "dotall": "--multiline-dotall",
            }.items():
                if options[name]:
                    command.append(flag)
            if options["regex_engine"] == "pcre2":
                command.append("--pcre2")
            for name, flag in (
                ("context", "--context"),
                ("before", "--before-context"),
                ("after", "--after-context"),
            ):
                if options[name] is not None and options[name]:
                    command += [flag, str(options[name])]
            for value in self._many(pattern):
                command += ["--regexp", value]
        for name, flag in (
            ("ignore_case", "--ignore-case"),
            ("hidden", "--hidden"),
            ("no_ignore", "--no-ignore"),
        ):
            if options[name]:
                command.append(flag)
        for value in options["glob"]:
            command += ["--glob", value]
        return [*command, "--", *options["paths"]]

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
            if (is_match or kind != "matches") and match_count >= max_matches:
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
            if is_match or kind != "matches":
                match_count += 1
        more = index < len(items)
        next_cursor = self.snapshots.cursor(snapshot["id"], index, kind) if more else None
        result: dict[str, Any] = {
            "matches": page if kind == "matches" else [],
            "files": page if kind == "files" else [],
            "counts": page if kind == "counts" else [],
            "exists": snapshot.get("exists"),
            "backend": snapshot.get("backend", "rg"),
            "mode": kind,
            "complete": snapshot.get("complete", not snapshot.get("truncated", False)),
            "stop_reason": snapshot.get("stop_reason"),
            "page_cursor": self.snapshots.cursor(snapshot["id"], offset, kind),
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

    @staticmethod
    def _paths(paths: str | list[str] | None) -> list[str]:
        values = Search._many(paths)
        if not values:
            return ["."]
        for value in values:
            if not value:
                raise ValueError("paths must contain non-empty strings")
        return list(dict.fromkeys(values))

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
        ranges = []
        raw = line.encode("utf-8", "surrogateescape")
        base_line = data.get("line_number")
        absolute = data.get("absolute_offset")
        newlines = [index for index, value in enumerate(raw) if value == 10]

        def point(offset):
            row = bisect_left(newlines, offset)
            previous = newlines[row - 1] if row else -1
            return {
                "line": base_line + row if type(base_line) is int else None,
                "column": offset - previous,
                "byte": absolute + offset if type(absolute) is int else None,
            }

        submatches = data.get("submatches")
        if item["type"] == "match" and isinstance(submatches, list):
            for match in submatches:
                if not isinstance(match, dict) or type(match.get("start")) is not int:
                    continue
                start = match["start"]
                end = match.get("end", start)
                if type(end) is not int or not 0 <= start <= end <= len(raw):
                    continue
                if column is None:
                    column = start + 1
                ranges.append(
                    {
                        "text": cls._decode(match.get("match")),
                        "start": start,
                        "end": end,
                        "range": {"start": point(start), "end": point(end)},
                    }
                )
        return {
            "kind": item["type"],
            "path": path,
            "line": base_line,
            "column": column,
            "text": line.rstrip("\r\n"),
            "submatches": ranges,
            "range": {"start": ranges[0]["range"]["start"], "end": ranges[-1]["range"]["end"]}
            if ranges
            else None,
        }

    @staticmethod
    def _bounded_item(item: dict[str, Any], budget: int) -> tuple[dict[str, Any] | None, int, bool]:
        cost = Search._json_cost(item)
        if cost <= budget:
            return item, cost, False
        shortened = dict(item)
        shortened["text_truncated"] = True
        if Search._json_cost({**shortened, "text": ""}) > budget:
            shortened = {
                key: item[key] for key in ("kind", "path", "line", "column") if key in item
            }
            if item.get("coordinate_space") == "extracted":
                shortened["coordinate_space"] = "extracted"
            shortened.update(text_truncated=True, details_truncated=True)
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
