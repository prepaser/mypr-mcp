"""Async, revision-aware filesystem helpers for the workspace API."""

from __future__ import annotations

import asyncio
import base64
import codecs
import difflib
import errno
import hashlib
import heapq
import inspect
import os
import stat
import tempfile
from collections.abc import Callable, Mapping
from contextlib import AsyncExitStack
from pathlib import Path
from threading import Lock
from typing import Any
from weakref import WeakValueDictionary

from .async_utils import wait_owned
from .change_plans import MAX_FILES, MAX_INPUT_OUTPUT_BYTES
from .revisions import RevisionIndexOutcomeUnknown

_PATH_LOCKS: WeakValueDictionary[Path, asyncio.Lock] = WeakValueDictionary()
_PATH_LOCKS_GUARD = Lock()


class _LspPathPins:
    def __init__(self, workspace: Path, paths: set[Path]) -> None:
        self.workspace = workspace
        self.paths = paths
        self._fds: dict[tuple[str, ...], int] = {}
        self._target_fds: dict[Path, int] = {}
        self._absent_targets: set[Path] = set()
        self._created: list[tuple[int, str]] = []
        self._committed = False

    def __enter__(self) -> _LspPathPins:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        if (
            not getattr(os, "O_DIRECTORY", 0)
            or not getattr(os, "O_NOFOLLOW", 0)
            or os.open not in os.supports_dir_fd
        ):
            raise RuntimeError("LSP edit path ownership cannot be pinned on this platform")
        root_fd = os.open(self.workspace, flags)
        self._fds[()] = root_fd
        try:
            for path in sorted(self.paths, key=str):
                self._pin_parent(path, flags)
            for path in self.paths:
                self._pin_target(path)
            self.assert_current()
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        if not self._committed:
            for parent_fd, name in reversed(self._created):
                try:
                    os.rmdir(name, dir_fd=parent_fd)
                except OSError:
                    pass
        for fd in reversed(tuple(self._fds.values())):
            try:
                os.close(fd)
            except OSError:
                pass
        for fd in self._target_fds.values():
            try:
                os.close(fd)
            except OSError:
                pass
        self._fds.clear()
        self._target_fds.clear()
        self._absent_targets.clear()

    def committed(self) -> None:
        self._committed = True

    def path(self, path: Path) -> Path:
        relative = path.relative_to(self.workspace)
        parent_fd = self._fds[relative.parts[:-1]]
        return Path(f"/proc/self/fd/{parent_fd}") / relative.parts[-1]

    def state_path(self, path: Path) -> Path:
        target_fd = self._target_fds.get(path)
        if target_fd is not None:
            return Path(f"/proc/self/fd/{target_fd}")
        return self.path(path)

    def assert_current(self) -> None:
        for relative, fd in self._fds.items():
            candidate = self.workspace.joinpath(*relative)
            try:
                current = os.stat(candidate, follow_symlinks=False)
                pinned = os.fstat(fd)
            except FileNotFoundError as exc:
                raise RuntimeError("LSP edit parent directory changed while applying") from exc
            if not stat.S_ISDIR(current.st_mode) or _inode(current) != _inode(pinned):
                raise RuntimeError("LSP edit parent directory changed while applying")
        for path, fd in self._target_fds.items():
            try:
                current = os.stat(path, follow_symlinks=False)
                pinned = os.fstat(fd)
            except FileNotFoundError as exc:
                raise RuntimeError("LSP edit target changed while applying patch") from exc
            if _inode(current) != _inode(pinned):
                raise RuntimeError("LSP edit target changed while applying patch")
        for path in self._absent_targets:
            try:
                os.stat(path, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise RuntimeError("LSP edit target changed while applying patch")

    def _pin_parent(self, path: Path, flags: int) -> None:
        relative = path.relative_to(self.workspace)
        prefix: tuple[str, ...] = ()
        for name in relative.parts[:-1]:
            next_prefix = (*prefix, name)
            if next_prefix in self._fds:
                prefix = next_prefix
                continue
            parent_fd = self._fds[prefix]
            try:
                fd = os.open(name, flags, dir_fd=parent_fd)
            except FileNotFoundError:
                os.mkdir(name, dir_fd=parent_fd)
                self._created.append((parent_fd, name))
                fd = os.open(name, flags, dir_fd=parent_fd)
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    raise ValueError("LSP edit path must not contain symlinks") from exc
                raise
            self._fds[next_prefix] = fd
            prefix = next_prefix

    def _pin_target(self, path: Path) -> None:
        relative = path.relative_to(self.workspace)
        parent_fd = self._fds[relative.parts[:-1]]
        flags = (
            os.O_RDONLY
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            fd = os.open(relative.parts[-1], flags, dir_fd=parent_fd)
        except FileNotFoundError:
            self._absent_targets.add(path)
            return
        except OSError as exc:
            if exc.errno == getattr(errno, "ELOOP", 40):
                raise ValueError(f"LSP edit path must not be a symlink: {path}") from exc
            raise
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            return
        self._target_fds[path] = fd


class _ReadLimitExceeded(ValueError):
    def __init__(self, path: Path, limit: int) -> None:
        super().__init__(f"file exceeds {limit} bytes: {path}")
        self.path = path
        self.limit = limit


class Filesystem:
    """Filesystem operations rooted at a workspace.

    Relative paths are resolved below ``workspace``. Absolute paths are
    intentionally accepted because this API runs inside the trusted
    workstation process. Returned revisions are SHA-256 hashes of the exact
    file bytes. Reads never decode a partial UTF-8 code point; when a long
    line is cut, ``next_cursor`` points at its byte offset so the caller can
    continue from the same line.
    """

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        shell: Any = None,
        searcher: Any = None,
        ensure_dependencies: Callable[..., Any] | None = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self._shell = shell
        self._searcher = searcher
        self._ensure_dependencies = ensure_dependencies

    async def _ensure(self, *names: str) -> dict[str, Any]:
        if not names or self._ensure_dependencies is None:
            return {}
        result = self._ensure_dependencies(*names, automatic=True)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, dict) else {}

    def _path(self, path: str | os.PathLike[str]) -> tuple[Path, str]:
        supplied = Path(path).expanduser()
        candidate = supplied if supplied.is_absolute() else self.workspace / supplied
        # Resolving follows a symlink for writes, keeping the link itself in
        # place while making relative symlink paths behave like normal files.
        resolved = candidate.resolve(strict=False)
        display = (
            str(resolved.relative_to(self.workspace))
            if _below(resolved, self.workspace)
            else str(resolved)
        )
        return resolved, display or "."

    @staticmethod
    def _lock(path: Path) -> asyncio.Lock:
        with _PATH_LOCKS_GUARD:
            return _PATH_LOCKS.setdefault(path, asyncio.Lock())

    async def read(
        self,
        path: str | os.PathLike[str],
        *,
        start_line: int = 1,
        end_line: int | None = None,
        start_byte: int | None = None,
        max_bytes: int = 32_768,
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        """Read a bounded, one-based line range with a content revision."""
        _validate_line_range(start_line, end_line, start_byte, max_bytes)
        resolved, display = self._path(path)
        return await _to_thread_uncancelled(
            _read_file,
            resolved,
            display,
            start_line,
            end_line,
            start_byte,
            max_bytes,
            encoding,
        )

    async def write(
        self,
        path: str | os.PathLike[str],
        text: str,
        *,
        expected_hash: str | None = None,
        overwrite: bool = False,
        encoding: str = "utf-8",
        create_parents: bool = False,
        history: bool = True,
    ) -> dict[str, Any]:
        """Atomically create or replace a text file using an optional CAS."""
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        if (
            type(overwrite) is not bool
            or type(create_parents) is not bool
            or type(history) is not bool
        ):
            raise TypeError("overwrite, create_parents, and history must be booleans")
        resolved, display = self._path(path)
        encoded = text.encode(encoding)
        if history:
            _validate_history_target(resolved, display)
        resource = self._history_resource(resolved) if history else None
        history_enabled = resource is not None
        async with AsyncExitStack() as stack:
            if resource is not None:
                await stack.enter_async_context(self._history_store().transaction(resource))
            lock = self._lock(resolved)
            await lock.acquire()
            stack.callback(lock.release)
            result = await _to_thread_uncancelled(
                _write_file,
                resolved,
                display,
                encoded,
                expected_hash,
                overwrite,
                create_parents,
                history_enabled,
            )
            old = result.pop("_old", None)
            if history_enabled:
                await self._record_history(resolved, display, old, encoded)
                result["history_recorded"] = self._history_resource(resolved) is not None
            return result

    async def read_bytes(
        self,
        path: str | os.PathLike[str],
        *,
        start_byte: int = 0,
        max_bytes: int = 32_768,
    ) -> dict[str, Any]:
        """Read a bounded byte page without decoding the source."""
        if type(start_byte) is not int or start_byte < 0:
            raise ValueError("start_byte must be a non-negative integer")
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        resolved, display = self._path(path)
        return await _to_thread_uncancelled(_read_bytes, resolved, display, start_byte, max_bytes)

    async def write_bytes(
        self,
        path: str | os.PathLike[str],
        data: bytes,
        *,
        expected_hash: str | None = None,
        overwrite: bool = False,
        create_parents: bool = False,
        history: bool = True,
    ) -> dict[str, Any]:
        """Atomically write raw bytes using an optional CAS."""
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        if (
            type(overwrite) is not bool
            or type(create_parents) is not bool
            or type(history) is not bool
        ):
            raise TypeError("overwrite, create_parents, and history must be booleans")
        resolved, display = self._path(path)
        if history:
            _validate_history_target(resolved, display)
        resource = self._history_resource(resolved) if history else None
        history_enabled = resource is not None
        async with AsyncExitStack() as stack:
            if resource is not None:
                await stack.enter_async_context(self._history_store().transaction(resource))
            lock = self._lock(resolved)
            await lock.acquire()
            stack.callback(lock.release)
            result = await _to_thread_uncancelled(
                _write_file,
                resolved,
                display,
                data,
                expected_hash,
                overwrite,
                create_parents,
                history_enabled,
            )
            old = result.pop("_old", None)
            if history_enabled:
                await self._record_history(resolved, display, old, data)
                result["history_recorded"] = self._history_resource(resolved) is not None
            return result

    async def apply_patch(
        self,
        patch: str,
        *,
        expected_hashes: Mapping[str, str | None] | None = None,
        dry_run: bool = False,
        max_diff_bytes: int = 32768,
        history: bool = True,
    ) -> dict[str, Any]:
        from .patching import apply_patch

        return await apply_patch(
            self,
            patch,
            expected_hashes=expected_hashes,
            dry_run=dry_run,
            max_diff_bytes=max_diff_bytes,
            history=history,
        )

    async def rewrite_ast(
        self,
        pattern: str | None = None,
        *,
        rule: Mapping[str, Any] | None = None,
        replacement: str,
        lang: str,
        paths: str | list[str] | None = None,
        glob: str | list[str] | None = None,
        history: bool = True,
    ) -> dict[str, Any]:
        from .ast_rewrite import rewrite_ast

        return await rewrite_ast(
            self,
            pattern=pattern,
            rule=rule,
            replacement=replacement,
            lang=lang,
            paths=paths,
            glob=glob,
            history=history,
        )

    async def apply_rewrite(self, plan_id: str) -> dict[str, Any]:
        from .ast_rewrite import apply_rewrite

        return await apply_rewrite(self, plan_id)

    async def replace(
        self,
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
        from .text_replace import replace

        return await replace(
            self,
            pattern,
            replacement,
            paths=paths,
            glob=glob,
            fixed=fixed,
            ignore_case=ignore_case,
            hidden=hidden,
            no_ignore=no_ignore,
            max_files=max_files,
            max_bytes=max_bytes,
            timeout=timeout,
            history=history,
        )

    async def apply_replace(self, plan_id: str) -> dict[str, Any]:
        from .text_replace import apply_replace

        return await apply_replace(self, plan_id)

    async def _apply_lsp_plan(self, plan: Any) -> dict[str, Any]:
        """Apply a validated LSP WorkspaceEdit as one CAS transaction."""
        from .patching import _commit, _plan, _read_state, _State

        operations = getattr(plan, "operations", None)
        if not isinstance(operations, list) or not operations:
            raise ValueError("LSP edit plan has no operations")
        paths: set[Path] = set()
        touched_paths: set[Path] = set()
        resolved_operations = []
        raw_preconditions = getattr(plan, "preconditions", None)
        if raw_preconditions is None:
            preconditions: dict[Path, str | None] = {}
        elif isinstance(raw_preconditions, Mapping):
            preconditions = {}
            for raw_path, expected in raw_preconditions.items():
                if not isinstance(raw_path, Path):
                    raise ValueError("LSP edit precondition contains an invalid path")
                if expected is not None and not isinstance(expected, str):
                    raise ValueError("LSP edit precondition contains an invalid revision")
                path = raw_path.resolve(strict=False)
                if not _below(path, self.workspace) or path == self.workspace:
                    raise ValueError("LSP edit precondition must remain inside the workspace")
                if path in preconditions:
                    raise ValueError(f"duplicate LSP edit precondition: {path}")
                preconditions[path] = expected
                paths.add(path)
        else:
            raise ValueError("LSP edit preconditions must be a mapping")
        for operation in operations:
            path = getattr(operation, "path", None)
            source = getattr(operation, "source", None)
            if not isinstance(path, Path) or (source is not None and not isinstance(source, Path)):
                raise ValueError("LSP edit plan contains an invalid path")
            resolved_path = path.resolve(strict=False)
            if not _below(resolved_path, self.workspace) or resolved_path == self.workspace:
                raise ValueError("LSP edit path must remain inside the workspace")
            resolved_source = None
            if source is not None:
                resolved_source = source.resolve(strict=False)
                if not _below(resolved_source, self.workspace) or resolved_source == self.workspace:
                    raise ValueError("LSP edit source must remain inside the workspace")
                paths.add(resolved_source)
                touched_paths.add(resolved_source)
            paths.add(resolved_path)
            touched_paths.add(resolved_path)
            resolved_operations.append((operation, resolved_path, resolved_source))

        locks = [self._lock(path) for path in sorted(paths, key=str)]
        history_store = self._history_store()
        resources = sorted(
            resource
            for path in paths
            if (resource := self._history_resource(path)) is not None
        )
        async with AsyncExitStack() as stack:
            for resource in resources:
                await stack.enter_async_context(history_store.transaction(resource))
            for lock in locks:
                await lock.acquire()
                stack.callback(lock.release)
            pins = _LspPathPins(self.workspace, paths)
            stack.enter_context(pins)

            initial: dict[Path, _State] = {}
            for path in sorted(paths, key=str):
                state_path = pins.state_path(path)
                initial[path] = await _to_thread_uncancelled(
                    _read_state,
                    state_path,
                    str(path.relative_to(self.workspace)),
                    no_symlink=state_path == pins.path(path),
                )
            pins.assert_current()
            for path, expected in preconditions.items():
                state = initial[path]
                actual = _sha256(state.data) if state.exists else None
                if actual != expected:
                    raise ValueError(
                        f"LSP edit plan precondition is stale: {path.relative_to(self.workspace)}"
                    )
            virtual = {
                path: _State(state.path, state.display, state.exists, state.data, state.info)
                for path, state in initial.items()
            }

            def check_expected(path: Path, expected: str | None, data: bytes | None) -> None:
                if expected is not None and _sha256(data or b"") != expected:
                    raise ValueError(f"LSP edit plan is stale: {path.relative_to(self.workspace)}")

            def check_data(path: Path, expected: bytes | None, actual: _State) -> None:
                if actual.data != expected or actual.exists != (expected is not None):
                    raise ValueError(f"LSP edit plan is stale: {path.relative_to(self.workspace)}")

            def mode(state: _State) -> int:
                return stat.S_IMODE(state.info.st_mode) if state.info is not None else 0o600

            for operation, path, source in resolved_operations:
                kind = operation.operation
                current = virtual[path]
                expected = getattr(operation, "expected", None)
                if kind == "create":
                    check_expected(path, expected, current.data)
                    if current.exists:
                        raise FileExistsError(str(path.relative_to(self.workspace)))
                    new = operation.new
                    if not isinstance(new, bytes):
                        raise ValueError("LSP create operation contains invalid bytes")
                    virtual[path] = _State(path, current.display, True, new, None)
                elif kind == "delete":
                    old = operation.old
                    check_expected(path, expected, current.data)
                    check_data(path, old, current)
                    virtual[path] = _State(path, current.display, False, None, None)
                elif kind == "update":
                    old = operation.old
                    check_expected(path, expected, current.data)
                    check_data(path, old, current)
                    new = operation.new
                    if not isinstance(new, bytes):
                        raise ValueError("LSP update operation contains invalid bytes")
                    virtual[path] = _State(path, current.display, True, new, current.info)
                elif kind == "rename":
                    if source is None:
                        raise ValueError("LSP rename operation has no source")
                    source_state = virtual[source]
                    destination_state = virtual[path]
                    check_expected(source, getattr(operation, "expected", None), source_state.data)
                    check_data(source, getattr(operation, "source_old", None), source_state)
                    if not source_state.exists:
                        raise FileNotFoundError(str(source.relative_to(self.workspace)))
                    if destination_state.exists:
                        raise FileExistsError(str(path.relative_to(self.workspace)))
                    new = operation.new
                    if not isinstance(new, bytes):
                        raise ValueError("LSP rename operation contains invalid bytes")
                    virtual[source] = _State(
                        source,
                        source_state.display,
                        False,
                        None,
                        None,
                    )
                    virtual[path] = _State(
                        path,
                        destination_state.display,
                        True,
                        new,
                        source_state.info,
                    )
                else:
                    raise ValueError(f"unsupported LSP edit operation: {kind}")

            plans: list[dict[str, Any]] = []
            if (
                len(resolved_operations) == 1
                and resolved_operations[0][0].operation == "rename"
            ):
                operation, path, source = resolved_operations[0]
                assert source is not None
                source_state = initial[source]
                destination_state = initial[path]
                plans.append(
                    _plan(
                        "move",
                        path,
                        str(path.relative_to(self.workspace)),
                        None,
                        operation.new,
                        destination_state.info,
                        stat.S_IMODE(source_state.info.st_mode),
                        source=source,
                        source_display=str(source.relative_to(self.workspace)),
                        source_old=source_state.data,
                        source_old_info=source_state.info,
                    )
                )
            else:
                for path in sorted(touched_paths, key=str):
                    old_state = initial[path]
                    new_state = virtual[path]
                    mode_changed = (
                        old_state.exists
                        and new_state.exists
                        and mode(old_state) != mode(new_state)
                    )
                    if (
                        old_state.exists == new_state.exists
                        and old_state.data == new_state.data
                        and not mode_changed
                    ):
                        if (
                            len(resolved_operations) == 1
                            and resolved_operations[0][0].operation == "update"
                            and old_state.exists
                        ):
                            display = str(path.relative_to(self.workspace))
                            plans.append(
                                _plan(
                                    "update",
                                    path,
                                    display,
                                    old_state.data,
                                    new_state.data,
                                    old_state.info,
                                    stat.S_IMODE(old_state.info.st_mode),
                                )
                            )
                        continue
                    display = str(path.relative_to(self.workspace))
                    if not old_state.exists:
                        plans.append(
                            _plan(
                                "add",
                                path,
                                display,
                                None,
                                new_state.data,
                                None,
                                mode(new_state),
                            )
                        )
                    elif not new_state.exists:
                        plans.append(
                            _plan(
                                "delete",
                                path,
                                display,
                                old_state.data,
                                None,
                                old_state.info,
                                None,
                            )
                        )
                    else:
                        plans.append(
                            _plan(
                                "update",
                                path,
                                display,
                                old_state.data,
                                new_state.data,
                                old_state.info,
                                mode(new_state),
                            )
                        )

            pinned_initial = {
                pins.path(path): _State(
                    pins.state_path(path),
                    state.display,
                    state.exists,
                    state.data,
                    state.info,
                )
                for path, state in initial.items()
            }
            pinned_plans = []
            for plan in plans:
                pinned = dict(plan)
                pinned["path"] = pins.path(plan["path"])
                if plan["source"] is not None:
                    pinned["source"] = pins.path(plan["source"])
                pinned_plans.append(pinned)

            await _to_thread_uncancelled(history_store.prepare_changes_sync, pinned_plans)
            pins.assert_current()
            result = await _to_thread_uncancelled(
                _commit,
                pinned_plans,
                pinned_initial,
                32 * 1024,
                history_store=history_store,
                no_symlink=True,
            )
            pins.committed()
            result["history_recorded"] = True
            return result

    async def history(
        self, path: str | os.PathLike[str], *, limit: int = 20, cursor: int | None = None
    ) -> dict[str, Any]:
        resource = self._history_resource_for_input(path)
        if resource is None:
            raise ValueError("file history requires a regular workspace file path")
        return await self._history_store().history(resource, limit=limit, cursor=cursor)

    async def read_revision(
        self,
        path: str | os.PathLike[str],
        revision: str,
        *,
        start_byte: int = 0,
        max_bytes: int = 32_768,
    ) -> dict[str, Any]:
        resource = self._history_resource_for_input(path)
        if resource is None:
            raise ValueError("file history requires a workspace file path")
        return await self._history_store().read_revision(
            resource, revision, start_byte=start_byte, max_bytes=max_bytes
        )

    async def restore(
        self,
        path: str | os.PathLike[str],
        revision: str,
        *,
        expected_hash: str | None = None,
        history: bool = True,
    ) -> dict[str, Any]:
        resource = self._history_resource_for_input(path)
        if resource is None:
            raise ValueError("file history requires a workspace file path")
        if type(history) is not bool:
            raise TypeError("history must be a boolean")
        store = self._history_store()
        data = await store.restore_bytes(resource, revision)
        if data is None:
            resolved, display = self._path(path)
            current = await _to_thread_uncancelled(
                _read_history_optional_bytes if history else _read_optional_bytes,
                resolved,
                display,
            )
            if current is None:
                if expected_hash is not None:
                    raise ValueError(f"Revision mismatch: expected {expected_hash}, got None")
                return {
                    "path": display,
                    "restored": True,
                    "absent": True,
                    "changed": False,
                    "history_recorded": False,
                }
            result = await self.delete(path, expected_hash=expected_hash, history=history)
            result.update({"restored": True, "absent": True, "changed": True})
            return result
        return await self.write_bytes(
            path,
            data,
            expected_hash=expected_hash,
            overwrite=expected_hash is not None,
            create_parents=True,
            history=history,
        )

    async def delete(
        self,
        path: str | os.PathLike[str],
        *,
        expected_hash: str | None = None,
        history: bool = True,
    ) -> dict[str, Any]:
        if expected_hash is None:
            raise ValueError("delete requires expected_hash")
        _reject_symlink_input(self.workspace, path)
        resolved, display = self._path(path)
        if history:
            _validate_history_target(resolved, display)
        resource = self._history_resource(resolved) if history else None
        history_enabled = resource is not None
        async with AsyncExitStack() as stack:
            if resource is not None:
                await stack.enter_async_context(self._history_store().transaction(resource))
            lock = self._lock(resolved)
            await lock.acquire()
            stack.callback(lock.release)
            old = await _to_thread_uncancelled(
                _read_history_optional_bytes if history_enabled else _read_optional_bytes,
                resolved,
                display,
            )
            _check_expected(old, expected_hash)
            if old is None:
                raise FileNotFoundError(display)
            history_enabled = resource is not None
            if history_enabled:
                _validate_history_size(old, None, display)
            await _to_thread_uncancelled(
                _unlink_expected,
                resolved,
                display,
                old,
                _history_blob_limit() if history_enabled else None,
            )
            if history_enabled:
                await self._record_history(resolved, display, old, None)
            return {
                "path": display,
                "deleted": True,
                "old_revision": _sha256(old),
                "history_recorded": history and self._history_resource(resolved) is not None,
            }

    async def move(
        self,
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        *,
        expected_hash: str | None = None,
        overwrite: bool = False,
        history: bool = True,
    ) -> dict[str, Any]:
        if expected_hash is None:
            raise ValueError("move requires expected_hash")
        return await self._copy_or_move(
            source, destination, expected_hash, overwrite, history, move=True
        )

    async def copy(
        self,
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        *,
        expected_hash: str | None = None,
        overwrite: bool = False,
        history: bool = True,
    ) -> dict[str, Any]:
        return await self._copy_or_move(
            source, destination, expected_hash, overwrite, history, move=False
        )

    async def _copy_or_move(
        self,
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        expected_hash: str | None,
        overwrite: bool,
        history: bool,
        *,
        move: bool,
    ) -> dict[str, Any]:
        if type(overwrite) is not bool or type(history) is not bool:
            raise TypeError("overwrite and history must be booleans")
        if overwrite:
            raise ValueError("destination overwrite is not supported")
        source_path, source_display = self._path(source)
        destination_path, destination_display = self._path(destination)
        _reject_symlink_input(self.workspace, source)
        _reject_symlink_input(self.workspace, destination)
        if source_path == destination_path:
            raise ValueError("source and destination are identical")
        if history:
            _validate_history_target(source_path, source_display)
            _validate_history_target(destination_path, destination_display)
        history_store = self._history_store() if history else None
        resources = (
            sorted(
                {
                    resource
                    for path in (source_path, destination_path)
                    if (resource := self._history_resource(path)) is not None
                }
            )
            if history
            else []
        )
        history_enabled = bool(resources)
        locks = [self._lock(path) for path in sorted({source_path, destination_path}, key=str)]
        async with AsyncExitStack() as stack:
            if history_store is not None:
                for resource in resources:
                    await stack.enter_async_context(history_store.transaction(resource))
            for lock in locks:
                await lock.acquire()
                stack.callback(lock.release)
            source_info = source_path.stat() if source_path.exists() else None
            source_resource = self._history_resource(source_path) if history else None
            destination_resource = self._history_resource(destination_path) if history else None
            read_limit = _history_blob_limit() if history_enabled else None
            if source_info is not None and read_limit is not None:
                _validate_history_stat_size(source_info, source_display)
            old = await _to_thread_uncancelled(
                _read_history_optional_bytes if read_limit is not None else _read_optional_bytes,
                source_path,
                source_display,
            )
            _check_expected(old, expected_hash)
            if old is None:
                raise FileNotFoundError(source_display)
            if source_info is None:
                raise RuntimeError(f"File changed while reading: {source_display}")
            try:
                source_after_read = source_path.stat()
            except FileNotFoundError as exc:
                raise RuntimeError(f"File changed while reading: {source_display}") from exc
            if _signature(source_info) != _signature(source_after_read):
                raise RuntimeError(f"File changed while reading: {source_display}")
            if destination_resource is not None:
                _validate_history_path_size(destination_path, destination_display)
            destination_old = await _to_thread_uncancelled(
                _read_history_optional_bytes
                if destination_resource is not None
                else _read_optional_bytes,
                destination_path,
                destination_display,
            )
            if destination_old is not None and not overwrite:
                raise FileExistsError(destination_display)
            if history_enabled:
                _validate_history_size(old, old, source_display)
            if destination_path.parent != self.workspace and not destination_path.parent.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
            transitions = []
            plans = []
            if source_resource is not None and move:
                transitions.append((source_path, source_display, old, None))
                plans.append(
                    {
                        "operation": "move",
                        "source": source_path,
                        "source_old": old,
                        "path": destination_path,
                        "new": old,
                    }
                )
            if destination_resource is not None:
                transitions.append((destination_path, destination_display, destination_old, old))
                if not move or source_resource is None:
                    plans.append(
                        {
                            "operation": "copy",
                            "path": destination_path,
                            "old": destination_old,
                            "new": old,
                        }
                    )
            if history_enabled:
                await _to_thread_uncancelled(history_store.prepare_changes_sync, plans)
            try:
                copy_state = {"destination_committed": False}
                await _to_thread_uncancelled(
                    _copy_bytes,
                    source_path,
                    destination_path,
                    old,
                    destination_old,
                    _signature(source_info),
                    move,
                    copy_state,
                    read_limit,
                )
            except BaseException as exc:
                if not copy_state["destination_committed"]:
                    raise
                try:
                    await _to_thread_uncancelled(
                        _restore_transition,
                        destination_path,
                        destination_old,
                        old,
                        read_limit,
                    )
                except BaseException as rollback_error:
                    raise RuntimeError(
                        f"lifecycle operation failed and destination rollback was incomplete: "
                        f"{rollback_error}"
                    ) from exc
                raise
            if history_enabled:
                try:
                    await _to_thread_uncancelled(history_store.record_changes_sync, plans)
                except RevisionIndexOutcomeUnknown as exc:
                    raise RuntimeError(
                        "history index outcome is unknown; the lifecycle change remains in "
                        "place. Inspect history before retrying."
                    ) from exc
                except BaseException as exc:
                    recovery = []
                    for target, _, before, after in reversed(transitions):
                        try:
                            await _to_thread_uncancelled(
                                _restore_transition,
                                target,
                                before,
                                after,
                                read_limit,
                            )
                        except BaseException as rollback_error:
                            recovery.append(f"{target}: {rollback_error}")
                    if recovery:
                        raise RuntimeError(
                            "history write failed and lifecycle rollback was incomplete: "
                            + "; ".join(recovery)
                        ) from exc
                    raise RuntimeError(
                        "history write failed; lifecycle change was rolled back"
                    ) from exc
            return {
                "source": source_display,
                "path": destination_display,
                "operation": "move" if move else "copy",
                "old_revision": _sha256(old),
                "revision": _sha256(old),
                "size": len(old),
                "history_recorded": history_enabled
                and (self._history_resource(source_path) is not None
                     or self._history_resource(destination_path) is not None),
            }

    def _history_store(self):
        from .revisions import RevisionStore

        return RevisionStore(self.workspace, self, "files")

    def _history_resource_for_input(self, path: str | os.PathLike[str]) -> str | None:
        resolved, display = self._path(path)
        _validate_history_target(resolved, display)
        return self._history_resource(resolved)

    def _history_resource(self, path: Path) -> str | None:
        try:
            relative = path.resolve(strict=False).relative_to(self.workspace)
        except ValueError:
            return None
        if not relative.parts:
            return None
        if relative.parts[0] == ".mypr":
            editable = relative.parts[1:2] == ("skills",) or relative.parts[:3] == (
                ".mypr",
                "lib",
                "ws_lib",
            )
            if not editable:
                return None
        return relative.as_posix()

    async def _record_history(
        self, path: Path, display: str, old: bytes | None, new: bytes | None
    ) -> None:
        resource = self._history_resource(path)
        if resource is None or old == new:
            return
        _validate_history_size(old, new, display)
        store = self._history_store()
        try:
            await store.record_bytes(resource, (old, new))
        except BaseException as exc:
            try:
                await _to_thread_uncancelled(
                    _restore_transition,
                    path,
                    old,
                    new,
                    _history_blob_limit(),
                )
            except BaseException as rollback_error:
                raise RuntimeError(
                    f"history write failed for {display}; rollback was incomplete: "
                    f"{rollback_error}"
                ) from exc
            raise RuntimeError(
                f"history write failed for {display}; file change was rolled back"
            ) from exc

    async def image(
        self,
        path: str | os.PathLike[str],
        *,
        max_bytes: int = 2 * 1024 * 1024,
        resize: tuple[int, int] | None = None,
        crop: tuple[int, int, int, int] | None = None,
        max_input_bytes: int = 64 * 1024 * 1024,
    ):
        """Load a PNG or JPEG for inline display. ``resize`` fits within a (width, height)
        box; ``crop`` uses (left, top, right, bottom) pixel coordinates. Modified images
        include source path, dimensions, crop, and SHA-256 in display metadata.
        """
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        if (
            not isinstance(max_input_bytes, int)
            or isinstance(max_input_bytes, bool)
            or not 1 <= max_input_bytes <= 64 * 1024 * 1024
        ):
            raise ValueError("max_input_bytes must be between 1 and 67108864")
        resolved, display = self._path(path)
        await _to_thread_uncancelled(_preflight_input, resolved, display, max_input_bytes)
        if resize is not None or crop is not None:
            from .media_tools import (
                _MAX_OUTPUT_BYTES,
                _validate_box,
                _validate_limit,
            )

            _validate_limit("max_output_bytes", max_bytes, _MAX_OUTPUT_BYTES)
            _validate_box("resize", resize, 2, max_pixels=16_000_000)
            _validate_box("crop", crop, 4)
            await self._ensure("pillow")
            from .media_tools import display_image, transform_image

            data, metadata = await transform_image(
                resolved,
                display,
                max_output_bytes=max_bytes,
                max_input_bytes=max_input_bytes,
                resize=resize,
                crop=crop,
            )
            return display_image(
                data=data,
                image_format=metadata["format"],
                metadata=metadata,
                alt=f"{display} ({metadata['width']}×{metadata['height']})",
            )
        data, image_format = await _to_thread_uncancelled(_read_image, resolved, display, max_bytes)
        from IPython.display import Image

        return Image(data=data, format=image_format, embed=True)

    async def image_info(
        self,
        path: str | os.PathLike[str],
        *,
        max_input_bytes: int = 64 * 1024 * 1024,
    ) -> dict[str, Any]:
        """Inspect a PNG or JPEG. Returns source path, SHA-256, byte size, format, mode,
        and pixel dimensions without decoding the full image in the kernel process.
        """
        from .media_tools import inspect_image

        if (
            not isinstance(max_input_bytes, int)
            or isinstance(max_input_bytes, bool)
            or not 1 <= max_input_bytes <= 64 * 1024 * 1024
        ):
            raise ValueError("max_input_bytes must be between 1 and 67108864")
        resolved, display = self._path(path)
        await _to_thread_uncancelled(_preflight_input, resolved, display, max_input_bytes)
        await self._ensure("pillow")
        return await inspect_image(resolved, display, max_input_bytes=max_input_bytes)

    async def patch(
        self,
        path: str | os.PathLike[str],
        edits: list[dict[str, Any]],
        *,
        expected_hash: str | None = None,
        dry_run: bool = False,
        encoding: str = "utf-8",
        max_diff_bytes: int = 32_768,
        history: bool = True,
    ) -> dict[str, Any]:
        """Apply exact text replacements atomically and return a bounded diff."""
        if not isinstance(edits, list):
            raise TypeError("edits must be a list")
        if type(dry_run) is not bool:
            raise TypeError("dry_run must be a boolean")
        if (
            not isinstance(max_diff_bytes, int)
            or isinstance(max_diff_bytes, bool)
            or max_diff_bytes < 1
        ):
            raise ValueError("max_diff_bytes must be a positive integer")
        if type(history) is not bool:
            raise TypeError("history must be a boolean")
        resolved, display = self._path(path)
        if history:
            _validate_history_target(resolved, display)
        resource = self._history_resource(resolved) if history else None
        history_enabled = resource is not None
        async with AsyncExitStack() as stack:
            if resource is not None:
                await stack.enter_async_context(self._history_store().transaction(resource))
            lock = self._lock(resolved)
            await lock.acquire()
            stack.callback(lock.release)
            result = await _to_thread_uncancelled(
                _patch_file,
                resolved,
                display,
                edits,
                expected_hash,
                dry_run,
                encoding,
                max_diff_bytes,
                history_enabled,
            )
            old = result.pop("_old", None)
            new = result.pop("_new", None)
            if history_enabled and not dry_run:
                await self._record_history(resolved, display, old, new)
                result["history_recorded"] = self._history_resource(resolved) is not None
            return result

    async def search(
        self,
        pattern=None,
        *,
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
        scan_bytes=16 * 1024 * 1024,
        scan_limit=None,
        max_matches=100,
        max_bytes=32_768,
        cursor=None,
        page_cursor=None,
    ):  # noqa: ASYNC109
        return await self._search_query(
            "rg",
            pattern,
            dict(
                mode=mode,
                paths=paths,
                glob=glob,
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
                timeout=timeout,
                scan_bytes=scan_bytes,
                scan_limit=scan_limit,
                max_matches=max_matches,
                max_bytes=max_bytes,
                cursor=cursor,
                page_cursor=page_cursor,
            ),
        )

    async def search_docs(
        self,
        pattern=None,
        *,
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
        adapters=None,
        accurate=False,
        cache=True,
        archive_depth=5,
        timeout=30,  # noqa: ASYNC109
        scan_bytes=16 * 1024 * 1024,
        scan_limit=None,
        max_matches=100,
        max_bytes=32_768,
        cursor=None,
        page_cursor=None,
    ):  # noqa: ASYNC109
        return await self._search_query(
            "rga",
            pattern,
            dict(
                mode=mode,
                paths=paths,
                glob=glob,
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
                timeout=timeout,
                scan_bytes=scan_bytes,
                scan_limit=scan_limit,
                max_matches=max_matches,
                max_bytes=max_bytes,
                cursor=cursor,
                page_cursor=page_cursor,
            ),
        )

    async def search_ast(
        self,
        pattern=None,
        *,
        lang=None,
        rule=None,
        constraints=None,
        utils=None,
        paths=None,
        glob=None,
        hidden=False,
        no_ignore=False,
        mode=None,
        strictness="smart",
        timeout=30,  # noqa: ASYNC109
        scan_bytes=16 * 1024 * 1024,
        scan_limit=None,
        max_matches=100,
        max_bytes=32_768,
        cursor=None,
        page_cursor=None,
    ):  # noqa: ASYNC109
        return await self._search_query(
            "ast",
            pattern,
            dict(
                lang=lang,
                rule=rule,
                constraints=constraints,
                utils=utils,
                paths=paths,
                glob=glob,
                hidden=hidden,
                no_ignore=no_ignore,
                mode=mode,
                strictness=strictness,
                timeout=timeout,
                scan_bytes=scan_bytes,
                scan_limit=scan_limit,
                max_matches=max_matches,
                max_bytes=max_bytes,
                cursor=cursor,
                page_cursor=page_cursor,
            ),
        )

    async def search_backends(self):
        return await self._search_query("info", None, {})

    async def _search_query(self, backend, pattern, options):
        if "backend" in options:
            raise ValueError("select a search method instead of overriding backend")
        options = dict(options)
        page_cursor = options.pop("page_cursor", None)
        cursor = options.get("cursor")
        if cursor is not None and page_cursor is not None:
            raise ValueError("cursor and page_cursor cannot both be provided")
        if page_cursor is not None:
            options["cursor"] = page_cursor
        args = dict(pattern=pattern, backend=backend, **options)
        if self._searcher is not None:
            return await self._searcher(**args)
        if self._shell is None:
            raise RuntimeError("workspace search requires the workspace shell")
        from .search import Search

        return await Search(
            self.workspace,
            self._shell,
            ensure_dependencies=self._ensure_dependencies,
        ).search(**args)

    async def tree(
        self,
        path: str | os.PathLike[str] = ".",
        *,
        depth: int = 3,
        max_entries: int = 200,
        hidden: bool = False,
    ) -> dict[str, Any]:
        """Return a deterministic, bounded directory tree without following links."""
        if not isinstance(depth, int) or isinstance(depth, bool) or depth < 0:
            raise ValueError("depth must be a non-negative integer")
        if not isinstance(max_entries, int) or isinstance(max_entries, bool) or max_entries < 1:
            raise ValueError("max_entries must be a positive integer")
        if type(hidden) is not bool:
            raise TypeError("hidden must be a boolean")
        candidate = _lexical_path(self.workspace, path)
        return await _to_thread_uncancelled(
            _tree,
            candidate,
            _display_path(candidate, self.workspace),
            self.workspace,
            depth,
            max_entries,
            hidden,
        )

    async def stat(
        self,
        path: str | os.PathLike[str],
        *,
        follow_symlinks: bool = False,
    ) -> dict[str, Any]:
        """Return bounded metadata for a path, preserving terminal symlinks by default."""
        if type(follow_symlinks) is not bool:
            raise TypeError("follow_symlinks must be a boolean")
        candidate = _lexical_path(self.workspace, path)
        return await _to_thread_uncancelled(
            _stat_path,
            candidate,
            _display_path(candidate, self.workspace),
            follow_symlinks,
        )


def _below(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _lexical_path(root: Path, path: str | os.PathLike[str]) -> Path:
    supplied = Path(path).expanduser()
    candidate = supplied if supplied.is_absolute() else root / supplied
    return Path(os.path.normpath(candidate))


def _display_path(path: Path, root: Path) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return str(path)
    return str(relative) or "."


def _metadata(path: Path, display: str, *, follow_symlinks: bool = False) -> dict[str, Any]:
    info = path.stat() if follow_symlinks else path.lstat()
    mode = stat.S_IMODE(info.st_mode)
    if stat.S_ISLNK(info.st_mode):
        kind = "symlink"
    elif stat.S_ISDIR(info.st_mode):
        kind = "directory"
    elif stat.S_ISREG(info.st_mode):
        kind = "file"
    else:
        kind = "other"
    result: dict[str, Any] = {
        "path": display,
        "kind": kind,
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "mode": mode,
    }
    if kind == "symlink":
        result["target"] = os.readlink(path)
    return result


class _ReverseName:
    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, _ReverseName):
            return NotImplemented
        return self.value > other.value


def _children(path: Path, limit: int, hidden: bool) -> tuple[list[os.DirEntry[str]], bool]:
    heap: list[tuple[_ReverseName, str, os.DirEntry[str]]] = []
    try:
        entries = os.scandir(path)
        with entries:
            for entry in entries:
                if not hidden and entry.name.startswith("."):
                    continue
                item = (_ReverseName(entry.name), entry.name, entry)
                if len(heap) < limit:
                    heapq.heappush(heap, item)
                elif entry.name < heap[0][1]:
                    heapq.heapreplace(heap, item)
    except OSError:
        raise
    return [item[2] for item in sorted(heap, key=lambda item: item[1])], len(heap) == limit


def _tree(
    path: Path,
    display: str,
    root: Path,
    depth: int,
    max_entries: int,
    hidden: bool,
) -> dict[str, Any]:
    count = 0
    truncated = False

    def visit(current: Path, current_display: str, remaining_depth: int) -> dict[str, Any]:
        nonlocal count, truncated
        node = _metadata(current, current_display)
        if node["kind"] != "directory" or remaining_depth <= 0:
            return node
        children, overflow = _children(current, max_entries - count + 1, hidden)
        truncated |= overflow
        result_children: list[dict[str, Any]] = []
        for entry in children:
            if count >= max_entries:
                truncated = True
                break
            child = Path(entry.path)
            child_display = _display_path(child, root)
            count += 1
            result_children.append(visit(child, child_display, remaining_depth - 1))
        if result_children:
            node["entries"] = result_children
        return node

    result = visit(path, display, depth)
    result["truncated"] = truncated
    return result


def _stat_path(path: Path, display: str, follow_symlinks: bool) -> dict[str, Any]:
    return _metadata(path, display, follow_symlinks=follow_symlinks)


def _validate_line_range(
    start_line: int, end_line: int | None, start_byte: int | None, max_bytes: int
) -> None:
    if not isinstance(start_line, int) or isinstance(start_line, bool) or start_line < 1:
        raise ValueError("start_line must be a positive integer")
    if end_line is not None and (
        not isinstance(end_line, int) or isinstance(end_line, bool) or end_line < start_line
    ):
        raise ValueError("end_line must be at least start_line")
    if start_byte is not None and (
        not isinstance(start_byte, int) or isinstance(start_byte, bool) or start_byte < 0
    ):
        raise ValueError("start_byte must be a non-negative integer")
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")


def _read_image(path: Path, display: str, limit: int) -> tuple[bytes, str]:
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        _require_regular(before, display)
        if before.st_size > limit:
            raise ValueError(f"Image exceeds max_bytes: {display}")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise ValueError(f"Image exceeds max_bytes: {display}")
        if _signature(before) != _signature(after):
            raise RuntimeError(f"File changed while reading: {display}")
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return data, "png"
        if data.startswith(b"\xff\xd8\xff"):
            return data, "jpeg"
        raise ValueError("image expects a PNG or JPEG file")
    finally:
        if fd >= 0:
            os.close(fd)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _inode(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _require_regular(info: os.stat_result, display: str) -> None:
    if stat.S_ISDIR(info.st_mode):
        raise IsADirectoryError(display)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Path must be a regular file: {display}")


def _preflight_input(path: Path, display: str, max_bytes: int) -> None:
    try:
        info = path.stat()
    except FileNotFoundError:
        raise FileNotFoundError(display) from None
    _require_regular(info, display)
    if info.st_size > max_bytes:
        raise ValueError(f"file exceeds max_input_bytes: {display}")


def _read_regular(
    path: Path,
    display: str,
    *,
    max_bytes: int | None = None,
    nofollow: bool = False,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if nofollow:
        flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        _require_regular(info, display)
        if max_bytes is not None and info.st_size > max_bytes:
            raise _ReadLimitExceeded(path, max_bytes)
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(-1 if max_bytes is None else max_bytes + 1)
        if max_bytes is not None and len(data) > max_bytes:
            raise _ReadLimitExceeded(path, max_bytes)
        return data, info
    finally:
        if fd >= 0:
            os.close(fd)


def _read_optional_bytes(
    path: Path, display: str, *, max_bytes: int | None = None
) -> bytes | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode):
        raise ValueError(f"Path must not be a symlink: {display}")
    _require_regular(info, display)
    if max_bytes is not None and info.st_size > max_bytes:
        raise _ReadLimitExceeded(path, max_bytes)
    data, opened = _read_regular(path, display, max_bytes=max_bytes)
    after = path.stat()
    if _signature(info) != _signature(opened) or _signature(info) != _signature(after):
        raise RuntimeError(f"File changed while reading: {display}")
    return data


def _read_history_optional_bytes(path: Path, display: str) -> bytes | None:
    try:
        return _read_optional_bytes(path, display, max_bytes=_history_blob_limit())
    except _ReadLimitExceeded as exc:
        raise _history_size_error(display, exc.limit) from exc


def _read_bytes(path: Path, display: str, start_byte: int, max_bytes: int) -> dict[str, Any]:
    before = path.stat()
    _require_regular(before, display)
    size = before.st_size
    if start_byte > size:
        raise ValueError("start_byte is beyond the file")
    end = min(start_byte + max_bytes, size)
    digest = hashlib.sha256()
    chunk = bytearray()
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        opened = os.fstat(fd)
        _require_regular(opened, display)
        if _signature(before) != _signature(opened):
            raise RuntimeError(f"File changed while reading: {display}")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            offset = 0
            while data := stream.read(1024 * 1024):
                digest.update(data)
                block_end = offset + len(data)
                left = max(start_byte, offset)
                right = min(end, block_end)
                if left < right:
                    chunk.extend(data[left - offset : right - offset])
                offset = block_end
            after = path.stat()
        if _signature(before) != _signature(after) or offset != size:
            raise RuntimeError(f"File changed while reading: {display}")
    finally:
        if fd >= 0:
            os.close(fd)
    return {
        "path": display,
        "data_base64": base64.b64encode(chunk).decode("ascii"),
        "start_byte": start_byte,
        "size": size,
        "revision": digest.hexdigest(),
        "next_cursor": end if end < size else None,
        "truncated": end < size,
    }


def _check_expected(old: bytes | None, expected_hash: str | None) -> None:
    if expected_hash is not None and (old is None or _sha256(old) != expected_hash):
        actual = None if old is None else _sha256(old)
        raise ValueError(f"Revision mismatch: expected {expected_hash}, got {actual}")


def _read_file(
    path: Path,
    display: str,
    start_line: int,
    end_line: int | None,
    start_byte: int | None,
    max_bytes: int,
    encoding: str,
) -> dict[str, Any]:
    if encoding.lower().replace("-", "") not in {"utf8"}:
        raise ValueError("read currently supports UTF-8 only")
    before = path.stat()
    _require_regular(before, display)
    digest = hashlib.sha256()
    decoder = codecs.getincrementaldecoder("utf-8")()
    output = bytearray()
    capture_limit = max_bytes + 3
    selected_seen = 0
    line_no = 1
    total = 0
    has_data = False
    start_line_at_byte: int | None = 1 if start_byte == 0 else None
    capture_start: int | None = start_byte
    ends_newline = False
    trunc_line: int | None = None

    def validate(chunk: bytes) -> None:
        nonlocal start_line_at_byte
        if start_byte is not None and start_byte == total:
            if decoder.getstate()[0]:
                raise ValueError("start_byte must be at an encoding boundary")
            start_line_at_byte = line_no
        if start_byte is None or not total < start_byte < total + len(chunk):
            decoder.decode(chunk, final=False)
            return
        split = start_byte - total
        decoder.decode(chunk[:split], final=False)
        if decoder.getstate()[0]:
            raise ValueError("start_byte must be at an encoding boundary")
        start_line_at_byte = start_line_at_byte or line_no
        decoder.decode(chunk[split:], final=False)

    def capture(segment: bytes, segment_start: int, current_line: int) -> None:
        nonlocal capture_start, selected_seen, trunc_line
        if current_line < start_line or (end_line is not None and current_line > end_line):
            return
        if start_byte is not None:
            segment = segment[max(0, start_byte - segment_start) :]
        if not segment:
            return
        if capture_start is None:
            capture_start = segment_start
        selected_seen += len(segment)
        if len(output) < capture_limit:
            output.extend(segment[: capture_limit - len(output)])
        if selected_seen > max_bytes and trunc_line is None:
            trunc_line = current_line

    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    opened = os.fstat(fd)
    try:
        _require_regular(opened, display)
        stream = os.fdopen(fd, "rb")
        fd = -1
    finally:
        if fd >= 0:
            os.close(fd)
    with stream:
        while chunk := stream.read(64 * 1024):
            has_data = True
            digest.update(chunk)
            validate(chunk)
            position = 0
            while position < len(chunk):
                newline = chunk.find(b"\n", position)
                if newline < 0:
                    if start_byte is not None and total + position <= start_byte < total + len(
                        chunk
                    ):
                        start_line_at_byte = line_no
                    capture(chunk[position:], total + position, line_no)
                    break
                segment_end = newline + 1
                if start_byte is not None and total + position <= start_byte < total + segment_end:
                    start_line_at_byte = line_no
                capture(chunk[position:segment_end], total + position, line_no)
                line_no += 1
                if start_byte == total + segment_end:
                    start_line_at_byte = line_no
                ends_newline = True
                position = segment_end
            total += len(chunk)
            ends_newline = chunk.endswith(b"\n")
    decoder.decode(b"", final=True)
    after = path.stat()
    if _signature(before) != _signature(after):
        raise RuntimeError(f"File changed while reading: {display}")
    if start_byte is not None and start_byte > total:
        raise ValueError("start_byte is beyond the file")
    if start_byte is not None and start_byte == total:
        start_line_at_byte = line_no
    if (
        start_byte is not None
        and start_line_at_byte is not None
        and start_line_at_byte != start_line
    ):
        raise ValueError("start_byte does not belong to start_line")
    if not has_data:
        line_no = 0
    if trunc_line is not None:
        valid = _utf8_prefix(bytes(output), max_bytes)
        if not valid:
            raise ValueError("max_bytes is too small for the first UTF-8 code point")
        text = valid.decode("utf-8")
        next_byte = (capture_start if capture_start is not None else 0) + len(valid)
        next_line = trunc_line
        actual_end = start_line + valid.count(b"\n") - (1 if valid.endswith(b"\n") else 0)
    else:
        valid = bytes(output)
        text = valid.decode("utf-8")
        next_byte = None
        next_line = None
        last_line = line_no - 1 if ends_newline else line_no
        actual_end = (
            start_line - 1
            if not valid
            else last_line
            if end_line is None
            else min(last_line, end_line)
        )
    revision = digest.hexdigest()
    return {
        "path": display,
        "text": text,
        "start_line": start_line,
        "start_byte": start_byte,
        "end_line": actual_end,
        "next_line": next_line,
        "next_byte": next_byte,
        "next_cursor": None if next_line is None else {"line": next_line, "byte": next_byte},
        "truncated": trunc_line is not None,
        "revision": revision,
        "sha256": revision,
        "size": total,
    }


def _utf8_prefix(data: bytes, limit: int) -> bytes:
    candidate = data[:limit]
    try:
        candidate.decode("utf-8")
    except UnicodeDecodeError as exc:
        if exc.reason != "unexpected end of data":
            raise
        candidate = candidate[: exc.start]
    return candidate


def _write_file(
    path: Path,
    display: str,
    data: bytes,
    expected_hash: str | None,
    overwrite: bool,
    create_parents: bool,
    history: bool = False,
) -> dict[str, Any]:
    try:
        old_stat = path.stat()
    except FileNotFoundError:
        old_stat = None
    if old_stat is not None:
        _require_regular(old_stat, display)
        if history:
            _validate_history_stat_size(old_stat, display)
        try:
            old, old_stat = _read_regular(
                path,
                display,
                max_bytes=_history_blob_limit() if history else None,
            )
        except _ReadLimitExceeded as exc:
            raise _history_size_error(display, exc.limit) from exc
    else:
        old = None
    _check_expected(old, expected_hash)
    if old is not None and not (overwrite or expected_hash is not None):
        raise FileExistsError(f"File already exists: {display}")
    if history:
        _validate_history_size(old, data, display)
    if create_parents:
        path.parent.mkdir(parents=True, exist_ok=True)
    elif not path.parent.exists():
        raise FileNotFoundError(str(path.parent))
    info = _atomic_write(
        path,
        data,
        old,
        old_stat,
        max_bytes=_history_blob_limit() if history else None,
    )
    result = _write_result(display, data, info, old is not None)
    result["_old"] = old
    return result


def _patch_file(
    path: Path,
    display: str,
    edits: list[dict[str, Any]],
    expected_hash: str | None,
    dry_run: bool,
    encoding: str,
    max_diff_bytes: int,
    history: bool = False,
) -> dict[str, Any]:
    old_stat = path.stat()
    _require_regular(old_stat, display)
    if history:
        _validate_history_stat_size(old_stat, display)
    try:
        old, old_stat = _read_regular(
            path,
            display,
            max_bytes=_history_blob_limit() if history else None,
        )
    except _ReadLimitExceeded as exc:
        raise _history_size_error(display, exc.limit) from exc
    _check_expected(old, expected_hash)
    original = old.decode(encoding)
    updated = _apply_edits(original, edits)
    old_hash = _sha256(old)
    new_bytes = updated.encode(encoding)
    if history:
        _validate_history_size(old, new_bytes, display)
    new_hash = _sha256(new_bytes)
    diff, diff_truncated = _bounded_diff(display, original, updated, max_diff_bytes)
    if not dry_run and new_bytes != old:
        _atomic_write(
            path,
            new_bytes,
            old,
            old_stat,
            max_bytes=_history_blob_limit() if history else None,
        )
    return {
        "path": display,
        "changed": new_bytes != old,
        "dry_run": dry_run,
        "old_revision": old_hash,
        "revision": new_hash,
        "sha256": new_hash,
        "size": len(new_bytes),
        "diff": diff,
        "diff_truncated": diff_truncated,
        "_old": old,
        "_new": new_bytes,
    }


def _apply_edits(original: str, edits: list[dict[str, Any]]) -> str:
    result = original
    for number, edit in enumerate(edits, start=1):
        if (
            not isinstance(edit, dict)
            or not isinstance(edit.get("old"), str)
            or not isinstance(edit.get("new"), str)
        ):
            raise TypeError(f"edit {number} requires string old and new values")
        old, new = edit["old"], edit["new"]
        if not old:
            raise ValueError(f"edit {number} has an empty old value")
        count = edit.get("count")
        if count is None:
            matches = result.count(old)
            if matches != 1:
                raise ValueError(f"edit {number} expected one match, found {matches}")
            result = result.replace(old, new, 1)
            continue
        if count == "all" or (type(count) is int and count == -1):
            if result.count(old) == 0:
                raise ValueError(f"edit {number} expected at least one match, found 0")
            result = result.replace(old, new)
            continue
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError(f"edit {number} count must be a positive integer or 'all'")
        matches = result.count(old)
        if matches < count:
            raise ValueError(f"edit {number} requested {count} matches, found {matches}")
        result = result.replace(old, new, count)
    return result


def _bounded_diff(path: str, old: str, new: str, limit: int) -> tuple[str, bool]:
    lines = difflib.unified_diff(
        _diff_lines(old),
        _diff_lines(new),
        fromfile=path,
        tofile=path,
    )
    output = bytearray()
    truncated = False
    for line in lines:
        if not line.endswith("\n"):
            line += "\n\\ No newline at end of file\n"
        encoded = line.encode("utf-8")
        if len(output) + len(encoded) > limit:
            truncated = True
            break
        output.extend(encoded)
    return output.decode("utf-8"), truncated


def _diff_lines(value: str) -> list[str]:
    if not value:
        return []
    parts = value.split("\n")
    lines = [f"{part}\n" for part in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return lines


def _atomic_write(
    path: Path,
    data: bytes,
    old: bytes | None,
    old_stat: os.stat_result | None,
    *,
    on_commit: Callable[[], None] | None = None,
    max_bytes: int | None = None,
) -> os.stat_result:
    mode = stat.S_IMODE(old_stat.st_mode) if old_stat is not None else 0o600
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if old is None:
            os.link(temporary_path, path)
            if on_commit is not None:
                on_commit()
            temporary_path.unlink(missing_ok=True)
        else:
            try:
                current = path.stat()
                try:
                    current_data, current = _read_regular(
                        path, str(path), max_bytes=max_bytes
                    )
                except _ReadLimitExceeded as exc:
                    raise _history_size_error(str(path), exc.limit) from exc
                unchanged = _signature(current) == _signature(old_stat) and current_data == old
            except FileNotFoundError:
                unchanged = False
            if not unchanged:
                raise RuntimeError(f"File changed while updating: {path}")
            os.replace(temporary_path, path)
            if on_commit is not None:
                on_commit()
        return path.stat()
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_result(
    display: str, data: bytes, info: os.stat_result, overwritten: bool
) -> dict[str, Any]:
    digest = _sha256(data)
    return {
        "path": display,
        "created": not overwritten,
        "overwritten": overwritten,
        "revision": digest,
        "sha256": digest,
        "size": len(data),
        "mode": stat.S_IMODE(info.st_mode),
    }


def _validate_history_target(path: Path, display: str) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    if path.is_symlink():
        raise ValueError(f"History does not support symlink paths: {display}")
    if path.is_dir():
            raise IsADirectoryError(display)


def _reject_symlink_input(workspace: Path, value: str | os.PathLike[str]) -> None:
    supplied = Path(value).expanduser()
    candidate = supplied if supplied.is_absolute() else workspace / supplied
    if candidate.is_symlink():
        raise ValueError(f"Path must not be a symlink: {value}")


def _validate_history_size(old: bytes | None, new: bytes | None, display: str) -> None:
    limit = _history_blob_limit()

    for data in (old, new):
        if data is not None and len(data) > limit:
            raise _history_size_error(display, limit)


def _history_blob_limit() -> int:
    from .revisions import _MAX_BLOB_BYTES

    return _MAX_BLOB_BYTES


def _history_size_error(display: str, limit: int | None = None) -> ValueError:
    limit = _history_blob_limit() if limit is None else limit
    return ValueError(
        f"history for {display} exceeds {limit} bytes; retry with history=False"
    )


def _validate_history_stat_size(info: os.stat_result, display: str) -> None:
    limit = _history_blob_limit()
    _require_regular(info, display)
    if info.st_size > limit:
        raise _history_size_error(display, limit)


def _validate_history_path_size(path: Path, display: str) -> os.stat_result | None:
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    _validate_history_stat_size(info, display)
    return info


def _unlink_expected(
    path: Path, display: str, expected: bytes, max_bytes: int | None = None
) -> None:
    try:
        current = _read_optional_bytes(path, display, max_bytes=max_bytes)
    except _ReadLimitExceeded as exc:
        if max_bytes is not None:
            raise _history_size_error(display, exc.limit) from exc
        raise
    if current != expected:
        raise RuntimeError(f"File changed while deleting: {display}")
    path.unlink()


def _copy_bytes(
    source: Path,
    destination: Path,
    data: bytes,
    destination_old: bytes | None,
    source_signature: tuple[int, int, int, int, int],
    move: bool,
    state: dict[str, bool] | None = None,
    max_bytes: int | None = None,
) -> None:
    source_mode = stat.S_IMODE(source.stat().st_mode)
    destination_info = destination.stat() if destination.exists() else None
    destination.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(
        destination,
        data,
        destination_old,
        destination_info,
        on_commit=(
            None
            if state is None
            else lambda: state.__setitem__("destination_committed", True)
        ),
        max_bytes=max_bytes,
    )
    if destination_info is None:
        destination.chmod(source_mode)
    if move:
        try:
            current = _read_optional_bytes(destination, str(destination), max_bytes=max_bytes)
        except _ReadLimitExceeded as exc:
            if max_bytes is not None:
                raise _history_size_error(str(destination), exc.limit) from exc
            raise
        if current != data:
            raise RuntimeError(f"Destination changed while moving: {destination}")
        try:
            current = _read_optional_bytes(source, str(source), max_bytes=max_bytes)
        except _ReadLimitExceeded as exc:
            if max_bytes is not None:
                raise _history_size_error(str(source), exc.limit) from exc
            raise
        try:
            source_info = source.stat()
        except FileNotFoundError as exc:
            raise RuntimeError(f"Source changed while moving: {source}") from exc
        if current != data or _signature(source_info) != source_signature:
            raise RuntimeError(f"Source changed while moving: {source}")
        source.unlink()


def _restore_transition(
    path: Path,
    old: bytes | None,
    new: bytes | None,
    max_bytes: int | None = None,
) -> None:
    try:
        current = _read_optional_bytes(path, str(path), max_bytes=max_bytes)
    except _ReadLimitExceeded as exc:
        if max_bytes is not None:
            raise _history_size_error(str(path), exc.limit) from exc
        raise
    if new is None:
        if current is not None:
            raise RuntimeError(f"File changed while rolling back: {path}")
        if old is None:
            return
        _atomic_write(path, old, None, None, max_bytes=max_bytes)
        return
    if current != new:
        raise RuntimeError(f"File changed while rolling back: {path}")
    if old is None:
        path.unlink(missing_ok=True)
    else:
        _atomic_write(path, old, new, path.stat(), max_bytes=max_bytes)


async def _to_thread_uncancelled(function, *args, **kwargs):
    return await wait_owned(asyncio.to_thread(function, *args, **kwargs))
