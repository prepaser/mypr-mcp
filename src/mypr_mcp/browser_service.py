"""Manager-owned Playwright server and browser installation support."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import re
import secrets
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .async_utils import finish_owned, wait_owned
from .file_io import open_regular

_ENGINES = {"chromium", "firefox", "webkit"}
_MAX_SERVER_OUTPUT = 64 * 1024
_SERVER_START_TIMEOUT = 30.0
_SERVER_STOP_TIMEOUT = 5.0
_INSTALL_LOCK_TIMEOUT = 300.0
_ENDPOINT = re.compile(r"^ws://(?:127\.0\.0\.1|localhost|\[::1\]):\d+/.+$")


def validate_launch_options(
    browser: str, options: dict[str, Any] | None, *, executable_path=None, channel=None
):
    if not isinstance(browser, str) or browser not in _ENGINES:
        raise ValueError("browser must be chromium, firefox, or webkit")
    if options is not None and not isinstance(options, dict):
        raise TypeError("launch_options must be a mapping or None")
    options = dict(options or {})
    if executable_path is None:
        executable_path = options.get("executable_path", options.get("executablePath"))
    if executable_path is not None:
        executable_path = str(executable_path)
        if not executable_path:
            raise ValueError("executable_path must not be empty")
    if channel is None:
        channel = options.get("channel")
    if channel is not None and (not isinstance(channel, str) or not channel):
        raise ValueError("channel must be a non-empty string")
    return options, executable_path, channel


@dataclass
class _Server:
    process: asyncio.subprocess.Process
    endpoint: str
    browser: str
    token: str
    stdout_task: asyncio.Task[None]
    stderr_task: asyncio.Task[None]
    monitor_task: asyncio.Task[None] | None = None


class BrowserService:
    """Own one Playwright server for a kernel generation.

    The server is intentionally independent from :class:`Shells`: a browser
    being open must not make a workspace appear to have an active command.
    Browser installation commands are ordinary, short-lived shell jobs so
    they remain visible in task history.
    """

    def __init__(
        self,
        workspace: str | Path,
        py: str | Path,
        kernel_pid: int | None = None,
        generation: str | None = None,
        shells: Any = None,
        track: Callable[..., Any] | None = None,
        *,
        manager_pid: int | None = None,
    ):
        self.workspace = Path(workspace).resolve()
        self.py = Path(py)
        self.kernel_pid = kernel_pid
        self.generation = str(generation or "local")
        self.shells = shells
        self.track = track
        self.manager_pid = manager_pid or os.getpid()
        self._server: _Server | None = None
        self._lock = asyncio.Lock()
        self._closing = False
        self._operation_tasks: set[asyncio.Task] = set()
        self._close_task: asyncio.Task[None] | None = None
        self._verified: set[str] = set()
        self._install_root = self.workspace / ".mypr" / "browser"
        self._driver_package_root: Path | None = None
        self._install_root.mkdir(parents=True, exist_ok=True)

    @property
    def endpoint(self) -> str | None:
        return self._server.endpoint if self._server else None

    @property
    def active(self) -> bool:
        return self._server is not None and self._server.process.returncode is None

    async def ensure(
        self,
        browser: str = "chromium",
        *,
        launch_options: dict[str, Any] | None = None,
        executable_path: str | None = None,
        channel: str | None = None,
        install: bool = True,
        track: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        if self._closing:
            raise RuntimeError("browser service is closed")
        operation = asyncio.create_task(
            self._ensure(
                browser,
                launch_options=launch_options,
                executable_path=executable_path,
                channel=channel,
                install=install,
                track=track,
            ),
            name="mypr:browser-ensure",
        )
        self._operation_tasks.add(operation)
        caller = asyncio.current_task()
        cancelling = caller.cancelling() if caller is not None else 0
        try:
            result = await asyncio.shield(operation)
            if self._closing:
                raise RuntimeError("browser service is closed")
            return result
        except asyncio.CancelledError:
            caller_cancelled = caller is not None and caller.cancelling() > cancelling
            if not self._closing or caller_cancelled:
                operation.cancel()
                await wait_owned(self._drain_task(operation), propagate=False)
                raise
            await wait_owned(self._drain_task(operation), propagate=False)
            raise RuntimeError("browser service is closed") from None
        finally:
            self._operation_tasks.discard(operation)

    async def _ensure(
        self,
        browser: str = "chromium",
        *,
        launch_options: dict[str, Any] | None = None,
        executable_path: str | None = None,
        channel: str | None = None,
        install: bool = True,
        track: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        launch_options, executable_path, channel = validate_launch_options(
            browser, launch_options, executable_path=executable_path, channel=channel,
        )
        if type(install) is not bool:
            raise TypeError("install must be a boolean")
        if executable_path is not None or channel is not None:
            install = False
        async with self._lock:
            if self._closing:
                raise RuntimeError("browser service is closed")
            current = self._server
            if executable_path is None and install:
                await self._ensure_installed(browser, track=track)
            if self._closing:
                raise RuntimeError("browser service is closed")
            if current is not None and current.process.returncode is None:
                return {
                    "endpoint": current.endpoint,
                    "browser": browser,
                    "generation": self.generation,
                    "reused": True,
                    "launch_options": launch_options,
                }
            if current is not None:
                await self._close_locked()
            server = asyncio.create_task(
                self._start_server(browser, executable_path=executable_path, channel=channel)
            )
            try:
                result = await asyncio.shield(server)
            except asyncio.CancelledError:
                if not server.done():
                    server.cancel()
                await wait_owned(self._drain_task(server), propagate=False)
                if self._server is not None:
                    await self._close_locked()
                raise
            result["launch_options"] = launch_options
            return result

    async def prepare(self, browser: str, *, track=None) -> dict[str, Any]:
        """Install a missing engine without launching a browser server."""
        if browser not in _ENGINES:
            raise ValueError("browser must be chromium, firefox, or webkit")
        if self._closing:
            raise RuntimeError("browser service is closed")
        operation = asyncio.create_task(self._ensure_installed(browser, track=track))
        self._operation_tasks.add(operation)
        try:
            await asyncio.shield(operation)
            return {"browser": browser, "installed": True}
        except asyncio.CancelledError:
            operation.cancel()
            await wait_owned(self._drain_task(operation), propagate=False)
            raise
        finally:
            self._operation_tasks.discard(operation)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_owned(), name="mypr:browser-close"
            )
        await wait_owned(self._close_task)

    async def _close_owned(self) -> None:
        self._closing = True
        operations = list(self._operation_tasks)
        for operation in operations:
            operation.cancel()
        await asyncio.gather(*operations, return_exceptions=True)
        async with self._lock:
            await self._close_locked()

    async def reset(self) -> None:
        """Close resources before a generation is replaced."""

        await self.close()

    async def _start_server(
        self,
        browser: str,
        *,
        executable_path: str | None,
        channel: str | None,
    ) -> dict[str, Any]:
        token = secrets.token_urlsafe(24)
        guard = Path(__file__).with_name("process_guard.py")
        command = [
            str(self.py),
            "-m",
            "playwright",
            "run-server",
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--path",
            f"/{token}",
            "--unsafe",
        ]
        args = [
            sys.executable,
            "-I",
            str(guard),
            "--parent-pid",
            str(self.manager_pid),
            "--tree",
        ]
        if self.kernel_pid is not None:
            args += ["--watch-pid", str(self.kernel_pid)]
        args += ["--", *command]
        caller = asyncio.current_task()
        launch_cancelling = caller.cancelling() if caller is not None else 0
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *args,
                cwd=str(self.workspace),
                env=dict(os.environ),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            ),
            name="mypr:browser-launch",
        )
        try:
            process, cancelled = await finish_owned(launch)
        except OSError as exc:
            if caller is not None and caller.cancelling() > launch_cancelling:
                raise asyncio.CancelledError from exc
            raise RuntimeError(f"unable to start Playwright server: {exc}") from exc
        if cancelled:
            await wait_owned(self._terminate_process(process), propagate=False)
            raise asyncio.CancelledError
        assert process.stdout is not None and process.stderr is not None
        stderr_task = asyncio.create_task(self._drain_pipe(process.stderr))
        endpoint = None
        lines: list[str] = []
        deadline = time.monotonic() + _SERVER_START_TIMEOUT
        try:
            while time.monotonic() < deadline:
                remaining = max(0.01, deadline - time.monotonic())
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), remaining)
                except TimeoutError:
                    break
                if not line:
                    break
                text = line.decode("utf-8", "replace").strip()
                if text:
                    lines.append(text)
                    if len("\n".join(lines).encode()) > _MAX_SERVER_OUTPUT:
                        lines = lines[-8:]
                match = re.search(r"ws://(?:127\.0\.0\.1|localhost|\[::1\]):\d+/[^\s]+", text)
                if match and _ENDPOINT.fullmatch(match.group(0)):
                    endpoint = match.group(0)
                    break
                if process.returncode is not None:
                    break
            if endpoint is None:
                await self._terminate_process(process)
                stderr = await stderr_task
                detail = stderr.decode("utf-8", "replace").strip() or "no endpoint returned"
                raise RuntimeError(f"Playwright server did not start: {detail[-1024:]}")
            stdout_task = asyncio.create_task(self._drain_pipe(process.stdout))
            server = _Server(process, endpoint, browser, token, stdout_task, stderr_task)
            self._server = server
            server.monitor_task = asyncio.create_task(self._monitor(server))
            return {
                "endpoint": endpoint,
                "browser": browser,
                "generation": self.generation,
                "reused": False,
                "executable_path": executable_path,
                "channel": channel,
            }
        except BaseException:
            await wait_owned(self._terminate_process(process), propagate=False)
            if not stderr_task.done():
                stderr_task.cancel()
            await wait_owned(self._drain_task(stderr_task), propagate=False)
            raise

    async def _close_locked(self) -> None:
        server = self._server
        self._server = None
        if server is None:
            return
        monitor_task = getattr(server, "monitor_task", None)
        if monitor_task is not None and not monitor_task.done():
            monitor_task.cancel()
        await self._terminate_process(server.process)
        for task in (server.stdout_task, server.stderr_task, monitor_task):
            if task is None:
                continue
            if not task.done():
                task.cancel()
        await asyncio.gather(server.stdout_task, server.stderr_task, return_exceptions=True)
        if monitor_task is not None:
            await asyncio.gather(monitor_task, return_exceptions=True)

    async def _monitor(self, server: _Server) -> None:
        with contextlib.suppress(Exception):
            await server.process.wait()
        if self._server is server:
            self._server = None

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            with contextlib.suppress(Exception):
                await process.wait()
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError, PermissionError:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), _SERVER_STOP_TIMEOUT)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), 2)

    async def _ensure_installed(
        self, browser: str, *, track: Callable[..., Any] | None = None
    ) -> None:
        sdk_version = await self._sdk_version()
        fingerprint = self._fingerprint(browser, sdk_version)
        marker = self._install_root / f"{browser}.json"
        if fingerprint in self._verified and await self._installed(browser):
            return
        lock_path = self._cache_lock_path(browser)
        async with _InstallLock(lock_path, _INSTALL_LOCK_TIMEOUT):
            if fingerprint in self._verified and await self._installed(browser):
                return
            if self.shells is None:
                raise RuntimeError("browser installation requires a shell service")
            start = asyncio.create_task(
                self.shells.start(
                    [str(self.py), "-m", "playwright", "install", browser],
                    cwd=str(self.workspace),
                    env=dict(os.environ),
                )
            )
            ident = None
            job = None
            wait = None
            try:
                job = await asyncio.shield(start)
                ident = job.get("id") if isinstance(job, dict) else None
                if not ident:
                    raise RuntimeError("browser installer did not return a job ID")
                await self._track_install(str(ident), browser, track=track)
                wait = asyncio.create_task(self.shells.wait(str(ident)))
                result = await asyncio.shield(wait)
            except asyncio.CancelledError:
                if not start.done():
                    start.cancel()
                if ident is None:
                    result = await wait_owned(self._drain_result(start), propagate=False)
                    if isinstance(result, dict):
                        job = result
                    ident = job.get("id") if isinstance(job, dict) else None
                if ident:
                    with contextlib.suppress(Exception):
                        await wait_owned(
                            self._cancel_install(str(ident), wait), propagate=False
                        )
                raise
            except BaseException:
                if ident:
                    with contextlib.suppress(Exception):
                        await wait_owned(
                            self._cancel_install(str(ident), wait), propagate=False
                        )
                raise
            if result.get("state") != "succeeded":
                detail = result.get("error") or (result.get("result") or {}).get("returncode")
                raise RuntimeError(f"Playwright browser installation failed: {detail}")
            if not await self._installed(browser):
                raise RuntimeError(f"Playwright did not install {browser}")
            temporary = marker.with_suffix(".tmp")
            temporary.write_text(json.dumps({"fingerprint": fingerprint}), encoding="utf-8")
            os.replace(temporary, marker)
            self._verified.add(fingerprint)

    async def _track_install(
        self, ident: str, browser: str, *, track: Callable[..., Any] | None = None
    ) -> None:
        callback = track or self.track
        if callback is None:
            return
        value = callback(
            ident,
            kind="shell",
            purpose="browser_install",
            browser=browser,
        )
        if isinstance(value, Awaitable):
            await value

    async def _cancel_install(self, ident: str, wait: asyncio.Task | None) -> None:
        with contextlib.suppress(Exception):
            await self.shells.cancel(ident)
        if wait is not None:
            with contextlib.suppress(Exception):
                await _await_shielded(wait, propagate=False)

    @staticmethod
    async def _drain_task(task: asyncio.Task) -> None:
        await asyncio.gather(task, return_exceptions=True)

    @staticmethod
    async def _drain_result(task: asyncio.Task) -> Any:
        result, = await asyncio.gather(task, return_exceptions=True)
        return result

    async def _installed(self, browser: str) -> bool:
        command = [
            str(self.py),
            "-c",
            (
                "import json\n"
                "from pathlib import Path\n"
                "from playwright.sync_api import sync_playwright\n"
                "with sync_playwright() as p:\n"
                f"    path = p.{browser}.executable_path\n"
                "    print(json.dumps({'path': path, 'exists': Path(path).is_file()}))\n"
            ),
        ]
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.workspace),
                env=dict(os.environ),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), 15)
        except asyncio.CancelledError:
            if process is not None:
                await self._terminate_probe(process)
            raise
        except OSError, TimeoutError:
            if process is not None:
                await self._terminate_probe(process)
            return False
        if process.returncode != 0:
            return False
        try:
            value = json.loads(stdout.decode("utf-8", "replace"))
            path = value.get("path")
        except ValueError, AttributeError:
            return False
        return isinstance(path, str) and value.get("exists") is True

    async def _sdk_version(self) -> str:
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                str(self.py),
                "-c",
                "import importlib.metadata as m; print(m.version('playwright'))",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=dict(os.environ),
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), 5)
        except asyncio.CancelledError:
            if process is not None:
                await self._terminate_probe(process)
            raise
        except (OSError, TimeoutError) as exc:
            if process is not None:
                await self._terminate_probe(process)
            raise RuntimeError("unable to determine Playwright SDK version") from exc
        if process.returncode != 0:
            raise RuntimeError("unable to determine Playwright SDK version")
        return stdout.decode("utf-8", "replace").strip()

    def _fingerprint(self, browser: str, sdk_version: str) -> str:
        payload = f"{sdk_version}:{browser}:{self._cache_path()}"
        return hashlib.sha256(payload.encode()).hexdigest()

    def _find_driver_package_root(self) -> Path | None:
        if self._driver_package_root is not None:
            return self._driver_package_root
        roots = [self.py.parent.parent]
        with contextlib.suppress(OSError):
            resolved = self.py.resolve()
            if resolved.parent.parent not in roots:
                roots.append(resolved.parent.parent)
        candidates: list[Path] = []
        for root in roots:
            candidates.extend(
                [
                    root / "Lib" / "site-packages" / "playwright" / "driver" / "package",
                    root / "lib" / "site-packages" / "playwright" / "driver" / "package",
                ]
            )
            for lib_root in (root / "Lib", root / "lib"):
                with contextlib.suppress(OSError):
                    candidates.extend(
                        lib_root.glob("python*/site-packages/playwright/driver/package")
                    )
        for candidate in candidates:
            if candidate.is_dir():
                self._driver_package_root = candidate.resolve()
                return self._driver_package_root
        return None

    def _resolve_playwright_path(self, value: str | os.PathLike[str]) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path.resolve()
        base = Path(os.environ.get("INIT_CWD") or self.workspace)
        if not base.is_absolute():
            base = self.workspace / base
        return (base / path).resolve()

    def _cache_path(self) -> Path:
        value = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
        if value == "0":
            package_root = self._find_driver_package_root()
            if package_root is None:
                raise RuntimeError("unable to locate the Playwright driver package")
            return package_root / ".local-browsers"
        if value:
            return self._resolve_playwright_path(value)
        cache = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
        return self._resolve_playwright_path(Path(cache) / "ms-playwright")

    def _cache_lock_path(self, browser: str) -> Path:
        cache = self._cache_path()
        return cache.parent / f".mypr-playwright-{browser}.lock"

    @staticmethod
    async def _terminate_probe(process) -> None:
        if process.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 2)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), 2)

    @staticmethod
    async def _read_pipe(pipe, *, limit: int) -> bytes:
        data = bytearray()
        while len(data) < limit:
            chunk = await pipe.read(min(8192, limit - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)

    @classmethod
    async def _drain_pipe(cls, pipe) -> bytes:
        data = bytearray()
        with contextlib.suppress(Exception):
            while chunk := await pipe.read(8192):
                data.extend(chunk)
                if len(data) > _MAX_SERVER_OUTPUT:
                    del data[: len(data) - _MAX_SERVER_OUTPUT]
        return bytes(data)

    @staticmethod
    def _validate_browser(browser: str) -> str:
        if not isinstance(browser, str) or browser not in _ENGINES:
            raise ValueError("browser must be one of chromium, firefox, or webkit")
        return browser


class _InstallLock:
    def __init__(self, path: Path, timeout: float):
        self.path = path
        self.timeout = timeout
        self.file = None

    async def __aenter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open_regular(self.path, "ab")
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                try:
                    fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return self
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("timed out waiting for browser installation") from None
                    await asyncio.sleep(0.1)
        except BaseException:
            self.file.close()
            self.file = None
            raise

    async def __aexit__(self, *_):
        if self.file is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            self.file.close()
            self.file = None


async def _await_shielded(task: asyncio.Task, *, propagate: bool = True):
    return await wait_owned(task, propagate=propagate)
