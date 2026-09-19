"""Managed Playwright browser contexts for the workspace Python API.

The manager owns the browser server.  This module only owns the Playwright
client, contexts created by the current kernel, and the small amount of
workspace metadata needed to reconnect them.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
import zipfile
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SHARED = object()


class BrowserError(RuntimeError):
    """Raised when a managed browser cannot be used."""


@dataclass(slots=True)
class _Connection:
    key: tuple[object, str]
    name: str
    owner: object
    browser_name: str
    browser: Any
    external: bool
    protocol: str
    endpoint: str | None
    signature: str


@dataclass(slots=True)
class _Context:
    key: tuple[object, str]
    name: str
    owner: str
    shared: bool
    context: Any
    browser_name: str
    connection_key: tuple[object, str]
    signature: str
    har_path: Path | None
    har_content: str | None


async def _shielded(awaitable: Awaitable[Any]) -> tuple[Any, bool]:
    """Finish an owned operation before propagating caller cancellation."""

    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while True:
        try:
            return await asyncio.shield(task), cancelled
        except asyncio.CancelledError:
            if task.done():
                if task.cancelled():
                    raise
                return task.result(), True
            cancelled = True


class BrowserTools:
    """Expose native async Playwright objects with workspace ownership."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        identity: Callable[[], Any],
        rpc: Callable[..., Awaitable[Any]],
        fs: Any = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self._identity = identity
        self._rpc = rpc
        self._fs = fs
        self._playwright: Any = None
        self._driver_task: asyncio.Task[Any] | None = None
        self._start_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._context_lock = asyncio.Lock()
        self._connections: dict[tuple[object, str], _Connection] = {}
        self._contexts: dict[tuple[object, str], _Context] = {}
        self._closed = False
        self._cleanup_task: asyncio.Task[None] | None = None

    async def _driver(self) -> Any:
        if self._closed:
            raise BrowserError("browser manager is closed")
        if self._playwright is not None:
            return self._playwright
        async with self._start_lock:
            if self._playwright is not None:
                return self._playwright
            if self._driver_task is None:
                self._driver_task = asyncio.create_task(self._start_driver())
            task = self._driver_task
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except BaseException:
            async with self._start_lock:
                if task.done() and self._driver_task is task:
                    self._driver_task = None
            raise

    async def _start_driver(self) -> Any:
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise BrowserError(
                "Playwright is unavailable; install the mypr-mcp browser dependencies"
            ) from exc
        try:
            driver = await async_playwright().start()
        except Exception as exc:
            raise BrowserError(f"unable to start Playwright: {exc}") from exc
        self._playwright = driver
        return driver

    def _owner(self) -> str:
        value = self._identity()
        if isinstance(value, Mapping):
            value = value.get("client_id", value.get("id"))
        if value is None:
            return "__anonymous__"
        return str(value)

    @staticmethod
    def _name(name: str) -> str:
        if not isinstance(name, str) or not name or len(name) > 128:
            raise ValueError("browser name must be a non-empty string of at most 128 characters")
        if any(char in name for char in "\\/\x00"):
            raise ValueError("browser name must not contain path separators")
        return name

    @staticmethod
    def _json_signature(value: Any) -> str:
        try:
            return json.dumps(
                BrowserTools._json_options(value), sort_keys=True, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise TypeError("browser options must be JSON serializable") from exc

    @staticmethod
    def _json_options(value: Any) -> Any:
        if isinstance(value, os.PathLike):
            return os.fspath(value)
        if isinstance(value, Mapping):
            return {str(key): BrowserTools._json_options(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [BrowserTools._json_options(item) for item in value]
        return value

    @staticmethod
    def _launch_options_for_node(options: Mapping[str, Any]) -> dict[str, Any]:
        names = {
            "executable_path": "executablePath",
            "ignore_default_args": "ignoreDefaultArgs",
            "handle_sigint": "handleSIGINT",
            "handle_sigterm": "handleSIGTERM",
            "handle_sighup": "handleSIGHUP",
            "slow_mo": "slowMo",
            "downloads_path": "downloadsPath",
            "traces_dir": "tracesDir",
        }
        return {names.get(key, key): value for key, value in options.items()}

    def _path(self, value: str | os.PathLike[str], *, directory: Path | None = None) -> Path:
        supplied = Path(value).expanduser()
        candidate = supplied if supplied.is_absolute() else self.workspace / supplied
        candidate = candidate.resolve(strict=False)
        if directory is not None or not supplied.is_absolute():
            base = (directory or self.workspace).resolve()
            try:
                candidate.relative_to(base)
            except ValueError as exc:
                raise ValueError(f"path must stay below {base}") from exc
        return candidate

    def _context_options_for_playwright(self, options: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(options)
        for key in ("record_har_path", "record_video_dir"):
            value = normalized.get(key)
            if value is not None:
                target = self._path(value)
                if key == "record_video_dir":
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                normalized[key] = str(target)
        storage_state = normalized.get("storage_state")
        if isinstance(storage_state, (str, os.PathLike)):
            normalized["storage_state"] = str(self._path(storage_state))
        return normalized

    def _context_key(self, owner: str, name: str, shared: bool) -> tuple[object, str]:
        return (_SHARED if shared else owner, name)

    def _connection_key(self, owner: object, name: str) -> tuple[object, str]:
        return owner, name

    @staticmethod
    def _browser_type(driver: Any, browser: str) -> Any:
        if browser not in {"chromium", "firefox", "webkit"}:
            raise ValueError("browser must be chromium, firefox, or webkit")
        try:
            return getattr(driver, browser)
        except AttributeError as exc:
            raise ValueError(f"unsupported browser: {browser}") from exc

    async def _managed_browser(
        self, owner: object, browser: str, launch_options: Mapping[str, Any]
    ) -> _Connection:
        key = (owner, f"__managed__:{browser}")
        signature = self._json_signature(dict(launch_options))
        async with self._connection_lock:
            existing = self._connections.get(key)
            if existing is not None:
                if not self._browser_alive(existing):
                    self._forget_connection(existing)
                else:
                    if existing.signature != signature:
                        raise RuntimeError(
                            f"browser launch settings changed for {browser}; "
                            "close the connection first"
                        )
                    return existing
            if self._closed:
                raise BrowserError("browser manager is closed")
            driver = await self._driver()
            if self._closed:
                raise BrowserError("browser manager is closed")
            try:
                result = await self._rpc(
                    "browser_server",
                    browser=browser,
                    launch_options=dict(launch_options),
                )
            except Exception as exc:
                raise BrowserError(f"unable to start managed browser server: {exc}") from exc
            endpoint = (
                result.get("endpoint", result.get("ws_endpoint"))
                if isinstance(result, Mapping)
                else result
            )
            if not isinstance(endpoint, str) or not endpoint:
                raise BrowserError("browser manager returned no Playwright endpoint")
            browser_type = self._browser_type(driver, browser)
            headers = {
                "x-playwright-browser": browser,
                "x-playwright-launch-options": self._json_signature(
                    self._launch_options_for_node(launch_options)
                ),
            }
            try:
                native, cancelled = await _shielded(browser_type.connect(endpoint, headers=headers))
            except Exception as exc:
                raise BrowserError(f"unable to connect to managed browser server: {exc}") from exc
            if self._closed:
                with suppress(Exception):
                    await _shielded(native.close())
                raise BrowserError("browser manager is closed")
            if cancelled:
                with suppress(Exception):
                    await _shielded(native.close())
                raise asyncio.CancelledError
            record = _Connection(
                key=key,
                name=f"__managed__:{browser}",
                owner=owner,
                browser_name=browser,
                browser=native,
                external=False,
                protocol="playwright",
                endpoint=endpoint,
                signature=signature,
            )
            self._connections[key] = record
            return record

    @staticmethod
    def _ensure_connected(record: _Connection) -> None:
        connected = getattr(record.browser, "is_connected", None)
        if callable(connected) and not connected():
            raise BrowserError(
                f"browser connection {record.name!r} is no longer available; close and recreate it"
            )

    async def context(
        self,
        name: str = "default",
        *,
        shared: bool = False,
        browser: str = "chromium",
        launch_options: Mapping[str, Any] | None = None,
        connection: str | None = None,
        **context_options: Any,
    ) -> Any:
        """Return a managed native :class:`BrowserContext`."""

        name = self._name(name)
        if type(shared) is not bool:
            raise TypeError("shared must be a boolean")
        if connection is not None:
            connection = self._name(connection)
        if launch_options is None:
            launch_options = {}
        if not isinstance(launch_options, Mapping):
            raise TypeError("launch_options must be a mapping or None")
        owner = self._owner()
        key = self._context_key(owner, name, shared)
        native_context_options = self._context_options_for_playwright(context_options)
        signature = self._json_signature(
            {
                "browser": browser,
                "launch_options": dict(launch_options),
                "connection": connection,
                "context_options": native_context_options,
            }
        )
        return await self._create_context(
            name,
            shared,
            browser,
            launch_options,
            connection,
            native_context_options,
            owner,
            key,
            signature,
        )

    async def _create_context(
        self,
        name: str,
        shared: bool,
        browser: str,
        launch_options: Mapping[str, Any],
        connection: str | None,
        context_options: Mapping[str, Any],
        owner: str,
        key: tuple[object, str],
        signature: str,
    ) -> Any:
        async with self._context_lock:
            if self._closed:
                raise BrowserError("browser manager is closed")
            existing = self._contexts.get(key)
            if existing is not None:
                record = self._connections.get(existing.connection_key)
                if record is None or not self._browser_alive(record):
                    self._contexts.pop(key, None)
                    if record is not None:
                        self._forget_connection(record)
                    if record is not None and record.external:
                        raise BrowserError(
                            f"browser connection {record.name!r} is no longer available; "
                            "reconnect it before creating a context"
                        )
                elif getattr(existing.context, "is_closed", lambda: False)():
                    self._contexts.pop(key, None)
                elif existing.signature != signature:
                    raise RuntimeError(f"browser context {name!r} settings changed; close it first")
                else:
                    return existing.context
            if connection is None:
                record = await self._managed_browser(
                    owner if not shared else _SHARED, browser, launch_options
                )
            else:
                record = self._connections.get(self._connection_key(owner, connection))
                if record is None:
                    record = self._connections.get(self._connection_key(_SHARED, connection))
                if record is None:
                    raise BrowserError(f"unknown browser connection: {connection}")
                self._ensure_connected(record)
                if launch_options:
                    raise ValueError("launch_options cannot be used with an external connection")
                if browser != record.browser_name:
                    raise ValueError(f"connection {connection!r} uses {record.browser_name}")
            try:
                native_context, cancelled = await _shielded(
                    record.browser.new_context(**context_options)
                )
            except Exception as exc:
                if not record.external and not self._browser_alive(record):
                    self._forget_connection(record)
                raise BrowserError(f"unable to create browser context: {exc}") from exc
            if self._closed or cancelled:
                with suppress(Exception):
                    await _shielded(native_context.close())
                if cancelled:
                    raise asyncio.CancelledError
                raise BrowserError("browser manager is closed")
            self._contexts[key] = _Context(
                key=key,
                name=name,
                owner=owner,
                shared=shared,
                context=native_context,
                browser_name=browser,
                connection_key=record.key,
                signature=signature,
                har_path=(
                    Path(context_options["record_har_path"])
                    if context_options.get("record_har_path") is not None
                    else None
                ),
                har_content=(
                    str(context_options["record_har_content"])
                    if context_options.get("record_har_content") is not None
                    else None
                ),
            )
            on_close = getattr(native_context, "on", None)
            if callable(on_close):
                on_close("close", lambda *_args: self._context_closed(key, native_context))
            return native_context

    def _context_closed(self, key: tuple[object, str], context: Any) -> None:
        item = self._contexts.get(key)
        if item is not None and item.context is context:
            try:
                self._finalize_context_artifacts(item)
            except Exception:
                return
            self._forget_context(key, context)

    def _forget_context(self, key: tuple[object, str], context: Any) -> None:
        item = self._contexts.get(key)
        if item is not None and item.context is context:
            self._contexts.pop(key, None)

    @staticmethod
    def _finalize_context_artifacts(item: _Context) -> None:
        if item.har_path is None:
            return
        temporary = Path(f"{item.har_path}.tmp")
        if not temporary.exists():
            return
        try:
            with zipfile.ZipFile(temporary) as archive:
                if item.har_path.suffix.lower() == ".zip":
                    BrowserTools._atomic_write(item.har_path, temporary.read_bytes())
                else:
                    member = next(
                        (name for name in archive.namelist() if name.endswith("har.har")), None
                    )
                    if member is None:
                        raise RuntimeError("Playwright HAR archive has no har.har member")
                    content = archive.read(member)
                    json.loads(content.decode("utf-8"))
                    BrowserTools._atomic_write(item.har_path, content)
                    if item.har_content == "attach":
                        for resource in archive.infolist():
                            if resource.filename == member or resource.is_dir():
                                continue
                            destination = (item.har_path.parent / resource.filename).resolve()
                            try:
                                destination.relative_to(item.har_path.parent.resolve())
                            except ValueError as exc:
                                raise RuntimeError(
                                    f"Playwright HAR resource escapes its destination: "
                                    f"{resource.filename}"
                                ) from exc
                            BrowserTools._atomic_write(destination, archive.read(resource))
        except (OSError, ValueError, UnicodeDecodeError, zipfile.BadZipFile) as exc:
            raise RuntimeError(f"unable to finalize HAR {item.har_path}: {exc}") from exc

        temporary.unlink()

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        output = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
        fd = None
        try:
            fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as file:
                fd = None
                file.write(content)
            os.replace(output, path)
        finally:
            if fd is not None:
                with suppress(OSError):
                    os.close(fd)
            with suppress(FileNotFoundError):
                output.unlink()

    @staticmethod
    def _browser_alive(record: _Connection) -> bool:
        connected = getattr(record.browser, "is_connected", None)
        return True if not callable(connected) else bool(connected())

    def _forget_connection(self, record: _Connection) -> None:
        self._connections.pop(record.key, None)
        for key, item in list(self._contexts.items()):
            if item.connection_key == record.key:
                self._contexts.pop(key, None)

    async def connect(
        self,
        endpoint: str,
        *,
        protocol: str = "playwright",
        browser: str = "chromium",
        name: str = "default",
        shared: bool = False,
        **options: Any,
    ) -> Any:
        """Connect to an externally managed Playwright or CDP browser."""

        if not isinstance(endpoint, str) or not endpoint:
            raise TypeError("endpoint must be a non-empty string")
        if protocol not in {"playwright", "cdp"}:
            raise ValueError("protocol must be 'playwright' or 'cdp'")
        if type(shared) is not bool:
            raise TypeError("shared must be a boolean")
        name = self._name(name)
        owner = self._owner()
        key = self._connection_key(_SHARED if shared else owner, name)
        signature = self._json_signature(
            {
                "endpoint": endpoint,
                "protocol": protocol,
                "browser": browser,
                "shared": shared,
                "options": options,
            }
        )
        async with self._connection_lock:
            if self._closed:
                raise BrowserError("browser manager is closed")
            existing = self._connections.get(key)
            if existing is not None:
                if self._browser_alive(existing):
                    if existing.signature != signature:
                        raise RuntimeError(
                            f"browser connection {name!r} settings changed; close it first"
                        )
                    return existing.browser
                self._forget_connection(existing)
            driver = await self._driver()
            if self._closed:
                raise BrowserError("browser manager is closed")
            try:
                if protocol == "cdp":
                    if browser != "chromium":
                        raise ValueError("CDP connections require chromium")
                    native, cancelled = await _shielded(
                        driver.chromium.connect_over_cdp(endpoint, **options)
                    )
                else:
                    native, cancelled = await _shielded(
                        self._browser_type(driver, browser).connect(endpoint, **options)
                    )
            except Exception as exc:
                if isinstance(exc, ValueError):
                    raise
                raise BrowserError(f"unable to connect to browser: {exc}") from exc
            self._connections[key] = _Connection(
                key=key,
                name=name,
                owner=_SHARED if shared else owner,
                browser_name=browser,
                browser=native,
                external=True,
                protocol=protocol,
                endpoint=endpoint,
                signature=signature,
            )
            if cancelled or self._closed:
                with suppress(Exception):
                    await _shielded(native.close())
                    self._connections.pop(key, None)
                if cancelled:
                    raise asyncio.CancelledError
                raise BrowserError("browser manager is closed")
            return native

    def list(self) -> list[dict[str, Any]]:
        owner = self._owner()
        result: list[dict[str, Any]] = []
        for item in self._connections.values():
            if item.owner not in {owner, _SHARED}:
                continue
            result.append(
                {
                    "type": "connection",
                    "name": item.name,
                    "owner": "shared" if item.owner is _SHARED else item.owner,
                    "browser": item.browser_name,
                    "protocol": item.protocol,
                    "external": item.external,
                    "connected": self._browser_alive(item),
                }
            )
        for item in self._contexts.values():
            if item.owner != owner and not item.shared:
                continue
            result.append(
                {
                    "type": "context",
                    "name": item.name,
                    "owner": item.owner,
                    "shared": item.shared,
                    "browser": item.browser_name,
                    "closed": bool(getattr(item.context, "is_closed", lambda: False)()),
                }
            )
        return sorted(result, key=lambda value: (value["type"], value["name"], str(value["owner"])))

    async def close(
        self,
        name: str | None = None,
        *,
        shared: bool = False,
        connection: bool = False,
    ) -> dict[str, int]:
        """Close owned contexts, or a named connection and its contexts."""

        owner = self._owner()
        if name is not None:
            name = self._name(name)
        errors: list[BaseException] = []
        cancelled = False
        closed_contexts = 0
        closed_connections = 0
        async with self._context_lock:
            async with self._connection_lock:
                context_items = [
                    item
                    for item in self._contexts.values()
                    if (
                        name is None
                        or item.name == name
                        or (
                            connection
                            and any(
                                record.key == item.connection_key and record.name == name
                                for record in self._connections.values()
                            )
                        )
                    )
                    and (item.owner == owner or (shared and item.shared))
                ]
                orphan_candidates = {item.connection_key for item in context_items}
                for item in context_items:
                    try:
                        _, operation_cancelled = await _shielded(item.context.close())
                    except asyncio.CancelledError:
                        cancelled = True
                    except BaseException as exc:
                        errors.append(RuntimeError(f"context {item.name!r} close failed: {exc}"))
                    else:
                        cancelled |= operation_cancelled
                        try:
                            self._finalize_context_artifacts(item)
                        except BaseException as exc:
                            errors.append(
                                RuntimeError(f"context {item.name!r} artifact flush failed: {exc}")
                            )
                        else:
                            self._forget_context(item.key, item.context)
                            closed_contexts += 1
                candidate_keys = set(orphan_candidates)
                if connection or name is None:
                    candidate_keys.update(
                        item.key
                        for item in self._connections.values()
                        if (name is None or item.name == name)
                        and (item.owner == owner or (shared and item.owner is _SHARED))
                    )
                for key in candidate_keys:
                    item = self._connections.get(key)
                    if item is None:
                        continue
                    if any(context.connection_key == key for context in self._contexts.values()):
                        continue
                    try:
                        _, operation_cancelled = await _shielded(item.browser.close())
                    except asyncio.CancelledError:
                        cancelled = True
                    except BaseException as exc:
                        errors.append(
                            RuntimeError(f"browser connection {item.name!r} close failed: {exc}")
                        )
                    else:
                        cancelled |= operation_cancelled
                        self._connections.pop(key, None)
                        closed_connections += 1
        if errors:
            raise BrowserError("; ".join(str(error) for error in errors)) from errors[0]
        if cancelled:
            raise asyncio.CancelledError
        return {"contexts": closed_contexts, "connections": closed_connections}

    async def save_state(
        self,
        context: Any = None,
        *,
        name: str = "default",
        path: str | os.PathLike[str] | None = None,
        shared: bool = False,
        **options: Any,
    ) -> dict[str, Any]:
        if isinstance(context, str):
            name, context = context, None
        if context is None:
            context = await self.context(name, shared=shared)
        options.setdefault("indexed_db", True)
        state = await context.storage_state(**options)
        target = self._state_path(name, path, shared=shared)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{secrets.token_hex(6)}.tmp")
        fd = None
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                fd = None
                file.write(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
            os.replace(temporary, target)
        finally:
            if fd is not None:
                with suppress(OSError):
                    os.close(fd)
            with suppress(FileNotFoundError):
                temporary.unlink()
        return {"name": name, "path": self._display_path(target), "state": state}

    async def load_state(
        self,
        name: str = "default",
        *,
        path: str | os.PathLike[str] | None = None,
        shared: bool = False,
    ) -> dict[str, Any]:
        target = self._state_path(name, path, shared=shared)
        try:
            state = json.loads(await asyncio.to_thread(target.read_text, encoding="utf-8"))
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"browser state does not exist: {target}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid browser state: {target}") from exc
        if not isinstance(state, dict):
            raise ValueError("browser state must be a JSON object")
        return state

    def _state_path(self, name: str, path: str | os.PathLike[str] | None, *, shared: bool) -> Path:
        name = self._name(name)
        if path is not None:
            return self._path(path)
        owner = self._owner()
        if shared:
            state_root = (self.workspace / ".mypr" / "browser" / "state" / "shared").resolve()
            target = state_root / f"{name}.json"
        else:
            owner_hash = hashlib.sha256(owner.encode("utf-8")).hexdigest()
            state_root = (self.workspace / ".mypr" / "browser" / "state" / "private").resolve()
            target = state_root / owner_hash / f"{name}.json"
        target = target.resolve()
        try:
            target.relative_to((self.workspace / ".mypr" / "browser" / "state").resolve())
        except ValueError as exc:
            raise ValueError("browser state path escapes the workspace") from exc
        return target

    def _display_path(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.workspace))
        except ValueError:
            return str(path)

    async def screenshot(
        self,
        page: Any = None,
        path: str | os.PathLike[str] | None = None,
        *,
        name: str = "default",
        shared: bool = False,
        **options: Any,
    ) -> Any:
        if "path" in options:
            raise TypeError("pass screenshot path as the path argument")
        if isinstance(page, str) and path is None:
            name, page = page, None
        if page is None:
            context = await self.context(name, shared=shared)
            pages = getattr(context, "pages", [])
            if not pages:
                page = await context.new_page()
            else:
                page = pages[0]
        image_type = options.get("type", "png")
        if path is None:
            suffix = ".jpg" if image_type == "jpeg" else ".png"
            path = (
                self.workspace
                / ".mypr"
                / "artifacts"
                / f"screenshot-{name}-{time.time_ns()}{suffix}"
            )
        target = self._path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        await page.screenshot(path=str(target), **options)
        fs = self._fs
        if fs is None:
            from .filesystem import Filesystem

            fs = Filesystem(self.workspace)
        return await fs.image(self._display_path(target))

    async def aclose(self) -> None:
        if self._cleanup_task is None:
            self._closed = True
            self._cleanup_task = asyncio.create_task(self._cleanup())
        _, cancelled = await _shielded(self._cleanup_task)
        if cancelled:
            raise asyncio.CancelledError

    async def _cleanup(self) -> None:
        errors: list[BaseException] = []
        cancelled = False
        driver_task = self._driver_task
        if driver_task is not None and not driver_task.done():
            try:
                _, operation_cancelled = await _shielded(driver_task)
                cancelled |= operation_cancelled
            except BaseException as exc:
                errors.append(RuntimeError(f"Playwright startup cleanup failed: {exc}"))
        async with self._context_lock:
            async with self._connection_lock:
                contexts = list(self._contexts.values())
                for item in contexts:
                    try:
                        _, operation_cancelled = await _shielded(item.context.close())
                    except asyncio.CancelledError:
                        cancelled = True
                    except BaseException as exc:
                        errors.append(RuntimeError(f"context {item.name!r} close failed: {exc}"))
                    else:
                        cancelled |= operation_cancelled
                        try:
                            self._finalize_context_artifacts(item)
                        except BaseException as exc:
                            errors.append(
                                RuntimeError(f"context {item.name!r} artifact flush failed: {exc}")
                            )
                        else:
                            self._forget_context(item.key, item.context)
                connections = list(self._connections.values())
                for item in connections:
                    try:
                        _, operation_cancelled = await _shielded(item.browser.close())
                    except asyncio.CancelledError:
                        cancelled = True
                    except BaseException as exc:
                        errors.append(
                            RuntimeError(f"browser connection {item.name!r} close failed: {exc}")
                        )
                    else:
                        cancelled |= operation_cancelled
                        self._connections.pop(item.key, None)
                if self._playwright is not None:
                    try:
                        _, operation_cancelled = await _shielded(self._playwright.stop())
                    except asyncio.CancelledError:
                        cancelled = True
                    except BaseException as exc:
                        errors.append(RuntimeError(f"Playwright cleanup failed: {exc}"))
                    else:
                        cancelled |= operation_cancelled
                        self._playwright = None
        if errors:
            raise BrowserError("; ".join(str(error) for error in errors)) from errors[0]
        if cancelled:
            raise asyncio.CancelledError


__all__ = ["BrowserError", "BrowserTools"]
