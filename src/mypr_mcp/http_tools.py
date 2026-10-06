"""Workspace-scoped asynchronous HTTP clients.

The manager keeps one client registry per persistent Python kernel.  Client
instances are deliberately returned as native ``httpx2.AsyncClient`` objects;
the small wrapper only owns their lifetime and provides bounded convenience
operations.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import tempfile
from collections import OrderedDict
from collections.abc import (
    AsyncIterator,
    Callable,
    Mapping,
    MutableSequence,
    MutableSet,
)
from contextlib import asynccontextmanager
from http.cookiejar import CookieJar
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .async_utils import wait_owned
from .http_transport import RetryableTransport, close_client, retryable_transports

if TYPE_CHECKING:
    import httpx2

DEFAULT_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_TIMEOUT = 30.0
_MAX_WARNINGS = 4
_MAX_WARNING_TEXT = 256
_MAX_WARNING_CLIENTS = 32
_TEMP_PREFIX_NAME_LIMIT = 32


def _temporary_prefix(name: str) -> str:
    return f".{name[:_TEMP_PREFIX_NAME_LIMIT]}."


class BodyTooLarge(RuntimeError):
    """Raised when a bounded HTTP response exceeds its configured limit."""

    def __init__(self, limit: int, received: int):
        self.limit = limit
        self.received = received
        super().__init__(f"HTTP response body exceeds the {limit} byte limit")


class _MappingSnapshot:
    __slots__ = ("items",)

    def __init__(self, items: tuple[Any, ...] = ()):
        self.items = items

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _MappingSnapshot) or len(self.items) != len(other.items):
            return False
        unmatched = list(other.items)
        for item in self.items:
            for index, candidate in enumerate(unmatched):
                if item == candidate:
                    unmatched.pop(index)
                    break
            else:
                return False
        return True


class _CookieSnapshot:
    __slots__ = ("items", "policy")

    def __init__(self, items: tuple[Any, ...], policy: Any):
        self.items = items
        self.policy = policy

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _CookieSnapshot)
            and self.policy is other.policy
            and _MappingSnapshot(self.items) == _MappingSnapshot(other.items)
        )


def _cookie_snapshot(value: Any) -> _CookieSnapshot | None:
    if isinstance(value, CookieJar):
        jar = value
    else:
        try:
            jar = getattr(value, "jar", None)
        except Exception:
            return None
    if not isinstance(jar, CookieJar):
        return None
    records = []
    for cookie in jar:
        records.append(
            (
                cookie.version,
                cookie.name,
                cookie.value,
                cookie.port,
                cookie.port_specified,
                cookie.domain,
                cookie.domain_specified,
                cookie.domain_initial_dot,
                cookie.path,
                cookie.path_specified,
                cookie.secure,
                cookie.expires,
                cookie.discard,
                cookie.comment,
                cookie.comment_url,
                tuple(sorted(cookie._rest.items())),
                cookie.rfc2109,
            )
        )
    return _CookieSnapshot(tuple(records), getattr(jar, "_policy", None))


def _client_id(identity: Callable[[], Any] | None) -> str:
    if identity is None:
        return "anonymous"
    value = identity()
    if isinstance(value, Mapping):
        value = value.get("client_id") or value.get("id")
    else:
        value = getattr(value, "id", value)
    return str(value or "anonymous")


def _check_limit(max_bytes: int | None) -> None:
    if max_bytes is not None and (isinstance(max_bytes, bool) or not isinstance(max_bytes, int)):
        raise TypeError("max_bytes must be a non-negative integer or None")
    if max_bytes is not None and max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")


def _check_url(url: str) -> None:
    if not isinstance(url, str) or not url.strip():
        raise ValueError("url must be a non-empty string")


class HTTPTools:
    """Manage named native HTTPX2 clients for one workspace kernel."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        identity: Callable[[], Any] | None = None,
        ensure_dependencies: Callable[..., Any] | None = None,
    ):
        self.workspace = Path(workspace).resolve()
        self._identity = identity
        self._ensure_dependencies = ensure_dependencies
        self._clients: dict[tuple[str, str, str], Any] = {}
        self._options: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._transports: dict[int, tuple[RetryableTransport, ...]] = {}
        self._closed = False
        self._shutdown_task: asyncio.Task[None] | None = None
        self._html = None
        self._download_warnings: OrderedDict[str, list[dict[str, str]]] = OrderedDict()

    async def _ensure(self, *names: str) -> dict[str, Any]:
        if not names or self._ensure_dependencies is None:
            return {}
        result = self._ensure_dependencies(*names, automatic=True)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, dict) else {}

    @property
    def last_warnings(self) -> list[dict[str, str]]:
        """Return bounded warnings from the current client's most recent download."""

        warnings = self._download_warnings.get(_client_id(self._identity), ())
        return [dict(warning) for warning in warnings]

    @staticmethod
    def _warning(
        warnings: list[dict[str, str]], code: str, error: BaseException | str
    ) -> None:
        if len(warnings) >= _MAX_WARNINGS:
            return
        text = error if isinstance(error, str) else str(error).strip()
        warnings.append(
            {"code": code[:128], "text": (text or error.__class__.__name__)[:_MAX_WARNING_TEXT]}
        )

    def _record_download_warnings(self, client_id: str, warnings: list[dict[str, str]]) -> None:
        self._download_warnings[client_id] = [dict(warning) for warning in warnings]
        self._download_warnings.move_to_end(client_id)
        while len(self._download_warnings) > _MAX_WARNING_CLIENTS:
            self._download_warnings.popitem(last=False)

    async def extract_html(
        self,
        html: str | None = None,
        *,
        url: str | None = None,
        selector: str | None = None,
        include_structure: bool = False,
        max_bytes: int = 32 * 1024,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Extract a bounded, paged main-text and link view from HTML."""
        if self._closed:
            raise RuntimeError("HTTP service is closed")
        from .html_tools import HTMLExtractor

        if self._html is None:
            self._html = HTMLExtractor(self)
        return await self._html.extract_html(
            html,
            url=url,
            selector=selector,
            include_structure=include_structure,
            max_bytes=max_bytes,
            cursor=cursor,
        )

    async def read_html(
        self,
        url: str | None = None,
        *,
        name: str = "default",
        shared: bool = False,
        max_input_bytes: int = DEFAULT_MAX_BYTES,
        max_bytes: int = 32 * 1024,
        cursor: str | None = None,
        selector: str | None = None,
        include_structure: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Fetch HTML with a named client, then extract a bounded paged view."""
        from .html_tools import MAX_INPUT_BYTES, _validate_output_limit

        _validate_output_limit(max_bytes)
        if cursor is not None:
            if url is not None or selector is not None or kwargs:
                raise ValueError(
                    "url, selector, and request options cannot be combined with cursor"
                )
            return await self.extract_html(
                None,
                include_structure=include_structure,
                max_bytes=max_bytes,
                cursor=cursor,
            )
        if not isinstance(url, str) or not url:
            raise ValueError("url must be a non-empty string")
        if (
            not isinstance(max_input_bytes, int)
            or isinstance(max_input_bytes, bool)
            or not 1 <= max_input_bytes <= MAX_INPUT_BYTES
        ):
            raise ValueError(f"max_input_bytes must be between 1 and {MAX_INPUT_BYTES}")
        response = await self.get(
            url,
            name=name,
            shared=shared,
            max_bytes=max_input_bytes,
            **kwargs,
        )
        response.raise_for_status()
        return await self.extract_html(
            response.text,
            url=str(response.url),
            selector=selector,
            include_structure=include_structure,
            max_bytes=max_bytes,
        )

    def _key(self, name: str, shared: bool) -> tuple[str, str, str]:
        if not isinstance(name, str) or not name:
            raise ValueError("HTTP client name must be a non-empty string")
        if shared:
            return ("shared", "", name)
        return ("client", _client_id(self._identity), name)

    @staticmethod
    def _options_with_defaults(options: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(options)
        result.setdefault("timeout", DEFAULT_TIMEOUT)
        return result

    @classmethod
    def _snapshot_option(cls, value: Any, seen: dict[int, Any] | None = None) -> Any:
        """Copy mutable option containers while retaining native object identity."""

        if seen is None:
            seen = {}
        identity = id(value)
        if identity in seen:
            return seen[identity]
        if isinstance(value, bytearray):
            return bytearray(value)
        cookie_snapshot = _cookie_snapshot(value)
        if cookie_snapshot is not None:
            seen[identity] = cookie_snapshot
            return cookie_snapshot
        if isinstance(value, Mapping):
            multi_items = getattr(value, "multi_items", None)
            pairs = multi_items() if callable(multi_items) else value.items()
            result = _MappingSnapshot()
            seen[identity] = result
            result.items = tuple(
                (cls._snapshot_option(key, seen), cls._snapshot_option(item, seen))
                for key, item in pairs
            )
            return result
        if isinstance(value, MutableSequence):
            result = []
            seen[identity] = result
            result.extend(cls._snapshot_option(item, seen) for item in value)
            return result
        if isinstance(value, tuple):
            result = tuple(cls._snapshot_option(item, seen) for item in value)
            seen[identity] = result
            return result
        if isinstance(value, MutableSet):
            result = {cls._snapshot_option(item, seen) for item in value}
            seen[identity] = result
            return result
        return value

    @classmethod
    def _snapshot_options(cls, options: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: cls._snapshot_option(value)
            for key, value in options.items()
        }

    @staticmethod
    def _needs_socksio(options: Mapping[str, Any]) -> bool:
        for key in ("proxy", "proxies"):
            value = options.get(key)
            values = value.values() if isinstance(value, Mapping) else (value,)
            for item in values:
                if isinstance(item, str) and item.lower().split(":", 1)[0] in {
                    "socks4",
                    "socks4a",
                    "socks5",
                    "socks5h",
                }:
                    return True
                scheme = getattr(item, "scheme", None)
                if scheme is None:
                    scheme = getattr(getattr(item, "url", None), "scheme", None)
                if isinstance(scheme, str) and scheme.lower() in {
                    "socks4",
                    "socks4a",
                    "socks5",
                    "socks5h",
                }:
                    return True
        proxy = options.get("proxy")
        if (
            options.get("trust_env", True)
            and proxy is None
            and options.get("transport") is None
        ):
            try:
                from urllib.request import getproxies

                proxies = getproxies()
            except (OSError, ValueError):
                proxies = {}
            if not isinstance(proxies, Mapping):
                proxies = {}
            for key, value in proxies.items():
                if key.lower() in {"no", "no_proxy"} or not isinstance(value, str):
                    continue
                scheme = value.strip().split(":", 1)[0].lower()
                if scheme in {"socks4", "socks4a", "socks5", "socks5h"}:
                    return True
        return False

    async def _prepare_client_dependencies(self, options: Mapping[str, Any]) -> None:
        names = ["httpx2"]
        if options.get("http2") is True:
            names.append("h2")
        if self._needs_socksio(options):
            names.append("socksio")
        await self._ensure(*names)

    def _existing_client(
        self,
        key: tuple[str, str, str],
        name: str,
        requested: Mapping[str, Any],
        has_options: bool,
    ) -> Any | None:
        existing = self._clients.get(key)
        if existing is not None and existing.is_closed:
            if any(not transport.closed for transport in self._transports.get(id(existing), ())):
                raise RuntimeError(
                    f"HTTP client {name!r} cleanup is incomplete; "
                    "await ws.http.close(...) before reconfiguring it"
                )
            self._clients.pop(key, None)
            self._options.pop(key, None)
            self._transports.pop(id(existing), None)
            existing = None
        if existing is not None:
            if has_options and self._options[key] != self._snapshot_options(requested):
                raise RuntimeError(
                    f"HTTP client {name!r} already exists with different options; "
                    "await ws.http.close(...) before reconfiguring it"
                )
            return existing
        return None

    async def client(
        self,
        name: str = "default",
        *,
        shared: bool = False,
        **options: Any,
    ) -> httpx2.AsyncClient:
        """Return a native named ``httpx2.AsyncClient``.

        The returned object is an intentional escape hatch.  Requests made
        through it are not subject to :meth:`request`'s body limit.
        """

        if self._closed:
            raise RuntimeError("HTTP service is closed")
        requested = self._options_with_defaults(options)
        key = self._key(name, shared)
        existing = self._existing_client(key, name, requested, bool(options))
        if existing is not None:
            return existing

        await self._prepare_client_dependencies(requested)
        if self._closed:
            raise RuntimeError("HTTP service is closed")
        existing = self._existing_client(key, name, requested, bool(options))
        if existing is not None:
            return existing
        import httpx2

        option_snapshot = self._snapshot_options(requested)
        created = httpx2.AsyncClient(**requested)
        self._transports[id(created)] = retryable_transports(created)
        self._clients[key] = created
        self._options[key] = option_snapshot
        return created

    async def _get_client(
        self,
        name: str,
        shared: bool,
        options: Mapping[str, Any] | None = None,
    ) -> httpx2.AsyncClient:
        return await self.client(name, shared=shared, **(dict(options) if options else {}))

    async def request(
        self,
        method: str,
        url: str,
        *,
        name: str = "default",
        shared: bool = False,
        max_bytes: int | None = DEFAULT_MAX_BYTES,
        **kwargs: Any,
    ) -> httpx2.Response:
        """Send a request and return a fully read native response."""

        _check_url(url)
        _check_limit(max_bytes)
        client = await self._get_client(name, shared)
        async with client.stream(method, url, **kwargs) as response:
            body = bytearray()
            async for chunk in response.aiter_bytes():
                received = len(body) + len(chunk)
                if max_bytes is not None and received > max_bytes:
                    raise BodyTooLarge(max_bytes, received)
                body.extend(chunk)
            # Keep the native response metadata and cache the decoded body.
            response._content = bytes(body)
            return response

    async def get(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("DELETE", url, **kwargs)

    async def head(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("HEAD", url, **kwargs)

    async def options(self, url: str, **kwargs: Any) -> httpx2.Response:
        return await self.request("OPTIONS", url, **kwargs)

    @asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        name: str = "default",
        shared: bool = False,
        **kwargs: Any,
    ) -> AsyncIterator[httpx2.Response]:
        """Yield a native streaming response owned by the context manager."""

        _check_url(url)
        client = await self._get_client(name, shared)
        async with client.stream(method, url, **kwargs) as response:
            yield response

    async def download(
        self,
        url: str,
        path: str | os.PathLike[str],
        *,
        overwrite: bool = False,
        max_bytes: int | None = DEFAULT_DOWNLOAD_MAX_BYTES,
        name: str = "default",
        shared: bool = False,
        **kwargs: Any,
    ) -> Path:
        """Stream a response into an atomically committed workspace file."""

        _check_url(url)
        _check_limit(max_bytes)
        warning_client_id = _client_id(self._identity)
        warnings: list[dict[str, str]] = []
        target = self._path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not overwrite and (target.exists() or target.is_symlink()):
            raise FileExistsError(target)

        temporary: Path | None = None
        try:
            client = await self._get_client(name, shared)
            async with client.stream("GET", url, **kwargs) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=target.parent,
                    prefix=_temporary_prefix(target.name),
                    delete=False,
                ) as handle:
                    temporary = Path(handle.name)
                    received = 0
                    async for chunk in response.aiter_bytes():
                        received += len(chunk)
                        if max_bytes is not None and received > max_bytes:
                            raise BodyTooLarge(max_bytes, received)
                        await _run_blocking(handle.write, chunk)
                    await _run_blocking(handle.flush)
                    await _run_blocking(os.fsync, handle.fileno())
            if overwrite:
                os.replace(temporary, target)
                temporary = None
            else:
                os.link(temporary, target)
                committed_temporary = temporary
                temporary = None
                try:
                    os.unlink(committed_temporary)
                except OSError as exc:
                    self._warning(warnings, "download_cleanup_failed", exc)
            self._record_download_warnings(warning_client_id, warnings)
            return target
        finally:
            if temporary is not None:
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()

    def _path(self, path: str | os.PathLike[str]) -> Path:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.workspace / candidate
        return candidate.resolve(strict=False)

    async def close(self, name: str = "default", *, shared: bool = False) -> None:
        if self._closed:
            return
        key = self._key(name, shared)
        client = self._clients.get(key)
        if client is not None:
            await _wait_uncancelled(asyncio.create_task(self._close_client(client)))
            if self._clients.get(key) is client:
                self._clients.pop(key, None)
                self._options.pop(key, None)

    async def aclose(self) -> None:
        shutdown = self._shutdown_task
        if shutdown is not None and shutdown.done() and (
            shutdown.cancelled() or shutdown.exception() is not None
        ):
            shutdown = None
        self._closed = True
        if self._html is not None:
            self._html.clear()
        if shutdown is None:
            shutdown = asyncio.create_task(self._close_clients())
            self._shutdown_task = shutdown
        await _wait_uncancelled(shutdown)

    async def _close_client(self, client: Any) -> None:
        transports = self._transports.get(id(client))
        await close_client(client, transports)
        self._transports.pop(id(client), None)

    async def _close_clients(self) -> None:
        clients = tuple(self._clients.items())
        results = await asyncio.gather(
            *(self._close_client(client) for _, client in clients), return_exceptions=True
        )
        failures = []
        for (key, client), result in zip(clients, results, strict=True):
            if isinstance(result, BaseException):
                failures.append(result)
            elif self._clients.get(key) is client:
                self._clients.pop(key, None)
                self._options.pop(key, None)
        if failures:
            raise failures[0]


async def _run_blocking(function: Callable[..., Any], *args: Any) -> Any:
    """Run one file operation and finish it before propagating cancellation."""

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    return await _wait_uncancelled(task)


async def _wait_uncancelled(task: asyncio.Task[Any]) -> Any:
    return await wait_owned(task)


__all__ = ["BodyTooLarge", "HTTPTools"]
