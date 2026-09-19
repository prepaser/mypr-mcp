"""Workspace-scoped asynchronous HTTP clients.

The manager keeps one client registry per persistent Python kernel.  Client
instances are deliberately returned as native ``httpx2.AsyncClient`` objects;
the small wrapper only owns their lifetime and provides bounded convenience
operations.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx2

DEFAULT_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_TIMEOUT = 30.0


class BodyTooLarge(RuntimeError):
    """Raised when a bounded HTTP response exceeds its configured limit."""

    def __init__(self, limit: int, received: int):
        self.limit = limit
        self.received = received
        super().__init__(f"HTTP response body exceeds the {limit} byte limit")


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


class HTTPTools:
    """Manage named native HTTPX2 clients for one workspace kernel."""

    def __init__(
        self, workspace: str | os.PathLike[str], identity: Callable[[], Any] | None = None
    ):
        self.workspace = Path(workspace).resolve()
        self._identity = identity
        self._clients: dict[tuple[str, str, str], Any] = {}
        self._options: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._closed = False
        self._shutdown_task: asyncio.Task[None] | None = None

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

    def client(
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
        import httpx2

        key = self._key(name, shared)
        requested = self._options_with_defaults(options)
        existing = self._clients.get(key)
        if existing is not None and existing.is_closed:
            self._clients.pop(key, None)
            self._options.pop(key, None)
            existing = None
        if existing is not None:
            if options and self._options[key] != requested:
                raise RuntimeError(
                    f"HTTP client {name!r} already exists with different options; "
                    "await ws.http.close(...) before reconfiguring it"
                )
            else:
                return existing

        created = httpx2.AsyncClient(**requested)
        self._clients[key] = created
        self._options[key] = requested
        return created

    def _get_client(
        self,
        name: str,
        shared: bool,
        options: Mapping[str, Any] | None = None,
    ) -> httpx2.AsyncClient:
        return self.client(name, shared=shared, **(dict(options) if options else {}))

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

        _check_limit(max_bytes)
        client = self._get_client(name, shared)
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

        client = self._get_client(name, shared)
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

        _check_limit(max_bytes)
        target = self._path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not overwrite and (target.exists() or target.is_symlink()):
            raise FileExistsError(target)

        temporary: Path | None = None
        try:
            client = self._get_client(name, shared)
            async with client.stream("GET", url, **kwargs) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(
                    mode="wb", dir=target.parent, prefix=f".{target.name}.", delete=False
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
            else:
                os.link(temporary, target)
                os.unlink(temporary)
            temporary = None
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
            await _wait_uncancelled(asyncio.create_task(client.aclose()))
            if self._clients.get(key) is client:
                self._clients.pop(key, None)
                self._options.pop(key, None)

    async def aclose(self) -> None:
        if self._shutdown_task is not None:
            await _wait_uncancelled(self._shutdown_task)
            return
        self._closed = True
        clients = tuple(self._clients.values())
        self._clients.clear()
        self._options.clear()
        self._shutdown_task = asyncio.create_task(_close_all(clients))
        await _wait_uncancelled(self._shutdown_task)


async def _run_blocking(function: Callable[..., Any], *args: Any) -> Any:
    """Run one file operation and finish it before propagating cancellation."""

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    return await _wait_uncancelled(task)


async def _wait_uncancelled(task: asyncio.Task[Any]) -> Any:
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            if task.done():
                result = task.result()
                break
        else:
            break
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _close_all(clients: tuple[Any, ...]) -> None:
    results = await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result


__all__ = ["BodyTooLarge", "HTTPTools"]
