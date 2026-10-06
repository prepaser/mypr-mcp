"""Retryable lifecycle helpers for native HTTPX2 clients."""

from __future__ import annotations

import asyncio
from typing import Any


class RetryableTransport:
    def __init__(self, transport: Any) -> None:
        self.transport = transport
        self.closed = False
        self._lock = asyncio.Lock()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.transport, name)

    async def handle_async_request(self, request: Any) -> Any:
        return await self.transport.handle_async_request(request)

    async def __aenter__(self) -> RetryableTransport:
        await self.transport.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        async with self._lock:
            if not self.closed:
                await self.transport.__aexit__(*args)
                self.closed = True

    async def aclose(self) -> None:
        async with self._lock:
            if not self.closed:
                await self.transport.aclose()
                self.closed = True


def retryable_transports(client: Any) -> tuple[RetryableTransport, ...]:
    if not hasattr(client, "_transport") or not hasattr(client, "_mounts"):
        return ()
    transports: dict[int, RetryableTransport] = {}

    def wrap(transport: Any) -> RetryableTransport:
        key = id(transport)
        if key not in transports:
            transports[key] = RetryableTransport(transport)
        return transports[key]

    client._transport = wrap(client._transport)
    client._mounts = {
        key: wrap(transport) if transport is not None else None
        for key, transport in client._mounts.items()
    }
    return tuple(transports.values())


async def close_client(
    client: Any, transports: tuple[RetryableTransport, ...] | None = None
) -> None:
    if getattr(client, "is_closed", False) and transports:
        await close_transports(transports)
    else:
        await client.aclose()


async def close_transports(transports: tuple[RetryableTransport, ...]) -> None:
    results = await asyncio.gather(
        *(transport.aclose() for transport in transports), return_exceptions=True
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result
