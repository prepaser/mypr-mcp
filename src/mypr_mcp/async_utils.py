"""Finish owned async work before propagating caller cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable


async def finish_owned[T](awaitable: Awaitable[T]) -> tuple[T, bool]:
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while True:
        try:
            return await asyncio.shield(task), cancelled
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True


async def wait_owned[T](awaitable: Awaitable[T], *, propagate: bool = True) -> T:
    result, cancelled = await finish_owned(awaitable)
    if cancelled and propagate:
        raise asyncio.CancelledError
    return result
