"""Shared one-shot Unix RPC exchange mechanics."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable


async def exchange(
    path: str | os.PathLike[str], encode: Callable[[], bytes], limit: int
) -> bytes:
    reader, writer = await asyncio.open_unix_connection(str(path), limit=limit)
    try:
        writer.write(encode())
        await writer.drain()
        return await reader.readline()
    finally:
        writer.close()
        await writer.wait_closed()
