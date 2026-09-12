import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

MAX_MESSAGE = 32 * 1024 * 1024


def socket_path(workspace: Path) -> Path:
    root = Path(os.environ.get("XDG_RUNTIME_DIR", f"/tmp/mypr-{os.getuid()}")) / "mypr"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_uid != os.getuid():
        raise PermissionError(root)
    root.chmod(0o700)
    key = hashlib.sha256(os.fsencode(workspace.resolve())).hexdigest()[:32]
    return root / f"{key}.sock"


async def rpc(path: Path | str, **request):
    reader, writer = await asyncio.open_unix_connection(str(path), limit=MAX_MESSAGE)
    try:
        writer.write(json.dumps(request).encode() + b"\n")
        await writer.drain()
        data = await reader.readline()
        if not data:
            raise ConnectionError("Workspace manager disconnected")
        response = json.loads(data)
        if not response["ok"]:
            raise RuntimeError(response["error"])
        return response["result"]
    finally:
        writer.close()
        await writer.wait_closed()


class Attachment:
    """A long-lived manager connection used to track one MCP client."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.closed = asyncio.Event()
        self._watcher: asyncio.Task[None] | None = None

    async def wait_closed(self) -> None:
        """Wait until the workspace manager closes this attachment."""
        await self.closed.wait()

    async def _watch(self) -> None:
        try:
            while await self.reader.read(4096):
                pass
        finally:
            self.closed.set()

    async def close(self) -> None:
        if self._watcher is not None:
            self._watcher.cancel()
            await asyncio.gather(self._watcher, return_exceptions=True)
            self._watcher = None
        self.writer.close()
        await self.writer.wait_closed()
        self.closed.set()


@asynccontextmanager
async def attachment(
    path: Path | str,
    client_id: str,
    connection_id: str,
    client_name: str | None = None,
):
    """Attach a client and keep its manager connection open until shutdown."""
    reader, writer = await asyncio.open_unix_connection(str(path), limit=MAX_MESSAGE)
    try:
        writer.write(
            json.dumps(
                {
                    "op": "attach",
                    "client_id": client_id,
                    "connection_id": connection_id,
                    "client_name": client_name,
                },
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        await writer.drain()
        data = await reader.readline()
        if not data:
            raise ConnectionError("Workspace manager disconnected during attach")
        response = json.loads(data)
        if not response.get("ok"):
            raise RuntimeError(response.get("error", "Workspace manager attach failed"))
        attached = Attachment(reader, writer)
        attached._watcher = asyncio.create_task(attached._watch())
        yield attached
    finally:
        if "attached" in locals():
            await attached.close()
        else:
            writer.close()
            await writer.wait_closed()
