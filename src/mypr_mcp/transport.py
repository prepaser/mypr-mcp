import asyncio
import fcntl
import hashlib
import json
import os
import stat
from contextlib import asynccontextmanager
from pathlib import Path

MAX_MESSAGE = 32 * 1024 * 1024


def workspace_id(workspace: Path) -> str:
    info = workspace.stat()
    if not stat.S_ISDIR(info.st_mode):
        raise NotADirectoryError(workspace)
    return f"{info.st_dev:x}:{info.st_ino:x}"


def socket_path(workspace: Path) -> Path:
    root = Path(os.environ.get("XDG_RUNTIME_DIR", f"/tmp/mypr-{os.getuid()}")) / "mypr"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_uid != os.getuid():
        raise PermissionError(root)
    root.chmod(0o700)
    key = hashlib.sha256(f"workspace:{workspace_id(workspace)}".encode()).hexdigest()[:32]
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


def manager_running(workspace: Path) -> bool:
    try:
        lock = (workspace / ".mypr" / "manager.lock").open("r+")
    except FileNotFoundError:
        return False
    with lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(lock, fcntl.LOCK_UN)
    return False


async def find_runtime(workspace: Path):
    identity = workspace_id(workspace)
    primary = socket_path(workspace)
    candidates = [(primary, None)]
    try:
        metadata = json.loads((workspace / ".mypr" / "runtime.json").read_text())
        saved_path = Path(metadata["socket"])
        saved_identity = metadata.get("workspace_id")
        matches = saved_identity == identity
        if saved_identity is None:
            matches = await asyncio.to_thread(workspace.samefile, metadata["workspace"])
        if matches and saved_path != primary and manager_running(workspace):
            candidates.append((saved_path, metadata))
    except OSError, ValueError, KeyError, TypeError:
        pass
    for path, metadata in candidates:
        try:
            state = await asyncio.wait_for(rpc(path, op="status"), 5)
        except OSError, ConnectionError, TimeoutError:
            continue
        actual = state.get("workspace_id")
        if actual == identity:
            return path, state
        if (
            actual is None
            and metadata is not None
            and state.get("generation") == metadata.get("generation")
        ):
            return path, state
        raise RuntimeError("Workspace manager identity mismatch; refusing to attach")
    return None


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
    connection_id: str,
):
    """Attach a client and keep its manager connection open until shutdown."""
    reader, writer = await asyncio.open_unix_connection(str(path), limit=MAX_MESSAGE)
    try:
        writer.write(
            json.dumps(
                {
                    "op": "attach",
                    "connection_id": connection_id,
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
