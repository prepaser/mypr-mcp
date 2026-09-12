import asyncio
import hashlib
import json
import os
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
