import asyncio
import base64
import contextlib
import fcntl
import importlib.metadata
import json
import os
import signal
import sys
import time
import tomllib
import uuid
from pathlib import Path

from jupyter_client import AsyncKernelManager
from jupyter_client.kernelspec import KernelSpec

from . import __version__
from .services import MCPBridge, Shells
from .transport import MAX_MESSAGE, socket_path

TERMINAL = {"succeeded", "failed", "cancelled", "lost"}


class Runtime:
    def __init__(self, workspace):
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / ".mypr"
        self.root.mkdir(exist_ok=True)
        self.socket = socket_path(self.workspace)
        self.generation = uuid.uuid4().hex
        self.execs = {}
        self.requests = {}
        self.history_requests = {}
        self.queue = asyncio.Queue()
        self.active = None
        self.clients = set()
        self.stopping = asyncio.Event()
        self.resetting = False
        self.healthy = False
        self.km = None
        self.kc = None
        self.worker = None
        self.iopub = None
        self.monitor = None
        self.config = {}
        config = self.root / "config.toml"
        if config.exists():
            self.config = tomllib.loads(config.read_text())
        limits = self.config.get("limits", {})
        self.output_limit = int(limits.get("output_bytes", 16 * 1024 * 1024))
        self.response_limit = int(limits.get("response_bytes", 32768))
        if min(self.output_limit, self.response_limit) < 1024:
            raise ValueError("Output limits must be at least 1024 bytes")
        self.shells = Shells(self.workspace, output_limit=self.output_limit)
        self.mcp = MCPBridge(self.workspace)
        self.background = set()

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        return task

    async def prepare(self):
        for name in ["lib/ws_lib", "skills", "runs", "artifacts", "ipython", "jupyter"]:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / "lib/ws_lib/__init__.py").touch(exist_ok=True)
        ignore = self.root / ".gitignore"
        if not ignore.exists():
            ignore.write_text(
                "venv/\nruns/\nartifacts/\njobs/\nipython/\njupyter/\n*.json\n*.log\n*.lock\n"
            )
        config = self.root / "config.toml"
        if not config.exists():
            config.write_text("[mcp.servers]\n")
        py = self.root / "venv/bin/python"
        if not py.exists():
            await self.command("uv", "venv", str(self.root / "venv"), "--python", sys.executable)
        marker = self.root / "venv/.mypr-version"
        if not marker.exists() or marker.read_text() != __version__:
            specs = [f"{p}=={importlib.metadata.version(p)}" for p in ["ipykernel", "pyyaml"]]
            await self.command("uv", "pip", "install", "--python", str(py), *specs)
            marker.write_text(__version__)
        self.py = py
        for path in (self.root / "runs").glob("*.json"):
            try:
                old = json.loads(path.read_text())
                if old.get("request_id"):
                    self.history_requests[old["request_id"]] = old
                if old["state"] not in TERMINAL:
                    old.update(state="lost", error="Manager stopped before completion")
                    path.write_text(json.dumps(old))
            except ValueError, KeyError:
                continue

    async def command(self, *args):
        proc = await asyncio.create_subprocess_exec(*args, stdout=sys.stderr, stderr=sys.stderr)
        if await proc.wait():
            raise RuntimeError(f"Command failed: {args[0]}")

    async def start_kernel(self):
        env = dict(
            os.environ,
            MYPR_SOCKET=str(self.socket),
            MYPR_WORKSPACE=str(self.workspace),
            MYPR_GENERATION=self.generation,
            MYPR_PARENT_PID=str(os.getpid()),
            MYPR_OUTPUT_LIMIT=str(self.output_limit),
            PYTHONDONTWRITEBYTECODE="1",
            IPYTHONDIR=str(self.root / "ipython"),
            JUPYTER_RUNTIME_DIR=str(self.root / "jupyter"),
        )
        boot = Path(__file__).with_name("kernel_boot.py")
        self.km = AsyncKernelManager(
            autorestart=False,
            transport="ipc",
            ip=str(self.socket.with_suffix(".kernel")),
            connection_file=str(self.root / "kernel.json"),
        )
        self.km._kernel_spec = KernelSpec(
            argv=[str(self.py), str(boot), "-f", "{connection_file}"],
            display_name="mypr",
            language="python",
        )
        await self.km.start_kernel(
            cwd=str(self.workspace), env=env, stdout=sys.stderr, stderr=sys.stderr
        )
        self.kc = self.km.client()
        self.kc.start_channels()
        await self.kc.wait_for_ready(timeout=60)
        self.healthy = True
        self.iopub = asyncio.create_task(self.read_output())
        self.worker = asyncio.create_task(self.run_queue())
        self.monitor = asyncio.create_task(self.watch_kernel())
        self.write_info()

    def write_info(self):
        (self.root / "runtime.json").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "socket": str(self.socket),
                    "generation": self.generation,
                    "version": __version__,
                    "workspace": str(self.workspace),
                }
            )
        )

    def save(self, rec):
        data = {k: v for k, v in rec.items() if k not in {"done", "idle", "events"}}
        path = self.root / "runs" / f"{rec['id']}.json"
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(data))
        temp.replace(path)

    def finish(self, rec, state, error=None):
        rec.update(state=state, error=error)
        rec["done"].set()
        self.save(rec)

    async def watch_kernel(self):
        while True:
            await asyncio.sleep(1)
            if not await self.km.is_alive():
                self.healthy = False
                for rec in self.execs.values():
                    if rec["state"] not in TERMINAL:
                        self.finish(rec, "lost", "Python kernel exited; use CLI reset")
                return

    async def run_queue(self):
        while True:
            rec = await self.queue.get()
            if rec["state"] in TERMINAL:
                continue
            self.active = rec
            rec["state"] = "running"
            self.save(rec)
            try:
                msg_id = self.kc.execute(rec["code"], allow_stdin=False, stop_on_error=False)
                rec["msg_id"] = msg_id
                while True:
                    reply = await self.kc.get_shell_msg()
                    if reply.get("parent_header", {}).get("msg_id") == msg_id:
                        break
                await rec["idle"].wait()
                if not self.resetting and rec["state"] not in TERMINAL:
                    content = reply["content"]
                    self.finish(
                        rec,
                        "succeeded" if content["status"] == "ok" else "failed",
                        content.get("evalue"),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if rec["state"] not in TERMINAL:
                    self.finish(rec, "lost", str(exc))
            finally:
                self.active = None
            if self.resetting:
                await asyncio.Future()

    async def read_output(self):
        while True:
            msg = await self.kc.get_iopub_msg()
            parent = msg.get("parent_header", {}).get("msg_id")
            rec = next((r for r in self.execs.values() if r.get("msg_id") == parent), None)
            if rec is None:
                continue
            kind, content = msg["msg_type"], msg["content"]
            if kind == "status" and content["execution_state"] == "idle":
                rec["idle"].set()
                continue
            event = None
            if kind == "stream":
                event = {"type": "stream", "stream": content["name"], "text": content["text"]}
            elif kind == "error":
                if self.resetting and content.get("ename") == "ResetRequested":
                    continue
                event = {"type": "error", "text": "\n".join(content.get("traceback", []))}
            elif kind in {"execute_result", "display_data", "update_display_data"}:
                event = {"type": "result", "text": content.get("data", {}).get("text/plain", "")}
                artifacts = []
                for mime, value in content.get("data", {}).items():
                    if mime == "text/plain":
                        continue
                    binary = mime in {"image/png", "image/jpeg", "audio/wav"}
                    raw = base64.b64decode(value) if binary else json.dumps(value).encode()
                    if rec["bytes"] + len(raw) > self.output_limit:
                        rec["truncated"] = True
                        continue
                    path = self.root / "artifacts" / uuid.uuid4().hex
                    path.write_bytes(raw)
                    rec["bytes"] += len(raw)
                    artifacts.append({"mime": mime, "path": str(path)})
                event["artifacts"] = artifacts
            if event:
                self.append(rec, event)

    def append(self, rec, event):
        raw = event.get("text", "").encode()
        room = max(0, self.output_limit - rec["bytes"])
        was_truncated = rec["truncated"]
        if len(raw) > room:
            rec["truncated"] = True
        text = raw[:room].decode(errors="ignore")
        if not text and not event.get("artifacts"):
            if rec["truncated"] != was_truncated:
                self.save(rec)
            return
        step = min(1024, max(1, (self.response_limit - 256) // 12))
        with (self.root / "runs" / f"{rec['id']}.jsonl").open("a") as file:
            for start in range(0, max(1, len(text)), step):
                piece = dict(event, text=text[start : start + step])
                if start:
                    piece.pop("artifacts", None)
                rec["events"].append(piece)
                file.write(json.dumps(piece) + "\n")
        rec["bytes"] += min(len(raw), room)
        self.save(rec)

    async def poll(self, ident, cursor=0, wait_ms=1000):
        if ident not in self.execs:
            if len(ident) != 32 or any(c not in "0123456789abcdef" for c in ident):
                raise ValueError("Invalid execution ID")
            path = self.root / "runs" / f"{ident}.json"
            if not path.exists():
                raise ValueError("Unknown execution")
            rec = json.loads(path.read_text())
            events = self.root / "runs" / f"{ident}.jsonl"
            rec["events"] = (
                [json.loads(s) for s in events.read_text().splitlines()] if events.exists() else []
            )
        else:
            rec = self.execs[ident]
            if rec["state"] not in TERMINAL and wait_ms:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(rec["done"].wait(), min(30000, max(0, wait_ms)) / 1000)
        if cursor < 0 or cursor > len(rec["events"]):
            raise ValueError("Invalid output cursor")
        output, size = [], 0
        for event in rec["events"][cursor:]:
            n = len(json.dumps(event).encode())
            if output and size + n > self.response_limit:
                break
            output.append(event)
            size += n
        return {
            "exec_id": ident,
            "generation": self.generation,
            "execution_generation": rec["generation"],
            "state": rec["state"],
            "output": output,
            "cursor": cursor + len(output),
            "has_more": cursor + len(output) < len(rec["events"]),
            "truncated": rec["truncated"],
            "error": rec.get("error"),
        }

    async def dispatch(self, req):
        op = req.pop("op")
        client = req.pop("client_id", None)
        if op == "attach":
            self.clients.add(client)
            return await self.dispatch({"op": "status"})
        if op == "detach":
            self.clients.discard(client)
            return None
        if op == "status":
            return {
                "version": __version__,
                "generation": self.generation,
                "healthy": self.healthy,
                "resetting": self.resetting,
                "connections": sorted(self.clients),
                "active": self.active["id"] if self.active else None,
                "queued": [r["id"] for r in self.execs.values() if r["state"] == "queued"],
            }
        generation = req.pop("generation", None)
        if generation and generation != self.generation:
            raise RuntimeError("Expired kernel generation")
        if op == "execute":
            if not self.healthy or self.resetting:
                raise RuntimeError("Kernel unavailable; use CLI reset")
            code = req["code"]
            key = req.get("request_id")
            if key and key in self.history_requests:
                old = self.history_requests[key]
                if old["code"] != code:
                    raise ValueError("request_id already used for different code")
                return await self.poll(old["id"], wait_ms=0)
            if key and key in self.requests:
                rec = self.execs[self.requests[key]]
                if rec["code"] != code:
                    raise ValueError("request_id already used for different code")
            else:
                ident = uuid.uuid4().hex
                rec = dict(
                    id=ident,
                    generation=self.generation,
                    code=code,
                    client=client,
                    request_id=key,
                    state="queued",
                    events=[],
                    bytes=0,
                    truncated=False,
                    done=asyncio.Event(),
                    idle=asyncio.Event(),
                    created=time.time(),
                )
                self.execs[ident] = rec
                if key:
                    self.requests[key] = ident
                self.save(rec)
                self.queue.put_nowait(rec)
            return await self.poll(rec["id"], wait_ms=req.get("wait_ms", 1000))
        if op == "poll":
            return await self.poll(req["exec_id"], req.get("cursor") or 0, req.get("wait_ms", 1000))
        if op == "shell_start":
            return await self.shells.start(
                req["command"],
                req.get("cwd", str(self.workspace)),
                req.get("env", dict(os.environ)),
            )
        if op == "shell_poll":
            return await self.shells.poll(req["id"], req.get("cursor", 0))
        if op == "shell_cancel":
            return await self.shells.cancel(req["id"])
        if op == "mcp":
            return await self.mcp.dispatch(req["method"], req.get("args", {}))
        if op == "packages_add":
            import shlex

            specs = req["specs"]
            if not specs or any(not s or s.startswith("-") for s in specs):
                raise ValueError("Expected package requirements, not command options")
            command = shlex.join(["uv", "pip", "install", "--python", str(self.py), *specs])
            command += " && " + shlex.join(["uv", "pip", "freeze", "--python", str(self.py)])
            command += " > " + shlex.quote(str(self.root / "requirements.txt"))
            return await self.shells.start(command, str(self.workspace), dict(os.environ))
        if op == "reset":
            from_kernel = req.get("from_kernel", False)
            others = any(r["state"] == "queued" for r in self.execs.values())
            busy = self.active is not None and not from_kernel
            if self.resetting:
                raise RuntimeError("Reset already in progress")
            if not req.get("force", False) and (others or busy or self.shells.active):
                raise RuntimeError("Workspace has active work; pass force=True to reset")
            self.resetting = True
            current = self.active if from_kernel else None
            if from_kernel:
                self.spawn(self.reset(current))
                return {"accepted": True}
            await self.reset(None)
            return {"generation": self.generation, "reset": True}
        if op == "stop":
            if not req.get("force") and (self.active or self.shells.active):
                raise RuntimeError("Workspace has active work; pass --force")
            self.stopping.set()
            return {"stopped": True}
        raise ValueError(f"Unknown operation: {op}")

    async def reset(self, current):
        try:
            if current:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(current["idle"].wait(), 3)
            for rec in self.execs.values():
                if rec is not current and rec["state"] not in TERMINAL:
                    self.finish(rec, "cancelled", "Workspace reset")
            await self.close_kernel()
            await self.shells.close()
            await self.mcp.close()
            self.mcp = MCPBridge(self.workspace)
            self.shells = Shells(self.workspace, output_limit=self.output_limit)
            self.queue = asyncio.Queue()
            self.generation = uuid.uuid4().hex
            await self.start_kernel()
            if current:
                self.append(current, {"type": "result", "text": "Workspace reset completed"})
                self.finish(current, "succeeded")
        except Exception as exc:
            self.healthy = False
            if current:
                self.finish(current, "lost", f"Reset failed: {exc}")
            else:
                raise
        finally:
            self.resetting = False

    async def close_kernel(self):
        self.healthy = False
        for task in [self.worker, self.iopub, self.monitor]:
            if task:
                task.cancel()
        await asyncio.gather(
            *(t for t in [self.worker, self.iopub, self.monitor] if t), return_exceptions=True
        )
        if self.kc:
            self.kc.stop_channels()
        if self.km:
            with contextlib.suppress(Exception):
                await self.km.shutdown_kernel(now=True)

    async def connection(self, reader, writer):
        try:
            req = json.loads(await reader.readline())
            if req.get("op") == "mcp":
                operation = asyncio.create_task(self.dispatch(req))
                disconnected = asyncio.create_task(reader.read(1))
                try:
                    done, _ = await asyncio.wait(
                        [operation, disconnected], return_when=asyncio.FIRST_COMPLETED
                    )
                    if operation not in done:
                        operation.cancel()
                        await asyncio.gather(operation, return_exceptions=True)
                        writer.close()
                        await writer.wait_closed()
                        return
                    result = await operation
                finally:
                    disconnected.cancel()
                    await asyncio.gather(disconnected, return_exceptions=True)
            else:
                result = await self.dispatch(req)
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        try:
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()
        except ConnectionError, BrokenPipeError:
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def run(self):
        lock = (self.root / "manager.lock").open("a")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.umask(0o077)
        try:
            await self.prepare()
            self.socket.unlink(missing_ok=True)
            server = await asyncio.start_unix_server(
                self.connection, path=str(self.socket), limit=MAX_MESSAGE
            )
            try:
                await self.start_kernel()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    asyncio.get_running_loop().add_signal_handler(sig, self.stopping.set)
                async with server:
                    await self.stopping.wait()
            finally:
                server.close()
                await server.wait_closed()
                for rec in self.execs.values():
                    if rec["state"] not in TERMINAL:
                        self.finish(rec, "lost", "Manager stopped")
                await self.close_kernel()
                await self.shells.close()
                await self.mcp.close()
        finally:
            self.socket.unlink(missing_ok=True)
            lock.close()
