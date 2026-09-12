import asyncio
import base64
import contextlib
import fcntl
import importlib.metadata
import json
import os
import re
import signal
import sys
import time
import tomllib
import uuid
from pathlib import Path

from jupyter_client import AsyncKernelManager
from jupyter_client.kernelspec import KernelSpec

from . import __version__
from .history import History
from .services import MCPBridge, Shells
from .transport import MAX_MESSAGE, socket_path, workspace_id

TERMINAL = {"succeeded", "failed", "cancelled", "lost"}
MCP_MUTATIONS = {"configure", "remove", "restart", "reload"}


class Runtime:
    def __init__(self, workspace):
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / ".mypr"
        self.root.mkdir(exist_ok=True)
        self.workspace_id = workspace_id(self.workspace)
        self.socket = socket_path(self.workspace)
        self.generation = uuid.uuid4().hex
        self.execs = {}
        self.requests = {}
        self.history_requests = {}
        self.queue = asyncio.Queue()
        self.active = None
        self.clients = {}
        self.attachments = {}
        self.history = None
        self.task_records = {}
        self.shell_watchers = set()
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
        self.mcp = None
        self.background = set()

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        return task

    async def prepare(self):
        self.history = History(self.workspace)
        self.history.recover()
        for name in ["lib/ws_lib", "skills", "runs", "artifacts", "ipython", "jupyter"]:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / "lib/ws_lib/__init__.py").touch(exist_ok=True)
        ignore = self.root / ".gitignore"
        if not ignore.exists():
            ignore.write_text(
                "venv/\nruns/\nartifacts/\njobs/\nipython/\njupyter/\n*.json\nhistory.sqlite3*\n*.log\n*.lock\n"
            )
        elif "history.sqlite3*" not in ignore.read_text().splitlines():
            with ignore.open("a") as file:
                file.write("\nhistory.sqlite3*\n")
        config = self.root / "config.toml"
        if not config.exists():
            config.write_text("[mcp.servers]\n")
        self.mcp = MCPBridge(self.workspace)
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
                    owner = old.get("client_id", old.get("client")) or "legacy"
                    self.history_requests[(owner, old["request_id"])] = old
                if old["state"] not in TERMINAL:
                    old.update(state="lost", error="Manager stopped before completion")
                    path.write_text(json.dumps(old))
                old.setdefault("client_id", old.get("client") or "legacy")
                self.history.record("execution", old)
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

    def workspace_available(self):
        try:
            return workspace_id(self.workspace) == self.workspace_id
        except OSError:
            return False

    def write_info(self):
        (self.root / "runtime.json").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "socket": str(self.socket),
                    "generation": self.generation,
                    "version": __version__,
                    "workspace": str(self.workspace),
                    "workspace_id": self.workspace_id,
                }
            )
        )

    def save(self, rec):
        data = {k: v for k, v in rec.items() if k not in {"done", "idle", "events"}}
        path = self.root / "runs" / f"{rec['id']}.json"
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(data))
        temp.replace(path)
        if self.history is not None:
            self.history.record("execution", data)

    def finish(self, rec, state, error=None):
        rec.update(state=state, error=error, finished=time.time())
        rec["done"].set()
        self.save(rec)
        self.history.record("execution", self.public_record(rec), event=state)

    async def watch_kernel(self):
        while True:
            await asyncio.sleep(1)
            if not await self.km.is_alive():
                self.healthy = False
                self.lose_python_tasks("Kernel exited")
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
            rec.update(state="running", started=time.time())
            self.save(rec)
            self.history.record("execution", self.public_record(rec), event="running")
            try:
                context = {
                    key: rec.get(key) for key in ("client_id", "connection_id", "client_name")
                }
                context["exec_id"] = rec["id"]
                msg = self.kc.session.msg(
                    "execute_request",
                    {
                        "code": rec["code"],
                        "silent": False,
                        "store_history": True,
                        "user_expressions": {},
                        "allow_stdin": False,
                        "stop_on_error": False,
                    },
                    metadata={"mypr": context},
                )
                msg_id = msg["header"]["msg_id"]
                rec["msg_id"] = msg_id
                self.kc.shell_channel.send(msg)
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
                self.history.append(
                    "execution",
                    "output",
                    {
                        "id": rec["id"],
                        "exec_id": rec["id"],
                        "client_id": rec.get("client_id"),
                        "connection_id": rec.get("connection_id"),
                        **piece,
                    },
                )
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
            "client_id": rec.get("client_id", rec.get("client")),
            "connection_id": rec.get("connection_id"),
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
        client = req.pop("client_id", None) or "anonymous"
        connection_id = req.pop("connection_id", None)
        connection = self.clients.get(connection_id)
        if connection:
            if connection["client_id"] != client:
                raise ValueError("Connection belongs to another client")
            connection["last_activity"] = time.time()
        if op == "status":
            connections = []
            for info in self.clients.values():
                owned = [
                    r["id"]
                    for r in self.task_records.values()
                    if r.get("client_id") == info["client_id"] and r["state"] not in TERMINAL
                ]
                active = (
                    self.active
                    if self.active and self.active.get("connection_id") == info["connection_id"]
                    else None
                )
                connections.append(
                    dict(info, active=active["id"] if active else None, task_ids=owned)
                )
            return {
                "version": __version__,
                "workspace_id": self.workspace_id,
                "workspace": str(self.workspace),
                "workspace_available": self.workspace_available(),
                "generation": self.generation,
                "healthy": self.healthy,
                "resetting": self.resetting,
                "connections": connections,
                "connection_count": len(connections),
                "client_count": len({c["client_id"] for c in connections}),
                "active": self.active["id"] if self.active else None,
                "queued": [r["id"] for r in self.execs.values() if r["state"] == "queued"],
            }
        if op in {"history_list", "logs"}:
            method = self.history.list if op == "history_list" else self.history.logs
            return method(
                client_id=req.get("filter_client_id"),
                limit=req.get("limit", 20),
                cursor=req.get("cursor"),
            )
        if op == "history_get":
            record = self.history.get(req.get("id", req.get("exec_id")))
            if record is None:
                raise ValueError("Unknown history ID")
            if (
                record["kind"] in {"shell", "package"}
                and record["id"] in self.task_records
                and record.get("generation") == self.generation
            ):
                record = await self.refresh_shell(self.shells, self.task_records[record["id"]])
            if record["kind"] == "execution":
                page = await self.poll(record["id"], wait_ms=0)
                record.update(
                    {key: page[key] for key in ("output", "cursor", "has_more", "truncated")}
                )
            return record
        generation = req.pop("generation", None)
        if generation and generation != self.generation:
            raise RuntimeError("Expired kernel generation")
        if op == "execute":
            if not self.workspace_available():
                raise RuntimeError("The workspace moved; stop its manager and reconnect")
            if not self.healthy or self.resetting:
                raise RuntimeError("Kernel unavailable; use CLI reset")
            code = req["code"]
            request_id = req.get("request_id")
            key = (client, request_id) if request_id is not None else None
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
                    client_id=client,
                    connection_id=connection_id,
                    client_name=connection.get("client_name")
                    if connection
                    else req.get("client_name"),
                    request_id=request_id,
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
                self.history.record("execution", self.public_record(rec), event="queued")
                self.queue.put_nowait(rec)
            return await self.poll(rec["id"], wait_ms=req.get("wait_ms", 1000))
        if op == "poll":
            return await self.poll(req["exec_id"], req.get("cursor") or 0, req.get("wait_ms", 1000))
        if op == "shell_start":
            job = await self.shells.start(
                req["command"],
                req.get("cwd", str(self.workspace)),
                req.get("env", dict(os.environ)),
            )
            self.track_shell(
                job["id"], client, connection_id, req.get("exec_id"), command=req["command"]
            )
            return job
        if op == "shell_poll":
            return await self.shells.poll(req["id"], req.get("cursor", 0))
        if op == "shell_cancel":
            return await self.shells.cancel(req["id"])
        if op == "mcp":
            method = req["method"]
            args = req.get("args", {})
            if method not in MCP_MUTATIONS:
                return await self.mcp.dispatch(method, args)
            event = {
                "client_id": client,
                "connection_id": connection_id,
                "exec_id": req.get("exec_id"),
                "server": args.get("server"),
            }
            self.history.append("mcp", method, dict(event, state="running"))
            try:
                result = await self.mcp.dispatch(method, args)
            except Exception as exc:
                self.history.append("mcp", method, dict(event, state="failed", error=str(exc)))
                raise
            self.history.append("mcp", method, dict(event, state="succeeded"))
            return result
        if op == "packages_add":
            import shlex

            specs = req["specs"]
            if not specs or any(not s or s.startswith("-") for s in specs):
                raise ValueError("Expected package requirements, not command options")
            command = shlex.join(["uv", "pip", "install", "--python", str(self.py), *specs])
            command += " && " + shlex.join(["uv", "pip", "freeze", "--python", str(self.py)])
            command += " > " + shlex.quote(str(self.root / "requirements.txt"))
            job = await self.shells.start(command, str(self.workspace), dict(os.environ))
            self.track_shell(
                job["id"], client, connection_id, req.get("exec_id"), kind="package", specs=specs
            )
            return job
        if op == "task_event":
            event = dict(req["event"])
            event.update(
                client_id=client,
                connection_id=connection_id,
                exec_id=req.get("exec_id"),
                generation=self.generation,
            )
            old = self.task_records.get(event["id"], {})
            self.task_records[event["id"]] = {**old, **event, "kind": "python"}
            delta = event.pop("output_delta", None)
            self.task_records[event["id"]].pop("output_delta", None)
            if delta is None and event.get("output") != old.get("output"):
                delta = event.get("output")
            if delta:
                self.history.append(
                    "python",
                    "output",
                    {
                        "id": event["id"],
                        "exec_id": event.get("exec_id"),
                        "client_id": client,
                        "connection_id": connection_id,
                        "text": delta,
                        "truncated": event.get("output_truncated", False),
                    },
                )
            self.history.record(
                "python",
                self.task_records[event["id"]],
                event=event["state"] if old.get("state") != event["state"] else None,
            )
            return None
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
            self.lose_python_tasks("Workspace reset", state="cancelled")
            await self.close_shells()
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

    @staticmethod
    def public_record(rec):
        return {k: v for k, v in rec.items() if k not in {"done", "idle", "events"}}

    async def attach(self, reader, writer, req):
        if not self.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")
        client_id = req.get("client_id")
        connection_id = req.get("connection_id")
        for value in (client_id, connection_id):
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
                raise ValueError("Client and connection IDs must be 1..128 identifier characters")
        if connection_id in self.clients:
            raise ValueError("Connection ID already attached")
        name = req.get("client_name")
        if name is not None and (not isinstance(name, str) or len(name) > 128):
            raise ValueError("Client name must be at most 128 characters")
        info = dict(
            client_id=client_id,
            connection_id=connection_id,
            client_name=name,
            connected_at=time.time(),
            last_activity=time.time(),
        )
        self.clients[connection_id] = info
        self.attachments[connection_id] = writer
        self.history.append("connection", "connected", info)
        try:
            status = await self.dispatch({"op": "status"})
            writer.write(json.dumps({"ok": True, "result": status}).encode() + b"\n")
            await writer.drain()
            await reader.read(1)
        finally:
            self.clients.pop(connection_id, None)
            self.attachments.pop(connection_id, None)
            if self.history:
                self.history.append("connection", "disconnected", info)
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    def track_shell(self, ident, client_id, connection_id, exec_id, kind="shell", **fields):
        record = dict(
            id=ident,
            kind=kind,
            client_id=client_id,
            connection_id=connection_id,
            exec_id=exec_id,
            generation=self.generation,
            created=time.time(),
            state="running",
            **fields,
        )
        self.task_records[ident] = record
        self.history.record(kind, record, event="running")
        task = self.spawn(self.watch_shell(self.shells, record))
        self.shell_watchers.add(task)
        task.add_done_callback(self.shell_watchers.discard)

    async def close_shells(self):
        await self.shells.close()
        await asyncio.gather(*list(self.shell_watchers), return_exceptions=True)

    async def watch_shell(self, shells, record):
        cursor = 0
        while True:
            result = await shells.poll(record["id"], cursor)
            for output in result["output"]:
                self.history.append(
                    record["kind"],
                    "output",
                    {
                        "id": record["id"],
                        "client_id": record["client_id"],
                        "connection_id": record["connection_id"],
                        "exec_id": record["exec_id"],
                        **output,
                    },
                )
            cursor = result["cursor"]
            if result["state"] in TERMINAL:
                await self.refresh_shell(shells, record)
                return
            await asyncio.sleep(0.25)

    async def refresh_shell(self, shells, record):
        full = await shells.poll(record["id"])
        if full["state"] not in TERMINAL:
            return record
        previous = record["state"]
        text = "".join(item["text"] for item in full["output"])
        record.update(
            state=full["state"],
            finished=record.get("finished", time.time()),
            error=full.get("error"),
            result=full.get("result"),
            output=text.encode()[:65536].decode(errors="ignore"),
            output_truncated=full.get("truncated") or len(text.encode()) > 65536,
        )
        self.history.record(
            record["kind"], record, event=full["state"] if previous != full["state"] else None
        )
        return record

    def lose_python_tasks(self, error, state="lost"):
        for record in self.task_records.values():
            if record["kind"] == "python" and record["state"] not in TERMINAL:
                record.update(state=state, error=error, finished=time.time())
                self.history.record("python", record, event=state)

    async def connection(self, reader, writer):
        try:
            req = json.loads(await reader.readline())
            if req.get("op") == "attach":
                await self.attach(reader, writer, req)
                return
            if req.get("op") == "mcp" and req.get("method") not in MCP_MUTATIONS:
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
            writer.write(json.dumps(response, ensure_ascii=False).encode() + b"\n")
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
                self.lose_python_tasks("Manager stopped")
                await self.close_shells()
                await self.mcp.close()
        finally:
            for writer in list(self.attachments.values()):
                writer.close()
            self.socket.unlink(missing_ok=True)
            if self.history:
                self.history.close()
                self.history = None
            lock.close()
