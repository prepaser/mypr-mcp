"""Manager-owned network scans and durable scan result pages."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import math
import os
import secrets
import shutil
import sys
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .services import Shells

_RESULT_LIMIT = 16 * 1024 * 1024
_DEFAULT_PAGE_ENTRIES = 100
_DEFAULT_PAGE_BYTES = 32768
_TERMINAL = {"succeeded", "failed", "cancelled", "lost"}


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class ScanService:
    """Start, supervise, and page scans without owning a kernel task."""

    def __init__(self, workspace: str | os.PathLike[str], shells: Shells, track=None):
        self.workspace = Path(workspace).expanduser().resolve()
        self.shells = shells
        self.root = self.workspace / ".mypr" / "scans"
        self.root.mkdir(parents=True, exist_ok=True)
        self._records: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._monitors: dict[str, asyncio.Task[None]] = {}
        self._load_records()
        self._track = track

    def _load_records(self) -> None:
        for path in self.root.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except OSError, ValueError:
                continue
            if not isinstance(record, dict) or record.get("id") != path.stem:
                continue
            if record.get("state") not in _TERMINAL:
                record.update(state="lost", error="scan manager restarted before completion")
                self._write_record(record)

    def _cache(self, record: dict[str, Any]) -> dict[str, Any]:
        ident = str(record["id"])
        self._records.pop(ident, None)
        self._records[ident] = record
        completed = [key for key, value in self._records.items() if value.get("state") in _TERMINAL]
        limit = self.shells.completed_records
        for key in completed[: max(0, len(completed) - limit)]:
            self._records.pop(key, None)
        return record

    async def start(
        self,
        mode: str,
        *,
        targets: Any,
        ports: Any = None,
        concurrency: int = 64,
        rate: float = 200,
        timeout: float = 1.0,  # noqa: ASYNC109
        args: list[str] | None = None,
        client_id: str | None = None,
        connection_id: str | None = None,
        exec_id: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"tcp", "nmap"}:
            raise ValueError("mode must be 'tcp' or 'nmap'")
        target_list = self._validate_targets(targets)
        request_id = secrets.token_hex(16)
        result_path = self.root / f"{request_id}.jsonl"
        artifact_path = self.root / f"{request_id}.xml"
        summary_path = self.root / f"{request_id}.summary.json"
        config = {
            "mode": mode,
            "targets": target_list,
            "ports": ports,
            "concurrency": concurrency,
            "rate": rate,
            "timeout": timeout,
            "result_path": str(result_path),
            "artifact_path": str(artifact_path),
            "summary_path": str(summary_path),
        }
        if mode == "tcp":
            self._validate_tcp(config)
        else:
            config = {
                "mode": mode,
                "targets": target_list,
                "args": self._validate_nmap_args(args),
                "result_path": str(result_path),
                "artifact_path": str(artifact_path),
                "summary_path": str(summary_path),
            }
        if mode == "nmap" and shutil.which("nmap") is None:
            raise RuntimeError("nmap is not installed")
        config_path = self.root / f"{request_id}.request.json"
        self._atomic_json(config_path, config)
        command = [
            sys.executable,
            str(Path(__file__).with_name("scan_worker.py")),
            "--config",
            str(config_path),
        ]
        launch = asyncio.create_task(self.shells.start(command, cwd=str(self.workspace)))
        try:
            job = await asyncio.shield(launch)
        except asyncio.CancelledError:
            job = await self._finish_cancelled_launch(launch)
            with contextlib.suppress(Exception):
                await self.shells.cancel(job["id"])
            raise
        except Exception:
            with contextlib.suppress(OSError):
                config_path.unlink()
            raise
        ident = str(job["id"])
        record = {
            "id": ident,
            "mode": mode,
            "state": "running",
            "created": time.time(),
            "targets": target_list,
            "config": str(config_path),
            "result_path": str(config["result_path"]),
            "summary_path": str(summary_path),
            "artifact": str(config["artifact_path"]) if mode == "nmap" else None,
            "result_count": 0,
            "result_bytes": 0,
            "truncated": False,
            "warnings": [],
            "client_id": client_id,
            "connection_id": connection_id,
            "exec_id": exec_id,
            "shell_id": ident,
        }
        try:
            self._cache(record)
            self._write_record(record)
            if self._track is not None:
                self._track(
                    ident,
                    client_id,
                    connection_id,
                    exec_id,
                    kind="scan",
                    mode=mode,
                    shell_id=ident,
                )
        except BaseException:
            with contextlib.suppress(Exception):
                await self._finish_cancelled_launch(asyncio.create_task(self.shells.cancel(ident)))
            self._records.pop(ident, None)
            with contextlib.suppress(OSError):
                config_path.unlink()
            raise
        monitor = asyncio.create_task(self._monitor(record), name=f"mypr:scan:{ident}")
        self._monitors[ident] = monitor
        monitor.add_done_callback(lambda _: self._monitors.pop(ident, None))
        return {"id": ident, "state": record["state"], "mode": mode}

    @staticmethod
    async def _finish_cancelled_launch(launch: asyncio.Task[Any]) -> dict[str, Any]:
        while True:
            try:
                job = await asyncio.shield(launch)
                break
            except asyncio.CancelledError:
                if launch.cancelled():
                    raise
        return job

    async def results(
        self,
        scan_id: str,
        *,
        cursor: str | None = None,
        max_entries: int = _DEFAULT_PAGE_ENTRIES,
        max_bytes: int = _DEFAULT_PAGE_BYTES,
    ) -> dict[str, Any]:
        record = self._record(scan_id)
        if type(max_entries) is not int or not 1 <= max_entries <= 1000:
            raise ValueError("max_entries must be between 1 and 1000")
        if type(max_bytes) is not int or not 1 <= max_bytes <= _RESULT_LIMIT:
            raise ValueError(f"max_bytes must be between 1 and {_RESULT_LIMIT}")
        offset = self._decode_cursor(cursor, scan_id) if cursor is not None else 0
        rows, next_offset, more = await asyncio.to_thread(
            self._read_results_page,
            Path(record["result_path"]),
            offset,
            max_entries,
            max_bytes,
            record.get("state") not in _TERMINAL,
        )
        next_cursor = self._encode_cursor(scan_id, next_offset) if more else None
        return {
            "id": scan_id,
            "results": rows,
            "cursor": next_cursor,
            "next_cursor": next_cursor,
            "has_more": more,
            "state": record.get("state", "unknown"),
            "truncated": bool(record.get("truncated")),
            "warnings": list(record.get("warnings", [])),
        }

    @staticmethod
    def _read_results_page(
        path: Path, offset: int, max_entries: int, max_bytes: int, running: bool
    ) -> tuple[list[dict[str, Any]], int, bool]:
        rows: list[dict[str, Any]] = []
        used = 0
        next_offset = offset
        budget_more = False
        eof = False
        more_data = False
        if not path.is_file():
            return rows, offset, running
        with path.open("rb") as stream:
            if offset > os.fstat(stream.fileno()).st_size:
                raise ValueError("scan cursor is beyond the retained results")
            stream.seek(offset)
            while len(rows) < max_entries:
                line = stream.readline()
                if not line:
                    eof = True
                    break
                next_offset = stream.tell()
                if not line.endswith(b"\n"):
                    if running:
                        return rows, next_offset - len(line), True
                    raise RuntimeError("scan results contain an incomplete final record")
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except (UnicodeDecodeError, ValueError) as exc:
                    raise RuntimeError("scan results contain a damaged record") from exc
                cost = len(_json_bytes(row))
                if not rows and cost > max_bytes:
                    raise ValueError("max_bytes is too small for the next scan result")
                if rows and used + cost > max_bytes:
                    budget_more = True
                    next_offset -= len(line)
                    break
                rows.append(row)
                used += cost
            if not eof and not budget_more:
                more_data = stream.peek(1) if hasattr(stream, "peek") else bool(stream.read(1))
            else:
                more_data = False
        return rows, next_offset, bool(running or budget_more or more_data)

    async def summary(self, scan_id: str, *, wait_ms: int = 0) -> dict[str, Any]:
        record = self._record(scan_id)
        if type(wait_ms) is not int or not 0 <= wait_ms <= 30000:
            raise ValueError("wait_ms must be between 0 and 30000")
        monitor = self._monitors.get(scan_id)
        if wait_ms and monitor is not None and not monitor.done():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(monitor), wait_ms / 1000)
        return dict(record)

    async def wait(self, scan_id: str) -> dict[str, Any]:
        monitor = self._monitors.get(scan_id)
        if monitor is not None:
            await asyncio.shield(monitor)
        return await self.summary(scan_id)

    async def cancel(self, scan_id: str) -> dict[str, Any]:
        record = self._record(scan_id)
        if record.get("state") in _TERMINAL:
            return dict(record)
        shell_id = record.get("shell_id")
        if shell_id:
            await self.shells.cancel(shell_id)
        record["state"] = "cancelled"
        record["finished"] = time.time()
        self._write_record(record)
        return dict(record)

    async def close(self) -> None:
        active = [
            ident for ident, record in self._records.items() if record.get("state") not in _TERMINAL
        ]
        await asyncio.gather(*(self.cancel(ident) for ident in active), return_exceptions=True)
        monitors = list(self._monitors.values())
        if monitors:
            await asyncio.gather(*monitors, return_exceptions=True)

    def _record(self, scan_id: str) -> dict[str, Any]:
        if (
            not isinstance(scan_id, str)
            or len(scan_id) != 32
            or any(char not in "0123456789abcdef" for char in scan_id)
            or not (self.root / f"{scan_id}.json").is_file()
        ):
            raise ValueError("unknown scan")
        current = self._records.get(scan_id)
        if current is not None:
            self._records.move_to_end(scan_id)
            return current
        try:
            record = json.loads((self.root / f"{scan_id}.json").read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError("invalid persisted scan") from exc
        if not isinstance(record, dict) or record.get("id") != scan_id:
            raise ValueError("invalid persisted scan")
        return self._cache(record)

    async def _monitor(self, record: dict[str, Any]) -> None:
        cursor = None
        pending = ""
        try:
            while True:
                page = await self.shells.read(
                    record["shell_id"], cursor=cursor, max_bytes=32768, wait_ms=1000
                )
                cursor = page.get("cursor", cursor)
                events = page.get("output", page.get("events", []))
                if isinstance(events, str):
                    events = [{"text": events}]
                for event in events if isinstance(events, list) else []:
                    if not isinstance(event, dict):
                        continue
                    text = str(event.get("text", ""))
                    if event.get("stream") != "stdout":
                        continue
                    pending += text
                    lines = pending.splitlines(keepends=True)
                    pending = "" if not lines or lines[-1].endswith(("\n", "\r")) else lines.pop()
                    for line in lines:
                        self._consume_line(record, line)
                terminal = page.get("state") not in {"running", "cancelling", "queued"}
                if terminal and not page.get("has_more"):
                    if pending.strip():
                        self._consume_line(record, pending)
                    await self._finish(record, page)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with contextlib.suppress(Exception):
                await self.shells.cancel(record["shell_id"])
            await self._finish(record, {"state": "failed", "error": str(exc), "result": None})

    def _consume_line(self, record: dict[str, Any], line: str) -> None:
        try:
            message = json.loads(line)
        except ValueError:
            self._warning(record, "invalid_scan_output", line.strip()[:256])
            return
        if not isinstance(message, dict):
            return
        if message.get("type") == "progress":
            for key in (
                "count",
                "bytes",
                "truncated",
                "artifact_bytes",
                "artifact_truncated",
                "returncode",
            ):
                if key in message:
                    target = {
                        "count": "result_count",
                        "bytes": "result_bytes",
                    }.get(key, key)
                    record[target] = message[key]
            if message.get("stderr"):
                self._warning(record, "nmap_stderr", str(message["stderr"])[-256:])
            self._write_record(record)
        elif message.get("type") == "error":
            record["error"] = str(message.get("error", "scan failed"))

    async def _finish(self, record: dict[str, Any], page: dict[str, Any]) -> None:
        summary_path = record.get("summary_path")
        if summary_path:
            try:
                summary = json.loads(await asyncio.to_thread(Path(summary_path).read_text))
                if isinstance(summary, dict):
                    for key in (
                        "truncated",
                        "artifact_bytes",
                        "artifact_truncated",
                        "returncode",
                        "error",
                    ):
                        if key in summary:
                            record[key] = summary[key]
            except (OSError, ValueError) as exc:
                self._warning(record, "summary_unavailable", str(exc)[:256])
        state = str(page.get("state", "failed"))
        if state not in _TERMINAL:
            state = "failed"
        if record.get("state") == "cancelled":
            state = "cancelled"
        result = page.get("result") or {}
        returncode = record.get("returncode", result.get("returncode"))
        if state == "succeeded" and returncode not in (None, 0):
            state = "failed"
        result_bytes, result_count = await asyncio.to_thread(
            self._result_stats, Path(record["result_path"])
        )
        if result_bytes is not None:
            record["result_bytes"] = result_bytes
            record["result_count"] = result_count
        record.update(
            state=state,
            finished=time.time(),
            returncode=returncode,
            error=record.get("error") or page.get("error"),
            shell_truncated=bool(page.get("truncated")),
        )
        if page.get("warnings"):
            for warning in page["warnings"]:
                if warning not in record["warnings"] and len(record["warnings"]) < 4:
                    record["warnings"].append(warning)
        with contextlib.suppress(OSError):
            await asyncio.to_thread(Path(record["config"]).unlink)
        self._write_record(record)
        self._cache(record)

    @staticmethod
    def _result_stats(path: Path) -> tuple[int | None, int]:
        try:
            with path.open("rb") as stream:
                return path.stat().st_size, sum(1 for line in stream if line.strip())
        except OSError:
            return None, 0

    def _warning(self, record: dict[str, Any], code: str, text: str) -> None:
        warnings = record.setdefault("warnings", [])
        item = {"code": code, "text": text[:256]}
        if item not in warnings and len(warnings) < 4:
            warnings.append(item)
        self._write_record(record)

    def _results_path(self, ident: str) -> Path:
        return self.root / f"{ident}.jsonl"

    def _write_record(self, record: dict[str, Any]) -> None:
        self._atomic_json(self.root / f"{record['id']}.json", record)

    @staticmethod
    def _atomic_json(path: Path, value: Any) -> None:
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, ensure_ascii=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)

    @staticmethod
    def _validate_targets(targets: Any) -> list[str]:
        values = list(targets) if isinstance(targets, (list, tuple)) else [targets]
        if not values or len(values) > 1024:
            raise ValueError("targets must contain between 1 and 1024 entries")
        result = []
        for target in values:
            if not isinstance(target, str) or not target.strip() or target.startswith("-"):
                raise ValueError("targets must contain non-empty host names or addresses")
            result.append(target.strip())
        return result

    @staticmethod
    def _validate_tcp(config: dict[str, Any]) -> None:
        concurrency = config["concurrency"]
        if type(concurrency) is not int or not 1 <= concurrency <= 1024:
            raise ValueError("concurrency must be between 1 and 1024")
        for name in ("rate", "timeout"):
            if (
                isinstance(config[name], bool)
                or not isinstance(config[name], (int, float))
                or not math.isfinite(config[name])
                or config[name] <= 0
            ):
                raise ValueError(f"{name} must be positive")

    @staticmethod
    def _validate_nmap_args(args: list[str] | None) -> list[str]:
        if args is None:
            return []
        if not isinstance(args, list) or any(not isinstance(item, str) for item in args):
            raise TypeError("nmap args must be a list of strings")
        if any(
            item in {"-oX", "--xml", "-oA", "-oN", "-oG"} or item.startswith("-o") for item in args
        ):
            raise ValueError("nmap output options are managed by mypr")
        return list(args)

    @staticmethod
    def _encode_cursor(ident: str, index: int) -> str:
        raw = _json_bytes({"v": 1, "id": ident, "index": index})
        return "mypr-scan1." + base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str, ident: str) -> int:
        if not isinstance(cursor, str) or not cursor.startswith("mypr-scan1."):
            raise ValueError("invalid scan cursor")
        try:
            encoded = cursor.split(".", 1)[1]
            value = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid scan cursor") from exc
        if (
            not isinstance(value, dict)
            or value.get("v") != 1
            or value.get("id") != ident
            or type(value.get("index")) is not int
            or value["index"] < 0
        ):
            raise ValueError("invalid scan cursor")
        return value["index"]


__all__ = ["ScanService"]
