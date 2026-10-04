"""Manager-launched network scanner worker."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import errno
import ipaddress
import json
import os
import signal
import socket
import time
import xml.etree.ElementTree as ET
from collections import OrderedDict, deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mypr_mcp.async_utils import finish_owned, wait_owned
from mypr_mcp.file_io import open_regular

_RESULT_LIMIT = 16 * 1024 * 1024
_DEFAULT_MAX_PROBES = 1_000_000
_DEFAULT_MAX_DURATION = 3600.0


def validate_tcp_config(config: dict[str, Any]) -> None:
    from mypr_mcp.scan_config import normalize_native_config

    _ports(config.get("ports"))
    normalize_native_config({**config, "mode": "tcp"})


def _family(value: str) -> int:
    return {"any": socket.AF_UNSPEC, "ipv4": socket.AF_INET, "ipv6": socket.AF_INET6}[value]


def _targets(value: Any) -> Iterator[str]:
    values = value if isinstance(value, (list, tuple, set)) else [value]
    produced = 0
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("targets must contain non-empty strings")
        target = item.strip()
        try:
            network = ipaddress.ip_network(target, strict=False)
        except ValueError:
            yield target
        else:
            scope = (
                target.partition("%")[2].partition("/")[0]
                if network.version == 6 and "%" in target else None
            )
            for address in network.hosts():
                produced += 1
                if produced > 1_000_000:
                    raise ValueError("target network expands to more than 1000000 hosts")
                text = str(address)
                yield f"{text.partition('%')[0]}%{scope}" if scope else text


def _ports(value: Any) -> list[int]:
    if value is None:
        value = "1-1024"
    values = value if isinstance(value, (list, tuple, set, frozenset)) else [value]
    result: set[int] = set()
    for item in values:
        if isinstance(item, int) and not isinstance(item, bool):
            starts = ends = item
        elif isinstance(item, str):
            for part in item.split(","):
                part = part.strip()
                if not part:
                    continue
                if "-" in part:
                    left, right = part.split("-", 1)
                    starts, ends = int(left), int(right)
                else:
                    starts = ends = int(part)
                if starts > ends:
                    starts, ends = ends, starts
                if starts < 1 or ends > 65535:
                    raise ValueError("ports must be between 1 and 65535")
                result.update(range(starts, ends + 1))
            continue
        else:
            raise TypeError("ports must be integers or ranges")
        if starts < 1 or ends > 65535:
            raise ValueError("ports must be between 1 and 65535")
        result.add(starts)
    if not result:
        raise ValueError("ports must not be empty")
    return sorted(result)


class _Results:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open_regular(path, "ab")
        self.count = 0
        self.bytes = 0
        self.truncated = False
        self.stop_reason: str | None = None

    def append(self, row: dict[str, Any]) -> bool:
        encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        if self.bytes + len(encoded) > _RESULT_LIMIT:
            self.truncated = True
            self.stop_reason = self.stop_reason or "result_size_limit"
            return False
        self.file.write(encoded)
        self.file.flush()
        self.count += 1
        self.bytes += len(encoded)
        return True

    def close(self) -> None:
        self.file.close()


def _progress(**fields: Any) -> None:
    print(json.dumps({"type": "progress", **fields}, separators=(",", ":")), flush=True)


class _Rate:
    def __init__(self, rate: float | None):
        self.delay = 0.0 if rate is None or rate <= 0 else 1.0 / rate
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        if not self.delay:
            return
        async with self.lock:
            now = time.monotonic()
            pause = self.next_at - now
            if pause > 0:
                await asyncio.sleep(pause)
            self.next_at = time.monotonic() + self.delay


class _OutputLimit(Exception):
    pass


_LOCAL_ERRORS = {
    errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM,
    errno.EADDRNOTAVAIL, errno.EACCES, errno.EPERM,
}


async def _resolve(host: str, family: int, protocol: str = "tcp") -> list[tuple[int, tuple]]:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None:
        resolved_family = socket.AF_INET if address.version == 4 else socket.AF_INET6
        if family not in (socket.AF_UNSPEC, resolved_family):
            return []
        if "%" not in host:
            sockaddr = (str(address), 0) if address.version == 4 else (str(address), 0, 0, 0)
            return [(resolved_family, sockaddr)]
    from mypr_mcp.network_tools import _resolve_worker

    kind = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
    proto = socket.IPPROTO_UDP if protocol == "udp" else socket.IPPROTO_TCP
    rows = await _resolve_worker(host, 0, family, kind, proto, 5.0)
    found = []
    seen = set()
    for row in rows:
        resolved_family = row["family"]
        if resolved_family not in (socket.AF_INET, socket.AF_INET6):
            continue
        if family not in (socket.AF_UNSPEC, resolved_family):
            continue
        sockaddr = tuple(row["sockaddr"])
        key = (resolved_family, sockaddr)
        if key not in seen:
            seen.add(key)
            found.append(key)
    return found


async def _probe(
    host: str,
    port: int,
    timeout: float,  # noqa: ASYNC109
    *,
    family: int,
    sockaddr: tuple,
    banner: bool = False,
    banner_timeout: float = 0.5,
    banner_bytes: int = 1024,
) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {
        "host": host, "address": sockaddr[0], "port": port,
        "family": "ipv4" if family == socket.AF_INET else "ipv6",
        "protocol": "tcp",
    }
    if family == socket.AF_INET6 and len(sockaddr) > 3 and sockaddr[3]:
        result["scope_id"] = sockaddr[3]
    connection = None
    try:
        connection = socket.socket(family, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        connection.setblocking(False)
        endpoint = (sockaddr[0], port, *sockaddr[2:])
        async with asyncio.timeout(timeout):
            await asyncio.get_running_loop().sock_connect(connection, endpoint)
        result["state"] = "open"
        if banner:
            data = bytearray()
            eof = False
            try:
                async with asyncio.timeout(banner_timeout):
                    while len(data) <= banner_bytes:
                        chunk = await asyncio.get_running_loop().sock_recv(
                            connection, banner_bytes + 1 - len(data)
                        )
                        if not chunk:
                            eof = True
                            break
                        data.extend(chunk)
            except TimeoutError:
                result["banner_error"] = "read_timeout"
            except OSError as exc:
                result["banner_error"] = errno.errorcode.get(exc.errno, "read_error")
            result.update(
                banner=data[:banner_bytes].decode("utf-8", errors="replace"),
                banner_bytes=min(len(data), banner_bytes),
                banner_truncated=len(data) > banner_bytes, banner_complete=eof,
            )
    except TimeoutError:
        result.update(state="timeout", reason="connection timed out", reason_code="connect_timeout")
    except OSError as exc:
        if exc.errno in _LOCAL_ERRORS:
            raise
        state = "closed" if exc.errno == errno.ECONNREFUSED else "unreachable"
        if exc.errno == errno.ETIMEDOUT:
            state = "timeout"
        result.update(
            state=state, reason=str(exc)[:256], errno=exc.errno,
            reason_code=errno.errorcode.get(exc.errno, "connect_error"),
        )
    finally:
        if connection is not None:
            with contextlib.suppress(OSError):
                connection.close()
    result["duration_ms"] = round((time.monotonic() - started) * 1000, 3)
    return result


async def _until_stop(operation, stop):
    task = asyncio.create_task(operation)
    stopped = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait((task, stopped), return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            return True, task.result()
        return False, None
    finally:
        for pending in (task, stopped):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(task, stopped, return_exceptions=True)


async def _addresses(config, store, stop):
    family = _family(config.get("family", "any"))
    cache = {}
    for host in _targets(config.get("targets")):
        if stop.is_set():
            return
        try:
            literal = ipaddress.ip_address(host)
        except ValueError:
            literal = None
        if literal is None or "%" in host:
            resolved = cache.get(host)
            if resolved is None:
                try:
                    allowed, resolved = await _until_stop(
                        _resolve(host, family, config.get("mode", "tcp")), stop
                    )
                    if not allowed:
                        return
                except OSError as exc:
                    if exc.errno in _LOCAL_ERRORS:
                        config["stop_reason"] = "local_resource_error"
                        raise
                    resolved = exc
                if literal is None:
                    cache[host] = resolved
        else:
            resolved = await _resolve(host, family, config.get("mode", "tcp"))
        if isinstance(resolved, Exception) or not resolved:
            excluded = literal is not None and (
                (family == socket.AF_INET and literal.version == 6)
                or (family == socket.AF_INET6 and literal.version == 4)
            )
            if not excluded:
                config["resolve_errors"] += 1
                if not config.get("open_only", False):
                    store({
                        "host": host, "port": None, "phase": "resolve",
                        "protocol": config.get("mode", "tcp"), "state": "unreachable",
                        "reason": str(resolved)[:256] if isinstance(resolved, Exception)
                        else "no matching addresses",
                        "reason_code": "resolve_timeout" if isinstance(resolved, TimeoutError)
                        else "resolve_failed",
                    })
            continue
        for resolved_family, sockaddr in resolved:
            yield host, resolved_family, sockaddr


def _native_stats(config, states):
    config.update(
        attempts=0, completed=0, discarded_results=0, resolve_errors=0, complete=False,
        state_counts=dict.fromkeys(states, 0),
        estimate=estimate_probes(config.get("targets", []), config.get("ports"),
                                 family=config.get("family", "any")),
    )


def _native_progress(config, results):
    _progress(
        count=results.count, bytes=results.bytes,
        **{key: config[key] for key in (
            "attempts", "completed", "state_counts", "discarded_results", "resolve_errors"
        )},
    )


@dataclass(eq=False)
class _AddressSlot:
    host: str
    family: int
    sockaddr: tuple
    position: int = 0
    running: int = 0
    retries: deque = field(default_factory=deque)

    @property
    def key(self):
        return self.family, self.sockaddr[0], self.sockaddr[3] if len(self.sockaddr) > 3 else 0


async def _fair_native(config, results):
    from mypr_mcp.scan_config import TCP_STATES, UDP_STATES, normalize_native_config

    config.update(normalize_native_config(config))
    udp = config["mode"] == "udp"
    ports = _ports(config.get("ports"))
    workers = config["concurrency"]
    _native_stats(config, UDP_STATES if udp else TCP_STATES)
    window = deque()
    active = {}
    host_next = OrderedDict()
    stop = asyncio.Event()
    global_next = 0.0
    delay = 1.0 / config["rate"]
    host_delay = 1.0 / config["per_host_rate"] if config["per_host_rate"] else 0.0
    window_limit = max(16, workers)
    exhausted = False
    planned = 0
    lookup = None

    def store(row):
        if not results.append(row):
            config["discarded_results"] += 1
            if not config["continue_after_output_limit"]:
                config["stop_reason"] = "result_size_limit"
                raise _OutputLimit

    addresses = _addresses(config, store, stop)

    async def probe(slot, port):
        if udp:
            from mypr_mcp.udp_scan import probe as udp_probe

            return await udp_probe(
                slot.host, port, config["timeout"], family=slot.family,
                sockaddr=slot.sockaddr, config=config,
            )
        return await _probe(
            slot.host, port, config["timeout"], family=slot.family, sockaddr=slot.sockaddr,
            banner=config["banner"], banner_timeout=config["banner_timeout"],
            banner_bytes=config["banner_bytes"],
        )

    def finish(row, attempts):
        row["attempts"] = attempts
        config["completed"] += 1
        config["state_counts"][row["state"]] += 1
        if not config["open_only"] or row["state"] == "open":
            store(row)
        if config["completed"] % 100 == 0:
            _native_progress(config, results)

    def halt_budget():
        config["stop_reason"] = "probe_limit"
        stop.set()
        for slot in window:
            while slot.retries:
                _, attempts, row = slot.retries.popleft()
                if not udp:
                    finish(row, attempts)

    async def drive():
        nonlocal lookup, exhausted, planned, global_next
        while True:
            for task in tuple(active):
                if not task.done():
                    continue
                slot, port, attempts = active.pop(task)
                slot.running -= 1
                try:
                    row = task.result()
                except OSError as exc:
                    config["stop_reason"] = (
                        "local_resource_error" if exc.errno in _LOCAL_ERRORS else "probe_error"
                    )
                    raise
                retry_state = "open|filtered" if udp else "timeout"
                retry = row["state"] == retry_state and attempts <= config["retries"]
                if retry and config["max_probes"] is not None and (
                    config["attempts"] >= config["max_probes"]
                ):
                    halt_budget()
                if retry and not stop.is_set():
                    slot.retries.append((port, attempts, row))
                    continue
                if retry and udp and stop.is_set():
                    continue
                finish(row, attempts)

            if stop.is_set():
                if not active:
                    return
                await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
                continue

            window_size = len(window)
            for _ in range(window_size):
                slot = window.popleft()
                if slot.position < len(ports) or slot.running or slot.retries:
                    window.append(slot)

            if lookup is not None and lookup.done():
                try:
                    host, family, sockaddr = lookup.result()
                except StopAsyncIteration:
                    exhausted = True
                    if not config["resolve_errors"]:
                        config["estimate"] = planned
                else:
                    window.append(_AddressSlot(host, family, sockaddr))
                    planned += len(ports)
                lookup = None

            if lookup is None and not exhausted and len(window) < window_limit:
                lookup = asyncio.create_task(anext(addresses))
            if exhausted and not window and not active:
                return

            now = time.monotonic()
            while host_next and next(iter(host_next.values())) <= now:
                host_next.popitem(last=False)
            ready_at = None
            for _ in range(len(window)):
                slot = window.popleft()
                window.append(slot)
                if not slot.retries and slot.position == len(ports):
                    continue
                if config["max_probes"] is not None and config["attempts"] >= config["max_probes"]:
                    halt_budget()
                    break
                eligible = max(global_next, host_next.get(slot.key, 0.0))
                if slot.key not in host_next and len(host_next) >= 65536:
                    eligible = max(eligible, next(iter(host_next.values())))
                if ready_at is None or eligible < ready_at:
                    ready_at = eligible
                if len(active) >= workers:
                    break
                now = time.monotonic()
                if eligible > now:
                    continue
                if slot.retries:
                    port, attempts, _ = slot.retries.popleft()
                else:
                    port, attempts = ports[slot.position], 0
                    slot.position += 1
                config["attempts"] += 1
                slot.running += 1
                active[asyncio.create_task(probe(slot, port))] = slot, port, attempts + 1
                global_next = now + delay
                if host_delay:
                    host_next.pop(slot.key, None)
                    host_next[slot.key] = now + host_delay
                ready_at = global_next

            waiting = set(active)
            if lookup is not None:
                waiting.add(lookup)
            pause = None
            if ready_at is not None and len(active) < workers:
                pause = max(0.0, ready_at - time.monotonic())
            if waiting:
                await asyncio.wait(waiting, timeout=pause, return_when=asyncio.FIRST_COMPLETED)
            elif pause is not None:
                await asyncio.sleep(pause)
            else:
                raise RuntimeError("native scan scheduler has no pending work")

    try:
        async with asyncio.timeout(config["max_duration"]):
            await drive()
    except _OutputLimit:
        pass
    except TimeoutError:
        config["stop_reason"] = "time_limit"
    finally:
        stop.set()
        pending = [*active, *([lookup] if lookup is not None else [])]
        for task in pending:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await addresses.aclose()
    config.setdefault("stop_reason", results.stop_reason)
    config["complete"] = (
        exhausted and not config.get("stop_reason") and not config["resolve_errors"]
        and not results.truncated
    )
    return 0


async def _tcp(config: dict[str, Any], results: _Results) -> int:
    from mypr_mcp.scan_config import TCP_STATES, normalize_native_config

    config.update(normalize_native_config(config))
    ports = _ports(config.get("ports"))
    if config.get("per_host_rate") is not None:
        return await _fair_native(config, results)
    workers = config.get("concurrency", 64)
    timeout = float(config.get("timeout", 1.0))
    rate = _Rate(float(config.get("rate", 200)))
    max_probes = config.get("max_probes", _DEFAULT_MAX_PROBES)
    max_duration = config.get("max_duration", _DEFAULT_MAX_DURATION)
    retries = config.get("retries", 0)
    continue_after_output_limit = config.get("continue_after_output_limit", False)
    queue: asyncio.Queue[tuple | None] = asyncio.Queue(maxsize=workers * 2)
    stop = asyncio.Event()
    _native_stats(config, TCP_STATES)
    exhausted = False

    def stop_for(reason: str) -> None:
        if not stop.is_set():
            config["stop_reason"] = reason
            stop.set()

    async def until_stop(operation):
        return await _until_stop(operation, stop)

    def store(row: dict[str, Any]) -> None:
        if not results.append(row):
            config["discarded_results"] += 1
            if not continue_after_output_limit:
                stop_for("result_size_limit")
                raise _OutputLimit

    async def produce() -> None:
        nonlocal exhausted
        planned = 0
        async for host, resolved_family, sockaddr in _addresses(config, store, stop):
            for port in ports:
                if stop.is_set():
                    return
                queued, _ = await until_stop(queue.put((host, port, resolved_family, sockaddr)))
                if not queued:
                    return
                planned += 1
        if stop.is_set():
            return
        exhausted = True
        if not config["resolve_errors"]:
            config["estimate"] = planned
        for _ in range(workers):
            queued, _ = await until_stop(queue.put(None))
            if not queued:
                return

    async def consume() -> None:
        while not stop.is_set():
            received, pair = await until_stop(queue.get())
            if not received or pair is None:
                return
            host, port, resolved_family, sockaddr = pair
            row = None
            endpoint_attempts = 0
            for _ in range(retries + 1):
                if max_probes is not None and config["attempts"] >= max_probes:
                    stop_for("probe_limit")
                    break
                allowed, _ = await until_stop(rate.wait())
                if not allowed or stop.is_set():
                    break
                if max_probes is not None and config["attempts"] >= max_probes:
                    stop_for("probe_limit")
                    break
                config["attempts"] += 1
                endpoint_attempts += 1
                try:
                    row = await _probe(
                        host, port, timeout, family=resolved_family, sockaddr=sockaddr,
                        banner=config.get("banner", False),
                        banner_timeout=config.get("banner_timeout", 0.5),
                        banner_bytes=config.get("banner_bytes", 1024),
                    )
                except OSError:
                    stop_for("local_resource_error")
                    raise
                if row["state"] != "timeout":
                    break
            if row is None:
                return
            row["attempts"] = endpoint_attempts
            config["completed"] += 1
            config["state_counts"][row["state"]] += 1
            if not config.get("open_only", False) or row["state"] == "open":
                store(row)
            if config["completed"] % 100 == 0:
                _native_progress(config, results)

    try:
        async with asyncio.timeout(max_duration):
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(produce())
                    for _ in range(workers):
                        group.create_task(consume())
            except* _OutputLimit:
                pass
    except TimeoutError:
        stop_for("time_limit")
    config.setdefault("stop_reason", results.stop_reason)
    config["complete"] = (
        exhausted and not config.get("stop_reason") and not config["resolve_errors"]
        and not results.truncated
    )
    return 0


def estimate_probes(targets: list[str], ports: Any, *, family: str = "any") -> int | None:
    """Return a cheap exact estimate for literal IP/CIDR targets."""

    try:
        port_count = len(_ports(ports))
    except (TypeError, ValueError):
        return None
    target_count = 0
    for value in targets if isinstance(targets, (list, tuple)) else [targets]:
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError:
            return None
        if (family == "ipv4" and network.version != 4) or (
            family == "ipv6" and network.version != 6
        ):
            continue
        count = network.num_addresses
        if network.version == 4 and network.prefixlen < 31:
            count = max(0, count - 2)
        elif network.version == 6 and network.prefixlen < 127:
            count = max(0, count - 1)
        target_count += count
    return target_count * port_count


def _bounded_text(value: str | None, limit: int = 8192) -> str | None:
    if value is None:
        return None
    return value if len(value) <= limit else value[:limit] + "…"


def _script_row(script: ET.Element) -> dict[str, Any]:
    row: dict[str, Any] = {}
    if script.get("id"):
        row["id"] = script.get("id")
    output = _bounded_text(script.get("output"))
    if output is not None:
        row["output"] = output
    tables = []
    for table in script.findall("table"):
        item: dict[str, Any] = {}
        if table.get("key"):
            item["key"] = table.get("key")
        values = [
            _bounded_text(value.get("key") or value.text)
            for value in table.findall("elem")
            if value.get("key") or value.text
        ]
        if values:
            item["values"] = values[:64]
        if item:
            tables.append(item)
    if tables:
        row["tables"] = tables[:64]
    return row


def _nmap_row(host: ET.Element) -> dict[str, Any]:
    status = host.find("status")
    row: dict[str, Any] = {
        "host": next(
            (item.get("addr") for item in host.findall("address") if item.get("addr")), None
        ),
        "state": status.get("state") if status is not None else "unknown",
    }
    names = [item.get("name") for item in host.findall("hostnames/hostname") if item.get("name")]
    if names:
        row["hostnames"] = names
    ports = []
    for item in host.findall("ports/port"):
        state = item.find("state")
        entry: dict[str, Any] = {
            "port": int(item.get("portid", 0)),
            "protocol": item.get("protocol"),
        }
        if state is not None:
            entry["state"] = state.get("state")
            if state.get("reason"):
                entry["reason"] = state.get("reason")
        service = item.find("service")
        if service is not None:
            service_data = {
                key: _bounded_text(service.get(key))
                for key in ("name", "product", "version", "extrainfo", "tunnel", "method")
                if service.get(key)
            }
            cpes = [cpe.text for cpe in service.findall("cpe") if cpe.text]
            if cpes:
                service_data["cpe"] = cpes[:16]
            if service_data:
                entry["service"] = service_data
        scripts = [_script_row(script) for script in item.findall("script")]
        if scripts:
            entry["scripts"] = scripts[:64]
        ports.append(entry)
    if ports:
        row["ports"] = ports
    host_scripts = [_script_row(script) for script in host.findall("hostscript/script")]
    if host_scripts:
        row["scripts"] = host_scripts[:64]
    os_matches = []
    for match in host.findall("os/osmatch"):
        item = {
            key: _bounded_text(match.get(key))
            for key in ("name", "accuracy", "line")
            if match.get(key)
        }
        cpes = [cpe.text for cpe in match.findall("osclass/cpe") if cpe.text]
        if cpes:
            item["cpe"] = cpes[:16]
        if item:
            os_matches.append(item)
    if os_matches:
        row["os"] = os_matches[:64]
    return row


async def _read_tail(stream: asyncio.StreamReader, limit: int = 64 * 1024) -> bytes:
    tail = bytearray()
    while chunk := await stream.read(8192):
        tail.extend(chunk)
        if len(tail) > limit:
            del tail[: len(tail) - limit]
    return bytes(tail)


async def _nmap(config: dict[str, Any], results: _Results) -> int:
    command = ["nmap", *config.get("args", []), "-oX", "-", "--", *config["targets"]]
    parser = ET.XMLPullParser(["start", "end"])
    stack: list[ET.Element] = []
    parse_error: str | None = None
    artifact_file = open_regular(Path(config["artifact_path"]), "wb")
    artifact_bytes = 0
    artifact_truncated = False
    process = None
    stderr_task = None
    try:
        process, cancelled = await finish_owned(
            asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        )
        if cancelled:
            raise asyncio.CancelledError
        stderr_task = asyncio.create_task(_read_tail(process.stderr))
        while True:
            chunk = await process.stdout.read(65536)
            if not chunk:
                break
            if not artifact_truncated:
                kept = chunk[: max(0, _RESULT_LIMIT - artifact_bytes)]
                artifact_file.write(kept)
                artifact_bytes += len(kept)
                artifact_truncated = len(kept) < len(chunk)
            if parser is not None:
                try:
                    parser.feed(chunk)
                    for event, element in parser.read_events():
                        if event == "start":
                            stack.append(element)
                            continue
                        if element.tag == "host":
                            stored = results.append(_nmap_row(element))
                            _progress(count=results.count, bytes=results.bytes)
                            if not stored and not config.get("continue_after_output_limit", False):
                                config["stop_reason"] = "result_size_limit"
                                process.terminate()
                                break
                            if len(stack) >= 2:
                                with contextlib.suppress(ValueError):
                                    stack[-2].remove(element)
                            element.clear()
                        if stack:
                            stack.pop()
                except ET.ParseError as exc:
                    parse_error = str(exc)
                    parser = None
                    stack.clear()
            if config.get("stop_reason") == "result_size_limit":
                break
        if config.get("stop_reason") == "result_size_limit" and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 2.0)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
        else:
            await process.wait()
        if parser is not None:
            try:
                parser.close()
            except ET.ParseError as exc:
                parse_error = str(exc)
        stderr = await stderr_task
        if stderr:
            _progress(stderr=stderr.decode("utf-8", "replace")[-1024:])
        _progress(
            count=results.count,
            bytes=results.bytes,
            artifact_bytes=artifact_bytes,
            artifact_truncated=artifact_truncated,
            returncode=process.returncode,
        )
        if parse_error and config.get("stop_reason") != "result_size_limit":
            config["error"] = f"invalid or incomplete Nmap XML: {parse_error}"
            return process.returncode or 2
        if config.get("stop_reason") == "result_size_limit":
            return 0
        return process.returncode or 0
    finally:
        config["artifact_bytes"] = artifact_bytes
        config["artifact_truncated"] = artifact_truncated
        try:
            await wait_owned(_close_nmap(process, stderr_task))
        finally:
            artifact_file.close()


async def _close_nmap(process, stderr_task) -> None:
    try:
        if process is not None and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            drains = [_read_tail(process.stdout), process.wait()]
            if stderr_task is None:
                drains.append(_read_tail(process.stderr))
            await asyncio.gather(*drains)
    finally:
        if stderr_task is not None:
            if not stderr_task.done():
                stderr_task.cancel()
            await asyncio.gather(stderr_task, return_exceptions=True)


async def run(config: dict[str, Any]) -> int:
    results = _Results(Path(config["result_path"]))
    started = time.monotonic()
    returncode = 2
    error = None
    cancelled = False
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    signal_installed = False
    if current is not None and hasattr(loop, "add_signal_handler"):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal.SIGTERM, current.cancel)
            signal_installed = True
    try:
        if config.get("mode", "tcp") == "tcp":
            returncode = await _tcp(config, results)
        elif config.get("mode") == "udp":
            returncode = await _fair_native(config, results)
        elif config.get("mode") == "nmap":
            max_duration = config.get("max_duration", _DEFAULT_MAX_DURATION)
            if max_duration is None:
                returncode = await _nmap(config, results)
            else:
                try:
                    async with asyncio.timeout(float(max_duration)):
                        returncode = await _nmap(config, results)
                except TimeoutError:
                    config["stop_reason"] = "time_limit"
                    returncode = 0
        else:
            raise ValueError("unknown scan mode")
        _progress(
            count=results.count,
            bytes=results.bytes,
            truncated=results.truncated,
            attempts=config.get("attempts", results.count),
            stop_reason=config.get("stop_reason") or results.stop_reason,
            returncode=returncode,
            **({key: config[key] for key in (
                "completed", "state_counts", "discarded_results", "resolve_errors",
                "complete", "estimate",
            ) if key in config} if config.get("mode", "tcp") in {"tcp", "udp"} else {}),
        )
        return returncode
    except asyncio.CancelledError:
        cancelled = True
        returncode = 143
        return returncode
    except Exception as exc:
        error = _exception_text(exc)
        _progress(error=error, count=results.count, bytes=results.bytes)
        return returncode
    finally:
        results.close()
        if signal_installed:
            loop.remove_signal_handler(signal.SIGTERM)
        summary_path = config.get("summary_path")
        if summary_path:
            summary = {
                "count": results.count,
                "bytes": results.bytes,
                "truncated": results.truncated,
                "attempts": config.get("attempts", results.count),
                "stop_reason": config.get("stop_reason") or results.stop_reason,
                "duration_seconds": round(time.monotonic() - started, 3),
                "artifact_bytes": config.get("artifact_bytes", 0),
                "artifact_truncated": config.get("artifact_truncated", False),
                "returncode": returncode,
                "cancelled": cancelled,
                "error": error or config.get("error"),
            }
            if config.get("mode", "tcp") in {"tcp", "udp"}:
                summary.update({
                    key: config[key] for key in (
                        "completed", "state_counts", "discarded_results", "resolve_errors",
                        "estimate",
                    ) if key in config
                })
                summary["complete"] = (
                    bool(config.get("complete")) and not cancelled and returncode == 0
                    and not summary["error"]
                )
            temporary = Path(summary_path).with_suffix(".tmp")
            try:
                temporary.write_text(json.dumps(summary, separators=(",", ":")), encoding="utf-8")
                os.replace(temporary, summary_path)
            finally:
                with contextlib.suppress(OSError):
                    temporary.unlink()


def _exception_text(exc: BaseException) -> str:
    if isinstance(exc, BaseExceptionGroup):
        details = [_exception_text(item) for item in exc.exceptions]
        return "; ".join(item for item in details if item) or str(exc)
    return str(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        return asyncio.run(run(config))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(json.dumps({"type": "error", "error": str(exc)}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
