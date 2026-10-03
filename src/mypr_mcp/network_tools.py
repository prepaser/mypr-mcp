"""Small, bounded network diagnostics for the workspace Python API."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import signal
import socket
import ssl
import sys
import time
from pathlib import Path
from typing import Any

_DNS_WORKERS = asyncio.Semaphore(2)
_DNS_TIMEOUT = 5.0
_DNS_OUTPUT_LIMIT = 64 * 1024
_DNS_CLEANUP_TIMEOUT = 2.0


def _timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("timeout must be a positive number")
    value = float(value)
    if value <= 0 or value != value or value == float("inf"):
        raise ValueError("timeout must be positive and finite")
    return value


def _port(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 65535:
        raise ValueError("port must be an integer between 0 and 65535")
    return value


def _host(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.startswith("-"):
        raise ValueError("host must be a non-empty hostname or address")
    return value


def _fingerprint(value: str) -> str:
    value = value.lower().replace(":", "").replace(" ", "")
    if value.startswith("sha256="):
        value = value[7:]
    if value.startswith("sha256/"):
        value = value[7:]
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("fingerprint must be a SHA-256 hex fingerprint")
    return value


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    wait_task = asyncio.create_task(asyncio.wait_for(writer.wait_closed(), 1.0))
    try:
        await asyncio.shield(wait_task)
    except TimeoutError:
        wait_task.cancel()
        await asyncio.gather(wait_task, return_exceptions=True)
    except asyncio.CancelledError:
        wait_task.cancel()
        await asyncio.gather(wait_task, return_exceptions=True)
        raise
    except OSError:
        pass


async def _stop_dns_worker(
    process: asyncio.subprocess.Process, communication: asyncio.Task[Any] | None = None
) -> None:
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)

    async def wait() -> None:
        await process.wait()
        if communication is not None:
            await asyncio.gather(communication, return_exceptions=True)

    waiter = asyncio.create_task(wait())
    try:
        await asyncio.wait_for(asyncio.shield(waiter), _DNS_CLEANUP_TIMEOUT)
    except TimeoutError:
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(waiter), _DNS_CLEANUP_TIMEOUT)
    finally:
        if communication is not None and not communication.done():
            communication.cancel()
            await asyncio.gather(communication, return_exceptions=True)
        if not waiter.done():
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)


async def _resolve_worker(
    host: str,
    port: int | None,
    family: int,
    kind: int,
    proto: int,
    timeout: float,  # noqa: ASYNC109
) -> list[dict[str, Any]]:
    payload = json.dumps(
        {"host": host, "port": port, "family": family, "type": kind, "proto": proto},
        separators=(",", ":"),
    ).encode()
    guard = Path(__file__).with_name("process_guard.py")
    worker = Path(__file__).with_name("dns_worker.py")
    async with _DNS_WORKERS:
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                str(guard),
                "--parent-pid",
                str(os.getpid()),
                "--tree",
                "--",
                sys.executable,
                str(worker),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        )
        process: asyncio.subprocess.Process | None = None
        communication: asyncio.Task[Any] | None = None
        try:
            process = await asyncio.shield(launch)
            communication = asyncio.create_task(process.communicate(payload))
            try:
                output, errors = await asyncio.wait_for(
                    asyncio.shield(communication), timeout
                )
            except TimeoutError as exc:
                await _stop_dns_worker(process, communication)
                raise TimeoutError(f"DNS resolution exceeded its {timeout:g}-second limit") from exc
            except BaseException:
                await _stop_dns_worker(process, communication)
                raise
        except asyncio.CancelledError:
            if process is None:
                while True:
                    try:
                        process = await asyncio.shield(launch)
                        break
                    except asyncio.CancelledError:
                        if launch.done():
                            process = launch.result()
                            break
            if process.returncode is None:
                await _stop_dns_worker(process, communication)
            raise
        except BaseException:
            if process is None:
                with contextlib.suppress(Exception):
                    process = await asyncio.shield(launch)
            if process is not None and process.returncode is None:
                await _stop_dns_worker(process, communication)
            raise
    if len(output) > _DNS_OUTPUT_LIMIT:
        raise RuntimeError("DNS worker returned too much output")
    try:
        response = json.loads(output)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("DNS worker returned invalid JSON") from exc
    if not isinstance(response, dict):
        raise RuntimeError("DNS worker returned an invalid response")
    if response.get("ok") is not True:
        if response.get("kind") == "gaierror":
            errno = response.get("errno")
            message = str(response.get("error", "DNS resolution failed"))
            if errno is None:
                raise socket.gaierror(message)
            raise socket.gaierror(errno, message)
        raise OSError(str(response.get("error", "DNS resolution failed")))
    result = response.get("result")
    if not isinstance(result, list):
        raise RuntimeError("DNS worker returned an invalid result")
    return result


class NetworkTools:
    """Async DNS, native scan and TLS helpers plus managed scan delegation."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        tasks: Any = None,
        rpc: Any = None,
        *,
        ensure_dependencies: Any = None,
    ):
        self.workspace = Path(workspace).resolve()
        self.tasks = tasks
        self._rpc = rpc
        from .system_tools import SystemTools

        self._system = SystemTools(self.workspace, ensure_dependencies=ensure_dependencies)

    async def sockets(
        self,
        *,
        protocol: str | None = None,
        local_address: str | None = None,
        local_port: int | None = None,
        remote_address: str | None = None,
        remote_port: int | None = None,
        state: str | None = None,
        pid: int | None = None,
        cursor: str | None = None,
        limit: int = 20,
        timeout: float = 5.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        """List local sockets with snapshot paging.

        Filters are exact matches. `protocol` accepts tcp, udp, or unix;
        address and port filters apply to the local or remote endpoint, while
        `state` and `pid` filter connection state and owner. The first page
        defaults to 20 records (limit 1-50). Pass only `cursor` to continue
        the same snapshot; snapshots expire after 60 seconds. Each page is
        capped at 32 KiB. `timeout` defaults to 5 seconds.
        """
        return await self._system.sockets(
            protocol=protocol,
            local_address=local_address,
            local_port=local_port,
            remote_address=remote_address,
            remote_port=remote_port,
            state=state,
            pid=pid,
            cursor=cursor,
            limit=limit,
            timeout=timeout,
        )

    async def resolve(
        self,
        host: str,
        port: int | None = None,
        *,
        family: int = socket.AF_UNSPEC,
        type: int = socket.SOCK_STREAM,
        proto: int = 0,
        timeout: float = _DNS_TIMEOUT,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        _host(host)
        if port is not None:
            _port(port)
        timeout = _timeout(timeout)
        try:
            async with asyncio.timeout(timeout):
                result = await _resolve_worker(host, port, family, type, proto, timeout)
        except TimeoutError as exc:
            raise TimeoutError(f"DNS resolution exceeded its {timeout:g}-second limit") from exc
        addresses: list[dict[str, Any]] = []
        seen: set[tuple[int, str, int]] = set()
        for item in result:
            if not isinstance(item, dict):
                raise RuntimeError("DNS worker returned an invalid address")
            item_family = int(item["family"])
            canonname = item.get("canonical_name")
            sockaddr = item.get("sockaddr")
            if not isinstance(sockaddr, (list, tuple)) or not sockaddr:
                raise RuntimeError("DNS worker returned an invalid socket address")
            address = sockaddr[0]
            item_port = sockaddr[1] if len(sockaddr) > 1 else None
            key = (item_family, address, item_port or 0)
            if key in seen:
                continue
            seen.add(key)
            value: dict[str, Any] = {
                "family": socket.AddressFamily(item_family).name,
                "address": address,
            }
            if item_port is not None:
                value["port"] = item_port
            if canonname:
                value["canonical_name"] = canonname
            addresses.append(value)
        return {"host": host, "addresses": addresses}

    async def connect(  # noqa: ASYNC109
        self,
        host: str,
        port: int,
        *,
        timeout: float = 3.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        _host(host)
        _port(port)
        timeout = _timeout(timeout)
        started = time.monotonic()
        writer: asyncio.StreamWriter | None = None
        state = "error"
        reason: str | None = None
        address: Any = None
        try:
            async with asyncio.timeout(timeout):
                _, writer = await asyncio.open_connection(host, port)
                address = writer.get_extra_info("peername")
                state = "open"
        except TimeoutError:
            state, reason = "timeout", "connect_timeout"
        except socket.gaierror as exc:
            state, reason = "unreachable", str(exc)
        except OSError as exc:
            reason = str(exc)
            if exc.errno == 111:  # ECONNREFUSED
                state = "closed"
            elif exc.errno in {101, 113, 99}:  # ENETUNREACH/EHOSTUNREACH/EADDRNOTAVAIL
                state = "unreachable"
        finally:
            if writer is not None:
                await _close_writer(writer)
        value: dict[str, Any] = {
            "host": host,
            "port": port,
            "state": state,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
        }
        if address:
            value["address"] = address[0] if isinstance(address, tuple) else str(address)
        if reason:
            value["reason"] = reason
        return value

    async def tls(  # noqa: ASYNC109
        self,
        host: str,
        port: int = 443,
        *,
        timeout: float = 5.0,  # noqa: ASYNC109
        verify: bool = True,
        ssl_context: ssl.SSLContext | None = None,
        cert_pem: str | bytes | None = None,
        fingerprint: str | None = None,
        server_hostname: str | None = None,
    ) -> dict[str, Any]:
        _host(host)
        _port(port)
        timeout = _timeout(timeout)
        if type(verify) is not bool:
            raise TypeError("verify must be a boolean")
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise TypeError("ssl_context must be an SSLContext")
        if verify and ssl_context is not None:
            if ssl_context.verify_mode == ssl.CERT_NONE or not ssl_context.check_hostname:
                raise ValueError(
                    "verify=True requires certificate verification and hostname checks"
                )
        if verify:
            context = ssl_context or ssl.create_default_context()
        else:
            context = ssl_context or ssl._create_unverified_context()
        expected_fingerprint = _fingerprint(fingerprint) if fingerprint is not None else None
        expected_der: bytes | None = None
        if cert_pem is not None:
            if isinstance(cert_pem, str):
                cert_pem = cert_pem.encode("ascii")
            if not isinstance(cert_pem, bytes):
                raise TypeError("cert_pem must be text or bytes")
            try:
                encoded_der = ssl.PEM_cert_to_DER_cert(cert_pem.decode("ascii"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise ValueError("cert_pem is not a valid PEM certificate") from exc
            expected_der = (
                bytes.fromhex(encoded_der) if isinstance(encoded_der, str) else encoded_der
            )

        started = time.monotonic()
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(timeout):
                _, writer = await asyncio.open_connection(
                    host,
                    port,
                    ssl=context,
                    server_hostname=server_hostname or host,
                )
            ssl_object = writer.get_extra_info("ssl_object")
            if ssl_object is None:
                raise RuntimeError("TLS handshake did not produce an SSL object")
            der = ssl_object.getpeercert(binary_form=True) or b""
            actual_fingerprint = hashlib.sha256(der).hexdigest() if der else None
            if expected_der is not None and der != expected_der:
                raise ssl.SSLError("peer certificate does not match cert_pem")
            if expected_fingerprint is not None and actual_fingerprint != expected_fingerprint:
                raise ssl.SSLError("peer certificate fingerprint does not match")
            cipher = ssl_object.cipher()
            result: dict[str, Any] = {
                "host": host,
                "port": port,
                "verified": bool(
                    verify or expected_der is not None or expected_fingerprint is not None
                ),
                "version": ssl_object.version(),
                "cipher": cipher[0] if cipher else None,
                "certificate_fingerprint": actual_fingerprint,
                "duration_ms": round((time.monotonic() - started) * 1000, 3),
            }
            if der:
                result["certificate_pem"] = ssl.DER_cert_to_PEM_cert(der)
            return result
        finally:
            if writer is not None:
                await _close_writer(writer)

    async def scan(
        self,
        targets: str | list[str],
        ports: Any = "1-1024",
        *,
        protocol: str = "tcp",
        concurrency: int | None = None,
        rate: float | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
        max_probes: int | None = 1_000_000,
        max_duration: float | None = 3600.0,
        continue_after_output_limit: bool = False,
        family: str = "any",
        retries: int | None = None,
        banner: bool = False,
        banner_timeout: float = 0.5,
        banner_bytes: int = 1024,
        open_only: bool = False,
        per_host_rate: float | None = None,
        probe: str | None = None,
        payload: bytes | None = None,
        capture_response: bool = False,
        response_bytes: int = 1024,
    ) -> Any:
        return await self._delegate(
            "scan", targets=targets, ports=ports, concurrency=concurrency, rate=rate,
            timeout=timeout, max_probes=max_probes, max_duration=max_duration,
            continue_after_output_limit=continue_after_output_limit, family=family,
            retries=retries, banner=banner, banner_timeout=banner_timeout,
            banner_bytes=banner_bytes, open_only=open_only, protocol=protocol,
            per_host_rate=per_host_rate, probe=probe, payload=payload,
            capture_response=capture_response, response_bytes=response_bytes,
        )

    async def nmap(
        self,
        targets: str | list[str],
        args: list[str] | None = None,
        *,
        max_duration: float | None = 3600.0,
        continue_after_output_limit: bool = False,
    ) -> Any:
        return await self._delegate(
            "nmap", targets=targets, args=args or [], max_duration=max_duration,
            continue_after_output_limit=continue_after_output_limit,
        )

    async def _delegate(self, kind: str, **options: Any) -> Any:
        """Load the manager scan implementation only when a scan is requested."""
        try:
            from . import scan_api
        except ImportError as exc:
            raise RuntimeError("network scan support is not available") from exc
        manager_net = scan_api.Net(self._rpc, self.tasks)
        if kind == "scan":
            return await manager_net.scan(
                options.pop("targets"),
                ports=options.pop("ports", "1-1024"),
                **options,
            )
        return await manager_net.nmap(
            options.pop("targets"), args=options.pop("args", []), **options
        )


__all__ = ["NetworkTools"]
