"""Small, bounded network diagnostics for the workspace Python API."""

from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import ssl
import time
from pathlib import Path
from typing import Any


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


class NetworkTools:
    """Async DNS, TCP and TLS helpers plus managed scan delegation."""

    def __init__(self, workspace: str | os.PathLike[str], tasks: Any = None, rpc: Any = None):
        self.workspace = Path(workspace).resolve()
        self.tasks = tasks
        self.rpc = rpc

    async def resolve(
        self,
        host: str,
        port: int | None = None,
        *,
        family: int = socket.AF_UNSPEC,
        type: int = socket.SOCK_STREAM,
        proto: int = 0,
    ) -> dict[str, Any]:
        _host(host)
        if port is not None:
            _port(port)
        result = await asyncio.to_thread(
            socket.getaddrinfo,
            host,
            port,
            family,
            type,
            proto,
            0,
        )
        addresses: list[dict[str, Any]] = []
        seen: set[tuple[int, str, int]] = set()
        for item_family, _item_type, _item_proto, canonname, sockaddr in result:
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

    async def scan(self, targets: Any, ports: Any = "1-1024", **options: Any) -> Any:
        return await self._delegate("scan", targets=targets, ports=ports, **options)

    async def nmap(self, targets: Any, args: list[str] | None = None) -> Any:
        return await self._delegate("nmap", targets=targets, args=args or [])

    async def _delegate(self, kind: str, **options: Any) -> Any:
        """Load the manager scan implementation only when a scan is requested."""
        try:
            from . import scan_api
        except ImportError as exc:
            raise RuntimeError("network scan support is not available") from exc
        manager_net = scan_api.Net(self.rpc, self.tasks)
        if kind == "scan":
            return await manager_net.scan(
                options.pop("targets"),
                ports=options.pop("ports", "1-1024"),
                **options,
            )
        return await manager_net.nmap(options.pop("targets"), args=options.pop("args", []))


__all__ = ["NetworkTools"]
