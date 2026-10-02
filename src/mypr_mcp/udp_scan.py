"""Low-level, Linux-native UDP probes used by the network scanner."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import errno
import random
import socket
import struct
import time
from typing import Any

from .scan_config import decode_payload, ensure_udp_available

_ERRQUEUE_HEADER = struct.Struct("=IBBBBII")
_SO_EE_ORIGIN_LOCAL = 1
_SO_EE_ORIGIN_ICMP = 2
_SO_EE_ORIGIN_ICMP6 = 3
_LOCAL_ERRORS = {
    errno.EMFILE,
    errno.ENFILE,
    errno.ENOBUFS,
    errno.ENOMEM,
    errno.EADDRNOTAVAIL,
    errno.EACCES,
    errno.EPERM,
}
_ROUTE_ERRORS = {errno.ENETUNREACH, errno.EHOSTUNREACH}


def _dns_payload() -> bytes:
    transaction_id = random.SystemRandom().randrange(0, 65536)
    header = struct.pack("!HHHHHH", transaction_id, 0, 1, 0, 0, 0)
    qname = b"\x04mypr\x07invalid\x00"
    return header + qname + struct.pack("!HH", 1, 1)


def _ntp_payload() -> bytes:
    packet = bytearray(48)
    packet[0] = 0x23
    now = time.time() + 2_208_988_800
    seconds = int(now)
    fraction = int((now - seconds) * (1 << 32)) & 0xFFFFFFFF
    struct.pack_into("!II", packet, 40, seconds & 0xFFFFFFFF, fraction)
    return bytes(packet)


def build_payload(port: int, config: dict[str, Any]) -> tuple[bytes, str]:
    """Return the datagram and the profile name that produced it."""

    payload_b64 = config.get("payload_b64")
    if payload_b64 is not None:
        return decode_payload(payload_b64) or b"", "custom"
    probe = config.get("probe", "auto")
    if probe == "auto":
        if port == 53:
            return _dns_payload(), "dns"
        if port == 123:
            return _ntp_payload(), "ntp"
        return b"", "empty"
    if probe == "empty":
        return b"", "empty"
    if probe == "dns":
        return _dns_payload(), "dns"
    if probe == "ntp":
        return _ntp_payload(), "ntp"
    raise ValueError("probe must be auto, empty, dns, or ntp")


def _family_name(family: int) -> str:
    return "ipv4" if family == socket.AF_INET else "ipv6"


def _endpoint(sockaddr: tuple, port: int) -> tuple:
    if len(sockaddr) >= 4:
        return (sockaddr[0], port, sockaddr[2], sockaddr[3])
    return (sockaddr[0], port)


def _base_result(host: str, port: int, family: int, sockaddr: tuple, probe: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "host": host,
        "address": sockaddr[0],
        "port": port,
        "family": _family_name(family),
        "protocol": "udp",
        "probe": probe,
    }
    if family == socket.AF_INET6 and len(sockaddr) > 3 and sockaddr[3]:
        result["scope_id"] = sockaddr[3]
    return result


def _set_error_queue(sock: socket.socket, family: int) -> bool:
    option = "IP_RECVERR" if family == socket.AF_INET else "IPV6_RECVERR"
    value = getattr(socket, option, None)
    if value is None:
        raise RuntimeError(f"UDP error queue option {option} is unavailable")
    level = socket.IPPROTO_IP if family == socket.AF_INET else socket.IPPROTO_IPV6
    try:
        sock.setsockopt(level, value, 1)
    except OSError as exc:
        if exc.errno in _LOCAL_ERRORS:
            raise
        if exc.errno in {errno.ENOPROTOOPT, errno.EINVAL, errno.ENOSYS}:
            raise RuntimeError(f"UDP error queue option {option} is unsupported") from exc
        raise RuntimeError(f"unable to enable UDP error queue option {option}") from exc
    return True


def _offender(data: bytes, family: int) -> dict[str, Any] | None:
    if len(data) <= _ERRQUEUE_HEADER.size:
        return None
    offset = _ERRQUEUE_HEADER.size
    try:
        if family == socket.AF_INET and len(data) >= offset + 16:
            offender_family, offender_port, raw_address = struct.unpack_from(
                "=HH4s", data, offset
            )
            if offender_family != socket.AF_INET:
                return None
            return {
                "address": socket.inet_ntop(socket.AF_INET, raw_address),
                "port": socket.ntohs(offender_port),
            }
        if family == socket.AF_INET6 and len(data) >= offset + 28:
            offender_family, offender_port, _flow, raw_address, scope_id = struct.unpack_from(
                "=HHI16sI", data, offset
            )
            if offender_family != socket.AF_INET6:
                return None
            entry = {
                "address": socket.inet_ntop(socket.AF_INET6, raw_address),
                "port": socket.ntohs(offender_port),
            }
            if scope_id:
                entry["scope_id"] = scope_id
            return entry
    except (OSError, struct.error):
        return None
    return None


def _matches_destination(destination: tuple | None, endpoint: tuple, family: int) -> bool:
    if not destination:
        return True
    if destination[0] != endpoint[0]:
        return False
    if len(destination) > 1 and destination[1] not in (0, endpoint[1]):
        return False
    if family == socket.AF_INET6 and len(endpoint) > 3:
        return len(destination) <= 3 or destination[3] in (0, endpoint[3])
    return True


def _error_cmsg_type(family: int) -> int | None:
    return getattr(socket, "IP_RECVERR" if family == socket.AF_INET else "IPV6_RECVERR", None)


def parse_error_queue(
    ancdata: list[tuple[int, int, bytes]],
    *,
    family: int,
    sockaddr: tuple,
    destination: tuple | None = None,
    message_flags: int = 0,
) -> dict[str, Any] | None:
    """Parse one Linux extended-error message into a scan result fragment."""

    if message_flags & getattr(socket, "MSG_CTRUNC", 0):
        raise RuntimeError("truncated UDP error queue control data")
    expected_level = socket.IPPROTO_IP if family == socket.AF_INET else socket.IPPROTO_IPV6
    expected_type = _error_cmsg_type(family)
    matching = [
        data for level, kind, data in ancdata
        if level == expected_level and (expected_type is None or kind == expected_type)
    ]
    if not matching:
        return None
    data = matching[0]
    if len(data) < _ERRQUEUE_HEADER.size:
        raise RuntimeError("malformed UDP error queue message")
    ee_errno, origin, icmp_type, icmp_code, _pad, _info, _ee_data = (
        _ERRQUEUE_HEADER.unpack_from(data)
    )
    offender = _offender(data, family)
    icmp = {
        "origin": origin,
        "type": icmp_type,
        "code": icmp_code,
        "errno": ee_errno,
    }
    if offender is not None:
        icmp["offender"] = offender
    if destination is not None and not _matches_destination(destination, sockaddr, family):
        return {
            "state": "unreachable",
            "reason_code": "error_destination_mismatch",
            "icmp": icmp,
        }
    if origin == _SO_EE_ORIGIN_LOCAL:
        if ee_errno in _LOCAL_ERRORS:
            raise OSError(ee_errno, errno.errorcode.get(ee_errno, "local UDP error"))
        return {
            "state": "unreachable",
            "reason_code": errno.errorcode.get(ee_errno, "local_error"),
            "icmp": icmp,
        }
    expected_origin = _SO_EE_ORIGIN_ICMP if family == socket.AF_INET else _SO_EE_ORIGIN_ICMP6
    if origin != expected_origin:
        raise RuntimeError(f"unsupported UDP error queue origin {origin}")
    if family == socket.AF_INET and icmp_type == 3 and icmp_code == 3:
        state, reason = "closed", "icmp_port_unreachable"
    elif family == socket.AF_INET and icmp_type == 3 and icmp_code in {9, 10, 13}:
        state, reason = "filtered", "icmp_filtered"
    elif family == socket.AF_INET6 and icmp_type == 1 and icmp_code == 4:
        state, reason = "closed", "icmp_port_unreachable"
    elif family == socket.AF_INET6 and icmp_type == 1 and icmp_code in {1, 5, 6}:
        state, reason = "filtered", "icmp_filtered"
    else:
        state, reason = "unreachable", "icmp_unreachable"
    return {"state": state, "reason_code": reason, "icmp": icmp}


def _read_error_queue(
    sock: socket.socket,
    *,
    family: int,
    sockaddr: tuple,
    destination: tuple | None = None,
) -> dict[str, Any] | None:
    flags = getattr(socket, "MSG_ERRQUEUE", 0) | getattr(socket, "MSG_DONTWAIT", 0)
    if not flags:
        return None
    try:
        _data, ancdata, msg_flags, address = sock.recvmsg_into([bytearray(1)], 256, flags)
    except BlockingIOError:
        return None
    except OSError as exc:
        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
            return None
        raise
    return parse_error_queue(
        ancdata,
        family=family,
        sockaddr=sockaddr,
        destination=address if address else destination,
        message_flags=msg_flags,
    )


async def _wait_for_datagram(
    sock: socket.socket,
    *,
    family: int,
    sockaddr: tuple,
    endpoint: tuple,
    timeout: float,  # noqa: ASYNC109
    response_bytes: int,
) -> tuple[str, Any]:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[tuple[str, Any]] = loop.create_future()
    normal_flags = getattr(socket, "MSG_TRUNC", 0) | getattr(socket, "MSG_DONTWAIT", 0)

    def complete(value: tuple[str, Any]) -> None:
        if not future.done():
            future.set_result(value)

    def read_ready() -> None:
        if future.done():
            return
        pending_error: OSError | None = None
        try:
            buffer = bytearray(max(1, response_bytes))
            count, ancdata, flags, address = sock.recvmsg_into([buffer], 0, normal_flags)
        except BlockingIOError:
            pass
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                pass
            else:
                pending_error = exc
        else:
            complete(
                ("response", (count, bytes(buffer[: min(count, len(buffer))]), flags, address))
            )
            return
        try:
            error = _read_error_queue(
                sock,
                family=family,
                sockaddr=endpoint,
                destination=None,
            )
        except BaseException as exc:
            future.set_exception(exc)
        else:
            if error is not None or pending_error is not None:
                try:
                    buffer = bytearray(max(1, response_bytes))
                    count, ancdata, flags, address = sock.recvmsg_into(
                        [buffer], 0, normal_flags
                    )
                except BlockingIOError:
                    complete(("error", error) if error is not None else ("os_error", pending_error))
                except OSError as retry_error:
                    if error is not None:
                        complete(("error", error))
                    elif retry_error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        complete(("os_error", pending_error))
                    else:
                        future.set_exception(retry_error)
                else:
                    complete(
                        (
                            "response",
                            (count, bytes(buffer[: min(count, len(buffer))]), flags, address),
                        )
                    )

    try:
        loop.add_reader(sock.fileno(), read_ready)
    except (NotImplementedError, RuntimeError) as exc:
        raise RuntimeError("native UDP scanning requires an event loop with add_reader") from exc
    try:
        await asyncio.wait_for(asyncio.shield(future), timeout)
        return future.result()
    finally:
        loop.remove_reader(sock.fileno())
        if not future.done():
            future.cancel()


def _attach_response(
    result: dict[str, Any], event: tuple[str, Any], config: dict[str, Any]
) -> None:
    count, data, flags, _address = event[1]
    result.update(state="open", reason_code="udp_response", response_bytes=count)
    if config.get("capture_response", False):
        kept = data[: int(config.get("response_bytes", 1024))]
        result["response_b64"] = base64.b64encode(kept).decode("ascii")
        result["response_truncated"] = (
            bool(flags & getattr(socket, "MSG_TRUNC", 0)) or count > len(kept)
        )


async def probe(
    host: str,
    port: int,
    timeout: float,  # noqa: ASYNC109
    *,
    family: int,
    sockaddr: tuple,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Send one UDP datagram and classify the first response or ICMP error."""

    ensure_udp_available()
    payload, profile = build_payload(port, config)
    result = _base_result(host, port, family, sockaddr, profile)
    started = time.monotonic()
    sock: socket.socket | None = None
    endpoint = _endpoint(sockaddr, port)
    try:
        sock = socket.socket(family, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setblocking(False)
        _set_error_queue(sock, family)
        loop = asyncio.get_running_loop()
        async with asyncio.timeout(timeout):
            await loop.sock_connect(sock, endpoint)
            waiter = asyncio.create_task(
                _wait_for_datagram(
                    sock,
                    family=family,
                    sockaddr=sockaddr,
                    endpoint=endpoint,
                    timeout=timeout,
                    response_bytes=int(config.get("response_bytes", 1024)),
                )
            )
            try:
                await loop.sock_sendto(sock, payload, endpoint)
                event = await waiter
            finally:
                if not waiter.done():
                    waiter.cancel()
                    await asyncio.gather(waiter, return_exceptions=True)
        if event[0] == "response":
            _attach_response(result, event, config)
        elif event[0] == "error":
            result.update(event[1])
        else:
            exc = event[1]
            if exc.errno == errno.ECONNREFUSED:
                result.update(state="closed", reason_code="connection_refused", errno=exc.errno)
            else:
                raise exc
    except TimeoutError:
        result.update(state="open|filtered", reason_code="no_response")
    except OSError as exc:
        if exc.errno in _LOCAL_ERRORS:
            raise
        if exc.errno == errno.ECONNREFUSED:
            error = (
                _read_error_queue(sock, family=family, sockaddr=endpoint)
                if sock is not None
                else None
            )
            if error is not None:
                result.update(error)
            else:
                result.update(state="closed", reason_code="connection_refused", errno=exc.errno)
        elif exc.errno in _ROUTE_ERRORS:
            result.update(
                state="unreachable",
                reason_code=errno.errorcode.get(exc.errno, "route_error"),
                errno=exc.errno,
            )
        else:
            raise
    finally:
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.close()
    result["duration_ms"] = round((time.monotonic() - started) * 1000, 3)
    return result
