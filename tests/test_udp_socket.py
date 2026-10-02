from __future__ import annotations

import asyncio
import base64
import errno
import socket
import struct

import pytest

from mypr_mcp import udp_scan


def _config(**values):
    config = {
        "probe": "auto",
        "payload_b64": None,
        "capture_response": False,
        "response_bytes": 1024,
    }
    config.update(values)
    return config


def test_profiles_and_payload_limits():
    payload, profile = udp_scan.build_payload(53, _config())
    assert profile == "dns"
    assert struct.unpack_from("!HHHHHH", payload)[1] == 0
    assert payload.endswith(b"\x00\x00\x01\x00\x01")

    payload, profile = udp_scan.build_payload(123, _config())
    assert profile == "ntp"
    assert len(payload) == 48
    assert payload[0] == 0x23

    custom = b"\x00\xffhello"
    payload, profile = udp_scan.build_payload(
        80, _config(payload_b64=base64.b64encode(custom).decode())
    )
    assert payload == custom
    assert profile == "custom"
    with pytest.raises(ValueError, match="4096"):
        udp_scan.decode_payload(base64.b64encode(b"x" * 4097).decode())
    with pytest.raises(ValueError, match="valid Base64"):
        udp_scan.decode_payload("!")


def _cmsg(family: int, *, origin: int, icmp_type: int, code: int, error: int = 0, address=None):
    data = bytearray(struct.pack("=IBBBBII", error, origin, icmp_type, code, 0, 0, 0))
    if address is not None:
        if family == socket.AF_INET:
            data.extend(
                struct.pack("=HH4s8x", socket.AF_INET, 0, socket.inet_pton(family, address))
            )
        else:
            data.extend(
                struct.pack(
                    "=HHI16sI4x", socket.AF_INET6, 0, 0,
                    socket.inet_pton(family, address), 0,
                )
            )
    level = socket.IPPROTO_IP if family == socket.AF_INET else socket.IPPROTO_IPV6
    kind = getattr(socket, "IP_RECVERR" if family == socket.AF_INET else "IPV6_RECVERR", 11)
    return [(level, kind, bytes(data))]


def test_error_queue_maps_ipv4_and_ipv6_states():
    peer4 = ("127.0.0.1", 9999)
    row = udp_scan.parse_error_queue(
        _cmsg(
            socket.AF_INET, origin=2, icmp_type=3, code=3,
            error=errno.ECONNREFUSED, address=peer4[0],
        ),
        family=socket.AF_INET,
        sockaddr=peer4,
    )
    assert row["state"] == "closed"
    assert row["icmp"]["offender"]["address"] == peer4[0]

    row = udp_scan.parse_error_queue(
        _cmsg(socket.AF_INET, origin=2, icmp_type=3, code=13, address=peer4[0]),
        family=socket.AF_INET,
        sockaddr=peer4,
    )
    assert row["state"] == "filtered"

    peer6 = ("::1", 9999, 0, 0)
    row = udp_scan.parse_error_queue(
        _cmsg(socket.AF_INET6, origin=3, icmp_type=1, code=4, address=peer6[0]),
        family=socket.AF_INET6,
        sockaddr=peer6,
    )
    assert row["state"] == "closed"


def test_error_queue_keeps_router_offender_and_checks_original_destination():
    peer = ("127.0.0.1", 9999)
    row = udp_scan.parse_error_queue(
        _cmsg(socket.AF_INET, origin=2, icmp_type=3, code=13, address="127.0.0.2"),
        family=socket.AF_INET,
        sockaddr=peer,
        destination=peer,
    )
    assert row["state"] == "filtered"
    assert row["icmp"]["offender"]["address"] == "127.0.0.2"

    row = udp_scan.parse_error_queue(
        _cmsg(socket.AF_INET, origin=2, icmp_type=3, code=13, address="127.0.0.2"),
        family=socket.AF_INET,
        sockaddr=peer,
        destination=(peer[0], peer[1] + 1),
    )
    assert row["state"] == "unreachable"
    assert row["reason_code"] == "error_destination_mismatch"


def test_error_queue_rejects_malformed_or_unknown_evidence():
    with pytest.raises(RuntimeError, match="malformed"):
        udp_scan.parse_error_queue(
            [(socket.IPPROTO_IP, getattr(socket, "IP_RECVERR", 11), b"short")],
            family=socket.AF_INET,
            sockaddr=("127.0.0.1", 9999),
        )
    with pytest.raises(RuntimeError, match="origin"):
        udp_scan.parse_error_queue(
            _cmsg(socket.AF_INET, origin=99, icmp_type=3, code=3),
            family=socket.AF_INET,
            sockaddr=("127.0.0.1", 9999),
        )
    with pytest.raises(RuntimeError, match="truncated"):
        udp_scan.parse_error_queue(
            _cmsg(socket.AF_INET, origin=2, icmp_type=3, code=3),
            family=socket.AF_INET,
            sockaddr=("127.0.0.1", 9999),
            message_flags=socket.MSG_CTRUNC,
        )


@pytest.mark.asyncio
async def test_wait_prioritizes_remote_error_over_normal_eacces():
    trigger, peer_trigger = socket.socketpair()

    class FakeSocket:
        def fileno(self):
            return trigger.fileno()

        def recvmsg_into(self, _buffers, _ancillary, flags):
            if flags & socket.MSG_ERRQUEUE:
                return (
                    0,
                    _cmsg(
                        socket.AF_INET,
                        origin=2,
                        icmp_type=3,
                        code=13,
                        address="127.0.0.2",
                    ),
                    0,
                    ("127.0.0.1", 9999),
                )
            raise PermissionError(errno.EACCES, "remote policy")

    try:
        task = asyncio.create_task(
            udp_scan._wait_for_datagram(
                FakeSocket(),
                family=socket.AF_INET,
                sockaddr=("127.0.0.1", 9999),
                endpoint=("127.0.0.1", 9999),
                timeout=1,
                response_bytes=8,
            )
        )
        await asyncio.sleep(0)
        peer_trigger.send(b"x")
        kind, row = await task
        assert kind == "error"
        assert row["state"] == "filtered"
    finally:
        trigger.close()
        peer_trigger.close()


@pytest.mark.asyncio
async def test_wait_keeps_local_eacces_fatal():
    trigger, peer_trigger = socket.socketpair()

    class FakeSocket:
        def fileno(self):
            return trigger.fileno()

        def recvmsg_into(self, _buffers, _ancillary, flags):
            if flags & socket.MSG_ERRQUEUE:
                return (
                    0,
                    _cmsg(socket.AF_INET, origin=1, icmp_type=0, code=0, error=errno.EACCES),
                    0,
                    ("127.0.0.1", 9999),
                )
            raise PermissionError(errno.EACCES, "local policy")

    try:
        task = asyncio.create_task(
            udp_scan._wait_for_datagram(
                FakeSocket(),
                family=socket.AF_INET,
                sockaddr=("127.0.0.1", 9999),
                endpoint=("127.0.0.1", 9999),
                timeout=1,
                response_bytes=8,
            )
        )
        await asyncio.sleep(0)
        peer_trigger.send(b"x")
        with pytest.raises(PermissionError):
            await task
    finally:
        trigger.close()
        peer_trigger.close()


async def test_wait_reads_response_after_clearing_pending_icmp_error():
    trigger, peer_trigger = socket.socketpair()

    class FakeSocket:
        cleared = False

        def fileno(self):
            return trigger.fileno()

        def recvmsg_into(self, buffers, _ancillary, flags):
            if flags & socket.MSG_ERRQUEUE:
                self.cleared = True
                return 0, _cmsg(socket.AF_INET, origin=2, icmp_type=3, code=13), 0, (
                    "127.0.0.1", 9999
                )
            if not self.cleared:
                raise PermissionError(errno.EACCES, "pending remote error")
            buffers[0][:2] = b"ok"
            return 2, [], 0, ("127.0.0.1", 9999)

    try:
        task = asyncio.create_task(udp_scan._wait_for_datagram(
            FakeSocket(), family=socket.AF_INET, sockaddr=("127.0.0.1", 9999),
            endpoint=("127.0.0.1", 9999), timeout=1, response_bytes=8,
        ))
        await asyncio.sleep(0)
        peer_trigger.send(b"x")
        kind, response = await task
        assert kind == "response" and response[1] == b"ok"
    finally:
        trigger.close()
        peer_trigger.close()


@pytest.mark.asyncio
async def test_probe_echoes_empty_and_custom_payload():
    received = asyncio.Event()
    seen: list[bytes] = []

    class Echo(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            seen.append(data)
            self.transport.sendto(data, addr)
            received.set()

        def connection_made(self, transport):
            self.transport = transport

    loop = asyncio.get_running_loop()
    transport, _protocol = await loop.create_datagram_endpoint(
        Echo, local_addr=("127.0.0.1", 0)
    )
    try:
        port = transport.get_extra_info("sockname")[1]
        row = await udp_scan.probe(
            "127.0.0.1",
            port,
            1,
            family=socket.AF_INET,
            sockaddr=("127.0.0.1", 0),
            config=_config(payload_b64=base64.b64encode(b"hello").decode(), capture_response=True),
        )
        await asyncio.wait_for(received.wait(), 1)
        assert row["state"] == "open"
        assert row["probe"] == "custom"
        assert base64.b64decode(row["response_b64"]) == b"hello"
        assert seen == [b"hello"]
    finally:
        transport.close()


@pytest.mark.asyncio
async def test_probe_accepts_zero_length_response():
    received = asyncio.Event()

    class Empty(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, addr):
            self.transport.sendto(b"", addr)
            received.set()

    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(Empty, local_addr=("127.0.0.1", 0))
    try:
        port = transport.get_extra_info("sockname")[1]
        row = await udp_scan.probe(
            "127.0.0.1", port, 1, family=socket.AF_INET,
            sockaddr=("127.0.0.1", 0), config=_config(),
        )
        await asyncio.wait_for(received.wait(), 1)
        assert row["state"] == "open"
        assert row["response_bytes"] == 0
    finally:
        transport.close()


@pytest.mark.asyncio
async def test_probe_times_out_as_open_or_filtered():
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0)
    )
    try:
        port = transport.get_extra_info("sockname")[1]
    finally:
        transport.close()
    await asyncio.sleep(0)
    row = await udp_scan.probe(
        "127.0.0.1", port, 1, family=socket.AF_INET,
        sockaddr=("127.0.0.1", 0), config=_config(),
    )
    assert row["state"] == "closed"
    assert row["duration_ms"] >= 0


@pytest.mark.asyncio
async def test_probe_silent_bound_port_is_open_or_filtered():
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0)
    )
    try:
        port = transport.get_extra_info("sockname")[1]
        row = await udp_scan.probe(
            "127.0.0.1", port, 0.03, family=socket.AF_INET,
            sockaddr=("127.0.0.1", 0), config=_config(),
        )
        assert row["state"] == "open|filtered"
    finally:
        transport.close()


@pytest.mark.asyncio
async def test_probe_truncates_captured_response():
    class Echo(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, addr):
            self.transport.sendto(b"x" * 2048, addr)

    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        Echo, local_addr=("127.0.0.1", 0)
    )
    try:
        port = transport.get_extra_info("sockname")[1]
        row = await udp_scan.probe(
            "127.0.0.1", port, 1, family=socket.AF_INET,
            sockaddr=("127.0.0.1", 0),
            config=_config(capture_response=True, response_bytes=8),
        )
        assert row["state"] == "open"
        assert row["response_bytes"] == 2048
        assert len(base64.b64decode(row["response_b64"])) == 8
        assert row["response_truncated"] is True
    finally:
        transport.close()


@pytest.mark.asyncio
async def test_probe_cancellation_closes_its_waiter():
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0)
    )
    try:
        port = transport.get_extra_info("sockname")[1]
        task = asyncio.create_task(
            udp_scan.probe(
                "127.0.0.1", port, 10, family=socket.AF_INET,
                sockaddr=("127.0.0.1", 0), config=_config(),
            )
        )
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        assert not [item for item in asyncio.all_tasks() if item is not asyncio.current_task()]
    finally:
        transport.close()


@pytest.mark.asyncio
async def test_probe_ipv6_loopback_when_available():
    try:
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("::1", 0)
        )
    except OSError:
        pytest.skip("IPv6 loopback unavailable")
    try:
        port = transport.get_extra_info("sockname")[1]
        row = await udp_scan.probe(
            "::1", port, 1, family=socket.AF_INET6,
            sockaddr=("::1", 0, 0, 0), config=_config(),
        )
        assert row["state"] == "open|filtered"
    finally:
        transport.close()
