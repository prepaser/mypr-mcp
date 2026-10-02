from __future__ import annotations

import base64
import socket
from types import SimpleNamespace

import pytest

from mypr_mcp import scan_config


def test_tcp_defaults_and_extra_fields_are_preserved():
    result = scan_config.normalize_native_config({"mode": "tcp", "marker": {"id": 3}})

    assert result["mode"] == result["protocol"] == "tcp"
    assert result["concurrency"] == 64
    assert result["rate"] == 200
    assert result["timeout"] == 1.0
    assert result["retries"] == 0
    assert result["per_host_rate"] is None
    assert result["max_probes"] == 1_000_000
    assert result["max_duration"] == 3600.0
    assert result["payload_b64"] is None
    assert result["marker"] == {"id": 3}


def test_udp_defaults_and_explicit_none_limits(monkeypatch):
    monkeypatch.setattr(scan_config, "ensure_udp_available", lambda: None)

    result = scan_config.normalize_native_config(
        {"mode": "udp", "max_probes": None, "max_duration": None}
    )

    assert result["protocol"] == "udp"
    assert result["concurrency"] == 16
    assert result["rate"] == 20
    assert result["timeout"] == 2.0
    assert result["retries"] == 1
    assert result["per_host_rate"] == 1.0
    assert result["max_probes"] is None
    assert result["max_duration"] is None
    assert result["probe"] == "auto"


def test_mode_can_supply_missing_protocol_and_must_match_it(monkeypatch):
    monkeypatch.setattr(scan_config, "ensure_udp_available", lambda: None)

    assert scan_config.normalize_native_config({"protocol": "udp"})["mode"] == "udp"
    with pytest.raises(ValueError, match="mode and protocol"):
        scan_config.normalize_native_config({"mode": "tcp", "protocol": "udp"})


def test_payload_round_trip_preserves_binary_and_empty_values():
    payload = bytes(range(256))
    encoded = scan_config.encode_payload(payload)
    assert scan_config.decode_payload(encoded) == payload
    assert scan_config.decode_payload(scan_config.encode_payload(b"")) == b""
    assert scan_config.encode_payload(None) is None
    assert scan_config.decode_payload(None) is None


def test_udp_payload_is_only_allowed_with_auto_probe(monkeypatch):
    monkeypatch.setattr(scan_config, "ensure_udp_available", lambda: None)
    encoded = scan_config.encode_payload(b"probe")
    result = scan_config.normalize_native_config(
        {"mode": "udp", "payload_b64": encoded, "probe": "auto"}
    )
    assert result["payload_b64"] == encoded
    with pytest.raises(ValueError, match="auto UDP probe"):
        scan_config.normalize_native_config(
            {"mode": "udp", "payload_b64": encoded, "probe": "dns"}
        )
    with pytest.raises(ValueError, match="only available for UDP"):
        scan_config.normalize_native_config({"mode": "tcp", "payload_b64": encoded})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("concurrency", True),
        ("concurrency", 0),
        ("rate", float("nan")),
        ("timeout", float("inf")),
        ("retries", True),
        ("retries", 4),
        ("family", "local"),
        ("max_probes", 0),
        ("max_duration", False),
        ("open_only", 1),
        ("capture_response", "yes"),
        ("banner_bytes", 4097),
        ("response_bytes", 0),
    ],
)
def test_common_options_are_strictly_validated(name, value):
    with pytest.raises((TypeError, ValueError)):
        scan_config.normalize_native_config({name: value})


def test_tcp_and_udp_specific_combinations_are_rejected(monkeypatch):
    monkeypatch.setattr(scan_config, "ensure_udp_available", lambda: None)
    with pytest.raises(ValueError, match="capture_response"):
        scan_config.normalize_native_config({"capture_response": True})
    with pytest.raises(ValueError, match="response_bytes"):
        scan_config.normalize_native_config({"response_bytes": 2048})
    with pytest.raises(ValueError, match="banner options"):
        scan_config.normalize_native_config(
            {"mode": "udp", "banner": True, "banner_timeout": 1.0}
        )
    with pytest.raises(ValueError, match="banner options"):
        scan_config.normalize_native_config({"mode": "udp", "banner": True})
    with pytest.raises(ValueError, match="banner options"):
        scan_config.normalize_native_config({"mode": "udp", "banner_timeout": 1.0})
    with pytest.raises(ValueError, match="banner options"):
        scan_config.normalize_native_config({"mode": "udp", "banner_bytes": 2048})
    with pytest.raises(ValueError, match="probe"):
        scan_config.normalize_native_config({"probe": "dns"})


@pytest.mark.parametrize("value", ["not-base64", "====", "YQ", "é"])
def test_payload_wire_encoding_is_strict(value):
    with pytest.raises(ValueError):
        scan_config.decode_payload(value)
    with pytest.raises(TypeError):
        scan_config.decode_payload(1)


def test_payload_wire_size_and_type_limits():
    with pytest.raises(ValueError):
        scan_config.encode_payload(b"x" * 4097)
    with pytest.raises(TypeError):
        scan_config.encode_payload(bytearray(b"x"))
    with pytest.raises(ValueError):
        scan_config.decode_payload("A" * 5465)
    with pytest.raises(ValueError):
        scan_config.decode_payload(base64.b64encode(b"x" * 4097).decode())


@pytest.mark.parametrize("name", ["banner", "open_only", "capture_response"])
def test_explicit_none_is_not_a_boolean_default(name):
    with pytest.raises(TypeError, match=name):
        scan_config.normalize_native_config({name: None})


def test_udp_capability_check_does_not_create_socket():
    scan_config.ensure_udp_available()


def test_udp_capability_check_reports_missing_features(monkeypatch):
    monkeypatch.setattr(scan_config.sys, "platform", "darwin")
    with pytest.raises(RuntimeError, match="Linux"):
        scan_config.ensure_udp_available()

    monkeypatch.setattr(scan_config.sys, "platform", "linux")
    class SocketWithoutRecvmsg:
        def recvmsg_into(self):
            pass

    monkeypatch.setattr(
        scan_config,
        "socket",
        SimpleNamespace(
            socket=SocketWithoutRecvmsg,
            IP_RECVERR=11,
            IPV6_RECVERR=25,
            MSG_ERRQUEUE=8192,
            MSG_TRUNC=32,
        ),
    )
    with pytest.raises(RuntimeError, match="recvmsg"):
        scan_config.ensure_udp_available()


@pytest.mark.parametrize("name", ["IP_RECVERR", "IPV6_RECVERR", "MSG_ERRQUEUE", "MSG_TRUNC"])
def test_udp_capability_check_reports_missing_constants(monkeypatch, name):
    values = {
        "IP_RECVERR": 11,
        "IPV6_RECVERR": 25,
        "MSG_ERRQUEUE": 8192,
        "MSG_TRUNC": 32,
    }
    values.pop(name)
    monkeypatch.setattr(
        scan_config,
        "socket",
        SimpleNamespace(
            socket=socket.socket,
            **values,
        ),
    )
    with pytest.raises(RuntimeError, match=name):
        scan_config.ensure_udp_available()
