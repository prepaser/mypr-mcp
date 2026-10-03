import inspect

import pytest

from mypr_mcp.config import ConfigError, validate_servers
from mypr_mcp.kernel_api import Workspace
from mypr_mcp.network_tools import NetworkTools
from mypr_mcp.scan_api import Net


@pytest.mark.parametrize("url", ["http://host:notaport", "http://host:99999", "http://[bad"])
def test_invalid_mcp_url_has_configuration_field_path(url):
    with pytest.raises(ConfigError) as failure:
        validate_servers({"sample": {"url": url}})
    assert failure.value.path == "mcp.servers.sample.url"


@pytest.mark.parametrize("method", ["scan", "nmap"])
def test_scan_helper_defaults_match_delegate_and_are_discoverable(tmp_path, method):
    ws = Workspace(tmp_path)
    public = inspect.signature(getattr(ws.net, method)).parameters
    delegate = inspect.signature(getattr(Net, method)).parameters
    assert all(parameter.kind != inspect.Parameter.VAR_KEYWORD for parameter in public.values())
    for name, parameter in delegate.items():
        if name != "self":
            assert public[name].default == parameter.default
    text = ws.help(f"net.{method}")
    assert "max_duration" in text
    assert "continue_after_output_limit" in text


async def test_scan_helper_forwards_explicit_limits_and_positional_ports(tmp_path, monkeypatch):
    calls = []

    async def delegate(kind, **kwargs):
        calls.append((kind, kwargs))
        return "scan-id"

    net = NetworkTools(tmp_path)
    monkeypatch.setattr(net, "_delegate", delegate)
    assert await net.scan(
        "127.0.0.1", "80", concurrency=2, rate=10, timeout=0.1, max_probes=1,
        max_duration=1, continue_after_output_limit=True,
    ) == "scan-id"
    expected = {
        "targets": "127.0.0.1", "ports": "80", "concurrency": 2, "rate": 10,
        "timeout": 0.1, "max_probes": 1, "max_duration": 1,
        "continue_after_output_limit": True,
    }
    assert len(calls) == 1
    kind, params = calls[0]
    assert kind == "scan"
    assert {name: params[name] for name in expected} == expected
