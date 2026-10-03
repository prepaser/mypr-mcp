from __future__ import annotations

from types import SimpleNamespace

import pytest

from mypr_mcp.doctor import _service_status, _web_readiness


def test_web_readiness_reports_provider_references_without_secret_values(monkeypatch):
    secret = "kagi-secret-that-must-not-be-returned"
    monkeypatch.setenv("MYPR_KAGI_KEY", secret)
    monkeypatch.delenv("MYPR_BRAVE_KEY", raising=False)

    result = _web_readiness(
        {
            "default_provider": "kagi",
            "providers": {
                "kagi": {"api_key_env": "MYPR_KAGI_KEY"},
                "brave": {"api_key_env": "MYPR_BRAVE_KEY"},
            },
        }
    )

    assert result["configured"] is True
    assert result["default_provider"] == "kagi"
    assert result["providers"]["kagi"] == {
        "configured": True,
        "credentials": {"source": "MYPR_KAGI_KEY", "available": True},
        "ready": True,
    }
    assert result["providers"]["brave"]["credentials"]["available"] is False
    assert result["providers"]["tavily"]["configured"] is False
    assert secret not in str(result)
    assert result["network_checked"] is False


@pytest.mark.asyncio
async def test_live_doctor_checks_web_provider_status_without_search_request():
    calls = []

    class Web:
        def providers(self):
            calls.append("providers")
            return {"default_provider": None, "providers": []}

    result = await _service_status(SimpleNamespace(web=Web()), "web", "providers")

    assert calls == ["providers"]
    assert result == {
        "configured": True,
        "available": True,
        "value": {"default_provider": None, "providers": []},
    }


@pytest.mark.asyncio
async def test_live_doctor_handles_kernel_without_web_capability():
    result = await _service_status(SimpleNamespace(), "web", "providers")

    assert result == {"configured": False, "available": False}
