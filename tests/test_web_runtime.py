from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from conftest import execute, mcp_session, result_text

from mypr_mcp.diagnostics import RPCError
from mypr_mcp.runtime import Runtime
from mypr_mcp.web_service import WebService


class Transport:
    def __init__(self, config):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, operation, provider, params):
        self.entered.set()
        await self.release.wait()
        return {"operation": operation, "provider": provider, "results": [], "fetched_at": "now"}

    async def close(self):
        pass


def runtime(tmp_path):
    value = Runtime(tmp_path)
    value.web = WebService(
        {"providers": {"kagi": {"api_key_env": "MYPR_TEST_KEY"}}},
        transport_factory=Transport,
    )
    value.clients["connection"] = {
        "client_id": "alice",
        "connection_id": "connection",
        "last_activity": time.time(),
    }
    return value


async def test_web_admission_counts_work_and_survives_rpc_cancellation(tmp_path):
    value = runtime(tmp_path)
    request = asyncio.create_task(
        value.dispatch(
            {
                "op": "web",
                "connection_id": "connection",
                "method": "search",
                "params": {"query": "test"},
            }
        )
    )
    try:
        await asyncio.wait_for(value.web.transport.entered.wait(), 2)
        with pytest.raises(RuntimeError, match="active work"):
            value._check_restart_busy(None, False)
        with pytest.raises(RuntimeError, match="active work"):
            await value.dispatch({"op": "stop"})
        request.cancel()
        await asyncio.sleep(0)
        assert value.web.active_count == 1
        value.web.transport.release.set()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert value.web.active_count == 0
        assert len(value.web.snapshots._snapshots) == 1
    finally:
        value.web.transport.release.set()
        await asyncio.gather(request, return_exceptions=True)
        await value.web.close()


async def test_web_rejects_uninitialized_disconnected_and_reloading_clients(tmp_path):
    value = runtime(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="initialized client"):
            await value.dispatch({"op": "web", "method": "providers", "client_id": "alice"})
        value.settings.applying = True
        with pytest.raises(RuntimeError, match="reload"):
            await value.dispatch(
                {"op": "web", "connection_id": "connection", "method": "providers"}
            )
        value.settings.applying = False
        with pytest.raises(RuntimeError, match="Expired"):
            await value.dispatch(
                {
                    "op": "web",
                    "connection_id": "connection",
                    "method": "providers",
                    "generation": "old",
                }
            )
        value.web = None
        with pytest.raises(RPCError) as failure:
            await value.dispatch(
                {"op": "web", "connection_id": "connection", "method": "providers"}
            )
        assert failure.value.code == "service_unavailable"
    finally:
        if value.web is not None:
            await value.web.close()


async def test_disconnect_waits_for_request_admission_before_dropping_client(tmp_path):
    value = runtime(tmp_path)
    value.clients.clear()
    entered, disconnected = asyncio.Event(), asyncio.Event()
    value.history = SimpleNamespace(append=lambda *args: None, touch_client=lambda *args: None)

    async def io(function, *args, **kwargs):
        return function(*args)

    value.io = io

    class Reader:
        async def read(self, size):
            value.clients["connection"]["client_id"] = "alice"
            entered.set()
            await disconnected.wait()
            return b""

    class Writer:
        def write(self, data):
            pass

        async def drain(self):
            pass

        def close(self):
            pass

        async def wait_closed(self):
            pass

    attaching = asyncio.create_task(
        value.attach(Reader(), Writer(), {"connection_id": "connection"})
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        async with value._admission_lock:
            disconnected.set()
            await asyncio.sleep(0)
            assert "connection" in value.clients
            assert not attaching.done()
        await asyncio.wait_for(attaching, 2)
        assert "connection" not in value.clients
    finally:
        disconnected.set()
        await asyncio.gather(attaching, return_exceptions=True)
        await value.web.close()


async def test_web_api_and_live_configuration_reach_manager_without_paid_requests(
    workspace,
    monkeypatch,
):
    monkeypatch.delenv("MYPR_NO_SUCH_WEB_KEY", raising=False)
    async with mcp_session(workspace) as session:
        outcome = await execute(
            session,
            "\n".join(
                [
                    "ws.local['marker'] = object()",
                    "before = ws.local['marker']",
                    "generation = (await ws.status())['generation']",
                    "providers = await ws.web.providers()",
                    "assert len(providers['providers']) == 3",
                    "assert not any(item['configured'] for item in providers['providers'])",
                    "await ws.config.set('web.providers.kagi', "
                    "{'api_key_env': 'MYPR_NO_SUCH_WEB_KEY'})",
                    "reload = await ws.config.reload()",
                    "assert not reload['errors'] and not reload['deferred'], reload",
                    "info = await ws.web.providers()",
                    "kagi = next(item for item in info['providers'] if item['provider'] == 'kagi')",
                    "assert kagi['configured'] and not kagi['key_available']",
                    "explanation = await ws.config.explain('web.providers.kagi')",
                    "assert not explanation['pending'], explanation",
                    "try:",
                    "    await ws.web.search('test')",
                    "except Exception as exc:",
                    "    assert exc.code == 'credentials_missing', (exc.code, str(exc))",
                    "else:",
                    "    raise AssertionError('Missing key was accepted')",
                    "assert ws.local['marker'] is before",
                    "assert (await ws.status())['generation'] == generation",
                ]
            ),
        )
        assert outcome["state"] == "succeeded", result_text(outcome)
