from __future__ import annotations

import asyncio
import time

import pytest

from mypr_mcp.cli import tool_result
from mypr_mcp.doctor import _mail_readiness
from mypr_mcp.history import History
from mypr_mcp.messages import MessageStore
from mypr_mcp.runtime import Runtime
from mypr_mcp.timers import TimerStore


class Mail:
    def __init__(self):
        self.previews = {}
        self.active_count = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def snapshot(self, client):
        return self.previews.get(client)

    def status(self):
        return {"active_count": self.active_count}

    async def dispatch(self, method, client, params):
        self.active_count += 1
        try:
            self.entered.set()
            await self.release.wait()
            return {"state": "accepted", "client_id": client}
        finally:
            self.active_count -= 1


@pytest.fixture
def runtime(tmp_path):
    value = Runtime(tmp_path)
    value.history = History(tmp_path)
    for client in ("alice", "bob"):
        value.history.reserve_client_id(client)
    value.messages = MessageStore(tmp_path)
    value.timers = TimerStore(tmp_path)
    value.mail = Mail()
    value.clients["connection"] = {
        "client_id": "alice",
        "connection_id": "connection",
        "last_activity": time.time(),
    }
    try:
        yield value
    finally:
        value.timers.close()
        value.messages.close()
        value.history.close()


def execution(runtime):
    ident = "a" * 32
    runtime.execs[ident] = {
        "id": ident,
        "generation": runtime.generation,
        "client_id": "alice",
        "connection_id": "connection",
        "state": "running",
        "events": [],
        "truncated": False,
        "done": asyncio.Event(),
        "_revision": 0,
        "_changed": asyncio.Condition(),
    }
    return ident


@pytest.mark.parametrize("wake_on_output", [False, True])
async def test_mail_arrival_wakes_poll_without_stopping_execution(
    runtime,
    wake_on_output,
    monkeypatch,
):
    ident = execution(runtime)
    entered = asyncio.Event()
    original = runtime.wait_notifications

    async def wait(*args, **kwargs):
        entered.set()
        return await original(*args, **kwargs)

    monkeypatch.setattr(runtime, "wait_notifications", wait)
    waiting = asyncio.create_task(
        runtime.poll(
            ident,
            wait_ms=5000,
            inbox_client="alice",
            wake_on_output=wake_on_output,
        )
    )
    await asyncio.wait_for(entered.wait(), 2)
    preview = {"unacked": 1, "items": [{"id": "notice"}], "has_more": False}
    runtime.mail.previews["alice"] = preview
    runtime.notify_message("alice")
    result = await asyncio.wait_for(waiting, 2)
    assert result["state"] == "running"
    response = await runtime.dispatch(
        {
            "op": "poll",
            "connection_id": "connection",
            "exec_id": ident,
            "wait_ms": 0,
        }
    )
    assert response["mail"] == preview
    assert not runtime.message_waiters
    assert not runtime.timer_waiters


async def test_pending_mail_returns_immediately_and_is_client_scoped(runtime):
    ident = execution(runtime)
    runtime.mail.previews["bob"] = {"unacked": 1}
    first = await runtime.dispatch(
        {
            "op": "poll",
            "connection_id": "connection",
            "exec_id": ident,
            "wait_ms": 0,
        }
    )
    assert "mail" not in first
    runtime.mail.previews["alice"] = {"unacked": 1}
    pending = await asyncio.wait_for(
        runtime.dispatch(
            {
                "op": "poll",
                "connection_id": "connection",
                "exec_id": ident,
                "wait_ms": 5000,
            }
        ),
        2,
    )
    assert pending["mail"]["unacked"] == 1


async def test_send_is_admitted_before_restart_check_and_survives_rpc_cancellation(runtime):
    sending = asyncio.create_task(
        runtime.dispatch(
            {
                "op": "mail",
                "connection_id": "connection",
                "method": "send",
                "params": {"draft_id": "draft"},
            }
        )
    )
    await asyncio.wait_for(runtime.mail.entered.wait(), 2)
    with pytest.raises(RuntimeError, match="active work"):
        runtime._check_restart_busy(None, False)
    sending.cancel()
    runtime.mail.release.set()
    with pytest.raises(asyncio.CancelledError):
        await sending
    assert runtime.mail.active_count == 0


async def test_mail_requires_identity_and_rejects_reload(runtime):
    with pytest.raises(RuntimeError, match="client identity"):
        await runtime.dispatch({"op": "mail", "method": "accounts"})
    runtime.settings.applying = True
    with pytest.raises(RuntimeError, match="reload"):
        await runtime.dispatch(
            {
                "op": "mail",
                "connection_id": "connection",
                "method": "send",
            }
        )
    assert not runtime.mail.entered.is_set()


async def test_status_exposes_local_mail_state(runtime):
    runtime.mail.active_count = 1
    assert (await runtime.dispatch({"op": "status", "detail": False}))["mail"] == {
        "active_count": 1,
    }


async def test_mail_preview_remains_machine_readable_and_renders_text():
    result = await tool_result(
        {
            "state": "running",
            "output": [],
            "mail": {
                "unacked": 1,
                "items": [
                    {"id": "notice", "account": "work", "mailbox": "INBOX", "subject": "hello"}
                ],
                "has_more": False,
                "watches": [{"account": "work", "mailbox": "INBOX", "state": "offline"}],
                "sends": [{"id": "uncertain", "draft_id": "draft", "state": "unknown"}],
            },
        }
    )
    assert result.structured_content["mail"]["unacked"] == 1
    text = "\n".join(item.text for item in result.content if hasattr(item, "text"))
    assert "[mail] id=notice" in text
    assert "state=offline" in text
    assert "[mail send] id=uncertain draft_id=draft state=unknown" in text


def test_mail_doctor_checks_sources_without_exposing_password(monkeypatch):
    monkeypatch.setenv("MYPR_TEST_MAIL_PASSWORD", "secret-that-must-stay-private")
    monkeypatch.delenv("MYPR_TEST_MAIL_MISSING", raising=False)
    value = _mail_readiness(
        {
            "accounts": {
                "work": {
                    "imap": {"password_from": "MYPR_TEST_MAIL_PASSWORD"},
                    "smtp": {"password_from": "MYPR_TEST_MAIL_MISSING"},
                }
            }
        }
    )
    assert not value["accounts"]["work"]["ready"]
    assert "secret-that-must-stay-private" not in str(value)
    assert value["network_checked"] is False
