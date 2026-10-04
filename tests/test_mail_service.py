from __future__ import annotations

import asyncio
import copy
import threading
from email import policy
from email.message import EmailMessage
from pathlib import Path

import pytest
from test_mail_transport import _smtp_server

from mypr_mcp.diagnostics import RPCError
from mypr_mcp.mail_service import MailService

ACCOUNT = {
    "from": "Agent <agent@example.test>",
    "imap": {
        "host": "127.0.0.1",
        "port": 1,
        "security": "plain",
        "username": "test",
        "password_from": "MYPR_TEST_MAIL_PASSWORD",
    },
    "smtp": {"host": "127.0.0.1", "port": 1, "security": "plain"},
}


async def make_service(path, *, port=1):
    account = copy.deepcopy(ACCOUNT)
    account["smtp"]["port"] = port
    notices = asyncio.Queue()
    value = MailService(
        path, {"accounts": {"work": account, "other": ACCOUNT}}, notify=notices.put_nowait
    )
    await value.start()
    return value, notices


async def draft(service):
    return await service.dispatch(
        "draft",
        "alice",
        {
            "account": "work",
            "to": ["dest@example.test"],
            "subject": "Hello",
            "text": "body",
        },
    )


async def test_concurrent_send_replays_start_only_one_smtp_worker(tmp_path, monkeypatch):
    service, notices = await make_service(tmp_path)
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def send(*args):
        calls.append(args)
        entered.set()
        assert release.wait(30)
        return {"accepted": ["dest@example.test"], "rejected": [], "stage": "accepted"}

    monkeypatch.setattr(service.transport, "send", send)
    try:
        value = await draft(service)
        requests = [service.dispatch("send", "alice", {"draft_id": value["id"]}) for _ in range(2)]
        first, second = await asyncio.gather(*requests)
        assert first["id"] == second["id"]
        assert await asyncio.to_thread(entered.wait, 5)
        assert service.active_count == 1
        release.set()
        assert await asyncio.wait_for(notices.get(), 5) == "alice"
        result = await service.dispatch("get_send", "alice", {"send_id": first["id"]})
        assert result["state"] == "accepted"
        assert len(calls) == 1
        await service.dispatch("send", "alice", {"draft_id": value["id"]})
        assert len(calls) == 1
    finally:
        release.set()
        await service.close()


async def test_reload_defers_busy_account_even_with_force_and_applies_other(tmp_path, monkeypatch):
    service, notices = await make_service(tmp_path)
    entered = threading.Event()
    release = threading.Event()

    def send(*args):
        entered.set()
        assert release.wait(30)
        return {"accepted": ["dest@example.test"], "rejected": [], "stage": "accepted"}

    monkeypatch.setattr(service.transport, "send", send)
    try:
        value = await draft(service)
        await service.dispatch("send", "alice", {"draft_id": value["id"]})
        assert await asyncio.to_thread(entered.wait, 5)
        desired = copy.deepcopy(service.config)
        desired["accounts"]["work"]["smtp"]["port"] = 1234
        desired["accounts"]["other"]["smtp"]["port"] = 5678
        applied = await service.apply_config(desired, force=True)
        assert set(applied["deferred"]) == {"work"}
        assert applied["applied"] == ["other"]
        assert service.config["accounts"]["work"]["smtp"]["port"] == 1
        assert service.config["accounts"]["other"]["smtp"]["port"] == 5678
        release.set()
        await asyncio.wait_for(notices.get(), 5)
    finally:
        release.set()
        await service.close()


async def test_cancelled_request_keeps_account_busy_until_transport_finishes(tmp_path, monkeypatch):
    service, _ = await make_service(tmp_path)
    entered = threading.Event()
    release = threading.Event()

    def list_mailboxes(*args):
        entered.set()
        assert release.wait(30)
        return []

    monkeypatch.setattr(service.transport, "list_mailboxes", list_mailboxes)
    request = None
    try:
        request = asyncio.create_task(
            service.dispatch("mailboxes", "alice", {"account": "work"})
        )
        assert await asyncio.to_thread(entered.wait, 5)
        request.cancel()
        await asyncio.sleep(0)
        assert service.active_count == 1

        desired = copy.deepcopy(service.config)
        desired["accounts"]["work"]["smtp"]["port"] = 1234
        applied = await service.apply_config(desired, force=True)
        assert applied["deferred"] == {"work": "Account has active mail work"}
        assert service.config["accounts"]["work"]["smtp"]["port"] == 1

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert service.active_count == 0
    finally:
        release.set()
        if request is not None and not request.done():
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize(
    ("outcome", "state"),
    [
        (
            {"accepted": ["dest@example.test"], "rejected": [], "stage": "accepted"},
            "accepted",
        ),
        (
            {
                "accepted": ["dest@example.test"],
                "rejected": ["other@example.test"],
                "stage": "rcpt",
                "rejected_details": [],
            },
            "partial",
        ),
    ],
)
async def test_cancelled_send_preserves_settled_smtp_outcome(tmp_path, monkeypatch, outcome, state):
    service, notices = await make_service(tmp_path)
    entered = threading.Event()
    release = threading.Event()

    def send(*args):
        entered.set()
        assert release.wait(30)
        return outcome

    monkeypatch.setattr(service.transport, "send", send)
    try:
        value = await draft(service)
        queued = await service.dispatch("send", "alice", {"draft_id": value["id"]})
        assert await asyncio.to_thread(entered.wait, 5)
        task = service._send_tasks[queued["id"]]
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        release.set()
        assert await asyncio.wait_for(notices.get(), 5) == "alice"
        result = await service.dispatch("get_send", "alice", {"send_id": queued["id"]})
        assert result["state"] == state
        assert result["accepted"] == outcome["accepted"]
        assert result["rejected"] == outcome["rejected"]
        assert result["warning"] == "Interrupted after SMTP settled"
    finally:
        release.set()
        await service.close()


async def test_smtp_data_eof_is_persisted_unknown_and_never_retried(tmp_path):
    with _smtp_server("eof") as port:
        service, notices = await make_service(tmp_path, port=port)
        try:
            value = await draft(service)
            queued = await service.dispatch("send", "alice", {"draft_id": value["id"]})
            await asyncio.wait_for(notices.get(), 5)
            result = await service.dispatch("get_send", "alice", {"send_id": queued["id"]})
            assert result["state"] == "unknown"
            assert result["stage"] == "data"
            assert result["accepted"] == ["dest@example.test"]
            repeated = await service.dispatch("send", "alice", {"draft_id": value["id"]})
            assert repeated["id"] == result["id"]
            with pytest.raises(ValueError, match="submitted"):
                await service.dispatch(
                    "send",
                    "alice",
                    {
                        "draft_id": value["id"],
                        "request_id": "different",
                    },
                )
        finally:
            await service.close()


async def test_forward_includes_complete_large_body_and_original_attachment(tmp_path, monkeypatch):
    service, _ = await make_service(tmp_path)
    original = EmailMessage(policy=policy.SMTP)
    original["From"] = "original@example.test"
    original["Subject"] = "Original"
    body = "a" * (1024 * 1024 + 100)
    original.set_content(body)
    original.add_attachment(
        b"attachment", maintype="application", subtype="octet-stream", filename="original.bin"
    )
    monkeypatch.setattr(service.transport, "fetch", lambda *args, **kwargs: original.as_bytes())
    ref = service.store.message_ref(
        "work", "INBOX", 41, 1, service.transport.endpoint_identity("work")
    )
    try:
        value = await service.dispatch(
            "draft",
            "alice",
            {
                "to": ["dest@example.test"],
                "forward": ref,
            },
        )
        assert value["account"] == "work"
        assert value["subject"] == "Fwd: Original"
        saved = service.store.get_draft(value["id"], "alice")
        raw = await asyncio.to_thread(Path(saved["mime_path"]).read_bytes)
        assert b"original.bin" in raw
        from email.parser import BytesParser

        parsed = BytesParser(policy=policy.default).parsebytes(raw)
        assert body in parsed.get_body(preferencelist=("plain",)).get_content()
        parts = [part for part in parsed.walk() if part.get_filename()]
        assert parts[0].get_payload(decode=True) == b"attachment"
    finally:
        await service.close()


async def test_download_pins_symlink_target_and_rejects_concurrent_changes(tmp_path, monkeypatch):
    service, _ = await make_service(tmp_path)
    first, second, link = tmp_path / "first", tmp_path / "second", tmp_path / "link"
    first.write_bytes(b"old")
    second.write_bytes(b"old")
    link.symlink_to(first)
    original = EmailMessage()
    original.set_content("body")
    original.add_attachment(
        b"downloaded", maintype="application", subtype="octet-stream", filename="data.bin"
    )
    raw = original.as_bytes()

    def fetch(*args, **kwargs):
        link.unlink()
        link.symlink_to(second)
        return raw

    monkeypatch.setattr(service.transport, "fetch", fetch)
    ref = service.store.message_ref(
        "work", "INBOX", 41, 1, service.transport.endpoint_identity("work")
    )
    try:
        await service.dispatch(
            "download_attachment",
            "alice",
            {
                "message_id": ref,
                "attachment_id": "2",
                "path": "link",
                "overwrite": True,
            },
        )
        assert first.read_bytes() == b"downloaded"
        assert second.read_bytes() == b"old"
    finally:
        await service.close()


async def test_body_cursor_is_bound_to_original_message(tmp_path, monkeypatch):
    service, _ = await make_service(tmp_path)
    raw = b"Subject: same\r\n\r\n" + b"content" * 100
    monkeypatch.setattr(service.transport, "fetch", lambda *args, **kwargs: raw)
    identity = service.transport.endpoint_identity("work")
    first = service.store.message_ref("work", "INBOX", 41, 1, identity)
    second = service.store.message_ref("work", "INBOX", 41, 2, identity)
    try:
        page = await service.dispatch("read", "alice", {"message_id": first, "max_bytes": 32})
        with pytest.raises(RPCError, match="another message"):
            await service.dispatch(
                "read",
                "alice",
                {
                    "message_id": second,
                    "cursor": page["next_cursor"],
                },
            )
    finally:
        await service.close()


async def test_sent_copy_failure_does_not_change_acceptance_or_trigger_resend(
    tmp_path, monkeypatch
):
    service, notices = await make_service(tmp_path)
    calls = []

    def send(*args):
        calls.append(args)
        return {"accepted": ["dest@example.test"], "rejected": [], "stage": "accepted"}

    def append(*args):
        raise ConnectionError("Sent copy unavailable")

    monkeypatch.setattr(service.transport, "send", send)
    monkeypatch.setattr(service.transport, "append_sent", append)
    try:
        value = await draft(service)
        queued = await service.dispatch("send", "alice", {"draft_id": value["id"]})
        await asyncio.wait_for(notices.get(), 5)
        result = await service.dispatch("get_send", "alice", {"send_id": queued["id"]})
        assert result["state"] == "accepted"
        assert "Sent copy" in result["warning"]
        await service.dispatch("send", "alice", {"draft_id": value["id"]})
        assert len(calls) == 1
    finally:
        await service.close()
