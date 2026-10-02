import pytest

import mypr_mcp.kernel_api as kernel_api
from mypr_mcp.kernel_api import RPCError, Workspace, execution_context
from mypr_mcp.mail_api import MailAPI


@pytest.mark.asyncio
async def test_mail_api_forwards_manager_methods(monkeypatch, tmp_path):
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        return fields

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    ws = Workspace(tmp_path)
    with execution_context({"client_id": "calm-otter", "connection_id": "conn"}):
        await ws.mail.accounts()
        await ws.mail.status("work")
        await ws.mail.search("work", unread=True, limit=2)
        await ws.mail.read("mail-1")
        await ws.mail.mark_read(["mail-1"])
        await ws.mail.draft(to=["to@example.test"], subject="Hi", text="Body")
        await ws.mail.send("draft-1", request_id="send-1")
        await ws.mail.watch(mailbox="Archive")
        await ws.mail.notifications(limit=3, cursor="next")
        await ws.mail.ack(["notification-1"])

    assert calls == [
        ("mail", {"method": "accounts", "params": {}}),
        ("mail", {"method": "status", "params": {"account": "work"}}),
        (
            "mail",
            {
                "method": "search",
                "params": {
                    "account": "work",
                    "mailbox": "INBOX",
                    "unread": True,
                    "sender": None,
                    "subject": None,
                    "text": None,
                    "since": None,
                    "before": None,
                    "limit": 2,
                    "cursor": None,
                },
            },
        ),
        (
            "mail",
            {
                "method": "read",
                "params": {"message_id": "mail-1", "max_bytes": 32768, "cursor": None},
            },
        ),
        ("mail", {"method": "mark_read", "params": {"message_ids": ["mail-1"]}}),
        (
            "mail",
            {
                "method": "draft",
                "params": {
                    "account": None,
                    "to": ["to@example.test"],
                    "cc": None,
                    "bcc": None,
                    "subject": "Hi",
                    "text": "Body",
                    "html": None,
                    "attachments": None,
                    "reply_to": None,
                    "forward": None,
                },
            },
        ),
        ("mail", {"method": "send", "params": {"draft_id": "draft-1", "request_id": "send-1"}}),
        ("mail", {"method": "watch", "params": {"account": None, "mailbox": "Archive"}}),
        ("mail", {"method": "notifications", "params": {"limit": 3, "cursor": "next"}}),
        ("mail", {"method": "ack", "params": {"notification_ids": ["notification-1"]}}),
    ]


@pytest.mark.asyncio
async def test_mail_api_requires_client_and_validates_bounds(tmp_path):
    mail = Workspace(tmp_path).mail
    with pytest.raises(RPCError, match="initialized client"):
        await mail.accounts()

    with execution_context({"client_id": "calm-otter"}):
        with pytest.raises(ValueError, match="between 1 and 100"):
            await mail.sends(limit=101)
        with pytest.raises(ValueError, match="1048576"):
            await mail.read("mail-1", max_bytes=1048577)
        with pytest.raises(ValueError, match="cannot both"):
            await mail.draft(reply_to="mail-1", forward="mail-2")


def test_mail_api_is_exposed_and_helpful(tmp_path):
    ws = Workspace(tmp_path)
    assert isinstance(ws.mail, MailAPI)
    assert "ws.mail.search" in ws.help("mail")
    assert "request_id" in ws.help("mail.send")


@pytest.mark.asyncio
async def test_mail_api_reports_old_manager_capability_gap():
    async def rpc(*_args, **_kwargs):
        raise RPCError("ValueError: Unknown operation: mail")

    with execution_context({"client_id": "calm-otter"}):
        with pytest.raises(RPCError, match="does not support mail") as failure:
            await MailAPI(rpc, kernel_api._client_context).accounts()
    assert failure.value.code == "capability_missing"
    assert failure.value.details["restart_required"] is True
