import os
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser

import pytest

import mypr_mcp.mail_content as mail_content
from mypr_mcp.mail_content import (
    MailContentError,
    attachment_bytes,
    build_mime,
    normalize_addresses,
    parse_message,
)


def test_build_mime_validates_headers_recipients_and_bcc():
    with pytest.raises(MailContentError, match="CRLF"):
        build_mime(
            sender="sender@example.test",
            to=["a@example.test\r\nBcc: bad@example.test"],
            text="body",
        )
    with pytest.raises(MailContentError, match="CRLF"):
        build_mime(sender="sender@example.test\r\nX-Bad: yes", to="a@example.test", text="body")

    raw, recipients, _ = build_mime(
        sender="sender@example.test",
        to=["to@example.test"],
        bcc=["hidden@example.test"],
        subject="hello",
        text="body",
    )
    message = BytesParser(policy=policy.default).parsebytes(raw)
    assert recipients == ["to@example.test", "hidden@example.test"]
    assert message["Bcc"] is None


def test_build_mime_reads_workspace_relative_regular_attachment(tmp_path):
    (tmp_path / "note.txt").write_text("attachment", encoding="utf-8")
    raw, _, _ = build_mime(
        sender="sender@example.test",
        to="to@example.test",
        text="body",
        attachments=[{"path": "note.txt", "filename": "renamed.txt", "content_type": "text/plain"}],
        workspace_root=tmp_path,
    )
    message = BytesParser(policy=policy.default).parsebytes(raw)
    part = next(part for part in message.walk() if part.get_filename())
    assert part.get_filename() == "renamed.txt"
    assert part.get_content_type() == "text/plain"
    assert part.get_payload(decode=True) == b"attachment"

    with pytest.raises(MailContentError, match="outside"):
        build_mime(
            sender="sender@example.test",
            to="to@example.test",
            text="body",
            attachments=[{"path": "../note.txt"}],
            workspace_root=tmp_path,
        )

    fifo = tmp_path / "blocked"
    os.mkfifo(fifo)
    with pytest.raises(MailContentError, match="cannot read attachment"):
        build_mime(
            sender="sender@example.test",
            to="to@example.test",
            text="body",
            attachments=[{"path": str(fifo)}],
        )


def test_build_mime_bounds_aggregate_attachment_reads(tmp_path, monkeypatch):
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"a" * 3000)
    second.write_bytes(b"b" * 3000)
    monkeypatch.setattr(mail_content, "MAX_MIME_BYTES", 4096)
    with pytest.raises(MailContentError, match="attachment"):
        build_mime(
            sender="sender@example.test",
            to="to@example.test",
            text="body",
            attachments=[{"path": str(first)}, {"path": str(second)}],
        )


def test_build_mime_reply_uses_reply_to_without_copying_header():
    raw, recipients, _ = build_mime(
        sender="sender@example.test",
        to=None,
        text="reply",
        reply_to={
            "message_id": "<opaque>",
            "headers": {
                "from": "Original <from@example.test>",
                "reply_to": "reply@example.test",
                "message_id": "<original@example.test>",
                "references": "<older@example.test>",
                "subject": "Topic",
            },
        },
    )
    message = BytesParser(policy=policy.default).parsebytes(raw)
    assert recipients == ["reply@example.test"]
    assert message["Reply-To"] is None
    assert message["In-Reply-To"] == "<original@example.test>"
    assert "<older@example.test>" in message["References"]
    assert "<original@example.test>" in message["References"]
    assert message["Subject"] == "Re: Topic"


def test_parse_message_pages_body_and_preserves_unicode():
    message = EmailMessage()
    message["Subject"] = "Привет"
    message["From"] = "sender@example.test"
    message["To"] = "to@example.test"
    message.set_content("안녕하세요\n" * 20)
    message.add_alternative("<p>Привет</p>" * 20, subtype="html")
    raw = message.as_bytes()

    page = parse_message(raw, max_bytes=32)
    assert len((page["text"] + page["html"]).encode()) <= 32
    assert page["has_more"] is True
    rest = parse_message(raw, max_bytes=32, cursor=page["next_cursor"])
    assert len((rest["text"] + rest["html"]).encode()) <= 32
    assert page["headers"]["subject"] == "Привет"


def test_parse_message_falls_back_for_unknown_charset_and_attachment_metadata():
    raw = (
        b"Content-Type: multipart/mixed; boundary=x\r\n\r\n"
        b"--x\r\nContent-Type: text/plain; charset=unknown\r\n\r\nbody\r\n"
        b'--x\r\nContent-Disposition: attachment; filename="a.txt"\r\n'
        b"Content-Type: text/plain\r\n\r\ndata\r\n--x--\r\n"
    )
    result = parse_message(raw)
    assert result["text"] == "body"
    assert result["attachments"][0]["filename"] == "a.txt"
    assert attachment_bytes(raw, result["attachments"][0]["id"])[1] == b"data"


def test_parse_message_bounds_attachment_metadata_but_preserves_ids():
    message = EmailMessage()
    message["From"] = "sender@example.test"
    message["To"] = "to@example.test"
    message.set_content("body")
    for index in range(mail_content.MAX_ATTACHMENT_METADATA + 3):
        message.add_attachment(
            f"data-{index}".encode(), maintype="text", subtype="plain", filename=f"{index}.txt"
        )
    raw = message.as_bytes()

    result = parse_message(raw)
    assert len(result["attachments"]) == mail_content.MAX_ATTACHMENT_METADATA
    assert result["attachment_total"] == mail_content.MAX_ATTACHMENT_METADATA + 3
    assert result["attachments_truncated"] is True
    parts = list(BytesParser(policy=policy.default).parsebytes(raw).walk())
    omitted = next(
        str(index)
        for index, part in enumerate(parts)
        if part.get_filename() == "66.txt"
    )
    assert attachment_bytes(raw, omitted)[1] == b"data-66"


def test_parse_message_rejects_too_many_attachments_when_forwarding():
    message = EmailMessage()
    message["From"] = "sender@example.test"
    message["To"] = "to@example.test"
    message.set_content("body")
    for index in range(mail_content.MAX_ATTACHMENTS + 1):
        message.add_attachment(
            f"data-{index}".encode(), maintype="text", subtype="plain", filename=f"{index}.txt"
        )

    with pytest.raises(MailContentError, match="more than"):
        parse_message(message.as_bytes(), include_attachment_data=True)


def test_build_mime_rejects_forward_with_omitted_attachments():
    with pytest.raises(MailContentError, match="omitted attachments"):
        build_mime(
            sender="sender@example.test",
            to="to@example.test",
            forward={
                "text": "body",
                "attachment_data": [],
                "attachment_total": 1,
                "attachments_truncated": True,
            },
        )


def test_normalize_addresses_rejects_invalid_values_and_limits():
    assert normalize_addresses("A <a@example.test>", "to") == ["a@example.test"]
    with pytest.raises(MailContentError):
        normalize_addresses("not-an-address", "to")
    with pytest.raises(MailContentError):
        normalize_addresses(["a@example.test"] * 257, "to")
