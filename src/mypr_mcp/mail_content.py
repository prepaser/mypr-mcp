"""Bounded MIME parsing and draft construction for :mod:`mail_service`."""

from __future__ import annotations

import mimetypes
import re
from collections.abc import Iterable, Mapping
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage, Message
from email.parser import BytesParser
from email.utils import formataddr, formatdate, getaddresses, make_msgid
from pathlib import Path
from typing import Any

from .file_io import read_bytes

MAX_BODY_BYTES = 1 * 1024 * 1024
MAX_MIME_BYTES = 25 * 1024 * 1024
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
MAX_ATTACHMENTS = 32
MAX_ATTACHMENT_METADATA = 64
MAX_RECIPIENTS = 256
_HEADER_LIMIT = 4096
_CONTENT_TYPE = re.compile(r"^[^\s/;]+/[^\s/;]+$")


class MailContentError(ValueError):
    pass


def decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        result = str(make_header(decode_header(value)))
    except LookupError, UnicodeError, ValueError:
        result = value
    return result[:_HEADER_LIMIT]


def _header_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or "\r" in value or "\n" in value:
        raise MailContentError(f"{name} must be a CRLF-free string")
    if len(value.encode("utf-8", "replace")) > _HEADER_LIMIT:
        raise MailContentError(f"{name} is too long")
    return value


def _normalize_mailbox(value: str, name: str) -> str:
    value = value.strip()
    if (
        not value
        or any(char in value for char in "\r\n\x00")
        or "@" not in value
        or value.startswith(".")
        or value.endswith(".")
    ):
        raise MailContentError(f"{name} contains an invalid recipient: {value!r}")
    local, domain = value.rsplit("@", 1)
    if not local or not domain or any(char.isspace() for char in domain):
        raise MailContentError(f"{name} contains an invalid recipient: {value!r}")
    if domain.startswith(".") or domain.endswith(".") or ".." in domain:
        raise MailContentError(f"{name} contains an invalid recipient: {value!r}")
    try:
        domain = domain.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise MailContentError(f"{name} contains an invalid recipient: {value!r}") from exc
    return f"{local}@{domain}"


def normalize_mailbox(value: str, name: str = "address") -> str:
    """Validate one mailbox and encode its domain with IDNA."""
    if not isinstance(value, str):
        raise MailContentError(f"{name} contains an invalid recipient")
    parsed = getaddresses([value])
    if len(parsed) != 1 or not parsed[0][1]:
        raise MailContentError(f"{name} contains an invalid recipient")
    return _normalize_mailbox(parsed[0][1], name)


def _address_items(value: str, name: str) -> list[tuple[str, str]]:
    _header_text(value, name)
    parsed = getaddresses([value])
    if not parsed or any(not address for _, address in parsed):
        raise MailContentError(f"{name} contains an invalid recipient")
    result = []
    for display, address in parsed:
        result.append((display, _normalize_mailbox(address, name)))
    return result


def _parse_addresses(value: str, name: str) -> list[str]:
    return [address for _, address in _address_items(value, name)]


def normalize_addresses(value: str | Iterable[str] | None, name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError as exc:
            raise MailContentError(f"{name} must be a string or list of strings") from exc
    if len(values) > MAX_RECIPIENTS:
        raise MailContentError(f"{name} has too many recipients")
    result: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise MailContentError(f"{name} contains an invalid recipient")
        result.extend(_parse_addresses(item, name))
    if len(result) > MAX_RECIPIENTS:
        raise MailContentError(f"{name} has too many recipients")
    return result


def _format_addresses(
    value: str | Iterable[str] | None, name: str
) -> tuple[list[str], list[str]]:
    if value is None:
        return [], []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError as exc:
            raise MailContentError(f"{name} must be a string or list of strings") from exc
    if len(values) > MAX_RECIPIENTS or any(not isinstance(item, str) for item in values):
        raise MailContentError(f"{name} has too many recipients or contains an invalid recipient")
    headers: list[str] = []
    addresses: list[str] = []
    for item in values:
        parsed = _address_items(item, name)
        if any(not address.rsplit("@", 1)[0].isascii() for _, address in parsed):
            raise MailContentError(
                f"{name} contains an international local part; SMTPUTF8 is not supported"
            )
        headers.extend(
            formataddr((display, address), charset="utf-8") for display, address in parsed
        )
        addresses.extend(address for _, address in parsed)
    if len(addresses) > MAX_RECIPIENTS:
        raise MailContentError(f"{name} has too many recipients")
    return headers, addresses


def _page_body(
    text: str,
    html: str,
    max_bytes: int,
    cursor: str | int | None,
) -> tuple[str, str, str | None, bool]:
    combined = text + html
    encoded = combined.encode("utf-8", "replace")
    if cursor is None:
        offset = 0
    else:
        try:
            offset = int(cursor)
        except (TypeError, ValueError) as exc:
            raise MailContentError("cursor must be a non-negative byte offset") from exc
        if offset < 0 or offset > len(encoded):
            raise MailContentError("cursor is outside the message body")
    if offset < len(encoded) and 0x80 <= encoded[offset] <= 0xBF:
        raise MailContentError("cursor must align with a UTF-8 character boundary")
    end = min(offset + max_bytes, len(encoded))
    while end > offset and end < len(encoded) and 0x80 <= encoded[end] <= 0xBF:
        end -= 1
    if end == offset and offset < len(encoded):
        raise MailContentError("max_bytes cannot fit the next UTF-8 character")
    text_end = len(text.encode("utf-8", "replace"))
    separator_end = text_end
    text_start, text_stop = min(offset, text_end), min(end, text_end)
    html_start, html_stop = max(offset, separator_end), max(end, separator_end)
    page_text = encoded[text_start:text_stop].decode("utf-8")
    page_html = encoded[html_start:html_stop].decode("utf-8")
    has_more = end < len(encoded)
    return page_text, page_html, str(end) if has_more else None, has_more


def parse_message(
    raw: bytes,
    *,
    max_bytes: int = MAX_BODY_BYTES,
    cursor: str | int | None = None,
    include_attachment_data: bool = False,
) -> dict[str, Any]:
    if not isinstance(raw, bytes):
        raise TypeError("message must be bytes")
    if len(raw) > MAX_MIME_BYTES:
        raise MailContentError("message exceeds the 25 MiB limit")
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_MIME_BYTES:
        raise MailContentError(f"max_bytes must be between 1 and {MAX_MIME_BYTES}")
    try:
        message = BytesParser(policy=policy.default).parsebytes(raw)
    except (LookupError, UnicodeError, ValueError) as exc:
        raise MailContentError(f"invalid MIME message: {exc}") from exc
    headers: dict[str, str] = {}
    for key in (
        "Subject",
        "From",
        "To",
        "Cc",
        "Date",
        "Message-ID",
        "Reply-To",
        "In-Reply-To",
        "References",
    ):
        value = message.get(key)
        if value is not None:
            headers[key.lower().replace("-", "_")] = decode_header_value(value)
    text, html = _body_parts(message)
    text, html, next_cursor, has_more = _page_body(text, html, max_bytes, cursor)
    attachments = []
    attachment_data = []
    attachment_total = 0
    for index, part in enumerate(message.walk()):
        if part.is_multipart():
            continue
        filename = part.get_filename()
        disposition = part.get_content_disposition()
        if filename or disposition == "attachment":
            attachment_total += 1
            if include_attachment_data and attachment_total > MAX_ATTACHMENTS:
                raise MailContentError(
                    f"forwarded message has more than {MAX_ATTACHMENTS} attachments"
                )
            if not include_attachment_data and len(attachments) >= MAX_ATTACHMENT_METADATA:
                continue
            payload = part.get_payload(decode=True) or b""
            if include_attachment_data:
                attachment_data.append(
                    {
                        "filename": _filename(filename or f"attachment-{index}"),
                        "content_type": _content_type(part.get_content_type()),
                        "data": payload,
                    }
                )
            attachments.append(
                {
                    "id": str(index),
                    "filename": decode_header_value(filename)
                    if filename
                    else f"attachment-{index}",
                    "content_type": part.get_content_type(),
                    "size": len(payload),
                }
            )
    result = {
        "headers": headers,
        "text": text,
        "html": html,
        "attachments": attachments,
        "attachment_total": attachment_total,
        "attachments_truncated": attachment_total > len(attachments),
        "size": len(raw),
        "has_more": has_more,
        "next_cursor": next_cursor,
    }
    if include_attachment_data:
        result["attachment_data"] = attachment_data
    return result


def attachment_bytes(
    raw: bytes, attachment_id: str, *, max_bytes: int = MAX_ATTACHMENT_BYTES
) -> tuple[str, bytes, str]:
    if not isinstance(raw, bytes) or len(raw) > MAX_MIME_BYTES:
        raise MailContentError("message exceeds the 25 MiB limit")
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_ATTACHMENT_BYTES:
        raise MailContentError(f"max_bytes must be between 1 and {MAX_ATTACHMENT_BYTES}")
    try:
        message = BytesParser(policy=policy.default).parsebytes(raw)
    except (LookupError, UnicodeError, ValueError) as exc:
        raise MailContentError(f"invalid MIME message: {exc}") from exc
    try:
        wanted = int(attachment_id)
    except (TypeError, ValueError) as exc:
        raise MailContentError("attachment_id must be a valid attachment ID") from exc
    for index, part in enumerate(message.walk()):
        if index != wanted or part.is_multipart():
            continue
        if not part.get_filename() and part.get_content_disposition() != "attachment":
            break
        payload = part.get_payload(decode=True) or b""
        if len(payload) > max_bytes:
            raise MailContentError("attachment exceeds the 25 MiB limit")
        filename = decode_header_value(part.get_filename()) or f"attachment-{index}"
        filename = _filename(filename)
        return filename, payload, _content_type(part.get_content_type())
    raise MailContentError(f"unknown attachment ID: {attachment_id}")


def _filename(value: str) -> str:
    value = decode_header_value(value)
    if not value or "\x00" in value or "/" in value or "\\" in value or value in {".", ".."}:
        raise MailContentError("attachment filename is invalid")
    return value[:255]


def _content_type(value: str) -> str:
    value = str(value or "application/octet-stream").strip().lower()
    if not _CONTENT_TYPE.fullmatch(value):
        raise MailContentError("attachment content_type is invalid")
    return value


def build_mime(
    *,
    sender: str,
    to: Iterable[str] | str | None,
    cc: Iterable[str] | str = (),
    bcc: Iterable[str] | str = (),
    subject: str = "",
    text: str | None = None,
    html: str | None = None,
    attachments: Iterable[Mapping[str, Any]] = (),
    reply_to: Mapping[str, Any] | None = None,
    forward: Mapping[str, Any] | None = None,
    workspace_root: Path | None = None,
) -> tuple[bytes, list[str], str]:
    if reply_to is not None and forward is not None:
        raise MailContentError("reply_to and forward cannot both be set")
    if forward is not None:
        if not isinstance(forward, Mapping):
            raise MailContentError("forward must be an object")
        forward_data = forward.get("attachment_data") or ()
        if forward.get("attachments_truncated"):
            raise MailContentError("forward contains omitted attachments")
        total = forward.get("attachment_total")
        if total is not None:
            if type(total) is not int or total < 0:
                raise MailContentError("forward attachment_total is invalid")
            if total != len(forward_data):
                raise MailContentError("forward attachment metadata is incomplete")
    sender_headers, sender_values = _format_addresses(sender, "sender")
    if len(sender_values) != 1:
        raise MailContentError("sender must contain exactly one address")
    sender_address = sender_values[0]
    to_headers, to_values = _format_addresses(to, "to")
    cc_headers, cc_values = _format_addresses(cc, "cc")
    _, bcc_values = _format_addresses(bcc, "bcc")
    reply_headers = reply_to.get("headers", {}) if isinstance(reply_to, Mapping) else {}
    if reply_to is not None and not isinstance(reply_headers, Mapping):
        raise MailContentError("reply_to headers are invalid")
    if reply_to is not None and not to_values:
        target = reply_headers.get("reply_to") or reply_headers.get("from")
        to_headers, to_values = _format_addresses(target, "reply recipient")
    if not to_values and not cc_values and not bcc_values:
        raise MailContentError("at least one recipient is required")
    if text is not None and not isinstance(text, str):
        raise MailContentError("text must be a string")
    if html is not None and not isinstance(html, str):
        raise MailContentError("html must be a string")
    body_bytes = sum(
        len(value.encode("utf-8", "replace")) for value in (text, html) if value is not None
    )
    if body_bytes > MAX_MIME_BYTES:
        raise MailContentError("draft body exceeds the 25 MiB limit")
    if text is None and html is None and not forward:
        raise MailContentError("text or html is required")
    subject = str(subject or "")
    _header_text(subject, "subject")
    if forward and not subject:
        subject = "Fwd: " + str(forward.get("headers", {}).get("subject") or "")
    if reply_to and not subject:
        original_subject = str(reply_headers.get("subject") or "")
        subject = (
            original_subject
            if original_subject.lower().startswith("re:")
            else f"Re: {original_subject}".strip()
        )
    _header_text(subject, "subject")
    msg = EmailMessage(policy=policy.SMTP)
    msg["From"] = sender_headers[0]
    if to_headers:
        msg["To"] = ", ".join(to_headers)
    if cc_headers:
        msg["Cc"] = ", ".join(cc_headers)
    msg["Subject"] = subject[:_HEADER_LIMIT]
    msg["Date"] = formatdate(localtime=False)
    msg["Message-ID"] = make_msgid(domain=sender_address.rsplit("@", 1)[1])
    if reply_to:
        original = reply_headers.get("message_id") or reply_to.get("message_id")
        if original:
            _header_text(str(original), "message_id")
            msg["In-Reply-To"] = str(original)
        references = str(reply_headers.get("references") or "").strip()
        if original and str(original) not in references.split():
            references = f"{references} {original}".strip()
        if references:
            _header_text(references, "references")
            msg["References"] = references
    if forward:
        original_text = str(forward.get("text") or "")
        original_html = str(forward.get("html") or "")
        text = (text or "") + "\n\n---------- Forwarded message ----------\n" + original_text
        if original_html:
            html = (
                (html or "") + "<hr><p>---------- Forwarded message ----------</p>" + original_html
            )
        attachments = [*(attachments or ()), *(forward.get("attachment_data") or ())]
    if text is not None and html is not None:
        msg.set_content(text, cte="quoted-printable")
        msg.add_alternative(html, subtype="html", cte="quoted-printable")
    elif text is not None:
        msg.set_content(text, cte="quoted-printable")
    else:
        msg.add_alternative(html or "", subtype="html", cte="quoted-printable")
    body_wire = msg.as_bytes()
    if len(body_wire) > MAX_MIME_BYTES:
        raise MailContentError("draft MIME exceeds the 25 MiB limit")
    attachment_budget = MAX_MIME_BYTES - len(body_wire)
    attachments = list(attachments)
    if len(attachments) > MAX_ATTACHMENTS:
        raise MailContentError(f"at most {MAX_ATTACHMENTS} attachments are allowed")
    recipients = [*to_values, *cc_values, *bcc_values]
    for item in attachments:
        if not isinstance(item, Mapping):
            raise MailContentError("attachments must contain objects")
        filename = item.get("filename")
        if "data" in item:
            data = item["data"]
            if not isinstance(data, bytes):
                raise MailContentError("attachment data must be bytes")
            if len(data) > min(MAX_ATTACHMENT_BYTES, attachment_budget):
                raise MailContentError("attachments exceed the 25 MiB draft limit")
            filename = filename or "attachment"
        else:
            path_value = item.get("path")
            if not isinstance(path_value, str) or not path_value or "\x00" in path_value:
                raise MailContentError("attachment path is invalid")
            path = Path(path_value).expanduser()
            if workspace_root is not None:
                root = Path(workspace_root).expanduser().resolve()
                if not path.is_absolute():
                    path = root / path
                path = path.resolve()
                try:
                    path.relative_to(root)
                except ValueError as exc:
                    raise MailContentError("attachment path is outside the workspace") from exc
            filename = filename or path.name
            try:
                data = read_bytes(
                    path,
                    max_bytes=min(MAX_ATTACHMENT_BYTES, attachment_budget),
                )
            except (OSError, ValueError) as exc:
                raise MailContentError(f"cannot read attachment: {path}") from exc
        attachment_budget -= len(data)
        filename = _filename(str(filename))
        mime = _content_type(
            item.get("content_type")
            or mimetypes.guess_type(filename)[0]
            or "application/octet-stream"
        )
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)
    data = msg.as_bytes()
    if len(data) > MAX_MIME_BYTES:
        raise MailContentError("draft MIME exceeds the 25 MiB limit")
    return data, recipients, str(msg["Message-ID"])


def _body_parts(message: Message) -> tuple[str, str]:
    text_parts: list[str] = []
    html_parts: list[str] = []
    parts = message.walk() if message.is_multipart() else (message,)
    for part in parts:
        if part.is_multipart() or part.get_content_disposition() == "attachment":
            continue
        kind = part.get_content_type()
        try:
            value = part.get_content()
        except LookupError, UnicodeError, ValueError:
            payload = part.get_payload(decode=True) or b""
            try:
                value = payload.decode(part.get_content_charset() or "utf-8", "replace")
            except LookupError, UnicodeError:
                value = payload.decode("utf-8", "replace")
        if not isinstance(value, str):
            continue
        if kind == "text/plain":
            text_parts.append(value)
        elif kind == "text/html":
            html_parts.append(value)
    text = "\n".join(text_parts)
    html = "\n".join(html_parts)
    return text, html
