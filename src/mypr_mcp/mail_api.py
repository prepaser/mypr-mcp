"""Kernel-facing asynchronous mail helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

from .diagnostics import RPCError


class MailAPI:
    """Access manager-owned IMAP and SMTP services for the current client."""

    def __init__(
        self,
        rpc: Callable[..., Awaitable[Any]],
        client_context: Any,
    ) -> None:
        self._rpc = rpc
        self._client_context = client_context

    def _require_client(self) -> None:
        if self._client_context.get() is None:
            raise RPCError("ws.mail requires an initialized client")

    async def _call(self, method: str, **params: Any) -> Any:
        self._require_client()
        try:
            return await self._rpc("mail", method=method, params=params)
        except RPCError as exc:
            message = str(exc).strip().casefold()
            if message.removeprefix("valueerror: ").strip() in {
                "unknown operation: mail",
                "unknown operation mail",
            }:
                raise RPCError(
                    "The running workspace manager does not support mail; "
                    "restart it with the current mypr-mcp installation.",
                    code="capability_missing",
                    operation="mail",
                    details={"capability": "mail", "restart_required": True},
                ) from exc
            raise

    @staticmethod
    def _limit(value: int, *, maximum: int = 100) -> int:
        if type(value) is not int or value < 1 or value > maximum:
            raise ValueError(f"limit must be an integer between 1 and {maximum}")
        return value

    @staticmethod
    def _ids(values: Sequence[str], name: str = "message_ids") -> list[str]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise TypeError(f"{name} must be a sequence of strings")
        if len(values) > 100:
            raise ValueError(f"{name} may contain at most 100 IDs")
        if any(not isinstance(value, str) for value in values):
            raise TypeError(f"{name} must contain only strings")
        result = list(values)
        if any(not value for value in result):
            raise ValueError(f"{name} must contain non-empty strings")
        return result

    async def accounts(self) -> Any:
        """List configured mail accounts without exposing credential values."""
        return await self._call("accounts")

    async def status(self, account: str | None = None) -> Any:
        """Return account connection and watch readiness information."""
        return await self._call("status", account=account)

    async def mailboxes(self, account: str | None = None) -> Any:
        """List mailboxes for an account."""
        return await self._call("mailboxes", account=account)

    async def search(
        self,
        account: str | None = None,
        mailbox: str = "INBOX",
        *,
        unread: bool | None = None,
        sender: str | None = None,
        subject: str | None = None,
        text: str | None = None,
        since: str | date | datetime | None = None,
        before: str | date | datetime | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> Any:
        """Search bounded message headers and return opaque message references."""
        self._limit(limit)
        if type(unread) not in (bool, type(None)):
            raise TypeError("unread must be a boolean or None")

        def _date(value: str | date | datetime | None) -> str | None:
            if value is None or isinstance(value, str):
                return value
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            raise TypeError("since and before must be ISO dates or date values")

        params = {
            "account": account,
            "mailbox": mailbox,
            "unread": unread,
            "sender": sender,
            "subject": subject,
            "text": text,
            "since": _date(since),
            "before": _date(before),
            "limit": limit,
            "cursor": cursor,
        }
        return await self._call("search", **params)

    async def read(
        self,
        message_id: str,
        max_bytes: int = 32768,
        *,
        cursor: str | None = None,
    ) -> Any:
        """Read one bounded message without marking it seen."""
        if type(max_bytes) is not int or not 1 <= max_bytes <= 1024 * 1024:
            raise ValueError("max_bytes must be between 1 and 1048576")
        return await self._call("read", message_id=message_id, max_bytes=max_bytes, cursor=cursor)

    async def download_attachment(
        self,
        message_id: str,
        attachment_id: str,
        path: str,
        *,
        overwrite: bool = False,
    ) -> Any:
        """Download one bounded attachment through a revision-checked workspace path."""
        if type(overwrite) is not bool:
            raise TypeError("overwrite must be a boolean")
        return await self._call(
            "download_attachment",
            message_id=message_id,
            attachment_id=attachment_id,
            path=path,
            overwrite=overwrite,
        )

    async def mark_read(self, message_ids: Sequence[str]) -> Any:
        """Mark message references as read."""
        return await self._call("mark_read", message_ids=self._ids(message_ids))

    async def mark_unread(self, message_ids: Sequence[str]) -> Any:
        """Mark message references as unread."""
        return await self._call("mark_unread", message_ids=self._ids(message_ids))

    async def move(self, message_ids: Sequence[str], mailbox: str) -> Any:
        """Move message references to a mailbox."""
        return await self._call("move", message_ids=self._ids(message_ids), mailbox=mailbox)

    async def draft(
        self,
        *,
        account: str | None = None,
        to: Sequence[str] | str | None = None,
        cc: Sequence[str] | str | None = None,
        bcc: Sequence[str] | str | None = None,
        subject: str | None = None,
        text: str | None = None,
        html: str | None = None,
        attachments: Sequence[Mapping[str, Any]] | None = None,
        reply_to: str | None = None,
        forward: str | None = None,
    ) -> Any:
        """Create an immutable validated draft; sending is a separate operation."""
        if to is not None and isinstance(to, bytes):
            raise TypeError("to must be a string or sequence of strings")
        if cc is not None and isinstance(cc, bytes):
            raise TypeError("cc must be a string or sequence of strings")
        if bcc is not None and isinstance(bcc, bytes):
            raise TypeError("bcc must be a string or sequence of strings")
        if reply_to is not None and forward is not None:
            raise ValueError("reply_to and forward cannot both be set")
        return await self._call(
            "draft",
            account=account,
            to=to,
            cc=cc,
            bcc=bcc,
            subject=subject,
            text=text,
            html=html,
            attachments=attachments,
            reply_to=reply_to,
            forward=forward,
        )

    async def get_draft(self, draft_id: str) -> Any:
        """Read one client-owned immutable draft."""
        return await self._call("get_draft", draft_id=draft_id)

    async def send(self, draft_id: str, request_id: str | None = None) -> Any:
        """Queue one draft for idempotent SMTP delivery."""
        return await self._call("send", draft_id=draft_id, request_id=request_id)

    async def get_send(self, send_id: str) -> Any:
        """Read one client-owned send transaction, including unknown outcomes."""
        return await self._call("get_send", send_id=send_id)

    async def sends(self, limit: int = 20, cursor: str | None = None) -> Any:
        """List this client's bounded send transaction history."""
        self._limit(limit)
        return await self._call("sends", limit=limit, cursor=cursor)

    async def watch(self, account: str | None = None, mailbox: str = "INBOX") -> Any:
        """Subscribe this client to new-message notifications for a mailbox."""
        return await self._call("watch", account=account, mailbox=mailbox)

    async def unwatch(self, watch_id: str) -> Any:
        """Remove one client-owned mailbox subscription."""
        return await self._call("unwatch", watch_id=watch_id)

    async def watches(self) -> Any:
        """List this client's mailbox subscriptions and their sync state."""
        return await self._call("watches")

    async def notifications(self, limit: int = 20, cursor: str | None = None) -> Any:
        """Read cached mail notifications without acknowledging them."""
        self._limit(limit)
        return await self._call("notifications", limit=limit, cursor=cursor)

    async def ack(self, notification_ids: Sequence[str]) -> Any:
        """Acknowledge cached notifications without marking messages as read."""
        return await self._call(
            "ack", notification_ids=self._ids(notification_ids, "notification_ids")
        )


__all__ = ["MailAPI"]
