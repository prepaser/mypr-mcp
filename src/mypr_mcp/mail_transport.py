"""Thread-isolated IMAP and SMTP transport for the mail service."""

from __future__ import annotations

import asyncio
import base64
import datetime as _datetime
import hashlib
import imaplib
import os
import re
import smtplib
import socket
import ssl
import threading
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parseaddr
from pathlib import Path
from typing import Any

from .async_utils import wait_owned
from .mail_content import MailContentError, normalize_mailbox

MAX_MESSAGE_BYTES = 25 * 1024 * 1024
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NETWORK_ERRORS = (OSError, socket.timeout, TimeoutError)
_PROTOCOL_ERRORS = _NETWORK_ERRORS + (imaplib.IMAP4.error,)
_IMAP_CONNECT_ERRORS = _PROTOCOL_ERRORS + (ssl.SSLError,)
_SMTP_CONNECT_ERRORS = _NETWORK_ERRORS + (smtplib.SMTPException, ssl.SSLError)


class MailTransportError(ConnectionError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "connection_error",
        ambiguous: bool = False,
        stage: str | None = None,
        accepted: list[str] | None = None,
        rejected: list[str] | None = None,
        rejected_details: list[dict[str, Any]] | None = None,
    ) -> None:
        self.code = code
        self.ambiguous = ambiguous
        self.details = {"outcome_unknown": True} if ambiguous else {}
        self.stage = stage
        self.accepted = list(accepted or ())
        self.rejected = list(rejected or ())
        self.rejected_details = list(rejected_details or ())
        super().__init__(message)


class MailTransport:
    """Run blocking mail protocol operations on a bounded worker pool.

    IMAP selection is connection state, so each connection lock covers both
    SELECT/EXAMINE and the command using the selected mailbox.
    """

    def __init__(
        self,
        config: Mapping[str, Any] | None = None,
        *,
        timeout: float = 30.0,
        max_workers: int = 4,
    ) -> None:
        self.config = dict(config or {})
        self.timeout = float(timeout)
        worker_count = max(1, int(max_workers))
        self._executor = ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="mypr-mail"
        )
        self._watch_executor = ThreadPoolExecutor(
            max_workers=max(1, worker_count // 2), thread_name_prefix="mypr-mail-watch"
        )
        self._imap: dict[str, tuple[imaplib.IMAP4, threading.RLock]] = {}
        self._watch_imap: dict[tuple[str, str], tuple[imaplib.IMAP4, threading.RLock]] = {}
        self._smtp: dict[str, tuple[smtplib.SMTP, threading.RLock]] = {}
        self._connect_locks: dict[tuple[str, ...], threading.Lock] = {}
        self._guard = threading.RLock()
        self._epochs: dict[tuple[str, ...], int] = {}
        self._connect_refs: dict[tuple[str, ...], int] = {}
        self._retired_connections: set[tuple[str, ...]] = set()
        self._closed = False

    async def run(self, function: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise MailTransportError("mail transport is closed", code="service_closed")
        loop = asyncio.get_running_loop()
        executor = (
            self._watch_executor
            if getattr(function, "__name__", "").startswith("watch_")
            else self._executor
        )
        return await wait_owned(loop.run_in_executor(executor, lambda: function(*args, **kwargs)))

    async def run_watch(self, function: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise MailTransportError("mail transport is closed", code="service_closed")
        loop = asyncio.get_running_loop()
        future = self._watch_executor.submit(function, *args, **kwargs)
        awaitable = asyncio.wrap_future(future, loop=loop)
        try:
            return await asyncio.shield(awaitable)
        except asyncio.CancelledError:
            if future.cancel():
                raise
            await wait_owned(awaitable, propagate=False)
            raise

    def account_names(self) -> list[str]:
        accounts = self.config.get("accounts", self.config)
        if not isinstance(accounts, Mapping):
            return []
        return [
            str(name)
            for name, value in accounts.items()
            if isinstance(value, Mapping) and value.get("enabled", True)
        ]

    def account(self, name: str) -> Mapping[str, Any]:
        accounts = self.config.get("accounts", self.config)
        value = accounts.get(name) if isinstance(accounts, Mapping) else None
        if not isinstance(value, Mapping) or value.get("enabled", True) is False:
            raise ValueError(f"unknown mail account: {name}")
        return value

    def cached_status(self) -> dict[str, Any]:
        with self._guard:
            return {
                "imap": sorted(self._imap),
                "smtp": sorted(self._smtp),
                "watches": [
                    {"account": account, "mailbox": mailbox}
                    for account, mailbox in sorted(self._watch_imap)
                ],
            }

    def account_status(self, name: str) -> dict[str, Any]:
        account = self.account(name)
        conn, lock = self._get_imap(name, account)
        with lock:
            try:
                typ, data = conn.noop()
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(f"IMAP NOOP failed: {exc}") from exc
        return {
            "account": name,
            "imap": typ == "OK",
            "smtp": self._smtp_ready(account),
            "mailboxes": len(data or []),
        }

    def list_mailboxes(self, name: str) -> list[dict[str, Any]]:
        account = self.account(name)
        conn, lock = self._get_imap(name, account)
        with lock:
            try:
                typ, data = conn.list()
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(f"IMAP LIST failed: {exc}") from exc
        if typ != "OK":
            raise MailTransportError("IMAP LIST failed", code="imap_error")
        result: list[dict[str, Any]] = []
        for raw in data or ():
            if not isinstance(raw, bytes):
                continue
            result.append({"name": _list_mailbox_name(raw), "raw": raw.decode("utf-8", "replace")})
        return result

    def namespace(self, name: str, mailbox: str = "INBOX") -> dict[str, int]:
        account = self.account(name)
        conn, lock = self._get_imap(name, account)
        with lock:
            try:
                return self._select_locked(conn, mailbox, readonly=True)
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(f"IMAP namespace lookup failed: {exc}") from exc

    def search(
        self,
        name: str,
        mailbox: str = "INBOX",
        *,
        unread: bool | None = None,
        sender: str | None = None,
        subject: str | None = None,
        text: str | None = None,
        since: str | None = None,
        before: str | None = None,
        after_uid: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        conn, lock = self._get_imap(name, self.account(name))
        criteria = ["UNSEEN" if unread is True else "SEEN" if unread is False else "ALL"]
        if sender:
            criteria.extend(("FROM", _imap_quote(sender)))
        if subject:
            criteria.extend(("SUBJECT", _imap_quote(subject)))
        if text:
            criteria.extend(("TEXT", _imap_quote(text)))
        if since:
            criteria.extend(("SINCE", _imap_date(since)))
        if before:
            criteria.extend(("BEFORE", _imap_date(before)))
        limit = _limit(limit)
        after_uid = _nonnegative_int(after_uid, "after_uid")
        with lock:
            try:
                namespace = self._select_locked(conn, mailbox, readonly=True)
                charset, search_args = _search_arguments(conn, criteria)
                typ, data = conn.uid("SEARCH", charset, *search_args)
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(f"IMAP SEARCH failed: {exc}") from exc
        if typ != "OK":
            raise MailTransportError("IMAP SEARCH failed", code="imap_error")
        all_uids = sorted({uid for uid in _uids(data) if uid > after_uid})
        values = all_uids[:limit]
        return {
            "namespace": namespace,
            "uids": values,
            "has_more": len(all_uids) > len(values),
            "next_cursor": values[-1] if values else after_uid,
        }

    def fetch(
        self,
        name: str,
        mailbox: str,
        uidvalidity: int,
        uid: int,
        *,
        headers_only: bool = False,
        max_bytes: int = MAX_MESSAGE_BYTES,
        offset: int = 0,
        request_size: int | None = None,
    ) -> bytes:
        cap = min(MAX_MESSAGE_BYTES, max(1, int(max_bytes)))
        offset = _nonnegative_int(offset, "offset")
        if request_size is None:
            response_cap = min(cap, 256 * 1024) if headers_only else cap
        else:
            response_cap = min(cap, _positive_int(request_size, "request_size"))
        conn, lock = self._get_imap(name, self.account(name))
        with lock:
            try:
                current = self._select_locked(conn, mailbox, readonly=True)
                _verify_namespace(current, uidvalidity)
                typ, data = _bounded_imap_call(
                    conn,
                    cap,
                    lambda: conn.uid("FETCH", str(_positive_int(uid, "uid")), "(RFC822.SIZE)"),
                )
                if typ != "OK":
                    raise MailTransportError("IMAP FETCH size preflight failed", code="imap_error")
                size = _fetch_size(data)
                if not headers_only and size is not None and size > cap:
                    raise MailTransportError(
                        "message exceeds the configured size limit", code="size_limit"
                    )
                query = "BODY.PEEK[HEADER]" if headers_only else "BODY.PEEK[]"
                if request_size is not None or headers_only:
                    query += f"<{offset}.{response_cap}>"
                typ, data = _bounded_imap_call(
                    conn, response_cap, lambda: conn.uid("FETCH", str(uid), f"({query})")
                )
            except MailTransportError as exc:
                if exc.code == "size_limit" and "literal" in str(exc):
                    self._drop_imap(name, conn)
                raise
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(f"IMAP FETCH failed: {exc}") from exc
        if typ != "OK":
            raise MailTransportError("IMAP FETCH failed", code="imap_error")
        payload = _fetch_literal(data)
        if payload is None:
            raise MailTransportError("IMAP FETCH returned no message", code="not_found")
        if len(payload) > response_cap:
            raise MailTransportError("message exceeds the configured size limit", code="size_limit")
        return payload

    def set_seen(
        self,
        name: str,
        mailbox: str,
        uids: list[int],
        seen: bool,
        *,
        uidvalidity: int | None = None,
    ) -> None:
        if not uids:
            return
        conn, lock = self._get_imap(name, self.account(name))
        with lock:
            try:
                current = self._select_locked(conn, mailbox, readonly=False)
                if uidvalidity is not None:
                    _verify_namespace(current, uidvalidity)
                typ, _ = conn.uid(
                    "STORE",
                    _uid_set(uids),
                    "+FLAGS.SILENT" if seen else "-FLAGS.SILENT",
                    "(\\Seen)",
                )
            except MailTransportError:
                raise
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(
                    f"IMAP STORE outcome is unknown: {exc}",
                    code="outcome_unknown",
                    ambiguous=True,
                ) from exc
        if typ != "OK":
            raise MailTransportError("IMAP STORE failed", code="imap_error")

    def move(
        self,
        name: str,
        mailbox: str,
        uids: list[int],
        destination: str,
        *,
        uidvalidity: int | None = None,
    ) -> None:
        if not uids:
            return
        conn, lock = self._get_imap(name, self.account(name))
        with lock:
            try:
                current = self._select_locked(conn, mailbox, readonly=False)
                if uidvalidity is not None:
                    _verify_namespace(current, uidvalidity)
                uidset = _uid_set(uids)
                try:
                    typ, _ = conn.uid("MOVE", uidset, _mailbox_argument(destination))
                except imaplib.IMAP4.error, AttributeError:
                    typ = "NO"
                if typ == "OK":
                    return
                if not _has_capability(conn, "UIDPLUS"):
                    raise MailTransportError(
                        "IMAP MOVE is unavailable and UIDPLUS is not supported", code="unsupported"
                    )
                typ, _ = conn.uid("COPY", uidset, _mailbox_argument(destination))
                if typ != "OK":
                    raise MailTransportError("IMAP COPY failed", code="imap_error")
                typ, _ = conn.uid("STORE", uidset, "+FLAGS.SILENT", "(\\Deleted)")
                if typ != "OK":
                    raise MailTransportError(
                        "IMAP STORE delete failed", code="imap_error", ambiguous=True
                    )
                typ, _ = conn.uid("EXPUNGE", uidset)
                if typ != "OK":
                    raise MailTransportError(
                        "IMAP UID EXPUNGE failed", code="imap_error", ambiguous=True
                    )
            except MailTransportError:
                raise
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(f"IMAP MOVE failed: {exc}", ambiguous=True) from exc

    def send(
        self, name: str, mime: bytes, recipients: list[str], *, sender: str | None = None
    ) -> dict[str, Any]:
        if not isinstance(mime, bytes):
            raise TypeError("mime must be bytes")
        if len(mime) > MAX_MESSAGE_BYTES:
            raise MailTransportError("message exceeds the 25 MiB limit", code="size_limit")
        account = self.account(name)
        try:
            addresses = [_address(value) for value in recipients]
        except (MailContentError, ValueError) as exc:
            raise MailTransportError(
                f"SMTP recipient is invalid: {exc}", code="configuration_error", stage="mail"
            ) from exc
        if not addresses:
            raise ValueError("at least one recipient is required")
        if sender is None:
            try:
                sender_value = BytesParser(policy=policy.default).parsebytes(mime).get("From", "")
                sender = parseaddr(sender_value)[1]
                if not sender and "@[" in sender_value:
                    sender = parseaddr(sender_value, strict=False)[1]
            except (TypeError, ValueError):
                sender = ""
        sender = sender or str(account.get("from") or account.get("from_address") or "")
        parsed_sender = parseaddr(sender)[1]
        if "@[" in sender and not parsed_sender:
            parsed_sender = parseaddr(sender, strict=False)[1] or sender
        parsed_sender = parsed_sender or sender
        try:
            sender = normalize_mailbox(parsed_sender, "sender")
        except MailContentError as exc:
            raise MailTransportError(
                f"SMTP sender is invalid: {exc}", code="configuration_error", stage="mail"
            ) from exc
        conn, lock = self._get_smtp(name, account)
        stage = "mail"
        accepted: list[str] = []
        rejected: list[str] = []
        rejected_details: list[dict[str, Any]] = []
        with lock:
            try:
                code, message = conn.mail(sender)
                if code != 250:
                    _reset_smtp(conn)
                    return {
                        "accepted": [],
                        "rejected": addresses,
                        "stage": "mail",
                        "error": _smtp_message(message),
                    }
                stage = "rcpt"
                for address in addresses:
                    rcpt_code, rcpt_message = conn.rcpt(address)
                    if 200 <= int(rcpt_code) < 300:
                        accepted.append(address)
                    else:
                        rejected.append(address)
                        rejected_details.append(
                            {
                                "address": address,
                                "code": int(rcpt_code),
                                "response": _smtp_message(rcpt_message),
                            }
                        )
                if not accepted:
                    _reset_smtp(conn)
                    return {
                        "accepted": [],
                        "rejected": rejected,
                        "rejected_details": rejected_details,
                        "stage": "rcpt",
                        "error": "all recipients were refused",
                    }
                stage = "data"
                try:
                    data_code, data_message = conn.data(mime)
                except smtplib.SMTPDataError as exc:
                    _reset_smtp(conn)
                    return {
                        "accepted": [],
                        "rejected": addresses,
                        "stage": "data",
                        "error": str(exc),
                    }
                if int(data_code) < 200 or int(data_code) >= 300:
                    _reset_smtp(conn)
                    return {
                        "accepted": [],
                        "rejected": addresses,
                        "stage": "data",
                        "error": _smtp_message(data_message),
                    }
                return {
                    "accepted": accepted,
                    "rejected": rejected,
                    "rejected_details": rejected_details,
                    "stage": "accepted",
                }
            except smtplib.SMTPDataError as exc:
                _reset_smtp(conn)
                return {"accepted": [], "rejected": addresses, "stage": "data", "error": str(exc)}
            except UnicodeError as exc:
                if stage == "data":
                    self._drop_smtp(name, conn)
                    raise MailTransportError(
                        f"SMTP transaction outcome is unknown: {exc}",
                        code="outcome_unknown",
                        ambiguous=True,
                        stage=stage,
                        accepted=accepted,
                        rejected=rejected,
                        rejected_details=rejected_details,
                    ) from exc
                _reset_smtp(conn)
                return {
                    "accepted": [],
                    "rejected": addresses,
                    "stage": stage,
                    "error": f"SMTP address or message encoding failed: {exc}",
                }
            except _NETWORK_ERRORS + (smtplib.SMTPServerDisconnected,) as exc:
                self._drop_smtp(name, conn)
                raise MailTransportError(
                    f"SMTP transaction ended unexpectedly: {exc}",
                    ambiguous=stage == "data",
                    stage=stage,
                    accepted=accepted,
                    rejected=rejected,
                    rejected_details=rejected_details,
                ) from exc
            except smtplib.SMTPException as exc:
                if stage == "data":
                    self._drop_smtp(name, conn)
                    raise MailTransportError(
                        f"SMTP transaction outcome is unknown: {exc}",
                        code="outcome_unknown",
                        ambiguous=True,
                        stage=stage,
                        accepted=accepted,
                        rejected=rejected,
                        rejected_details=rejected_details,
                    ) from exc
                _reset_smtp(conn)
                raise MailTransportError(
                    f"SMTP transaction failed: {exc}", code="smtp_error"
                ) from exc

    def append_sent(self, name: str, mime: bytes) -> None:
        account = self.account(name)
        mailbox = account.get("sent_mailbox")
        if not mailbox:
            return
        conn, lock = self._get_imap(name, account)
        with lock:
            try:
                typ, _ = conn.append(_mailbox_argument(str(mailbox)), None, None, mime)
            except _PROTOCOL_ERRORS as exc:
                self._drop_imap(name, conn)
                raise MailTransportError(
                    f"IMAP APPEND to sent mailbox failed: {exc}", ambiguous=True
                ) from exc
        if typ != "OK":
            raise MailTransportError(
                "IMAP APPEND to sent mailbox failed", code="imap_error", ambiguous=True
            )

    def watch_search(
        self, name: str, mailbox: str = "INBOX", *, after_uid: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        account = self.account(name)
        conn, lock = self._get_watch_imap(name, mailbox, account)
        with lock:
            try:
                namespace = self._select_locked(conn, mailbox, readonly=True)
                typ, data = conn.uid("SEARCH", None, "ALL")
            except _PROTOCOL_ERRORS as exc:
                self._drop_watch_imap(name, mailbox, conn)
                raise MailTransportError(f"IMAP watch search failed: {exc}") from exc
        if typ != "OK":
            raise MailTransportError("IMAP watch search failed", code="imap_error")
        after_uid = _nonnegative_int(after_uid, "after_uid")
        all_uids = sorted({uid for uid in _uids(data) if uid > after_uid})
        page_limit = _limit(limit)
        values = all_uids[:page_limit]
        return {
            "namespace": namespace,
            "uids": values,
            "has_more": len(all_uids) > len(values),
            "next_cursor": values[-1] if values else after_uid,
        }

    def watch_idle(
        self,
        name: str,
        mailbox: str = "INBOX",
        *,
        duration: float = 29 * 60,
        callback: Callable[[], Any] | None = None,
    ) -> Any:
        account = self.account(name)
        conn, lock = self._get_watch_imap(name, mailbox, account)
        with lock:
            try:
                idle = getattr(conn, "idle", None)
                if callable(idle) and _has_capability(conn, "IDLE"):
                    value = []
                    with idle(duration=min(max(float(duration), 1.0), 29 * 60)) as responses:
                        for response in responses:
                            value.append(response)
                            break
                else:
                    return None
            except _PROTOCOL_ERRORS as exc:
                self._drop_watch_imap(name, mailbox, conn)
                raise MailTransportError(f"IMAP watch wait failed: {exc}") from exc
        return callback() if callback is not None else value

    def close_watch(self, name: str, mailbox: str = "INBOX") -> None:
        """Close one long-lived watch socket without touching command sockets."""
        with self._guard:
            key = ("watch", name, mailbox)
            self._epochs[key] = self._epochs.get(key, 0) + 1
            self._retired_connections.add(key)
            value = self._watch_imap.pop((name, mailbox), None)
            self._reclaim_connection_locked(key)
        if value is None:
            return
        conn, lock = value
        _interrupt_connection(conn)
        with lock:
            try:
                conn.logout()
            except Exception:
                pass

    def close(self) -> None:
        with self._guard:
            if self._closed:
                return
            self._closed = True
            values = [*self._imap.values(), *self._watch_imap.values(), *self._smtp.values()]
            self._imap.clear()
            self._watch_imap.clear()
            self._smtp.clear()
            self._retired_connections.update(self._epochs)
        for conn, lock in values:
            _interrupt_connection(conn)
            with lock:
                try:
                    conn.logout() if hasattr(conn, "logout") else conn.quit()
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
        self._executor.shutdown(wait=True, cancel_futures=False)
        self._watch_executor.shutdown(wait=True, cancel_futures=False)
        with self._guard:
            for key in tuple(self._retired_connections):
                self._reclaim_connection_locked(key)

    def reconfigure(
        self, config: Mapping[str, Any], *, accounts: Iterable[str] | None = None
    ) -> None:
        new_config = dict(config)
        if accounts is None:
            old_accounts = self.config.get("accounts", self.config)
            new_accounts = new_config.get("accounts", new_config)
            old_accounts = old_accounts if isinstance(old_accounts, Mapping) else {}
            new_accounts = new_accounts if isinstance(new_accounts, Mapping) else {}
            changed = {
                str(name)
                for name in set(old_accounts) | set(new_accounts)
                if old_accounts.get(name) != new_accounts.get(name)
            }
        else:
            changed = {str(name) for name in accounts}
        with self._guard:
            for key in tuple(self._epochs):
                if key[1] in changed:
                    self._epochs[key] += 1
                    self._retired_connections.add(key)
            for key in tuple(self._retired_connections):
                self._reclaim_connection_locked(key)
        self._close_connections(changed)
        with self._guard:
            for key in tuple(self._retired_connections):
                self._reclaim_connection_locked(key)
        self.config = new_config

    def endpoint_identity(self, name: str) -> str:
        account = self.account(name)
        spec = _endpoint(account, "imap")
        host, port, security = _endpoint_values(spec, 993)
        username = str(spec.get("username") or account.get("username") or "")
        values = (host, str(port), security, username)
        return hashlib.sha256("\x00".join(values).encode()).hexdigest()[:32]

    def _begin_connection(
        self, key: tuple[str, ...]
    ) -> tuple[threading.Lock, int]:
        with self._guard:
            self._retired_connections.discard(key)
            lock = self._connect_locks.setdefault(key, threading.Lock())
            generation = self._epochs.setdefault(key, 0)
            self._connect_refs[key] = self._connect_refs.get(key, 0) + 1
            return lock, generation

    def _end_connection(self, key: tuple[str, ...]) -> None:
        with self._guard:
            count = self._connect_refs.get(key, 0) - 1
            if count > 0:
                self._connect_refs[key] = count
            else:
                self._connect_refs.pop(key, None)
                self._reclaim_connection_locked(key)

    def _reclaim_connection_locked(self, key: tuple[str, ...]) -> None:
        if key not in self._retired_connections or self._connect_refs.get(key):
            return
        kind = key[0]
        if kind == "imap":
            live = key[1] in self._imap
        elif kind == "smtp":
            live = key[1] in self._smtp
        else:
            live = (key[1], key[2]) in self._watch_imap
        if live:
            return
        self._connect_locks.pop(key, None)
        self._epochs.pop(key, None)
        self._retired_connections.discard(key)

    def _get_imap(
        self, name: str, account: Mapping[str, Any]
    ) -> tuple[imaplib.IMAP4, threading.RLock]:
        key = ("imap", name)
        with self._guard:
            existing = self._imap.get(name)
            if existing is not None:
                return existing
            connect_lock, generation = self._begin_connection(key)
        try:
            with connect_lock:
                with self._guard:
                    existing = self._imap.get(name)
                    if existing is not None:
                        return existing
                conn = _connect_imap(account, self.timeout)
                value = (conn, threading.RLock())
                with self._guard:
                    if self._closed or generation != self._epochs.get(key, 0):
                        _close_quietly(conn)
                        code = "service_closed" if self._closed else "reconfigured"
                        raise MailTransportError(
                            "mail transport is closed"
                            if self._closed
                            else "mail transport was reconfigured",
                            code=code,
                        )
                    existing = self._imap.setdefault(name, value)
                    if existing is not value:
                        _close_quietly(conn)
                    return existing
        finally:
            self._end_connection(key)

    def _get_watch_imap(
        self, name: str, mailbox: str, account: Mapping[str, Any]
    ) -> tuple[imaplib.IMAP4, threading.RLock]:
        key = (name, mailbox)
        connection_key = ("watch", name, mailbox)
        with self._guard:
            existing = self._watch_imap.get(key)
            if existing is not None:
                return existing
            connect_lock, generation = self._begin_connection(connection_key)
        try:
            with connect_lock:
                with self._guard:
                    existing = self._watch_imap.get(key)
                    if existing is not None:
                        return existing
                conn = _connect_imap(account, self.timeout)
                value = (conn, threading.RLock())
                with self._guard:
                    if self._closed or generation != self._epochs.get(connection_key, 0):
                        _close_quietly(conn)
                        code = "service_closed" if self._closed else "reconfigured"
                        raise MailTransportError(
                            "mail transport is closed"
                            if self._closed
                            else "mail transport was reconfigured",
                            code=code,
                        )
                    existing = self._watch_imap.setdefault(key, value)
                    if existing is not value:
                        _close_quietly(conn)
                    return existing
        finally:
            self._end_connection(connection_key)

    def _get_smtp(
        self, name: str, account: Mapping[str, Any]
    ) -> tuple[smtplib.SMTP, threading.RLock]:
        key = ("smtp", name)
        with self._guard:
            existing = self._smtp.get(name)
            if existing is not None:
                return existing
            connect_lock, generation = self._begin_connection(key)
        try:
            with connect_lock:
                with self._guard:
                    existing = self._smtp.get(name)
                    if existing is not None:
                        return existing
                conn = _connect_smtp(account, self.timeout)
                value = (conn, threading.RLock())
                with self._guard:
                    if self._closed or generation != self._epochs.get(key, 0):
                        _close_quietly(conn)
                        code = "service_closed" if self._closed else "reconfigured"
                        raise MailTransportError(
                            "mail transport is closed"
                            if self._closed
                            else "mail transport was reconfigured",
                            code=code,
                        )
                    existing = self._smtp.setdefault(name, value)
                    if existing is not value:
                        _close_quietly(conn)
                    return existing
        finally:
            self._end_connection(key)

    def _smtp_ready(self, account: Mapping[str, Any]) -> bool:
        try:
            _endpoint(account, "smtp")
            return True
        except ValueError:
            return False

    def _drop_imap(self, name: str, conn: imaplib.IMAP4) -> None:
        with self._guard:
            value = self._imap.get(name)
            if value is None or value[0] is not conn:
                return
            self._imap.pop(name, None)
        try:
            conn.logout()
        except Exception:
            pass

    def _drop_watch_imap(self, name: str, mailbox: str, conn: imaplib.IMAP4) -> None:
        with self._guard:
            value = self._watch_imap.get((name, mailbox))
            if value is None or value[0] is not conn:
                return
            self._watch_imap.pop((name, mailbox), None)
        try:
            conn.logout()
        except Exception:
            pass

    def _drop_smtp(self, name: str, conn: smtplib.SMTP) -> None:
        with self._guard:
            value = self._smtp.get(name)
            if value is None or value[0] is not conn:
                return
            self._smtp.pop(name, None)
        try:
            conn.close()
        except Exception:
            pass

    def _close_connections(self, accounts: set[str] | None = None) -> None:
        with self._guard:
            imap = {
                name: value
                for name, value in self._imap.items()
                if accounts is None or name in accounts
            }
            watch = {
                key: value
                for key, value in self._watch_imap.items()
                if accounts is None or key[0] in accounts
            }
            smtp = {
                name: value
                for name, value in self._smtp.items()
                if accounts is None or name in accounts
            }
            for name in imap:
                self._imap.pop(name, None)
            for key in watch:
                self._watch_imap.pop(key, None)
            for name in smtp:
                self._smtp.pop(name, None)
        for conn, lock in [*imap.values(), *watch.values(), *smtp.values()]:
            _interrupt_connection(conn)
            with lock:
                try:
                    conn.logout() if hasattr(conn, "logout") else conn.quit()
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass

    @staticmethod
    def _select_locked(conn: imaplib.IMAP4, mailbox: str, *, readonly: bool) -> dict[str, int]:
        wire = _mailbox_argument(mailbox)
        method = getattr(conn, "examine", None) if readonly else None
        typ, data = method(wire) if method is not None else conn.select(wire, readonly=readonly)
        if typ != "OK":
            raise MailTransportError(f"cannot select mailbox: {mailbox}", code="imap_error")
        uidvalidity = _response_int(conn, "UIDVALIDITY")
        uidnext = _response_int(conn, "UIDNEXT")
        exists = _literal_int((data or [b"0"])[0])
        if uidvalidity is None:
            raise MailTransportError(
                "IMAP server did not return UIDVALIDITY", code="protocol_error"
            )
        return {"uidvalidity": uidvalidity, "uidnext": uidnext or 0, "exists": exists or 0}


def _endpoint(account: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
    nested = account.get(kind)
    if isinstance(nested, Mapping):
        return nested
    prefix = kind + "_"
    values = {
        key[len(prefix) :]: value
        for key, value in account.items()
        if isinstance(key, str) and key.startswith(prefix)
    }
    if values:
        return values
    raise ValueError(f"mail account has no {kind} endpoint")


def _endpoint_values(spec: Mapping[str, Any], default_port: int) -> tuple[str, int, str]:
    host = spec.get("host")
    if not isinstance(host, str) or not host or any(char in host for char in "\r\n\x00"):
        raise ValueError("mail endpoint host is invalid")
    try:
        port = int(spec.get("port") or default_port)
    except (TypeError, ValueError) as exc:
        raise ValueError("mail endpoint port is invalid") from exc
    if not 1 <= port <= 65535:
        raise ValueError("mail endpoint port is invalid")
    security = str(spec.get("security") or ("ssl" if port in (465, 993) else "starttls")).lower()
    if security in {"tls", "implicit_tls", "ssl_tls"}:
        security = "ssl"
    if security not in {"ssl", "starttls", "plain"}:
        raise ValueError("mail endpoint security must be ssl, starttls, or plain")
    return host, port, security


def _ssl_context(spec: Mapping[str, Any]) -> ssl.SSLContext:
    context = ssl.create_default_context()
    ca_file = spec.get("ca_file")
    if ca_file:
        context.load_verify_locations(cafile=str(Path(str(ca_file)).expanduser()))
    return context


def _connect_imap(account: Mapping[str, Any], timeout: float) -> imaplib.IMAP4:
    spec = _endpoint(account, "imap")
    host, port, security = _endpoint_values(spec, 993)
    credentials = _credentials(account, required=True)
    conn: imaplib.IMAP4 | None = None
    try:
        if security == "ssl":
            conn = imaplib.IMAP4_SSL(host, port, timeout=timeout, ssl_context=_ssl_context(spec))
        else:
            conn = imaplib.IMAP4(host, port, timeout=timeout)
            if security == "starttls":
                typ, _ = conn.starttls(ssl_context=_ssl_context(spec))
                if typ != "OK":
                    raise MailTransportError("IMAP STARTTLS failed", code="tls_error")
        _login(conn, account, required=True, credentials=credentials)
        return conn
    except MailTransportError:
        if conn is not None:
            _close_quietly(conn)
        raise
    except _IMAP_CONNECT_ERRORS as exc:
        if conn is not None:
            _close_quietly(conn)
        raise MailTransportError(f"IMAP connection failed: {exc}") from exc


def _connect_smtp(account: Mapping[str, Any], timeout: float) -> smtplib.SMTP:
    spec = _endpoint(account, "smtp")
    host, port, security = _endpoint_values(spec, 465)
    credentials = _credentials(account, required=False, smtp=True)
    conn: smtplib.SMTP | None = None
    try:
        if security == "ssl":
            conn = smtplib.SMTP_SSL(host, port, timeout=timeout, context=_ssl_context(spec))
        else:
            conn = smtplib.SMTP(host, port, timeout=timeout)
            if security == "starttls":
                conn.starttls(context=_ssl_context(spec))
        conn.ehlo_or_helo_if_needed()
        _login(conn, account, required=False, smtp=True, credentials=credentials)
        return conn
    except MailTransportError:
        if conn is not None:
            _close_quietly(conn)
        raise
    except _SMTP_CONNECT_ERRORS as exc:
        if conn is not None:
            _close_quietly(conn)
        raise MailTransportError(f"SMTP connection failed: {exc}") from exc


def _credentials(
    account: Mapping[str, Any], *, required: bool, smtp: bool = False
) -> tuple[str | None, str | None]:
    spec = account.get("smtp" if smtp else "imap")
    if not isinstance(spec, Mapping):
        spec = account
    username = spec.get("username") or account.get("username")
    password_from = spec.get("password_from") or account.get("password_from")
    if not username:
        if required:
            raise MailTransportError("IMAP username is required", code="configuration_error")
        return None, None
    if not password_from:
        raise MailTransportError("mail password_from is required", code="configuration_error")
    source = str(password_from)
    password = os.environ.get(source)
    if password is None:
        raise MailTransportError(
            f"mail credential environment variable is missing: {source}", code="credential_missing"
        )
    return str(username), password


def _login(
    conn: Any,
    account: Mapping[str, Any],
    *,
    required: bool,
    smtp: bool = False,
    credentials: tuple[str | None, str | None] | None = None,
) -> None:
    username, password = (
        credentials
        if credentials is not None
        else _credentials(account, required=required, smtp=smtp)
    )
    if username is None or password is None:
        return
    try:
        conn.login(username, password)
    except Exception as exc:
        raise MailTransportError(
            f"mail authentication failed for {username}", code="authentication_failed"
        ) from exc


def _response_int(conn: imaplib.IMAP4, name: str) -> int | None:
    try:
        data = conn.response(name)[1]
    except imaplib.IMAP4.error, AttributeError:
        return None
    if not data:
        return None
    return _literal_int(data[-1], default=None)


def _literal_int(value: Any, default: int | None = 0) -> int | None:
    try:
        return int(value.decode() if isinstance(value, bytes) else value)
    except TypeError, ValueError, AttributeError:
        return default


def _uids(data: list[Any] | tuple[Any, ...] | None) -> list[int]:
    result: list[int] = []
    for item in data or ():
        if isinstance(item, bytes):
            for value in item.split():
                try:
                    result.append(int(value))
                except ValueError:
                    pass
    return result


def _fetch_size(data: list[Any] | tuple[Any, ...] | None) -> int | None:
    for item in data or ():
        if isinstance(item, tuple) and item and isinstance(item[0], bytes):
            text = item[0].decode("ascii", "ignore")
        elif isinstance(item, bytes):
            text = item.decode("ascii", "ignore")
        else:
            continue
        match = re.search(r"RFC822\.SIZE\s+(\d+)", text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _fetch_literal(data: list[Any] | tuple[Any, ...] | None) -> bytes | None:
    for item in data or ():
        if isinstance(item, tuple) and len(item) > 1 and isinstance(item[1], bytes):
            return item[1]
    return None


def _bounded_imap_call(conn: Any, max_literal: int, operation: Callable[[], Any]) -> Any:
    """Reject an advertised IMAP literal before the stdlib reads it."""
    read = getattr(conn, "read", None)
    if not callable(read):
        return operation()

    remaining = max_literal

    def bounded(size: int) -> bytes:
        nonlocal remaining
        size = int(size)
        if size > remaining:
            _interrupt_connection(conn)
            raise MailTransportError(
                "IMAP literal exceeds the configured size limit", code="size_limit"
            )
        payload = read(size)
        remaining -= len(payload)
        return payload

    try:
        conn.read = bounded
    except AttributeError, TypeError:
        return operation()
    try:
        return operation()
    finally:
        try:
            conn.read = read
        except AttributeError, TypeError:
            pass


def _verify_namespace(current: Mapping[str, Any], expected: int) -> None:
    if int(current.get("uidvalidity", 0)) != int(expected):
        raise MailTransportError(
            "mail reference is from an outdated mailbox namespace", code="stale_reference"
        )


def _uid_set(values: list[int]) -> str:
    if not isinstance(values, list):
        raise ValueError("uids must be a list")
    unique = sorted({_positive_int(value, "uid") for value in values})
    if not unique:
        raise ValueError("uids must not be empty")
    return ",".join(str(value) for value in unique)


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if result <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return result


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative integer") from exc
    if result < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return result


def _limit(value: Any) -> int:
    result = _nonnegative_int(value, "limit")
    return min(max(result, 1), 100)


def _imap_quote(value: str) -> str:
    if not isinstance(value, str) or any(char in value for char in "\r\n\x00"):
        raise ValueError("IMAP search value contains a control character")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _search_arguments(conn: Any, criteria: list[str]) -> tuple[str | None, list[str | bytes]]:
    if not any(
        any(ord(char) > 127 for char in value) for value in criteria if isinstance(value, str)
    ):
        return None, criteria
    if getattr(conn, "utf8_enabled", False):
        return None, criteria
    return "UTF-8", [
        value.encode("utf-8") if isinstance(value, str) else value for value in criteria
    ]


def _quote(value: str) -> str:
    return _imap_quote(value)


def _imap_date(value: str) -> str:
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ValueError("mail dates must use YYYY-MM-DD")
    try:
        parsed = _datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("mail date is invalid") from exc
    return parsed.strftime("%d-%b-%Y")


def _mailbox_wire(value: str) -> str:
    if not isinstance(value, str) or not value or any(char in value for char in "\r\n\x00"):
        raise ValueError("mailbox is invalid")
    encoder = getattr(imaplib, "_encode_utf7", None)
    return encoder(value) if encoder is not None else _encode_modified_utf7(value)


def _mailbox_argument(value: str) -> str:
    wire = _mailbox_wire(value)
    if re.fullmatch(r"[A-Za-z0-9_./&,+:=@-]+", wire):
        return wire
    return _imap_quote(wire)


def _mailbox_display(value: str) -> str:
    decoder = getattr(imaplib, "_decode_utf7", None)
    return decoder(value) if decoder is not None else _decode_modified_utf7(value)


def _list_mailbox_name(raw: bytes) -> str:
    text = raw.decode("ascii", "replace")
    match = re.search(r"\)\s+(?:\"[^\"]*\"|[^\s]+)\s+(.+)$", text)
    value = match.group(1).strip() if match else text.rsplit(" ", 1)[-1]
    if len(value) >= 2 and value[0] == value[-1] == '"':
        value = value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return _mailbox_display(value)


def _encode_modified_utf7(value: str) -> str:
    result: list[str] = []
    pending: list[str] = []

    def flush() -> None:
        if not pending:
            return
        encoded = (
            base64.b64encode("".join(pending).encode("utf-16-be"))
            .decode("ascii")
            .rstrip("=")
            .replace("/", ",")
        )
        result.extend(("&", encoded, "-"))
        pending.clear()

    for char in value:
        if 0x20 <= ord(char) <= 0x7E:
            flush()
            result.append("&-" if char == "&" else char)
        else:
            pending.append(char)
    flush()
    return "".join(result)


def _decode_modified_utf7(value: str) -> str:
    result: list[str] = []
    index = 0
    while index < len(value):
        if value[index] != "&":
            result.append(value[index])
            index += 1
            continue
        end = value.find("-", index + 1)
        if end < 0:
            raise ValueError("invalid modified UTF-7 mailbox name")
        encoded = value[index + 1 : end]
        if not encoded:
            result.append("&")
        else:
            try:
                padded = encoded.replace(",", "/") + "=" * (-len(encoded) % 4)
                result.append(base64.b64decode(padded, validate=True).decode("utf-16-be"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise ValueError("invalid modified UTF-7 mailbox name") from exc
        index = end + 1
    return "".join(result)


def _has_capability(conn: Any, name: str) -> bool:
    capabilities = getattr(conn, "capabilities", ()) or ()
    normalized = {
        value.decode("ascii", "replace").upper() if isinstance(value, bytes) else str(value).upper()
        for value in capabilities
    }
    return name.upper() in normalized


def _address(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("recipient address is invalid")
    parsed = getaddresses([value])
    if not any(address for _, address in parsed) and "@[" in value:
        parsed = getaddresses([value], strict=False)
    if len(parsed) != 1 or not parsed[0][1]:
        raise ValueError("recipient address is invalid")
    try:
        return normalize_mailbox(parsed[0][1], "recipient")
    except MailContentError as exc:
        raise ValueError(str(exc)) from exc


def _smtp_message(value: Any) -> str:
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)


def _reset_smtp(conn: Any) -> None:
    try:
        conn.rset()
    except Exception:
        pass


def _interrupt_connection(conn: Any) -> None:
    """Wake a socket blocked in IMAP IDLE before acquiring its connection lock."""
    try:
        sock = getattr(conn, "sock", None)
        if sock is not None:
            sock.shutdown(socket.SHUT_RDWR)
    except OSError, AttributeError:
        pass
    _close_quietly(conn)


def _close_quietly(conn: Any) -> None:
    try:
        # IMAP CLOSE can expunge a mailbox; shutdown only closes its transport.
        if callable(shutdown := getattr(conn, "shutdown", None)):
            shutdown()
        else:
            conn.close()
    except OSError, AttributeError, imaplib.IMAP4.error:
        pass


__all__ = ["MAX_MESSAGE_BYTES", "MailTransport", "MailTransportError"]
