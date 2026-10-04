"""Workspace-scoped IMAP/SMTP service with durable local cursors and outbox."""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import inspect
import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from .async_utils import finish_owned, wait_owned
from .config import validate_mail_config
from .diagnostics import RPCError, safe_error
from .file_io import read_bytes as read_persisted_bytes
from .filesystem import Filesystem
from .mail_content import (
    MAX_BODY_BYTES,
    MAX_MIME_BYTES,
    MailContentError,
    attachment_bytes,
    build_mime,
    normalize_addresses,
    parse_message,
)
from .mail_store import MailStore
from .mail_transport import MailTransport, MailTransportError

_PUBLIC_METHODS = frozenset(
    {
        "accounts",
        "status",
        "mailboxes",
        "search",
        "read",
        "download_attachment",
        "mark_read",
        "mark_unread",
        "move",
        "draft",
        "send",
        "get_draft",
        "get_send",
        "sends",
        "watch",
        "unwatch",
        "watches",
        "notifications",
        "ack",
    }
)


class MailService:
    def __init__(
        self,
        root: Path,
        config: Mapping[str, Any] | None = None,
        io: Callable[..., Any] | None = None,
        notify: Callable[[str], Any] | None = None,
        connected: Callable[[], set[str]] | None = None,
        *,
        watch_interval: float = 30.0,
    ) -> None:
        self.root = Path(root).expanduser().resolve()
        self.config = validate_mail_config(config)
        self.io = io
        self.notify = notify or (lambda _client: None)
        self.connected = connected or (lambda: set())
        self.store: MailStore | None = None
        self.transport = MailTransport(self.config)
        self._watch_tasks: dict[tuple[str, str], asyncio.Task[Any]] = {}
        self._send_tasks: dict[str, asyncio.Task[Any]] = {}
        self._closed = False
        self._started = False
        self._active = 0
        self._request_accounts = {}
        self._send_accounts = {}
        self._config_applying = False
        self._close_task = None
        self._content_slots = asyncio.Semaphore(2)
        self._lock = asyncio.Lock()
        self.watch_interval = max(0.1, float(watch_interval))
        self._account_errors = {}
        self._status_cache = self._make_status()

    @property
    def active_count(self) -> int:
        return self._active + len(self._send_tasks)

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("mail service is closed")
        if self._started:
            return
        self.store = await self._persist(MailStore, self.root)
        await self._persist(self.store.recover_inflight)
        self._started = True
        await self._refresh_status_cache_async()
        await self.client_changed()

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await wait_owned(self._close_task)

    async def _close(self) -> None:
        self._closed = True
        tasks = [*self._watch_tasks.values(), *self._send_tasks.values()]
        for task in tasks:
            task.cancel()
        await asyncio.to_thread(self.transport.close)
        await asyncio.gather(*tasks, return_exceptions=True)
        requests = tuple(self._request_accounts)
        if requests:
            await asyncio.gather(*requests, return_exceptions=True)
        late = [task for task in self._send_tasks.values() if not task.done()]
        if late:
            await asyncio.gather(*late, return_exceptions=True)
        self._watch_tasks.clear()
        self._send_tasks.clear()
        if self.store is not None:
            try:
                await self._persist(self.store.close)
            except Exception:
                await asyncio.to_thread(self.store.close)
        self._refresh_status_cache()

    async def dispatch(self, method: str, client_id: str, params=None):
        if self._closed or self._config_applying:
            raise RuntimeError("Mail service is closing or reconfiguring")
        if params is None:
            params = {}
        if method not in _PUBLIC_METHODS:
            raise ValueError(f"unknown mail method: {method}")
        _required_string(client_id, "client_id")
        if not isinstance(params, Mapping):
            raise ValueError("mail parameters must be an object")
        task = asyncio.current_task()
        local = {
            "accounts",
            "status",
            "get_draft",
            "get_send",
            "sends",
            "watches",
            "notifications",
            "ack",
            "unwatch",
        }
        self._request_accounts[task] = set() if method in local else None
        self._active += 1
        try:
            result = await getattr(self, f"_{method}")(client_id, dict(params))
        except MailTransportError as exc:
            for name in self._request_accounts[task] or ():
                self._account_errors[name] = safe_error(exc)
            raise
        else:
            for name in self._request_accounts[task] or ():
                self._account_errors.pop(name, None)
        finally:
            self._active -= 1
            self._request_accounts.pop(task, None)
            self._refresh_status_cache()
        await self._refresh_status_cache_async()
        return result

    def _uses_accounts(self, names):
        task = asyncio.current_task()
        if task in self._request_accounts:
            current = self._request_accounts[task]
            self._request_accounts[task] = set(names) | (current or set())

    async def _content(self, function, /, *args, **kwargs):
        async with self._content_slots:
            return await wait_owned(asyncio.to_thread(function, *args, **kwargs))

    async def apply_config(self, config, force=False):
        if type(force) is not bool:
            raise TypeError("force must be a boolean")
        desired = validate_mail_config(config)
        async with self._lock:
            if self._closed:
                raise RuntimeError("Mail service is closed")
            self._config_applying = True
            try:
                old = self.config["accounts"]
                new = desired["accounts"]
                changed = {
                    name for name in old.keys() | new.keys() if old.get(name) != new.get(name)
                }
                unknown = any(value is None for value in self._request_accounts.values())
                busy = set(self._send_accounts.values())
                for value in self._request_accounts.values():
                    busy.update(value or ())
                deferred = changed if unknown else changed & busy
                applied = changed - deferred
                effective = copy.deepcopy(self.config)
                for name in applied:
                    if name in new:
                        effective["accounts"][name] = copy.deepcopy(new[name])
                    else:
                        effective["accounts"].pop(name, None)
                default = desired["default_account"]
                if not default or default in effective["accounts"]:
                    effective["default_account"] = default
                elif effective["default_account"] not in effective["accounts"]:
                    effective["default_account"] = ""
                for key in tuple(self._watch_tasks):
                    if key[0] in applied:
                        await self._stop_watch(key)
                await asyncio.to_thread(self.transport.reconfigure, effective, accounts=applied)
                self.config = effective
                await self._sync_watches()
                await self._refresh_status_cache_async()
                delays = {name: "Account has active mail work" for name in sorted(deferred)}
                if default != effective["default_account"]:
                    delays["default_account"] = "Selected account is deferred"
                return {
                    "applied": sorted(applied),
                    "deferred": delays,
                    "errors": {},
                    "applied_config": copy.deepcopy(effective),
                }
            finally:
                self._config_applying = False

    def snapshot(self, client_id: str) -> dict[str, Any] | None:
        if self.store is None:
            return None
        return self.store.snapshot(client_id)

    def status(self) -> dict[str, Any]:
        return copy.deepcopy(self._status_cache)

    def _make_status(self) -> dict[str, Any]:
        accounts = self._account_map()
        connections = self.transport.cached_status()
        result_accounts = []
        for name, account in accounts.items():
            result_accounts.append(
                {
                    "name": name,
                    "enabled": account.get("enabled", True) is not False,
                    "imap": _endpoint_status(account, "imap"),
                    "smtp": _endpoint_status(account, "smtp"),
                    "credentials": _credential_status(account),
                    "imap_connected": name in connections["imap"],
                    "smtp_connected": name in connections["smtp"],
                    "last_error": self._account_errors.get(name),
                }
            )
        return {
            "accounts": result_accounts,
            "watches": [],
            "pending_sends": [],
            "active": self._active,
            "active_count": self._active,
            "started": self._started,
            "closed": self._closed,
            "connections": connections,
        }

    def _refresh_status_cache(self) -> None:
        self._status_cache.update(
            accounts=self._make_status()["accounts"],
            connections=self.transport.cached_status(),
            active=self.active_count,
            active_count=self.active_count,
            started=self._started,
            closed=self._closed,
        )

    async def _refresh_status_cache_async(self) -> None:
        self._refresh_status_cache()
        if self.store is None or self._closed:
            return
        watches, sends = await self._persist(
            lambda: (self.store.watches(), self.store.sends_for_status())
        )
        self._status_cache.update(watches=watches, pending_sends=sends)

    async def client_changed(self) -> None:
        async with self._lock:
            await self._sync_watches()

    async def _stop_watch(self, key):
        task = self._watch_tasks.pop(key, None)
        if task is not None:
            await asyncio.to_thread(self.transport.close_watch, *key)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.to_thread(self.transport.close_watch, *key)

    async def _sync_watches(self):
        if self._closed or self.store is None:
            return
        watches = await self._persist(self.store.watches)
        online = set(self.connected())
        accounts = self._account_map()
        wanted = {
            (w["account"], w["mailbox"])
            for w in watches
            if w["client_id"] in online and w["account"] in accounts
        }
        for key in tuple(self._watch_tasks):
            if key not in wanted:
                await self._stop_watch(key)
        for watch in watches:
            key = (watch["account"], watch["mailbox"])
            if key not in wanted:
                enabled = watch["account"] in accounts
                await self._persist(
                    self.store.update_watch,
                    watch["id"],
                    state="offline" if enabled else "error",
                    error="No connected subscribers" if enabled else "Account is not configured",
                )
        for key in wanted:
            if key not in self._watch_tasks or self._watch_tasks[key].done():
                self._watch_tasks[key] = asyncio.create_task(
                    self._watch_loop(*key),
                    name=f"mypr:mail:{key[0]}:{key[1]}",
                )
        await self._refresh_status_cache_async()

    def _account_map(self):
        return {
            name: value
            for name, value in self.config["accounts"].items()
            if value.get("enabled", True)
        }

    def _account_name(self, params: Mapping[str, Any], *, fallback: str | None = None) -> str:
        value = params.get("account")
        if value is None:
            value = fallback
        if value is None:
            value = self.config["default_account"] or None
        accounts = self._account_map()
        if value is None:
            if len(accounts) != 1:
                raise ValueError("account requires a default or exactly one configured account")
            value = next(iter(accounts))
        if value not in accounts:
            raise ValueError(f"unknown mail account: {value}")
        return str(value)

    async def _accounts(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "items": [
                {"name": name, "enabled": value.get("enabled", True) is not False}
                for name, value in self._account_map().items()
            ]
        }

    async def _status(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        result = self.status()
        result["watches"] = [
            item for item in result["watches"] if item.get("client_id") == client_id
        ]
        result["pending_sends"] = [
            item for item in result["pending_sends"] if item.get("client_id") == client_id
        ]
        account = params.get("account")
        if account is not None:
            result["accounts"] = [item for item in result["accounts"] if item["name"] == account]
        return result

    async def _mailboxes(self, client_id, params):
        name = self._account_name(params)
        self._uses_accounts({name})
        return {
            "account": name,
            "items": await self.transport.run(
                self.transport.list_mailboxes,
                name,
            ),
        }

    async def _search(self, client_id, params):
        name = self._account_name(params)
        mailbox = _mailbox(params.get("mailbox", "INBOX"))
        self._uses_accounts({name})
        limit = _limit(params.get("limit", 20))
        query = {
            key: params.get(key)
            for key in ("unread", "sender", "subject", "text", "since", "before")
        }
        if query["unread"] is not None and type(query["unread"]) is not bool:
            raise TypeError("unread must be a boolean")
        for key, value in query.items():
            if key != "unread" and value is not None:
                if not isinstance(value, str) or len(value.encode()) > 16384:
                    raise ValueError(f"{key} must be a string of at most 16 KiB")
        query_hash = hashlib.sha256(json.dumps(query, sort_keys=True).encode()).hexdigest()
        identity = self.transport.endpoint_identity(name)
        bound = {
            "kind": "search",
            "account": name,
            "mailbox": mailbox,
            "identity": identity,
            "query": query_hash,
        }
        saved = _decode_cursor(params.get("cursor"))
        if saved is not None and any(saved.get(key) != value for key, value in bound.items()):
            raise RPCError("Search cursor belongs to another query", code="invalid_cursor")
        after_uid = _int_or_none(saved.get("uid"), "cursor UID") if saved is not None else 0
        if saved is not None and not after_uid:
            raise RPCError("Invalid search cursor UID", code="invalid_cursor")
        result = await self.transport.run(
            self.transport.search,
            name,
            mailbox,
            **query,
            after_uid=after_uid,
            limit=limit,
        )
        namespace = int(result["namespace"]["uidvalidity"])
        if saved is not None and saved.get("uidvalidity") != namespace:
            raise RPCError("Mailbox namespace changed while paging", code="stale_reference")
        refs = []
        for uid in result["uids"]:
            ref = await self._persist(
                self.store.message_ref,
                name,
                mailbox,
                namespace,
                uid,
                identity,
            )
            raw = await self.transport.run(
                self.transport.fetch,
                name,
                mailbox,
                namespace,
                uid,
                headers_only=True,
            )
            parsed = await self._content(parse_message, raw, max_bytes=32768)
            refs.append({"id": ref, "uid": uid, "headers": parsed["headers"]})
        more = bool(result.get("has_more"))
        cursor = (
            _encode_cursor({**bound, "uidvalidity": namespace, "uid": refs[-1]["uid"]})
            if more and refs
            else None
        )
        return {
            "account": name,
            "mailbox": mailbox,
            "items": refs,
            "has_more": more,
            "next_cursor": cursor,
        }

    async def _resolve_ref(self, ident):
        ref = await self._persist(self.store.resolve_ref, _required_string(ident, "message_id"))
        if ref is None:
            raise RPCError("Unknown message reference", code="not_found")
        account = ref["account"]
        if account not in self._account_map() or (
            ref["endpoint_identity"] != self.transport.endpoint_identity(account)
        ):
            raise RPCError("Mail account endpoint changed", code="stale_reference")
        return ref

    async def _raw_message(self, ref, *, headers_only=False):
        return await self.transport.run(
            self.transport.fetch,
            ref["account"],
            ref["mailbox"],
            ref["uidvalidity"],
            ref["uid"],
            headers_only=headers_only,
        )

    async def _read(self, client_id, params):
        ident = _required_string(params.get("message_id"), "message_id")
        ref = await self._resolve_ref(ident)
        self._uses_accounts({ref["account"]})
        maximum = params.get("max_bytes", 32768)
        if type(maximum) is not int or not 1 <= maximum <= MAX_BODY_BYTES:
            raise ValueError("max_bytes must be between 1 and 1048576")
        raw = await self._raw_message(ref)
        revision = hashlib.sha256(raw).hexdigest()
        saved = _decode_cursor(params.get("cursor"))
        bound = {"kind": "body", "message_id": ident, "revision": revision}
        if saved is not None and any(saved.get(key) != value for key, value in bound.items()):
            raise RPCError(
                "Message changed or body cursor belongs to another message", code="invalid_cursor"
            )
        offset = _int_or_none(saved.get("offset"), "cursor offset") if saved is not None else None
        if saved is not None and offset is None:
            raise RPCError("Invalid body cursor offset", code="invalid_cursor")
        parsed = await self._content(parse_message, raw, max_bytes=maximum, cursor=offset)
        if parsed["has_more"]:
            parsed["next_cursor"] = _encode_cursor({**bound, "offset": parsed["next_cursor"]})
        parsed.update(
            id=ident,
            account=ref["account"],
            mailbox=ref["mailbox"],
            uid=ref["uid"],
            revision=revision,
        )
        return parsed

    async def _download_attachment(self, client_id, params):
        path = _required_string(params.get("path"), "path")
        overwrite = params.get("overwrite", False)
        if type(overwrite) is not bool:
            raise TypeError("overwrite must be a boolean")
        fs = Filesystem(self.root)
        supplied = await self._content(lambda: Path(path).expanduser())
        path = await self._content(
            lambda: (supplied if supplied.is_absolute() else self.root / supplied).resolve()
        )
        try:
            expected = (await fs.read_bytes(path, max_bytes=1))["revision"]
        except FileNotFoundError:
            expected = None
        if expected is not None and not overwrite:
            raise FileExistsError(path)
        ref = await self._resolve_ref(params.get("message_id"))
        self._uses_accounts({ref["account"]})
        raw = await self._raw_message(ref)
        filename, data, content_type = await self._content(
            attachment_bytes,
            raw,
            _required_string(params.get("attachment_id"), "attachment_id"),
        )
        result = await fs.write_bytes(
            path,
            data,
            expected_hash=expected,
            overwrite=expected is not None,
            create_parents=True,
        )
        return {
            **result,
            "filename": filename,
            "content_type": content_type,
            "size": len(data),
            "message_id": ref["id"],
        }

    async def _mark_read(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return await self._mark_seen(params, True)

    async def _mark_unread(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return await self._mark_seen(params, False)

    async def _mark_seen(self, params: Mapping[str, Any], seen: bool) -> dict[str, Any]:
        refs = await self._refs(params.get("message_ids"))
        grouped: dict[tuple[str, str, int], list[int]] = {}
        for ref in refs:
            grouped.setdefault(
                (ref["account"], ref["mailbox"], int(ref["uidvalidity"])), []
            ).append(int(ref["uid"]))
        for (account, mailbox, uidvalidity), uids in grouped.items():
            await self.transport.run(
                self.transport.set_seen, account, mailbox, uids, seen, uidvalidity=uidvalidity
            )
        return {"changed": len(refs), "seen": seen}

    async def _move(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        refs = await self._refs(params.get("message_ids"))
        destination = _mailbox(params.get("mailbox"))
        grouped: dict[tuple[str, str, int], list[int]] = {}
        for ref in refs:
            grouped.setdefault(
                (ref["account"], ref["mailbox"], int(ref["uidvalidity"])), []
            ).append(int(ref["uid"]))
        for (account, mailbox, uidvalidity), uids in grouped.items():
            await self.transport.run(
                self.transport.move, account, mailbox, uids, destination, uidvalidity=uidvalidity
            )
        return {"moved": len(refs), "mailbox": destination}

    async def _draft(self, client_id, params):
        if params.get("reply_to") is not None and params.get("forward") is not None:
            raise ValueError("reply_to and forward cannot both be set")
        reply = await self._reply_data(params["reply_to"]) if params.get("reply_to") else None
        forward = (
            await self._reply_data(params["forward"], include_attachments=True)
            if params.get("forward")
            else None
        )
        source = reply or forward
        account = self._account_name(params, fallback=source["account"] if source else None)
        self._uses_accounts({account, *([source["account"]] if source else [])})
        sender = self._account_map()[account]["from"]
        mime, recipients, message_id = await self._content(
            build_mime,
            sender=sender,
            to=params.get("to"),
            cc=params.get("cc") or (),
            bcc=params.get("bcc") or (),
            subject=params.get("subject") or "",
            text=params.get("text"),
            html=params.get("html"),
            attachments=params.get("attachments") or (),
            reply_to=reply,
            forward=forward,
            workspace_root=self.root,
        )
        preview = await self._content(parse_message, mime, max_bytes=32768)
        metadata = {
            "sender": sender,
            "to": normalize_addresses(preview["headers"].get("to"), "to"),
            "cc": normalize_addresses(params.get("cc"), "cc"),
            "bcc": normalize_addresses(params.get("bcc"), "bcc"),
            "subject": preview["headers"].get("subject", ""),
            "message_id": message_id,
            "recipients": recipients,
            "size": len(mime),
        }
        record = await self._persist(self.store.persist_draft, client_id, account, mime, metadata)
        return {
            **{key: value for key, value in record.items() if key != "mime_path"},
            "preview": preview,
        }

    async def _send(self, client_id, params):
        draft_id = _required_string(params.get("draft_id"), "draft_id")
        request_id = params.get("request_id")
        request_id = (
            _required_string(request_id, "request_id")
            if request_id is not None
            else f"draft:{draft_id}"
        )
        draft = await self._persist(self.store.get_draft, draft_id, client_id)
        if draft is None:
            raise RPCError("Unknown draft", code="not_found")
        self._uses_accounts({draft["account"]})
        record = await self._persist(self.store.create_send, client_id, draft_id, request_id)
        ident = record["id"]
        if record["state"] == "queued" and ident not in self._send_tasks:
            self._send_accounts[ident] = draft["account"]
            task = asyncio.create_task(self._run_send(ident), name=f"mypr:mail:send:{ident}")
            self._send_tasks[ident] = task
            task.add_done_callback(lambda done: self._send_done(ident, done))
        return record

    def _send_done(self, ident, task):
        self._send_tasks.pop(ident, None)
        self._send_accounts.pop(ident, None)
        self._refresh_status_cache()
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                self._status_cache["background_error"] = type(error).__name__

    async def _run_send(self, send_id):
        record = await self._persist(self.store.get_send, send_id)
        if record is None:
            return
        attempted = False
        outcome = None
        try:
            draft = await self._persist(self.store.get_draft, record["draft_id"])
            if draft is None:
                raise FileNotFoundError("Draft no longer exists")
            mime = await self._content(
                read_persisted_bytes,
                Path(draft["mime_path"]),
                max_bytes=MAX_MIME_BYTES,
            )
            if draft.get("mime_sha256") != hashlib.sha256(mime).hexdigest():
                raise MailContentError("Draft MIME changed after creation")
            if self._closed:
                raise RuntimeError("Mail service stopped before SMTP was attempted")
            await self._persist(self.store.update_send, send_id, state="sending")
            attempted = True
            outcome, send_cancelled = await finish_owned(
                self.transport.run(
                    self.transport.send,
                    draft["account"],
                    mime,
                    list(draft["recipients"]),
                )
            )
            if send_cancelled:
                raise asyncio.CancelledError
            accepted, rejected = outcome["accepted"], outcome["rejected"]
            state = "partial" if accepted and rejected else "accepted" if accepted else "failed"
            await self._persist(
                self.store.update_send,
                send_id,
                state=state,
                accepted=accepted,
                rejected=rejected,
                stage=outcome.get("stage"),
                rejected_details=outcome.get("rejected_details", []),
                error=outcome.get("error"),
            )
            if accepted:
                try:
                    await self.transport.run(self.transport.append_sent, draft["account"], mime)
                except Exception as exc:
                    await self._persist(
                        self.store.update_send,
                        send_id,
                        state=state,
                        warning=f"Sent copy was not confirmed: {type(exc).__name__}",
                    )
        except asyncio.CancelledError:
            if outcome is not None:
                accepted, rejected = outcome["accepted"], outcome["rejected"]
                state = "partial" if accepted and rejected else "accepted" if accepted else "failed"
                await self._persist(
                    self.store.update_send,
                    send_id,
                    state=state,
                    accepted=accepted,
                    rejected=rejected,
                    stage=outcome.get("stage"),
                    warning="Interrupted after SMTP settled",
                )
            else:
                state = "unknown" if attempted else "failed"
                await self._persist(
                    self.store.update_send,
                    send_id,
                    state=state,
                    stage="interrupted",
                    error="Mail service stopped before the transaction settled",
                )
            raise
        except Exception as exc:
            if outcome is not None:
                accepted, rejected = outcome["accepted"], outcome["rejected"]
                state = "partial" if accepted and rejected else "accepted" if accepted else "failed"
            else:
                state = "unknown" if attempted else "failed"
                if isinstance(exc, MailTransportError) and not exc.ambiguous:
                    state = "failed"
            await self._persist(
                self.store.update_send,
                send_id,
                state=state,
                error=safe_error(exc),
                stage=outcome.get("stage") if outcome is not None else getattr(exc, "stage", None),
                accepted=outcome["accepted"]
                if outcome is not None
                else getattr(exc, "accepted", None),
                rejected=outcome["rejected"]
                if outcome is not None
                else getattr(exc, "rejected", None),
                rejected_details=getattr(exc, "rejected_details", None),
            )
        finally:
            self.notify(record["client_id"])
            await self._refresh_status_cache_async()

    async def _get_draft(self, client_id, params):
        ident = _required_string(params.get("draft_id"), "draft_id")
        value = await self._persist(self.store.get_draft, ident, client_id)
        if value is None:
            raise RPCError("Unknown draft", code="not_found")
        result = {key: item for key, item in value.items() if key != "mime_path"}
        try:
            raw = await self._content(
                read_persisted_bytes,
                Path(value["mime_path"]),
                max_bytes=MAX_MIME_BYTES,
            )
            result["preview"] = await self._content(parse_message, raw, max_bytes=32768)
        except (OSError, ValueError) as exc:
            result["preview_error"] = safe_error(exc)
        return result

    async def _get_send(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        ident = _required_string(params.get("send_id"), "send_id")
        value = await self._persist(self.store.get_send, ident, client_id)
        if value is None:
            raise ValueError(f"unknown send ID: {ident}")
        return value

    async def _sends(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return await self._persist(
            self.store.sends,
            client_id,
            limit=_limit(params.get("limit", 20)),
            cursor=_int_or_none(params.get("cursor"), "cursor"),
        )

    async def _watch(self, client_id, params):
        account = self._account_name(params)
        mailbox = _mailbox(params.get("mailbox", "INBOX"))
        self._uses_accounts({account})
        existing = next(
            (
                w
                for w in await self._persist(self.store.watches, client_id)
                if w["account"] == account and w["mailbox"] == mailbox
            ),
            None,
        )
        if existing is not None:
            await self.client_changed()
            return await self._persist(self.store.get_watch, existing["id"], client_id)
        namespace = await self.transport.run(self.transport.namespace, account, mailbox)
        watch = await self._persist(
            self.store.create_watch,
            client_id,
            account,
            mailbox,
            uidvalidity=namespace["uidvalidity"],
            last_uid=max(0, namespace["uidnext"] - 1),
            endpoint_identity=self.transport.endpoint_identity(account),
        )
        await self.client_changed()
        return await self._persist(self.store.get_watch, watch["id"], client_id)

    async def _unwatch(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        ident = _required_string(params.get("watch_id"), "watch_id")
        result = await self._persist(self.store.delete_watch, ident, client_id)
        await self.client_changed()
        return result

    async def _watches(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return {"items": await self._persist(self.store.watches, client_id)}

    async def _notifications(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        return await self._persist(
            self.store.notifications,
            client_id,
            limit=_limit(params.get("limit", 20)),
            cursor=_int_or_none(params.get("cursor"), "cursor"),
        )

    async def _ack(self, client_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        ids = _ids(params.get("notification_ids"), "notification_ids")
        return {"acknowledged": await self._persist(self.store.ack_notifications, client_id, ids)}

    async def _refs(self, values):
        ids = _ids(values, "message_ids")
        refs = [await self._resolve_ref(ident) for ident in ids]
        self._uses_accounts({ref["account"] for ref in refs})
        return refs

    async def _reply_data(self, ident, *, include_attachments=False):
        ref = await self._resolve_ref(ident)
        self._uses_accounts({ref["account"]})
        raw = await self._raw_message(ref, headers_only=not include_attachments)
        result = await self._content(
            parse_message,
            raw,
            max_bytes=MAX_MIME_BYTES if include_attachments else 32768,
            include_attachment_data=include_attachments,
        )
        if result["has_more"]:
            raise MailContentError("Forwarded body exceeds the MIME size limit")
        result["account"] = ref["account"]
        return result

    async def _watch_loop(self, account, mailbox):
        while not self._closed:
            watches = [
                w
                for w in await self._persist(self.store.watches)
                if w["account"] == account and w["mailbox"] == mailbox
            ]
            if not watches:
                return
            try:
                result = await self.transport.run_watch(
                    self.transport.watch_search,
                    account,
                    mailbox,
                    after_uid=min(w["last_uid"] for w in watches),
                    limit=100,
                )
                ns = result["namespace"]
                identity = self.transport.endpoint_identity(account)
                headers = {}
                for uid in result["uids"]:
                    raw = await self.transport.run(
                        self.transport.fetch,
                        account,
                        mailbox,
                        ns["uidvalidity"],
                        uid,
                        headers_only=True,
                        max_bytes=64 * 1024,
                    )
                    parsed = await self._content(parse_message, raw, max_bytes=32768)
                    headers[uid] = {
                        key: value
                        for key, value in parsed["headers"].items()
                        if key in {"subject", "from", "date"}
                    }
                for watch in watches:
                    changed = (
                        watch["uidvalidity"] != ns["uidvalidity"]
                        or watch["endpoint_identity"] != identity
                    )
                    if changed:
                        await self._persist(
                            self.store.reset_watch_namespace,
                            watch["id"],
                            uidvalidity=ns["uidvalidity"],
                            last_uid=max(0, ns["uidnext"] - 1),
                            endpoint_identity=identity,
                        )
                        self.notify(watch["client_id"])
                        continue
                    notes = [
                        {"uid": uid, "kind": "new_mail", "payload": headers[uid]}
                        for uid in result["uids"]
                        if uid > watch["last_uid"]
                    ]
                    last = max(watch["last_uid"], max((item["uid"] for item in notes), default=0))
                    committed = await self._persist(
                        self.store.commit_watch,
                        watch["id"],
                        uidvalidity=ns["uidvalidity"],
                        last_uid=last,
                        endpoint_identity=identity,
                        notifications=notes,
                    )
                    if committed:
                        self.notify(watch["client_id"])
                await self._refresh_status_cache_async()
                if result.get("has_more"):
                    continue
                idle = await self.transport.run_watch(
                    self.transport.watch_idle,
                    account,
                    mailbox,
                    duration=self.watch_interval,
                )
                if idle is None:
                    await asyncio.sleep(self.watch_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                for watch in watches:
                    await self._persist(
                        self.store.update_watch,
                        watch["id"],
                        state="error",
                        error=safe_error(exc),
                    )
                    self.notify(watch["client_id"])
                await self._refresh_status_cache_async()
                await asyncio.sleep(self.watch_interval)

    async def _persist(self, function, /, *args, **kwargs):
        call = (
            asyncio.to_thread(function, *args, **kwargs)
            if self.io is None
            else self.io(function, *args, **kwargs)
        )
        return await wait_owned(call) if inspect.isawaitable(call) else call

    def storage_gc_snapshot(self):
        if self.store is None:
            raise RuntimeError("Mail storage is unavailable")
        return self.store.storage_gc_snapshot()

    def storage_gc_before_delete(self, candidates):
        return self.store.storage_gc_before_delete(candidates)

    def storage_gc_after_delete(self, deleted):
        return self.store.storage_gc_after_delete(deleted)

    async def prune(self, retention_days=30):
        return await self._persist(self.store.prune, retention_days=retention_days)


def _required_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    if len(value) > 4096 or "\x00" in value:
        raise ValueError(f"{name} is too long or contains NUL")
    return value.strip()


def _mailbox(value: Any) -> str:
    _required_string(value, "mailbox")
    if "\r" in value or "\n" in value or len(value.encode()) > 512:
        raise ValueError("mailbox is invalid")
    return "INBOX" if value.casefold() == "inbox" else value


def _int_or_none(value, name):
    if value is None:
        return None
    if type(value) is int:
        result = value
    elif isinstance(value, str) and value.isascii() and value.isdecimal():
        result = int(value)
    else:
        raise ValueError(f"{name} must be a non-negative integer")
    if not 0 <= result <= 2**63 - 1:
        raise ValueError(f"{name} is out of range")
    return result


def _limit(value):
    if type(value) is not int or not 1 <= value <= 100:
        raise ValueError("limit must be between 1 and 100")
    return value


def _ids(value, name):
    if not isinstance(value, list) or len(value) > 100:
        raise ValueError(f"{name} must contain at most 100 IDs")
    return [_required_string(item, name) for item in value]


def _encode_cursor(value):
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()


def _decode_cursor(value):
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise RPCError("Invalid mail cursor", code="invalid_cursor")
    try:
        result = json.loads(base64.b64decode(value, altchars=b"-_", validate=True))
        if not isinstance(result, dict):
            raise ValueError
        return result
    except (ValueError, UnicodeError) as exc:
        raise RPCError("Invalid mail cursor", code="invalid_cursor") from exc


def _endpoint_status(account: Mapping[str, Any], kind: str) -> dict[str, Any]:
    endpoint = account.get(kind)
    if not isinstance(endpoint, Mapping):
        endpoint = {
            key[len(kind) + 1 :]: value
            for key, value in account.items()
            if isinstance(key, str) and key.startswith(kind + "_")
        }
    if not endpoint:
        return {"configured": False}
    return {
        "configured": bool(endpoint.get("host")),
        "host": endpoint.get("host"),
        "port": endpoint.get("port"),
        "security": endpoint.get("security"),
    }


def _credential_status(account: Mapping[str, Any]) -> dict[str, Any]:
    result = {}
    for kind in ("imap", "smtp"):
        endpoint = account.get(kind) if isinstance(account.get(kind), Mapping) else account
        name = endpoint.get("password_from") if isinstance(endpoint, Mapping) else None
        required = kind == "imap" or bool(endpoint.get("username"))
        result[kind] = {
            "configured": bool(name),
            "required": required,
            "available": (name in os.environ) if required and name else not required,
            "source": name,
        }
    return result
