from __future__ import annotations

import ast
import asyncio
import contextlib
import re
import shlex
import socketserver
import threading
from pathlib import Path

from conftest import decode_result, execute, mcp_session, result_text, stop_manager

MESSAGE = (
    b"From: sender@example.test\r\n"
    b"To: receiver@example.test\r\n"
    b"Subject: =?utf-8?b?5pel5pys6Kqe?=\r\n"
    b"Message-ID: <initial@example.test>\r\n"
    b"\r\n"
    b"initial body\r\n"
)


class _MailState:
    def __init__(self, messages: list[bytes] | None = None) -> None:
        self.lock = threading.RLock()
        self.messages: dict[str, list[dict[str, object]]] = {
            "INBOX": [],
            "Sent": [],
        }
        self.uidvalidity = 41
        self.next_uid = 1
        self.appended: list[bytes] = []
        for raw in messages or ():
            self.add("INBOX", raw)

    def add(self, mailbox: str, raw: bytes) -> int:
        with self.lock:
            uid = self.next_uid
            self.next_uid += 1
            self.messages.setdefault(mailbox, []).append({"uid": uid, "raw": raw, "seen": False})
            return uid

    def rows(self, mailbox: str) -> list[dict[str, object]]:
        with self.lock:
            return list(self.messages.get(mailbox, ()))


class _IMAPHandler(socketserver.StreamRequestHandler):
    state: _MailState

    def handle(self) -> None:
        self.wfile.write(b"* OK local mypr IMAP ready\r\n")
        selected = "INBOX"
        while True:
            raw = self.rfile.readline()
            if not raw:
                return
            line = raw.rstrip(b"\r\n").decode("utf-8", "replace")
            parts = line.split(" ", 2)
            if len(parts) < 2:
                continue
            tag, command = parts[0], parts[1].upper()
            args = parts[2] if len(parts) > 2 else ""
            if command == "CAPABILITY":
                self._reply(tag, b"* CAPABILITY IMAP4rev1 UIDPLUS\r\n", b"OK CAPABILITY")
            elif command == "NOOP":
                self._reply(tag, b"", b"OK NOOP")
            elif command == "LOGIN":
                self._reply(tag, b"", b"OK LOGIN")
            elif command == "LOGOUT":
                self._reply(tag, b"* BYE closing\r\n", b"OK LOGOUT")
                return
            elif command == "LIST":
                lines = b"".join(
                    f'* LIST (\\HasNoChildren) "/" "{name}"\r\n'.encode()
                    for name in self.state.messages
                )
                self._reply(tag, lines, b"OK LIST")
            elif command in {"SELECT", "EXAMINE"}:
                selected = _quoted_or_last(args)
                rows = self.state.rows(selected)
                extra = (
                    f"* {len(rows)} EXISTS\r\n".encode()
                    + b"* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft \\Recent)\r\n"
                    + b"* OK [PERMANENTFLAGS "
                    b"(\\Answered \\Flagged \\Deleted \\Seen \\Draft \\*)] flags\r\n"
                    + f"* OK [UIDVALIDITY {self.state.uidvalidity}] uidvalidity\r\n".encode()
                    + f"* OK [UIDNEXT {self.state.next_uid}] uidnext\r\n".encode()
                )
                self._reply(tag, extra, b"OK [READ-WRITE] SELECT")
            elif command == "UID":
                subparts = args.split(" ", 2)
                if not subparts:
                    self._reply(tag, b"", b"BAD UID")
                    continue
                subcommand = subparts[0].upper()
                rest = subparts[1:]
                if subcommand == "SEARCH":
                    uids = [str(row["uid"]) for row in self.state.rows(selected) if not row["seen"]]
                    if "UNSEEN" not in args.upper():
                        uids = [str(row["uid"]) for row in self.state.rows(selected)]
                    self._reply(tag, b"* SEARCH " + " ".join(uids).encode() + b"\r\n", b"OK SEARCH")
                elif subcommand == "FETCH":
                    uid = _first_int(rest[0] if rest else "0")
                    row = next(
                        (row for row in self.state.rows(selected) if row["uid"] == uid), None
                    )
                    if row is None:
                        self._reply(tag, b"", b"OK FETCH")
                        continue
                    raw_message = row["raw"]
                    assert isinstance(raw_message, bytes)
                    if "BODY" not in args.upper():
                        response = (
                            f"* {uid} FETCH (UID {uid} RFC822.SIZE {len(raw_message)})\r\n".encode()
                        )
                    else:
                        body = raw_message
                        section = "HEADER" if "[HEADER]" in args.upper() else ""
                        if section:
                            body = body.split(b"\r\n\r\n", 1)[0] + b"\r\n\r\n"
                        partial = re.search(r"<(\d+)\.(\d+)>", args)
                        offset = ""
                        if partial:
                            start_byte, size = map(int, partial.groups())
                            body = body[start_byte : start_byte + size]
                            offset = f"<{start_byte}>"
                        response = (
                            f"* {uid} FETCH (UID {uid} RFC822.SIZE {len(raw_message)} "
                            f"BODY[{section}]{offset} {{{len(body)}}}\r\n".encode()
                            + body
                            + b")\r\n"
                        )
                    self._reply(tag, response, b"OK FETCH")
                elif subcommand == "STORE":
                    wanted = {_first_int(value) for value in (rest[0] if rest else "").split(",")}
                    seen = "+FLAGS" in args.upper()
                    with self.state.lock:
                        for row in self.state.messages.get(selected, ()):
                            if row["uid"] in wanted:
                                row["seen"] = seen
                    self._reply(tag, b"", b"OK STORE")
                elif subcommand == "MOVE":
                    uid = _first_int(rest[0] if rest else "0")
                    destination = rest[1].strip('"') if len(rest) > 1 else ""
                    with self.state.lock:
                        source_rows = self.state.messages.setdefault(selected, [])
                        moved = [row for row in source_rows if row["uid"] == uid]
                        self.state.messages[selected] = [
                            row for row in source_rows if row["uid"] != uid
                        ]
                        for row in moved:
                            self.state.add(destination, row["raw"])
                    self._reply(tag, b"", b"OK MOVE")
                else:
                    self._reply(tag, b"", b"OK UID")
            elif command == "APPEND":
                literal = args.rsplit("{", 1)[-1].rstrip("}")
                size = int(literal)
                self.wfile.write(b"+ Ready for literal\r\n")
                data = self.rfile.read(size)
                self.rfile.readline()
                self.state.add(_first_quoted(args), data)
                self._reply(tag, b"", b"OK APPEND")
            else:
                self._reply(tag, b"", b"OK")

    def _reply(self, tag: str, untagged: bytes, status: bytes) -> None:
        self.wfile.write(untagged + tag.encode() + b" " + status + b"\r\n")


class _SMTPHandler(socketserver.StreamRequestHandler):
    state: _MailState

    def handle(self) -> None:
        self.wfile.write(b"220 local mypr SMTP ready\r\n")
        recipients: list[str] = []
        while True:
            raw = self.rfile.readline()
            if not raw:
                return
            line = raw.rstrip(b"\r\n")
            upper = line.upper()
            if upper.startswith((b"EHLO", b"HELO")):
                self.wfile.write(b"250-local\r\n250 SIZE 26214400\r\n")
            elif upper.startswith(b"MAIL FROM:"):
                recipients = []
                self.wfile.write(b"250 sender ok\r\n")
            elif upper.startswith(b"RCPT TO:"):
                recipients.append(line.split(b":", 1)[1].strip().decode("utf-8", "replace"))
                self.wfile.write(b"250 recipient ok\r\n")
            elif upper == b"DATA":
                self.wfile.write(b"354 end with <CRLF>.<CRLF>\r\n")
                chunks: list[bytes] = []
                while True:
                    chunk = self.rfile.readline()
                    if chunk in (b"", b".\r\n"):
                        break
                    chunks.append(chunk)
                message = b"".join(chunks)
                self.state.appended.append(message)
                self.wfile.write(b"250 accepted\r\n")
            elif upper == b"QUIT":
                self.wfile.write(b"221 bye\r\n")
                return
            else:
                self.wfile.write(b"250 ok\r\n")


class _ThreadedServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class LocalMailServer:
    def __init__(self, initial: list[bytes] | None = None) -> None:
        self.state = _MailState(initial)
        imap_handler = type("LocalIMAPHandler", (_IMAPHandler,), {"state": self.state})
        smtp_handler = type("LocalSMTPHandler", (_SMTPHandler,), {"state": self.state})
        self.imap = _ThreadedServer(("127.0.0.1", 0), imap_handler)
        self.smtp = _ThreadedServer(("127.0.0.1", 0), smtp_handler)
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for server in (self.imap, self.smtp):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        for server in (self.imap, self.smtp):
            server.shutdown()
            server.server_close()
        for thread in self._threads:
            thread.join(timeout=2)

    @property
    def imap_port(self) -> int:
        return int(self.imap.server_address[1])

    @property
    def smtp_port(self) -> int:
        return int(self.smtp.server_address[1])

    def add_message(self, raw: bytes = MESSAGE) -> int:
        return self.state.add("INBOX", raw)


def _quoted_or_last(value: str) -> str:
    try:
        values = shlex.split(value)
    except ValueError:
        values = []
    return values[-1] if values else value.strip().split()[-1].strip('"')


def _first_quoted(value: str) -> str:
    try:
        values = shlex.split(value)
    except ValueError:
        values = []
    return values[0] if values else value.strip().split()[0].strip('"')


def _first_int(value: str) -> int:
    for token in value.replace("(", " ").replace(")", " ").split():
        if token.isdigit():
            return int(token)
    return 0


@contextlib.contextmanager
def local_mail_server(initial: list[bytes] | None = None):
    server = LocalMailServer(initial)
    server.start()
    try:
        yield server
    finally:
        server.stop()


def _write_config(workspace: Path, server: LocalMailServer) -> None:
    root = workspace / ".mypr"
    root.mkdir(exist_ok=True)
    (root / "config.toml").write_text(
        "[mail]\n"
        'default_account = "local"\n\n'
        "[mail.accounts.local]\n"
        'from = "agent@example.test"\n'
        'sent_mailbox = "Sent"\n\n'
        "[mail.accounts.local.imap]\n"
        'host = "127.0.0.1"\n'
        f"port = {server.imap_port}\n"
        'security = "plain"\n'
        'username = "user"\n'
        'password_from = "MYPR_TEST_MAIL_PASSWORD"\n\n'
        "[mail.accounts.local.smtp]\n"
        'host = "127.0.0.1"\n'
        f"port = {server.smtp_port}\n"
        'security = "plain"\n'
    )


async def _wait_for_notification(session, *, deadline_seconds: float = 8) -> dict:
    deadline = asyncio.get_running_loop().time() + deadline_seconds
    while asyncio.get_running_loop().time() < deadline:
        result = await execute(session, "await ws.mail.notifications(limit=10)", wait_ms=0)
        text = result_text(result)
        if "mail-" in text:
            return result
        await asyncio.sleep(0.1)
    raise AssertionError("mail notification did not arrive")


async def test_mail_watch_metadata_is_client_scoped_and_read_does_not_mark_seen(
    workspace, monkeypatch
):
    monkeypatch.setenv("MYPR_TEST_MAIL_PASSWORD", "password")
    with local_mail_server([MESSAGE]) as server:
        _write_config(workspace, server)
        async with (
            mcp_session(workspace, client_id="mail-a") as first,
            mcp_session(workspace, client_id="mail-b") as second,
        ):
            initial = decode_result(await first.call_tool("init", {}))
            assert "mail" in initial["runtime"]["capabilities"]
            await execute(first, "await ws.mail.watch()")
            await execute(second, "await ws.mail.watch()")
            server.add_message()
            notification = await _wait_for_notification(first, deadline_seconds=35)
            assert "mail" in notification
            second_notification = await _wait_for_notification(second, deadline_seconds=35)
            assert "mail" in second_notification
            page = ast.literal_eval(result_text(notification).strip())
            message_id = page["items"][0]["message_id"]
            read = await execute(first, f"await ws.mail.read({message_id!r})")
            assert "initial body" in result_text(read) or "body" in result_text(read)
            unread = await execute(first, "await ws.mail.search(unread=True)")
            assert message_id in result_text(unread)
            ids = ast.literal_eval(result_text(notification).strip())["items"]
            await execute(first, f"await ws.mail.ack([{ids[0]['id']!r}])")
            cleared = decode_result(await first.call_tool("execute", {"code": "1", "wait_ms": 0}))
            assert "mail" not in cleared
            still_pending = decode_result(
                await second.call_tool("execute", {"code": "1", "wait_ms": 0})
            )
            assert "mail" in still_pending


async def test_mail_draft_send_and_subscription_survive_kernel_reset(workspace, monkeypatch):
    monkeypatch.setenv("MYPR_TEST_MAIL_PASSWORD", "password")
    with local_mail_server() as server:
        _write_config(workspace, server)
        async with mcp_session(workspace, client_id="mail-reset") as session:
            await execute(session, "await ws.mail.watch()")
            draft = await execute(
                session,
                "draft = await ws.mail.draft(to=['dest@example.test'], "
                "subject='Persist', text='body')\n"
                "draft['id']",
            )
            draft_id = result_text(draft).strip(" '\n\"")
            send = await execute(
                session, f"await ws.mail.send({draft_id!r}, request_id='reset-send')"
            )
            assert "send-" in result_text(send)
            for _ in range(50):
                sent = await execute(session, "await ws.mail.sends()")
                if "accepted" in result_text(sent):
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("SMTP send did not reach accepted state")
            reset = await execute(session, "await ws.reset(force=True)", wait_ms=15000)
            assert reset["state"] == "succeeded"
            assert "watch-" in result_text(await execute(session, "await ws.mail.watches()"))
            assert draft_id in result_text(
                await execute(session, f"await ws.mail.get_draft({draft_id!r})")
            )
            assert "accepted" in result_text(await execute(session, "await ws.mail.sends()"))
        assert server.state.appended


async def test_mail_cache_is_available_after_manager_restart_when_provider_is_offline(
    workspace, monkeypatch
):
    monkeypatch.setenv("MYPR_TEST_MAIL_PASSWORD", "password")
    server = LocalMailServer()
    server.start()
    try:
        _write_config(workspace, server)
        async with mcp_session(workspace, client_id="mail-restart") as session:
            await execute(session, "await ws.mail.watch()")
            server.add_message()
            notification = await _wait_for_notification(session, deadline_seconds=35)
            assert "mail-" in result_text(notification)
        await stop_manager(workspace)
        server.stop()
        async with mcp_session(
            workspace, client_id="mail-restart", initialize_client=False
        ) as session:
            initialized = decode_result(
                await session.call_tool("init", {"client_id": "mail-restart"})
            )
            assert "mail" in initialized
            assert initialized["mail"]["items"]
            assert any(
                watch["state"] in {"offline", "error", "syncing", "pending"}
                for watch in initialized["mail"].get("watches", [])
            )
    finally:
        server.stop()


async def test_mail_config_reload_reconnects_changed_account(workspace, monkeypatch):
    monkeypatch.setenv("MYPR_TEST_MAIL_PASSWORD", "password")
    with local_mail_server() as first, local_mail_server() as second:
        _write_config(workspace, first)
        async with mcp_session(workspace, client_id="mail-reload") as session:
            before = await execute(session, "await ws.mail.status()")
            assert "local" in result_text(before)
            await execute(session, "await ws.mail.mailboxes()")
            _write_config(workspace, second)
            reloaded = await execute(session, "await ws.config.reload()")
            applied = ast.literal_eval(result_text(reloaded).strip())
            assert applied["errors"] == {}
            assert applied["applied"]["mail"] == ["local"]
            first.stop()
            after = await execute(session, "await ws.mail.mailboxes()")
            assert "local" in result_text(after)
