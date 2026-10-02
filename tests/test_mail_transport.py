from __future__ import annotations

import contextlib
import socketserver
import threading

import pytest

from mypr_mcp.mail_transport import (
    MailTransport,
    MailTransportError,
    _bounded_imap_call,
    _mailbox_display,
    _mailbox_wire,
)


class FakeIMAP:
    capabilities = (b"IMAP4rev1", b"UIDPLUS")

    def __init__(self, *, size=10, body=b"Subject: test\r\n\r\nbody"):
        self.size = size
        self.body = body
        self.commands = []
        self._responses = {"UIDVALIDITY": [b"41"], "UIDNEXT": [b"9"]}

    def examine(self, mailbox):
        self.commands.append(("EXAMINE", mailbox))
        return "OK", [b"8"]

    def select(self, mailbox, readonly=False):
        self.commands.append(("SELECT", mailbox, readonly))
        return "OK", [b"8"]

    def response(self, key):
        return key, self._responses.get(key)

    def uid(self, command, *args):
        self.commands.append((command, *args))
        if command == "SEARCH":
            return "OK", [b"1 3 7 9"]
        if command == "FETCH" and args[-1] == "(RFC822.SIZE)":
            return "OK", [(b"1 FETCH (RFC822.SIZE 10)", None)]
        if command == "FETCH":
            return "OK", [(b"1 FETCH", self.body)]
        return "OK", [b""]


def _transport(fake):
    transport = MailTransport(
        {"accounts": {"test": {"imap": {"host": "example", "security": "plain"}}}}
    )
    transport._get_imap = lambda name, account: (fake, fake_lock)  # type: ignore[method-assign]
    return transport


fake_lock = threading.RLock()


class _SMTPHandler(socketserver.StreamRequestHandler):
    mode = "normal"

    def handle(self):
        self.wfile.write(b"220 local test\r\n")
        greeted = False
        while True:
            line = self.rfile.readline()
            if not line:
                return
            upper = line.upper()
            if upper.startswith((b"EHLO", b"HELO")):
                greeted = True
                self.wfile.write(b"250-local\r\n250 SIZE 26214400\r\n")
            elif upper.startswith(b"MAIL FROM:"):
                self.wfile.write(b"250 sender\r\n" if greeted else b"503 EHLO required\r\n")
            elif upper.startswith(b"RCPT TO:"):
                self.wfile.write(
                    b"550 rejected\r\n" if b"REJECT" in upper else b"250 recipient\r\n"
                )
            elif upper == b"DATA\r\n":
                self.wfile.write(b"354 data\r\n")
                while self.rfile.readline() not in (b"", b".\r\n"):
                    pass
                if self.mode == "eof":
                    return
                self.wfile.write(
                    b"550 data rejected\r\n" if self.mode == "reject" else b"250 accepted\r\n"
                )
            elif upper == b"QUIT\r\n":
                self.wfile.write(b"221 bye\r\n")
                return
            else:
                self.wfile.write(b"250 ok\r\n")


class _SMTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


@contextlib.contextmanager
def _smtp_server(mode="normal"):
    handler = type("Handler", (_SMTPHandler,), {"mode": mode})
    server = _SMTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _smtp_transport(port):
    return MailTransport(
        {
            "accounts": {
                "test": {
                    "from": "sender@example.test",
                    "smtp": {"host": "127.0.0.1", "port": port, "security": "plain"},
                }
            }
        }
    )


def test_mailbox_names_use_modified_utf7():
    value = "Sent & 日本語"
    assert _mailbox_display(_mailbox_wire(value)) == value


def test_search_uses_examine_and_bounded_uid_pages():
    fake = FakeIMAP()
    result = _transport(fake).search(
        "test", "日本語", sender="a b", since="2026-10-02", after_uid=1, limit=2
    )
    assert result["uids"] == [3, 7]
    assert result["has_more"] is True
    assert fake.commands[0] == ("EXAMINE", "&ZeVnLIqe-")
    assert fake.commands[1][0] == "SEARCH"


def test_unicode_search_uses_utf8_charset_and_bytes():
    fake = FakeIMAP()
    _transport(fake).search("test", sender="日本語")
    search = next(command for command in fake.commands if command[0] == "SEARCH")
    assert search[1] == "UTF-8"
    assert any(isinstance(value, bytes) and "日本語".encode() in value for value in search)


def test_mailbox_arguments_are_quoted_when_needed():
    fake = FakeIMAP()
    _transport(fake).namespace("test", "Sent & Stuff")
    assert fake.commands[0] == ("EXAMINE", '"Sent &- Stuff"')


def test_fetch_checks_namespace_before_returning_body():
    fake = FakeIMAP()
    with pytest.raises(MailTransportError, match="outdated"):
        _transport(fake).fetch("test", "INBOX", 99, 1)
    assert not any(command[0] == "FETCH" for command in fake.commands)


def test_fetch_rejects_declared_oversize_before_literal_fetch():
    fake = FakeIMAP(size=30 * 1024 * 1024)
    fake.uid = lambda command, *args: (
        ("OK", [(b"1 FETCH (RFC822.SIZE 31457280)", None)]) if command == "FETCH" else ("OK", [b""])
    )
    with pytest.raises(MailTransportError, match="size limit"):
        _transport(fake).fetch("test", "INBOX", 41, 1)


def test_fetch_can_request_a_bounded_partial_literal():
    fake = FakeIMAP(body=b"abc")
    _transport(fake).fetch("test", "INBOX", 41, 1, offset=2, request_size=3)
    assert any(command[0] == "FETCH" and "<2.3>" in command[-1] for command in fake.commands)


def test_bounded_literal_cap_is_total_and_interrupts_connection():
    class Reader:
        def __init__(self):
            self.closed = False

        def read(self, size):
            return b"x" * size

        def close(self):
            self.closed = True

    reader = Reader()

    with pytest.raises(MailTransportError, match="literal"):
        _bounded_imap_call(reader, 5, lambda: (reader.read(3), reader.read(3)))
    assert reader.closed is True


def test_set_seen_requires_current_uidvalidity():
    fake = FakeIMAP()
    with pytest.raises(MailTransportError, match="outdated"):
        _transport(fake).set_seen("test", "INBOX", [1], True, uidvalidity=42)
    assert not any(command[0] == "STORE" for command in fake.commands)


def test_smtp_reports_partial_rcpt_acceptance():
    with _smtp_server() as port:
        transport = _smtp_transport(port)
        try:
            result = transport.send(
                "test",
                b"Subject: test\r\n\r\nbody\r\n",
                ["good@example.test", "reject@example.test"],
            )
        finally:
            transport.close()
    assert result["accepted"] == ["good@example.test"]
    assert result["rejected"] == ["reject@example.test"]
    assert result["rejected_details"][0]["code"] == 550


def test_smtp_eof_after_data_is_ambiguous():
    with _smtp_server("eof") as port:
        transport = _smtp_transport(port)
        try:
            with pytest.raises(MailTransportError) as caught:
                transport.send("test", b"Subject: test\r\n\r\nbody\r\n", ["good@example.test"])
        finally:
            transport.close()
    assert caught.value.ambiguous is True
    assert caught.value.details["outcome_unknown"] is True


def test_smtp_data_rejection_is_known_failure():
    with _smtp_server("reject") as port:
        transport = _smtp_transport(port)
        try:
            result = transport.send("test", b"Subject: test\r\n\r\nbody\r\n", ["good@example.test"])
        finally:
            transport.close()
    assert result["accepted"] == []
    assert result["rejected"] == ["good@example.test"]
    assert result["stage"] == "data"


def test_imap_socket_cleanup_never_issues_close_or_expunge():
    from mypr_mcp.mail_transport import _close_quietly

    class IMAP:
        def __init__(self):
            self.stopped = False

        def shutdown(self):
            self.stopped = True

        def close(self):
            raise AssertionError("CLOSE would expunge unrelated messages")

    conn = IMAP()
    _close_quietly(conn)
    assert conn.stopped


def test_reconfiguring_other_account_does_not_abort_connecting_smtp(monkeypatch):
    import copy
    from concurrent.futures import ThreadPoolExecutor

    import mypr_mcp.mail_transport as module

    entered, release = threading.Event(), threading.Event()

    class SMTP:
        def close(self):
            pass

        def quit(self):
            pass

    connection = SMTP()

    def connect(*args):
        entered.set()
        assert release.wait(5)
        return connection

    monkeypatch.setattr(module, "_connect_smtp", connect)
    config = {
        "accounts": {"first": {"smtp": {"host": "first"}}, "other": {"smtp": {"host": "old"}}}
    }
    transport = MailTransport(config)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(transport._get_smtp, "first", config["accounts"]["first"])
            assert entered.wait(2)
            changed = copy.deepcopy(config)
            changed["accounts"]["other"]["smtp"]["host"] = "new"
            transport.reconfigure(changed)
            release.set()
            assert result.result(timeout=2)[0] is connection
    finally:
        release.set()
        transport.close()
