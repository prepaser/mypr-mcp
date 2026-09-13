import json
from pathlib import Path

import pytest

from mypr_mcp.history import History
from mypr_mcp.messages import MessageStore


@pytest.fixture
def store(tmp_path: Path):
    history = History(tmp_path)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.reserve_client_id("carol")
    messages = MessageStore(tmp_path)
    try:
        yield messages
    finally:
        messages.close()
        history.close()


def test_send_read_and_ack(store: MessageStore):
    sent = store.send("alice", "bob", "hello")
    assert sent["from"] == "alice"
    assert sent["to"] == "bob"
    assert sent["text"] == "hello"
    assert isinstance(sent["created_at"], float)

    assert store.read("alice")["messages"] == []
    page = store.read("bob")
    assert page["messages"] == [sent]
    assert page["next_cursor"] is None
    assert page["has_more"] is False
    assert store.ack("bob", [sent["id"]]) == 1
    assert store.ack("bob", [sent["id"]]) == 0
    assert store.read("bob")["messages"] == []


def test_paging_is_ascending_and_only_unacked(store: MessageStore):
    messages = [store.send("alice", "bob", str(index)) for index in range(3)]
    first = store.read("bob", limit=2)
    assert first["messages"] == messages[:2]
    assert first["has_more"] is True
    second = store.read("bob", after=first["next_cursor"])
    assert second["messages"] == [messages[2]]
    assert second["has_more"] is False
    store.ack("bob", [messages[0]["id"]])
    assert store.read("bob")["messages"] == messages[1:]


def test_send_requires_registered_clients_and_bounded_text(store: MessageStore):
    with pytest.raises(ValueError, match="unknown sender"):
        store.send("unknown", "bob", "hello")
    with pytest.raises(ValueError, match="unknown recipient"):
        store.send("alice", "unknown", "hello")
    with pytest.raises(ValueError, match="non-empty"):
        store.send("alice", "bob", "")
    with pytest.raises(ValueError, match="16 KiB"):
        store.send("alice", "bob", "가" * 5462)
    with pytest.raises(TypeError, match="string"):
        store.send("alice", "bob", 1)  # type: ignore[arg-type]


def test_ack_is_atomic_for_unknown_or_foreign_ids(store: MessageStore):
    own = store.send("alice", "bob", "own")
    foreign = store.send("alice", "carol", "foreign")
    with pytest.raises(ValueError, match="unknown"):
        store.ack("bob", [own["id"], 999999])
    assert store.read("bob")["messages"] == [own]
    with pytest.raises(ValueError, match="another client"):
        store.ack("bob", [own["id"], foreign["id"]])
    assert store.read("bob")["messages"] == [own]
    with pytest.raises(ValueError, match="positive"):
        store.ack("bob", [True])  # type: ignore[list-item]


def test_inbox_preview_is_bounded_and_does_not_ack(store: MessageStore):
    sent = [store.send("alice", "bob", "x" * 10000) for _ in range(6)]
    inbox = store.inbox("bob")
    assert inbox["unacked"] == 6
    assert len(inbox["messages"]) == 1
    assert inbox["messages"][0]["truncated"] is True
    assert len(json.dumps(inbox["messages"], ensure_ascii=False).encode()) <= 4096
    assert inbox["has_more"] is True
    assert store.read("bob")["messages"][0] == sent[0]


def test_unicode_and_persistence(tmp_path: Path):
    history = History(tmp_path)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    first = MessageStore(tmp_path)
    sent = first.send("alice", "bob", "안녕 👋")
    first.close()
    second = MessageStore(tmp_path)
    try:
        assert second.read("bob")["messages"] == [sent]
        assert second.ack("bob", [sent["id"]]) == 1
    finally:
        second.close()


def test_validation(store: MessageStore):
    with pytest.raises(ValueError, match="between 1 and 100"):
        store.read("bob", limit=0)
    with pytest.raises(ValueError, match="positive"):
        store.read("bob", after=0)
    with pytest.raises(TypeError, match="list"):
        store.ack("bob", (1,))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="closed"):
        store.close()
        store.inbox("bob")


def test_read_byte_budget_and_escaped_text_validation(store: MessageStore):
    sent = [store.send("alice", "bob", "가" * 5400) for _ in range(3)]
    page = store.read("bob")
    assert len(json.dumps(page, ensure_ascii=False).encode()) <= 32768
    assert page["messages"] == sent[:2]
    assert store.read("bob", after=page["next_cursor"])["messages"] == sent[2:]
    with pytest.raises(ValueError, match="too large"):
        store.send("alice", "bob", "\x00" * 16000)
    assert store.inbox("bob")["unacked"] == 3


def test_inbox_caps_message_count_and_preserves_unicode(store: MessageStore):
    for index in range(6):
        store.send("alice", "bob", str(index))
    inbox = store.inbox("bob")
    assert len(inbox["messages"]) == 5
    assert inbox["has_more"]
    text = "안녕 👋" * 1000
    message = store.send("bob", "alice", text)
    preview = store.inbox("alice")["messages"][0]
    assert preview["truncated"]
    assert text.startswith(preview["text"])
    assert len(json.dumps([preview], ensure_ascii=False).encode()) <= 4096
    assert store.read("alice")["messages"] == [message]
