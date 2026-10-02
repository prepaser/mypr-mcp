from __future__ import annotations

import pytest
from test_timer_restart import _restart_fixture

from mypr_mcp.bridge import ConnectionBridge
from mypr_mcp.history import History
from mypr_mcp.mail_store import MailStore


@pytest.mark.parametrize("initialized", [False, True])
async def test_restart_fallback_returns_only_owned_cached_mail(tmp_path, monkeypatch, initialized):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    history = History(workspace)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    store = MailStore(workspace)
    watch = store.create_watch("alice", "work", "INBOX", uidvalidity=41)
    notice = store.add_notification(
        client_id="alice",
        watch_id=watch["id"],
        account="work",
        mailbox="INBOX",
        uidvalidity=41,
        uid=1,
        kind="new_mail",
        payload={"message_id": "ref", "subject": "cached"},
    )
    store.add_notification(
        client_id="bob",
        watch_id="bob-watch",
        account="work",
        mailbox="INBOX",
        uidvalidity=41,
        uid=2,
        kind="new_mail",
        payload={"message_id": "other"},
    )
    store.close()
    exec_id, _ = _restart_fixture(workspace)
    bridge = ConnectionBridge(workspace)
    bridge.client_id = "alice" if initialized else None

    async def no_network(*args):
        return None

    monkeypatch.setattr(bridge, "_recover_restart", no_network)
    result = await bridge.request("poll", exec_id=exec_id, wait_ms=0)
    if initialized:
        assert [item["id"] for item in result["mail"]["items"]] == [notice["id"]]
        assert result["mail"]["watches"][0]["state"] == "offline"
    else:
        assert "mail" not in result
