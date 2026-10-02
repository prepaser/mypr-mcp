from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from mypr_mcp.storage import Storage


def old(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    timestamp = time.time() - 40 * 24 * 60 * 60
    os.utime(path, (timestamp, timestamp))


class MailGC:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.before: list[dict] = []
        self.after: list[dict] = []

    def storage_gc_snapshot(self):
        return self.snapshot

    def storage_gc_before_delete(self, candidates):
        self.before.extend(candidates)
        return [item["path"] for item in candidates]

    def storage_gc_after_delete(self, candidates):
        self.after.extend(candidates)


@pytest.mark.asyncio
async def test_mail_usage_is_reported_as_its_own_category(tmp_path: Path):
    old(tmp_path / ".mypr" / "mail" / "drafts" / "draft.eml", "draft")
    old(tmp_path / ".mypr" / "mail" / "outbox" / "queued.eml", "queued")

    result = await Storage(tmp_path).usage()

    assert result["categories"]["mail"] == {"files": 2, "bytes": 11}


@pytest.mark.asyncio
async def test_mail_snapshot_controls_candidates_and_protected_paths(tmp_path: Path):
    protected = tmp_path / ".mypr" / "mail" / "drafts" / "unsent.eml"
    queued = tmp_path / ".mypr" / "mail" / "outbox" / "queued.eml"
    accepted = tmp_path / ".mypr" / "mail" / "sent" / "accepted.eml"
    for path in (protected, queued, accepted):
        old(path, path.name)
    adapter = MailGC(
        {
            "protected_paths": [
                ".mypr/mail/drafts/unsent.eml",
                ".mypr/mail/outbox/queued.eml",
            ],
            "candidates": [
                {
                    "path": ".mypr/mail/sent/accepted.eml",
                    "reason": "acked_notification",
                    "group": "send:1",
                }
            ],
        }
    )

    plan = await Storage(tmp_path, mail=adapter).gc(max_bytes=0)

    assert [item["path"] for item in plan["candidates"]] == [".mypr/mail/sent/accepted.eml"]
    assert ".mypr/mail/drafts/unsent.eml" in plan["protected"]["paths"]
    assert ".mypr/mail/outbox/queued.eml" in plan["protected"]["paths"]
    assert plan["mail"]["candidates"][0]["reason"] == "acked_notification"

    await Storage(tmp_path, mail=adapter).gc(dry_run=False, max_bytes=0)

    assert not accepted.exists()
    assert protected.exists()
    assert queued.exists()
    assert adapter.after and adapter.after[0]["path"] == ".mypr/mail/sent/accepted.eml"


@pytest.mark.asyncio
async def test_mail_snapshot_error_protects_all_mail_files(tmp_path: Path):
    draft = tmp_path / ".mypr" / "mail" / "drafts" / "draft.eml"
    old(draft, "draft")

    class BrokenMail:
        def storage_gc_snapshot(self):
            raise RuntimeError("persistence unavailable")

    plan = await Storage(tmp_path, mail=BrokenMail()).gc(max_bytes=0)

    assert plan["mail"]["error"] == "mail snapshot failed: RuntimeError"
    assert ".mypr/mail/drafts/draft.eml" in plan["protected"]["paths"]
    assert plan["candidates"] == []


@pytest.mark.asyncio
async def test_mail_candidates_are_grouped_for_atomic_quota_selection(tmp_path: Path):
    first = tmp_path / ".mypr" / "mail" / "sent" / "first.eml"
    second = tmp_path / ".mypr" / "mail" / "sent" / "first.json"
    other = tmp_path / ".mypr" / "mail" / "sent" / "other.eml"
    for path in (first, second, other):
        old(path, "12345")
    now = time.time()
    os.utime(first, (now - 10, now - 10))
    os.utime(second, (now - 10, now - 10))
    os.utime(other, (now - 5, now - 5))
    adapter = MailGC(
        {
            "candidates": [
                {"path": ".mypr/mail/sent/first.eml", "group": "send:first"},
                {"path": ".mypr/mail/sent/first.json", "group": "send:first"},
                {"path": ".mypr/mail/sent/other.eml", "group": "send:other"},
            ]
        }
    )

    plan = await Storage(tmp_path, mail=adapter).gc(max_bytes=10)

    assert {item["path"] for item in plan["candidates"]} == {
        ".mypr/mail/sent/first.eml",
        ".mypr/mail/sent/first.json",
    }


@pytest.mark.asyncio
async def test_invalid_mail_snapshot_never_deletes_mail_files(tmp_path: Path):
    path = tmp_path / ".mypr" / "mail" / "sent" / "accepted.eml"
    old(path)
    adapter = MailGC(
        {
            "candidates": [
                {"path": "../outside.eml"},
                {"path": ".mypr/mail/sent/accepted.eml", "requires_tombstone": "yes"},
            ]
        }
    )

    plan = await Storage(tmp_path, mail=adapter).gc(max_bytes=0)

    assert plan["mail"]["error"]
    assert plan["candidates"] == []
    assert path.exists()


@pytest.mark.asyncio
async def test_mail_tombstone_requires_mail_callback(tmp_path: Path):
    path = tmp_path / ".mypr" / "mail" / "sent" / "accepted.eml"
    old(path)

    class MailWithoutMarker:
        def storage_gc_snapshot(self):
            return {
                "candidates": [
                    {
                        "path": ".mypr/mail/sent/accepted.eml",
                        "requires_tombstone": True,
                    }
                ]
            }

    storage = Storage(tmp_path, mail=MailWithoutMarker())
    plan = await storage.gc(max_bytes=0)
    result = await storage.gc_apply(plan["plan_id"])

    assert path.exists()
    assert result["skipped"][0]["reason"] == "tombstone_required"
