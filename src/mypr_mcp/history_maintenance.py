"""Small SQLite maintenance helpers used by workspace history."""

from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

VACUUM_MIN_DB_BYTES = 16 * 1024 * 1024
VACUUM_MIN_FREE_BYTES = 4 * 1024 * 1024
VACUUM_MIN_FREE_RATIO = 0.25
VACUUM_INTERVAL = 24 * 60 * 60


def vacuum_if_worthwhile(
    database: sqlite3.Connection, db_path: Path, **kwargs: Any
) -> dict[str, Any]:
    """Run one maintenance pass with a short lock wait."""
    previous = int(database.execute("PRAGMA busy_timeout").fetchone()[0])
    database.execute("PRAGMA busy_timeout=250")
    try:
        return _vacuum_if_worthwhile(database, db_path, **kwargs)
    finally:
        database.execute(f"PRAGMA busy_timeout={previous}")


def _vacuum_if_worthwhile(
    database: sqlite3.Connection,
    db_path: Path,
    *,
    last_vacuum: float | None,
    now: float | None = None,
    mail_migrated: Callable[[], bool] | None = None,
    save_last_vacuum: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Checkpoint and reclaim free SQLite pages when the database is large enough.

    The caller owns the transaction that produced the free pages and invokes
    this only after committing it.  VACUUM is deliberately skipped when the
    mail cursor migration has not completed, or when SQLite reports a busy
    database.
    """
    current = time.time() if now is None else float(now)
    if mail_migrated is not None and not mail_migrated():
        return {"attempted": False, "vacuumed": False, "reason": "mail_cursor_migration_pending"}
    try:
        checkpoint = database.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is not None and int(checkpoint[0]) != 0:
            return {
                "attempted": False,
                "vacuumed": False,
                "reason": "busy_or_unavailable:checkpoint",
                "checkpoint": list(checkpoint),
            }
        size = db_path.stat().st_size
        disk = os.statvfs(db_path.parent)
        available = int(disk.f_bavail) * int(disk.f_frsize)
        if last_vacuum is not None and current - float(last_vacuum) < VACUUM_INTERVAL:
            return {
                "attempted": False,
                "vacuumed": False,
                "reason": "rate_limited",
                "checkpoint": list(checkpoint) if checkpoint is not None else None,
                "filesystem_free_bytes": available,
            }
        if available < 2 * size:
            return {
                "attempted": False,
                "vacuumed": False,
                "reason": "insufficient_filesystem_space",
                "checkpoint": list(checkpoint) if checkpoint is not None else None,
                "filesystem_free_bytes": available,
            }
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        free_pages = int(database.execute("PRAGMA freelist_count").fetchone()[0])
    except (OSError, sqlite3.Error) as exc:
        return {
            "attempted": False,
            "vacuumed": False,
            "reason": f"busy_or_unavailable:{type(exc).__name__}",
        }
    free_bytes = free_pages * page_size
    if size < VACUUM_MIN_DB_BYTES:
        return {
            "attempted": False,
            "vacuumed": False,
            "reason": "database_too_small",
            "filesystem_free_bytes": available,
        }
    if (
        free_bytes < VACUUM_MIN_FREE_BYTES
        or not page_count
        or free_pages / page_count < VACUUM_MIN_FREE_RATIO
    ):
        return {
            "attempted": False,
            "vacuumed": False,
            "reason": "insufficient_free_space",
            "filesystem_free_bytes": available,
        }
    try:
        database.execute("VACUUM")
    except sqlite3.Error as exc:
        return {
            "attempted": True,
            "vacuumed": False,
            "reason": f"busy_or_unavailable:{type(exc).__name__}",
            "checkpoint": list(checkpoint) if checkpoint is not None else None,
            "filesystem_free_bytes": available,
        }
    if save_last_vacuum is not None:
        save_last_vacuum(current)
    try:
        post_checkpoint = database.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        post_checkpoint_result = (
            list(post_checkpoint) if post_checkpoint is not None else None
        )
        if post_checkpoint is not None and int(post_checkpoint[0]) != 0:
            return {
                "attempted": True,
                "vacuumed": True,
                "reason": "busy_or_unavailable:post_checkpoint",
                "checkpoint": list(checkpoint) if checkpoint is not None else None,
                "post_checkpoint": post_checkpoint_result,
                "reclaimed_bytes": 0,
                "filesystem_free_bytes": available,
            }
        size_after = db_path.stat().st_size
        disk_after = os.statvfs(db_path.parent)
        available_after = int(disk_after.f_bavail) * int(disk_after.f_frsize)
    except (OSError, sqlite3.Error) as exc:
        return {
            "attempted": True,
            "vacuumed": True,
            "reason": f"busy_or_unavailable:post_checkpoint:{type(exc).__name__}",
            "checkpoint": list(checkpoint) if checkpoint is not None else None,
            "reclaimed_bytes": 0,
            "filesystem_free_bytes": available,
        }
    reclaimed = max(0, size - size_after)
    return {
        "attempted": True,
        "vacuumed": True,
        "freelist_bytes": free_bytes,
        "checkpoint": list(checkpoint) if checkpoint is not None else None,
        "post_checkpoint": post_checkpoint_result,
        "vacuumed_at": current,
        "reclaimed_bytes": reclaimed,
        "filesystem_free_bytes": available_after,
    }
