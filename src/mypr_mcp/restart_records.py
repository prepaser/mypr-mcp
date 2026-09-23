"""Durable execution results for cells that replace their own manager."""

import json
import os
import time
from pathlib import Path

from .history import History
from .journal import read_page


def finalize_origin(workspace, ticket):
    origin = ticket.get("origin") or {}
    ident = origin.get("exec_id")
    if not ident:
        return
    path = Path(workspace) / ".mypr" / "runs" / f"{ident}.json"
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError:
        return
    if record.get("restart_finalized") == ticket["id"]:
        return
    state = ticket["state"]
    if state not in {"succeeded", "failed"}:
        return
    error = ticket.get("error") if state == "failed" else None
    text = (
        f"Workspace restart completed: {ticket.get('new_version', 'unknown')} "
        f"(generation {ticket.get('new_generation', 'unknown')})"
        if state == "succeeded"
        else f"Workspace restart failed: {error}"
    )
    record.update(
        state=state,
        error=error,
        finished=time.time(),
        restart_id=ticket["id"],
        restart_finalized=ticket["id"],
        restart_result=text,
    )
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(record))
    os.replace(temporary, path)
    history = History(Path(workspace))
    try:
        history.record("execution", record, event=state)
    finally:
        history.close()


def poll_restart(workspace, exec_id, cursor=0):
    from .restart import read_ticket

    if (
        not isinstance(exec_id, str)
        or len(exec_id) != 32
        or any(char not in "0123456789abcdef" for char in exec_id)
    ):
        return None
    if type(cursor) is not int or cursor < 0:
        raise ValueError("Invalid output cursor")
    path = Path(workspace) / ".mypr" / "runs" / f"{exec_id}.json"
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    ident = record.get("restart_id")
    if not ident:
        return None
    ticket = read_ticket(workspace, ident)
    if not ticket or (ticket.get("origin") or {}).get("exec_id") != exec_id:
        return None
    state = ticket["state"]
    events, count = [], cursor
    if state in {"succeeded", "failed"}:
        # The coordinator does not write the manager's output journal.
        _, total = read_page(path.with_suffix(".jsonl"), 0, 0)
        if type(cursor) is not int or not 0 <= cursor <= total + 1:
            raise ValueError("Invalid output cursor")
        if cursor <= total:
            events, count = read_page(path.with_suffix(".jsonl"), cursor, 32768)
            if cursor + len(events) == total and record.get("restart_result"):
                final = {"type": "result", "text": record["restart_result"]}
                if (
                    not events
                    or len(json.dumps([*events, final], ensure_ascii=False).encode()) <= 32768
                ):
                    events.append(final)
        count = total + bool(record.get("restart_result"))
    return {
        "exec_id": exec_id,
        "client_id": record.get("client_id"),
        "connection_id": record.get("connection_id"),
        "generation": ticket.get("new_generation") or record["generation"],
        "execution_generation": record["generation"],
        "state": state if state in {"succeeded", "failed"} else "running",
        "output": events,
        "cursor": cursor + len(events),
        "has_more": cursor + len(events) < count,
        "truncated": record.get("truncated", False),
        "error": ticket.get("error") if state == "failed" else None,
        "restart": {
            key: ticket[key]
            for key in ("id", "state", "new_version", "new_generation")
            if key in ticket
        },
    }
