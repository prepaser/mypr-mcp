"""Keep a process group alive only while its parent is alive."""

from __future__ import annotations

import os
import select
import signal
import sys
import time

_GRACE_SECONDS = 2.0
_POLL_SECONDS = 0.05
_terminating = False


def _mark_terminating(_signum: int, _frame: object) -> None:
    global _terminating
    _terminating = True


def _group_has_live_members(group_id: int, own_pid: int) -> bool:
    try:
        entries = os.scandir("/proc")
    except OSError:
        try:
            os.killpg(group_id, 0)
        except PermissionError, ProcessLookupError:
            return False
        return True
    with entries:
        for entry in entries:
            if not entry.name.isdecimal() or int(entry.name) == own_pid:
                continue
            try:
                with open(f"/proc/{entry.name}/stat", encoding="ascii", errors="replace") as file:
                    text = file.read()
                _, rest = text.rsplit(") ", 1)
                fields = rest.split()
                if fields[0] not in {"Z", "X"} and fields[2] == str(group_id):
                    return True
            except OSError, ValueError, IndexError:
                continue
    return False


def _signal_group(group_id: int, signum: signal.Signals) -> None:
    try:
        os.killpg(group_id, signum)
    except ProcessLookupError:
        pass


def _child_status(pid: int) -> tuple[bool, int]:
    result, status = os.waitpid(pid, os.WNOHANG)
    if result == 0:
        return False, 0
    if os.WIFEXITED(status):
        return True, os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return True, -os.WTERMSIG(status)
    return True, 1


def _close_parent_fds() -> None:
    for fd in (0, 1, 2):
        try:
            os.close(fd)
        except OSError:
            pass


def _parent_dead(parent_pid: int, pidfd: int | None) -> bool:
    if pidfd is not None:
        readable, _, _ = select.select([pidfd], [], [], 0)
        return bool(readable)
    return os.getppid() != parent_pid


def _open_pidfd(parent_pid: int) -> int | None:
    try:
        return os.pidfd_open(parent_pid)
    except AttributeError, OSError:
        return None


def main(argv: list[str]) -> int:
    if len(argv) < 4 or argv[0] != "--parent-pid" or argv[2] != "--":
        return 2
    try:
        parent_pid = int(argv[1])
    except ValueError:
        return 2
    if parent_pid <= 1 or len(argv) == 3:
        return 2
    command = argv[3:]
    if os.getppid() != parent_pid or os.getpgrp() != os.getpid():
        return 1
    signal.signal(signal.SIGTERM, _mark_terminating)
    signal.signal(signal.SIGINT, _mark_terminating)
    pidfd = _open_pidfd(parent_pid)
    try:
        if os.getppid() != parent_pid:
            return 1
        try:
            child = os.fork()
        except OSError:
            return 127
        if child == 0:
            try:
                os.execvp(command[0], command)
            except OSError as exc:
                os.write(2, f"{exc}\n".encode("utf-8", "replace"))
                os._exit(127)

        _close_parent_fds()

        finished = False
        returncode = 1
        while True:
            if not finished:
                finished, returncode = _child_status(child)
            if _terminating or _parent_dead(parent_pid, pidfd):
                _signal_group(os.getpgrp(), signal.SIGTERM)
                deadline = time.monotonic() + _GRACE_SECONDS
                while time.monotonic() < deadline and _group_has_live_members(
                    os.getpgrp(), os.getpid()
                ):
                    time.sleep(_POLL_SECONDS)
                if _group_has_live_members(os.getpgrp(), os.getpid()):
                    _signal_group(os.getpgrp(), signal.SIGKILL)
                    return 137
                if not finished:
                    _, returncode = _child_status(child)
                return returncode
            if finished and not _group_has_live_members(os.getpgrp(), os.getpid()):
                return returncode
            time.sleep(_POLL_SECONDS)
    finally:
        if pidfd is not None:
            os.close(pidfd)


if __name__ == "__main__":
    code = main(sys.argv[1:])
    if code < 0:
        signum = -code
        if signum not in {signal.SIGKILL, signal.SIGSTOP}:
            signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)
    sys.exit(code)
