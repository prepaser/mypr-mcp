"""Keep a process group alive only while its parent is alive."""

from __future__ import annotations

import ctypes
import fcntl
import os
import select
import signal
import sys
import termios
import time

_GRACE_SECONDS = 2.0
_POLL_SECONDS = 0.05
_terminating = False


def _enable_subreaper() -> None:
    if sys.platform != "linux":
        raise RuntimeError("process tree mode requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    # PR_SET_CHILD_SUBREAPER is deliberately done before the guarded fork.
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _proc_starttime(pid: int) -> int | None:
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii", errors="replace") as file:
            text = file.read()
        _, rest = text.rsplit(") ", 1)
        fields = rest.split()
        return int(fields[19])
    except OSError, ValueError, IndexError:
        return None


class _ProcessRef:
    __slots__ = ("pid", "starttime", "pidfd")

    def __init__(self, pid: int):
        self.pid = pid
        self.starttime = _proc_starttime(pid)
        self.pidfd: int | None = None
        try:
            self.pidfd = os.pidfd_open(pid)
        except AttributeError, OSError:
            pass

    def alive(self) -> bool:
        if self.starttime is None or _proc_starttime(self.pid) != self.starttime:
            return False
        if self.pidfd is None:
            return True
        poller = select.poll()
        poller.register(self.pidfd, select.POLLIN)
        return not poller.poll(0)

    def signal(self, signum: signal.Signals) -> None:
        if not self.alive():
            return
        try:
            sender = getattr(signal, "pidfd_send_signal", None)
            if sender is not None and self.pidfd is not None:
                sender(self.pidfd, signum)
            else:
                os.kill(self.pid, signum)
        except ProcessLookupError, PermissionError:
            pass

    def close(self) -> None:
        if self.pidfd is not None:
            os.close(self.pidfd)
            self.pidfd = None


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


def _session_groups(session_id: int, own_pid: int) -> set[int]:
    groups: set[int] = set()
    try:
        entries = os.scandir("/proc")
    except OSError:
        return groups
    with entries:
        for entry in entries:
            if not entry.name.isdecimal() or int(entry.name) == own_pid:
                continue
            try:
                with open(f"/proc/{entry.name}/stat", encoding="ascii", errors="replace") as file:
                    text = file.read()
                _, rest = text.rsplit(") ", 1)
                fields = rest.split()
                if fields[0] not in {"Z", "X"} and int(fields[3]) == session_id:
                    groups.add(int(fields[2]))
            except OSError, ValueError, IndexError:
                continue
    return groups


def _session_has_live_members(session_id: int, own_pid: int) -> bool:
    return bool(_session_groups(session_id, own_pid))


def _signal_session(session_id: int, own_pid: int, signum: signal.Signals) -> None:
    groups = sorted(
        _session_groups(session_id, own_pid), key=lambda group_id: group_id == session_id
    )
    for group_id in groups:
        _signal_group(group_id, signum)


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


def _watched_dead(pid: int, pidfd: int | None, starttime: int | None) -> bool:
    if pidfd is not None:
        readable, _, _ = select.select([pidfd], [], [], 0)
        return bool(readable)
    return starttime is None or _proc_starttime(pid) != starttime


def _proc_parents() -> dict[int, int]:
    parents: dict[int, int] = {}
    try:
        entries = os.scandir("/proc")
    except OSError:
        return parents
    with entries:
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                with open(f"/proc/{entry.name}/stat", encoding="ascii", errors="replace") as file:
                    _, rest = file.read().rsplit(") ", 1)
                fields = rest.split()
                parents[int(entry.name)] = int(fields[1])
            except OSError, ValueError, IndexError:
                continue
    return parents


def _discover_tree(root_pids: set[int], known: dict[int, _ProcessRef], own_pid: int) -> None:
    """Remember descendants while their parentage is still observable.

    The subreaper makes detached descendants children of this guard. Keeping
    the identity (starttime and, where available, pidfd) prevents a later PID
    reuse from turning cleanup into an unrelated process kill.
    """

    for pid, ref in tuple(known.items()):
        if not ref.alive():
            ref.close()
            known.pop(pid, None)
    parents = _proc_parents()
    children: dict[int, list[int]] = {}
    for pid, parent in parents.items():
        children.setdefault(parent, []).append(pid)
    queue = list(root_pids)
    seen = set(root_pids)
    while queue:
        parent = queue.pop()
        for pid in children.get(parent, ()):
            if pid in seen or pid == own_pid:
                continue
            seen.add(pid)
            queue.append(pid)
            if pid not in known:
                ref = _ProcessRef(pid)
                if ref.starttime is not None:
                    known[pid] = ref


def _tree_has_live(known: dict[int, _ProcessRef]) -> bool:
    return any(ref.alive() for ref in known.values())


def _stop_tree(
    known: dict[int, _ProcessRef],
    *,
    roots: set[int] | None = None,
    include_root: _ProcessRef | None = None,
) -> bool:
    if include_root is not None:
        known.setdefault(include_root.pid, include_root)
    if roots is not None:
        _discover_tree(roots, known, os.getpid())
    for ref in known.values():
        ref.signal(signal.SIGTERM)
    deadline = time.monotonic() + _GRACE_SECONDS
    while time.monotonic() < deadline:
        if roots is not None:
            _discover_tree(roots, known, os.getpid())
        if not _tree_has_live(known):
            return False
        time.sleep(_POLL_SECONDS)
    killed = False
    deadline = time.monotonic() + _GRACE_SECONDS
    while True:
        if roots is not None:
            _discover_tree(roots, known, os.getpid())
        live = [ref for ref in known.values() if ref.alive()]
        if not live:
            _reap_adopted()
            return killed
        for ref in live:
            killed = True
            ref.signal(signal.SIGKILL)
        _reap_adopted()
        if time.monotonic() >= deadline:
            return killed
        time.sleep(_POLL_SECONDS)


def _reap_adopted() -> None:
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError, OSError:
            return
        if pid == 0:
            return


def main(argv: list[str]) -> int:
    if len(argv) < 4 or argv[0] != "--parent-pid":
        return 2
    try:
        parent_pid = int(argv[1])
    except ValueError:
        return 2
    if parent_pid <= 1:
        return 2
    position = 2
    pty = False
    tree = False
    watch_pid: int | None = None
    while position < len(argv) and argv[position] != "--":
        option = argv[position]
        if option == "--pty":
            pty = True
            position += 1
        elif option == "--tree":
            tree = True
            position += 1
        elif option == "--watch-pid" and position + 1 < len(argv):
            try:
                watch_pid = int(argv[position + 1])
            except ValueError:
                return 2
            if watch_pid <= 1:
                return 2
            position += 2
        else:
            return 2
    if position >= len(argv) or argv[position] != "--" or position + 1 >= len(argv):
        return 2
    command = argv[position + 1 :]
    if os.getppid() != parent_pid or os.getpgrp() != os.getpid():
        return 1
    signal.signal(signal.SIGTERM, _mark_terminating)
    if pty:
        try:
            fcntl.ioctl(0, termios.TIOCSCTTY, 0)
        except OSError:
            return 1
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        signal.signal(signal.SIGTSTP, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
    else:
        signal.signal(signal.SIGINT, _mark_terminating)
    pidfd = _open_pidfd(parent_pid)
    watchfd = _open_pidfd(watch_pid) if watch_pid is not None else None
    watch_starttime = _proc_starttime(watch_pid) if watch_pid is not None else None
    try:
        if os.getppid() != parent_pid:
            return 1
        if tree:
            try:
                _enable_subreaper()
            except OSError, RuntimeError:
                return 1
        try:
            child = os.fork()
        except OSError:
            return 127
        if child == 0:
            try:
                if pty:
                    signal.signal(signal.SIGINT, signal.SIG_DFL)
                    signal.signal(signal.SIGQUIT, signal.SIG_DFL)
                    signal.signal(signal.SIGTSTP, signal.SIG_DFL)
                    signal.signal(signal.SIGHUP, signal.SIG_DFL)
                os.execvp(command[0], command)
            except OSError as exc:
                os.write(2, f"{exc}\n".encode("utf-8", "replace"))
                os._exit(127)

        _close_parent_fds()

        if tree:
            root = _ProcessRef(child)
            known = {child: root}
            try:
                while True:
                    finished, returncode = _child_status(child)
                    _discover_tree({os.getpid()}, known, os.getpid())
                    parent_gone = _parent_dead(parent_pid, pidfd)
                    watched_gone = watch_pid is not None and _watched_dead(
                        watch_pid, watchfd, watch_starttime
                    )
                    if _terminating or parent_gone or watched_gone or finished:
                        _discover_tree({os.getpid()}, known, os.getpid())
                        dead = _stop_tree(known, roots={os.getpid()})
                        _reap_adopted()
                        if dead:
                            return 137
                        return returncode
                    time.sleep(_POLL_SECONDS)
            finally:
                for ref in known.values():
                    ref.close()

        finished = False
        returncode = 1
        while True:
            if not finished:
                finished, returncode = _child_status(child)
            if _terminating or _parent_dead(parent_pid, pidfd):
                if pty:
                    _signal_session(os.getsid(0), os.getpid(), signal.SIGTERM)
                else:
                    _signal_group(os.getpgrp(), signal.SIGTERM)
                deadline = time.monotonic() + _GRACE_SECONDS
                while time.monotonic() < deadline and (
                    _session_has_live_members(os.getsid(0), os.getpid())
                    if pty
                    else _group_has_live_members(os.getpgrp(), os.getpid())
                ):
                    time.sleep(_POLL_SECONDS)
                live = (
                    _session_has_live_members(os.getsid(0), os.getpid())
                    if pty
                    else _group_has_live_members(os.getpgrp(), os.getpid())
                )
                if live:
                    if pty:
                        _signal_session(os.getsid(0), os.getpid(), signal.SIGKILL)
                    else:
                        _signal_group(os.getpgrp(), signal.SIGKILL)
                    return 137
                if not finished:
                    _, returncode = _child_status(child)
                return returncode
            live = (
                _session_has_live_members(os.getsid(0), os.getpid())
                if pty
                else _group_has_live_members(os.getpgrp(), os.getpid())
            )
            if finished and not live:
                return returncode
            time.sleep(_POLL_SECONDS)
    finally:
        if pidfd is not None:
            os.close(pidfd)
        if watchfd is not None:
            os.close(watchfd)


if __name__ == "__main__":
    code = main(sys.argv[1:])
    if code < 0:
        signum = -code
        if signum not in {signal.SIGKILL, signal.SIGSTOP}:
            signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)
    sys.exit(code)
