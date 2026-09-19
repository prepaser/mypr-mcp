from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time

import pytest


@pytest.mark.skipif(sys.platform != "linux", reason="tree guard is Linux-only")
def test_tree_guard_reaps_double_fork_after_main_exit():
    command = (
        "import os,signal,sys,time; "
        "os.fork() or (signal.signal(signal.SIGTERM,signal.SIG_IGN), "
        "print(os.getpid(),flush=True), time.sleep(30)); sys.exit(0)"
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "src/mypr_mcp/process_guard.py",
            "--parent-pid",
            str(os.getpid()),
            "--tree",
            "--",
            sys.executable,
            "-c",
            command,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert process.stdout is not None
    try:
        descendant = int(process.stdout.readline())
        assert process.wait(timeout=5) == 137
        for _ in range(150):
            try:
                os.kill(descendant, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            pytest.fail("detached descendant survived tree guard")
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(descendant, signal.SIGKILL)
