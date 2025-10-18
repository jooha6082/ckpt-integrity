#!/usr/bin/env python3
# macOS filesystem/I/O tracers: fs_usage, iostat (with graceful fallback)
from __future__ import annotations
import subprocess, os, signal, shutil
from pathlib import Path
from typing import Optional

class ProcHandle:
    def __init__(self, popen: Optional[subprocess.Popen], log_path: Path):
        self.popen = popen
        self.log_path = log_path

    def stop(self):
        # Stop only if a real process is running
        if self.popen and self.popen.poll() is None:
            try:
                self.popen.terminate()
            except Exception:
                pass
            try:
                self.popen.wait(timeout=3)
            except Exception:
                try:
                    self.popen.kill()
                except Exception:
                    pass

def _find_exec(candidates: list[str]) -> Optional[str]:
    """
    Return the first existing executable path among:
    - names to resolve via PATH (shutil.which)
    - absolute paths to check directly
    """
    for c in candidates:
        if os.path.sep in c:
            if os.path.exists(c):
                return c
        else:
            p = shutil.which(c)
            if p:
                return p
    return None

def spawn_fs_usage(log_path: str) -> ProcHandle:
    """
    Try to launch fs_usage. If not found, write a stub note and continue.
    """
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    exe = _find_exec(["fs_usage", "/usr/sbin/fs_usage", "/usr/bin/fs_usage"])
    f = open(log_path, "w")
    if not exe:
        f.write("# NOTE: fs_usage not found on this system; skipping FS tracing.\n")
        f.close()
        return ProcHandle(None, Path(log_path))
    pop = subprocess.Popen([exe, "-w", "-f", "filesys"],
                           stdout=f, stderr=subprocess.STDOUT, text=True)
    return ProcHandle(pop, Path(log_path))

def spawn_iostat(log_path: str, interval_sec: int = 1) -> ProcHandle:
    """
    Capture raw iostat output but write the 'header lines' only once at the top.
    Subsequent repeating headers are filtered out by a small pump thread.
    """
    from pathlib import Path
    import subprocess, sys, os
    import re, threading

    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    exe = _find_exec(["iostat", "/usr/sbin/iostat", "/usr/bin/iostat"])

    f = open(log_path, "w", buffering=1)

    if not exe:
        f.write("# NOTE: iostat not found on this system; skipping disk I/O stats.\n")
        f.close()
        return ProcHandle(None, Path(log_path))

    pop = subprocess.Popen(
        [exe, "-d", "-w", str(interval_sec)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    hdr_cols = re.compile(r'^\s*KB/t\s+tps\s+MB/s\b')
    hdr_disks = re.compile(r'^\s*disk[0-9]')  # starts with spaces then diskN

    seen_cols = False
    seen_disks = False

    def pump():
        nonlocal seen_cols, seen_disks
        try:
            for line in pop.stdout:
                s = line.rstrip("\r\n")

                # filter repeating headers
                if hdr_cols.match(s):
                    if not seen_cols:
                        f.write(line)
                        seen_cols = True
                    continue
                if hdr_disks.match(s):
                    if not seen_disks:
                        f.write(line)
                        seen_disks = True
                    continue

                # normal data line
                f.write(line)
        finally:
            try:
                f.flush()
                f.close()
            except Exception:
                pass

    t = threading.Thread(target=pump, daemon=True)
    t.start()

    return ProcHandle(pop, Path(log_path))



