"""Freeze and thaw a whole process, like `kill -STOP` / `docker pause`.

POSIX: SIGSTOP / SIGCONT. Windows: NtSuspendProcess / NtResumeProcess (all threads).
Test helper only.
"""
import ctypes
import os
import signal

PROCESS_SUSPEND_RESUME = 0x0800


def _nt(function: str, pid: int) -> None:
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
    if not handle:
        raise ctypes.WinError()
    try:
        status = getattr(ctypes.windll.ntdll, function)(handle)
        if status != 0:
            raise OSError(f"{function} failed with NTSTATUS {status:#x}")
    finally:
        kernel32.CloseHandle(handle)


def freeze(pid: int) -> None:
    if os.name == "nt":
        _nt("NtSuspendProcess", pid)
    else:
        os.kill(pid, signal.SIGSTOP)


def thaw(pid: int) -> None:
    if os.name == "nt":
        _nt("NtResumeProcess", pid)
    else:
        os.kill(pid, signal.SIGCONT)
