"""Millisecond timers for the measurement scripts.

On Windows, Python 3.12's event loop reads time.monotonic(), which ticks every 15.6 ms,
and the OS wakes a waiting thread on the same tick, so a 5 ms timeout fires after about
15 ms. run() uses a loop that reads time.perf_counter() and asks Windows for 1 ms timer
resolution while it runs. On Linux and macOS the default loop is already that precise,
and run() is plain asyncio.run()."""
import asyncio
import sys
import time
from collections.abc import Coroutine
from typing import Any


def run(main: Coroutine[Any, Any, Any]) -> Any:
    if sys.platform != "win32":
        return asyncio.run(main)

    import ctypes

    class PreciseLoop(asyncio.ProactorEventLoop):
        def __init__(self) -> None:
            super().__init__()
            # The loop runs timers that are due within one clock tick, so tell it the
            # tick is now the performance counter's, not time.monotonic()'s 15.6 ms.
            self._clock_resolution = time.get_clock_info("perf_counter").resolution

        def time(self) -> float:
            return time.perf_counter()

    winmm = ctypes.WinDLL("winmm")
    winmm.timeBeginPeriod(1)
    try:
        return asyncio.run(main, loop_factory=PreciseLoop)
    finally:
        winmm.timeEndPeriod(1)
