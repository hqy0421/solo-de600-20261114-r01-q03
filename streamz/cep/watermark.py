"""Event-time watermarks for out-of-order event streams.

A watermark is an assertion that *all events with timestamp ``t < w`` have
already arrived*. The CEP engine relies on watermarks to:

* decide negative conditions (an absence can only be ruled out once the
  watermark passes the end of its observation window);
* emit matches in a stable, event-time order;
* release expired candidates and buffered events.

Watermarks are created by :class:`WatermarkAssigner` either from observed
event timestamps (bounded-out-of-orderness heuristic) or explicitly via
:meth:`set_watermark` / :meth:`advance_watermark`.
"""
from __future__ import annotations

from typing import Any, Optional

from .types import Event


class WatermarkAssigner:
    """Compute a monotonically increasing watermark from an event stream.

    Parameters
    ----------
    delay:
        Bounded out-of-orderness delay in seconds. With the heuristic
        strategy the watermark is ``max_seen_timestamp - delay``. A larger
        delay tolerates more disorder but delays decisions further.
    strategy:
        ``"bounded"`` (default): derive the watermark from observed event
        times. ``"manual"``: never advance the watermark automatically; the
        caller drives it through :meth:`set_watermark` /
        :meth:`advance_watermark` (or the ``watermark`` stream node's
        ``advance_watermark`` method).
    idle_timeout:
        If no event arrives for this many event-time units of observed
        progress (i.e. the source stalls), the watermark keeps creeping
        forward so that pending matches are not blocked forever. ``None``
        disables idle progression.
    """

    def __init__(
        self,
        delay: float = 0.0,
        *,
        strategy: str = "bounded",
        idle_timeout: Optional[float] = None,
    ):
        if strategy not in ("bounded", "manual"):
            raise ValueError("strategy must be 'bounded' or 'manual'")
        if delay < 0:
            raise ValueError("delay must be >= 0")
        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle_timeout must be > 0 or None")
        self.delay = delay
        self.strategy = strategy
        self.idle_timeout = idle_timeout
        self._max_timestamp: Optional[float] = None
        self._watermark: Optional[float] = None

    @property
    def watermark(self) -> Optional[float]:
        return self._watermark

    def observe(self, event: Event) -> Optional[float]:
        """Fold an event in; return the new watermark if it advanced.

        Note that the returned watermark already accounts for ``delay``, so
        on the very first events it may be ``None`` or smaller than the
        event timestamp.
        """
        ts = event.timestamp
        if self._max_timestamp is None or ts > self._max_timestamp:
            self._max_timestamp = ts
        if self.strategy == "manual":
            return None
        return self._bump(ts - self.delay)

    def set_watermark(self, w: float) -> Optional[float]:
        """Manually set a lower-bound watermark; returns it if it advanced."""
        return self._bump(w)

    def advance_watermark(self, by: float) -> Optional[float]:
        """Manually move the watermark forward by ``by`` seconds."""
        base = self._watermark if self._watermark is not None else (
            self._max_timestamp - self.delay
            if self._max_timestamp is not None
            else None
        )
        if base is None:
            base = 0.0
        return self._bump(base + by)

    def on_idle(self) -> Optional[float]:
        """Advance the watermark past the last timestamp when the source idles.

        Called by the stream node's periodic timer (see :mod:`streamz.cep.nodes`)
        or by the user. Returns the new watermark when it advanced.
        """
        if self.idle_timeout is None:
            return None
        if self._max_timestamp is None:
            return None
        return self._bump(self._max_timestamp - self.delay)

    def _bump(self, candidate: float) -> Optional[float]:
        if self._watermark is None or candidate > self._watermark:
            self._watermark = candidate
            return candidate
        return None


class Watermark:
    """Internal marker object flowing between CEP stream nodes."""

    __slots__ = ("timestamp",)

    def __init__(self, timestamp: float):
        self.timestamp = timestamp

    def __repr__(self):  # pragma: no cover - cosmetic
        return f"<Watermark t={self.timestamp}>"


def is_watermark(x: Any) -> bool:
    return isinstance(x, Watermark)
