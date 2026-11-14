"""streamz nodes wiring the CEP engine into a processing pipeline.

Two :class:`~streamz.core.Stream` subclasses are registered:

* :class:`watermark` -- turns raw events into timestamped
  :class:`~streamz.cep.types.Event` objects and injects
  :class:`~streamz.cep.watermark.Watermark` markers;
* :class:`pattern_match` -- runs one or more patterns and emits finalized
  :class:`~streamz.cep.types.Match` objects.

Typical pipeline::

    source = Stream()
    out = (
        source
        .watermark(delay=10)                       # tolerate 10s disorder
        .pattern_match([my_pattern], reuse_policy=NO_REUSE)
        .sink(print)
    )
    for raw in events:
        source.emit(raw)

The nodes accept either :class:`~streamz.cep.types.Event` instances or plain
dicts (field names configurable via the constructor arguments).
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence, Union

from tornado.ioloop import PeriodicCallback

from ..core import Stream
from .engine import CEPEngine, NO_REUSE, REUSE_EVENTS
from .pattern import Pattern
from .types import Event, Match
from .watermark import Watermark, WatermarkAssigner, is_watermark


def _coerce_event(
    x: Union[Event, dict],
    *,
    default_key: Any = None,
    type_field: str = "event_type",
    timestamp_field: str = "timestamp",
    id_field: Optional[str] = "event_id",
    key_field: Optional[str] = None,
) -> Event:
    if isinstance(x, Event):
        return x
    if isinstance(x, dict):
        return Event.from_mapping(
            x,
            key=default_key,
            type_field=type_field,
            timestamp_field=timestamp_field,
            id_field=id_field,
            key_field=key_field,
        )
    raise TypeError(
        "CEP nodes expect Event objects or dicts, got %r" % type(x).__name__
    )


@Stream.register_api()
class watermark(Stream):
    """Attach event-time watermarks to a behavioural event stream.

    Events pass through unchanged (coerced to
    :class:`~streamz.cep.types.Event`); :class:`Watermark` markers are
    injected whenever the watermark advances. Downstream
    :class:`pattern_match` nodes consume both.

    Parameters
    ----------
    delay:
        Bounded out-of-orderness delay in seconds (see
        :class:`WatermarkAssigner`).
    strategy:
        ``"bounded"`` (default) or ``"manual"``.
    idle_timeout:
        Event-time stall threshold after which the watermark creeps forward
        even without new events.
    period:
        Optional wall-clock period in seconds at which the idle check runs.
        Requires an event loop (one is created if needed).
    type_field, timestamp_field, id_field, key_field:
        Mapping used when input elements are plain dicts.
    default_key:
        Partition key used for events that do not carry one themselves.
    """

    _graphviz_shape = "octagon"

    def __init__(
        self,
        upstream,
        *,
        delay: float = 0.0,
        strategy: str = "bounded",
        idle_timeout: Optional[float] = None,
        period: Optional[float] = None,
        type_field: str = "event_type",
        timestamp_field: str = "timestamp",
        id_field: Optional[str] = "event_id",
        key_field: Optional[str] = None,
        default_key: Any = None,
        **kwargs,
    ):
        self.assigner = WatermarkAssigner(
            delay=delay,
            strategy=strategy,
            idle_timeout=idle_timeout,
        )
        self._coerce_kwargs = dict(
            default_key=default_key,
            type_field=type_field,
            timestamp_field=timestamp_field,
            id_field=id_field,
            key_field=key_field,
        )
        stream_name = kwargs.pop("stream_name", None)
        self._periodic = None
        if period is not None:
            kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, stream_name=stream_name, **kwargs)
        if period is not None:
            self._periodic = PeriodicCallback(
                self._idle_tick, period * 1000
            )
            self._periodic.start()

    def update(self, x, who=None, metadata=None):
        if is_watermark(x):
            return self._emit(x, metadata=metadata)
        event = _coerce_event(x, **self._coerce_kwargs)
        out = self._emit(event, metadata=metadata)
        advanced = self.assigner.observe(event)
        if advanced is not None:
            self._emit(Watermark(advanced), metadata=metadata)
        return out

    def set_watermark(self, timestamp: float) -> None:
        """Manually set a lower-bound watermark (manual strategy)."""
        advanced = self.assigner.set_watermark(timestamp)
        if advanced is not None:
            self._emit(Watermark(advanced))

    def advance_watermark(self, by: float) -> None:
        """Manually move the watermark forward by ``by`` seconds."""
        advanced = self.assigner.advance_watermark(by)
        if advanced is not None:
            self._emit(Watermark(advanced))

    def _idle_tick(self):
        advanced = self.assigner.on_idle()
        if advanced is not None:
            self._emit(Watermark(advanced))


@Stream.register_api()
class pattern_match(Stream):
    """Match configurable event-sequence patterns, emitting Match objects.

    All matching semantics live in :class:`~streamz.cep.engine.CEPEngine`;
    this node only adapts it to the streamz pull/push model. Matches are
    emitted downstream, never before the watermark confirms them, so the
    output is stable and complete under out-of-order arrival.

    Parameters
    ----------
    patterns:
        A :class:`~streamz.cep.pattern.Pattern` or a list of patterns.
    watermark_assigner:
        Used when the upstream does not inject :class:`Watermark` markers
        (e.g. when connecting directly to a plain source). Ignored for
        markers arriving from upstream.
    reuse_policy:
        :data:`~streamz.cep.engine.REUSE_EVENTS` (default) or
        :data:`~streamz.cep.engine.NO_REUSE`.
    on_late_event, on_expired:
        Optional side callbacks (see :class:`CEPEngine`). Late/expired
        material is never emitted as a match.
    type_field, timestamp_field, id_field, key_field, default_key:
        Mapping for plain-dict input elements.
    """

    _graphviz_shape = "diamond"

    def __init__(
        self,
        upstream,
        patterns: Union[Pattern, Sequence[Pattern]],
        *,
        watermark_assigner: Optional[WatermarkAssigner] = None,
        reuse_policy: str = REUSE_EVENTS,
        on_late_event: Optional[Callable[[Event], None]] = None,
        on_expired: Optional[
            Callable[[str, Any, List[Event]], None]
        ] = None,
        type_field: str = "event_type",
        timestamp_field: str = "timestamp",
        id_field: Optional[str] = "event_id",
        key_field: Optional[str] = None,
        default_key: Any = None,
        **kwargs,
    ):
        stream_name = kwargs.pop("stream_name", None)
        self._coerce_kwargs = dict(
            default_key=default_key,
            type_field=type_field,
            timestamp_field=timestamp_field,
            id_field=id_field,
            key_field=key_field,
        )
        self.engine = CEPEngine(
            patterns,
            watermark_assigner=watermark_assigner,
            reuse_policy=reuse_policy,
            on_late_event=on_late_event,
            on_expired=on_expired,
        )
        Stream.__init__(self, upstream, stream_name=stream_name, **kwargs)
        # If the direct upstream is a ``watermark`` node, its markers own
        # event time and the engine must never advance its own internal
        # assigner (which would pre-empt the configured disorder delay).
        if isinstance(upstream, watermark):
            self.engine.use_external_watermarks()

    def update(self, x, who=None, metadata=None):
        if is_watermark(x):
            # Markers from an upstream watermark node own event time.
            self.engine.use_external_watermarks()
            matches = self.engine.on_watermark(x.timestamp, external=True)
        else:
            event = _coerce_event(x, **self._coerce_kwargs)
            matches = self.engine.process(event)
        emitted = None
        for match in matches:
            emitted = self._emit(match, metadata=metadata)
        return emitted

    # -- pass-through helpers -------------------------------------------

    def flush(self) -> List[Match]:
        """Force emission of everything decidable at the current watermark."""
        matches = self.engine.flush()
        for match in matches:
            self._emit(match)
        return matches

    @property
    def active_keys(self):
        return self.engine.active_keys

    def candidate_count(self, key: Any) -> int:
        return self.engine.active_candidate_count(key)
