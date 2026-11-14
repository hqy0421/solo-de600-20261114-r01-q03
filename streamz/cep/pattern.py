"""Pattern DSL: declarative description of the event sequences to match.

A :class:`Pattern` is an ordered list of :class:`Step` objects.  Each step
selects events with a predicate, may repeat a configurable number of times
(optionally consecutively), may be time-bounded, and -- only for the final
step -- may carry a timed :class:`Absence` condition ("no matching event
occurs within the next X seconds").

Example (search, then at least two views, then no consultation for 5
minutes)::

    Pattern("search_views_no_consultation").step(
        Step("search", event_is("search"))
    ).step(
        Step("views", event_is("view"), min_count=2, max_count=None)
    ).step(
        Step(
            "quiet",
            event_is("consult"),
            absence=Absence(window=300),
        )
    )
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence

from .types import Event

Predicate = Callable[[Event], bool]


@dataclass(frozen=True)
class Repeat:
    """Repetition policy for a step.

    Parameters
    ----------
    min_count, max_count:
        Inclusive bounds on how many events the step captures. ``max_count``
        of ``None`` means unbounded ("at least min_count"). ``min_count=0``
        makes the step optional.
    consecutive:
        If ``True``, captured events must be adjacent among the events
        relevant to the pattern (no event accepted by a later step in
        between). If ``False`` (default), other non-matching events may be
        skipped between repetitions.
    """

    min_count: int = 1
    max_count: Optional[int] = 1
    consecutive: bool = False

    def __post_init__(self):
        if self.min_count < 0:
            raise ValueError("min_count must be >= 0")
        if self.max_count is not None and self.max_count < self.min_count:
            raise ValueError("max_count must be >= min_count")

    @property
    def optional(self) -> bool:
        return self.min_count == 0

    @property
    def unbounded(self) -> bool:
        return self.max_count is None


@dataclass(frozen=True)
class Absence:
    """Timed negative condition attached to the final step of a pattern.

    The condition is satisfied when no event matching ``predicate`` occurs in
    the half-open interval ``(last_event_time, last_event_time + window]``.

    Parameters
    ----------
    window:
        Length of the observation interval in event-time seconds.
    predicate:
        Predicate selecting the forbidden events. Defaults to the predicate
        of the step carrying the absence.
    """

    window: float
    predicate: Optional[Predicate] = None

    def __post_init__(self):
        if self.window <= 0:
            raise ValueError("absence window must be > 0")


@dataclass(frozen=True)
class Step:
    """One stage of a :class:`Pattern`.

    Parameters
    ----------
    name:
        Step label; used as the key in ``Match.events_by_step``.
    predicate:
        Callable receiving an :class:`Event` and returning whether the event
        is accepted by this step.
    min_count, max_count:
        Repetition bounds, see :class:`Repeat`. ``max_count=None`` means
        unbounded.
    consecutive:
        Require repetitions to be consecutive, see :class:`Repeat`.
    within:
        Optional event-time budget in seconds, measured from the first event
        of the previous step (or pattern start for the first step). Events
        that would exceed the budget are not accepted for this step.
    absence:
        Optional terminal :class:`Absence` condition. Only permitted on the
        last step of a pattern; such a step captures no events itself and the
        match can only be finalized once the watermark passes the end of the
        absence window.
    """

    name: str
    predicate: Predicate
    min_count: int = 1
    max_count: Optional[int] = 1
    consecutive: bool = False
    within: Optional[float] = None
    absence: Optional[Absence] = None

    @property
    def repeat(self) -> Repeat:
        return Repeat(self.min_count, self.max_count, self.consecutive)

    def is_absence_step(self) -> bool:
        return self.absence is not None

    def __post_init__(self):
        if not callable(self.predicate):
            raise TypeError("step predicate must be callable")
        # Validate the repeat policy up front.
        Repeat(self.min_count, self.max_count, self.consecutive)
        if self.within is not None and self.within <= 0:
            raise ValueError("within must be > 0")


class Pattern:
    """A named, ordered sequence of :class:`Step` objects.

    Parameters
    ----------
    name:
        Pattern name carried by every resulting :class:`~streamz.cep.types.Match`.
    steps:
        Ordered steps. Usually built incrementally with :meth:`step`.
    within:
        Optional global event-time budget in seconds between the first and
        last contributing event.
    ttl:
        Maximum event-time lifetime of an unfinished candidate. Candidates
        that cannot progress past ``start_time + ttl`` are released (and
        reported through the ``expired`` callback of the engine/node). If
        ``None`` (default), candidates are bounded only by step-level
        ``within`` constraints and absence windows -- use an explicit TTL for
        patterns containing unbounded repetitions without a terminal absence
        so that stalled candidates cannot accumulate forever.
    """

    def __init__(
        self,
        name: str,
        steps: Optional[Sequence[Step]] = None,
        *,
        within: Optional[float] = None,
        ttl: Optional[float] = None,
    ):
        self.name = name
        self.steps: List[Step] = list(steps) if steps else []
        self.within = within
        self.ttl = ttl
        self._validate()

    # -- construction -----------------------------------------------------

    def step(self, step: Step) -> "Pattern":
        """Return a new pattern with ``step`` appended (DSL chaining)."""
        steps = self.steps + [step]
        return Pattern(self.name, steps, within=self.within, ttl=self.ttl)

    def with_within(self, seconds: float) -> "Pattern":
        return Pattern(self.name, self.steps, within=seconds, ttl=self.ttl)

    def with_ttl(self, seconds: float) -> "Pattern":
        return Pattern(self.name, self.steps, within=self.within, ttl=seconds)

    @classmethod
    def sequence(cls, name: str, *predicates: Predicate, **kwargs) -> "Pattern":
        """Convenience constructor: one single-event step per predicate."""
        steps = [
            Step(f"step_{i}", pred) for i, pred in enumerate(predicates)
        ]
        return cls(name, steps, **kwargs)

    # -- introspection ----------------------------------------------------

    @property
    def last_step(self) -> Optional[Step]:
        return self.steps[-1] if self.steps else None

    def has_absence(self) -> bool:
        return bool(self.steps and self.steps[-1].is_absence_step())

    def max_horizon(self) -> Optional[float]:
        """Worst-case event-time span a candidate may cover."""
        candidates = []
        if self.within is not None:
            candidates.append(self.within)
        if self.ttl is not None:
            candidates.append(self.ttl)
        if self.has_absence():
            candidates.append(self.steps[-1].absence.window)
        if not candidates:
            return None
        return max(candidates)

    def _validate(self):
        if not self.name:
            raise ValueError("pattern needs a name")
        if self.within is not None and self.within <= 0:
            raise ValueError("pattern within must be > 0")
        if self.ttl is not None and self.ttl <= 0:
            raise ValueError("pattern ttl must be > 0")
        if not self.steps:
            # Allowed while the pattern is still being built with step();
            # the engine rejects empty patterns when matching starts.
            return
        if self.steps[0].is_absence_step():
            raise ValueError(
                f"pattern {self.name!r} needs an event-capturing step "
                "before its absence condition"
            )
        names = [s.name for s in self.steps]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate step names in pattern {self.name!r}")
        for i, step in enumerate(self.steps):
            if step.is_absence_step() and i != len(self.steps) - 1:
                raise ValueError(
                    "absence conditions are only allowed on the final step"
                )
        if self.has_absence():
            last = self.steps[-1]
            if last.min_count != 0 or last.max_count != 0:
                raise ValueError(
                    "an absence step must capture no events: "
                    "use min_count=0, max_count=0"
                )

    def __repr__(self):  # pragma: no cover - cosmetic
        return f"<Pattern {self.name!r} steps={[s.name for s in self.steps]}>"


# ---------------------------------------------------------------------------
# Predicate helpers
# ---------------------------------------------------------------------------

def event_is(*event_types: Any) -> Predicate:
    """Predicate matching events whose type is one of ``event_types``."""
    allowed = set(event_types)

    def _predicate(event: Event) -> bool:
        return event.event_type in allowed

    _predicate.__name__ = "event_is(%s)" % ",".join(map(str, event_types))
    return _predicate


def event_in(event_types: Sequence[Any]) -> Predicate:
    """Predicate matching events whose type appears in ``event_types``."""
    return event_is(*event_types)


def field_equals(field: str, value: Any) -> Predicate:
    """Predicate comparing an event attribute, or a key of its ``data``.

    Top-level :class:`Event` attributes (``event_type``, ``timestamp``, ...)
    take precedence over payload keys.
    """

    def _predicate(event: Event) -> bool:
        if hasattr(event, field):
            return getattr(event, field) == value
        return event.data.get(field) == value

    _predicate.__name__ = f"field_equals({field!r}, {value!r})"
    return _predicate


def all_of(*predicates: Predicate) -> Predicate:
    """Logical AND of predicates."""

    def _predicate(event: Event) -> bool:
        return all(p(event) for p in predicates)

    return _predicate


def any_of(*predicates: Predicate) -> Predicate:
    """Logical OR of predicates."""

    def _predicate(event: Event) -> bool:
        return any(p(event) for p in predicates)

    return _predicate
