"""
Complex event processing (CEP) for streamz.

Configurable event-sequence matching over behavioural streams with:

* ordered sequences of typed events with arbitrary predicates;
* repetition counts (exact / at-least / between, contiguous or not);
* time constraints (within a window, gaps between events);
* timed absence ("negation") conditions that can only be decided once the
  watermark passes the end of the observation window;
* multiple concurrent candidate paths per key (user/session);
* selectable event reuse between emitted matches;
* out-of-order tolerance via event-time watermarks, so output is stable and
  complete regardless of arrival order;
* deterministic release of expired candidates and buffered events.

See :mod:`streamz.cep.pattern` for the pattern DSL and
:mod:`streamz.cep.nodes` for the streamz integration.
"""
from .types import Event, Match
from .pattern import (
    Pattern,
    Step,
    Repeat,
    Absence,
    event_is,
    event_in,
    field_equals,
    all_of,
    any_of,
)
from .engine import CEPEngine, REUSE_EVENTS, NO_REUSE
from .watermark import WatermarkAssigner
from .nodes import pattern_match, watermark

__all__ = [
    "Event",
    "Match",
    "Pattern",
    "Step",
    "Repeat",
    "Absence",
    "event_is",
    "event_in",
    "field_equals",
    "all_of",
    "any_of",
    "CEPEngine",
    "REUSE_EVENTS",
    "NO_REUSE",
    "WatermarkAssigner",
    "pattern_match",
    "watermark",
]
