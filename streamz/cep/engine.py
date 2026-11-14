"""NFA-based CEP matching engine.

The engine is streamz-agnostic: it consumes :class:`~streamz.cep.types.Event`
objects and watermark updates and produces :class:`~streamz.cep.types.Match`
objects through callbacks. The stream nodes in :mod:`streamz.cep.nodes` are
thin wrappers around it, which keeps the matching logic testable without an
event loop.

Semantics
---------
* State is kept independently per partition key (one user/session each).
* Every pattern keeps its own set of *candidate* partial matches per key, so
  multiple candidate paths coexist.
* A candidate is a run through the pattern steps. Each new accepted event
  extends (and, for repetitions, branches) candidates; new candidates start
  whenever an event matches the first step.
* Repetition: ``min_count``/``max_count`` bound how many events a step
  captures (``max_count=None`` = unbounded). Non-consecutive repeats may skip
  non-matching events; consecutive repeats require adjacency.
* Optional steps (``min_count=0``) are resolved through epsilon transitions.
* A terminal :class:`~streamz.cep.pattern.Absence` captures no events. The
  candidate is parked with a deadline (last event time + absence window) and
  can only be decided when the watermark reaches that deadline.
* Nothing is emitted before the watermark confirms it. Out-of-order events
  therefore produce exactly the same matches, in the same order, as in-order
  events. Late events (timestamp below the current watermark) are dropped and
  reported through ``on_late_event``.
* Expired candidates (pattern TTL / exceeded ``within`` budget) are released
  and reported through ``on_expired``. Buffered events no longer needed by any
  candidate are pruned.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .pattern import Absence, Pattern, Step
from .types import Event, Match
from .watermark import WatermarkAssigner

# Event-reuse policies -------------------------------------------------------

#: Events may be shared by several emitted matches (default).
REUSE_EVENTS = "reuse_events"

#: An event consumed by an emitted match cannot be used by later matches.
NO_REUSE = "no_reuse"


@dataclass
class _Candidate:
    """A partial pattern match (one NFA run)."""

    pattern: Pattern
    key: Any
    # Step index the candidate is currently filling.
    step_index: int
    # Events captured per step index, in event-time/arrival order.
    captured: List[List[Event]]
    # All captured events flattened (same order as ``captured``).
    path: List[Event]
    # Whether the run has parked at the terminal absence step awaiting wm.
    awaiting_absence: bool = False
    # Event-time deadline for the absence (only when awaiting_absence).
    absence_deadline: Optional[float] = None
    # Event-time after which the run must be considered expired (TTL/within).
    expiry: Optional[float] = None
    # Per-step ids of events already evaluated by this run. The set is scoped
    # to a step: advancing to the next step starts with an empty set, so an
    # event ignored while filling an earlier step can be consumed by a later
    # step once the run reaches it (this is what makes out-of-order replay
    # equivalent to in-order processing).
    step_seen: List[set] = field(default_factory=list)

    def consumed_ids(self) -> set:
        """Ids of events actually taken (consumed) by this run."""
        return {e.event_id for e in self.path}

    def key_tuple(self) -> Tuple:
        """Deduplication key: two runs with the same key tuple are identical."""
        return (
            self.step_index,
            tuple(tuple(e.event_id for e in evs) for evs in self.captured),
            self.awaiting_absence,
        )

    def start_time(self) -> float:
        return self.path[0].timestamp

    def last_time(self) -> float:
        return self.path[-1].timestamp


@dataclass
class _KeyState:
    """All state belonging to one partition key."""

    key: Any = None
    candidates: List[_Candidate] = field(default_factory=list)
    # All relevant events kept for absence checks, sorted by (ts, arrival).
    buffer: List[Event] = field(default_factory=list)
    # Events already consumed by an emitted match under NO_REUSE.
    used_event_ids: set = field(default_factory=set)
    arrival_counter: int = 0
    # Earliest event time still referenced by the buffer.
    min_buffer_ts: Optional[float] = None


class CEPEngine:
    """Run one or more patterns over a stream of events.

    Parameters
    ----------
    patterns:
        A pattern or a list of patterns; every pattern is matched
        independently and in parallel.
    watermark_assigner:
        How event time progresses. Defaults to a bounded-delay assigner with
        zero delay (events assumed ordered); pass a delayed assigner to
        tolerate out-of-order arrival, or a manual one to drive event time
        yourself.
    reuse_policy:
        :data:`REUSE_EVENTS` (default) or :data:`NO_REUSE`.
    on_match:
        Optional callback invoked with every finalized
        :class:`~streamz.cep.types.Match`.
    on_late_event:
        Optional callback invoked with events that arrive after the
        watermark passed their timestamp (and are therefore ignored).
    on_expired:
        Optional callback invoked with ``(pattern_name, key, candidate_events)``
        whenever a candidate is released without matching.
    """

    def __init__(
        self,
        patterns: Sequence[Pattern],
        *,
        watermark_assigner: Optional[WatermarkAssigner] = None,
        reuse_policy: str = REUSE_EVENTS,
        on_match: Optional[Callable[[Match], None]] = None,
        on_late_event: Optional[Callable[[Event], None]] = None,
        on_expired: Optional[
            Callable[[str, Any, List[Event]], None]
        ] = None,
    ):
        if isinstance(patterns, Pattern):
            patterns = [patterns]
        self.patterns: List[Pattern] = list(patterns)
        if not self.patterns:
            raise ValueError("at least one pattern is required")
        names = [p.name for p in self.patterns]
        if len(set(names)) != len(names):
            raise ValueError("pattern names must be unique")
        for pattern in self.patterns:
            if not pattern.steps:
                raise ValueError(
                    f"pattern {pattern.name!r} has no steps"
                )
        if reuse_policy not in (REUSE_EVENTS, NO_REUSE):
            raise ValueError(
                f"unknown reuse_policy {reuse_policy!r}"
            )
        self.watermark = watermark_assigner or WatermarkAssigner(delay=0.0)
        self.reuse_policy = reuse_policy
        self.on_match = on_match
        self.on_late_event = on_late_event
        self.on_expired = on_expired

        self._states: Dict[Any, _KeyState] = {}
        self._current_wm: Optional[float] = None
        # Once an external watermark marker drives this engine (the typical
        # streamz topology with an upstream ``watermark`` node), the internal
        # assigner stops advancing event time automatically.
        self._external_watermark = False
        # Pending finalized matches, held until their flush timestamp so the
        # output order is stable under out-of-order arrival.
        self._pending: List[Tuple[float, int, Match]] = []
        # For deterministic flush ordering across keys.
        self._flush_counter = 0

    def use_external_watermarks(self) -> None:
        """Disable internal watermark generation; rely on on_watermark()."""
        self._external_watermark = True

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def process(self, event: Event) -> List[Match]:
        """Fold one event into the engine; return matches emitted right now.

        Most matches are emitted from :meth:`on_watermark` instead; the list
        returned here is typically empty except for zero-delay watermarks
        where everything can be decided immediately.
        """
        state = self._states.setdefault(
            event.key, _KeyState(key=event.key)
        )

        # Note: the late-event check happens *after* observing this event's
        # timestamp (below). An event that merely trails the highest timestamp
        # seen so far must still be accepted as long as it is not older than
        # the current watermark -- that is exactly the disorder the
        # configured delay is meant to absorb.
        state.arrival_counter += 1
        if event.event_id is None:
            object.__setattr__(
                event, "event_id", f"{event.key}#{state.arrival_counter}"
            )
        # Events are kept sorted by (timestamp, arrival order) so every
        # downstream decision is deterministic.
        self._insert_sorted(state.buffer, event)
        state.min_buffer_ts = state.buffer[0].timestamp

        advanced = None
        if not self._external_watermark:
            advanced = self.watermark.observe(event)

        # Only now that the assigner has folded this event in can we tell
        # whether it is genuinely late (older than the asserted watermark).
        if (
            self._current_wm is not None
            and event.timestamp < self._current_wm
        ):
            # The event was buffered above; take it straight back out so it
            # cannot influence absence checks or candidate progression.
            state.buffer.remove(event)
            if self.on_late_event:
                self.on_late_event(event)
            return []

        for pattern in self.patterns:
            # Snapshot *before* starting new runs so the triggering event is
            # consumed exactly once per run: a run started by this event must
            # not be extended by the same event again in this call.
            existing = [
                c for c in state.candidates if c.pattern is pattern
            ]
            self._start_candidates(pattern, state, event)
            self._extend_candidates(pattern, state, event, existing)
        self._epsilon_closure(state)
        # Newly started runs may be extendable by events already buffered
        # (out-of-order arrival). Catch them up before any watermark decision.
        self._replay_buffer(state)

        emitted: List[Match] = []
        if advanced is not None:
            emitted = self.on_watermark(advanced)
        return emitted

    def on_watermark(self, wm: float, *, external: bool = False) -> List[Match]:
        """Advance the watermark to ``wm``; return all matches now finalized.

        Parameters
        ----------
        external:
            Set by upstream watermark markers. Once an external marker has
            been seen, the internal assigner stops advancing event time.
        """
        if self._current_wm is not None and wm <= self._current_wm:
            return []
        if external:
            self._external_watermark = True
        self._current_wm = wm
        emitted: List[Match] = []

        for key, state in list(self._states.items()):
            # Absence decisions first: they need buffered events inside the
            # observation window, so they must run before buffer pruning.
            self._decide_absences(state, wm)
            self._expire_candidates(state, wm)
            self._prune_buffer(state, wm)
            if self._is_empty(state):
                del self._states[key]

        # Flush every match whose stability timestamp has been reached.
        ready = [
            item for item in self._pending if item[0] <= wm
        ]
        self._pending = [
            item for item in self._pending if item[0] > wm
        ]
        ready.sort(key=lambda item: (item[0], item[1]))
        for _, _, match in ready:
            emitted.extend(self._emit(match))
        return emitted

    def flush(self) -> List[Match]:
        """Emit everything currently decidable at the present watermark."""
        if self._current_wm is None:
            return []
        return self.on_watermark(self._current_wm)

    @property
    def active_keys(self):
        return list(self._states.keys())

    def active_candidate_count(self, key: Any) -> int:
        state = self._states.get(key)
        return len(state.candidates) if state else 0

    # ------------------------------------------------------------------ #
    # Candidate construction
    # ------------------------------------------------------------------ #

    def _start_candidates(
        self, pattern: Pattern, state: _KeyState, event: Event
    ) -> None:
        first = pattern.steps[0]
        if first.is_absence_step():
            # An absence-only pattern needs a reference point, which does not
            # exist; reject such patterns up front instead.
            raise ValueError(
                f"pattern {pattern.name!r} must contain at least one "
                "event-capturing step before its absence"
            )
        if self._accepts(first, event):
            cand = self._new_candidate(pattern, state.key, event)
            state.candidates.append(cand)

    def _new_candidate(
        self, pattern: Pattern, key: Any, event: Event
    ) -> _Candidate:
        captured: List[List[Event]] = [
            [event] if i == 0 else [] for i in range(len(pattern.steps))
        ]
        expiry = self._initial_expiry(pattern, event)
        return _Candidate(
            pattern=pattern,
            key=key,
            step_index=0,
            captured=captured,
            path=[event],
            expiry=expiry,
            step_seen=[{event.event_id}] + [
                set() for _ in range(len(pattern.steps) - 1)
            ],
        )

    def _initial_expiry(
        self, pattern: Pattern, event: Event
    ) -> Optional[float]:
        if pattern.ttl is not None:
            return event.timestamp + pattern.ttl
        if pattern.within is not None:
            return event.timestamp + pattern.within
        return None

    def _extend_candidates(
        self,
        pattern: Pattern,
        state: _KeyState,
        event: Event,
        existing: List[_Candidate],
    ) -> None:
        """NFA transition of ``existing`` runs on one event.

        Standard NFA-CEP transitions (a la Flink):

        * **Take** -- the event matches the current step and its repetition
          budget is not exhausted: consume it into the current step.
        * **Proceed** -- the current step already holds at least
          ``min_count`` events (or is optional with none): move to the next
          step *without* consuming the event, then let the event try there.
        * **Ignore** -- anything else: the run survives unchanged, so
          non-matching events may be skipped.
        """
        spawned: List[_Candidate] = []
        for cand in existing:
            if cand.awaiting_absence:
                continue
            for run in self._proceed_chain(cand):
                if run.awaiting_absence or run.step_index >= len(pattern.steps):
                    # Parked/finalized runs do not consume events.
                    continue
                seen_here = run.step_seen[run.step_index]
                if event.event_id in seen_here:
                    # Already evaluated against this exact NFA state.
                    continue
                seen_here.add(event.event_id)
                if event.event_id in run.consumed_ids():
                    # Already taken by an earlier step of this same run; an
                    # event can never be consumed twice by one match.
                    continue
                step = pattern.steps[run.step_index]
                if not self._accepts(step, event):
                    continue
                if not self._within_ok(run, step, event):
                    continue
                if step.consecutive and not self._consecutive_ok(
                    state, run, event
                ):
                    continue
                count = len(run.captured[run.step_index])
                can_repeat = step.max_count is None or count < step.max_count
                if not can_repeat:
                    continue
                taken = self._clone(run)
                taken.captured[taken.step_index].append(event)
                taken.path.append(event)
                taken.step_seen[taken.step_index].add(event.event_id)
                spawned.append(taken)
        state.candidates.extend(spawned)
        self._dedupe(state)

    def _replay_buffer(self, state: _KeyState) -> None:
        """Re-feed buffered events to runs that have not evaluated them yet.

        Out-of-order arrival can create a run (started by a late first-step
        event) whose preceding buffered events actually extend it. Replay
        walks the buffer in event-time order and gives every run a chance to
        evaluate each buffered event against its current step, so the final
        state equals the in-order result.

        Idempotence comes from the per-step ``step_seen`` markers inside
        :meth:`_extend_candidates`: an event already evaluated by a given NFA
        state is skipped, while a state reached later (after proceeding to a
        new step) gets a fresh marker set and may consume it.
        """
        if not state.buffer or not state.candidates:
            return
        # Fixed point: replay may spawn further runs that also need to catch
        # up, so repeat until a full pass produces nothing new.
        for _ in range(len(state.buffer) + 1):
            progressed = False
            for event in list(state.buffer):
                for pattern in self.patterns:
                    eligible = [
                        c
                        for c in state.candidates
                        if c.pattern is pattern
                        and not c.awaiting_absence
                        and event.timestamp >= c.start_time()
                        and event.event_id
                        not in c.step_seen[c.step_index]
                    ]
                    if not eligible:
                        continue
                    before = len(state.candidates)
                    self._extend_candidates(
                        pattern, state, event, eligible
                    )
                    self._epsilon_closure(state)
                    progressed = progressed or len(state.candidates) != before
            if not progressed:
                break

    def _epsilon_closure(self, state: _KeyState) -> None:
        """Resolve epsilon transitions after every event.

        Runs whose current step already holds at least ``min_count`` events
        move to the next step *in place* (proceed is a move, not a copy):
        the run at the old position is removed and replaced by the advanced
        clone, so one logical run is never finalized twice. An optional step
        with no captures is the only transition that branches: the run may
        skip the step while the original position remains able to capture
        later. The chain ends at the pattern boundary or the terminal absence
        step; completed runs are queued or parked.

        The scan is a fixed point over the *live* candidate list so advanced
        clones are themselves resolved in the same call and no stale
        intermediate position is left behind.
        """
        for _ in range(len(state.buffer) + len(self.patterns) + 2):
            moved_any = False
            for cand in list(state.candidates):
                pattern = cand.pattern
                if cand.awaiting_absence:
                    continue
                if cand.step_index >= len(pattern.steps):
                    self._park_or_finalize(cand, state)
                    moved_any = True
                    continue
                step = pattern.steps[cand.step_index]
                if step.is_absence_step():
                    self._park_or_finalize(cand, state)
                    moved_any = True
                    continue
                count = len(cand.captured[cand.step_index])
                saturated = (
                    step.max_count is not None and count >= step.max_count
                )
                satisfied = count >= step.min_count
                optional_empty = count == 0 and step.min_count == 0
                if saturated or (satisfied and not optional_empty):
                    # Proceed: replace the run with its advanced clone.
                    if cand in state.candidates:
                        state.candidates.remove(cand)
                    advanced = self._clone(cand)
                    advanced.step_index += 1
                    state.candidates.append(advanced)
                    moved_any = True
                elif optional_empty:
                    # Branch: keep the run in place (it may still capture
                    # into this optional step) AND add a skipping clone.
                    advanced = self._clone(cand)
                    advanced.step_index += 1
                    state.candidates.append(advanced)
                    moved_any = True
            self._dedupe(state)
            if not moved_any:
                break

    def _proceed_chain(self, cand: _Candidate) -> List[_Candidate]:
        """All states reachable from ``cand`` via Proceed (no-event) moves.

        A Proceed move is possible once the current step has collected at
        least ``min_count`` events, or is saturated. The chain ends at a
        step that still needs events, at the pattern end, or at the terminal
        absence step (which never consumes events).
        """
        pattern = cand.pattern
        chain = [cand]
        current = cand
        while True:
            if current.awaiting_absence:
                return chain
            if current.step_index >= len(pattern.steps):
                return chain
            step = pattern.steps[current.step_index]
            if step.is_absence_step():
                return chain
            count = len(current.captured[current.step_index])
            saturated = (
                step.max_count is not None and count >= step.max_count
            )
            satisfied = count >= step.min_count
            if not (saturated or satisfied):
                return chain
            moved = self._clone(current)
            moved.step_index += 1
            chain.append(moved)
            current = moved

    def _park_or_finalize(
        self, cand: _Candidate, state: _KeyState
    ) -> None:
        # The same logical run may reach this point through several closure
        # paths; only the first occurrence is still attached to the state.
        if cand not in state.candidates:
            return
        pattern = cand.pattern
        if pattern.has_absence():
            absence_step = pattern.steps[-1]
            absence = absence_step.absence
            cand.awaiting_absence = True
            cand.absence_deadline = cand.last_time() + absence.window
            # Absence deadline also bounds the candidate's lifetime.
            cand.expiry = cand.absence_deadline
        else:
            # Fully matched without a negative condition: queue it. It still
            # waits for the watermark so output is stable under disorder.
            self._queue_match(cand, state)
            state.candidates.remove(cand)

    # ------------------------------------------------------------------ #
    # Watermark-driven decisions
    # ------------------------------------------------------------------ #

    def _decide_absences(self, state: _KeyState, wm: float) -> None:
        for cand in list(state.candidates):
            if not cand.awaiting_absence:
                continue
            if wm < cand.absence_deadline:
                continue
            pattern = cand.pattern
            absence_step = pattern.steps[-1]
            predicate = (
                absence_step.absence.predicate or absence_step.predicate
            )
            window_start = cand.last_time()
            forbidden = [
                e
                for e in state.buffer
                if window_start < e.timestamp <= cand.absence_deadline
                and predicate(e)
            ]
            if forbidden:
                # Negative condition violated: the run dies.
                state.candidates.remove(cand)
                if self.on_expired:
                    self.on_expired(
                        pattern.name, cand.key, list(cand.path)
                    )
            else:
                self._queue_match(cand, state, verified_to=wm)
                state.candidates.remove(cand)

    def _expire_candidates(self, state: _KeyState, wm: float) -> None:
        for cand in list(state.candidates):
            deadline = cand.expiry
            if deadline is None:
                continue
            if wm < deadline:
                continue
            # An absence candidate whose deadline has been reached is handled
            # by _decide_absences; only hard TTL/within expiries land here.
            if cand.awaiting_absence:
                continue
            state.candidates.remove(cand)
            if self.on_expired:
                self.on_expired(
                    cand.pattern.name, cand.key, list(cand.path)
                )

    def _prune_buffer(self, state: _KeyState, wm: float) -> None:
        """Drop buffered events that can never be read again.

        A live candidate only ever looks at events at or after its start time
        (replay), its own capture list (consecutive checks), or inside its
        absence window. With no live candidate, buffered events must still be
        kept while they fall inside the out-of-order horizon: a first-step
        event may arrive later and replay them. Only events that are both

        * older than the current watermark, and
        * older than every live candidate's start time (when any exist),

        are truly unreachable. This is the "expired candidates release their
        events" guarantee without destroying recoverable disordered events.
        """
        if not state.buffer:
            return
        if state.candidates:
            floor = min(c.start_time() for c in state.candidates)
            retain_from = min(floor, wm)
        else:
            # No run alive: keep everything at/above the watermark. Events
            # below it are declared never to arrive again, so they cannot be
            # replayed by any run that starts in the future.
            retain_from = wm
        state.buffer = [e for e in state.buffer if e.timestamp >= retain_from]
        state.min_buffer_ts = (
            state.buffer[0].timestamp if state.buffer else None
        )

    # ------------------------------------------------------------------ #
    # Match output and reuse policy
    # ------------------------------------------------------------------ #

    def _queue_match(
        self,
        cand: _Candidate,
        state: _KeyState,
        verified_to: Optional[float] = None,
    ) -> None:
        pattern = cand.pattern

        def _ordered(events):
            return sorted(
                events, key=lambda e: (e.timestamp, self._arrival(e, state))
            )

        events_by_step = {
            step.name: _ordered(cand.captured[i])
            for i, step in enumerate(pattern.steps)
            if cand.captured[i]
        }
        ordered = _ordered(cand.path)
        match = Match(
            pattern_name=pattern.name,
            key=cand.key,
            events=ordered,
            events_by_step=events_by_step,
            started_at=ordered[0].timestamp,
            ended_at=ordered[-1].timestamp,
            absence_verified=verified_to,
        )
        flush_at = (
            cand.absence_deadline
            if cand.awaiting_absence
            else ordered[-1].timestamp
        )
        self._flush_counter += 1
        self._pending.append((flush_at, self._flush_counter, match))

    def _emit(self, match: Match) -> List[Match]:
        if self.reuse_policy == NO_REUSE:
            used = self._states.get(match.key, _KeyState()).used_event_ids
            ids = {e.event_id for e in match.events}
            if ids & used:
                # The match relies on an event already consumed elsewhere.
                self._drop_dependents(match)
                return []
            used.update(ids)
        object.__setattr__(
            match,
            "match_id",
            self._make_match_id(match),
        )
        if self.on_match:
            self.on_match(match)
        return [match]

    def _drop_dependents(self, match: Match) -> None:
        """Under NO_REUSE, kill candidates that still reference used events."""
        state = self._states.get(match.key)
        if state is None:
            return
        consumed = {e.event_id for e in match.events}
        for cand in list(state.candidates):
            if any(
                e.event_id in consumed for e in cand.path
            ):
                state.candidates.remove(cand)

    @staticmethod
    def _make_match_id(match: Match) -> str:
        ids = "|".join(str(e.event_id) for e in match.events)
        return f"{match.pattern_name}:{match.key}:{ids}"

    # ------------------------------------------------------------------ #
    # Predicate / constraint helpers
    # ------------------------------------------------------------------ #

    def _accepts(self, step: Step, event: Event) -> bool:
        try:
            return bool(step.predicate(event))
        except Exception:
            return False

    def _within_ok(
        self, cand: _Candidate, step: Step, event: Event
    ) -> bool:
        if step.within is None and cand.expiry is None:
            return True
        start = cand.start_time()
        if step.within is not None:
            if event.timestamp - start > step.within:
                return False
        if cand.expiry is not None and event.timestamp > cand.expiry:
            return False
        return True

    def _consecutive_ok(
        self, state: _KeyState, cand: _Candidate, event: Event
    ) -> bool:
        # The event immediately preceding this one (event-time order, ties
        # broken by arrival) must belong to the same step's capture list.
        captured = cand.captured[cand.step_index]
        if not captured:
            return True  # first event of the step is adjacent by definition
        idx = self._buffer_index(state.buffer, event)
        if idx <= 0:
            return False
        previous = state.buffer[idx - 1]
        captured_ids = {e.event_id for e in captured}
        return previous.event_id in captured_ids

    # ------------------------------------------------------------------ #
    # Bookkeeping
    # ------------------------------------------------------------------ #

    @staticmethod
    def _clone(cand: _Candidate) -> _Candidate:
        return _Candidate(
            pattern=cand.pattern,
            key=cand.key,
            step_index=cand.step_index,
            captured=[list(evs) for evs in cand.captured],
            path=list(cand.path),
            awaiting_absence=cand.awaiting_absence,
            absence_deadline=cand.absence_deadline,
            expiry=cand.expiry,
            # Deep-copy the per-step markers; advancing a step simply reads a
            # different (already empty) slot.
            step_seen=[set(s) for s in cand.step_seen],
        )

    @staticmethod
    def _dedupe(state: _KeyState) -> None:
        seen = set()
        unique: List[_Candidate] = []
        for cand in state.candidates:
            token = (id(cand.pattern), cand.key_tuple())
            if token in seen:
                continue
            seen.add(token)
            unique.append(cand)
        state.candidates = unique

    @staticmethod
    def _insert_sorted(buffer: List[Event], event: Event) -> None:
        # Arrival order breaks timestamp ties; appended events always sort
        # after equal-timestamp events already buffered.
        lo, hi = 0, len(buffer)
        while lo < hi:
            mid = (lo + hi) // 2
            if buffer[mid].timestamp <= event.timestamp:
                lo = mid + 1
            else:
                hi = mid
        buffer.insert(lo, event)

    def _arrival(self, event: Event, state: _KeyState) -> int:
        # event_id encodes per-key arrival as generated in process(); for
        # caller-supplied ids fall back to buffer position.
        for i, e in enumerate(state.buffer):
            if e.event_id == event.event_id:
                return i
        return 0

    @staticmethod
    def _buffer_index(buffer: List[Event], event: Event) -> int:
        for i, e in enumerate(buffer):
            if e.event_id == event.event_id:
                return i
        return -1

    @staticmethod
    def _is_empty(state: _KeyState) -> bool:
        return not state.candidates and not state.buffer
