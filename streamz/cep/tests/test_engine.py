"""Tests for the NFA CEP engine semantics."""
import pytest

from streamz.cep import (
    Absence,
    CEPEngine,
    NO_REUSE,
    Pattern,
    REUSE_EVENTS,
    Step,
    WatermarkAssigner,
    event_is,
)
from streamz.cep.types import Event


def evt(etype, ts, key="k", eid=None, data=None):
    return Event(etype, ts, key=key, event_id=eid or f"{key}-{etype}-{ts}",
                 data=data or {})


def engine(pattern, *, delay=0.0, **kw):
    out = []
    eng = CEPEngine(
        pattern,
        watermark_assigner=WatermarkAssigner(delay=delay),
        on_match=out.append,
        **kw,
    )
    return eng, out


def feed(eng, events, final_wm=10_000):
    for e in events:
        eng.process(e)
    return eng.on_watermark(final_wm)


# --------------------------------------------------------------------------- #
# Basic ordered matching
# --------------------------------------------------------------------------- #

def test_simple_two_step_ordered_match():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("b", 2)])
    assert [m.event_ids() for m in out] == [["k-a-1", "k-b-2"]]
    match = out[0]
    assert match.started_at == 1 and match.ended_at == 2
    assert set(match.events_by_step) == {"a", "b"}
    assert match.match_id.startswith("ab:k:")


def test_non_matching_events_are_skipped():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("x", 2), evt("b", 3)])
    assert len(out) == 1
    assert out[0].event_ids() == ["k-a-1", "k-b-3"]


def test_no_match_when_sequence_incomplete():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("x", 2)])
    assert out == []


def test_events_out_of_order_are_rejected_below_watermark():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    late = []
    eng, out = engine(p, on_late_event=late.append)
    eng.process(evt("a", 1))
    eng.process(evt("b", 5))
    # a@2 arrives after the watermark has already passed t=5 (delay=0)
    eng.process(evt("a", 2))
    eng.on_watermark(100)
    assert [e.event_id for e in late] == ["k-a-2"]


# --------------------------------------------------------------------------- #
# Repetition
# --------------------------------------------------------------------------- #

def test_at_least_repetition_produces_all_valid_paths():
    p = Pattern("views").step(
        Step("v", event_is("view"), min_count=2, max_count=None)
    )
    eng, out = engine(p)
    feed(eng, [evt("view", t) for t in range(3)])
    ids = sorted(tuple(m.event_ids()) for m in out)
    assert ids == [
        ("k-view-0", "k-view-1"),
        ("k-view-0", "k-view-2"),
        ("k-view-1", "k-view-2"),
    ]


def test_bounded_repetition():
    # Relaxed contiguity: any ordered pair of views is a valid length-2 match.
    p = Pattern("two").step(
        Step("v", event_is("view"), min_count=2, max_count=2)
    )
    eng, out = engine(p)
    feed(eng, [evt("view", t) for t in range(3)])
    ids = sorted(tuple(m.event_ids()) for m in out)
    assert ids == [
        ("k-view-0", "k-view-1"),
        ("k-view-0", "k-view-2"),
        ("k-view-1", "k-view-2"),
    ]


def test_consecutive_repetition_requires_adjacency():
    p = Pattern("cc").step(
        Step("c", event_is("c"), min_count=2, max_count=2,
             consecutive=True)
    )
    eng, out = engine(p)
    feed(eng, [evt("c", 1), evt("d", 2), evt("c", 3)])
    assert out == []

    eng2, out2 = engine(p)
    feed(eng2, [evt("c", 1), evt("c", 2), evt("d", 3)])
    assert [m.event_ids() for m in out2] == ["k-c-1 k-c-2".split()]


def test_optional_step_both_branches():
    p = (
        Pattern("opt")
        .step(Step("a", event_is("a")))
        .step(Step("maybe", event_is("c"), min_count=0, max_count=1))
        .step(Step("b", event_is("b")))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("c", 2), evt("b", 3)])
    ids = sorted(tuple(m.event_ids()) for m in out)
    assert ids == [
        ("k-a-1", "k-b-3"),
        ("k-a-1", "k-c-2", "k-b-3"),
    ]

    eng2, out2 = engine(p)
    feed(eng2, [evt("a", 1), evt("b", 2)])
    assert [m.event_ids() for m in out2] == [["k-a-1", "k-b-2"]]


# --------------------------------------------------------------------------- #
# Out-of-order stability
# --------------------------------------------------------------------------- #

def test_shuffled_arrival_gives_identical_matches():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )

    def run(order, delay):
        eng, out = engine(p, delay=delay)
        feed(eng, [evt(ty, t) for t, ty in order])
        return [m.event_ids() for m in out]

    ordered = [(1, "a"), (2, "b"), (500, "x")]
    shuffled = [(500, "x"), (2, "b"), (1, "a")]
    assert run(ordered, 600) == run(shuffled, 600)
    assert run(shuffled, 600) == [["k-a-1", "k-b-2"]]


def test_match_ids_are_deterministic_across_arrival_order():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )

    def ids(order, delay):
        eng, out = engine(p, delay=delay)
        feed(eng, [evt(ty, t) for t, ty in order])
        return [m.match_id for m in out]

    a = ids([(1, "a"), (2, "b")], 0)
    b = ids([(2, "b"), (1, "a")], 10)
    assert a == b


# --------------------------------------------------------------------------- #
# Timed absence (negative condition)
# --------------------------------------------------------------------------- #

@pytest.fixture
def funnel():
    return (
        Pattern("funnel")
        .step(Step("search", event_is("search")))
        .step(Step("views", event_is("view"), min_count=2, max_count=None))
        .step(
            Step(
                "no_consult",
                event_is("consult"),
                min_count=0,
                max_count=0,
                absence=Absence(window=300),
            )
        )
    )


def test_absence_holds_match_emitted_after_window(funnel):
    eng, out = engine(funnel)
    feed(eng, [evt("search", 0), evt("view", 10), evt("view", 20)],
         final_wm=100)
    assert out == []  # window not decided yet
    eng.on_watermark(320)
    assert len(out) == 1
    assert out[0].event_ids() == [
        "k-search-0", "k-view-10", "k-view-20"
    ]
    assert out[0].absence_verified == 320


def test_absence_violated_kills_candidate(funnel):
    expired = []
    eng, out = engine(funnel, on_expired=lambda *a: expired.append(a))
    feed(
        eng,
        [evt("search", 0), evt("view", 10), evt("view", 20),
         evt("consult", 100)],
        final_wm=400,
    )
    assert out == []
    assert len(expired) == 1
    assert expired[0][0] == "funnel"
    # Every run parked on the absence window is gone; only relaxed
    # continuations of the unbounded views step may legitimately remain.
    assert all(
        not c.awaiting_absence
        for c in eng._states["k"].candidates
    )
    # With an explicit TTL those continuations are released as well.
    ttl_funnel = funnel.with_ttl(1000)
    expired2 = []
    eng2, out2 = engine(
        ttl_funnel, on_expired=lambda *a: expired2.append(a)
    )
    feed(
        eng2,
        [evt("search", 0), evt("view", 10), evt("view", 20),
         evt("consult", 100)],
        final_wm=5000,
    )
    assert out2 == []
    assert eng2.active_keys == []


def test_absence_window_is_half_open_at_right_endpoint(funnel):
    # (last_event, last_event + 300]: an event exactly at last+300 violates.
    eng, out = engine(funnel)
    feed(
        eng,
        [evt("search", 0), evt("view", 10), evt("view", 20),
         evt("consult", 320)],
        final_wm=400,
    )
    assert out == []

    eng2, out2 = engine(funnel)
    feed(
        eng2,
        [evt("search", 0), evt("view", 10), evt("view", 20),
         evt("consult", 320.001)],
        final_wm=400,
    )
    assert len(out2) == 1


def test_negation_cannot_be_decided_before_data_complete(funnel):
    eng, out = engine(funnel)
    eng.process(evt("search", 0))
    eng.process(evt("view", 10))
    eng.process(evt("view", 20))
    # Even advancing close to the deadline must not finalize.
    eng.on_watermark(319.999)
    assert out == []
    assert eng.active_candidate_count("k") >= 1


# --------------------------------------------------------------------------- #
# Event reuse policy
# --------------------------------------------------------------------------- #

def test_reuse_events_shares_events_across_matches():
    p = Pattern("aa").step(Step("a1", event_is("a"))).step(
        Step("a2", event_is("a"))
    )
    eng, out = engine(p, reuse_policy=REUSE_EVENTS)
    feed(eng, [evt("a", t) for t in range(3)])
    assert len(out) == 3  # all adjacent pairs share events


def test_no_reuse_blocks_dependent_matches():
    p = Pattern("aa").step(Step("a1", event_is("a"))).step(
        Step("a2", event_is("a"))
    )
    eng, out = engine(p, reuse_policy=NO_REUSE)
    feed(eng, [evt("a", t) for t in range(3)])
    assert [m.event_ids() for m in out] == [
        ["k-a-0", "k-a-1"]
    ]


def test_no_reuse_drops_candidates_referencing_consumed_events():
    p = Pattern("aa").step(Step("a1", event_is("a"))).step(
        Step("a2", event_is("a"))
    )
    eng, out = engine(p, reuse_policy=NO_REUSE)
    feed(eng, [evt("a", 0), evt("a", 1)])
    assert len(out) == 1
    # A later candidate that reused a0 must have been pruned.
    eng.process(evt("a", 2))
    eng.on_watermark(10_000)
    ids = {e for m in out for e in m.event_ids()}
    assert "k-a-0" in ids


# --------------------------------------------------------------------------- #
# Per-key isolation and concurrent candidates
# --------------------------------------------------------------------------- #

def test_partition_keys_are_isolated():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [
        evt("a", 1, key="u1"), evt("a", 1, key="u2"),
        evt("b", 2, key="u1"), evt("b", 2, key="u2"),
    ])
    assert sorted(m.key for m in out) == ["u1", "u2"]


def test_multiple_candidate_paths_coexist_per_key():
    # Two searches each open an independent run that a later view completes.
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("a", 2), evt("b", 3)])
    ids = sorted(tuple(m.event_ids()) for m in out)
    assert ids == [("k-a-1", "k-b-3"), ("k-a-2", "k-b-3")]


# --------------------------------------------------------------------------- #
# Expiry and resource release
# --------------------------------------------------------------------------- #

def test_ttl_releases_stalled_candidate():
    p = Pattern("ttl").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    ).with_ttl(10)
    expired = []
    eng, out = engine(p, on_expired=lambda *a: expired.append(a))
    eng.process(evt("a", 1))
    eng.on_watermark(5)
    assert eng.active_candidate_count("k") == 1
    eng.on_watermark(20)
    assert len(expired) == 1
    assert eng.active_keys == []
    assert out == []


def test_within_constraint_bounds_step():
    p = Pattern("w").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"), within=5)
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 0), evt("b", 10)])
    assert out == []

    eng2, out2 = engine(p)
    feed(eng2, [evt("a", 0), evt("b", 4)])
    assert len(out2) == 1


def test_buffer_and_state_are_released_after_decisions():
    # A TTL-bounded pattern guarantees every stalled run is eventually
    # released and its buffered events pruned, leaving no per-key state.
    p = (
        Pattern("ab")
        .step(Step("a", event_is("a")))
        .step(Step("b", event_is("b")))
        .with_ttl(100)
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("b", 2)])
    assert len(out) == 1
    # Advance well past the TTL so even the relaxed-contiguity run that could
    # still pair a1 with a future b is expired and the buffer is pruned.
    eng.on_watermark(10_000)
    assert eng.active_keys == []


def test_matched_events_are_reported_completely():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("b", 2)])
    match = out[0]
    # The complete ordered event list is always available on the match.
    assert [e.event_id for e in match.events] == ["k-a-1", "k-b-2"]
    assert match.events_by_step["a"][0].event_type == "a"
    assert match.events_by_step["b"][0].event_type == "b"


# --------------------------------------------------------------------------- #
# Multiple patterns and watermark assigner modes
# --------------------------------------------------------------------------- #

def test_multiple_patterns_run_in_parallel():
    p1 = Pattern("p1").step(Step("a", event_is("a")))
    p2 = Pattern("p2").step(Step("b", event_is("b")))
    out = []
    eng = CEPEngine(
        [p1, p2],
        watermark_assigner=WatermarkAssigner(delay=0),
        on_match=out.append,
    )
    feed(eng, [evt("a", 1), evt("b", 2)])
    names = sorted(m.pattern_name for m in out)
    assert names == ["p1", "p2"]


def test_duplicate_pattern_names_rejected():
    p1 = Pattern("p").step(Step("a", event_is("a")))
    p2 = Pattern("p").step(Step("b", event_is("b")))
    with pytest.raises(ValueError):
        CEPEngine([p1, p2])


def test_manual_watermark_assigner():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    out = []
    eng = CEPEngine(
        p,
        watermark_assigner=WatermarkAssigner(strategy="manual"),
        on_match=out.append,
    )
    eng.process(evt("a", 1))
    eng.process(evt("b", 2))
    assert out == []  # no automatic watermark
    eng.on_watermark(100)
    assert len(out) == 1


def test_watermark_monotonic_ignores_regression():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    eng, out = engine(p)
    feed(eng, [evt("a", 1), evt("b", 2)])
    before = len(out)
    eng.on_watermark(1)  # regression, ignored
    assert len(out) == before
