"""Conversion-path analysis with streamz CEP.

Scenario: detect users who *search*, then view products at least twice, and
then do NOT start a consultation within five minutes. The same events are fed
in two different arrival orders (in order and shuffled) to show that the
watermark + replay logic emits identical, stable matches.

Run with::

    python examples/cep_conversion_funnel.py
"""
from streamz import Stream
from streamz.cep import (
    Absence,
    NO_REUSE,
    Pattern,
    REUSE_EVENTS,
    Step,
    event_is,
)

# ---------------------------------------------------------------------------
# Pattern: search -> >=2 views -> no consultation for 300 event-time seconds
# ---------------------------------------------------------------------------
funnel = (
    Pattern("search_then_views_then_quiet")
    .step(Step("search", event_is("search")))
    .step(Step("views", event_is("view"), min_count=2, max_count=None))
    .step(
        Step(
            "no_consultation",
            event_is("consult"),
            min_count=0,
            max_count=0,
            absence=Absence(window=300),
        )
    )
)


def build_pipeline(reuse_policy):
    source = Stream()
    matches = []
    late = []
    expired = []
    node = (
        source
        # Tolerate up to 30 seconds of out-of-order arrival.
        .watermark(delay=30, key_field="user")
        .pattern_match(
            funnel,
            key_field="user",
            reuse_policy=reuse_policy,
            on_late_event=late.append,
            on_expired=lambda name, key, events: expired.append(
                (name, key, [e.event_id for e in events])
            ),
        )
        .sink(matches.append)
    )
    return source, matches, late, expired, node


def show(title, matches, late, expired):
    print(f"\n=== {title} ===")
    for m in matches:
        steps = {
            name: [e.event_id for e in evs]
            for name, evs in m.events_by_step.items()
        }
        print(
            f"  MATCH user={m.key} events={m.event_ids()} "
            f"steps={steps} verified_to={m.absence_verified}"
        )
    for name, key, ids in expired:
        print(f"  EXPIRED {name} user={key} partial={ids}")
    for e in late:
        print(f"  LATE (dropped) {e.event_id} t={e.timestamp}")


# Each tuple is (event_type, timestamp_seconds, user).
in_order = [
    ("search", 0, "alice"),
    ("view", 10, "alice"),
    ("view", 20, "alice"),
    # A second user interleaves: per-key state is fully isolated.
    ("search", 5, "bob"),
    ("view", 15, "bob"),
    ("view", 25, "bob"),
    ("consult", 120, "bob"),   # bob consults inside his window -> no match
    # Far-future event pushes the watermark past every absence deadline.
    ("view", 5000, "alice"),
    ("view", 5000, "bob"),
]

# Same logical events, scrambled across users and times (within the 30s delay).
shuffled = [
    ("view", 20, "alice"),
    ("view", 25, "bob"),
    ("search", 0, "alice"),
    ("view", 15, "bob"),
    ("view", 10, "alice"),
    ("search", 5, "bob"),
    ("consult", 120, "bob"),
    ("view", 5000, "alice"),
    ("view", 5000, "bob"),
]


def run(events, reuse_policy):
    source, matches, late, expired, node = build_pipeline(reuse_policy)
    for etype, ts, user in events:
        source.emit(
            {"event_type": etype, "timestamp": ts, "user": user,
             "event_id": f"{user}-{etype}-{ts}"}
        )
    return matches, late, expired


if __name__ == "__main__":
    m1, l1, x1 = run(in_order, REUSE_EVENTS)
    show("in-order arrival", m1, l1, x1)

    m2, l2, x2 = run(shuffled, REUSE_EVENTS)
    show("shuffled arrival (same events, 30s disorder tolerated)", m2, l2, x2)

    ids1 = sorted(m.match_id for m in m1)
    ids2 = sorted(m.match_id for m in m2)
    assert ids1 == ids2, "out-of-order input must yield identical matches"
    print("\nStable output confirmed: both arrival orders produce",
          f"{len(ids1)} identical match id(s).")

    # Event-reuse policy on an overlapping a->a style pattern.
    overlap = (
        Pattern("two_views")
        .step(Step("v1", event_is("view")))
        .step(Step("v2", event_is("view")))
    )
    src = Stream()
    out = []
    (
        src.watermark(delay=0, key_field="user")
        .pattern_match(overlap, key_field="user",
                       reuse_policy=NO_REUSE)
        .sink(out.append)
    )
    for ts in range(3):
        src.emit({"event_type": "view", "timestamp": ts, "user": "carol"})
    print(
        "\nNO_REUSE over three views yields one match (events not shared):",
        [m.event_ids() for m in out],
    )
