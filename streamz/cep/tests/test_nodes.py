"""Tests for the streamz ``watermark`` and ``pattern_match`` nodes."""
import streamz.cep  # noqa: F401  registers the Stream API
import pytest

from streamz import Stream
from streamz.cep import (
    Absence,
    NO_REUSE,
    Pattern,
    Step,
    WatermarkAssigner,
    event_is,
)
from streamz.cep.types import Event
from streamz.cep.watermark import Watermark, is_watermark


FUNNEL = (
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


def raw(etype, ts, user="u1", **extra):
    return {"event_type": etype, "timestamp": ts, "user": user, **extra}


def test_watermark_node_emits_events_then_markers():
    source = Stream()
    seen = []
    source.watermark(delay=5).sink(seen.append)
    source.emit(raw("a", 10))
    assert len(seen) == 2
    assert not is_watermark(seen[0])
    assert isinstance(seen[1], Watermark)
    assert seen[1].timestamp == 5  # 10 - delay


def test_watermark_node_coerces_dict_to_event():
    from streamz.cep.types import Event

    source = Stream()
    seen = []
    source.watermark(delay=0, key_field="user").sink(seen.append)
    source.emit(raw("a", 10, amount=42))
    event = seen[0]
    assert isinstance(event, Event)
    assert event.key == "u1"
    assert event.data == {"amount": 42}


def test_pattern_match_node_full_pipeline_out_of_order():
    source = Stream()
    wm = source.watermark(delay=25, key_field="user")
    matches = []
    wm.pattern_match(FUNNEL, key_field="user").sink(matches.append)

    for payload in [
        raw("view", 20),
        raw("search", 0),
        raw("view", 10),
    ]:
        source.emit(payload)

    # Nothing decided before the absence window closes.
    assert matches == []
    wm.set_watermark(400)
    assert len(matches) == 1
    match = matches[0]
    assert [e.event_type for e in match.events] == [
        "search", "view", "view"
    ]
    assert match.events[0].timestamp == 0
    assert match.absence_verified == 400


def test_pattern_match_node_absence_violation_emits_nothing():
    source = Stream()
    wm = source.watermark(delay=0, key_field="user")
    matches = []
    expired = []
    wm.pattern_match(
        FUNNEL, key_field="user",
        on_expired=lambda *a: expired.append(a),
    ).sink(matches.append)

    for payload in [
        raw("search", 0), raw("view", 10), raw("view", 20),
        raw("consult", 100),
    ]:
        source.emit(payload)
    wm.set_watermark(400)
    assert matches == []
    assert len(expired) == 1


def test_pattern_match_node_no_reuse_policy():
    p = Pattern("aa").step(Step("a1", event_is("a"))).step(
        Step("a2", event_is("a"))
    )
    source = Stream()
    matches = []
    (
        source.watermark(delay=0, key_field="user")
        .pattern_match(p, key_field="user", reuse_policy=NO_REUSE)
        .sink(matches.append)
    )
    for t in range(3):
        source.emit(raw("a", t))
    assert len(matches) == 1


def test_pattern_match_direct_source_uses_internal_assigner():
    # No upstream watermark node: the engine advances event time itself.
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    source = Stream()
    matches = []
    source.pattern_match(p, key_field="user").sink(matches.append)
    source.emit(raw("a", 1))
    source.emit(raw("b", 2))
    assert len(matches) == 1
    assert matches[0].event_ids() == ["u1#1", "u1#2"]


def test_manual_watermark_node_holds_output_until_advanced():
    source = Stream()
    wm = source.watermark(strategy="manual", key_field="user")
    matches = []
    wm.pattern_match(
        Pattern("ab").step(Step("a", event_is("a"))).step(
            Step("b", event_is("b"))
        ),
        key_field="user",
    ).sink(matches.append)
    source.emit(raw("a", 1))
    source.emit(raw("b", 2))
    assert matches == []
    wm.set_watermark(100)
    assert len(matches) == 1


def test_late_events_are_reported_not_matched():
    p = (
        Pattern("ab")
        .step(Step("a", event_is("a")))
        .step(Step("b", event_is("b")))
        .with_ttl(1000)
    )
    source = Stream()
    late = []
    node = (
        source.watermark(delay=0, key_field="user")
        .pattern_match(p, key_field="user", on_late_event=late.append)
    )
    source.emit(raw("a", 1))
    source.emit(raw("b", 50))
    source.emit(raw("a", 20))  # older than current watermark
    assert [e.event_id for e in late] == ["u1#3"]
    # The late event was removed from the buffer, so it cannot seed a match.
    state = node.engine._states["u1"]
    assert all(e.timestamp != 20 for e in state.buffer)
    node.engine.on_watermark(10_000)
    assert node.engine.active_keys == []


def test_custom_id_field_flows_into_match():
    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    source = Stream()
    matches = []
    (
        source.watermark(delay=0, key_field="user", id_field="id")
        .pattern_match(p, key_field="user", id_field="id")
        .sink(matches.append)
    )
    source.emit(raw("a", 1, id="A1"))
    source.emit(raw("b", 2, id="B1"))
    assert matches[0].event_ids() == ["A1", "B1"]


def test_explicit_event_objects_pass_through():
    from streamz.cep.types import Event

    p = Pattern("ab").step(Step("a", event_is("a"))).step(
        Step("b", event_is("b"))
    )
    source = Stream()
    matches = []
    source.pattern_match(p).sink(matches.append)
    source.emit(Event("a", 1, key="k", event_id="e-a"))
    source.emit(Event("b", 2, key="k", event_id="e-b"))
    assert matches[0].event_ids() == ["e-a", "e-b"]


def test_invalid_input_type_raises():
    source = Stream()
    node = source.pattern_match(
        Pattern("ab").step(Step("a", event_is("a"))).step(
            Step("b", event_is("b"))
        )
    )
    with pytest.raises(TypeError):
        source.emit(12345)


def test_watermark_assigner_idle_advances_on_stall():
    assigner = WatermarkAssigner(delay=5, idle_timeout=100)
    assigner.observe(Event("a", 50, key="k", event_id="e1"))
    # While events flow the assigner is at 50-5=45; an idle tick re-asserts
    # the same bound (no regression) rather than jumping.
    assert assigner.watermark == 45
    assert assigner.on_idle() is None  # already at the idle bound


def test_watermark_node_manual_advance():
    source = Stream()
    wm = source.watermark(strategy="manual")
    seen = []
    wm.sink(seen.append)
    wm.advance_watermark(10)
    wm.advance_watermark(5)
    markers = [x.timestamp for x in seen if isinstance(x, Watermark)]
    # Monotonic: 10 then 15, the regression is ignored.
    assert markers == [10, 15]
