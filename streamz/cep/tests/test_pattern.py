"""Tests for the CEP pattern DSL and validation."""
import pytest

from streamz.cep import (
    Absence,
    Pattern,
    Repeat,
    Step,
    event_in,
    event_is,
    field_equals,
)
from streamz.cep.types import Event


def evt(etype, ts, key="k", eid=None):
    return Event(etype, ts, key=key, event_id=eid or f"{etype}{ts}")


class TestRepeat:
    def test_bounds_validation(self):
        with pytest.raises(ValueError):
            Repeat(min_count=-1)
        with pytest.raises(ValueError):
            Repeat(min_count=3, max_count=2)

    def test_optional_and_unbounded(self):
        assert Repeat(0, 1).optional
        assert Repeat(2, None).unbounded
        assert not Repeat(1, 1).unbounded


class TestStep:
    def test_predicate_must_be_callable(self):
        with pytest.raises(TypeError):
            Step("s", "not-callable")

    def test_within_must_be_positive(self):
        with pytest.raises(ValueError):
            Step("s", event_is("a"), within=0)

    def test_absence_requires_zero_repeat(self):
        with pytest.raises(ValueError):
            Pattern("p").step(
                Step(
                    "bad",
                    event_is("x"),
                    min_count=1,
                    absence=Absence(window=10),
                )
            )


class TestPattern:
    def test_chaining_builds_steps(self):
        p = (
            Pattern("p")
            .step(Step("a", event_is("a")))
            .step(Step("b", event_is("b")))
        )
        assert [s.name for s in p.steps] == ["a", "b"]

    def test_duplicate_step_names_rejected(self):
        with pytest.raises(ValueError):
            Pattern("p", [
                Step("same", event_is("a")),
                Step("same", event_is("b")),
            ])

    def test_absence_only_on_final_step(self):
        with pytest.raises(ValueError):
            Pattern("p", [
                Step(
                    "neg",
                    event_is("x"),
                    min_count=0,
                    max_count=0,
                    absence=Absence(window=5),
                ),
                Step("b", event_is("b")),
            ])

    def test_absence_only_pattern_rejected(self):
        with pytest.raises(ValueError):
            Pattern("p").step(
                Step(
                    "neg",
                    event_is("x"),
                    min_count=0,
                    max_count=0,
                    absence=Absence(window=5),
                )
            )

    def test_ttl_and_within_validation(self):
        with pytest.raises(ValueError):
            Pattern("p").with_ttl(0)
        with pytest.raises(ValueError):
            Pattern("p").with_within(-1)

    def test_max_horizon(self):
        p = (
            Pattern("p", ttl=50)
            .step(Step("a", event_is("a")))
            .step(
                Step(
                    "neg",
                    event_is("x"),
                    min_count=0,
                    max_count=0,
                    absence=Absence(window=300),
                )
            )
        )
        assert p.max_horizon() == 300

    def test_sequence_constructor(self):
        p = Pattern.sequence("p", event_is("a"), event_is("b"))
        assert [s.name for s in p.steps] == ["step_0", "step_1"]


class TestPredicates:
    def test_event_is_and_in(self):
        e = evt("search", 1)
        assert event_is("search")(e)
        assert not event_is("view")(e)
        assert event_in(["view", "search"])(e)

    def test_field_equals_attribute_and_payload(self):
        e = Event("click", 1, key="k", event_id="e1",
                  data={"page": "/home"})
        assert field_equals("event_type", "click")(e)
        assert field_equals("page", "/home")(e)
        assert not field_equals("page", "/other")(e)
