Complex Event Processing (CEP)
==============================

.. currentmodule:: streamz.cep

The :mod:`streamz.cep` package matches configurable *event sequences* over a
behavioural stream -- conversion funnels, sessions, "did A then B but never C"
rules -- while tolerating out-of-order arrival and bounding the memory used by
partial matches.

A pattern is an ordered list of :class:`~streamz.cep.pattern.Step` objects.
Each step selects events with a predicate, may repeat a configurable number of
times, may be time-bounded, and -- only as the final step -- may carry a timed
:class:`~streamz.cep.pattern.Absence` condition ("no matching event occurs
within the next N seconds"). Matching runs on an event-time
:class:`~streamz.cep.watermark.WatermarkAssigner`, so nothing is emitted before
the data needed to decide it has actually arrived.

The headline example -- *a search, then at least two product views, followed by
no consultation for five minutes* -- looks like this:

.. code-block:: python

    from streamz import Stream
    from streamz.cep import Pattern, Step, Absence, event_is, NO_REUSE

    funnel = (
        Pattern("search_views_no_consultation")
        .step(Step("search", event_is("search")))
        .step(Step("views", event_is("view"), min_count=2, max_count=None))
        .step(
            Step(
                "no_consult",
                event_is("consult"),
                min_count=0,
                max_count=0,
                absence=Absence(window=300),  # 5 minutes, event-time seconds
            )
        )
    )

    source = Stream()
    (
        source
        .watermark(delay=10, key_field="user_id")      # tolerate 10s disorder
        .pattern_match(funnel, key_field="user_id")
        .sink(print)
    )

Defining patterns
-----------------

Predicates
~~~~~~~~~~

A step predicate is any callable receiving an
:class:`~streamz.cep.types.Event` and returning a boolean. Helpers are provided
for the common cases:

.. code-block:: python

    from streamz.cep import event_is, event_in, field_equals, all_of, any_of

    Step("click", event_is("click"))
    Step("nav", event_in(["click", "scroll"]))
    Step("home", field_equals("page", "/home"))
    Step("big_order", all_of(event_is("order"),
                             lambda e: e.data["amount"] > 100))

Repetition
~~~~~~~~~~

``min_count`` / ``max_count`` bound how many events a step captures
(``max_count=None`` means unbounded, i.e. "at least min_count").
``min_count=0`` makes a step optional. With ``consecutive=True`` the captured
events must be adjacent; otherwise non-matching events may be skipped between
repetitions (relaxed contiguity).

.. code-block:: python

    Step("two_to_four", event_is("view"), min_count=2, max_count=4)
    Step("at_least_two", event_is("view"), min_count=2, max_count=None)
    Step("optional_coupon", event_is("coupon"), min_count=0, max_count=1)
    Step("burst", event_is("click"), min_count=3, consecutive=True)

Time constraints
~~~~~~~~~~~~~~~~

* ``Step(..., within=seconds)`` bounds the time a step may span, measured from
  the first event of the pattern (or the previous step's first event).
* ``Pattern(..., within=seconds)`` sets a global budget between the first and
  last contributing event.
* ``Pattern(...).with_ttl(seconds)`` bounds the lifetime of an unfinished
  candidate. Always set a TTL (or use a terminal absence) for patterns with
  unbounded repetitions, otherwise a stalled candidate has no deadline and is
  kept forever.

Timed absence (negative conditions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A terminal step with ``min_count=0, max_count=0`` and an
:class:`~streamz.cep.pattern.Absence` captures no events. Instead the engine
parks the candidate and checks, once the watermark reaches
``last_event_time + window``, that no event matching the predicate occurred in
the half-open interval ``(last_event_time, last_event_time + window]``. A
negative condition therefore **cannot produce a false positive from late
data**: the match is only finalized after the watermark guarantees the window
is complete. If a forbidden event does occur, the candidate is discarded and
reported through the ``on_expired`` callback.

Matching semantics
------------------

Multiple candidates per key
~~~~~~~~~~~~~~~~~~~~~~~~~~~

State is kept independently per partition key (one user or session each), and
every pattern keeps its own set of *candidate* partial matches per key. When an
event can extend several runs (for example two searches followed by one view),
all valid runs coexist until they complete or expire. This is implemented as an
NFA with **Take / Proceed / Ignore** transitions, the same model used by Flink
CEP.

Event reuse
~~~~~~~~~~~

``pattern_match(..., reuse_policy=...)`` selects whether events may be shared
between emitted matches:

* :data:`~streamz.cep.engine.REUSE_EVENTS` (default): an event can appear in
  several matches -- three ``a`` events against an ``a -> a`` pattern yield all
  adjacent pairs.
* :data:`~streamz.cep.engine.NO_REUSE`: once an event is consumed by an emitted
  match it is locked; later matches that would need it are suppressed, and live
  candidates referencing the consumed event are dropped.

Out-of-order handling and stability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`~streamz.cep.nodes.watermark` node derives a watermark as
``max_seen_timestamp - delay`` (bounded out-of-orderness), or can be driven
manually with ``strategy="manual"`` and :meth:`set_watermark` /
:meth:`advance_watermark`. Events arriving within the disorder window are
buffered; when a late first-step event starts a new candidate, the engine
*replays* the buffered events in event-time order so the candidate reaches
exactly the state it would have reached under in-order arrival. Consequently:

* the same input events produce the same matches, in the same order, regardless
  of arrival order;
* every :class:`~streamz.cep.types.Match` carries the complete, ordered list of
  contributing events (``match.events``) and a per-step breakdown
  (``match.events_by_step``);
* events older than the current watermark are genuinely late: they are dropped
  and reported through ``on_late_event`` rather than silently changing results.

Resource release
~~~~~~~~~~~~~~~~

Candidates past their TTL/``within`` deadline and candidates whose absence
condition is violated are released (and reported via ``on_expired``). Buffered
events that no live candidate can still read are pruned on every watermark, and
a partition key is removed entirely once it holds neither candidates nor
buffered events.

Engine without streamz
----------------------

The matching core is streamz-agnostic and can be embedded directly:

.. code-block:: python

    from streamz.cep import CEPEngine, WatermarkAssigner
    from streamz.cep.types import Event

    engine = CEPEngine(
        funnel,
        watermark_assigner=WatermarkAssigner(delay=10),
        on_match=lambda match: handle(match),
        on_late_event=lambda event: log_late(event),
        on_expired=lambda name, key, events: log_expiry(name, key, events),
    )
    for raw in ingest():
        engine.process(Event.from_mapping(raw, key=raw["user_id"]))

API reference
-------------

.. autoclass:: streamz.cep.pattern.Pattern
   :members:
.. autoclass:: streamz.cep.pattern.Step
   :members:
.. autoclass:: streamz.cep.pattern.Repeat
   :members:
.. autoclass:: streamz.cep.pattern.Absence
   :members:
.. autofunction:: streamz.cep.pattern.event_is
.. autofunction:: streamz.cep.pattern.event_in
.. autofunction:: streamz.cep.pattern.field_equals
.. autofunction:: streamz.cep.pattern.all_of
.. autofunction:: streamz.cep.pattern.any_of
.. autoclass:: streamz.cep.engine.CEPEngine
   :members:
.. autoclass:: streamz.cep.watermark.WatermarkAssigner
   :members:
.. autoclass:: streamz.cep.types.Event
   :members:
.. autoclass:: streamz.cep.types.Match
   :members:
.. autoclass:: streamz.cep.nodes.watermark
   :members:
.. autoclass:: streamz.cep.nodes.pattern_match
   :members:
