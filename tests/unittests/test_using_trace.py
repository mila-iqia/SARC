# test the spans helper context manager

import pytest
from opentelemetry.trace import StatusCode

from sarc.traces import using_trace


def test_using_trace_noerror(captrace):
    with using_trace("test_using_trace_noerror", "span1") as span:
        span.add_event("event1")
    with using_trace("test_using_trace_noerror", "span2") as span:
        span.add_event("event2.1")
        span.add_event("event2.2")

    # check spans
    spans = captrace.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].name == "span1"
    assert len(spans[0].events) == 1
    assert "event1" in [e.name for e in spans[0].events]
    assert spans[0].status.status_code == StatusCode.OK
    assert spans[1].name == "span2"
    assert len(spans[1].events) == 2
    assert "event2.1" in [e.name for e in spans[1].events]
    assert "event2.2" in [e.name for e in spans[1].events]
    assert spans[1].status.status_code == StatusCode.OK


def test_using_trace_default_exception(captrace):
    with using_trace("test_using_trace_noerror", "span1") as span:
        pass
    with using_trace("test_using_trace_default_exception", "span2") as span:
        span.add_event("event1")
        raise Exception("test exception")
    spans = captrace.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].status.status_code == StatusCode.OK
    assert spans[1].status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in spans[1].events)


def test_using_trace_expected_exception(captrace):
    with using_trace("test_using_trace_noerror", "span1") as span:
        pass
    with using_trace(
        "test_using_trace_default_exception",
        "span2",
        exception_types=(ZeroDivisionError,),
    ) as span:
        span.add_event("event1")
        _ = 1 / 0

    spans = captrace.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].status.status_code == StatusCode.OK
    assert spans[1].status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in spans[1].events)


def test_using_trace_unexpected_exception(captrace):
    with pytest.raises(AssertionError):
        with using_trace("test_using_trace_noerror", "span1") as span:
            pass
        with using_trace(
            "test_using_trace_default_exception",
            "span2",
            exception_types=(ZeroDivisionError,),
        ) as span:
            span.add_event("event1")
            assert False

    spans = captrace.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].status.status_code == StatusCode.OK
    assert spans[1].status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in spans[1].events)


def test_using_trace_noerror_nested_1_tracer(captrace):
    # test nested spans in the same tracer
    with using_trace("test_using_trace_nested_spans", "span1") as span:
        span.add_event("event1.1")
        with using_trace("test_using_trace_nested_spans", "span2") as span2:
            span2.add_event("event2")
        span.add_event("event1.2")

    # check spans
    spans = captrace.get_finished_spans()
    print(f"spans: {spans}")
    assert len(spans) == 2

    assert (
        spans[0].name == "span2"
    )  # spans are in their order of ending, so span2 is first
    assert len(spans[0].events) == 1
    assert spans[0].events[0].name == "event2"
    assert spans[0].parent is not None
    assert spans[0].parent == spans[1].get_span_context()

    assert spans[1].name == "span1"
    assert len(spans[1].events) == 2
    assert "event1.1" in [e.name for e in spans[1].events]
    assert "event1.2" in [e.name for e in spans[1].events]
    assert spans[1].parent is None


def test_using_trace_noerror_nested_2_tracers(captrace):
    # test netsed spans in different tracers
    with using_trace("test_using_trace_nested_trace_1", "span1") as span:
        span.add_event("event1.1")
        with using_trace("test_using_trace_nested_trace_2", "span2") as span2:
            span2.add_event("event2")
        span.add_event("event1.2")

    # check spans
    spans = captrace.get_finished_spans()
    print(f"spans: {spans}")
    assert len(spans) == 2

    assert (
        spans[0].name == "span2"
    )  # spans are in their order of ending, so span2 is first
    assert len(spans[0].events) == 1
    assert spans[0].events[0].name == "event2"
    assert spans[0].parent is not None
    assert spans[0].parent == spans[1].get_span_context()

    assert spans[1].name == "span1"
    assert len(spans[1].events) == 2
    assert "event1.1" in [e.name for e in spans[1].events]
    assert "event1.2" in [e.name for e in spans[1].events]
    assert spans[1].parent is None


def test_using_trace_error_nested(captrace):
    # test nested spans in the same tracer
    with using_trace("test_using_trace_nested_spans", "span1") as span:
        span.add_event("event1.1")
        with using_trace(
            "test_using_trace_nested_spans",
            "span2",
            exception_types=(ZeroDivisionError,),
        ) as span2:
            span2.add_event("event2")
            _ = 1 / 0
        span.add_event("event1.2")

    # check spans
    spans = captrace.get_finished_spans()
    print(f"spans: {spans}")
    assert len(spans) == 2

    assert (
        spans[0].name == "span2"
    )  # spans are in their order of ending, so span2 is first
    assert len(spans[0].events) == 2
    assert spans[0].events[0].name == "event2"
    assert spans[0].events[1].name == "exception"
    assert spans[0].status.status_code == StatusCode.ERROR

    assert spans[1].name == "span1"
    assert len(spans[1].events) == 2
    assert "event1.1" in [e.name for e in spans[1].events]
    assert "event1.2" in [e.name for e in spans[1].events]
    assert spans[1].status.status_code == StatusCode.OK


def test_using_trace_swallowed_exception_is_logged(caplog):
    # An exception caught and swallowed by using_trace (the default) must be
    # visible in the logs, not only recorded on the span.
    with caplog.at_level("ERROR", logger="sarc.traces"):
        with using_trace("test_using_trace_logging", "span_swallowed"):
            raise ValueError("boom")

    assert "boom" in caplog.text
    assert "span_swallowed" in caplog.text


def test_using_trace_reraised_exception_not_logged(caplog):
    # Exceptions that are re-raised are left to the caller: using_trace must
    # not log them (it would double-report them up the call stack).
    with caplog.at_level("ERROR", logger="sarc.traces"):
        with pytest.raises(ValueError):
            with using_trace(
                "test_using_trace_logging",
                "span_reraised",
                exception_types=(ZeroDivisionError,),
            ):
                raise ValueError("not swallowed")

    assert "not swallowed" not in caplog.text
