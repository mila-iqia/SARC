# test the traces helper context manager

import pytest
from opentelemetry.trace import Status, StatusCode, get_tracer

from sarc.traces import using_trace


def test_using_trace_noerror(captrace):
    with using_trace("test_using_trace_noerror", "span1") as span:
        span.add_event("event1")
    with using_trace("test_using_trace_noerror", "span2") as span:
        span.add_event("event2.1")
        span.add_event("event2.2")

    # check traces
    traces = captrace.get_finished_spans()
    assert len(traces) == 2
    assert traces[0].name == "span1"
    assert len(traces[0].events) == 1
    assert "event1" in [e.name for e in traces[0].events]
    assert traces[0].status.status_code == StatusCode.OK
    assert traces[1].name == "span2"
    assert len(traces[1].events) == 2
    assert "event2.1" in [e.name for e in traces[1].events]
    assert "event2.2" in [e.name for e in traces[1].events]
    assert traces[1].status.status_code == StatusCode.OK


def test_using_trace_default_exception(captrace):
    with using_trace("test_using_trace_noerror", "span1") as span:
        pass
    with using_trace("test_using_trace_default_exception", "span2") as span:
        span.add_event("event1")
        raise Exception("test exception")
    traces = captrace.get_finished_spans()
    assert len(traces) == 2
    assert traces[0].status.status_code == StatusCode.OK
    assert traces[1].status.status_code == StatusCode.ERROR


def test_using_trace_expected_exception(captrace):
    exception_caught = False
    with using_trace("test_using_trace_noerror", "span1") as span:
        pass
    with using_trace(
        "test_using_trace_default_exception",
        "span2",
        exception_types=[ZeroDivisionError],
    ) as span:
        span.add_event("event1")
        a = 1 / 0

    traces = captrace.get_finished_spans()
    assert len(traces) == 2
    assert traces[0].status.status_code == StatusCode.OK
    assert traces[1].status.status_code == StatusCode.ERROR


def test_using_trace_unexpected_exception(captrace):
    exception_caught = False
    with pytest.raises(AssertionError):
        with using_trace("test_using_trace_noerror", "span1") as span:
            pass
        with using_trace(
            "test_using_trace_default_exception",
            "span2",
            exception_types=[ZeroDivisionError],
        ) as span:
            span.add_event("event1")
            assert False

    traces = captrace.get_finished_spans()
    assert len(traces) == 2
    assert traces[0].status.status_code == StatusCode.OK
    assert traces[1].status.status_code == StatusCode.ERROR


def test_using_trace_noerror_nested_1_tracer(captrace):
    # test nested spans in the same tracer
    with using_trace("test_using_trace_nested_spans", "span1") as span:
        span.add_event("event1.1")
        with using_trace("test_using_trace_nested_spans", "span2") as span2:
            span2.add_event("event2")
        span.add_event("event1.2")

    # check traces
    traces = captrace.get_finished_spans()
    print(f"traces: {traces}")
    assert len(traces) == 2

    assert (
        traces[0].name == "span2"
    )  # spans are in their order of ending, so span2 is first
    assert len(traces[0].events) == 1
    assert traces[0].events[0].name == "event2"

    assert traces[1].name == "span1"
    assert len(traces[1].events) == 2
    assert "event1.1" in [e.name for e in traces[1].events]
    assert "event1.2" in [e.name for e in traces[1].events]


def test_using_trace_noerror_nested_2_tracers(captrace):
    # test netsed spans in different tracers
    with using_trace("test_using_trace_nested_trace_1", "span1") as span:
        span.add_event("event1.1")
        with using_trace("test_using_trace_nested_trace_2", "span2") as span2:
            span2.add_event("event2")
        span.add_event("event1.2")

    # check traces
    traces = captrace.get_finished_spans()
    print(f"traces: {traces}")
    assert len(traces) == 2

    assert (
        traces[0].name == "span2"
    )  # spans are in their order of ending, so span2 is first
    assert len(traces[0].events) == 1
    assert traces[0].events[0].name == "event2"

    assert traces[1].name == "span1"
    assert len(traces[1].events) == 2
    assert "event1.1" in [e.name for e in traces[1].events]
    assert "event1.2" in [e.name for e in traces[1].events]


def test_using_trace_error_nested(captrace):
    # test nested spans in the same tracer
    with using_trace("test_using_trace_nested_spans", "span1") as span:
        span.add_event("event1.1")
        with using_trace(
            "test_using_trace_nested_spans",
            "span2",
            exception_types=[ZeroDivisionError],
        ) as span2:
            span2.add_event("event2")
            a = 1 / 0
        span.add_event("event1.2")

    # check traces
    traces = captrace.get_finished_spans()
    print(f"traces: {traces}")
    assert len(traces) == 2

    assert (
        traces[0].name == "span2"
    )  # spans are in their order of ending, so span2 is first
    assert len(traces[0].events) == 2
    assert traces[0].events[0].name == "event2"
    assert traces[0].events[1].name == "exception"
    assert traces[0].status.status_code == StatusCode.ERROR

    assert traces[1].name == "span1"
    assert len(traces[1].events) == 2
    assert "event1.1" in [e.name for e in traces[1].events]
    assert "event1.2" in [e.name for e in traces[1].events]
    assert traces[1].status.status_code == StatusCode.OK
