# test the traces helper context manager

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
    try:
        with using_trace("test_using_trace_noerror", "span1") as span:
            pass
        with using_trace(
            "test_using_trace_default_exception",
            "span2",
            exception_types=[ZeroDivisionError],
        ) as span:
            span.add_event("event1")
            a = 1 / 0
    except Exception:
        exception_caught = True

    assert (
        exception_caught == False
    )  # the exception should be handled by the context manager
    traces = captrace.get_finished_spans()
    assert len(traces) == 2
    assert traces[0].status.status_code == StatusCode.OK
    assert traces[1].status.status_code == StatusCode.ERROR


def test_using_trace_unexpected_exception(captrace):
    exception_caught = False
    try:
        with using_trace("test_using_trace_noerror", "span1") as span:
            pass
        with using_trace(
            "test_using_trace_default_exception",
            "span2",
            exception_types=[ZeroDivisionError],
        ) as span:
            span.add_event("event1")
            assert False
    except Exception:
        exception_caught = True

    assert (
        exception_caught == True
    )  # the ExceptionError is not handled by the context manager
    traces = captrace.get_finished_spans()
    assert len(traces) == 2
    assert traces[0].status.status_code == StatusCode.OK
    assert traces[1].status.status_code == StatusCode.ERROR
