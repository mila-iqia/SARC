import pytest
from opentelemetry.trace import StatusCode

from sarc.traces import trace_decorator


def fn_no_error(val):
    return val


def fn_with_error():
    raise ValueError("An error")


def test_decorator_fn_no_error(captrace):
    trace_decorator()(fn_no_error)(0)
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "fn_no_error"
    assert spans[0].status.status_code == StatusCode.OK


def test_decorator_fn_no_error_renamed(captrace):
    trace_decorator(span_name="function without error")(fn_no_error)(0)
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "function without error"
    assert spans[0].status.status_code == StatusCode.OK


def test_decorator_fn_with_error(captrace):
    with pytest.raises(ValueError):
        trace_decorator()(fn_with_error)()
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "fn_with_error"
    assert spans[0].status.status_code == StatusCode.ERROR


def test_decorator_fn_with_error_renamed(captrace):
    with pytest.raises(ValueError):
        trace_decorator(span_name="function with value error")(fn_with_error)()
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "function with value error"
    assert spans[0].status.status_code == StatusCode.ERROR


def test_decorator_fn_with_error_captured(captrace):
    trace_decorator(exception_types=[ValueError])(fn_with_error)()
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "fn_with_error"
    assert spans[0].status.status_code == StatusCode.ERROR


def test_decorator_fn_with_error_renamed_captured(captrace):
    trace_decorator(
        span_name="function with value error", exception_types=[ValueError]
    )(fn_with_error)()
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "function with value error"
    assert spans[0].status.status_code == StatusCode.ERROR
