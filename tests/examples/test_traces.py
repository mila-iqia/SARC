from opentelemetry.trace import Status, StatusCode, get_tracer


def test_trace1(captrace):
    tracer = get_tracer("test_trace1")

    # This is fake traced work
    with tracer.start_as_current_span("span1") as _span:
        pass

    finished_spans = captrace.get_finished_spans()

    assert len(finished_spans) == 1
    span1 = finished_spans[0]
    assert span1.name == "span1"


def test_trace2(captrace):
    tracer = get_tracer("test_trace2")

    with tracer.start_as_current_span("span2") as span:
        try:
            1 / 0
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(exc)

    span_list = captrace.get_finished_spans()

    assert len(span_list) == 1
    span2 = span_list[0]
    assert span2.name == "span2"
    assert span2.status.status_code == StatusCode.ERROR
    assert len(span2.events) == 1
    ev = span2.events[0]
    assert ev.name == "exception"
    assert ev.attributes["exception.type"] == "ZeroDivisionError"
