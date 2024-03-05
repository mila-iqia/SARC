from contextlib import contextmanager

from opentelemetry.trace import Status, StatusCode, get_tracer


# context manager to manage traces & span,
# and catch exceptions (in span) without breaking the whole program execution
@contextmanager
def using_trace(tracer_name: str, span_name: str):
    tracer = get_tracer(tracer_name)
    with tracer.start_as_current_span(span_name) as span:
        try:
            yield span
            span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(exc)
        finally:
            # nothing to do, end or close...
            pass
