from contextlib import contextmanager

from opentelemetry.trace import Status, StatusCode, get_tracer


# context manager to manage traces & span,
# and catch exceptions (in span) without breaking the whole program execution
@contextmanager
def using_trace(
    tracer_name: str, span_name: str, exception_types: tuple = (Exception,)
):  # pylint: disable=dangerous-default-value
    """
    Context manager to manage traces & span, and catch exceptions (in span) without breaking the whole program execution.

    Parameters
    ----------
    tracer_name : str
        name of the tracer (can be shared between different spans)
    span_name : str
        name of the span
    exception_types : tuple, optional
        Types of exceptions to catch, other types will be raised.
        By default, `Exception` is caught so that nothing would be raised.

    Yields
    ------
    span : opentelemetry.trace.Span
        The span created by the context manager.
    """
    tracer = get_tracer(tracer_name)
    with tracer.start_as_current_span(span_name) as span:
        try:
            yield span
            span.set_status(Status(StatusCode.OK))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(exc)

            # re-raise the exception to be caught by the caller
            if not any(isinstance(exc, t) for t in exception_types):
                raise
        finally:
            # nothing to do, end or close...
            pass
