from sarc.logging import tracing_enabled

from .app import create_app

app = create_app()

if tracing_enabled():
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)
