from fastapi import FastAPI

from sarc.logging import tracing_enabled

from ..config import config
from .metrics import router as metrics_router
from .v0 import router as v0_router

app = FastAPI()

app.include_router(v0_router)
app.include_router(metrics_router)
auth_config = config.server.auth
if auth_config is not None:
    auth_config.install(app)

if tracing_enabled():
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)
