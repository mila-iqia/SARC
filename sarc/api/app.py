from fastapi import FastAPI

from ..config import config
from .metrics import router as metrics_router
from .v0 import router as v0_router


def create_app(with_auth: bool = True) -> FastAPI:
    """
    Build the API app.

    `with_auth=False` skips the config lookup, so the OpenAPI spec can be
    generated without a SARC_CONFIG.
    """
    app = FastAPI()
    app.include_router(v0_router)
    app.include_router(metrics_router)
    if with_auth:
        auth_config = config.server.auth
        if auth_config is not None:
            auth_config.install(app)
    return app
