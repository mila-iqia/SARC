from fastapi import FastAPI

from ..config import config
from .v0 import router as v0_router

app = FastAPI()

app.include_router(v0_router)
auth_config = config().server.auth
if auth_config is not None:
    auth_config.install(app)
