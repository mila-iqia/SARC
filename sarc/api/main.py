from fastapi import FastAPI

from .v0 import router as v0_router
from ..config import config

app = FastAPI()

app.include_router(v0_router)
if auth := config().api.auth:
    auth.install(app)
