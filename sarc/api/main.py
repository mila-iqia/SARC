from fastapi import FastAPI

from ..config import config
from .v0 import router as v0_router

app = FastAPI()

app.include_router(v0_router)
api_config = config().api
assert api_config is not None
assert api_config.auth is not None
api_config.auth.install(app)
