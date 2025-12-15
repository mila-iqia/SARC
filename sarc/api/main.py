from fastapi import FastAPI

from .auth import get_oauth
from .v0 import build as v0_build

app = FastAPI()

if (auth := get_oauth()) is not None:
    auth.install(app)

app.include_router(v0_build())
