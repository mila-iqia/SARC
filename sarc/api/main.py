from fastapi import FastAPI

from .v0 import router as v0_router

app = FastAPI()

app.include_router(v0_router)
