from typing import Annotated

from fastapi import Depends, FastAPI

from sarc.client.users.api import User

from .auth import get_user
from .v1 import router as v1_router

app = FastAPI()

app.include_router(v1_router)
