from typing import Annotated

from client.users.api import User
from fastapi import Depends, FastAPI

from .auth import get_user

app = FastAPI()


@app.get("/test")
def test(user: Annotated[User, Depends(get_user)]):
    return f"Hello, {user.mila.username}"
