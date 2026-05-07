from datetime import datetime

from pydantic import BaseModel

from .job import SlurmJob
from .user import User


class SlurmJobList(BaseModel):
    jobs: list[SlurmJob]
    page: int | None
    last_id: int | None
    last_time: datetime | None
    per_page: int
    total: int


class UserList(BaseModel):
    users: list[User]
    page: int | None
    last_id: int | None
    per_page: int
    total: int
