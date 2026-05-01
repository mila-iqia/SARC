from datetime import datetime

from pydantic import BaseModel

from .job import SlurmJobBase
from .user import UserBase


class SlurmJob(SlurmJobBase):
    id: int


class User(UserBase):
    id: int


class SlurmJobList(BaseModel):
    jobs: list[SlurmJob]
    page: int | None
    last_id: int
    last_time: datetime
    per_page: int
    total: int


class UserList(BaseModel):
    users: list[User]
    page: int | None
    last_id: int
    per_page: int
    total: int
