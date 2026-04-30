from datetime import datetime

from pydantic import BaseModel

from .job import SlurmJob
from .user import User


class SlurmJobOutput(SlurmJob):
    id: int


class UserOutput(User):
    id: int


class SlurmJobList(BaseModel):
    jobs: list[SlurmJobOutput]
    page: int | None
    last_id: int
    last_time: datetime
    per_page: int
    total: int


class UserList(BaseModel):
    users: list[UserOutput]
    page: int | None
    last_id: int
    per_page: int
    total: int
