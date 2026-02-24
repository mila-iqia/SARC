from pydantic import BaseModel

from sarc.client.job import SlurmJob

from .users import UserData

MAX_PAGE_SIZE = 5_000


class SlurmJobList(BaseModel):
    jobs: list[SlurmJob]
    page: int
    per_page: int
    total: int


class UserList(BaseModel):
    users: list[UserData]
    page: int
    per_page: int
    total: int
