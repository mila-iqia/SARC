from pydantic import BaseModel

from sarc.db.job import SlurmJobDB
from sarc.db.users import UserDB


# TODO: rebuild the models so that they don't use database impls
class SlurmJobList(BaseModel):
    jobs: list[SlurmJobDB]
    page: int
    per_page: int
    total: int


class UserList(BaseModel):
    users: list[UserDB]
    page: int
    per_page: int
    total: int
