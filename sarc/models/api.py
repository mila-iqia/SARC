from typing import Literal

from pydantic import BaseModel

from .job import SlurmJob
from .series import JobSeries
from .user import User


class ResultsList[T](BaseModel):
    results: list[T]
    cursor: int | str | Literal[False]


SlurmJobList = ResultsList[SlurmJob]
UserList = ResultsList[User]
JobSeriesList = ResultsList[JobSeries]
