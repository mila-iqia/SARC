from uuid import uuid4

from pydantic import UUID4, BaseModel, Field

from .validators import datetime_utc


class Credentials(BaseModel):
    username: str
    uid: int
    gid: int
    active: bool


class UserData(BaseModel):
    uuid: UUID4 = Field(default_factory=lambda: uuid4())
    display_name: str
    email: str

    connection_id: str
    connection_type: str

    # this is per domain, not per cluster
    associated_accounts: dict[str, list[Credentials]]

    supervisor: UUID4 | None
    co_supervisors: list[UUID4] | None

    # Each user plugin can specify a matching ID which will be stored here.
    matching_id: dict[str, str]

    # voir avec Xavier pour ça
    # teacher_delegation

    record_start: datetime_utc | None
    record_end: datetime_utc | None
