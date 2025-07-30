from enum import Enum
from uuid import uuid4

from pydantic import UUID4, BaseModel, Field

from .validators import ValidField


# Can the username change (DRAC: no, mila: )
class Credentials(ValidField[str]):
    pass


# do we want to do this?
class AffiliationType(Enum):
    MASTER_STUDENT = "master"
    PHD_STUDENT = "phd"
    PROFESSOR = "prof"
    STAFF = "staff"
    INTERN = "intern"


class _Affiliation(BaseModel):
    university: str
    type: AffiliationType
    departement: str


class Affiliations(ValidField[_Affiliation]):
    pass


class UserData(BaseModel):
    uuid: UUID4 = Field(default_factory=lambda: uuid4())
    display_name: str
    email: str

    connection_id: str
    connection_type: str

    # this is per domain, not per cluster
    associated_accounts: dict[str, Credentials]

    affiliations: Affiliations

    supervisor: UUID4 | None
    co_supervisors: list[UUID4] | None

    github_username: str | None
    google_scholar_profile: str | None

    # Each user plugin can specify a matching ID which will be stored here.
    matching_ids: dict[str, str]

    # voir avec Xavier pour ça
    # teacher_delegation
