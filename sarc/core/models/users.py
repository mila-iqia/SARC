from enum import Enum
from uuid import uuid4

from pydantic import UUID4, BaseModel, Field

from .validators import ValidField


class Credentials(ValidField[str]):
    pass


class MemberType(str, Enum):
    MASTER_STUDENT = "master"
    PHD_STUDENT = "phd"
    POSTDOC = "postdoc"
    PROFESSOR = "professor"
    STAFF = "staff"
    INTERN = "intern"
    # There are probably some missing types so feel free to add them


class UserData(BaseModel):
    # This is the base data for a user. Except for the uuid, it can change but
    # we are not interested in tracking past values for this.
    uuid: UUID4 = Field(default_factory=uuid4, frozen=True)
    display_name: str
    email: str

    # Each user plugin can specify a matching ID which will be stored here.
    matching_ids: dict[str, str]

    # Below is the tracked data for a user. Each field or value tracks changes
    # and validity periods. Insert new values with field.insert(value, [start,
    # end]) and get the values with .get_value([date]). Do not modify the values
    # in the fields without going through those methods. See the ValidField
    # documentation for more details.

    member_type: ValidField[MemberType] = Field(default_factory=ValidField[MemberType])

    # this is per domain (i.e. "drac"), not per cluster
    associated_accounts: dict[str, Credentials] = Field(default_factory=dict)

    supervisor: ValidField[UUID4] = Field(default_factory=ValidField[UUID4])
    co_supervisors: ValidField[set[UUID4]] = Field(
        default_factory=ValidField[set[UUID4]]
    )

    github_username: ValidField[str] = Field(default_factory=ValidField[str])
    google_scholar_profile: ValidField[str] = Field(default_factory=ValidField[str])
