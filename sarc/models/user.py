from enum import Enum

from pydantic import BaseModel


class MemberType(str, Enum):
    MASTER_STUDENT = "master"
    PHD_STUDENT = "phd"
    POSTDOC = "postdoc"
    PROFESSOR = "professor"
    STAFF = "staff"
    INTERN = "intern"
    # There are probably some missing types so feel free to add them


class UserBase(BaseModel):
    display_name: str
    email: str

    # Either we add all the ValidFields here or we add methods on the API to query the values
    # and proxies in the client code
