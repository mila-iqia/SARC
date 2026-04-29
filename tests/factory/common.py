import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sarc.config import ClusterConfig
from sarc.models.user import MemberType, User


@dataclass
class UserGenerationParameters:
    # Total number of users to generate
    total: int

    # Total of full professors to start with
    full_professors: int


@dataclass
class Valid[T]:
    user_id: int
    relationship: T
    start: datetime | None
    end: datetime | None


@dataclass
class Supervision:
    supervisor_ids: list[int]


@dataclass
class Credential:
    domain: str
    username: str


@dataclass
class UserGenerationParameters:
    # Total number of users to generate
    total: int

    # Total of full professors to start with
    full_professors: int


@dataclass
class Data:
    users: list[User] = field(default_factory=list)
    memberships: list[Valid[MemberType]] = field(default_factory=list)
    supervisions: list[Valid[Supervision]] = field(default_factory=list)
    github_usernames: list[Valid[str]] = field(default_factory=list)
    google_scholar_profile: list[Valid[str]] = field(default_factory=list)
    credentials: list[Valid[Credential]] = field(default_factory=list)


@dataclass(kw_only=True)
class DataFactory:
    seed: int
    clusters: dict[str, ClusterConfig]
    t_start: date = date(2020, 1, 1)
    t_end: date = date(2025, 1, 1)
    tick: timedelta = timedelta(days=30)
    users: UserGenerationParameters

    def get_rng(self, name: str) -> random.Random:
        offset = int.from_bytes(name.encode(), "little") % (2**31)
        return random.Random(self.seed + offset)
