from collections.abc import Sequence
from typing import Any, Self

from serieux import deserialize, TaggedSubclass, serialize
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Index, Session, select

from .sqlmodel import SQLModel
from ..alerts.common import HealthCheck, CheckResult
from ..alerts.healthcheck_state import HealthCheckState


class HealthCheckStateDB(SQLModel, table=True):
    """State of a health check stored in MongoDB.

    This is used by the polling system to track when checks were last run
    and what their status was, enabling scheduled execution without a
    permanent daemon process.

    Combine:
    - a HealthCheck configuration object,
    - the last check result
    - the last result message
    """

    __table_args__ = (Index("idx_healthcheck_name", "name"),)

    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    name: str = Field(unique=True, nullable=False)

    # Check configuration
    # With annotated class for custom serialization
    check_dict: dict[str, Any] = Field(sa_type=JSONB)

    # Last check result (None if never run)
    # With annotated class for custom serialization
    # Contains `status` and `issue_date` (last run time)
    last_result_dict: dict[str, Any] | None = Field(sa_type=JSONB)

    # Optional summary message (e.g., error description)
    last_message: str | None = None

    @property
    def check(self) -> HealthCheck:
        return deserialize(TaggedSubclass[HealthCheck], self.check_dict)

    @check.setter
    def check(self, hc: HealthCheck):
        self.check_dict = serialize(TaggedSubclass[HealthCheck], hc)

    @property
    def last_result(self) -> CheckResult | None:
        return (
            None
            if self.last_result_dict is None
            else deserialize(TaggedSubclass[CheckResult], self.last_result_dict)
        )

    @last_result.setter
    def last_result(self, rc: CheckResult):
        self.last_result_dict = serialize(TaggedSubclass[CheckResult], rc)

    @classmethod
    def get_state(cls, sess: Session, name: str) -> Self | None:
        """Get the state for a specific check by name."""
        return sess.exec(select(cls).where(cls.name == name)).one_or_none()

    @classmethod
    def get_states(cls, sess: Session) -> Sequence[HealthCheckStateDB]:
        """Get an iterable of all states saved in database."""
        return sess.exec(select(cls)).all()

    @classmethod
    def get_or_create(cls, sess: Session, state: HealthCheckState) -> Self:
        state_dict = state.model_dump()
        if state_dict["last_result"]:
            state_dict["last_result"].pop("check", None)

        state_dict["name"] = state_dict["check"]["name"]
        state_dict["check_dict"] = state_dict.pop("check")
        state_dict["last_result_dict"] = state_dict.pop("last_result")

        res = cls.model_validate(state_dict)
        res.id = sess.exec(select(cls.id).where(cls.name == res.name)).one_or_none()
        return sess.merge(res)
