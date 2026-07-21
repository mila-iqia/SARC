import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Self, Type

from sqlalchemy.dialects.postgresql import TSTZRANGE, ExcludeConstraint, Range
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Session as SASession
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import (
    Field,
    Index,
    Relationship,
    Session,
    UniqueConstraint,
    col,
    exists,
    or_,
    select,
    update,
)

from sarc.models.user import MemberType
from sarc.traces import trace_decorator
from sarc.validators import datetime_utc

from .sqlmodel import SQLModel

logger = logging.getLogger(__name__)


def subtract_ranges(r1: Range[datetime], r2: Range[datetime]) -> list[Range[datetime]]:
    """Manually subtract r2 from r1, returning a list of 0, 1, or 2 Ranges."""
    if not r1.overlaps(r2):
        return [r1]

    res = []
    # 1. Check if there's a part of r1 before r2 starts
    if not r1.not_extend_left_of(r2) and r1.lower != r2.lower:
        res.append(Range(r1.lower, r2.lower, bounds="[)"))

    # 2. Check if there's a part of r1 after r2 ends
    if not r1.not_extend_right_of(r2) and r2.upper != r1.upper:
        res.append(Range(r2.upper, r1.upper, bounds="[)"))

    # Filter out any accidentally created empty ranges
    return [r for r in res if not r.isempty]


class DateOverlapError(Exception):
    def __init__(self, value1: Any, range1: Range, value2: Any, range2: Range):
        super().__init__(
            f"""Overlapping validity with different values:
{value1}: {range1.lower} - {range1.upper}
{value2}: {range2.lower} - {range2.upper}"""
        )


class DateMatchError(Exception):
    def __init__(self, date: datetime):
        super().__init__(f"No valid value for date: {date}")


class ValidDB(SQLModel):
    __table_args__ = (
        ExcludeConstraint(("user_id", "="), ("valid", "&&"), using="gist"),
    )
    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="users.id", ondelete="CASCADE", index=True)
    valid: Range[datetime_utc] = Field(sa_type=TSTZRANGE)


class ValidField[V]:
    def __init__(
        self, session: SASession | None, model_ref: Type[ValidDB], col_ref: str, id: int
    ):
        self.session = session
        self.model_ref = model_ref
        self.col_ref = col_ref
        self.id = id

    def _select_base(self):
        return select(self.model_ref).where(self.model_ref.user_id == self.id)

    def _create_record(self, valid: Range[datetime], value: V):
        data_arg = {self.col_ref: value}
        return self.model_ref(user_id=self.id, valid=valid, **data_arg)  # ty:ignore[invalid-argument-type]

    def insert(
        self,
        value: V,
        start: datetime | None = None,
        end: datetime | None = None,
        session: SASession | None = None,
    ) -> None:
        """Add a value with optional validity bounds.

        The validity bounds may not overlap with a different value. If the
        values are the same (checked with ==) and the bounds overlap, the
        entries will be merged otherwise a DateOverlapError is raised.

        As a special case, if you add a value that starts later than all
        exisiting values and the end bound of the most recent value is None (aka infinite),
        instead of an overlapping error, the bound of the most recent value will
        be adjusted to end at the start of the new value.
        """
        if session is None:
            session = self.session
        assert session is not None
        if start is not None:
            assert start.tzinfo is not None
            start = start.astimezone(UTC)
        if end is not None:
            assert end.tzinfo is not None
            end = end.astimezone(UTC)

        self._insert_tag(session, value, Range(start, end, bounds="[)"))
        session.flush()

    def _insert_tag(
        self,
        session: SASession,
        value: V,
        valid: Range[datetime],
        truncate: bool = False,
    ) -> None:
        records = session.execute(
            self._select_base().where(
                or_(
                    self.model_ref.valid.overlaps(valid),
                    self.model_ref.valid.adjacent_to(valid),
                )
            )
        ).all()
        to_merge = [r for r in records if getattr(r[0], self.col_ref) == value]
        to_conflict = [
            r
            for r in records
            if getattr(r[0], self.col_ref) != value and r[0].valid.overlaps(valid)
        ]

        if valid.upper_inf:
            # Only clip against conflicts that start strictly after the input's start;
            # conflicts starting before are handled in the loop below by clipping the DB record.
            later_conflicts = [
                r
                for r in to_conflict
                if valid._compare_edges(
                    r[0].valid.lower, r[0].valid.bounds[0], valid.lower, valid.bounds[0]
                )
                == 1
            ]
            if later_conflicts:
                valid = Range(
                    valid.lower,
                    min(record[0].valid.lower for record in later_conflicts),
                    bounds="[)",
                )

        if any(record[0].valid.contains(valid) for record in to_merge):
            # There is already a record in the DB that covers this valid range with this value, so nothing to do
            return

        final_range = valid
        for record in to_merge:
            final_range = record[0].valid.union(final_range)
            session.delete(record[0])
        session.flush()

        to_insert: list[Range[datetime]] = [final_range]

        for record in to_conflict:
            new_insert: list[Range[datetime]] = []
            for r_incoming in to_insert:
                if not r_incoming.overlaps(record[0].valid):
                    new_insert.append(r_incoming)
                    continue

                if truncate:
                    new_insert.extend(subtract_ranges(r_incoming, record[0].valid))
                elif (
                    record[0].valid.upper_inf
                    and r_incoming._compare_edges(
                        r_incoming.lower,
                        r_incoming.bounds[0],
                        record[0].valid.lower,
                        record[0].valid.bounds[0],
                    )
                    > 0
                ):
                    # insert new record AFTER existing one
                    record[0].valid = Range(
                        record[0].valid.lower, r_incoming.lower, bounds="[)"
                    )
                    session.flush()
                    new_insert.append(r_incoming)
                elif (
                    record[0].valid.lower_inf
                    and r_incoming._compare_edges(
                        r_incoming.upper,
                        r_incoming.bounds[1],
                        record[0].valid.upper,
                        record[0].valid.bounds[1],
                    )
                    < 0
                ):
                    # insert new record BEFORE existing one
                    record[0].valid = Range(
                        r_incoming.upper, record[0].valid.upper, bounds="[)"
                    )
                    session.flush()
                    new_insert.append(r_incoming)
                else:
                    raise DateOverlapError(
                        value, valid, getattr(record[0], self.col_ref), record[0].valid
                    )

            to_insert = new_insert

        for final_valid in to_insert:
            session.add(self._create_record(valid=final_valid, value=value))
        session.flush()

    def get_value(
        self, date: datetime | None = None, session: SASession | None = None
    ) -> V:
        """Get the valid value at specified time.

        If date is None, we use the current time as the date.
        If there was no value as the specified date, raises DateMatchError.
        """
        if session is None:
            session = self.session

        assert session is not None

        assert date is None or date.tzinfo is not None

        if date is None:
            date = datetime.now(UTC)

        date = date.astimezone(UTC)

        result = session.execute(
            self._select_base().where(self.model_ref.valid.contains(date))
        ).one_or_none()

        if result is None:
            raise DateMatchError(date)
        else:
            return getattr(result[0], self.col_ref)

    def values_in_range(
        self,
        start: datetime_utc | None,
        end: datetime_utc | None,
        session: SASession | None = None,
    ) -> list[V]:
        """Get values in a range

        The range starts at `start` and ends just before `end`.  This means that
        start is included, but end is not.
        """
        if session is None:
            session = self.session

        assert session is not None
        return [
            getattr(r[0], self.col_ref)
            for r in session.execute(
                self._select_base().where(
                    self.model_ref.valid.overlaps(Range(start, end, bounds="[)"))
                )
            ).all()
        ]

    def merge_with(
        self, other: Self, truncate=False, session: SASession | None = None
    ) -> None:
        """Insert all the values of other in self.

        If truncate=True, inserted values will have their validity periods
        reduced to fit around existing values.
        """
        if session is None:
            session = self.session

        assert session is not None

        for record in session.execute(other._select_base()):
            self._insert_tag(
                session,
                getattr(record[0], self.col_ref),
                record[0].valid,
                truncate=truncate,
            )


class CredentialsDB(ValidDB, table=True):
    __table_args__ = (
        ExcludeConstraint(
            ("user_id", "="), ("domain", "="), ("valid", "&&"), using="gist"
        ),
        ExcludeConstraint(
            ("domain", "="), ("username", "="), ("valid", "&&"), using="gist"
        ),
    )
    domain: str
    username: str


class CredentialsValid(ValidField[str]):
    def __init__(self, session: SASession | None, id: int, domain: str):
        super().__init__(session, CredentialsDB, "username", id)
        self.domain = domain

    def _select_base(self):
        return super()._select_base().where(CredentialsDB.domain == self.domain)

    def _create_record(self, valid: Range[datetime], value: str):
        return CredentialsDB(
            user_id=self.id, valid=valid, domain=self.domain, username=value
        )


class CredentialsDict:
    def __init__(self, session: SASession, id: int):
        self.session = session
        self.user_id = id

    def __getitem__(self, key: str) -> CredentialsValid:
        assert isinstance(key, str)
        return CredentialsValid(session=self.session, id=self.user_id, domain=key)

    def items(self) -> Iterable[tuple[str, CredentialsValid]]:
        domains = self.session.execute(
            select(CredentialsDB.domain)
            .where(CredentialsDB.user_id == self.user_id)
            .distinct()
        )
        for domain in domains:
            yield (
                domain[0],
                CredentialsValid(
                    session=self.session, id=self.user_id, domain=domain[0]
                ),
            )

    def __contains__(self, key: str) -> bool:
        return self.session.execute(
            select(
                exists().where(
                    col(CredentialsDB.user_id) == self.user_id,
                    col(CredentialsDB.domain) == key,
                )
            )
        ).one()[0]


class MemberTypeDB(ValidDB, table=True):
    member_type: MemberType


class SupervisorsHelper(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("list_id", "pos"),)
    id: int | None = Field(default=None, primary_key=True)
    list_id: int | None = Field(
        default=None,
        nullable=False,
        foreign_key="user_supervisors.id",
        index=True,
        ondelete="CASCADE",
    )
    pos: int = Field(ge=0)
    supervisor: int = Field(foreign_key="users.id", ondelete="RESTRICT")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SupervisorsHelper):
            return False
        return self.pos == other.pos and self.supervisor == other.supervisor

    def __hash__(self) -> int:
        return hash((self.pos, self.supervisor))


class SupervisorsDB(ValidDB, table=True):
    __tablename__ = "user_supervisors"
    supervisors: list[SupervisorsHelper] = Relationship(
        passive_deletes="all",
        sa_relationship_kwargs={"order_by": SupervisorsHelper.pos},
    )


class SupervisorIDsField:
    """Adapter over ValidField[list[SupervisorsHelper]] that exposes list[int] (supervisor IDs ordered by pos)."""

    def __init__(self, field: ValidField[list[SupervisorsHelper]]):
        self._field = field

    @staticmethod
    def _extract(helpers: list[SupervisorsHelper]) -> list[int]:
        return [h.supervisor for h in helpers]

    def get_value(
        self, date: datetime | None = None, session: SASession | None = None
    ) -> list[int]:
        return self._extract(self._field.get_value(date, session))

    def values_in_range(
        self, start: datetime_utc, end: datetime_utc, session: SASession | None = None
    ) -> list[list[int]]:
        return [
            self._extract(hs) for hs in self._field.values_in_range(start, end, session)
        ]

    def insert(
        self,
        value: list[int],
        start: datetime | None = None,
        end: datetime | None = None,
        session: SASession | None = None,
    ) -> None:
        helpers = [
            SupervisorsHelper(pos=i, supervisor=sid) for i, sid in enumerate(value)
        ]
        self._field.insert(helpers, start, end, session)


class MatchingID(SQLModel, table=True):
    __table_args__ = (
        Index("user_match_id_idx", "user_id", "plugin_name", unique=True),
        UniqueConstraint("plugin_name", "match_id"),
    )
    id: int | None = Field(default=None, primary_key=True)
    # This can't really be None, but it needs to be for a small period of time due to SQLAlchemy magic
    user_id: int | None = Field(
        default=None,
        foreign_key="users.id",
        ondelete="CASCADE",
        nullable=False,
        index=True,
    )
    plugin_name: str
    match_id: str


class UserDB(SQLModel, table=True):
    __tablename__ = "users"

    id: int | None = Field(default=None, primary_key=True)
    display_name: str
    email: str

    _match_ids: dict[str, MatchingID] = Relationship(
        sa_relationship=relationship(
            MatchingID,
            collection_class=attribute_keyed_dict("plugin_name"),
            passive_deletes="all",
        )
    )
    _match_ids_dict: AssociationProxy[dict[str, str]] = association_proxy(
        "_match_ids",
        "match_id",
        creator=lambda k, v: MatchingID(plugin_name=k, match_id=v),
    )

    # Each user plugin can specify a matching ID which will be stored here.
    @property
    def matching_ids(self) -> dict[str, str]:
        return self._match_ids_dict

    @property
    def _session(self) -> SASession:
        sess = Session.object_session(self)
        assert sess is not None
        return sess

    # Below is the tracked data for a user. Each field or value tracks changes
    # and validity periods. Insert new values with field.insert(value, [start,
    # end]) and get the values with .get_value([date]). Do not modify the values
    # in the fields without going through those methods. See the ValidField
    # documentation for more details.

    # this is per domain (i.e. "drac"), not per cluster
    @property
    def associated_accounts(self) -> CredentialsDict:
        assert self.id is not None
        return CredentialsDict(self._session, self.id)

    @property
    def member_type(self) -> ValidField[MemberType]:
        assert self.id is not None
        return ValidField(
            Session.object_session(self), MemberTypeDB, "member_type", self.id
        )

    @property
    def _supervisors(self) -> ValidField[list[SupervisorsHelper]]:
        assert self.id is not None
        return ValidField(
            Session.object_session(self), SupervisorsDB, "supervisors", self.id
        )

    # The first element of the list is the principal supervisor, the rest are co-supervisors.
    # Also this is essentially ValidField[list[int]], but with consistent ordering
    @property
    def supervisors(self) -> SupervisorIDsField:
        return SupervisorIDsField(self._supervisors)

    @classmethod
    def by_email(cls, sess: Session, email: str) -> Self | None:
        return sess.exec(select(cls).where(cls.email == email)).one_or_none()


@trace_decorator()
def get_user_id_for_cluster_user(
    sess: Session, cluster_id: int, user: str, submit_time: datetime
) -> int | None:
    from .cluster import SlurmClusterDB

    cluster = sess.get(SlurmClusterDB, cluster_id)
    assert cluster is not None
    return sess.exec(
        select(CredentialsDB.user_id).where(
            CredentialsDB.domain == cluster.domain,
            CredentialsDB.username == user,
            CredentialsDB.valid.contains(submit_time),
        )
    ).one_or_none()


# TODO: find out how it is used and if we can push the filtering down to the DB
def get_users(sess: Session) -> Sequence[UserDB]:
    return sess.exec(select(UserDB)).all()


@trace_decorator()
def merge_users(sess: Session, db_user1: UserDB, db_user2: UserDB) -> None:
    from .job import SlurmJobDB
    # Merge db_user2 into db_user1

    # we prefer attributes from db_user1
    if db_user2.display_name != db_user1.display_name:
        logger.warning(
            "Merging user %s into user %s and their display_name differs (%s vs %s), %s is picked",
            db_user2.id,
            db_user1.id,
            db_user2.display_name,
            db_user1.display_name,
            db_user1.display_name,
        )
    db_user1.member_type.merge_with(db_user2.member_type)
    db_user1._supervisors.merge_with(db_user2._supervisors)

    db2_matching_ids = db_user2.matching_ids.copy()

    credentials = dict()
    for name, creds in db_user2.associated_accounts.items():
        credentials[name] = [
            (entry.username, entry.valid.lower, entry.valid.upper)
            for entry in sess.exec(creds._select_base()).all()
        ]

    with sess.no_autoflush:
        sess.exec(
            update(SupervisorsHelper)
            .where(col(SupervisorsHelper.supervisor) == db_user2.id)
            .values(supervisor=db_user1.id)
        )
        sess.exec(
            update(SlurmJobDB)
            .where(col(SlurmJobDB.sarc_user_id) == db_user2.id)
            .values(sarc_user_id=db_user1.id)
        )
        sess.delete(db_user2)
    sess.flush()

    # This is done after the delete since otherwise there can be DB conflicts
    for name, mid in db2_matching_ids.items():
        if name not in db_user1.matching_ids:
            db_user1.matching_ids[name] = mid
        elif db_user1.matching_ids[name] != mid:
            logger.warning(
                "db_user1 %s has matching id (%s:%s) but db_user2 has %s, using db_user1 value",
                db_user1.id,
                name,
                db_user1.matching_ids[name],
                mid,
            )
    for domain, entries in credentials.items():
        for entry in entries:
            db_user1.associated_accounts[domain].insert(*entry)
    sess.flush()
