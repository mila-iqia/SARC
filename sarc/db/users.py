import logging
from datetime import UTC, datetime
from typing import Any, Self, Type

from sqlalchemy.dialects.postgresql import TSTZRANGE, ExcludeConstraint, Range
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Session as SASession
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import Field, Index, Relationship, Session, SQLModel, or_, select

from sarc.core.models.users import MemberType
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger(__name__)


def subtract_ranges(r1: Range[datetime], r2: Range[datetime]) -> list[Range[datetime]]:
    """Manually subtract r2 from r1, returning a list of 0, 1, or 2 Ranges."""
    if not r1.overlaps(r2):
        return [r1]

    res = []
    # 1. Check if there's a part of r1 before r2 starts
    if r1.lower_inf or (not r2.lower_inf and r1.lower < r2.lower):  # type: ignore[operator]
        res.append(Range(r1.lower, r2.lower, bounds="[)"))

    # 2. Check if there's a part of r1 after r2 ends
    if r1.upper_inf or (not r2.upper_inf and r1.upper > r2.upper):  # type: ignore[operator]
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
    user_id: int = Field(foreign_key="users.id", ondelete="CASCADE")
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
        return self.model_ref(user_id=self.id, valid=valid, **data_arg)

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

    def _insert_tag(
        self,
        session: SASession,
        value: V,
        valid: Range[datetime],
        truncate: bool = False,
    ) -> None:
        with session.begin():
            records = session.execute(
                self._select_base().where(
                    or_(
                        self.model_ref.valid.overlaps(valid),
                        self.model_ref.valid.adjacent_to(valid),
                    )
                )
            ).all()
            to_merge = [r for r in records if getattr(r, self.col_ref) == value]
            to_conflict = [
                r
                for r in records
                if getattr(r, self.col_ref) != value and r.valid.overlaps(valid)
            ]

            final_range = valid
            for record in to_merge:
                final_range = record.valid.union(final_range)
                session.delete(record)

            to_insert = [final_range]

            for record in to_conflict:
                new_insert = []
                for r_incoming in to_insert:
                    if not r_incoming.overlaps(record.valid):
                        new_insert.append(r_incoming)
                        continue

                    if truncate:
                        new_insert.extend(subtract_ranges(r_incoming, record.valid))
                    elif record.valid.upper_inf and valid.not_extend_right_of(
                        record.valid
                    ):
                        record.valid = Range(
                            record.valid.lower, valid.lower, bounds="[)"
                        )
                        session.add(record)
                        new_insert.append(valid)
                    else:
                        raise DateOverlapError(
                            value, valid, getattr(record, self.col_ref), record.valid
                        )

                to_insert = new_insert

            for final_valid in to_insert:
                session.add(self._create_record(valid=final_valid, value=value))

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
            return getattr(result, self.col_ref)

    def values_in_range(
        self, start: datetime_utc, end: datetime_utc, session: SASession | None = None
    ) -> list[V]:
        """Get values in a range

        The range starts at `start` and ends just before `end`.  This means that
        start is included, but end is not.
        """
        if session is None:
            session = self.session

        assert session is not None
        return [
            getattr(r, self.col_ref)
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
                session, getattr(record, self.col_ref), record.valid, truncate=truncate
            )


class CredentialsDB(ValidDB, table=True):
    __table_args__ = (
        ExcludeConstraint(
            ("user_id", "="), ("domain", "="), ("valid", "&&"), using="gist"
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
    def __init__(self, session: SASession | None, id: int):
        self.session = session
        self.user_id = id

    def __getitem__(self, key: str):
        assert isinstance(key, str)
        return CredentialsValid(session=self.session, id=self.user_id, domain=key)


class MemberTypeDB(ValidDB, table=True):
    member_type: MemberType


class SupervisorDB(ValidDB, table=True):
    supervisor: int = Field(foreign_key="users.id", ondelete="RESTRICT")


class CoSupervisorsHelper(SQLModel, table=True):
    id: int = Field(primary_key=True)
    list_id: int = Field(
        foreign_key="user_co_supervisors.id", index=True, ondelete="CASCADE"
    )
    co_supervisor: int = Field(foreign_key="users.id", ondelete="RESTRICT")


class CoSupervisorDB(ValidDB, table=True):
    __tablename__ = "user_co_supervisors"
    co_supervisors: set[CoSupervisorsHelper] = Relationship(
        sa_relationship=relationship(CoSupervisorsHelper, collection_class=set)
    )


class GithubUsernameDB(ValidDB, table=True):
    username: str


class GoogleScholarDB(ValidDB, table=True):
    profile_id: str


class MatchingID(SQLModel, table=True):
    __table_args__ = (
        Index("user_match_id_idx", "user_id", "plugin_name", unique=True),
    )
    id: int | None = Field(default=None, primary_key=True)
    # This can't really be None, but it needs to be for a small period of time due to SQLAlchemy magic
    user_id: int | None = Field(
        default=None, foreign_key="users.id", ondelete="CASCADE", nullable=False
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
            MatchingID, collection_class=attribute_keyed_dict("plugin_name")
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

    # Below is the tracked data for a user. Each field or value tracks changes
    # and validity periods. Insert new values with field.insert(value, [start,
    # end]) and get the values with .get_value([date]). Do not modify the values
    # in the fields without going through those methods. See the ValidField
    # documentation for more details.

    # this is per domain (i.e. "drac"), not per cluster
    @property
    def associated_accounts(self) -> CredentialsDict:
        return CredentialsDict(Session.object_session(self), self.id)

    @property
    def member_type(self) -> ValidField[MemberType]:
        return ValidField(
            Session.object_session(self), MemberTypeDB, "member_type", self.id
        )

    @property
    def supervisor(self) -> ValidField[int]:
        return ValidField(
            Session.object_session(self), SupervisorDB, "supervisor", self.id
        )

    @property
    def co_supervisor(self) -> ValidField[list[int]]:
        return ValidField(
            Session.object_session(self), CoSupervisorDB, "co_supervisors", self.id
        )

    @property
    def github_username(self) -> ValidField[str]:
        return ValidField(
            Session.object_session(self), GithubUsernameDB, "username", self.id
        )

    @property
    def google_scholar_profile(self) -> ValidField[str]:
        return ValidField(
            Session.object_session(self), GoogleScholarDB, "profile_id", self.id
        )


"""
class UserRepository(AbstractRepository[UserDB]):
    class Meta:
        collection_name = "users"

    def update_user(self, user: UserMatch) -> None:
        results = list(
            self.find_by(
                {f"matching_ids.{user.matching_id.name}": user.matching_id.mid}
            )
        )
        if len(results) == 0:
            return self._insert_new(user)
        elif len(results) == 1:
            return self._merge_and_update(results[0], user)
        else:
            db_merged = results[0]
            for db_user_extra in results[1:]:
                db_merged = self._combine_users(db_merged, db_user_extra)
                # Even if the merge fails for some attributes, we have the data
                # to recover missing info in the cache files.
                self.delete_by_id(db_user_extra.id)
            return self._merge_and_update(db_merged, user)

    def _lookup_matching_id(self, mid: MatchID) -> UserDB | None:
        result = list(self.find_by({f"matching_ids.{mid.name}": mid.mid}))
        if len(result) == 0:
            return None
        elif len(result) > 1:
            logger.error(
                "Multiple matching users in DB for match id (%s), selecting the first one",
                mid,
            )
        return result[0]

    def _update_supervisors(self, db_user: UserDB, user: UserMatch) -> None:
        for val in user.supervisor.values:
            db_val = self._lookup_matching_id(val.value)
            if db_val is None:
                logger.warning(
                    "supervisor (%s) not in db for user %s", val.value, db_user.uuid
                )
            else:
                db_user.supervisor.insert(
                    db_val.uuid, start=val.valid_start, end=val.valid_end
                )
        for cval in user.co_supervisors.values:
            db_set: set[UUID4] = set()
            for cmid in cval.value:
                db_val = self._lookup_matching_id(cmid)
                if db_val is None:
                    logger.warning(
                        "co_supervisor (%s) not in db for user %s", cmid, db_user.uuid
                    )
                else:
                    db_set.add(db_val.uuid)
            db_user.co_supervisors.insert(
                db_set, start=cval.valid_start, end=cval.valid_end
            )

    def _insert_new(self, user: UserMatch) -> None:
        if user.display_name is None or user.email is None:
            logger.error(
                "Attempting to add a new user with missing attributes: %s", user
            )
            return
        db_user = UserDB(
            display_name=user.display_name,
            email=user.email,
            matching_ids={},
            member_type=user.member_type,
            associated_accounts=user.associated_accounts,
            github_username=user.github_username,
            google_scholar_profile=user.google_scholar_profile,
        )
        for mid in user.known_matches:
            db_user.matching_ids[mid.name] = mid.mid
        db_user.matching_ids[user.matching_id.name] = user.matching_id.mid
        self._update_supervisors(db_user, user)
        self.save(db_user)

    def _merge_and_update(self, db_user: UserDB, user: UserMatch) -> None:
        if user.display_name is not None:
            db_user.display_name = user.display_name
        if user.email is not None:
            db_user.email = user.email
        try:
            db_user.member_type.merge_with(user.member_type)
        except DateOverlapError as e:
            logger.error(
                "Can't update member_type for user %s, date overlap error: %s",
                db_user.uuid,
                e,
            )
        try:
            db_user.github_username.merge_with(user.github_username)
        except DateOverlapError as e:
            logger.error(
                "Can't update github_username for user %s, date overlap error: %s",
                db_user.uuid,
                e,
            )
        try:
            db_user.google_scholar_profile.merge_with(user.google_scholar_profile)
        except DateOverlapError as e:
            logger.error(
                "Can't update google_scholar_profile for user %s, date overlap error: %s",
                db_user.uuid,
                e,
            )
        for name, creds in user.associated_accounts.items():
            if name in db_user.associated_accounts:
                try:
                    db_user.associated_accounts[name].merge_with(creds)
                except DateOverlapError as e:
                    logger.error(
                        "Can't update google_scholar_profile for user %s, date overlap error: %s",
                        db_user.uuid,
                        e,
                    )
            else:
                db_user.associated_accounts[name] = creds
        self._update_supervisors(db_user, user)
        for mid in user.known_matches:
            if mid.name not in db_user.matching_ids:
                db_user.matching_ids[mid.name] = mid.mid
            elif db_user.matching_ids[mid.name] != mid.mid:
                logger.error(
                    "User %s has matching id (%s:%s) but update has (%s:%s), using update",
                    db_user.uuid,
                    mid.name,
                    db_user.matching_ids[mid.name],
                    mid.name,
                    mid.mid,
                )
                db_user.matching_ids[mid.name] = mid.mid
        self.save(db_user)

    def _combine_users(self, db_user1: UserDB, db_user2: UserDB) -> UserDB:
        # Merge db_user2 into db_user1

        # we prefer the name from db_user1
        if db_user2.display_name != db_user1.display_name:
            logger.warning(
                "Merging user %s into user %s and their display_name differs (%s vs %s), %s is picked",
                db_user2.uuid,
                db_user1.uuid,
                db_user2.display_name,
                db_user1.display_name,
                db_user1.display_name,
            )
        # we ignore email as it's of no consequence.

        try:
            db_user1.member_type.merge_with(db_user2.member_type)
        except DateOverlapError as e:
            logger.error(
                "Can't update member_type for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        try:
            db_user1.github_username.merge_with(db_user2.github_username)
        except DateOverlapError as e:
            logger.error(
                "Can't update github_username for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        try:
            db_user1.google_scholar_profile.merge_with(db_user2.google_scholar_profile)
        except DateOverlapError as e:
            logger.error(
                "Can't update google_scholar_profile for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        for name, creds in db_user2.associated_accounts.items():
            if name in db_user1.associated_accounts:
                try:
                    db_user1.associated_accounts[name].merge_with(creds)
                except DateOverlapError as e:
                    logger.error(
                        "Can't update credentials for user %s, domain %s, date overlap error: %s",
                        db_user1.uuid,
                        name,
                        e,
                    )
            else:
                db_user1.associated_accounts[name] = creds

        try:
            db_user1.supervisor.merge_with(db_user2.supervisor)
        except DateOverlapError as e:
            logger.error(
                "Can't update supervisor for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        try:
            db_user1.co_supervisors.merge_with(db_user2.co_supervisors)
        except DateOverlapError as e:
            logger.error(
                "Can't update co_supervisors for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        # Merge matching_ids - prefer values from db_user1 if there's a conflict
        for name, mid in db_user2.matching_ids.items():
            if name not in db_user1.matching_ids:
                db_user1.matching_ids[name] = mid
            elif db_user1.matching_ids[name] != mid:
                logger.warning(
                    "User %s has matching id (%s:%s) but db_user2 has %s, using db_user1 value",
                    db_user1.uuid,
                    name,
                    db_user1.matching_ids[name],
                    mid,
                )

        return db_user1
"""
