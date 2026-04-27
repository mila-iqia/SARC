import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from importlib.metadata import entry_points
from typing import Any, Callable, Protocol, Type

from pydantic import BaseModel, Field, field_serializer
from serieux import IncludeFile, Serieux, WorkingDirectory
from serieux.features.encrypt import EncryptionKey
from sqlmodel import Session, select

from sarc.cache import Cache, CacheEntry
from sarc.config import config_path
from sarc.core.models.users import Credentials, MemberType
from sarc.core.models.validators import ValidField
from sarc.db.users import MatchingID, UserDB
from sarc.db.users import ValidField as ValidFieldDB

deserialize = (Serieux + IncludeFile)().deserialize  # type: ignore[operator]

logger = logging.getLogger(__name__)


class MatchID(BaseModel):
    name: str
    mid: str

    def __hash__(self) -> int:
        return hash((self.name, self.mid))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, MatchID)
            and self.name == other.name
            and self.mid == other.mid
        )


# Any value set to None is considered to mean "unknown"
class UserMatch(BaseModel):
    display_name: str | None = None
    email: str | None = None

    matching_id: MatchID
    # If the plugin gets an id that works with another plugin, it can be stored here.
    known_matches: set[MatchID] = Field(default_factory=set)

    member_type: ValidField[MemberType] = Field(default_factory=ValidField[MemberType])
    # this is per domain, not per cluster
    associated_accounts: dict[str, Credentials] = Field(default_factory=dict)

    # The strings must be matching_ids from the plugin
    supervisor: ValidField[MatchID] = Field(default_factory=ValidField[MatchID])
    co_supervisors: ValidField[set[MatchID]] = Field(
        default_factory=ValidField[set[MatchID]]
    )

    github_username: ValidField[str] = Field(default_factory=ValidField[str])
    google_scholar_profile: ValidField[str] = Field(default_factory=ValidField[str])

    # This is not really required for serialization, but it makes the order or
    # known_matches random if not present and complicates testing. If it becomes
    # a problem outside of tests we can find another solution.
    @field_serializer("known_matches", when_used="json")
    def _serialize_deterministic(self, value: set[MatchID]):
        return sorted(value, key=lambda m: (m.name, m.mid))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, UserMatch) and self.matching_id == other.matching_id

    def __hash__(self) -> int:
        return hash(self.matching_id)


# plugins are run in the order they are defined in the config file and the first plugin to define a value wins.
class UserScraper[T](Protocol):
    config_type: Type[T]

    def validate_config(self, config_data: Any) -> T:
        return deserialize(
            self.config_type,
            config_data,
            WorkingDirectory(directory=config_path)  # type: ignore[call-arg, operator]
            + EncryptionKey(password=os.environ.get("SERIEUX_PASSWORD", None)),  # type: ignore[call-arg]
        )

    def get_user_data(self, config: T) -> bytes: ...  # pragma: nocover

    def parse_user_data(
        self, data: bytes, cache_time: datetime
    ) -> Iterable[UserMatch]: ...  # pragma: nocover


_builtin_scrapers: dict[str, UserScraper] = dict()
_user_scrapers = entry_points(group="sarc.user_scraper")


def get_user_scraper(name: str) -> UserScraper:
    """Raises KeyError if the name is not found"""
    try:
        return _builtin_scrapers[name]
    except KeyError:
        pass
    val = _user_scrapers[name]
    return val.load()()


def update_user_match(*, value: UserMatch, update: UserMatch) -> None:
    """
    Fills in any missing information in value with the data in update.
    """
    if value.display_name is None:
        value.display_name = update.display_name

    if value.email is None:
        value.email = update.email

    name_dict = {mid.name: mid for mid in value.known_matches}

    # Add the matching ids of the new usermatch to make sure that we have all
    # the ids that this user is known under.
    assert (
        name_dict.get(update.matching_id.name, update.matching_id) == update.matching_id
    )
    value.known_matches.add(update.matching_id)
    for mid in update.known_matches:
        assert name_dict.get(mid.name, mid) == mid, f"{name_dict}: {mid}"
        value.known_matches.add(mid)

    value.member_type.merge_with(update.member_type, truncate=True)

    for domain, credentials in update.associated_accounts.items():
        if domain not in value.associated_accounts:
            value.associated_accounts[domain] = credentials
        else:
            value.associated_accounts[domain].merge_with(credentials, truncate=True)

    value.supervisor.merge_with(update.supervisor, truncate=True)
    value.co_supervisors.merge_with(update.co_supervisors, truncate=True)

    value.github_username.merge_with(update.github_username, truncate=True)
    value.google_scholar_profile.merge_with(
        update.google_scholar_profile, truncate=True
    )


def fetch_users(scrapers: list[tuple[str, Any]]) -> None:
    """Fetch user data and place the results in cache.

    This method should never raise any exceptions, but will instead log all
    execution errors.

    The goal is to make sure that all scrapers have a chance to run and that
    temporary or permanent errors will not discard any previously retrieved
    data.
    """
    cache = Cache(subdirectory="users")
    with cache.create_entry(datetime.now(UTC)) as ce:
        for scraper_name, config_data in scrapers:
            try:
                scraper = get_user_scraper(scraper_name)
            except Exception as e:
                logger.error(
                    "Could not fetch user scraper: %s", scraper_name, exc_info=e
                )
                continue
            try:
                config = scraper.validate_config(config_data)
            except Exception as e:
                logger.error(
                    "Error parsing config for scraper: %s", scraper_name, exc_info=e
                )
                continue
            try:
                ce.add_value(key=scraper_name, value=scraper.get_user_data(config))
            except Exception as e:
                logger.error(
                    "Error fetching data for scraper: %s", scraper_name, exc_info=e
                )


def parse_users(from_: datetime) -> Iterable[CacheEntry]:
    """Parse user data from the cache.

    This returns one UserMatch structure per scraped user, across all plugins.
    The collected information is aggregated amongst plugins, but not with the
    information in the database.

    from_: start parsing cached date from that date. If None, uses runstate value from database
    update_parsed_date : to update runstate last parsed date value
    """
    cache = Cache(subdirectory="users")

    return cache.read_from(from_time=from_)


def parse_ce(ce: CacheEntry) -> Iterable[UserMatch]:
    # UserMatches, referenced by matching id
    user_refs: dict[MatchID, UserMatch] = {}
    # Used for getting results precedence.
    scraper_names = [it[0] for it in ce.items()]
    for item in ce.items():
        try:
            scraper = get_user_scraper(item[0])
        except KeyError as e:
            raise ValueError("Invalid user scraper") from e
        for userm in scraper.parse_user_data(item[1], ce.entry_datetime):
            userm.matching_id.name = item[0]
            # First, get all the userm that match with this one.
            prev_userms: list[UserMatch] = [userm]
            prev = user_refs.get(userm.matching_id, None)
            if prev is not None:
                prev_userms.append(prev)
            for mid in userm.known_matches:
                prev = user_refs.get(mid, None)
                if prev is not None:
                    prev_userms.append(prev)
            # Second, filter out duplicates and sort the rest according to plugin rank
            matching_userms = sorted(
                set(prev_userms),
                key=lambda um: scraper_names.index(um.matching_id.name),
            )
            # Third, merge everything into the oldest entry
            oldest_userm = matching_userms.pop(0)
            for newer_userm in matching_userms:
                update_user_match(value=oldest_userm, update=newer_userm)
            # Finally, update all references to point to the new merged UserMatch
            user_refs[oldest_userm.matching_id] = oldest_userm
            for mid in oldest_userm.known_matches:
                user_refs[mid] = oldest_userm

    def _get_refs(um: UserMatch) -> set[MatchID]:
        res = set[MatchID]()
        for tag in um.supervisor.values:
            res.add(tag.value)
        for tags in um.co_supervisors.values:
            res.update(tags.value)
        return res

    # Filter for "primary" UserMatches (those whose reference name match the
    # original plugin name).
    refs = {k: _get_refs(v) for k, v in user_refs.items() if v.matching_id == k}

    # Here we do a topological sort of the usermatches to ensure that the
    # supervisors are yielded first and make it to the database before their
    # students.
    while len(refs) != 0:
        roots = set()
        for k, v in refs.items():
            if len(v) == 0:
                yield user_refs[k]
                roots.add(k)
        refs = {k: v - roots for k, v in refs.items() if k not in roots}
        if len(roots) == 0:
            for k in refs:
                yield user_refs[k]


def lookup_match_id(sess: Session, match_id: MatchID) -> UserDB | None:
    return sess.exec(
        select(UserDB)
        .join(MatchingID)
        .where(
            MatchingID.plugin_name == match_id.name, MatchingID.match_id == match_id.mid
        )
    ).all()


def update_user(sess: Session, user: UserMatch) -> None:
    results = sess.exec(
        select(MatchingID).where(
            MatchingID.plugin_name == user.matching_id.name,
            MatchingID.match_id == user.matching_id.mid,
        )
    ).all()
    if len(results) == 0:
        with sess.begin():
            insert_new(sess, user)
            sess.commit()
    elif len(results) >= 1:
        db_user = sess.get(UserDB, results[0].user_id)
        if user.display_name is not None:
            db_user.display_name = user.display_name
        if user.email is not None:
            db_user.email = user.email
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
        update_user_db(user, db_user)


def valid_merge[T, U](
    valid: ValidField[T],
    db_valid: ValidFieldDB[T],
    *,
    map: Callable[[T], U] = lambda v: v,
) -> None:
    for tag in valid.values:
        db_valid.insert(map(tag.value), tag.valid_start, tag.valid_end)


def update_user_db(user: UserMatch, db_user: UserDB) -> None:
    for domain, creds in user.associated_accounts.item():
        valid_merge(creds, db_user.associated_accounts[domain])
    valid_merge(user.member_type, db_user.member_type)
    valid_merge(user.github_username, db_user.github_username)
    valid_merge(user.google_scholar_profile, db_user.google_scholar_profile)

    def map_super(match_id: MatchID) -> int:
        res = lookup_match_id(match_id)
        if len(res) == 0:
            raise ValueError("Supervisor (%s) not found in database")
        else:
            if len(res) > 1:
                logger.error(
                    "Multiple matching users in DB for match id (%s), selecting the first one",
                    match_id,
                )
            return res[0]

    valid_merge(user.supervisor, db_user.supervisor, map=map_super)
    valid_merge(
        user.co_supervisors,
        db_user.co_supervisor,
        map=lambda v: sorted(map_super(m) for m in v),
    )


def insert_new(sess: Session, user: UserMatch) -> None:
    if user.display_name is None or user.email is None:
        logger.error("Attempting to add a new user with missing attributes: %s", user)
        return
    db_user = UserDB(display_name=user.display_name, email=user.email)
    sess.add(db_user)
    for match_id in user.known_matches:
        db_user.matching_ids[match_id.name] = match_id.mid
    db_user.matching_ids[user.matching_id.name] = user.matching_id.mid
    update_user_db(user, db_user)
