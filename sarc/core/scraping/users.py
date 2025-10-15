from collections.abc import Iterable
from datetime import UTC, datetime
from importlib.metadata import entry_points
from typing import Any, Protocol, Type

from pydantic import BaseModel, Field, field_serializer
from serieux import deserialize

from sarc.cache import Cache
from sarc.core.models.users import Credentials, MemberType
from sarc.core.models.validators import ValidField


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
        return deserialize(self.config_type, config_data)

    def get_user_data(self, config: T) -> bytes: ...  # pragma: nocover

    def parse_user_data(
        self, data: bytes
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
        assert name_dict.get(mid.name, mid) == mid
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
    """Fetch user data and place the results in cache."""
    cache = Cache(subdirectory="users")
    for scraper_name, config_data in scrapers:
        try:
            scraper = get_user_scraper(scraper_name)
        except KeyError as e:
            raise ValueError("Invalid user scraper") from e
        config = scraper.validate_config(config_data)
        cache.save(
            key=scraper_name,
            at_time=datetime.now(UTC),
            value=scraper.get_user_data(config),
        )


def parse_users(from_: datetime) -> Iterable[UserMatch]:
    """Parse user data from the cache.

    This returns one UserMatch structure per scraped user, across all plugins.
    The collected information is aggregated amongst plugins, but not with the
    information in the database.

    from_: start parsing cached date from that date.
    """
    cache = Cache(subdirectory="users")
    # UserMatches, referenced by plugin name and matching id
    user_refs: dict[MatchID, UserMatch] = {}
    scrapers_dict: dict[str, tuple[UserScraper, Any]] = {}
    for scraper_name, config_data in scrapers:
        try:
            scraper = get_user_scraper(scraper_name)
        except KeyError as e:
            raise ValueError("Invalid user scraper") from e
        config = scraper.validate_config(config_data)
        scrapers_dict[scraper_name] = (scraper, config)

    for key, rdata in cache.read_from_all(from_time=from_):
        scraper, config = scrapers_dict[key]
        for userm in scraper.parse_user_data(config, rdata):
            userm.matching_id.name = scraper_name
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
            scraper_names = [name for name, _ in scrapers]
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

    # Yield all "primary" UserMatches (those whose reference name match the
    # original plugin name)
    for mid, umatch in user_refs.items():
        if umatch.matching_id != mid:
            continue
        yield umatch
