from collections.abc import Iterable
from importlib.metadata import entry_points
from typing import Any, Protocol, Type

from pydantic import BaseModel, Field
from serieux import deserialize

from sarc.core.models.users import Credentials, MemberType
from sarc.core.models.validators import ValidField


# Any value set to None is considered to mean "unknown"
class UserMatch(BaseModel):
    display_name: str | None = None
    email: str | None = None

    original_plugin: str
    matching_id: str
    # If the plugins gets an id that works with another plugin, it can be stored here.
    known_matches: dict[str, str] = Field(default_factory=dict)

    member_type: ValidField[MemberType] = Field(default_factory=ValidField[MemberType])
    # this is per domain, not per cluster
    associated_accounts: dict[str, Credentials]

    # The strings must be matching_ids from the plugin
    supervisor: ValidField[str] = Field(default_factory=ValidField[str])
    co_supervisors: ValidField[list[str]] = Field(default_factory=ValidField[list[str]])

    github_username: ValidField[str] = Field(default_factory=ValidField[str])
    google_scholar_profile: ValidField[str] = Field(default_factory=ValidField[str])

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, UserMatch)
            and self.original_plugin == other.original_plugin
            and self.matching_id == other.matching_id
        )

    def __hash__(self) -> int:
        return hash((self.original_plugin, self.matching_id))


# plugins are run in the order they are defined in the config file and the first plugin to define a value wins.
class UserScraper[T](Protocol):
    config_type: Type[T]

    def validate_config(self, config_data: Any) -> T:
        return deserialize(self.config_type, config_data)

    def get_user_data(self, config: T) -> str: ...

    def parse_user_data(self, config: T, data: str) -> Iterable[UserMatch]: ...


_builtin_scrapers: dict[str, UserScraper] = dict()
_user_scrapers = entry_points(group="sarc.user_scraper")


def get_user_scraper(name: str) -> UserScraper:
    """Raises KeyError if the name is not found"""
    try:
        return _builtin_scrapers[name]
    except KeyError:
        pass
    val = _user_scrapers[name]
    return val.load()


def update_user_match(*, value: UserMatch, update: UserMatch) -> None:
    """
    Fills in any missing information in value with the data in update.
    """
    if value.display_name is None:
        value.display_name = update.display_name

    if value.email is None:
        value.email = update.email

    # Add the matching ids of the new usermatch to make sure that we have all
    # the ids that this user is known under.
    assert (
        value.known_matches.get(update.original_plugin, update.matching_id)
        == update.matching_id
    )
    value.known_matches[update.original_plugin] = update.matching_id
    for name, id in update.known_matches.items():
        assert value.known_matches.get(name, id) == id
        value.known_matches[name] = id

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


def scrape_users(scrapers: list[tuple[str, Any]]) -> Iterable[UserMatch]:
    """
    Perform user scraping and matching according to the list of plugins passed in.

    The first plugin to specify information wins in case of conflict.

    This returns one UserMatch structure per scraped user, across all plugins.
    The collected information is aggregated amongst plugins, but not with the
    information in the database.
    """
    raw_data: dict[str, tuple[str, Any]] = {}
    for scraper_name, config_data in scrapers:
        try:
            scraper = get_user_scraper(scraper_name)
        except KeyError as e:
            raise ValueError("Invalid user scraper") from e
        config = scraper.validate_config(config_data)
        raw_data[scraper_name] = (scraper.get_user_data(config), config)

    # TODO: save the raw data for cache purposes

    # UserMatches, referenced by plugin name and matching id
    user_refs: dict[tuple[str, str], UserMatch] = {}
    for scraper_name, (rdata, config) in raw_data.items():
        for userm in scraper.parse_user_data(config, rdata):
            userm.original_plugin = scraper_name
            # First, get all the userm that matche with this one.
            key = (userm.original_plugin, userm.matching_id)
            prev_userms: list[UserMatch] = [userm]
            prev = user_refs.get(key, None)
            if prev is not None:
                prev_userms.append(prev)
            for name, id in userm.known_matches.items():
                key = (name, id)
                prev = user_refs.get(key, None)
                if prev is not None:
                    prev_userms.append(prev)
            # Second, filter out duplicates and sort the rest according to plugin rank
            scraper_names = [name for name, _ in scrapers]
            matching_userms = sorted(
                set(prev_userms), key=lambda um: scraper_names.index(um.original_plugin)
            )
            # Third, merge everything into the oldest entry
            oldest_userm = matching_userms.pop(0)
            for newer_userm in matching_userms:
                update_user_match(value=oldest_userm, update=newer_userm)
            # Finally, update all references to point to the new merged UserMatch
            user_refs[(oldest_userm.original_plugin, oldest_userm.matching_id)] = (
                oldest_userm
            )
            for name, id in oldest_userm.known_matches.items():
                user_refs[(name, id)] = oldest_userm

    # Yield all "primary" UserMatches (those whose reference name match the
    # original plugin name)
    for (name, id), umatch in user_refs.items():
        if umatch.original_plugin != name:
            continue
        yield umatch
