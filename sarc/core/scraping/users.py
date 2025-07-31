from collections.abc import Iterable
from importlib.metadata import entry_points
from typing import Any, Protocol, Type

from pydantic import BaseModel
from serieux import deserialize

from sarc.core.models.users import Credentials
from sarc.core.models.validators import ValidField


# Any value set to None is considered to mean "unknown"
class UserMatch(BaseModel):
    display_name: str | None = None
    email: str | None = None

    original_plugin: str
    matching_id: str
    # If the plugins gets an id that works with another plugin, it can be stored here.
    known_matches: dict[str, str]

    member_type: ValidField[str] | None
    # this is per domain, not per cluster
    associated_accounts: dict[str, Credentials]

    # The strings must be matching_ids from the plugin
    supervisor: ValidField[str] | None
    co_supervisors: ValidField[list[str]] | None

    github_username: ValidField[str] | None
    google_scholar_profile: ValidField[str] | None


# plugins are run in the order they are defined in the config file and the first plugin to define a value wins.
class UserScraper[T](Protocol):
    config_type: Type[T]

    def validate_config(self, config_data: Any) -> T:
        return deserialize(self.config_type, config_data)

    def get_user_data(self, config: T) -> str: ...

    def parse_user_data(self, config: T, data: str) -> Iterable[UserMatch]: ...


_builtin_scrapers: dict[str, UserScraper] = dict()
_diskusage_scrapers = entry_points(group="sarc.user_scraper")


def get_user_scraper(name: str) -> UserScraper:
    """Raises KeyError if the name is not found"""
    try:
        return _builtin_scrapers[name]
    except KeyError:
        pass
    val = _diskusage_scrapers[name]
    return val.load()


def update_user_match(*, value: UserMatch, update: UserMatch) -> None:
    """
    Fills in any missing information in value with the data in update.
    """
    if value.display_name is None:
        value.display_name = update.display_name

    if value.email is None:
        value.email = update.email

    # Add the matching ids of the new usermatch to make sure that we have all the ids that this user is known under.
    name, id = update.matching_id.split(":", maxsplit=1)
    if name in value.known_matches:
        assert value.known_matches[name] == id
    else:
        value.known_matches[name] = id

    # TODO: complete this.

    if value.supervisor is None:
        value.supervisor = update.supervisor

    if value.co_supervisors is None:
        value.co_supervisors = update.co_supervisors
    elif update.co_supervisors is not None:
        value.co_supervisors.merge_with(update.co_supervisors)

    for domain, credentials in update.associated_accounts.items():
        if domain not in value.associated_accounts:
            value.associated_accounts[domain] = credentials
        else:
            value.associated_accounts[domain].merge_with(credentials)


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
        for userm in scraper.update_user_data(config, rdata):
            userm.original_plugin = scraper_name
            key = (userm.original_plugin, userm.matching_id)
            prev_userm = user_refs.get(key, None)
            if prev_userm is None:
                user_refs[key] = userm
            else:
                update_user_match(value=prev_userm, update=userm)
                userm = prev_userm
            for name, id in userm.known_matches.items():
                key = (name, id)
                old_userm = user_refs.get(key, None)
                if old_userm is None:
                    user_refs[key] = userm
                elif prev_userm is None:
                    # If there was no match using the main matching ID, but we found one with the alternates, treat that as the main one.
                    update_user_match(value=old_userm, update=userm)
                    user_refs[(userm.original_plugin, userm.matching_id)] = old_userm
                    prev_userm = old_userm
                    userm = old_userm
                elif prev_userm is old_userm:
                    # If we just found another name for the same entry, then we don't need to do anything
                    # The update was already done on prev_userm before.
                    pass
                else:
                    # We found two matches that are not the same entry.  Order them by plugin name according to the input list and merge them
                    scraper_names = list(name for name, _ in scrapers)
                    old_pos = scraper_names.index(old_userm.original_plugin)
                    prev_pos = scraper_names.index(prev_userm.original_plugin)
                    if old_pos < prev_pos:
                        update_user_match(value=old_userm, update=prev_userm)
                        final_userm = old_userm
                    else:
                        update_user_match(value=prev_userm, update=old_userm)
                        final_userm = prev_userm
                    # Update all references to the same UserMatch struct
                    old_userm = final_userm
                    prev_userm = final_userm
                    userm = final_userm
                    for name, id in final_userm.known_matches.items():
                        user_refs[(name, id)] = final_userm

    for (name, id), umatch in user_refs.items():
        if umatch.original_plugin != name:
            # We only want to handle "primary" entries to avoid duplication
            continue
        yield umatch
