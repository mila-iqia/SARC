from collections.abc import Iterable
from importlib.metadata import entry_points
from typing import Any, Protocol, Type

from pydantic import BaseModel
from serieux import deserialize

from sarc.core.models.users import Credentials
from sarc.core.models.validators import datetime_utc


# Any value set to None is considered to mean "unknown"
class UserMatch(BaseModel):
    display_name: str | None = None
    email: str | None = None

    # this is per domain, not per cluster
    associated_accounts: dict[str, list[Credentials]]

    # The strings must be matching_ids from the plugin
    supervisor: str | None
    co_supervisors: list[str] | None

    # when set by the plugin it indicates the start and end of the validity of the data
    record_start: datetime_utc | None
    record_end: datetime_utc | None

    matching_id: str
    # If the plugins gets an id that works with another plugin, it can be stored here.
    known_matches: dict[str, str]


# plugins are run in the order they are defined in the config file and the first plugin to define a value wins.
class UserScraper[T](Protocol):
    config_type: Type[T]

    def validate_config(self, config_data: Any) -> T:
        return deserialize(self.config_type, config_data)

    def get_user_data(self, config: T) -> str: ...

    def update_user_data(self, config: T, data: str) -> Iterable[UserMatch]: ...


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


def _fix_ids(scraper_name: str, userm: UserMatch) -> None:
    """
    Fix ups ids by prepending the scraper_name to them to make them unique
    """
    userm.matching_id = f"{scraper_name}:{userm.matching_id}"
    if userm.supervisor is not None:
        userm.supervisor = f"{scraper_name}:{userm.supervisor}"
    if userm.co_supervisors is not None:
        userm.co_supervisors = [
            f"{scraper_name}:{co_sup}" for co_sup in userm.co_supervisors
        ]


def _find_pos(scraper_names: Iterable[str], matching_id: str) -> int:
    """
    Find the position of a scraper in the scraper list based on a matching_id.
    """
    scraper_name = matching_id.split(":", 1)[0]
    for i, name in enumerate(scraper_names):
        if name == scraper_name:
            return i
    raise ValueError("Invalid inputs: scraper not in list")


def update_user_match(*, value: UserMatch, update: UserMatch) -> None:
    """
    Fills in any missing information in value with the data in update.
    """
    if value.display_name is None:
        value.display_name = update.display_name

    if value.email is None:
        value.email = update.email

    if value.supervisor is None:
        value.supervisor = update.supervisor

    if value.co_supervisors is None:
        value.co_supervisors = update.co_supervisors
    elif update.co_supervisors is not None:
        value.co_supervisors.extend(update.co_supervisors)

    if value.record_start is None:
        value.record_start = update.record_start

    if value.record_end is None:
        value.record_end = update.record_end

    for domain, credentials in update.associated_accounts.items():
        if domain not in value.associated_accounts:
            value.associated_accounts[domain] = credentials

    # Add the matching id of the new usermatch to make sure that we have all the ids that this user is known under.
    name, id = update.matching_id.split(":", maxsplit=1)
    if name in value.known_matches:
        assert value.known_matches[name] == id
    else:
        value.known_matches[name] = id


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
    user_refs: dict[str, UserMatch] = {}
    for scraper_name, (rdata, config) in raw_data.items():
        for userm in scraper.update_user_data(config, rdata):
            _fix_ids(scraper_name, userm)
            prev_userm = user_refs.get(userm.matching_id, None)
            if prev_userm is None:
                user_refs[userm.matching_id] = userm
            else:
                update_user_match(value=prev_userm, update=userm)
                user_refs[userm.matching_id] = userm
                userm = prev_userm
            for name, id in userm.known_matches.items():
                key = f"{name}:{id}"
                old_userm = user_refs.get(key, None)
                if old_userm is None:
                    user_refs[key] = userm
                elif prev_userm is None:
                    # If there was no match using the main matching ID, but we found one with the alternates, treat that as the main one.
                    update_user_match(value=old_userm, update=userm)
                    user_refs[userm.matching_id] = old_userm
                    prev_userm = old_userm
                    userm = old_userm
                elif prev_userm is old_userm:
                    # If we just found another name for the same entry, then we don't need to do anything
                    # The update was already done on prev_userm before.
                    pass
                else:
                    # We found two matches that are not the same entry.  Order them by plugin name according to the input list and merge them
                    old_pos = _find_pos(
                        (name for name, _ in scrapers), old_userm.matching_id
                    )
                    prev_pos = _find_pos(
                        (name for name, _ in scrapers), prev_userm.matching_id
                    )
                    if old_pos < prev_pos:
                        update_user_match(value=old_userm, update=prev_userm)
                        final_userm = old_userm
                    else:
                        update_user_match(value=prev_userm, update=old_userm)
                        final_userm = prev_userm
                    # Update all references to the same UserMatch struct
                    for name, id in final_userm.known_matches.items():
                        user_refs[f"{name}:{id}"] = final_userm

    for id, umatch in user_refs.items():
        if umatch.matching_id != id:
            # We only want to handle "primary" entries to avoid duplication
            continue
        yield umatch
