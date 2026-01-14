import json
from collections.abc import Iterable
from dataclasses import dataclass

from serieux import deserialize, serialize

from sarc.core.scraping.users import MatchID, UserMatch, UserScraper, _builtin_scrapers


@dataclass
class ConfigMatchID:
    name: str
    mid: str


@dataclass
class ManualUserConfig:
    id_pairs: dict[str, list[ConfigMatchID]]


class ManualUserScraper(UserScraper[ManualUserConfig]):
    """
    Add manual matches for user entries.

    This does not scrape any sources, but can manually add matches that are not
    reflected in the sources.
    """

    config_type = ManualUserConfig

    def get_user_data(self, config: ManualUserConfig) -> bytes:
        return json.dumps(serialize(ManualUserConfig, config)).encode("utf-8")

    def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
        # TODO: get the list of matches from the DB instead of the config file?
        #  - Maybe but in theory plugins should not require access to the DB
        #  - It could be another db on the side, like a SQLite or something.
        items = deserialize(ManualUserConfig, json.loads(data.decode("utf-8")))
        for name, mids in items.id_pairs.items():
            yield UserMatch(
                matching_id=MatchID(name="manual", mid=name),
                known_matches=set(MatchID(name=m.name, mid=m.mid) for m in mids),
            )


_builtin_scrapers["manual"] = ManualUserScraper()
