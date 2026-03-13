import csv
import datetime
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from serieux.features.encrypt import Secret

from sarc.core.models.users import Credentials
from sarc.core.scraping.users import MatchID, UserMatch, UserScraper, _builtin_scrapers


@dataclass
class DRACRolesConfig:
    csv: Secret[str]


def _dict_to_lowercase[T](D: dict[str, T]) -> dict[str, T]:
    return dict((k.lower(), v) for (k, v) in D.items())


class DRACRolesScraper(UserScraper[DRACRolesConfig]):
    config_type = DRACRolesConfig

    def get_user_data(self, config: DRACRolesConfig) -> bytes:
        return config.csv.encode("utf-8")

    def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
        for d in csv.DictReader(data.decode("utf-8")):
            d = _dict_to_lowercase(d)
            yield UserMatch(
                display_name=d["nom"],
                email=d["email"],
                matching_id=MatchID(name="drac_role", mid=d["ccri"][:-3]),
            )


_builtin_scrapers["drac_role"] = DRACRolesScraper()


@dataclass
class DRACMemberConfig:
    csv: Secret[str]


class DRACMemberScraper(UserScraper[DRACMemberConfig]):
    config_type = DRACMemberConfig

    def get_user_data(self, config: DRACMemberConfig) -> bytes:
        return config.csv.encode("utf-8")

    def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
        for d in csv.DictReader(data.decode("utf-8")):
            d = _dict_to_lowercase(d)
            creds = Credentials()
            creds.insert(
                d["username"],
                start=datetime.datetime.strptime(
                    d["member_since"], "%Y-%m-%d %H:%M:%S %z"
                ),
            )
            yield UserMatch(
                display_name=d["name"],
                email=d["email"],
                matching_id=MatchID(name="drac_member", mid=d["ccri"][:-3]),
                associated_accounts={"drac": creds},
            )


_builtin_scrapers["drac_member"] = DRACMemberScraper()
