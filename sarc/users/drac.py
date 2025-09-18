import csv
import datetime
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from sarc.core.models.users import Credentials
from sarc.core.scraping.users import MatchID, UserMatch, UserScraper, _builtin_scrapers


@dataclass
class DRACRolesConfig:
    csv_path: Path


def _dict_to_lowercase[T](D: dict[str, T]) -> dict[str, T]:
    return dict((k.lower(), v) for (k, v) in D.items())


class DRACRolesScraper(UserScraper[DRACRolesConfig]):
    config_type = DRACRolesConfig

    def get_user_data(self, config: DRACRolesConfig) -> bytes:
        with open(config.csv_path, "r", encoding="utf-8") as f_in:
            return json.dumps(
                [_dict_to_lowercase(d) for d in csv.DictReader(f_in)]
            ).encode()

    def parse_user_data(
        self, _config: DRACRolesConfig, data: bytes
    ) -> Iterable[UserMatch]:
        for d in json.loads(data.decode()):
            yield UserMatch(
                display_name=d["nom"],
                email=d["email"],
                matching_id=MatchID(name="drac_role", mid=d["ccri"][:-3]),
            )


_builtin_scrapers["drac_role"] = DRACRolesScraper()


@dataclass
class DRACMemberConfig:
    csv_path: Path


class DRACMemberScraper(UserScraper[DRACMemberConfig]):
    config_type = DRACMemberConfig

    def get_user_data(self, config: DRACMemberConfig) -> bytes:
        with open(config.csv_path, "r", encoding="utf-8") as f_in:
            return json.dumps(
                [_dict_to_lowercase(d) for d in csv.DictReader(f_in)]
            ).encode()

    def parse_user_data(
        self, _config: DRACMemberConfig, data: bytes
    ) -> Iterable[UserMatch]:
        for d in json.loads(data.decode()):
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
