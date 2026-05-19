import csv
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from serieux.features.encrypt import Secret
from sqlmodel import func, select

from sarc.config import config
from sarc.db.users import CredentialsDB
from sarc.scraping.users import (
    Credentials,
    MatchID,
    UserMatch,
    UserScraper,
    _builtin_scrapers,
)


def _dict_to_lowercase[T](D: dict[str, T]) -> dict[str, T]:
    return dict((k.lower(), v) for (k, v) in D.items())


@dataclass
class DRACMemberConfig:
    csv: Secret[str]
    csv_date: datetime


class DRACMemberScraper(UserScraper[DRACMemberConfig]):
    config_type = DRACMemberConfig

    def get_user_data(self, config: DRACMemberConfig) -> bytes:
        return json.dumps(
            {"csv": config.csv, "csv_date": config.csv_date.isoformat()}
        ).encode("utf-8")

    def parse_user_data(self, data: bytes, cache_time: datetime) -> Iterable[UserMatch]:  # noqa: ARG002
        try:
            j = json.loads(data.decode("utf-8"))
            # new cache format, data is a JSON containing a dict {csv, csv_date}
            yield from parse_csv(j["csv"], j["csv_date"])

        except json.JSONDecodeError:
            # old cache, data is the CSV
            yield from parse_csv(data.decode("utf-8"), None)


def parse_csv(csv_file: str, csv_date: datetime | None) -> Iterable[UserMatch]:

    with config().db.session() as sess:
        for d in csv.DictReader(csv_file.split("\n")):
            d = _dict_to_lowercase(d)
            creds = Credentials()
            if d["activation_status"] in ["activated", "recently_renewed"]:
                creds.insert(
                    d["username"],
                    start=datetime.strptime(d["member_since"], "%Y-%m-%d %H:%M:%S %z"),
                )
            elif (
                csv_date is not None
                and sess.exec(
                    select(
                        select(CredentialsDB)
                        .where(
                            CredentialsDB.domain == "drac",
                            CredentialsDB.username == d["username"],
                            func.upper_inf(CredentialsDB.valid),
                        )
                        .exists()
                    )
                ).one()
            ):
                creds.insert(
                    d["username"],
                    start=datetime.strptime(d["member_since"], "%Y-%m-%d %H:%M:%S %z"),
                    end=csv_date,
                )
            if creds.values != []:
                yield UserMatch(
                    display_name=d["name"],
                    email=d["email"],
                    matching_id=MatchID(name="drac_member", mid=d["ccri"][:-3]),
                    associated_accounts={"drac": creds},
                )


_builtin_scrapers["drac_member"] = DRACMemberScraper()
