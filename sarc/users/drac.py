import csv
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time

from serieux.features.encrypt import Secret
from sqlmodel import Session, func, select

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
    csv_date: date


class DRACMemberScraper(UserScraper[DRACMemberConfig]):
    config_type = DRACMemberConfig

    def get_user_data(self, config: DRACMemberConfig) -> bytes:
        return json.dumps(
            {"csv": config.csv, "csv_date": config.csv_date.isoformat()}
        ).encode("utf-8")

    def parse_user_data(self, data: bytes, cache_time: datetime) -> Iterable[UserMatch]:  # noqa: ARG002
        try:
            j = json.loads(data.decode("utf-8"))
            if isinstance(j, list):
                with config().db.session() as sess:
                    for d in j:
                        yield from parse_csv_line(d, sess, None)
            else:
                # new cache format, data is a JSON containing a dict {csv, csv_date}
                yield from parse_csv(j["csv"], date.fromisoformat(j["csv_date"]))

        except json.JSONDecodeError:
            # old cache, data is the CSV
            yield from parse_csv(data.decode("utf-8"), None)


def parse_csv(csv_file: str, csv_date: date | None) -> Iterable[UserMatch]:

    with config().db.session() as sess:
        for d in csv.DictReader(csv_file.split("\n")):
            yield from parse_csv_line(d, sess, csv_date)


def parse_csv_line(d: dict, sess: Session, csv_date: date | None):
    d = _dict_to_lowercase(d)
    if d["ccri"] == "":
        return
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
            end=datetime.combine(csv_date, time.min, tzinfo=UTC),
        )
    if creds.values != []:
        yield UserMatch(
            display_name=d["name"],
            email=d["email"],
            matching_id=MatchID(name="drac_member", mid=d["ccri"][:-3]),
            associated_accounts={"drac": creds},
        )


_builtin_scrapers["drac_member"] = DRACMemberScraper()
