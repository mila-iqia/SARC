import csv
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from serieux.features.encrypt import Secret
from sqlmodel import col, select

from sarc.config import config
from sarc.db.users import CredentialsDB, MatchingID
from sarc.scraping.users import (
    Credentials,
    MatchID,
    UserMatch,
    UserScraper,
    _builtin_scrapers,
)

SCRAPER_NAME = "slurm_allocation_members"


@dataclass
class SlurmAllocationMembersConfig:
    # CSV with columns Slurm_Username, Nom, Email (may be empty), Allocations
    csv: Secret[str]
    # Domain of the cluster accounts (key of associated_accounts)
    domain: str = "drac"


class SlurmAllocationMembersScraper(UserScraper[SlurmAllocationMembersConfig]):
    """
    Import the members of Slurm allocations, for whom we only know the name,
    the cluster username and maybe an email.

    Members are identified by their username. The drac_member plugin declares
    the same id for its users, so both are matched together.
    """

    config_type = SlurmAllocationMembersConfig

    def get_user_data(self, config: SlurmAllocationMembersConfig) -> bytes:
        return json.dumps({"csv": config.csv, "domain": config.domain}).encode("utf-8")

    def parse_user_data(self, data: bytes, cache_time: datetime) -> Iterable[UserMatch]:  # noqa: ARG002
        j = json.loads(data.decode("utf-8"))
        yield from parse_csv(j["csv"], j["domain"])


def parse_csv(csv_file: str, domain: str) -> Iterable[UserMatch]:
    rows = [
        {k.lower(): (v or "").strip() for k, v in d.items()}
        for d in csv.DictReader(csv_file.split("\n"))
    ]
    rows = [d for d in rows if d["slurm_username"]]
    known = _known_matches(domain, {d["slurm_username"] for d in rows})

    for d in rows:
        username = d["slurm_username"]
        creds = Credentials()
        # The CSV gives no dates: the account is considered always valid.
        creds.insert(username)
        yield UserMatch(
            display_name=d["nom"] or None,
            email=d["email"].lower() or None,
            sort_sub=0,
            matching_id=MatchID(name=SCRAPER_NAME, mid=username),
            known_matches=known.get(username, set()),
            associated_accounts={domain: creds},
        )


def _known_matches(domain: str, usernames: set[str]) -> dict[str, set[MatchID]]:
    """Matching ids of the users already in the DB with these usernames.

    The accounts are imported without validity bounds and two users may not
    share a username over overlapping periods, so members must be matched with
    any user already holding the account (e.g. imported by legacy_dump).
    """
    stmt = (
        select(CredentialsDB.username, MatchingID.plugin_name, MatchingID.match_id)
        .join(MatchingID, col(MatchingID.user_id) == col(CredentialsDB.user_id))
        .where(
            CredentialsDB.domain == domain,
            col(CredentialsDB.username).in_(usernames),
            MatchingID.plugin_name != SCRAPER_NAME,
        )
        .distinct()
    )
    res: dict[str, set[MatchID]] = {}
    with config.db.session() as sess:
        for username, plugin_name, match_id in sess.exec(stmt):
            res.setdefault(username, set()).add(MatchID(name=plugin_name, mid=match_id))
    return res


_builtin_scrapers[SCRAPER_NAME] = SlurmAllocationMembersScraper()
