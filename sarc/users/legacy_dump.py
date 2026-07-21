"""
Legacy user data scraper for importing data from the old users database.

This scraper processes JSON dumps from the legacy system and converts them
to the new UserMatch format, handling timezone conversion from MTL to UTC
and preserving historical validity periods.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from serieux.features.encrypt import Secret

from sarc.scraping.users import (
    Credentials,
    MatchID,
    UserMatch,
    UserScraper,
    _builtin_scrapers,
)

MTL = ZoneInfo("America/Montreal")


def _parse_mtl_datetime(date_str: str | None) -> datetime | None:
    """Parse a datetime string from MTL timezone and convert to UTC."""
    if date_str is None:
        return None

    return datetime.fromisoformat(date_str).replace(tzinfo=MTL).astimezone(UTC)


@dataclass
class LegacyDumpConfig:
    json: Secret[str]


class LegacyDumpScraper(UserScraper[LegacyDumpConfig]):
    config_type = LegacyDumpConfig

    def get_user_data(self, config: LegacyDumpConfig) -> bytes:
        """Read the JSON dump file."""
        return config.json.encode("utf-8")

    def parse_user_data(self, data: bytes, cache_time: datetime) -> Iterable[UserMatch]:  # noqa: ARG002
        """Parse the legacy user data and convert to UserMatch format."""
        records = json.loads(data.decode("utf-8"))
        for record in records:
            # Extract basic information
            name = record.get("name")

            # Parse timestamps
            record_start = _parse_mtl_datetime(record.get("record_start"))
            record_end = _parse_mtl_datetime(record.get("record_end"))

            email = record["mila"]["email"].lower()
            matching_id = MatchID(name="legacy_dump", mid=email)

            # Create UserMatch
            user_match = UserMatch(
                display_name=name, email=email, matching_id=matching_id
            )

            # Add known matches from different sources
            known_matches = set()

            # Mila LDAP match
            known_matches.add(MatchID(name="mila_ldap", mid=record["mila"]["email"]))
            # DRAC members match
            drac_members_data = record.get("drac_members") or record.get("drac_roles")
            if drac_members_data and drac_members_data.get("ccri"):
                known_matches.add(
                    MatchID(name="drac_member", mid=drac_members_data["ccri"][:-3])
                )

            user_match.known_matches = known_matches

            # Add associated accounts
            # Mila account
            if record.get("mila", {}).get("active", False):
                mila_creds = Credentials()
                mila_creds.insert(record["mila"]["username"], record_start, record_end)
                user_match.associated_accounts["mila"] = mila_creds

            # DRAC account
            drac_data = record.get("drac")
            if drac_data and drac_data.get("username"):
                drac_creds = Credentials()
                drac_creds.insert(drac_data["username"], record_start, record_end)
                user_match.associated_accounts["drac"] = drac_creds

            yield user_match


_builtin_scrapers["legacy_dump"] = LegacyDumpScraper()
