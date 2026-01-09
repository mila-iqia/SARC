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
from pathlib import Path
from typing import Any

from sarc.config import TZLOCAL
from sarc.core.models.users import Credentials, MemberType
from sarc.core.models.validators import END_TIME, START_TIME
from sarc.core.scraping.users import MatchID, UserMatch, UserScraper, _builtin_scrapers


def _parse_mtl_datetime(date_str: str | None) -> datetime | None:
    """Parse a datetime string from MTL timezone and convert to UTC."""
    if date_str is None:
        return None

    return datetime.fromisoformat(date_str).replace(tzinfo=TZLOCAL).astimezone(UTC)


def _determine_member_type(drac_members: dict[str, Any] | None) -> MemberType | None:  # noqa: PLR0911
    """Determine member type from DRAC members data."""
    if not drac_members or not isinstance(drac_members, dict):
        return None

    position = drac_members.get("position", "").lower()

    if "professeur" in position or "professor" in position:
        return MemberType.PROFESSOR
    elif "étudiant à la maîtrise" in position or "master" in position:
        return MemberType.MASTER_STUDENT
    elif (
        "étudiant au doctorat" in position
        or "phd" in position
        or "doctorat" in position
    ):
        return MemberType.PHD_STUDENT
    elif "postdoc" in position or "post-doc" in position:
        return MemberType.POSTDOC
    elif "stagiaire" in position or "intern" in position:
        return MemberType.INTERN
    elif "staff" in position or "employé" in position:
        return MemberType.STAFF

    return None  # pragma: no cover


@dataclass
class LegacyDumpConfig:
    json_file_path: Path


class LegacyDumpScraper(UserScraper[LegacyDumpConfig]):
    config_type = LegacyDumpConfig

    def get_user_data(self, config: LegacyDumpConfig) -> bytes:
        """Read the JSON dump file."""
        with open(config.json_file_path, "r", encoding="utf-8") as f:
            return f.read().encode("utf-8")

    def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
        """Parse the legacy user data and convert to UserMatch format."""
        records = json.loads(data.decode("utf-8"))
        for record in records:
            # Extract basic information
            name = record.get("name")

            # Parse timestamps
            record_start = _parse_mtl_datetime(record.get("record_start"))
            record_end = _parse_mtl_datetime(record.get("record_end"))

            # Use START_TIME if no record_start, END_TIME if no record_end
            if record_start is None:
                record_start = START_TIME
            if record_end is None:
                record_end = END_TIME

            email = record["mila_ldap"]["mila_email_username"]
            matching_id = MatchID(name="legacy_dump", mid=email)

            # Create UserMatch
            user_match = UserMatch(
                display_name=name,
                email=email,
                matching_id=matching_id,
            )

            # Add known matches from different sources
            known_matches = set()

            # Mila LDAP match
            known_matches.add(
                MatchID(
                    name="mila_ldap", mid=record["mila_ldap"]["mila_email_username"]
                )
            )
            # DRAC members match
            drac_members_data = record.get("drac_members")
            if drac_members_data and drac_members_data.get("ccri"):
                known_matches.add(
                    MatchID(name="drac_member", mid=drac_members_data["ccri"][:-3])
                )
                known_matches.add(
                    MatchID(name="drac_role", mid=drac_members_data["ccri"][:-3])
                )

            user_match.known_matches = known_matches

            # Set member type
            member_type = _determine_member_type(record.get("drac_members"))
            if member_type:
                user_match.member_type.insert(member_type, record_start, record_end)

            # Add associated accounts
            # Mila account
            if record.get("mila_ldap", {}).get("status") == "enabled":
                mila_creds = Credentials()
                mila_creds.insert(
                    record["mila_ldap"]["mila_cluster_username"],
                    record_start,
                    record_end,
                )
                user_match.associated_accounts["mila"] = mila_creds

            # DRAC account
            drac_data = record.get("drac")
            if drac_data and drac_data.get("username"):
                drac_creds = Credentials()
                drac_creds.insert(drac_data["username"], record_start, record_end)
                user_match.associated_accounts["drac"] = drac_creds

            # Supervisor information
            supervisor_email = record.get("mila_ldap", {}).get("supervisor")
            if supervisor_email:
                supervisor_match = MatchID(name="legacy_dump", mid=supervisor_email)
                user_match.supervisor.insert(supervisor_match, record_start, record_end)

            # Co-supervisor information
            co_supervisor_email = record.get("mila_ldap", {}).get("co_supervisor")
            if co_supervisor_email:
                co_supervisor_match = MatchID(
                    name="legacy_dump", mid=co_supervisor_email
                )
                user_match.co_supervisors.insert(
                    {co_supervisor_match}, record_start, record_end
                )

            yield user_match


_builtin_scrapers["legacy_dump"] = LegacyDumpScraper()
