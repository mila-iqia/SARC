"""
This is a plugin to read user data from MyMila

A MyMila entry contains the following fields:

- Affiliated_university
- Affiliation_type
- Alliance-DRAC_account
- Co-Supervisor_Membership_Type
- Co-Supervisor__MEMBER_NAME_
- Co-Supervisor__MEMBER_NUM_
- Department_affiliated
- End_date_of_academic_nomination
- End_date_of_studies
- End_date_of_visit-internship
- Faculty_affiliated
- First_Name
- GitHub_username
- Google_Scholar_profile
- Last_Name
- MILA_Email
- Membership_Type
- Mila_Number
- Preferred_First_Name
- Profile_Type
- Start_Date_with_MILA
- Start_date_of_academic_nomination
- Start_date_of_studies
- Start_date_of_visit-internship
- End_Date_with_MILA
- Status
- Supervisor_Principal_Membership_Type
- Supervisor_Principal__MEMBER_NAME_
- Supervisor_Principal__MEMBER_NUM_
- internal_id
- Co-Supervisor_CCAI_Chair_CIFAR
- Supervisor_Principal_CCAI_Chair_CIFAR
- CCAI_Chair_CIFAR
- _MEMBER_NUM_
"""

import json
import logging
import re
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from enum import IntEnum, unique
from itertools import chain, repeat
from typing import Sequence

import pyodbc  # type: ignore[import-not-found]
from azure.identity import ClientSecretCredential

from sarc.core.scraping.users import MatchID, UserMatch, UserScraper, _builtin_scrapers

logger = logging.getLogger(__name__)


CCI_RE = re.compile(r"[a-z]{3}-\d{3}")
CCRI_RE = re.compile(r"[a-z]{3}-\d{3}-\d{2}")


@unique
class Headers(IntEnum):
    Affiliated_university = 0
    Affiliation_type = 1
    Alliance_DRAC_account = 2
    Co_Supervisor_Membership_Type = 3
    Co_Supervisor__MEMBER_NAME_ = 4
    Co_Supervisor__MEMBER_NUM_ = 5
    Department_affiliated = 6
    End_date_of_academic_nomination = 7
    End_date_of_studies = 8
    End_date_of_visit_internship = 9
    Faculty_affiliated = 10
    First_Name = 11
    GitHub_username = 12
    Google_Scholar_profile = 13
    Last_Name = 14
    MILA_Email = 15
    Membership_Type = 16
    Mila_Number = 17
    Preferred_First_Name = 18
    Profile_Type = 19
    Start_Date_with_MILA = 20
    Start_date_of_academic_nomination = 21
    Start_date_of_studies = 22
    Start_date_of_visit_internship = 23
    End_Date_with_MILA = 24
    Status = 25
    Supervisor_Principal_Membership_Type = 26
    Supervisor_Principal__MEMBER_NAME_ = 27
    Supervisor_Principal__MEMBER_NUM_ = 28
    internal_id = 29
    Co_Supervisor_CCAI_Chair_CIFAR = 30
    Supervisor_Principal_CCAI_Chair_CIFAR = 31
    CCAI_Chair_CIFAR = 32
    MEMBER_NUM = 33


@dataclass
class MyMilaConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    sql_endpoint: str
    database: str = "wh_sarc"


def _json_serial(obj: object) -> str:
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))  # pragma: no cover


class MyMilaScraper(UserScraper[MyMilaConfig]):
    config_type = MyMilaConfig

    def get_user_data(self, config: MyMilaConfig) -> bytes:
        return json.dumps(_query_mymila(config), default=_json_serial).encode()

    def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
        records, headers = json.loads(data.decode())
        headers = [h.replace("-", "_") for h in headers]
        assert headers[-1] == "_MEMBER_NUM_"
        headers[-1] = "MEMBER_NUM"
        assert headers == [h.name for h in Headers]
        for record in records:
            first_name = record[Headers.Preferred_First_Name]
            if first_name is None:
                first_name = record[Headers.First_Name]
            um = UserMatch(
                display_name=f"{first_name} {record[Headers.Last_Name]}",
                email=record[Headers.MILA_Email],
                matching_id=MatchID(name="mymila", mid=str(record[Headers.MEMBER_NUM])),
                known_matches={
                    MatchID(name="mila_ldap", mid=record[Headers.MILA_Email])
                },
            )
            supervisor = record[Headers.Supervisor_Principal__MEMBER_NUM_]
            if supervisor is not None:
                # TODO: figure out which dates apply
                um.supervisor.insert(
                    MatchID(name="mymila", mid=str(supervisor)), start=None, end=None
                )
            co_supervisor = record[Headers.Co_Supervisor__MEMBER_NUM_]
            if co_supervisor is not None:
                # TODO: figure out the dates
                um.co_supervisors.insert(
                    {MatchID(name="mymila", mid=str(co_supervisor))},
                    start=None,
                    end=None,
                )
            drac_account: str | None = record[Headers.Alliance_DRAC_account]
            if drac_account:
                drac_account = drac_account.strip()
                if CCI_RE.fullmatch(drac_account):
                    um.known_matches.add(MatchID(name="drac", mid=drac_account))
                if CCRI_RE.fullmatch(drac_account):
                    um.known_matches.add(MatchID(name="drac", mid=drac_account[:-3]))
                logger.warning(
                    "Invalid data in 'Alliance-DRAC_account' field (not a CCI or CCRI): %s",
                    drac_account,
                )
            gh_user = record[Headers.GitHub_username]
            if gh_user:
                um.github_username.insert(gh_user)
            gs_profile = record[Headers.Google_Scholar_profile]
            if gs_profile:
                um.google_scholar_profile.insert(gs_profile)
            yield um


_builtin_scrapers["mymila"] = MyMilaScraper()


def _query_mymila(cfg: MyMilaConfig):
    """
    Contact MyMila in order to retrieve users data,
    then return these data as MyMilaUser elements.
    """
    # Retrieve MyMila data
    credential = ClientSecretCredential(
        client_id=cfg.client_id,
        tenant_id=cfg.tenant_id,
        client_secret=cfg.client_secret,
    )
    connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={cfg.sql_endpoint},1433;Database={cfg.database};Encrypt=Yes;TrustServerCertificate=No"
    token_object = credential.get_token("https://database.windows.net/.default")
    token_as_bytes = token_object.token.encode("UTF-8")
    encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
    token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
    attrs_before: dict[int, int | bytes | bytearray | str | Sequence[str]] = {
        1256: token_bytes
    }

    connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM MyMila_Extract_Etudiants_2")
    records = [tuple(row) for row in cursor.fetchall()]
    headers = [i[0] for i in cursor.description]

    return records, headers
