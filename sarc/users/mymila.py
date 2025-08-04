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
from itertools import chain, repeat
from typing import Sequence

import pandas as pd
import pyodbc
from attr import dataclass
from azure.identity import ClientSecretCredential

from sarc.core.scraping.users import UserMatch, UserScraper, _builtin_scrapers

logger = logging.getLogger(__name__)


CCI_RE = re.compile(r"[a-z]{3}-\d{3}")
CCRI_RE = re.compile(r"[a-z]{3}-\d{3}-\d{2}")


@dataclass
class MyMilaConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    sql_endpoint: str
    database: str = "wh_sarc"


class MyMilaScraper(UserScraper[MyMilaConfig]):
    config_type = MyMilaConfig

    def get_user_data(self, config: MyMilaConfig) -> bytes:
        return json.dumps(_query_mymila(config)).encode()

    def parse_user_data(
        self, _config: MyMilaConfig, data: bytes
    ) -> Iterable[UserMatch]:
        records, headers = json.loads(data.decode())
        for _, s in pd.DataFrame(records, columns=headers).iterrows():
            first_name = s["Preferred_First_Name"]
            if first_name is None:
                first_name = s["First_Name"]
            um = UserMatch(
                display_name=f"{first_name} {s['Last_Name']}",
                email=s["MILA_Email"],
                original_plugin="mymila",
                matching_id=s["_MEMBER_NUM_"],
                known_matches={"mila_ldap": s["MILA_Email"]},
            )
            supervisor = s["Supervisor_Principal__MEMBER_NUM_"]
            if supervisor:
                # TODO: figure out which dates apply
                um.supervisor.insert(supervisor, start=None, end=None)
            co_supervisor = s["Co-Supervisor__MEMBER_NUM_"]
            if co_supervisor:
                # TODO: figure out the dates
                um.co_supervisors.insert([co_supervisor], start=None, end=None)
            drac_account: str | None = s["Alliance-DRAC_account"]
            if drac_account:
                drac_account = drac_account.strip()
                if CCI_RE.fullmatch(drac_account):
                    um.known_matches["drac"] = drac_account
                if CCRI_RE.fullmatch(drac_account):
                    um.known_matches["drac"] = drac_account[:-3]
                logger.warning(
                    "Invalid data in 'Alliance-DRAC_account' field (not a CCI or CCRI): %s",
                    drac_account,
                )
            gh_user = s["GitHub_username"]
            if gh_user:
                um.github_username.insert(gh_user)
            gs_profile = s["Google_Scholar_profile"]
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
    records = cursor.fetchall()

    # Convert these data into a pandas Dataframe
    headers = [i[0] for i in cursor.description]
    return records, headers
