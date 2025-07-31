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

#
# Requirements
#

from attr import dataclass
from azure.identity import ClientSecretCredential
from collections.abc import Iterable
from itertools import chain, repeat
from pydantic import BaseModel
from typing import Sequence

import logging
import pandas as pd
import struct

from sarc.core.models.users import Credentials
from sarc.core.scraping.users import UserMatch, UserScraper, _builtin_scrapers


logger = logging.getLogger(__name__)


#
# Plugin data models
#


class Affiliation(BaseModel):
    """
    Data related to the affiliation of a user
    from MyMila
    """

    type: str | None  # Affiliation_type
    university: str  # Affiliated_university
    faculty: str  # Faculty_affiliated
    department: str  # Department_affiliated


class Accounts(BaseModel):
    """
    Third parties accounts of the user
    """

    drac_account: str | None  # Alliance-DRAC_account
    github_username: str | None  # GitHub_username
    google_scholar_profile: str | None  # Google_Scholar_profile


class Supervision(BaseModel):
    """
    Supervisor and co-supervisor of the user
    """

    supervisor_member_num: float  # Supervisor_Principal__MEMBER_NUM_
    co_supervisor_member_num: float  # Co-Supervisor__MEMBER_NUM_


class TimePeriod(BaseModel):
    """ """

    start: str | None
    end: str | None


class Status(BaseModel):
    status: str  # Status
    profile_type: str  # Profile_Type
    membership_type: str  # Membership_Type
    ccai_chair_cifar: str  # CCAI_Chair_CIFAR

    academic_nomination_dates: TimePeriod  # Start_date_of_academic_nomination and End_date_of_academic_nomination
    studies_dates: TimePeriod  # Start_date_of_studies and End_date_of_studies
    visit_internship_dates: (
        TimePeriod  # Start_date_of_visit_internship and End_date_of_visit_internship
    )
    mila_dates: TimePeriod  # Start_Date_with_MILA and End_Date_with_MILA


class MyMilaUser(BaseModel):
    """
    User data from MyMila

    The id used in this plugin is the parameter "member_num"
    """

    member_num: float  # _MEMBER_NUM_
    mymila_id: int  # internal_id
    mila_number: str | None  # Mila_Number

    first_name: str  # Preferred_First_Name or First_Name if None
    last_name: str  # Last_Name
    mila_email: str | None  # MILA_Email

    status: Status
    affiliation: Affiliation
    supervision: Supervision
    accounts: Accounts

    def get_display_name(self):
        """
        Return the preferred first name or first name, followed
        by the name of the user
        """

        return f"{self.first_name} {self.last_name}"

    def to_common_user(self):
        """
        This function is used to convert a MyMilaUser to a common
        User. This is done in order to be able to merge the data of
        different sources
        """
        # TODO: once a common class User has been defined
        pass


#
# Plugin configuration
#


@dataclass
class MyMilaConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    sql_endpoint: str
    database: str = "wh_sarc"


#
# Plugin scraper
#
class MyMilaScraper(UserScraper[MyMilaConfig]):
    """ """

    config_type = MyMilaConfig

    def get_user_data(self, config: MyMilaConfig) -> str:
        """
        Get the list of users from the MyMila data source
        Parameters:
            config  Configuration used to access MyMila data

        Returns
            A list of MyMilaUsers which describe the user retrieved
            from MyMila
        """
        # TODO: once the MyMilaUser.to_common_user will be defined,
        #       we will be able to return a list of common users instead
        return _query_mymila(config)

    def update_user_data(self, config: MyMilaConfig, data: str) -> Iterable[UserMatch]:
        """

        Parameters:
            config  Configuration of the MyMila scraper
            data    String describing a list of users

        Returns an iteration of UserMatch
        """

        for user in users:
            yield UserMatch(
                display_name=user.get_display_name(),
                email=user.mila_email,
                associated_accounts={
                    "drac": [
                        Credentials(
                            username=user.accounts.drac_account,
                            uid=None,  # TODO
                            gid=None,  # TODO
                            active=False,
                        ),  # TODO
                    ],
                    "github": [
                        Credentials(
                            username=user.accounts.github_username,
                            uid=None,  # TODO
                            gid=None,  # TODO
                            active=False,
                        ),  # TODO
                    ],
                    "google_scholar": [
                        Credentials(
                            username=user.accounts.google_scholar_profile,
                            uid=None,  # TODO
                            gid=None,  # TODO
                            active=False,
                        ),  # TODO
                    ],
                },
                supervisor=user.supervision.supervisor_member_num,
                co_supervisor=user.supervision.co_supervisor_member_num,
                record_start=None,  # TODO
                record_end=None,  # TODO
                matching_id=user.member_num,
                known_matches={},  # TODO
            )


_builtin_scrapers["mymila"] = MyMilaScraper()


@with_cache(
    subdirectory="mymila",
    key=lambda cfg: "mymila_export_{time}.json",
    validity=timedelta(days=120),
)
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
    df = pd.DataFrame(records, columns=headers)

    # Return the data converted as MyMilUser elements
    return [_to_entry(s) for s in df]


def _to_entry(s: pd.Series) -> MyMilaUser:
    """
    Convert user data retrieved from MyMila and stored as a pandas dataframe
    to MyMilaUser entries
    """

    # Personal data
    first_name = s["Preferred_First_Name"]
    if first_name is None:
        first_name = s["First_Name"]

    # Status
    status = Status(
        status=s["Status"],
        profile_type=s["Profile_Type"],
        membership_type=s["Membership_Type"],
        ccai_chair_cifar=s["CCAI_Chair_CIFAR"],
        academic_nomination_dates=TimePeriod(
            start=s["Start_date_of_academic_nomination"],
            end=s["End_date_of_academic_nomination"],
        ),
        studies_dates=TimePeriod(
            start=s["Start_date_of_studies"], end=s["End_date_of_studies"]
        ),
        visit_internship_dates=TimePeriod(
            start=s["Start_date_of_visit_internship"],
            end=s["End_date_of_visit_internship"],
        ),
        mila_dates=TimePeriod(
            start=s["Start_Date_with_MILA"], end=s["End_Date_with_MILA"]
        ),
    )

    # Affiliation
    affiliation = Affiliation(
        type=s["Affiliation_type"],
        faculty=s["Faculty_affiliated"],
        university=s["Affiliated_university"],
        departement=s["Department_affiliated"],
    )

    # Supervision
    supervision = Supervision(
        supervisor=s["Supervisor_Principal__MEMBER_NUM_"] | None,
        co_supervisor=s["Co-Supervisor__MEMBER_NUM_"] | None,
    )

    # Accounts
    accounts = Accounts(
        drac_account=s["Alliance-DRAC_account"] | None,
        github_username=s["GitHub_username"] | None,
        google_scholar_profile=s["Google_Scholar_profile"] | None,
    )

    # Return the resulting MyMilaUser
    return MyMilaUser(
        # Identifiers
        member_num=s["_MEMBER_NUM_"],
        mymila_id=s["internal_id"],
        mila_number=s["Mila_Number"],
        # Personal data
        first_name=first_name,
        last_name=s["Last_Name"],
        mila_email=s["MILA_Email"],
        # Status
        status=status,
        # Affiliation
        affiliation=affiliation | None,
        # Supervision
        supervision=supervision | None,
        # Accounts
        accounts=accounts,
    )
