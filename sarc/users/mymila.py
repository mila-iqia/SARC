import struct
from datetime import timedelta
from itertools import chain, repeat
from typing import Sequence

import pandas as pd
import pyodbc
from azure.identity import ClientSecretCredential

from sarc.cache import with_cache
from sarc.config import MyMilaConfig

START_DATE_KEY = "Start Date with MILA"
END_DATE_KEY = "End Date with MILA"


@with_cache(
    subdirectory="mymila",
    key=lambda cfg: "mymila_export_{time}.json",
    validity=timedelta(days=120),
)
def query_mymila(cfg: MyMilaConfig):
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
    cursor.execute("SELECT * FROM MyMila_Extract_Etudiants")
    records = cursor.fetchall()
    headers = [i[0] for i in cursor.description]
    df = pd.DataFrame(records, columns=headers)
    return df


def to_records(df):
    # NOTE: Select columns that should be used from MyMila.
    wanted_cols = [
        "mila_email_username",
        "mila_cluster_username",
        "mila_cluster_uid",
        "mila_cluster_gid",
        "display_name",
        "supervisor",
        "co_supervisor",
        "status",
        "mymila_start",
        "mymila_end",
    ]

    selected = []
    for col in wanted_cols:
        if col in df.columns:
            selected.append(col)

    records = df[selected].to_dict("records")

    # Pandas really likes NaT (Not a Time)
    # but mongo does not
    for record in records:
        end_date = record.get("mymila_end", None)
        if pd.isna(end_date):
            end_date = None
        record["mymila_end"] = end_date

    return records


def combine(LD_users, mymila_data):
    if not mymila_data.empty:
        df_users = pd.DataFrame(LD_users)
        # Set the empty values to NA
        df_users = df_users.where((pd.notnull(df_users)) & (df_users != ""), pd.NA)

        # Preprocess
        mymila_data = mymila_data.rename(columns={"MILA_Email": "mila_email_username"})
        # Set the empty values to NA
        mymila_data = mymila_data.where(
            (pd.notnull(mymila_data)) & (mymila_data != ""), pd.NA
        )

        if LD_users:
            df = pd.merge(df_users, mymila_data, on="mila_email_username", how="outer")

            # mymila value should take precedence here
            #   Take the mymila columns and fill it with ldap if missing
            def mergecol(mymila_col, ldap_col):
                df[mymila_col] = df[mymila_col].fillna(df[ldap_col])

            mergecol("Status_VALUE", "status")
            mergecol("Supervisor Principal", "supervisor")
            mergecol("Co-Supervisor", "co_supervisor")

        else:
            df = mymila_data

        # Use mymila field
        df = df.rename(
            columns={
                "Status": "status",
                "Supervisor Principal": "supervisor",
                "Co-Supervisor": "co_supervisor",
            }
        )

        # Create the new display name
        df["display_name"] = df["Preferred First Name"] + " " + df["Last Name"]

        # Coerce datetime.date into datetime because bson does not understand date
        def convert_datetime(col, origin):
            df[col] = pd.to_datetime(df[origin], errors="ignore")

        convert_datetime("mymila_start", START_DATE_KEY)
        convert_datetime("mymila_end", END_DATE_KEY)

        LD_users = to_records(df)
    return LD_users


def fetch_mymila(cfg, LD_users, cache_policy=True):
    mymila_data = query_mymila(cfg.mymila, cache_policy=cache_policy)
    return combine(LD_users, mymila_data)
