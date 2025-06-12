from datetime import timedelta
from typing import IO, Any

import pandas as pd

from sarc.cache import CachePolicy, FormatterProto, with_cache
from sarc.config import Config, MyMilaConfig

START_DATE_KEY = "Start Date with MILA"
END_DATE_KEY = "End Date with MILA"


class CSV_formatter(FormatterProto[pd.DataFrame]):
    read_flags = "r"
    write_flags = "w"

    @staticmethod
    def load(fp: IO[Any]) -> pd.DataFrame:
        return pd.read_csv(fp.name)

    @staticmethod
    def dump(obj: pd.DataFrame, fp: IO[Any]):
        raise NotImplementedError("Cannot dump mymila CSV cache yet.")
        # obj.to_csv(fp.name)


@with_cache(
    subdirectory="mymila",
    formatter=CSV_formatter,
    key=lambda *_, **__: "mymila_export_{time}.csv",
    validity=timedelta(days=120),
)  # type: ignore
def query_mymila_csv(cfg: MyMilaConfig) -> pd.DataFrame:
    raise NotImplementedError("Cannot read from mymila yet.")


def query_mymila(
    cfg: MyMilaConfig, cache_policy: CachePolicy = CachePolicy.use
) -> pd.DataFrame:
    return pd.DataFrame(query_mymila_csv(cfg, cache_policy=cache_policy))


def to_records(df: pd.DataFrame) -> list[dict]:
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


def combine(LD_users: list[dict], mymila_data: pd.DataFrame) -> list[dict]:
    if not mymila_data.empty:
        df_users = pd.DataFrame(LD_users)
        # Set the empty values to NA
        df_users = df_users.where((pd.notnull(df_users)) & (df_users != ""), pd.NA)

        # Preprocess
        mymila_data = mymila_data.rename(columns={"MILA Email": "mila_email_username"})
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

            mergecol("Status", "status")
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


def fetch_mymila(
    cfg: Config, LD_users: list[dict], cache_policy: CachePolicy = CachePolicy.use
) -> list[dict]:
    assert cfg.mymila is not None
    mymila_data = query_mymila(cfg.mymila, cache_policy=cache_policy)
    return combine(LD_users, mymila_data)
