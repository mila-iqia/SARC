import json

import pandas as pd

from sarc.config import MyMilaConfig

START_DATE_KEY = "Start Date with MILA"
END_DATE_KEY = "End Date with MILA"


def query_mymila(cfg: MyMilaConfig):
    if cfg is None:
        return pd.DataFrame()

    # NOTE: Using json loads on open instead of pd.read_json
    #       because the mocked config is on `open`. It stinks, but we
    #       will replace this part of the code in favor of direct calls to
    #       MyMila API anyway.
    return pd.DataFrame(
        json.loads(open(cfg.tmp_json_path, "r", encoding="uft-8").read())
    )


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


def combine(LD_users, mymila_data: pd.DataFrame):
    if not mymila_data.empty:
        df_users = pd.DataFrame(LD_users)
        # Set the empty values to NA
        df_users = df_users.where((pd.notnull(df_users)) & (df_users != ""), pd.NA)

        # Preprocess
        mymila_data.set_index(mymila_data["in1touch_id"], inplace=True)
        mymila_data.rename(columns={"MILA Email": "mila_email_username"}, inplace=True)
        # Set the empty values to NA
        mymila_data = mymila_data.where(
            (pd.notnull(mymila_data)) & (mymila_data != ""), pd.NA
        )
        for _id in mymila_data["in1touch_id"]:
            supervisor_filter = mymila_data["Supervisor Principal"] == _id
            cosupervisor_filter = mymila_data["Co-Supervisor"] == _id
            email = mymila_data.loc[_id]["mila_email_username"]
            mymila_data["Supervisor Principal"][supervisor_filter] = email
            mymila_data["Co-Supervisor"][cosupervisor_filter] = email

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


def fetch_mymila(cfg, LD_users):
    mymila_data = query_mymila(cfg.mymila)
    return combine(LD_users, mymila_data)
