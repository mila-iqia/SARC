import json

import pandas as pd

from sarc.config import MyMilaConfig
from sarc.ldap.revision import (
    END_DATE_KEY,
    START_DATE_KEY,
)

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
    return df[
        [
            "mila_email_username",
            "mila_cluster_username",
            "mila_cluster_uid",
            "mila_cluster_gid",
            "display_name",
            "supervisor",
            "co_supervisor",
            "status",
            START_DATE_KEY,
            END_DATE_KEY,
        ]
    ].to_dict("records")


def combine(LD_users, mymila_data):
    if not mymila_data.empty:
        df_users = pd.DataFrame(LD_users)
        mymila_data = mymila_data.rename(columns={"MILA Email": "mila_email_username"})

        if LD_users:
            df = pd.merge(df_users, mymila_data, on="mila_email_username", how="outer")
        else:
            df = df_users
        
        LD_users = to_records(df)
    return LD_users
    
    
def fetch_mymila(cfg, LD_users):
    mymila_data = query_mymila(cfg.mymila)
    return combine(LD_users, mymila_data)
