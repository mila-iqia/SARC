"""
This script is basically a wrapper around the "read_mila_ldap.py" script.
Instead of taking arguments from the command line, it takes them from
the SARC configuration file.

This is possible because the "read_mila_ldap.py" script has a `run` function
that takes the arguments as parameters, so the argparse step comes earlier.

As a result of running this script, the values in the collection
referenced by "cfg.ldap.mongo_collection_name" will be updated.
"""

import json

import pandas as pd
from pymongo import UpdateOne

import sarc.account_matching.make_matches
import sarc.ldap.mymila
from sarc.config import config
from sarc.ldap.read_mila_ldap import fetch_ldap
from sarc.ldap.revision import commit_matches_to_database


def run(prompt=False):
    """If prompt is True, script will prompt for manual matching."""

    cfg = config()

    LD_users = fetch_ldap(ldap=cfg.ldap)

    mymila_data = sarc.ldap.mymila.query_mymila(cfg.mymila)

    if not mymila_data.empty:
        df_users = pd.DataFrame(LD_users)
        mymila_data = mymila_data.rename(columns={"MILA Email": "mila_email_username"})

        df = pd.merge(df_users, mymila_data, on="mila_email_username", how="outer")

        # NOTE: Select columns that should be used from MyMila.
        LD_users = df[
            [
                "mila_email_username",
                "mila_cluster_username",
                "mila_cluster_uid",
                "mila_cluster_gid",
                "display_name",
                "supervisor",
                "co_supervisor",
                "status",
            ]
        ].to_dict("records")

    # Match DRAC/CC to mila accounts
    DLD_data = sarc.account_matching.make_matches.load_data_from_files(
        {
            "mila_ldap": LD_users,  # pass through
            "drac_roles": cfg.account_matching.drac_roles_csv_path,
            "drac_members": cfg.account_matching.drac_members_csv_path,
        }
    )

    # hint : To debug or manually adjust `perform_matching` to handle new edge cases
    #        that arrive each semester, you can inspect the contents of the temporary file
    #        to see what you're working with, or you can just inspect `DLD_data`
    #        by saving it somewhere.

    with open(
        cfg.account_matching.make_matches_config, "r", encoding="utf-8"
    ) as json_file:
        make_matches_config = json.load(json_file)

    (
        DD_persons_matched,
        new_manual_matches,
    ) = sarc.account_matching.make_matches.perform_matching(
        DLD_data=DLD_data,
        mila_emails_to_ignore=make_matches_config["L_phantom_mila_emails_to_ignore"],
        override_matches_mila_to_cc=make_matches_config[
            "D_override_matches_mila_to_cc_account_username"
        ],
        name_distance_delta_threshold=0,
        verbose=False,
        prompt=prompt,
    )

    # from pprint import pprint
    # pprint(DD_persons_matched)

    # `DD_persons_matched` is indexed by mila_email_username values,
    # and each entry is a dict with 3 keys:
    #     {
    #       "mila_ldap": {
    #           "mila_email_username": "john.appleseed@mila.quebec",
    #           ...
    #       },
    #       "drac_roles": {...} or None,
    #       "drac_members": {...} or None
    #     }

    # These associations can now be propagated to the database.
    commit_matches_to_database(
        cfg.mongo.database_instance[cfg.ldap.mongo_collection_name],
        DD_persons_matched,
    )

    # If new manual matches are available, save them.
    if new_manual_matches:
        make_matches_config["D_override_matches_mila_to_cc_account_username"].update(
            new_manual_matches
        )
        with open(
            cfg.account_matching.make_matches_config, "w", encoding="utf-8"
        ) as json_file:
            json.dump(make_matches_config, json_file, indent=4)


if __name__ == "__main__":
    run()
