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

import sarc.account_matching.make_matches
import sarc.ldap.mymila
from sarc.config import config
from sarc.ldap.read_mila_ldap import fetch_ldap
from sarc.ldap.revision import commit_matches_to_database


def run(prompt=False):
    """If prompt is True, script will prompt for manual matching."""

    cfg = config()
    user_collection = cfg.mongo.database_instance[cfg.ldap.mongo_collection_name]

    LD_users = fetch_ldap(ldap=cfg.ldap)

    # Retrieve users data from MyMila
    mymila_data = sarc.ldap.mymila.query_mymila(cfg.mymila)

    # If data have been retrieved from MyMila
    if not mymila_data.empty:

        # Handle database users LDAP data as pandas dataframe
        df_users = pd.DataFrame(LD_users)
        # Define types of data and indexes of database users LDAP data
        # and set the empty values to NA
        df_users = homogeneize_users_data(df_users)

        # Rename some fields from MyMila data
        mymila_data = mymila_data.rename(
            columns={
                "MILA Email": "mila_email_username",
                "Supervisor Principal": "supervisor",
                "Co-Supervisor": "co_supervisor",
            }
        )
        # Define types of data and indexes of the MyMila data
        # and set the empty values to NA
        mymila_data = homogeneize_users_data(mymila_data)

        # Merge LDAP data retrieved from the database and MyMila data
        # prioritizing the MyMila data
        df = mymila_data.combine_first(df_users)

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

    for _, user in DD_persons_matched.items():
        fill_computed_fields(user)

    # These associations can now be propagated to the database.
    commit_matches_to_database(
        user_collection,
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


def set_types(df_data):
    """
    Set the types of some fields of the data.

    Parameters:
        df_data     The data in which the columns types are set
    """
    # Determine for each type the corresponding columns
    types_for_columns = {
        "string": [
            "mila_email_username",
            "supervisor",
            "co_supervisor",
        ]  # The values of the fields "supervisor" and "co_supervisor" are strings
    }

    for k_type, v_columns in types_for_columns.items():
        df_data[v_columns] = df_data[v_columns].astype(k_type)


def homogeneize_users_data(df_users_data):
    """
    Set the types of some columns, set mila_email_username
    as index and set the empty values to NA

    Parameters:
        df_users_data   Dataframe containing our data

    Return:
        The data after the modifications done
    """
    # Define the types of some fields in the dataframe
    set_types(df_users_data)
    # Set the empty values to NA
    df_users_data = df_users_data.where(
        (pd.notnull(df_users_data)) & (df_users_data != ""), pd.NA
    )
    # Define mila_email_username as the key
    return df_users_data.set_index("mila_email_username", drop=False)


def fill_computed_fields(data: dict):
    mila_ldap = data.get("mila_ldap", {}) or {}
    drac_members = data.get("drac_members", {}) or {}
    drac_roles = data.get("drac_roles", {}) or {}

    if "name" not in data:
        data["name"] = mila_ldap.get("display_name", "???")

    if "mila" not in data:
        data["mila"] = {
            "username": mila_ldap.get("mila_cluster_username", "???"),
            "email": mila_ldap.get("mila_email_username", "???"),
            "active": mila_ldap.get("status", None) == "enabled",
        }

    if "drac" not in data:
        if drac_members:
            data["drac"] = {
                "username": drac_members.get("username", "???"),
                "email": drac_members.get("email", "???"),
                "active": drac_members.get("activation_status", None) == "activated",
            }
        elif drac_roles:
            data["drac"] = {
                "username": drac_roles.get("username", "???"),
                "email": drac_roles.get("email", "???"),
                "active": drac_roles.get("status", None) == "Activated",
            }
        else:
            data["drac"] = None

    return data


if __name__ == "__main__":
    run()
