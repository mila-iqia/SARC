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

import sarc.account_matching.make_matches
import sarc.ldap.mymila
from sarc.config import config
from sarc.ldap.mymila import fetch_mymila
from sarc.ldap.read_mila_ldap import fetch_ldap
from sarc.ldap.revision import commit_matches_to_database


def run(prompt=False):
    """If prompt is True, script will prompt for manual matching."""

    cfg = config()
    user_collection = cfg.mongo.database_instance[cfg.ldap.mongo_collection_name]

    LD_users = fetch_ldap(ldap=cfg.ldap)

    LD_users = fetch_mymila(cfg, LD_users)

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
