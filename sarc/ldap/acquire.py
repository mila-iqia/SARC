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

from pymongo import UpdateOne

import sarc.account_matching.make_matches
import sarc.ldap.read_mila_ldap  # for the `run` function
from sarc.config import config


def run():
    cfg = config()

    sarc.ldap.read_mila_ldap.run(
        local_private_key_file=cfg.ldap.local_private_key_file,
        local_certificate_file=cfg.ldap.local_certificate_file,
        ldap_service_uri=cfg.ldap.ldap_service_uri,
        # write results in database
        mongodb_database_instance=cfg.mongo.database_instance,
        mongodb_collection=cfg.ldap.mongo_collection_name,
        #        output_json_file="secrets/account_matching/mila_users.json"
    )

    # It becomes really hard to test this with script when
    # we mock the `open` calls, so we'll instead rely on
    # what has already been populated in the database.
    LD_users = list(
        cfg.mongo.database_instance[cfg.ldap.mongo_collection_name].find({})
    )
    LD_users = [D_user["mila_ldap"] for D_user in LD_users]

    DLD_data = sarc.account_matching.make_matches.load_data_from_files(
        {
            "mila_ldap": LD_users,  # pass through
            "cc_roles": cfg.account_matching.cc_roles_csv_path,
            "cc_members": cfg.account_matching.cc_members_csv_path,
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
        
    DD_persons_matched = sarc.account_matching.make_matches.perform_matching(
        DLD_data=DLD_data,
        mila_emails_to_ignore=make_matches_config[
            "L_phantom_mila_emails_to_ignore"
        ],
        override_matches_mila_to_cc=make_matches_config[
            "D_override_matches_mila_to_cc_account_username"
        ],
        name_distance_delta_threshold=0,
        verbose=False,
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
    #       "cc_roles": {...} or None,
    #       "cc_members": {...} or None
    #     }

    # These associations can now be propagated to the database.
    commit_matches_to_database(
        cfg.mongo.database_instance[cfg.ldap.mongo_collection_name],
        DD_persons_matched,
    )


def commit_matches_to_database(users_collection, DD_persons_matched, verbose=False):
    L_updates_to_do = []
    for mila_email_username, D_match in DD_persons_matched.items():
        assert (
            D_match["mila_ldap"]["mila_email_username"] == mila_email_username
        )  # sanity check

        # if mila ldap user status = "unknown", which means the user does NOT exist in the Mila LDAP but was created as a dummy placeholder,
        # then UPSERT the document in the database if necessary
        # otherwise, juste update it
        if D_match["mila_ldap"]["status"] == "unknown":
            L_updates_to_do.append(
                UpdateOne(
                    {"mila_ldap.mila_email_username": mila_email_username},
                    {
                        # We set all the fields corresponding to the fields from `updated_user`,
                        # so that's a convenient way to do it. Note that this does not affect
                        # the fields in the database that are already present for that user.
                        "$set": {
                            "mila_ldap": D_match["mila_ldap"],
                            "cc_roles": D_match["cc_roles"],
                            "cc_members": D_match["cc_members"],
                        },
                    },
                    upsert=True,
                )
            )
        else:
            L_updates_to_do.append(
                UpdateOne(
                    {"mila_ldap.mila_email_username": mila_email_username},
                    {
                        # We don't modify the "mila_ldap" field,
                        # only add the "cc_roles" and "cc_members" fields.
                        "$set": {
                            "cc_roles": D_match["cc_roles"],
                            "cc_members": D_match["cc_members"],
                        },
                    },
                    # Don't add that entry if it doesn't exist.
                    # That would create some dangling entry that doesn't have a "mila_ldap" field.
                    upsert=False,
                )
            )

    if L_updates_to_do:
        result = users_collection.bulk_write(L_updates_to_do)  #  <- the actual commit
        if verbose:
            print(result.bulk_api_result)
    else:
        if verbose:
            print("Nothing to do.")

    # might as well return this result in case we'd like to write tests for it
    return result


if __name__ == "__main__":
    run()
