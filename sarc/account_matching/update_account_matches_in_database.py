"""
Connect to the local MongoDB database.
Get the information from the "matches_done.json" file.
Commit the new information to the database.

"""
import argparse
import json

from pymongo import MongoClient, UpdateOne

from sarc.common.config import get_config

parser = argparse.ArgumentParser(
    description="Updates the account matches in the database."
)
parser.add_argument(
    "--mongodb_database",
    default="sarc",
    type=str,
    help="(optional) MongoDB database to modify. Better left at default.",
)
parser.add_argument(
    "--mongodb_collection",
    default="users",
    type=str,
    help="(optional) MongoDB collection to modify. Better left at default.",
)
parser.add_argument(
    "--mongodb_connection_string",
    default=None,
    type=str,
    help="(optional) MongoDB connection string. Contains username and password.",
)
parser.add_argument(
    "--input_matches_path",
    type=str,
    default="secrets/account_matching/matches_done.json",
    help="Output of the make_matches.py script to be used as input here.",
)


def run(
    mongodb_connection_string, mongodb_database, mongodb_collection, input_matches_path
):
    users_collection = MongoClient(mongodb_connection_string)[mongodb_database][
        mongodb_collection
    ]

    with open(input_matches_path, "r", encoding='utf-8') as f:
        DD_matches = json.load(f)

    # DD_matches indexed by mila_email_username values,
    # and each entry is a dict with 3 keys:
    #     {
    #       "mila_ldap": {
    #           "mila_email_username": "john.appleseed@mila.quebec",
    #           ...
    #       },
    #       "cc_roles": null,
    #       "cc_members": null
    #     },

    L_updates_to_do = []
    for mila_email_username, D_match in DD_matches.items():
        assert (
            D_match["mila_ldap"]["mila_email_username"] == mila_email_username
        )  # sanity check

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
        print(result.bulk_api_result)
    else:
        print("Nothing to do.")


if __name__ == "__main__":
    args = parser.parse_args()
    run(
        mongodb_connection_string=args.mongodb_connection_string,
        mongodb_database=args.mongodb_database,
        mongodb_collection=args.mongodb_collection,
        input_matches_path=args.input_matches_path,
    )
