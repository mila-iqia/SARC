"""
What this script does
=====================

This script runs locally every day on a machine at Mila.
It queries our LDAP service for all users and it updates
the MongoDB instance for SARC so that the "users"
collection reflects those accounts.

It's unclear whether we should automatically disable
the existing accounts in CW for members that are not being
mentioned in the LDAP results. We probably want to do
such a thing periodically, with special care instead of
doing it automatically.

Two mutually-exclusive ways to input the data, by priority:
   1) use the --input_json_file argument
   2) use the LDAP

Two ways to output the data, that can be used together:
   1) to the --output_json_file
   2) to a MongoDB instance


Sample uses
===========

Two ways this can be used:

::

    python3 read_mila_ldap.py \\
        --local_private_key_file secrets/Google_2026_01_26_66827.key \\
        --local_certificate_file secrets/Google_2026_01_26_66827.crt \\
        --ldap_service_uri ldaps://ldap.google.com \\
        --mongodb_connection_string ${MONGODB_CONNECTION_STRING} \\
        --output_json_file mila_users.json

    python3 read_mila_ldap.py \\
        --mongodb_connection_string ${MONGODB_CONNECTION_STRING} \\
        --input_json_file mila_users.json


LDAP data structure
===================

When it comes to the LDAP entries that we get (and need to process),
they are structured as follows:

::

    {
        "attributes": {
            "apple-generateduid": [
                "AF54098F-29AE-990A-B1AC-F63F5A89B89"
            ],
            "cn": [
                "john.smith",
                "John Smith"
            ],
            "departmentNumber": [],
            "displayName": [
                "John Smith"
            ],
            "employeeNumber": [],
            "employeeType": [],
            "gecos": [
                ""
            ],
            "gidNumber": [
                "1500000001"
            ],
            "givenName": [
                "John"
            ],
            "googleUid": [
                "john.smith"
            ],
            "homeDirectory": [
                "/home/john.smith"
            ],
            "loginShell": [
                "/bin/bash"
            ],
            "mail": [
                "john.smith@mila.quebec"
            ],
            "memberOf": [],
            "objectClass": [
                "top",
                "person",
                "organizationalPerson",
                "inetOrgPerson",
                "posixAccount"
            ],
            "physicalDeliveryOfficeName": [],
            "posixUid": [
                "smithj"
            ],
            "sn": [
                "Smith"
            ],
            "suspended": [
                "false"
            ],
            "telephoneNumber": [],
            "title": [],
            "uid": [
                "john.smith"
            ],
            "uidNumber": [
                "1500000001"
            ]
        },
        "dn": "uid=john.smith,ou=IDT,ou=STAFF,ou=Users,dc=mila,dc=quebec"
    }

"""

import os
import argparse
import json

# Requirements
# - pip install ldap3
from ldap3 import Server, Connection, Tls, ALL_ATTRIBUTES, SUBTREE
import ssl

from pymongo import MongoClient, UpdateOne


parser = argparse.ArgumentParser(
    description="Query LDAP and update the MongoDB database users based on values returned."
)
parser.add_argument(
    "--local_private_key_file",
    type=str,
    help="local_private_key_file for LDAP connection",
)
parser.add_argument(
    "--local_certificate_file",
    type=str,
    help="local_certificate_file for LDAP connection",
)
parser.add_argument(
    "--ldap_service_uri",
    type=str,
    default="ldaps://ldap.google.com",
    help="ldap service uri",
)
# We have two possible things that we can do with the data fetched.
# Dumping to a json file is possible.
parser.add_argument(
    "--mongodb_connection_string",
    default=None,
    type=str,
    help="(optional) MongoDB connection string. Contains username and password.",
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
    "--input_json_file",
    default=None,
    type=str,
    help="(optional) Ignore the LDAP and load from this json file instead.",
)
parser.add_argument(
    "--output_json_file",
    default=None,
    type=str,
    help="(optional) Write results to json file.",
)
parser.add_argument(
    "--output_raw_LDAP_json_file",
    default=None,
    type=str,
    help="(optional) Write results of the raw LDAP query to json file.",
)


def query_ldap(local_private_key_file, local_certificate_file, ldap_service_uri):
    """
    Since we don't always query the LDAP (i.e. omitted when --input_json_file is given),
    we'll make this a separate function.
    """

    assert os.path.exists(
        local_private_key_file
    ), f"Missing local_private_key_file {local_private_key_file}."
    assert os.path.exists(
        local_certificate_file
    ), f"Missing local_certificate_file {local_certificate_file}."

    # Prepare TLS Settings
    tls_conf = Tls(
        local_private_key_file=local_private_key_file,
        local_certificate_file=local_certificate_file,
        validate=ssl.CERT_REQUIRED,
        version=ssl.PROTOCOL_TLSv1_2,
    )
    # Connect to LDAP
    server = Server(ldap_service_uri, use_ssl=True, tls=tls_conf)
    conn = Connection(server)
    conn.open()
    # Extract all the data
    conn.search(
        "dc=mila,dc=quebec",
        "(objectClass=inetOrgPerson)",
        search_scope=SUBTREE,
        attributes=ALL_ATTRIBUTES,
    )
    # We make the decision here to return only the "attributes"
    # and leave out the "dn" field.
    return [json.loads(entry.entry_to_json())["attributes"] for entry in conn.entries]


def process_user(user_raw: dict) -> dict:
    """
    This takes a dict with a LOT of fields, as described by GEN-1744,
    and it uses only the following ones, which are renamed.
    Note that all entries from `user_raw` are lists, and we expect
    them to contain only one element at [0].

    mail[0]        -> mila_email_username  (includes the "@mila.quebec")
    posixUid[0]    -> mila_cluster_username
    uidNumber[0]   -> mila_cluster_uid
    gidNumber[0]   -> mila_cluster_gid
    displayName[0] -> display_name
    suspended[0]   -> status  (as string "enabled" or "disabled")

    It also asserts, as sanity check, that the entries for
    "googleUid" and "uid" match that of "mail" (except for
    the "@mila.quebec" suffix).
    """
    user = {
        # include the suffix "@mila.quebec"
        "mila_email_username": user_raw["mail"][0],
        "mila_cluster_username": user_raw["posixUid"][0],
        "mila_cluster_uid": user_raw["uidNumber"][0],
        "mila_cluster_gid": user_raw["gidNumber"][0],
        "display_name": user_raw["displayName"][0],
        "status": "disabled"
        if (user_raw["suspended"][0] in ["True", "true", True])
        else "enabled",
    }
    assert user_raw["mail"][0].startswith(user_raw["googleUid"][0])
    assert user_raw["mail"][0].startswith(user_raw["uid"][0])
    return user


def client_side_user_updates(LD_users_DB, LD_users_LDAP):
    """
    Instead of having complicated updates that depend on multiple MongoDB
    updates to cover all cases involving the "status" field, we'll do all
    that logic locally in this function.

    We have `LD_users_DB` from our database, we have `LD_users_LDAP` from
    our LDAP server, and then we return list of updates to be commited.

    Note that both `LD_users_DB` and `LD_users_LDAP` use the same fields.
    """
    # The first step is to index everything by unique id, which is
    # the "mila_email_username". This is because we'll be matching
    # entries from both lists and we want to avoid N^2 performance.

    DD_users_DB = dict((e["mila_email_username"], e) for e in LD_users_DB)
    DD_users_LDAP = dict((e["mila_email_username"], e) for e in LD_users_LDAP)

    LD_users_to_update_or_insert = []
    for meu in set(list(DD_users_DB.keys()) + list(DD_users_LDAP.keys())):
        # `meu` is short for the mila_email_username value

        if meu in DD_users_DB and not meu in DD_users_LDAP:
            # User is in DB but not in the LDAP.
            # Let's mark it as archived.
            entry = DD_users_DB[meu]
            entry["status"] = "archived"
        elif meu not in DD_users_DB and meu in DD_users_LDAP:
            # User is not in DB but is in the LDAP. That's a new entry!
            # If you need to enter some fields for the first time
            # when entering a new user, do it here.
            # As of right now, we have no need to do that.
            ## entry = DD_users_LDAP[meu]
            ## entry["some_unique_id_or_whatever"] = None
            pass
        else:
            entry = DD_users_DB[meu]

        assert "status" in entry  # sanity check
        LD_users_to_update_or_insert.append(entry)
    return LD_users_to_update_or_insert


def run(
    local_private_key_file=None,
    local_certificate_file=None,
    ldap_service_uri=None,
    mongodb_connection_string=None,
    mongodb_database=None,
    mongodb_collection=None,
    input_json_file=None,
    output_json_file=None,
    output_raw_LDAP_json_file=None,
    LD_users=None,  # for external testing purposes
):

    if LD_users is not None:
        # Used mostly for testing purposes.
        # Overrides the "input_json_file" argument.
        # Just make sure it's a list of dict, at least.
        assert isinstance(LD_users, list)
        if LD_users:
            assert isinstance(LD_users[0], dict)
    elif input_json_file:
        with open(input_json_file, "r") as f_in:
            LD_users = json.load(f_in)
    else:
        # this is the usual branch taken in practice
        LD_users_raw = query_ldap(
            local_private_key_file, local_certificate_file, ldap_service_uri
        )
        if output_raw_LDAP_json_file:
            with open(output_raw_LDAP_json_file, "w") as f_out:
                json.dump(LD_users_raw, f_out, indent=4)
                print(f"Wrote {output_raw_LDAP_json_file}.")

        LD_users = [process_user(D_user_raw) for D_user_raw in LD_users_raw]

    if mongodb_connection_string and mongodb_database and mongodb_collection:

        users_collection = MongoClient(mongodb_connection_string)[mongodb_database][
            mongodb_collection
        ]

        # The "enabled" component has to be dealt with differently.
        #
        # For a user that already exists in our database,
        #   - if the LDAP says "disabled", then we propagate that to our database;
        #   - if the LDAP says "enabled", then we ignore it.
        # For a user that is not in our database,
        #   - we go with whatever the LDAP says.

        LD_users_DB = list(users_collection.find())

        L_updated_users = client_side_user_updates(
            LD_users_DB=LD_users_DB, LD_users_LDAP=LD_users
        )

        L_updates_to_do = [
            UpdateOne(
                {"mila_email_username": updated_user["mila_email_username"]},
                {
                    # We set all the fields corresponding to the fields from `updated_user`,
                    # so that's a convenient way to do it. Note that this does not affect
                    # the fields in the database that are already present for that user.
                    "$set": updated_user,
                },
                upsert=True,
            )
            for updated_user in L_updated_users
        ]

        if L_updates_to_do:
            result = users_collection.bulk_write(
                L_updates_to_do
            )  #  <- the actual commit
            # print(result.bulk_api_result)

    if output_json_file:
        with open(output_json_file, "w") as f_out:
            json.dump(LD_users, f_out, indent=4)
            print(f"Wrote {output_json_file}.")


if __name__ == "__main__":

    args = parser.parse_args()
    run(
        local_private_key_file=args.local_private_key_file,
        local_certificate_file=args.local_certificate_file,
        ldap_service_uri=args.ldap_service_uri,
        mongodb_connection_string=args.mongodb_connection_string,
        mongodb_database=args.mongodb_database,
        mongodb_collection=args.mongodb_collection,
        input_json_file=args.input_json_file,
        output_json_file=args.output_json_file,
        output_raw_LDAP_json_file=args.output_raw_LDAP_json_file,
    )
