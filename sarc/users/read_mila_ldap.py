"""
What this script does
=====================

This script can be called directly from the command line (legacy),
or it can be called from the `sarc.users.acquire` script (recommended)
using the `sarc ...` command.

The legacy usage is not covered by unit tests, but the sarc command is covered.
It is part of a pipeline that will fetch the user data from the LDAP service,
do some processing on it, and then write it to a MongoDB instance.


The legacy usage is as follows:

Two mutually-exclusive ways to input the data, by priority:
   1) use the --input_json_file argument
   2) use the LDAP

Two ways to output the data, that can be used together:
   1) to the --output_json_file
   2) to a MongoDB instance


Note that the format used for the user entries in the collection
is the following.
See https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2190737548/Planification.
```
    {"mila_ldap": {
      "mila_email_username": "john.appleseed@mila.quebec",
      "mila_cluster_username": "applej",
      ...
    },
    "drac_roles": None,
    "drac_members": {
        "username": "johns",
        ...}
```
This `read_mila_ldap.py` script will update only the "mila_ldap" part of the entry.



Sample uses (legacy)
====================

Two ways this can be used:

::

    python3 read_mila_ldap.py \\
        --local_private_key_file ../SARC_secrets/secrets/Google_2026_01_26_66827.key \\
        --local_certificate_file ../SARC_secrets/secrets/Google_2026_01_26_66827.crt \\
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

import json
import logging
import os
import ssl
from datetime import timedelta
from pathlib import Path
from typing import cast

# Requirements
# - pip install ldap3
from ldap3 import ALL_ATTRIBUTES, SUBTREE, Connection, Server, Tls
from pymongo import UpdateOne
from pymongo.collection import Collection

from sarc.cache import CachePolicy, with_cache

from ..config import LDAPConfig
from .supervisor import resolve_supervisors

logger = logging.getLogger(__name__)


def _query_ldap(
    local_private_key_file: Path, local_certificate_file: Path, ldap_service_uri: str
) -> list[dict[str, list[str]]]:
    """
    Since we don't always query the LDAP (i.e. omitted when --input_json_file is given),
    we'll make this a separate function.
    """

    assert os.path.exists(local_private_key_file), (
        f"Missing local_private_key_file {local_private_key_file}."
    )
    assert os.path.exists(local_certificate_file), (
        f"Missing local_certificate_file {local_certificate_file}."
    )

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


# mypy is bad at resolving types across functions (it tries to type the
# with_cache call independently and fails because it need the type information
# of the decorated function to work) so we do this
query_ldap = with_cache(_query_ldap, subdirectory="users", validity=timedelta(days=1))


def process_user(user_raw: dict[str, list[str]]) -> dict[str, str | None]:
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

    supervisor = cast(str | None, user_raw.get("supervisor"))
    cosupervisor = cast(str | None, user_raw.get("co_supervisor"))

    user: dict[str, str | None] = {
        # include the suffix "@mila.quebec"
        "mila_email_username": user_raw["mail"][0],
        "mila_cluster_username": user_raw["posixUid"][0],
        "mila_cluster_uid": user_raw["uidNumber"][0],
        "mila_cluster_gid": user_raw["gidNumber"][0],
        "display_name": user_raw["displayName"][0],
        "supervisor": supervisor if supervisor else None,
        "co_supervisor": cosupervisor if cosupervisor else None,
        "status": (
            "disabled"
            if (user_raw["suspended"][0] in ["True", "true", True])
            else "enabled"
        ),
    }
    assert user_raw["mail"][0].startswith(user_raw["googleUid"][0])
    assert user_raw["mail"][0].startswith(user_raw["uid"][0])
    return user


def client_side_user_updates(
    LD_users_DB: list[dict[str, str | None]], LD_users_LDAP: list[dict[str, str | None]]
) -> list[dict[str, str | None]]:
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

        if meu in DD_users_DB and meu not in DD_users_LDAP:
            # User is in DB but not in the LDAP.
            # Let's mark it as archived.
            entry = DD_users_DB[meu]
            entry["status"] = "archived"
        else:
            # either User is not in DB but is in the LDAP (new entry)
            # or User is in both DB and LDAP. We'll consider the LDAP more up-to-date.
            # If you need to enter some fields for the first time
            # when entering a new user, do it here.
            # As of right now, we have no need to do that.
            entry = DD_users_LDAP[meu]

        LD_users_to_update_or_insert.append(entry)
    return LD_users_to_update_or_insert


def _save_to_mongo(
    collection: Collection | None, LD_users: list[dict[str, str | None]]
):
    if collection is None:
        return

    # read only the "mila_ldap" field from the entries, and ignore the
    # "drac_roles" and "drac_members" components
    LD_users_DB = [u["mila_ldap"] for u in list(collection.find())]

    L_updated_users = client_side_user_updates(
        LD_users_DB=LD_users_DB,
        LD_users_LDAP=LD_users,
    )

    L_updates_to_do = [
        UpdateOne(
            {"mila_ldap.mila_email_username": updated_user["mila_email_username"]},
            {
                # We set all the fields corresponding to the fields from `updated_user`,
                # so that's a convenient way to do it. Note that this does not affect
                # the fields in the database that are already present for that user.
                "$set": {"mila_ldap": updated_user},
            },
            upsert=True,
        )
        for updated_user in L_updated_users
    ]

    if L_updates_to_do:
        result = collection.bulk_write(L_updates_to_do)  #  <- the actual commit
        logger.info(result.bulk_api_result)


def load_ldap_exceptions(ldap_config: LDAPConfig) -> dict[str, list[str]]:
    if ldap_config.exceptions_json_path is None:
        return {}

    with open(ldap_config.exceptions_json_path, "r", encoding="utf-8") as file:
        return json.load(file)


def load_group_to_prof_mapping(ldap_config: LDAPConfig) -> dict[str, str]:
    if ldap_config.group_to_prof_json_path is None:
        return {}

    with open(ldap_config.group_to_prof_json_path, "r", encoding="utf-8") as file:
        return json.load(file)


def fetch_ldap(
    ldap: LDAPConfig, cache_policy=CachePolicy.use
) -> list[dict[str, str | None]]:
    # Retrieve users from LDAP
    LD_users_raw = query_ldap(
        ldap.local_private_key_file,
        ldap.local_certificate_file,
        ldap.ldap_service_uri,
        cache_policy=cache_policy,
    )

    # Transform users into the json we will save
    group_to_prof = load_group_to_prof_mapping(ldap)
    exceptions = load_ldap_exceptions(ldap)
    errors = resolve_supervisors(LD_users_raw, group_to_prof, exceptions)

    LD_users = [process_user(D_user_raw) for D_user_raw in LD_users_raw]

    errors.show()
    return LD_users


def run(
    ldap: LDAPConfig,
    mongodb_collection: Collection | None = None,
    output_json_file: Path | None = None,
    cache_policy: CachePolicy = CachePolicy.use,
) -> None:
    """Runs periodically to synchronize mongodb with LDAP"""

    LD_users = fetch_ldap(ldap, cache_policy=cache_policy)

    _save_to_mongo(mongodb_collection, LD_users)

    if output_json_file:
        with open(output_json_file, "w", encoding="utf-8") as f_out:
            json.dump(LD_users, f_out, indent=4)
            logger.info(f"Wrote {output_json_file}.")
