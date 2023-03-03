"""
This script will ingest three different sources of data and will produce
a JSON file called "matches_done.json" that will contain a list of
all the possible matches between Mila users and CC/DRAC accounts.

There are a lot of irregularities in the worlds of accounts coming from Mila
and from DRAC, so this script needs to have many exceptions written somewhere.
Because those exceptions involve names of Mila members and their email addresses,
we have to keep this private, despite how convenient it would be at times to
have some statements like `if name == "johnappleseed@mila.quebec"`.

These all need to come from a specific configuration file, and that file is include
with the other secrets in the "secrets/account_matching" directory.
"""

import copy
import argparse
import csv
import json
import os
from collections import defaultdict

import numpy as np
# from pymongo import MongoClient, UpdateOne

from sarc.account_matching import name_distances

def _load_data_from_files(data_paths):
    """
    Takes in a dict of paths to data files, and returns a dict of the data.
        data_paths = {
            "mila_ldap": mila_ldap_path,
            "cc_members": cc_members_path,
            "cc_roles": cc_roles_path,
        }
    Returns
        data = {
            "mila_ldap": [...],  # list of dicts
            "cc_members": [...], # list of dicts
            "cc_roles": [...],   # list of dicts
        }
    """

    def dict_to_lowercase(D):
        return dict((k.lower(), v) for (k, v) in D.items())

    data = {}
    for k, v in data_paths.items():
        with open(v, "r", encoding='utf-8') as f_in:
            if v.endswith("csv"):
                data[k] = [dict_to_lowercase(D) for D in csv.DictReader(f_in)]
            elif v.endswith("json"):
                data[k] = json.load(f_in)
    return data


def run(config_path, mila_ldap_path, cc_members_path, cc_roles_path, output_path):

    data_paths = {
        "mila_ldap": mila_ldap_path,
        "cc_members": cc_members_path,
        "cc_roles": cc_roles_path,
    }

    DD_persons = perform_matching(config_path, _load_data_from_files(data_paths))
    
    with open(output_path, "w", encoding='utf-8') as f_out:
        json.dump(DD_persons, f_out, indent=2)
        print(f"Wrote {output_path}.")

    # maybe add something to commit the database here?



def perform_matching(DLD_data: dict[str, list[dict]],
                     mila_emails_to_ignore: list[str],
                     override_matches_mila_to_cc: dict[str, str],
                     verbose=False):
    """
    This is the function with the core functionality.
    The rest is just a wrapper. At this point, this function
    is also independent of SARC in that it does not need to
    fetch things from the `config` or to the `database`.
    All the SARC-related tasks are done outside of this function.

    Returns a dict of dicts, indexed by @mila.quebec email addresses,
    and containing entries of the form
        {"mila_ldap": {...}, "cc_roles": {...}, "cc_members": {...}}
    """

    # because this function feels entitled to modify the input data
    # set some things to lower case, for example
    DLD_data = copy.deepcopy(DLD_data)

    for k in DLD_data:
        assert k in {"mila_ldap", "cc_members", "cc_roles"}


    S_mila_emails_to_ignore = set(mila_emails_to_ignore)
    # The cc_account_username in `override_matches_mila_to_cc`
    # refers to values found in the "cc_members" data source.


    # Filter out the "cc_members" whose "Activation_Status" is "older_deactivated" or "expired".
    # These accounts might not have members present in the Mila LDAP.
    if "cc_members" in DLD_data:
        DLD_data["cc_members"] = [
            D
            for D in DLD_data["cc_members"]
            if D["activation_status"] not in ["older_deactivated", "expired"]
        ]
        # because "John.Appleseed@mila.quebec" wrote their email with uppercases
        for e in DLD_data["cc_members"]:
            e["email"] = e["email"].lower()

    if "cc_roles" in DLD_data:
        DLD_data["cc_roles"] = [
            D for D in DLD_data["cc_roles"] if D["status"].lower() in ["activated"]
        ]
        # because "John.Appleseed@mila.quebec" wrote their email with uppercases
        for e in DLD_data["cc_roles"]:
            e["email"] = e["email"].lower()

    # Dict indexed by @mila.quebec email addresses
    # with 3 subdicts : "mila_ldap", "cc_roles", "cc_members"
    # that contains all the information that we could match.
    DD_persons = {}
    for D in DLD_data["mila_ldap"]:
        DD_persons[D["mila_email_username"]] = {}
        DD_persons[D["mila_email_username"]]["mila_ldap"] = D
        # filling those more for documentation purposes than anything
        DD_persons[D["mila_email_username"]]["cc_roles"] = None
        DD_persons[D["mila_email_username"]]["cc_members"] = None

    ######################################
    ## and now we start matching things ##
    ######################################

    for key in ["cc_members", "cc_roles"]:
        if key not in DLD_data:
            # we might not have all three source files
            continue
        LD_members = _how_many_cc_accounts_with_mila_emails(DLD_data, key)
        for D_member in LD_members:
            assert D_member["email"].endswith("@mila.quebec")
            if D_member["email"] in S_mila_emails_to_ignore:
                print(f'Ignoring phantom {D_member["email"]}.')
                continue
            DD_persons[D_member["email"]][key] = D_member
    # We have 206 cc_members accounts with @mila.quebec, out of 610.
    # We have 42 cc_roles accounts with @mila.quebec, out of 610.

    for name_or_nom, key in [("name", "cc_members"), ("nom", "cc_roles")]:
        LP_name_matches = name_distances.find_exact_bag_of_words_matches(
            [e[name_or_nom] for e in DLD_data[key]],
            [e["display_name"] for e in DLD_data["mila_ldap"]],
        )
        for a, b, delta in LP_name_matches:
            if delta > 2:
                # let's skip those
                print(f"Skipped ({a}, {b}) because the delta is too large.")
                continue
            # Again with the O(N^2) matching.
            for D_person in DD_persons.values():
                if D_person["mila_ldap"]["display_name"] == b:
                    D_person_found = D_person
                    break
            # We know for FACT that this person is in there,
            # so when we `break`, then `D_person_found` is assigned.
            # It becomes the insertion point.

            # Again a strange construct that works because we know that
            # there is a match in there with `e[name_or_nom] == a` because
            # that's actually how we got it.
            match = [e for e in DLD_data[key] if e[name_or_nom] == a][0]

            # Matching names is less of a strong association than
            # matching emails, so let's not mess things up by overwriting
            # one by the other. It would still be interesting to report
            # divergences here, where emails suggest a match that names don't.
            if D_person_found.get(key, None) is None:
                # Note that this is different from `if key not in D_person_found:`.
                D_person_found[key] = match
            #else:
                # You can uncomment this to see the divergences,
                # but usually you don't want to see them.
                # This can be uncommented when we're doing the manual matching.
                # assert D_person_found[key] == match  # optional


    assert "cc_members" in DLD_data
    matching = dict((e["username"], e) for e in DLD_data["cc_members"])
    # The manual matching done previously by me.
    for (
        mila_email_username,
        cc_account_username,
    ) in override_matches_mila_to_cc.items():
        # If a key is missing here, it's because we messed up by writing
        # by hand the values in `override_matches_mila_to_cc`.
        DD_persons[mila_email_username]["cc_members"] = matching[cc_account_username]

    ###########################
    ###### status report ######
    ###########################

    if verbose:

        (good_count, bad_count, enabled_count, disabled_count) = (0, 0, 0, 0)
        for D_person in DD_persons.values():
            if D_person["mila_ldap"]["status"] == "enabled":
                if D_person["cc_members"] is not None or D_person["cc_roles"] is not None:
                    good_count += 1
                else:
                    bad_count += 1
                enabled_count += 1
            else:
                disabled_count += 1

        print(
            f"We have {enabled_count} enabled accounts and {disabled_count} disabled accounts."
        )
        print(
            f"Out of those enabled accounts, there are {good_count} successful matches "
            "and {bad_count} failed matches."
        )

        # Report on how many of the CC entries couldn't be matches to mila LDAP.

        if "cc_members" in DLD_data:
            count_cc_members_activated = len(
                [D for D in DLD_data["cc_members"] if D["activation_status"] in ["activated"]]
            )
            print(f"We have {count_cc_members_activated} activated cc_members.")

            # let's try to be more precise about things to find the missing accounts
            set_A = {
                    D_member["email"]
                    for D_member in DLD_data["cc_members"]
                    if D_member["activation_status"] in ["activated"]
            }
            set_B = {
                    D_person["cc_members"].get("email", None)
                    for D_person in DD_persons.values()
                    if D_person.get("cc_members", None) is not None
            }
            print(
                "We could not find matches in the Mila LDAP for the CC accounts "
                f"associated with the following emails: {set_A.difference(set_B)}."
            )

        # see "account_matching.md" for some explanations on the edge cases handled

        if "cc_roles" in DLD_data:
            count_cc_roles_activated = len(
                [D for D in DLD_data["cc_roles"] if D["status"].lower() in ["activated"]]
            )
            print(f"We have {count_cc_roles_activated} activated cc_roles.")

    # end of status report

    return DD_persons



def _how_many_cc_accounts_with_mila_emails(data, key="cc_members"):
    assert key in data
    LD_members = [
        D_member
        for D_member in data[key]
        if D_member.get("email", "").endswith("@mila.quebec")
    ]

    print(
        f"We have {len(LD_members)} {key} accounts with @mila.quebec, "
        "out of {len(data['cc_members'])}."
    )
    return LD_members
