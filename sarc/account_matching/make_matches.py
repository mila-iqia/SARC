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


import numpy as np
import json
import csv
import os
from collections import defaultdict
import argparse
from sarc.account_matching import name_distances

from pymongo import MongoClient, UpdateOne


parser = argparse.ArgumentParser(
    description="Make matches with Mila LDAP accounts and CC accounts. Update the MongoDB database users based on values returned."
)
# This config contains the following keys:
#     'L_phantom_mila_emails_to_ignore'
#     'D_override_matches_mila_to_cc_account_username'
# which were handcrafted based on weird edge cases
# that we got when we first wrote this script.
parser.add_argument(
    "--config_path",
    type=str,
    default="secrets/account_matching/make_matches_config.json",
    help="JSON file that contains the 'L_phantom_mila_emails_to_ignore' and 'D_override_matches_mila_to_cc_account_username' keys.",
)
parser.add_argument(
    "--mila_ldap_path",
    type=str,
    default="secrets/account_matching/2022-11-26_mila_users.json",
    help="Output from read_mila_ldap.py run at previous step.",
)
parser.add_argument(
    "--cc_members_path",
    type=str,
    default="secrets/account_matching/members-rrg-bengioy-ad-2022-11-25.csv",
    help="Downloaded CSV file from CC/DRAC.",
)
parser.add_argument(
    "--cc_roles_path",
    type=str,
    default="secrets/account_matching/sponsored_roles_for_Yoshua_Bengio_(CCI_jvb-000).csv",
    help="Downloaded CSV file from CC/DRAC.",
)
# Please be careful here because we currently don't have proper
# source-version control for the secrets, and we don't want to
# overwrite the tried-and-true "matches_done.json" file
# when running this code for fun.
parser.add_argument(
    "--output_path",
    type=str,
    default="secrets/account_matching/matches_done.json",
    help="local_private_key_file for LDAP connection",
)


def run(config_path, mila_ldap_path, cc_members_path, cc_roles_path, output_path):

    data_paths = {
        "mila_ldap": mila_ldap_path,
        "cc_members": cc_members_path,
        "cc_roles": cc_roles_path,
    }

    with open(config_path, "r") as f_in:
        config = json.load(f_in)
        S_phantom_mila_emails_to_ignore = set(config["L_phantom_mila_emails_to_ignore"])
        D_override_matches_mila_to_cc_account_username = config[
            "D_override_matches_mila_to_cc_account_username"
        ]
        del config
    # The cc_account_username in `D_override_matches_mila_to_cc_account_username`
    # refers to values found in the "cc_members" data source.

    def dict_to_lowercase(D):
        return dict((k.lower(), v) for (k, v) in D.items())

    data = {}
    for (k, v) in data_paths.items():
        with open(v, "r") as f_in:
            if v.endswith("csv"):
                data[k] = [dict_to_lowercase(D) for D in csv.DictReader(f_in)]
            elif v.endswith("json"):
                data[k] = json.load(f_in)

    # Filter out the "cc_members" whose "Activation_Status" is "older_deactivated" or "expired".
    # These accounts might not have members present in the Mila LDAP.
    # ex: francis.gregoire@mila.quebec is "older_deactivated"
    #     luccionis@mila.quebec is "expired"
    if "cc_members" in data:
        data["cc_members"] = [
            D
            for D in data["cc_members"]
            if D["activation_status"] not in ["older_deactivated", "expired"]
        ]
        # because "Arian.Khorasani@mila.quebec" wrote their email with uppercases
        for e in data["cc_members"]:
            e["email"] = e["email"].lower()

    if "cc_roles" in data:
        data["cc_roles"] = [
            D for D in data["cc_roles"] if D["status"].lower() in ["activated"]
        ]
        # because "Arian.Khorasani@mila.quebec" wrote their email with uppercases
        for e in data["cc_roles"]:
            e["email"] = e["email"].lower()

    # Dict indexed by @mila.quebec email addresses
    # with 3 subdicts : "mila_ldap", "cc_roles", "cc_members"
    # that contains all the information that we could match.
    DD_persons = {}
    for D in data["mila_ldap"]:
        DD_persons[D["mila_email_username"]] = {}
        DD_persons[D["mila_email_username"]]["mila_ldap"] = D
        # filling those more for documentation purposes than anything
        DD_persons[D["mila_email_username"]]["cc_roles"] = None
        DD_persons[D["mila_email_username"]]["cc_members"] = None

    ######################################
    ## and now we start matching things ##
    ######################################

    for key in ["cc_members", "cc_roles"]:
        if key not in data:
            # we might not have all three source files
            continue
        LD_members = how_many_cc_accounts_with_mila_emails(data, key)
        for D_member in LD_members:
            assert D_member["email"].endswith("@mila.quebec")
            if D_member["email"] in S_phantom_mila_emails_to_ignore:
                print(f'Ignoring phantom {D_member["email"]}.')
                continue
            DD_persons[D_member["email"]][key] = D_member
    # We have 206 cc_members accounts with @mila.quebec, out of 610.
    # We have 42 cc_roles accounts with @mila.quebec, out of 610.

    for (name_or_nom, key) in [("name", "cc_members"), ("nom", "cc_roles")]:
        LP_name_matches = name_distances.find_exact_bag_of_words_matches(
            [e[name_or_nom] for e in data[key]],
            [e["display_name"] for e in data["mila_ldap"]],
        )
        for (a, b, delta) in LP_name_matches:
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
            match = [e for e in data[key] if e[name_or_nom] == a][0]

            # Matching names is less of a strong association than
            # matching emails, so let's not mess things up by overwriting
            # one by the other. It would still be interesting to report
            # divergences here, where emails suggest a match that names don't.
            if D_person_found.get(key, None) is not None:
                # TODO : sanity check here if you want
                # assert D_person_found[key] == match  # this?
                continue
            else:
                D_person_found[key] = match

    assert "cc_members" in data
    matching = dict((e["username"], e) for e in data["cc_members"])
    # The manual matching done previously by me.
    for (
        mila_email_username,
        cc_account_username,
    ) in D_override_matches_mila_to_cc_account_username.items():
        # If a key is missing here, it's because we messed up by writing
        # by hand the values in `D_override_matches_mila_to_cc_account_username`.
        DD_persons[mila_email_username]["cc_members"] = matching[cc_account_username]

    ###########################
    ###### status report ######
    ###########################

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
        f"Out of those enabled accounts, there are {good_count} successful matches and {bad_count} failed matches."
    )

    # TODO : report on how many of the CC entries couldn't be matches to mila LDAP

    if "cc_members" in data:
        count_cc_members_activated = len(
            [D for D in data["cc_members"] if D["activation_status"] in ["activated"]]
        )
        print(f"We have {count_cc_members_activated} activated cc_members.")

        # let's try to be more precise about things to find the missing accounts
        set_A = set(
            [
                D_member["email"]
                for D_member in data["cc_members"]
                if D_member["activation_status"] in ["activated"]
            ]
        )
        set_B = set(
            [
                D_person["cc_members"].get("email", None)
                for D_person in DD_persons.values()
                if D_person.get("cc_members", None) is not None
            ]
        )
        print(
            f"We could not find matches in the Mila LDAP for the CC accounts associated with the following emails: {set_A.difference(set_B)}."
        )

    # see "account_matching.md" for some explanations on the edge cases handled

    if "cc_roles" in data:
        count_cc_roles_activated = len(
            [D for D in data["cc_roles"] if D["status"].lower() in ["activated"]]
        )
        print(f"We have {count_cc_roles_activated} activated cc_roles.")

    with open(output_path, "w") as f_out:
        json.dump(DD_persons, f_out, indent=2)
        print(f"Wrote {output_path}.")


def how_many_cc_accounts_with_mila_emails(data, key="cc_members"):
    assert key in data
    LD_members = [
        D_member
        for D_member in data[key]
        if D_member.get("email", "").endswith("@mila.quebec")
    ]

    print(
        f"We have {len(LD_members)} {key} accounts with @mila.quebec, out of {len(data['cc_members'])}."
    )
    return LD_members


if __name__ == "__main__":
    args = parser.parse_args()
    run(
        config_path=args.config_path,
        mila_ldap_path=args.mila_ldap_path,
        cc_members_path=args.cc_members_path,
        cc_roles_path=args.cc_roles_path,
        output_path=args.output_path,
    )
