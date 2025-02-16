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
import csv
import json
import logging
from pathlib import PosixPath

from sarc.account_matching import name_distances


def load_data_from_files(data_paths):
    """
    Takes in a dict of paths to data files, and returns a dict of the data.
        data_paths = {
            "mila_ldap": mila_ldap_path,
            "drac_members": drac_members_path,
            "drac_roles": drac_roles_path,
        }
    Returns
        data = {
            "mila_ldap": [...],  # list of dicts
            "drac_members": [...], # list of dicts
            "drac_roles": [...],   # list of dicts
        }

    In cases where the data is already a list and not a path,
    we can just return it directly.
    """

    def dict_to_lowercase(D):
        return dict((k.lower(), v) for (k, v) in D.items())

    data = {}
    for k, v in data_paths.items():
        if isinstance(v, list):
            # pass through
            data[k] = [dict_to_lowercase(D) for D in v]
        else:
            with open(v, "r", encoding="utf-8") as f_in:
                if (isinstance(v, str) and v.endswith("csv")) or (
                    isinstance(v, PosixPath) and v.suffix == ".csv"
                ):
                    data[k] = [dict_to_lowercase(D) for D in csv.DictReader(f_in)]
                elif (isinstance(v, str) and v.endswith("json")) or (
                    isinstance(v, PosixPath) and v.suffix == ".json"
                ):
                    data[k] = json.load(f_in)
                else:
                    raise ValueError(f"Unknown file type for {v}")
    return data


def perform_matching(
    # pylint: disable=too-many-branches
    DLD_data: dict[str, list[dict]],
    mila_emails_to_ignore: list[str],
    override_matches_mila_to_cc: dict[str, str],
    name_distance_delta_threshold=2,  # mostly for testing
    verbose=False,
    prompt=False,
):
    """
    This is the function with the core functionality.
    The rest is just a wrapper. At this point, this function
    is also independent of SARC in that it does not need to
    fetch things from the `config` or to the `database`.
    All the SARC-related tasks are done outside of this function.

    If `prompt` is True, a command-line prompt will be provided
    everywhere manual matching is required.

    Returns a couple containing:
    - a dict of dicts, indexed by @mila.quebec email addresses,
      and containing entries of the form
        {"mila_ldap": {...}, "drac_roles": {...}, "drac_members": {...}}
    - a dict of new manual matches occurred during matching,
      mapping a mila email to a DRAC username. As manual
      matches rely on prompt, dict will be empty if prompt is False.
    """

    # because this function feels entitled to modify the input data
    # set some things to lower case, for example
    DLD_data = copy.deepcopy(DLD_data)

    for k in DLD_data:
        assert k in {"mila_ldap", "drac_members", "drac_roles"}

    S_mila_emails_to_ignore = set(mila_emails_to_ignore)
    # The drac_account_username in `override_matches_mila_to_cc`
    # refers to values found in the "drac_members" data source.

    # Filter out the "drac_members" whose "Activation_Status" is "older_deactivated" or "expired".
    # These accounts might not have members present in the Mila LDAP.
    if "drac_members" in DLD_data:
        # because "John.Appleseed@mila.quebec" wrote their email with uppercases
        for e in DLD_data["drac_members"]:
            e["email"] = e["email"].lower()

    if "drac_roles" in DLD_data:
        DLD_data["drac_roles"] = [
            D for D in DLD_data["drac_roles"] if D["status"].lower() in ["activated"]
        ]
        # because "John.Appleseed@mila.quebec" wrote their email with uppercases
        for e in DLD_data["drac_roles"]:
            e["email"] = e["email"].lower()

    # Dict indexed by @mila.quebec email addresses
    # with 3 subdicts : "mila_ldap", "drac_roles", "drac_members"
    # that contains all the information that we could match.
    DD_persons = {}
    for D in DLD_data["mila_ldap"]:
        DD_persons[D["mila_email_username"]] = {}
        DD_persons[D["mila_email_username"]]["mila_ldap"] = D
        # filling those more for documentation purposes than anything
        DD_persons[D["mila_email_username"]]["drac_roles"] = None
        DD_persons[D["mila_email_username"]]["drac_members"] = None

    ######################################
    ## and now we start matching things ##
    ######################################

    for drac_source in ["drac_members", "drac_roles"]:
        if drac_source not in DLD_data:
            # we might not have all three source files
            if verbose:
                logging.warning(f"{drac_source} file missing !")
            continue
        LD_members = _how_many_drac_accounts_with_mila_emails(
            DLD_data, drac_source, verbose=verbose
        )
        for D_member in LD_members:
            assert D_member["email"].endswith("@mila.quebec")
            if D_member["email"] in S_mila_emails_to_ignore:
                if verbose:
                    logging.info(f'Ignoring phantom {D_member["email"]} (ignore list).')
                continue
            if D_member["email"] not in DD_persons:
                # we WANT to create an entry in DD_persons with the mila username, and the name from the cc_source !
                if verbose:
                    logging.info(
                        f'Creating phantom profile for {D_member["email"]} (automatic).'
                    )
                DD_persons[D_member["email"]] = {}
                mila_ldap = {}
                mila_ldap["mila_email_username"] = D_member["email"]
                mila_ldap["mila_cluster_username"] = D_member["email"].split("@")[0]
                mila_ldap["mila_cluster_uid"] = "0"
                mila_ldap["mila_cluster_gid"] = "0"
                for name_field in ["name", "nom"]:
                    if name_field in D_member:
                        mila_ldap["display_name"] = D_member[name_field]
                        continue
                mila_ldap["status"] = "unknown"
                DD_persons[D_member["email"]]["mila_ldap"] = mila_ldap
                DD_persons[D_member["email"]]["drac_members"] = None
                DD_persons[D_member["email"]]["drac_roles"] = None
            DD_persons[D_member["email"]][drac_source] = D_member

    # We have 206 drac_members accounts with @mila.quebec, out of 610.
    # We have 42 drac_roles accounts with @mila.quebec, out of 610.

    # Matching.
    new_manual_matches = _matching_names(
        DLD_data, DD_persons, name_distance_delta_threshold, prompt
    )

    # NB: In any case (even with prompt), match overriding is applied.
    # This means that even a manually-prompted matching may be overriden
    # if related mila username is present in override_matches_mila_to_cc.
    # Is it what we want ?
    _manual_matching(DLD_data, DD_persons, override_matches_mila_to_cc)

    if verbose:
        _make_matches_status_report(DLD_data, DD_persons)

    return DD_persons, new_manual_matches


def _matching_names(DLD_data, DD_persons, name_distance_delta_threshold, prompt=False):
    """
    Substep of the `perform_matching` function.
    Mutates the entries of `DD_persons` in-place.
    First argument names are the same as in the body of `perform_matching`.
    If `prompt` is True, a prompt is provided to solve ambiguous cases.

    Return a dictionary of manual matches,
    mapping a mila email to manually-associated DRAC username.
    """

    mila_email_to_cc_username = {}

    for name_or_nom, cc_source in [("name", "drac_members"), ("nom", "drac_roles")]:
        # Get 10 best matches for each mila display name.
        LP_best_name_matches = name_distances.find_best_word_matches(
            [e["display_name"] for e in DLD_data["mila_ldap"]],
            [e[name_or_nom] for e in DLD_data[cc_source]],
            nb_best_matches=10,
        )

        # Get best match for each mila display name.
        for mila_display_name, best_matches in LP_best_name_matches:
            match_is_manual = False

            # Try to make match if we find only 1 match <= threshold.
            matches_under_threshold = [
                match
                for match in best_matches
                if match[0] <= name_distance_delta_threshold
            ]
            if len(matches_under_threshold) == 1:
                cc_match = matches_under_threshold[0][1]
            # Otherwise, prompt if allowed (manual match).
            elif prompt:
                cc_match = _prompt_manual_match(
                    mila_display_name, cc_source, [match[1] for match in best_matches]
                )
                match_is_manual = True

            # Else, do not match.
            else:
                cc_match = None

            if cc_match is not None:
                # A match was selected.

                # Find which entry of `DD_persons` corresponds to `mila_display_name`
                D_person_found = [
                    D_person
                    for D_person in DD_persons.values()
                    if D_person["mila_ldap"]["display_name"] == mila_display_name
                ][0]
                # Find match that corresponds to `cc_match`.
                match = [e for e in DLD_data[cc_source] if e[name_or_nom] == cc_match][
                    0
                ]
                prev_match_data = D_person_found.get(cc_source, None)
                # If user already had a match,
                # make sure previous and new match do have same name.
                if prev_match_data is not None:
                    assert prev_match_data[name_or_nom] == cc_match
                # Update new match anyway.
                D_person_found[cc_source] = match

                # If match is manual, save it in output dictionary.
                if match_is_manual:
                    mila_email = D_person_found["mila_ldap"]["mila_email_username"]
                    cc_username = match["username"]
                    mila_email_to_cc_username[mila_email] = cc_username

                del D_person_found

    return mila_email_to_cc_username


def _prompt_manual_match(mila_display_name, cc_source, best_matches):
    """
    Sub-step of `_matching_names_with_prompt`

    Prompt script user to select a `cc_source` match for `mila_display_name`
    in `best_matches` choices.

    Return selected match, or None if script user did not make a choice.
    """
    prompt_message = (
        f"\n"
        f"Ambiguous {cc_source}. "
        f"Type a number to choose match for: {mila_display_name} "
        f"(default: matching ignored):\n"
        + "\n".join(f"[{i}] {match}" for i, match in enumerate(best_matches))
        + "\n"
    )

    # Loop as long as we don't get a valid prompt.
    while True:
        prompted_answer = input(prompt_message).strip()
        try:
            if prompted_answer:
                # Parse input if available.
                index_match = int(prompted_answer)
                cc_match = best_matches[index_match]
            else:
                # Otherwise, match is ignored.
                cc_match = None
            break
        except (ValueError, IndexError) as exc:
            # We may get a value error from parsing,
            # or an index error when selecting a match.
            print("Invalid index:", exc)
            # Re-prompt.

    if cc_match:
        logging.info(f"[prompt] {mila_display_name} (matched with) {cc_match}")
    else:
        logging.info(f"[prompt] {mila_display_name} (ignored)")

    return cc_match


def _manual_matching(DLD_data, DD_persons, override_matches_mila_to_cc):
    """
    Substep of the `perform_matching` function.
    Mutates the entries of `DD_persons` in-place.
    All argument names are the same as in the body of `perform_matching`.
    """

    # Finally, the manual matching that overrides everything.
    # If we ever supplied values in `override_matches_mila_to_cc`,
    # those are of the form
    #     {"overrido.dudette@mila.quebec": "duddirov"}
    # where "overrido.dudette@mila.quebec" is the Mila LDAP email address
    # and "duddirov" is the CC account username.
    #
    # We do this thing both for the "drac_members" and "drac_roles" sources.

    for drac_source in ["drac_members", "drac_roles"]:
        assert drac_source in DLD_data
        matching = dict((e["username"], e) for e in DLD_data[drac_source])
        for (
            mila_email_username,
            drac_account_username,
        ) in override_matches_mila_to_cc.items():
            if mila_email_username not in DD_persons:
                raise ValueError(
                    f'"{mila_email_username}" is not found in the actual sources.'
                    "This was supplied to `override_matches_mila_to_cc` in the `make_matches.py` file, "
                    f"but there are not such entries in LDAP.\n"
                    "Someone messed up the manual matching by specifying a Mila email username that does not exist."
                )
            # Note that `matching[drac_account_username]` is itself a dict
            # with user information from CC. It's not just a username string.
            if drac_account_username in matching:
                assert isinstance(matching[drac_account_username], dict)
                DD_persons[mila_email_username][drac_source] = matching[
                    drac_account_username
                ]


def _make_matches_status_report(DLD_data, DD_persons):
    """
    This function exists for the sole purpose of shortening
    the body of `perform_matching`.
    """

    ###########################
    ###### status report ######
    ###########################

    (good_count, bad_count, enabled_count, disabled_count) = (0, 0, 0, 0)
    for D_person in DD_persons.values():
        if D_person["mila_ldap"]["status"] == "enabled":
            if (
                D_person["drac_members"] is not None
                or D_person["drac_roles"] is not None
            ):
                good_count += 1
            else:
                bad_count += 1
            enabled_count += 1
        else:
            disabled_count += 1

    logging.info(
        f"We have {enabled_count} enabled accounts and {disabled_count} disabled accounts."
    )
    logging.info(
        f"Out of those enabled accounts, there are {good_count} successful matches "
        f"and {bad_count} failed matches."
    )

    # Report on how many of the CC entries couldn't be matches to mila LDAP.

    if "drac_members" in DLD_data:
        count_drac_members_activated = len(
            [
                D
                for D in DLD_data["drac_members"]
                if D["activation_status"] in ["activated"]
            ]
        )
        logging.info(f"We have {count_drac_members_activated} activated drac_members.")

        # let's try to be more precise about things to find the missing accounts
        set_A = {
            D_member["email"]
            for D_member in DLD_data["drac_members"]
            if D_member["activation_status"] in ["activated"]
        }
        set_B = {
            D_person["drac_members"].get("email", None)
            for D_person in DD_persons.values()
            if D_person.get("drac_members", None) is not None
        }
        logging.info(
            "We could not find matches in the Mila LDAP for the CC accounts "
            f"associated with the following emails: {set_A.difference(set_B)}."
        )

    # see "account_matching.md" for some explanations on the edge cases handled

    if "drac_roles" in DLD_data:
        count_drac_roles_activated = len(
            [D for D in DLD_data["drac_roles"] if D["status"].lower() in ["activated"]]
        )
        logging.info(f"We have {count_drac_roles_activated} activated drac_roles.")


def _how_many_drac_accounts_with_mila_emails(
    data, drac_source="drac_members", verbose=False
):
    assert drac_source in data
    LD_members = [
        D_member
        for D_member in data[drac_source]
        if D_member.get("email", "").endswith("@mila.quebec")
    ]

    if verbose:
        logging.info(
            f"We have {len(LD_members)} {drac_source} accounts with @mila.quebec, "
            f"out of {len(data['drac_members'])}."
        )
    return LD_members
