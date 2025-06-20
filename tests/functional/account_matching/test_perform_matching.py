import io

from sarc.account_matching.make_matches import perform_matching


def helper_extract_three_account_sources_from_ground_truth(account_matches):
    # Not accessing the database here. Just testing the matching algorithm.
    # Start with the ground truth, and then we'll split it into 3 parts
    # to see if it can be recovered.

    DLD_data = {"mila_ldap": [], "drac_members": [], "drac_roles": []}
    for _, data in account_matches.items():
        for k, l in DLD_data.items():  # "mila_ldap", "drac_members", "drac_roles"
            if data[k] is not None:
                l.append(data[k])

    # as described in test fixture
    mila_emails_to_ignore = ["ignoramus.mikey@mila.quebec"]
    # note that the point here is that this entry would never
    # get matched other if it wasn't for the override
    override_matches_mila_to_drac = {"overrido.dudette@mila.quebec": "duddirov"}

    # Add an entry for "ignoramus.mikey@mila.quebec" in "drac_members"
    # which is expected to be ignored by the matching algorithm
    # because we'll tell it to ignore it.
    # Note that we would still match against the "name" field
    # even if we ignored the email.
    DLD_data["drac_members"].append(
        {
            "rapi": "jvb-000-ag",
            "groupname": "rrg-bengioy-ad",
            "name": "Mikey the Man",
            "position": "Étudiant au doctorat",
            "institution": "Un. de Montréal",
            "department": "Informatique Et Recherche Opérationnelle",
            "sponsor": "Yoshua Bengio",
            "permission": "Member",
            "activation_status": "activated",
            "username": "appjohn",
            "ccri": "abc-002-10",
            "email": "ignoramus.mikey@mila.quebec",  # <-- this is the email we'll ignore
            "member_since": "2018-10-10 10:10:10 -0400",
        }
    )

    return DLD_data, mila_emails_to_ignore, override_matches_mila_to_drac


def test_perform_matching(account_matches):
    (
        DLD_data,
        mila_emails_to_ignore,
        override_matches_mila_to_cc,
    ) = helper_extract_three_account_sources_from_ground_truth(account_matches)

    DD_persons, new_matches = perform_matching(
        DLD_data,
        mila_emails_to_ignore=mila_emails_to_ignore,
        override_matches_mila_to_cc=override_matches_mila_to_cc,
        verbose=False,
    )

    assert account_matches == DD_persons

    assert not new_matches

    # for mila_email_username in DD_persons:
    #    # source_name in "mila_ldap", "drac_members", "drac_roles
    #    for source_name in DD_persons[mila_email_username]:
    #        # even when one entry is `None`, the other should also be `None` instead of being absent
    #        assert DD_persons[mila_email_username] == account_matches[mila_email_username][source_name]


# For later work:
# If you want to add a test, you can add one that makes good use
# of `mila_emails_to_ignore` and `override_matches_mila_to_cc`
# and ends up with a false positive and a false negative.


def test_perform_matching_with_bad_email_capitalization(account_matches):
    (
        DLD_data,
        mila_emails_to_ignore,
        override_matches_mila_to_cc,
    ) = helper_extract_three_account_sources_from_ground_truth(account_matches)

    assert isinstance(DLD_data["drac_members"], list)
    assert isinstance(DLD_data["drac_members"][0], dict)

    # mess up the capitalization of some email to see if it still matches
    DLD_data["drac_members"][0]["email"] = DLD_data["drac_members"][0]["email"].upper()
    DLD_data["drac_roles"][1]["email"] = DLD_data["drac_roles"][1]["email"].upper()

    DD_persons, new_matches = perform_matching(
        DLD_data,
        mila_emails_to_ignore=mila_emails_to_ignore,
        override_matches_mila_to_cc=override_matches_mila_to_cc,
        verbose=False,
    )

    # Since the matching tests will also make the email lowercase,
    # then we need to compare with the lowercase version of the email.
    # We'll do that transformation manually here.
    for source_name in ["drac_members", "drac_roles"]:
        LD_data = DLD_data[source_name]
        for D_data in LD_data:
            if D_data is not None:
                D_data["email"] = D_data["email"].lower()

    # recursive matching of dicts
    assert account_matches == DD_persons
    assert not new_matches
