
from sarc.account_matching.make_matches import perform_matching
from sarc.config import config


def helper_extract_three_account_sources_from_ground_truth(account_matches):

    # Not accessing the database here. Just testing the matching algorithm.
    # Start with the ground truth, and then we'll split it into 3 parts
    # to see if it can be recovered.

    DLD_data = {"mila_ldap": [],
                "cc_members": [],
                "cc_roles": []}
    for _, data in account_matches.items():
        for k in DLD_data:  # "mila_ldap", "cc_members", "cc_roles"
            if data[k] is not None:
                DLD_data[k].append(data[k])

    # TODO : mess up the capitalization of some email to see if it still matches

    # as described in test fixture
    mila_emails_to_ignore = ["ignoramus.mikey@mila.quebec"]
    # note that the point here is that this entry would never
    # get matched other if it wasn't for the override
    override_matches_mila_to_cc = {"overrido.dudette@mila.quebec": "duddirov"}

    # Add an entry for "ignoramus.mikey@mila.quebec" in "cc_members"
    # which is expected to be ignored by the matching algorithm
    # because we'll tell it to ignore it.
    DLD_data["cc_members"].append({
        "rapi": "jvb-000-ag",
        "groupname": "rrg-bengioy-ad",
        "name": "Michelangelo the Ignoramus",
        "position": "Étudiant au doctorat",
        "institution": "Un. de Montréal",
        "department": "Informatique Et Recherche Opérationnelle",
        "sponsor": "Yoshua Bengio",
        "permission": "Member",
        "activation_status": "activated",
        "username": "appjohn",
        "ccri": "abc-002-10",
        "email": "__@umontreal.ca",
        "member_since": "2018-10-10 10:10:10 -0400"})

    return DLD_data, mila_emails_to_ignore, override_matches_mila_to_cc


def test_perform_matching(account_matches):

    DLD_data, mila_emails_to_ignore, override_matches_mila_to_cc = helper_extract_three_account_sources_from_ground_truth(account_matches)
    
    DD_persons = perform_matching(DLD_data,
                     mila_emails_to_ignore=mila_emails_to_ignore,
                     override_matches_mila_to_cc=override_matches_mila_to_cc,
                     verbose=False)

    # recursive matching of dicts
    assert account_matches == DD_persons

    #for mila_email_username in DD_persons:
    #    # source_name in "mila_ldap", "cc_members", "cc_roles
    #    for source_name in DD_persons[mila_email_username]:
    #        # even when one entry is `None`, the other should also be `None` instead of being absent
    #        assert DD_persons[mila_email_username] == account_matches[mila_email_username][source_name]

# TODO : Add test that doesn't make good use of `mila_emails_to_ignore` and `override_matches_mila_to_cc`
#        and ends up with a false positive and a false negative.