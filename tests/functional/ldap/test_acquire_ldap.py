from unittest.mock import patch

import pytest
from sarc_mocks import fake_mymila_data, fake_raw_ldap_data

import sarc.account_matching.make_matches
import sarc.ldap.acquire
from sarc.ldap.api import get_user, get_users


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_ldap(patch_return_values, mock_file):
    """
    Override the LDAP queries.
    Have users with matches to do.
        (at least we don't need to flex `perform_matching` with edge cases)
    Inspect the results in the database to make sure they're all there.
    """
    nbr_users = 10

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(nbr_users),
            "sarc.ldap.mymila.query_mymila_csv": [],
        }
    )

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # Validate the results of all of this by inspecting the database.
    for i in range(3):
        js_user = get_user(mila_email_username=f"john.smith{i:03d}@mila.quebec")
        assert js_user is not None
        # L = list(
        #    cfg.mongo.database_instance[cfg.ldap.mongo_collection_name].find(
        #        {"mila_ldap.mila_email_username": f"john.smith{i:03d}@mila.quebec"}
        #    )
        # )

        # test some drac_roles and drac_members fields
        js_user_d = js_user.dict()
        print(i, js_user_d)
        for segment in ["drac_roles", "drac_members"]:
            assert segment in js_user_d
            assert js_user_d[segment] is not None
            assert "email" in js_user_d[segment]
            assert js_user_d[segment]["email"] == f"js{i:03d}@yahoo.ca"
            assert "username" in js_user_d[segment]
            assert js_user_d[segment]["username"] == f"john.smith{i:03d}"

        assert js_user.name == js_user.mila_ldap["display_name"]

        assert js_user.mila.email == js_user.mila_ldap["mila_email_username"]
        assert js_user.mila.username == js_user.mila_ldap["mila_cluster_username"]
        assert js_user.mila.active

        assert js_user.drac.email == js_user.drac_members["email"]
        assert js_user.drac.username == js_user.drac_members["username"]
        assert js_user.drac.active

        if i == 1:
            assert js_user_d["mila_ldap"]["supervisor"] is not None

    # test the absence of the mysterious stranger
    js_user = get_user(drac_account_username="ms@hotmail.com")
    assert js_user is None


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_ldap_revision_change(patch_return_values, mock_file):
    """
    Test two LDAP acquisition, with a change in the LDAP data.
    This should result in a new record in the database.
    Then, one third acquisition, with no change in the LDAP data.
    This should result in no change in the database.
    """
    nbr_users = 3

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(nbr_users),
            "sarc.ldap.mymila.query_mymila_csv": [],
        }
    )

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # inspect database to check the number of records
    # should be nbr_users
    users = get_users(latest=False)
    nb_users_1 = len(users)
    assert nb_users_1 == nbr_users

    # re-acquire the same data
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # inspect database to check the number of records
    # should be the same
    users = get_users(latest=False)
    assert len(users) == nb_users_1

    # change fake data
    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(
                nbr_users,
                hardcoded_values_by_user={
                    2: {  # The first user who is not a prof is the one with index 2
                        "supervisor": "new_supervisor@mila.quebec"
                    }
                },
            )
        }
    )

    # re-acquire the new data
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # inspect database to check the number of records
    # should be incremented by 1
    users = get_users(latest=False)
    assert len(users) == nb_users_1 + 1

    # re-acquire the same data
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # inspect database to check the number of records
    # should be the same
    users = get_users(latest=False)
    assert len(users) == nb_users_1 + 1


@pytest.mark.usefixtures("empty_read_write_db")
def test_merge_ldap_and_mymila(patch_return_values, mock_file):
    nbr_users = 10

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(nbr_users),
            "sarc.ldap.mymila.query_mymila_csv": fake_mymila_data(nbr_users),
        }
    )

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # TODO: Add checks for fields coming from mymila now saved in DB
