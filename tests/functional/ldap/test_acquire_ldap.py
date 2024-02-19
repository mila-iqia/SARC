from unittest.mock import MagicMock, mock_open, patch

import pytest

import sarc.account_matching.make_matches
import sarc.ldap.acquire
import sarc.ldap.mymila  # will monkeypatch "read_my_mila"
import sarc.ldap.read_mila_ldap  # will monkeypatch "query_ldap"
from sarc.ldap.api import get_user

from sarc_mocks import fake_mymila_data, fake_raw_ldap_data


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_ldap(monkeypatch, mock_file):
    """
    Override the LDAP queries.
    Have users with matches to do.
        (at least we don't need to flex `perform_matching` with edge cases)
    Inspect the results in the database to make sure they're all there.
    """
    nbr_users = 10

    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

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
def test_merge_ldap_and_mymila(monkeypatch, mock_file):
    nbr_users = 10

    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

    def mock_query_mymila(tmp_json_path):
        return fake_mymila_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.mymila, "query_mymila", mock_query_mymila)

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # TODO: Add checks for fields coming from mymila now saved in DB
