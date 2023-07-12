from unittest.mock import MagicMock, mock_open, patch

import pytest

import sarc.account_matching.make_matches
import sarc.ldap.acquire
import sarc.ldap.read_mila_ldap  # will monkeypatch "query_ldap"
from sarc.config import config
from sarc.ldap.api import get_user


def fake_raw_ldap_data(nbr_users=10):
    """
    Return a deterministically-generated list of fake LDAP users just as
    they would be returned by the function `query_ldap`.
    This is used for mocking the LDAP server.
    """
    return list(
        [
            {
                "apple-generateduid": ["AF54098F-29AE-990A-B1AC-F63F5A89B89"],
                "cn": [f"john.smith{i:03d}", f"John Smith{i:03d}"],
                "departmentNumber": [],
                "displayName": [f"John Smith the {i:03d}rd"],
                "employeeNumber": [],
                "employeeType": [],
                "gecos": [""],
                "gidNumber": [str(1500000001 + i)],
                "givenName": ["John"],
                "googleUid": [f"john.smith{i:03d}"],
                "homeDirectory": [f"/home/john.smith{i:03d}"],
                "loginShell": ["/bin/bash"],
                "mail": [f"john.smith{i:03d}@mila.quebec"],
                "memberOf": [],
                "objectClass": [
                    "top",
                    "person",
                    "organizationalPerson",
                    "inetOrgPerson",
                    "posixAccount",
                ],
                "physicalDeliveryOfficeName": [],
                "posixUid": [f"smithj{i:03d}"],
                "sn": [f"Smith {i:03d}"],
                "suspended": ["false"],
                "telephoneNumber": [],
                "title": [],
                "uid": [f"john.smith{i:03d}"],
                "uidNumber": [str(1500000001 + i)],
            }
            for i in range(nbr_users)
        ]
    )


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_users(cli_main, monkeypatch):
    """Test command line `sarc acquire users`.

    Copied from tests.functional.ldap.test_acquire_ldap.test_acquire_ldap
    and replaced direct call with CLI call.
    """

    cfg = config()
    nbr_users = 10

    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

    file1_content = """"Status","Username","Nom","Email","État du compte"
"Activated","john.smith000","John Smith the 000rd","js000@yahoo.ca","activé"
"Activated","john.smith001","John Smith the 001rd","js001@yahoo.ca","activé"
"Activated","john.smith002","John Smith the 002rd","js002@yahoo.ca","activé"
"Activated","stranger.person","Mysterious Stranger","ms@hotmail.com","activé"
"""
    # inspired by members-rrg-bengioy-ad-2022-11-25.csv
    file2_content = """Name,Sponsor,Permission,Activation_Status,username,Email
John Smith the 000rd,BigProf,Manager,activated,john.smith000,js000@yahoo.ca
John Smith the 001rd,BigProf,Manager,activated,john.smith001,js001@yahoo.ca
John Smith the 002rd,BigProf,Manager,activated,john.smith002,js002@yahoo.ca
Mysterious Stranger,BigProf,Manager,activated,stranger.person,ms@hotmail.com
"""
    # inspired by make_matches_comfig.json
    file3_content = """{
    "L_phantom_mila_emails_to_ignore":
        [
            "iamnobody@mila.quebec"
        ],
    "D_override_matches_mila_to_cc_account_username":
        {
            "john.smith001@mila.quebec": "js_the_first"
        }
}
"""

    file_contents = {
        cfg.account_matching.drac_roles_csv_path: file1_content,
        cfg.account_matching.drac_members_csv_path: file2_content,
        cfg.account_matching.make_matches_config: file3_content,
    }

    def mock_file(filename, *vargs, **kwargs):
        if filename in file_contents:
            return mock_open(read_data=file_contents[filename]).return_value
        else:
            raise FileNotFoundError(filename)

    with patch("builtins.open", side_effect=mock_file):
        assert (
            cli_main(
                [
                    "acquire",
                    "users",
                ]
            )
            == 0
        )

    # Validate the results of all of this by inspecting the database.
    for i in range(3):
        js_user = get_user(mila_email_username=f"john.smith{i:03d}@mila.quebec")
        assert js_user is not None

        # test some drac_roles and drac_members fields
        for segment in ["drac_roles", "drac_members"]:
            assert segment in js_user
            assert "email" in js_user[segment]
            assert js_user[segment]["email"] == f"js{i:03d}@yahoo.ca"
            assert "username" in js_user[segment]
            assert js_user[segment]["username"] == f"john.smith{i:03d}"

    # test the absence of the mysterious stranger
    js_user = get_user(drac_account_username="ms@hotmail.com")
    assert js_user is None
