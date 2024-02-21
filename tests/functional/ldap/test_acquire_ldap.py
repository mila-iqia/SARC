from unittest.mock import MagicMock, mock_open, patch

from datetime import date
import pytest
import random

import sarc.account_matching.make_matches
import sarc.ldap.acquire
import sarc.ldap.mymila  # will monkeypatch "read_my_mila"
import sarc.ldap.read_mila_ldap  # will monkeypatch "query_ldap"
from sarc.ldap.api import get_user

from .test_read_mila_ldap import fake_member_of, fake_mymila_data, fake_raw_ldap_data


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
    # Set the number of users we want to mock
    nbr_users = 10
    nbr_profs = 5 # Used only when generating MyMila data

    # Define how to mock queries to LDAP
    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

    # Define how to mock queries to MyMila
    def mock_query_mymila(tmp_json_path):
        return fake_mymila_data(nbr_users, nbr_profs)

    monkeypatch.setattr(sarc.ldap.mymila, "query_mymila", mock_query_mymila)

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        sarc.ldap.acquire.run()

    # TODO: Add checks for fields coming from mymila now saved in DB

    # -- Supervisor --
    # According to how MyMila data is emulated, the nbr_profs first users are profs
    # and the other are students. The first user (John Smith000) is the supervisor or
    # the nbr_profs+1 user (John Smith<nbr_profs>). Thus, the value in the supervisor field
    # for the user nbr_profs+1 should be john.smith000@mila.quebec, and its co-supervisor
    # should be john.smith001@mila.quebec.
        
    # Retrieve the user John Smith <nbr_profs+1>
    student_user = get_user(mila_email_username=f"john.smith{nbr_profs:03d}@mila.quebec").dict()

    assert student_user["supervisor"] == "john.smith000@mila.quebec"
    assert student_user["co_supervisor"] == "john.smith001@mila.quebec"
    # ----






@pytest.mark.parametrize(
        "ldap_supervisor,mymila_supervisor",
        [
            (None, None), # No supervisor in LDAP nor in MyMila
            ("super.visor@mila.quebec", None), # Supervisor only in LDAP
            #(None, "super.visor@mila.quebec"), # Supervisor only in MyMila: this case has already been checked in the previous test
            ("super.visor.ldap@mila.quebec", "super.visor.mymila@mila.quebec") # Supervisor in LDAP and in MyMila
        ]
)
@pytest.mark.usefixtures("empty_read_write_db")
def test_supervisor_retrieving(monkeypatch, mock_file, ldap_supervisor, mymila_supervisor):
    """
    This function tests the supervisor retrieving.
    """
    # Set the number of users we want to mock
    nbr_users = 3

    # Define how to mock queries to LDAP
    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        # Generate nbr_users random user
        users = fake_raw_ldap_data(nbr_users)

        # Generate a specific user having the predetermined supervisor
        # - Initialize this user
        specific_user = {
                "apple-generateduid": ["AF54098F-29AE-990A-B1AC-F63F5A89B89"],
                "cn": [f"jane.doe", f"Jane Doe"],
                "departmentNumber": [],
                "displayName": [f"Jane Doe the Only One"],
                "employeeNumber": [],
                "employeeType": [],
                "gecos": [""],
                "gidNumber": [str(1500000001 + nbr_users + 1)],
                "givenName": ["Jane"],
                "googleUid": [f"jane.doe"],
                "homeDirectory": [f"/home/jane.doe"],
                "loginShell": ["/bin/bash"],
                "mail": [f"jane.doe@mila.quebec"],
                "memberOf": fake_member_of(random.randin(0,2), nbr_users),
                "objectClass": [
                    "top",
                    "person",
                    "organizationalPerson",
                    "inetOrgPerson",
                    "posixAccount",
                ],
                "physicalDeliveryOfficeName": [],
                "posixUid": [f"doej"],
                "sn": [f"Doe 1"],
                "suspended": ["false"],
                "telephoneNumber": [],
                "title": [],
                "uid": [f"jane.doe"],
                "uidNumber": [str(1500000001 + nbr_users + 1)],
                "co_supervisor": ldap_supervisor
            }

        # - Add it to the list
        users.append(specific_user)

        # - Shuffle the list (just in case)
        random.shuffle(users)

        # Return the users list
        return users

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)


    # Define how to mock queries to MyMila
    def mock_query_mymila(tmp_json_path):
        # Generate nbr_users random users
        users = fake_mymila_data(nbr_users)

        # Generate a specific user having the predetermined supervisor
        # - Initialize this user
        specific_user = {
            "Profile Type": "",
            "Applicant Type": "",
            "internal id": "",
            "Mila Number": "",
            "Membership Type": "Research Intern", # Value randomly taken in the fake_mymila_data choices
            "Affiliation type": "HQP - PhD", # Value randomly taken in the fake_mymila_data choices
            "Assistant email": "",
            "Preferred email": "",
            "Faculty affiliated": "Mathematics and Statistics", # Value randomly taken in the fake_mymila_data choices
            "Department affiliated": "",
            "ID affiliated": "",
            "Affiliated university 2": "",
            "Second affiliated university": "",
            "Affiliated university 3": "",
            "Third affiliated university": "",
            "Program of study": "Computer Science", # Value randomly taken in the fake_mymila_data choices
            "GitHub username": "",
            "Google Scholar profile": "",
            "Cluster access": "",
            "Access privileges": "",
            "Status": "Active", # Value randomly taken in the fake_mymila_data choices
            "Membership Type.1": "",
            "Affiliation type.1": "",
            "Last Name": "Jane",
            "First Name": "Doe",
            "Preferred First Name": "Jane",
            "Email": "jane.doe@mila.quebec", # Must be the same than the one inserted in the fake data
            "Supervisor Principal": mymila_supervisor,
            "Co-Supervisor": "",
            "Start date of studies": date(year=2022, month=1, day=1),
            "End date of studies": date(year=2027, month=12, day=31),
            "Start date of visit-internship": "",
            "End date of visit-internship": "",
            "Affiliated university": "UdeM", # Value randomly taken in the fake_mymila_data choices
            "Current university title": "Canada Research Chair (Tier 2) and Assistant Professor", # Value randomly taken in the fake_mymila_data choices
            "Start date of academic nomination": date(2022, 1, 1),
            "End date of academic nomination": random.choice([date(2027, 12, 31), None]),
            "Alliance-DRAC account": "",
            "MILA Email": "jane.doe@mila.quebec",
            "Start Date with MILA": date(2022, 1, 1),
            "End Date with MILA": random.choice([date(2027, 12, 31), None]),
            "Type of membership": "",
        }

        # - Add it to the list


        # Return the users list
        return users

    monkeypatch.setattr(sarc.ldap.mymila, "query_mymila", mock_query_mymila)


    # TODO: Supervisor & co-supervisor
        
        
        
        
