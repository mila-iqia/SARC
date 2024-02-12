import io
import json
from io import StringIO
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


import random

def fake_mymila_data(nbr_users=10, nbr_profs=5):
    """
    Return a deterministically-generated list of fake MyMila users just as
    they would be returned by the function `load_mymila` (yet to be developped).
    This is used for mocking the reading of a CSV file, since we don't expect
    being able to read directly from the database itself in the short term.
    Records must have some matching points with the fake LDAP data, to allow
    for matching to be tested.
    Returns a list of dictionaries, easy to convert to a dataframe.
    """

    faculty_affiliated = [
        "Computer Science and Operations Research",
        "Electrical and Computer Engineering",
        "Mathematics and Statistics",
        "Physics",
        "Psychology",
        "Other",
        "",
    ]
    program_of_study = [
        "Computer Science",
        "Doctorat en Informatique",
        "",
    ]
    status = [
        "Active",
        "Inactive",
        "",
    ]
    affiliated_university = [
        "McGill",
        "UdeM",
        "Samsung SAIT",
        "",
    ]

    # by convention, first 'nbr_profs' names will be professors and the rest students
    def mymila_entry(i: int):
        # 2 different types of entries: prof and student
        is_prof = i < nbr_profs
        if is_prof:
            membership_types = [
                "Permanent HQP",
                "Visiting Researcher",
                "Collaborating Researcher",
            ]
            affiliation_types = [
                "Collaborating Alumni",
                "Collaborating researcher",
                "HQP - Postdoctorate",
                "visiting researcher",
                "",
            ]
            supervisors = [""]
            current_university_title = [
                "Canada Research Chair (Tier 2) and Assistant Professor",
                "Assistant Professor, School of Computer Science",
                "Professeur sous octrois agrégé / Associate Research Professor",
                "",
            ]

        else:
            membership_types = [
                "Research intern",
                "",
            ]
            affiliation_types = [
                "HQP - DESS",
                "HQP - Master's Research",
                "HQP - PhD",
                "HQP - Professional Master's",
                "HQP - Undergraduate",
                "Research Intern",
                "",
            ]
            supervisors = [
                f"John Smith{i:03d}" for i in range(nbr_profs)
            ]  # 'nbr_profs' first names for profs
            current_university_title = [
                "",
            ]

        first_name = "John"
        last_name = f"Smith{i:03d}"
        email = f"john.smith{i:03d}@mila.quebec"

        return {
            "Profile Type": "",
            "Applicant Type": "",
            "internal id": "",
            "Mila Number": "",
            "Membership Type": membership_types[i % len(membership_types)],
            "Affiliation type": affiliation_types[i % len(affiliation_types)],
            "Assistant email": "",
            "Preferred email": "",
            "Faculty affiliated": faculty_affiliated[i % len(faculty_affiliated)],
            "Department affiliated": "",
            "ID affiliated": "",
            "Affiliated university 2": "",
            "Second affiliated university": "",
            "Affiliated university 3": "",
            "Third affiliated university": "",
            "Program of study": program_of_study[i % len(program_of_study)],
            "GitHub username": "",
            "Google Scholar profile": "",
            "Cluster access": "",
            "Access privileges": "",
            "Status": status[i % len(status)],
            "Membership Type.1": "",
            "Affiliation type.1": "",
            "Last Name": first_name,
            "First Name": last_name,
            "Preferred First Name": first_name,
            "Email": email,
            "Supervisor Principal": supervisors[i % len(supervisors)],
            "Co-Supervisor": supervisors[(i + 1) % len(supervisors)],
            "Start date of studies": date(year=2022, month=1, day=1),
            "End date of studies": date(year=2027, month=12, day=31),
            "Start date of visit-internship": "",
            "End date of visit-internship": "",
            "Affiliated university": affiliated_university[
                i % len(affiliated_university)
            ],
            "Current university title": current_university_title[
                i % len(current_university_title)
            ],
            "Start date of academic nomination": date(2022, 1, 1),
            "End date of academic nomination": [date(2027, 12, 31), None][i % 2],
            "Alliance-DRAC account": "",
            "MILA Email": email,
            "Start Date with MILA": date(2022, 1, 1),
            "End Date with MILA": [date(2027, 12, 31), None][i % 2],
            "Type of membership": "",
        }

    return pd.DataFrame(list([mymila_entry(i) for i in range(nbr_users)]))


class MyStringIO(StringIO):
    """
    Special StringIO class which always save
    its content in a `text` field, especially
    on `close()`, so that content can be read
    even after object is closed.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text = self.getvalue()

    def close(self):
        self.text = self.getvalue()
        return super().close()


class FileSimulator:
    """
    Helper class to mock `open` builtin function.
    """

    def __init__(self, contents):
        """Initialize.

        contents must be a dictionary matching filename to (str) content,
        used to provide filename content when opening file.
        """
        self.contents = contents
        self.files = {}

    def get(self, filename):
        """Return filename content if loaded, empty string otherwise."""
        if filename in self.files:
            return self.files[filename].text
        return ""

    def __call__(self, filename, *args, **kwargs):
        """
        Mock for `open` function.

        File is managed as a MyStringIO object.
        """

        # Return an empty file if mode is "w", whatever the filename.
        if kwargs.get("mode") == "w" or (args and args[0] == "w"):
            file = MyStringIO()
        # Otherwise, return a file with content if filename is known.
        elif filename in self.contents:
            file = MyStringIO(self.contents[filename])
        # Otherwise, return an empty file.
        else:
            file = MyStringIO()

        # Store open file for further content reading.
        self.files[filename] = file

        # And return open file.
        return file


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_users(cli_main, monkeypatch, mock_file):
    """Test command line `sarc acquire users`.

    Copied from tests.functional.ldap.test_acquire_ldap.test_acquire_ldap
    and replaced direct call with CLI call.
    """
    nbr_users = 10

    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

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
        for segment in [js_user.drac_roles, js_user.drac_members]:
            assert segment is not None
            assert "email" in segment
            assert segment["email"] == f"js{i:03d}@yahoo.ca"
            assert "username" in segment
            assert segment["username"] == f"john.smith{i:03d}"

    # test the absence of the mysterious stranger
    js_user = get_user(drac_account_username="stranger.person")
    assert js_user is None


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_users_prompt(cli_main, monkeypatch, file_contents):
    """Test command line `sarc acquire users --prompt`."""
    nbr_users = 10

    def mock_query_ldap(
        local_private_key_file, local_certificate_file, ldap_service_uri
    ):
        assert ldap_service_uri.startswith("ldaps://")
        return fake_raw_ldap_data(nbr_users)

    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "query_ldap", mock_query_ldap)

    # Load config
    cfg = config()
    # Load mock for `open` builtin function
    file_simulator = FileSimulator(file_contents)
    # Preload manual matching file for to check initial content
    file_simulator(cfg.account_matching.make_matches_config)
    # Check initial content. Should contain only 1 default manual match.
    before = json.loads(file_simulator.get(cfg.account_matching.make_matches_config))
    assert before["D_override_matches_mila_to_cc_account_username"] == {
        "john.smith001@mila.quebec": "js_the_first"
    }

    # Feed input for prompt.
    # First input firstly receives `a` (invalid, should re-prompt)
    # then <enter> (valid, ignore).
    # Fourth input should receive `3`,
    # which should make mysterious stranger
    # be matched with john smith the 6rd as drac_member.
    monkeypatch.setattr("sys.stdin", io.StringIO("a\n\n\n\n3\n\n\n\n\n\n\n\n\n\n\n"))

    with patch("builtins.open", side_effect=file_simulator):
        assert (
            cli_main(
                [
                    "acquire",
                    "users",
                    "--prompt",
                ]
            )
            == 0
        )

    # Check manual matching file after execution. Should contain
    # 2 manual matches with the new one set from prompt.
    after = json.loads(file_simulator.get(cfg.account_matching.make_matches_config))
    assert after["D_override_matches_mila_to_cc_account_username"] == {
        "john.smith001@mila.quebec": "js_the_first",
        "john.smith006@mila.quebec": "stranger.person",
    }

    # Validate the results of all of this by inspecting the database.
    for i in range(3):
        js_user = get_user(mila_email_username=f"john.smith{i:03d}@mila.quebec")
        assert js_user is not None

        # test some drac_roles and drac_members fields
        for segment in ["drac_roles", "drac_members"]:
            assert hasattr(js_user, segment)
            field = getattr(js_user, segment)
            assert "email" in field
            assert field["email"] == f"js{i:03d}@yahoo.ca"
            assert "username" in field
            assert field["username"] == f"john.smith{i:03d}"

    # test mysterious stranger was indeed matched as drac_members with john smith the 6rd
    js_user = get_user(drac_account_username="stranger.person")
    assert js_user is not None
    assert js_user.mila_ldap["mila_email_username"] == "john.smith006@mila.quebec"
    assert js_user.drac_members is not None
    assert js_user.drac_members["username"] == "stranger.person"
    assert js_user.drac_roles is None
