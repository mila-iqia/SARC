import io
import json
import logging
import re
from io import StringIO
from unittest.mock import patch

import pytest
from opentelemetry.trace import StatusCode

from sarc.client.ldap.api import get_user
from sarc.config import config
from tests.common.sarc_mocks import fake_mymila_data, fake_raw_ldap_data


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
def test_acquire_users(cli_main, patch_return_values, mock_file, captrace):
    """Test command line `sarc acquire users`.

    Copied from tests.functional.ldap.test_acquire_ldap.test_acquire_ldap
    and replaced direct call with CLI call.
    """
    nbr_users = 10

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(nbr_users),
            "sarc.ldap.mymila.query_mymila_csv": [],
        }
    )

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

    # Check traces
    # NB: We don't check logging here, because
    # this execution won't display "acquire users" logs,
    # as everything goes well without corner cases.
    # We will test logging in test_acquire_users_prompt below.
    spans = captrace.get_finished_spans()
    assert len(spans) == 2

    assert spans[0].name == "fetch_mymila"
    assert spans[0].status.status_code == StatusCode.OK
    assert len(spans[0].events) == 0

    assert spans[1].name == "match_drac_to_mila_accounts"
    assert spans[1].status.status_code == StatusCode.OK
    assert len(spans[1].events) == 4
    assert (
        spans[1].events[0].name
        == "Loading mila_ldap, drac_roles and drac_members from files ..."
    )
    assert spans[1].events[1].name == "Loading matching config from file ..."
    assert spans[1].events[2].name == "Matching DRAC/CC to mila accounts ..."
    assert spans[1].events[3].name == "Committing matches to database ..."


@pytest.mark.parametrize(
    "ldap_supervisor,mymila_supervisor,expected_supervisor",
    [
        (None, None, None),  # No supervisor in LDAP nor in MyMila
        (
            "super.visor@mila.quebec",
            None,
            "super.visor@mila.quebec",
        ),  # Supervisor only in LDAP
        (
            None,
            "super.visor@mila.quebec",
            "super.visor@mila.quebec",
        ),  # Supervisor only in MyMila: this case has already been checked in the previous test
        (
            "super.visor.ldap@mila.quebec",
            "super.visor.mymila@mila.quebec",
            "super.visor.mymila@mila.quebec",
        ),  # Supervisor in LDAP and in MyMila
    ],
)
@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_users_supervisors(
    cli_main,
    patch_return_values,
    mock_file,
    ldap_supervisor,
    mymila_supervisor,
    expected_supervisor,
):
    """
    This function tests the supervisor retrieving from LDAP and MyMila data.

    Parameters:
        ldap_supervisor     The supervisor we want in the fake LDAP data used for this test
        mymila_supervisor   The supervisor we want in the fake MyMila data used for this test
        expected_supervisor The supervisor we expect as the one to be stored in the database
    """
    # Define the number of users and professors
    nbr_users = 4
    nbr_profs = 2

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(
                nbr_users,
                hardcoded_values_by_user={
                    2: {  # The first user who is not a prof is the one with index 2
                        "supervisor": ldap_supervisor
                    }
                },
            ),
            "sarc.ldap.mymila.query_mymila_csv": fake_mymila_data(
                nbr_users=nbr_users,
                nbr_profs=nbr_profs,
                hardcoded_values_by_user={
                    2: {  # The first user who is not a prof is the one with index 2
                        "Supervisor Principal": mymila_supervisor
                    }
                },
            ),
        }
    )

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        # sarc.ldap.acquire.run()
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
    js_user = get_user(
        mila_email_username=f"john.smith002@mila.quebec"
    )  # We modified the user with index 2; thus this is the one we retrieve
    assert js_user.mila_ldap["supervisor"] == expected_supervisor


@pytest.mark.parametrize(
    "ldap_co_supervisor,mymila_co_supervisor,expected_co_supervisor",
    [
        (None, None, None),  # No co-supervisor in LDAP nor in MyMila
        (
            "co.super.visor@mila.quebec",
            None,
            "co.super.visor@mila.quebec",
        ),  # Cosupervisor only in LDAP
        (
            None,
            "John Smith001",
            "john.smith001@mila.quebec",
        ),  # Cosupervisor only in MyMila: this case has already been checked in the previous test
        (
            "co.super.visor.ldap@mila.quebec",
            "John Smith001",
            "john.smith001@mila.quebec",
        ),  # Cosupervisor in LDAP and in MyMila
    ],
)
@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_users_co_supervisors(
    cli_main,
    patch_return_values,
    mock_file,
    ldap_co_supervisor,
    mymila_co_supervisor,
    expected_co_supervisor,
):
    """
    This function tests the co-supervisor retrieving from LDAP and MyMila data.

    Parameters:
        ldap_co_supervisor     The co-supervisor we want in the fake LDAP data used for this test
        mymila_co_supervisor   The co-supervisor we want in the fake MyMila data used for this test
        expected_co_supervisor The co-supervisor we expect as the one to be stored in the database
    """
    # Define the number of users and professors
    nbr_users = 4
    nbr_profs = 2

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(
                nbr_users,
                hardcoded_values_by_user={
                    2: {  # The first user who is not a prof is the one with index 2
                        "co_supervisor": ldap_co_supervisor
                    }
                },
            ),
            "sarc.ldap.mymila.query_mymila_csv": fake_mymila_data(
                nbr_users=nbr_users,
                nbr_profs=nbr_profs,
                hardcoded_values_by_user={
                    2: {  # The first user who is not a prof is the one with index 2
                        "Co-Supervisor": mymila_co_supervisor
                    }
                },
            ),
        }
    )

    # Patch the built-in `open()` function for each file path
    with patch("builtins.open", side_effect=mock_file):
        # sarc.ldap.acquire.run()
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
    js_user = get_user(
        mila_email_username=f"john.smith002@mila.quebec"
    )  # We modified the user with index 2; thus this is the one we retrieve
    assert js_user.mila_ldap["co_supervisor"] == expected_co_supervisor


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_users_prompt(
    cli_main, monkeypatch, patch_return_values, file_contents, caplog, captrace
):
    """Test command line `sarc acquire users --prompt`."""
    caplog.set_level(logging.INFO)
    nbr_users = 10

    patch_return_values(
        {
            "sarc.ldap.read_mila_ldap.query_ldap": fake_raw_ldap_data(nbr_users),
            "sarc.ldap.mymila.query_mymila_csv": [],
        }
    )

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

    # Check logging. There are logs from "acquire users" execution,
    # since we go through manual prompts, where some logs are printed.
    assert bool(
        re.search(
            r"root:make_matches\.py:[0-9]+ \[prompt] John Smith the 003rd \(ignored\)",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            r"root:acquire\.py:[0-9]+ Saving 1 manual matches \.\.\.", caplog.text
        )
    )

    # Check traces
    spans = captrace.get_finished_spans()
    assert len(spans) == 2

    assert spans[0].name == "fetch_mymila"
    assert spans[0].status.status_code == StatusCode.OK
    assert len(spans[0].events) == 0

    assert spans[1].name == "match_drac_to_mila_accounts"
    assert spans[1].status.status_code == StatusCode.OK
    assert len(spans[1].events) == 5
    assert (
        spans[1].events[0].name
        == "Loading mila_ldap, drac_roles and drac_members from files ..."
    )
    assert spans[1].events[1].name == "Loading matching config from file ..."
    assert spans[1].events[2].name == "Matching DRAC/CC to mila accounts ..."
    assert spans[1].events[3].name == "Committing matches to database ..."
    assert spans[1].events[4].name == "Saving 1 manual matches ..."
