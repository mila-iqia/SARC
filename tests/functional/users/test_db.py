import logging
from contextlib import nullcontext
from datetime import UTC, datetime
from uuid import UUID

import pytest

from sarc.core.models.users import Credentials, MemberType
from sarc.core.models.validators import END_TIME, START_TIME, ValidField, ValidTag
from sarc.core.scraping.users import MatchID, UserMatch
from sarc.users.db import get_user, get_user_collection, get_users


@pytest.mark.parametrize(
    "match_id,expect",
    [
        (MatchID(name="unmatched_module", mid="blah"), None),
        (
            MatchID(name="mila_ldap", mid="doej@mila.quebec"),
            UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344"),
        ),
    ],
)
def test_lookup_matching_id(read_only_db_with_users, match_id, expect):
    repo = get_user_collection()
    res = repo._lookup_matching_id(match_id)
    if expect is None:
        assert res is None
    else:
        assert res.uuid == expect


def test_lookup_matching_id_duplicate_id(read_only_db_with_users, caplog):
    repo = get_user_collection()
    with caplog.at_level(logging.ERROR):
        res = repo._lookup_matching_id(MatchID(name="test_match", mid="abc"))
    assert res is not None
    assert (
        "Multiple matching users in DB for match id (name='test_match' mid='abc'), selecting the first one"
        == caplog.messages[-1]
    )


@pytest.mark.parametrize(
    "uuid,um,expected",
    [
        (
            UUID("5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"),
            UserMatch(
                matching_id=MatchID(name="", mid=""),
                supervisor=ValidField(
                    values=[
                        ValidTag(
                            value=MatchID(name="mymila", mid="222"),
                            valid_start=START_TIME,
                            valid_end=END_TIME,
                        )
                    ]
                ),
            ),
            (1, UUID("7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"), 0, None, []),
        ),
        (
            UUID("5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"),
            UserMatch(
                matching_id=MatchID(name="", mid=""),
                supervisor=ValidField(
                    values=[
                        ValidTag(
                            value=MatchID(name="mymila", mid="333"),
                            valid_start=START_TIME,
                            valid_end=END_TIME,
                        )
                    ]
                ),
            ),
            (
                0,
                None,
                0,
                None,
                [
                    "supervisor (name='mymila' mid='333') not in db for user 5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"
                ],
            ),
        ),
        (
            UUID("8d5b2b67-d35b-4cc3-acaa-ec56c9c6f253"),
            UserMatch(
                matching_id=MatchID(name="", mid=""),
                co_supervisors=ValidField(
                    values=[
                        ValidTag(
                            value={
                                MatchID(name="mymila", mid="222"),
                                MatchID(name="mymila", mid="333"),
                            },
                            valid_start=START_TIME,
                            valid_end=END_TIME,
                        )
                    ]
                ),
            ),
            (
                0,
                None,
                1,
                {UUID("7ecd3a8a-ab71-499e-b38a-ceacd91a99c4")},
                [
                    "co_supervisor (name='mymila' mid='333') not in db for user 8d5b2b67-d35b-4cc3-acaa-ec56c9c6f253"
                ],
            ),
        ),
    ],
)
def test_update_supervisors(read_only_db_with_users, caplog, uuid, um, expected):
    repo = get_user_collection()
    user = repo.find_one_by({"uuid": uuid})
    assert user is not None
    repo._update_supervisors(user, um)
    assert len(user.supervisor.values) == expected[0]
    if expected[0] >= 1:
        assert user.supervisor.values[0].value == expected[1]
    assert len(user.co_supervisors.values) == expected[2]
    if expected[2] >= 1:
        assert user.co_supervisors.values[0].value == expected[3]
    assert caplog.messages == expected[4]


@pytest.mark.parametrize(
    "um, expected",
    [
        (UserMatch(matching_id=MatchID(name="", mid="")), False),
        (
            UserMatch(
                display_name="Test 123",
                email="test123@example.com",
                matching_id=MatchID(name="test", mid="123"),
                known_matches={
                    MatchID(name="test2", mid="abc"),
                    MatchID(name="test", mid="123"),
                },
            ),
            True,
        ),
        (
            UserMatch(
                display_name="Test 123",
                email="test123@example.com",
                matching_id=MatchID(name="test", mid="123"),
                known_matches={
                    MatchID(name="test", mid="abc"),
                },
            ),
            True,
        ),
    ],
)
def test_insert_new(read_write_db_with_users, caplog, data_regression, um, expected):
    repo = get_user_collection()
    repo._insert_new(um)
    if not expected:
        assert len(caplog.messages) == 1
        assert caplog.messages[0].startswith(
            "Attempting to add a new user with missing attributes: "
        )
    else:
        db_user = repo._lookup_matching_id(um.matching_id)
        assert db_user is not None
        data_regression.check(db_user.model_dump(exclude={"id", "uuid"}, mode="json"))


@pytest.mark.parametrize(
    "uuid,um,expected",
    [
        (
            UUID("5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"),
            UserMatch(
                display_name=None,
                email=None,
                matching_id=MatchID(name="mila_ldap", mid="bonhomme@mila.quebec"),
            ),
            [],
        ),
        (
            UUID("5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"),
            UserMatch(
                display_name="Updated Name",
                email="updated@example.com",
                matching_id=MatchID(name="mila_ldap", mid="bonhomme@mila.quebec"),
                member_type=ValidField(
                    values=[
                        ValidTag(
                            value=MemberType.PHD_STUDENT,
                            valid_start=datetime(2023, 1, 1, tzinfo=UTC),
                            valid_end=datetime(2024, 1, 1, tzinfo=UTC),
                        )
                    ]
                ),
                associated_accounts={
                    "new_cluster": Credentials(
                        values=[
                            ValidTag(
                                value="newuser",
                                valid_start=datetime(2023, 1, 1, tzinfo=UTC),
                                valid_end=END_TIME,
                            )
                        ]
                    ),
                    "mila": Credentials(
                        values=[
                            ValidTag(
                                value="bonhomme",
                                valid_start=START_TIME,
                                valid_end=END_TIME,
                            )
                        ]
                    ),
                },
                github_username=ValidField(
                    values=[
                        ValidTag(
                            value="newgithub",
                            valid_start=datetime(2023, 1, 1, tzinfo=UTC),
                            valid_end=END_TIME,
                        )
                    ]
                ),
                google_scholar_profile=ValidField(
                    values=[
                        ValidTag(
                            value="https://scholar.google.com/newprofile",
                            valid_start=datetime(2023, 1, 1, tzinfo=UTC),
                            valid_end=END_TIME,
                        )
                    ]
                ),
                supervisor=ValidField(
                    values=[
                        ValidTag(
                            value=MatchID(name="mymila", mid="222"),
                            valid_start=datetime(2023, 1, 1, tzinfo=UTC),
                            valid_end=datetime(2024, 1, 1, tzinfo=UTC),
                        )
                    ]
                ),
                known_matches={
                    MatchID(name="new_source", mid="new_id"),
                    MatchID(name="mila_ldap", mid="bonhomme@mila.quebec"),
                },
            ),
            [],
        ),
        (
            UUID("8d5b2b67-d35b-4cc3-acaa-ec56c9c6f253"),
            UserMatch(
                matching_id=MatchID(name="mila_ldap", mid="petitbonhomme@mila.quebec"),
                known_matches={
                    MatchID(name="drac_role", mid="abc-123"),
                },
            ),
            [
                "User 8d5b2b67-d35b-4cc3-acaa-ec56c9c6f253 has matching id (drac_role:aaa-001) but update has (drac_role:abc-123), using update",
            ],
        ),
        (
            UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344"),
            UserMatch(
                matching_id=MatchID(name="mymila", mid="111"),
                member_type=ValidField(
                    values=[
                        ValidTag(
                            value=MemberType.PHD_STUDENT,
                            valid_start=datetime(2024, 9, 1, tzinfo=UTC),
                            valid_end=END_TIME,
                        )
                    ]
                ),
            ),
            [
                "Can't update member_type for user 1f9b04e5-0ec4-4577-9196-2b03d254e344, date overlap error: Overlapping validity with different values:\nMemberType.PHD_STUDENT: 2024-09-01 00:00:00+00:00 - 3000-01-01 00:00:00+00:00\nMemberType.PROFESSOR: 2020-09-01 00:00:00+00:00 - 2027-09-01 00:00:00+00:00"
            ],
        ),
        (
            UUID("7d98dcd3-7268-49f7-9a44-04b575c4c4de"),
            UserMatch(
                matching_id=MatchID(name="mymila", mid="456"),
                github_username=ValidField(
                    values=[
                        ValidTag(
                            value="potato",
                            valid_start=datetime(2024, 9, 1, tzinfo=UTC),
                            valid_end=END_TIME,
                        )
                    ]
                ),
            ),
            [
                "Can't update github_username for user 7d98dcd3-7268-49f7-9a44-04b575c4c4de, date overlap error: Overlapping validity with different values:\npotato: 2024-09-01 00:00:00+00:00 - 3000-01-01 00:00:00+00:00\ntestuser: 2023-03-03 00:00:00+00:00 - 2030-12-30 00:00:00+00:00"
            ],
        ),
        (
            UUID("7d98dcd3-7268-49f7-9a44-04b575c4c4de"),
            UserMatch(
                matching_id=MatchID(name="mymila", mid="456"),
                google_scholar_profile=ValidField(
                    values=[
                        ValidTag(
                            value="test_profile",
                            valid_start=datetime(2024, 9, 1, tzinfo=UTC),
                            valid_end=END_TIME,
                        )
                    ]
                ),
            ),
            [
                "Can't update google_scholar_profile for user 7d98dcd3-7268-49f7-9a44-04b575c4c4de, date overlap error: Overlapping validity with different values:\ntest_profile: 2024-09-01 00:00:00+00:00 - 3000-01-01 00:00:00+00:00\nhttps://scholar.google.com/citations?user=PataTe_111AJ&hl=en: 2019-10-11 00:00:00+00:00 - 2030-12-30 00:00:00+00:00"
            ],
        ),
    ],
)
def test_merge_and_update(
    read_write_db_with_users, data_regression, caplog, uuid, um, expected
):
    repo = get_user_collection()

    db_user = repo.find_one_by({"uuid": uuid})
    assert db_user is not None

    with caplog.at_level(logging.ERROR):
        repo._merge_and_update(db_user, um)

    assert caplog.messages == expected
    data_regression.check(db_user.model_dump(exclude={"id", "uuid"}, mode="json"))


@pytest.mark.parametrize(
    "uuid1,uuid2,expected",
    [
        (
            UUID("7ee5849c-241e-4d84-a4d2-1f73e22784f9"),
            UUID("5f27fdad-d4ca-4417-8e82-5a9dc7979d1c"),
            [],
        ),
        (
            UUID("7ee5849c-241e-4d84-a4d2-1f73e22784f9"),
            UUID("8b4fef2b-8f47-4eb6-9992-3e7e1133b42a"),
            [
                "Merging user 8b4fef2b-8f47-4eb6-9992-3e7e1133b42a into user 7ee5849c-241e-4d84-a4d2-1f73e22784f9 and their display_name differs (Othername vs Test User), Test User is picked",
                "Can't update member_type for user 7ee5849c-241e-4d84-a4d2-1f73e22784f9, date overlap error:",
                "Can't update github_username for user 7ee5849c-241e-4d84-a4d2-1f73e22784f9, date overlap error:",
                "Can't update google_scholar_profile for user 7ee5849c-241e-4d84-a4d2-1f73e22784f9, date overlap error:",
                "Can't update credentials for user 7ee5849c-241e-4d84-a4d2-1f73e22784f9, domain test, date overlap error:",
                "Can't update supervisor for user 7ee5849c-241e-4d84-a4d2-1f73e22784f9, date overlap error:",
                "Can't update co_supervisors for user 7ee5849c-241e-4d84-a4d2-1f73e22784f9, date overlap error:",
                "User 7ee5849c-241e-4d84-a4d2-1f73e22784f9 has matching id (test1:aaa) but db_user2 has bbb, using db_user1 value",
            ],
        ),
    ],
)
def test_combine_users(
    read_write_db_with_users, data_regression, caplog, uuid1, uuid2, expected
):
    repo = get_user_collection()
    db_user1 = repo.find_one_by({"uuid": uuid1})
    assert db_user1 is not None
    db_user2 = repo.find_one_by({"uuid": uuid2})
    assert db_user2 is not None

    with caplog.at_level(logging.WARNING):
        res = repo._combine_users(db_user1, db_user2)

    assert len(caplog.messages) == len(expected)
    for msg, exp in zip(caplog.messages, expected):
        assert msg.startswith(exp)

    data_regression.check(res.model_dump(exclude={"id", "uuid"}, mode="json"))


@pytest.mark.parametrize(
    "um,expected",
    [
        (
            UserMatch(
                matching_id=MatchID(name="test", mid="newvalue"),
                display_name="Name",
                email="email@example.com",
            ),
            None,
        ),
        (
            UserMatch(matching_id=MatchID(name="mila_ldap", mid="doej@mila.quebec")),
            UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344"),
        ),
        (
            UserMatch(matching_id=MatchID(name="test_match", mid="abc")),
            # This seems to work, but could be disturbed if MongoDB return order
            # varies (it is not guaranteed in case of equality)
            UUID("7ee5849c-241e-4d84-a4d2-1f73e22784f9"),
        ),
    ],
)
def test_update_user(read_write_db_with_users, um, expected):
    repo = get_user_collection()
    repo.update_user(um)
    res = repo._lookup_matching_id(um.matching_id)
    assert res is not None
    if expected is not None:
        assert res.uuid == expected


def test_get_users(read_only_db_with_users):
    res = get_users()
    assert len(res) != 0
    res = get_users({"uuid": UUID("edf47681-5cd1-4be5-876d-1b8b3c6f6b71")}, {})
    assert len(res) == 1
    assert res[0].email == "beaubonhomme@mila.quebec"


def test_get_user(read_only_db_with_users):
    res = get_user("jsmith@example.com")
    assert res is not None
    assert res.uuid == UUID("7ecd3a8a-ab71-499e-b38a-ceacd91a99c4")
