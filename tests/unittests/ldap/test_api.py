from unittest.mock import MagicMock

import pytest

from sarc.ldap.api import get_users, query_latest_records


@pytest.mark.parametrize(
    "query,query_options,latest,expected",
    [
        [
            None,
            None,
            False,
            ({}, {}),
        ],
        [
            {"query": "q"},
            {"kwarg1": 1, "kwarg2": 2},
            False,
            None,
        ],
        [
            {"query": "q"},
            {},
            True,
            (
                {
                    "$and": [
                        query_latest_records(),
                        {"query": "q"},
                    ]
                },
                {},
            ),
        ],
    ],
)
def test_get_users(query, query_options, latest, expected, monkeypatch):
    if expected is None:
        expected = (query, query_options)

    monkeypatch.setattr("sarc.ldap.api.config", lambda: MagicMock())
    user_repository_mock = MagicMock()
    user_repository_mock.find_by.return_value = tuple()
    monkeypatch.setattr(
        "sarc.ldap.api.UserRepository", lambda *_args, **_kwargs: user_repository_mock
    )
    result = get_users(query, query_options, latest)
    assert isinstance(result, list)
    user_repository_mock.find_by.assert_called_once_with(expected[0], **expected[1])
