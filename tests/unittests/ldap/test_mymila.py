import pandas as pd
import pytest

from sarc.ldap.mymila import _get_collaborators, _map_affiliations

_collaborators = [
    "Student",
    "Active",
    [
        "Collaborating Researcher",
        "Visiting Researcher",
        "Research intern",
    ],
]
affiliation_types = [
    "Collaborating Alumni",
    "Collaborating researcher",
    "Research Intern",
    "visiting researcher",
]


def _get_valid_collaborators():
    for affiliation_type in _collaborators[2]:
        yield [*_collaborators[:2], affiliation_type]


def _get_invalid_collaborators():
    for pass_collaborators in _get_valid_collaborators():
        for i in range(len(pass_collaborators)):
            a = pass_collaborators[:]
            a[i] = f"Not {a[i]}"
            yield a


@pytest.mark.parametrize(
    "df_list,expected",
    [
        # Test that all combinations of valid with invalid values are not
        # matched
        [list(_get_invalid_collaborators()), 0],
        # Test that all combinations of valid values are matched
        [list(_get_valid_collaborators()), 1],
    ],
)
def test__get_collaborators(df_list, expected):
    for df in df_list:
        df = pd.DataFrame(
            [df],
            columns=["Profile Type", "Status", "Membership Type"],
        )
        collaborators = _get_collaborators(df)
        assert len(df[collaborators]) == expected


def test__map_affiliations(caplog):
    valid_collaborators = [list(_get_valid_collaborators())[0][:]] * len(
        affiliation_types
    )
    for i, affiliation_type in enumerate(affiliation_types):
        valid_collaborators[i] = [*valid_collaborators[i], affiliation_type, f"{i}@e.c"]

    df = pd.DataFrame(
        valid_collaborators,
        columns=[
            "Profile Type",
            "Status",
            "Membership Type",
            "Affiliation type",
            "mila_email_username",
        ],
    )
    # Test that every matching entries of affiliations are translated to their
    # own mapped value
    _map_affiliations(df)
    assert len(set(df["Affiliation type"])) == len(affiliation_types)

    for valid_collaborator in valid_collaborators:
        valid_collaborator[-2] = f"Not {valid_collaborator[-2]}"

    # Test that unkowned affiliations are reported as warnings
    df = pd.DataFrame(
        valid_collaborators,
        columns=df.columns,
    )
    _map_affiliations(df)
    assert len(caplog.messages) == len(affiliation_types)
    assert len(set(caplog.messages)) == len(affiliation_types)
    assert len([rec for rec in caplog.records if rec.levelname == "WARNING"]) == len(
        affiliation_types
    )
