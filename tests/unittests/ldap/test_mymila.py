import pandas as pd
import pytest

from sarc.config import MyMilaConfig, config
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


@pytest.mark.usefixtures("standard_config")
@pytest.mark.usefixtures("standard_config")
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
    cfg: MyMilaConfig = config().mymila
    for df in df_list:
        df = pd.DataFrame(
            [df],
            columns=["Profile Type", "Status", "Membership Type"],
        )
        collaborators = _get_collaborators(cfg, df)
        assert len(df[collaborators]) == expected


@pytest.mark.usefixtures("standard_config")
def test__map_affiliations(caplog):
    cfg: MyMilaConfig = config().mymila
    valid_collaborators = [list(_get_valid_collaborators())[0][:]] * len(
        cfg.collaborators_affiliations
    )
    for i, affiliation_type in enumerate(cfg.collaborators_affiliations):
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
    _map_affiliations(cfg, df)
    assert set(df["Affiliation type"]) == set(cfg.collaborators_affiliations.values())

    for valid_collaborator in valid_collaborators:
        valid_collaborator[-2] = f"Not {valid_collaborator[-2]}"

    # Test that unkowned affiliations are reported as warnings
    df = pd.DataFrame(
        valid_collaborators,
        columns=df.columns,
    )
    _map_affiliations(cfg, df)
    assert len(caplog.messages) == len(cfg.collaborators_affiliations)
    assert len(set(caplog.messages)) == len(cfg.collaborators_affiliations)
    assert len([rec for rec in caplog.records if rec.levelname == "WARNING"]) == len(
        cfg.collaborators_affiliations
    )
