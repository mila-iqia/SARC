import zoneinfo
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sarc.config import (
    ClusterConfig,
    Config,
    MongoConfig,
    config,
    parse_config,
    using_config,
)

pytest_plugins = "fabric.testing.fixtures"


@pytest.fixture(scope="session")
def standard_config_object():
    yield parse_config(Path(__file__).parent / "sarc-test.json")


@pytest.fixture(autouse=True)
def standard_config(standard_config_object, tmp_path):
    cfg = standard_config_object.replace(cache=tmp_path / "sarc-tmp-test-cache")
    with using_config(cfg) as cfg:
        yield cfg


@pytest.fixture
def disabled_cache():
    cfg = config().replace(cache=None)
    with using_config(cfg) as cfg:
        yield


@pytest.fixture
def tzlocal_is_mtl(monkeypatch):
    monkeypatch.setattr("sarc.config.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))
    monkeypatch.setattr("sarc.jobs.job.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))


@pytest.fixture
def test_config(
    request,
):
    current = config()

    vals = getattr(request, "param", dict())

    mongo_repl = vals.pop("mongo", {})
    clusters_repl = vals.pop("clusters", {})
    clusters_orig = current.clusters

    new_clusters = {}
    for name in clusters_orig:
        if name in clusters_repl:
            new_clusters[name] = clusters_orig[name].replace(**clusters_repl[name])
        else:
            # This is to make a clone
            new_clusters[name] = clusters_orig[name].replace()

    # Look at all the new names in repl
    for name in set(clusters_repl.keys()) - set(clusters_orig.keys()):
        new_clusters[name] = ClusterConfig(
            **(dict(host="test", timezone="America/Montreal") | clusters_repl[name])
        )

    conf = current.replace(
        mongo=current.mongo.replace(**mongo_repl),
        sshconfig=None,
        clusters=new_clusters,
    )
    with using_config(conf):
        yield conf


@pytest.fixture
def cli_main():
    from sarc.cli import main
    from sarc.cli.utils import clusters

    # Update possible choices based on the current test config
    clusters.choices = list(config().clusters.keys())

    yield main


@pytest.fixture
def prom_custom_query_mock(monkeypatch):
    """Mock the custom_query method of PrometheusConnect to avoid any real query.
    The object `prom_custom_query_mock` may then be used to check the query strings passed
    to `custom_query` using `prom_custom_query_mock.call_args[0][0]`."""
    from prometheus_api_client import PrometheusConnect

    monkeypatch.setattr(
        PrometheusConnect,
        "custom_query",
        MagicMock(return_value=[]),
    )

    yield PrometheusConnect.custom_query


@pytest.fixture
def file_contents():
    # We also need to generate the data for the two files being read:
    #     cfg.account_matching.drac_roles_csv_path
    #     cfg.account_matching.drac_members_csv_path
    #
    # We will define the content we want to inject for each file.
    # These are based on the fake users generated by `fake_raw_ldap_data`.
    # We don't need to create weird edge cases, because we are not testing
    # the details of the matching algorithm here. We are testing the pipeline.
    #
    # Naturally, the content of the CSV files must be consistent with the
    # fake users defined by `fake_raw_ldap_data`.
    # We'll add an extra use that won't match, called "Mysterious Stranger".

    cfg = config()

    # inspired by sponsored_roles_for_Yoshua_Bengio_(CCI_jvb-000).csv
    account_matching_drac_roles_csv_path = """"Status","Username","Nom","Email","État du compte"
"Activated","john.smith000","John Smith the 000rd","js000@yahoo.ca","activé"
"Activated","john.smith001","John Smith the 001rd","js001@yahoo.ca","activé"
"Activated","john.smith002","John Smith the 002rd","js002@yahoo.ca","activé"
"Activated","stranger.person","Mysterious Stranger","ms@hotmail.com","activé"
"""

    # inspired by members-rrg-bengioy-ad-2022-11-25.csv
    account_matching_drac_members_csv_path = """Name,Sponsor,Permission,Activation_Status,username,Email
John Smith the 000rd,BigProf,Manager,activated,john.smith000,js000@yahoo.ca
John Smith the 001rd,BigProf,Manager,activated,john.smith001,js001@yahoo.ca
John Smith the 002rd,BigProf,Manager,activated,john.smith002,js002@yahoo.ca
Mysterious Stranger,BigProf,Manager,activated,stranger.person,ms@hotmail.com
"""

    # inspired by make_matches_config.json
    account_matching_make_matches_config = """{
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

    group_to_prof = """
    {
        "supervisor000": "john.smith000@mila.quebec"
    }
    """
    exceptions_json_path = """
    {
        "not_prof": [],
        "not_student": [],
        "rename": {
            "john.smith000@mila.quebec": "New Name"
        }
    }
    """

    return {
        cfg.account_matching.drac_roles_csv_path: account_matching_drac_roles_csv_path,
        cfg.account_matching.drac_members_csv_path: account_matching_drac_members_csv_path,
        cfg.account_matching.make_matches_config: account_matching_make_matches_config,
        cfg.ldap.group_to_prof_json_path: group_to_prof,
        cfg.ldap.exceptions_json_path: exceptions_json_path,
    }
