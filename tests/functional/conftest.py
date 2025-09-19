from __future__ import annotations

import hashlib
from contextlib import contextmanager

import freezegun
import gifnoc
import pytest
from pytest_regressions.data_regression import RegressionYamlDumper

from sarc.config import config
from sarc.testing import MongoInstance

from .allocations.factory import create_allocations
from .diskusage.factory import create_diskusages
from .jobs.factory import (
    create_cluster_entries,
    create_gpu_billings,
    create_jobs,
    create_users,
)

# this is to make the pytest-freezegun types serializable by pyyaml
# (for use in pytest-regression)


def repr_fakedatetime(dumper, data):
    value = data.isoformat(" ")
    return dumper.represent_scalar("tag:yaml.org,2002:timestamp", value)


RegressionYamlDumper.add_custom_yaml_representer(
    freezegun.api.FakeDatetime, repr_fakedatetime
)


@pytest.fixture
def db_allocations():
    return create_allocations()


@pytest.fixture
def db_jobs():
    return create_jobs()


@contextmanager
def custom_db_config(db_name):
    assert "test" in db_name
    with gifnoc.overlay({"sarc.mongo.database_name": db_name}):
        # Ensure we do not use and thus wipe the production database
        assert config().mongo.database_instance.name == db_name
        yield


def clear_db(db):
    db.allocations.drop()
    db.jobs.drop()
    db.diskusage.drop()
    db.users.drop()
    db.clusters.drop()
    db.gpu_billing.drop()
    db.node_gpu_mapping.drop()


def fill_db(db, with_users=False, with_clusters=False, job_patch=None):
    db.allocations.insert_many(create_allocations())
    db.jobs.insert_many(create_jobs(job_patch=job_patch))
    db.diskusage.insert_many(create_diskusages())
    db.gpu_billing.insert_many(create_gpu_billings())
    if with_users:
        db.users.insert_many(create_users())

    if with_clusters:
        # Fill collection `clusters`.
        db.clusters.insert_many(create_cluster_entries())


def create_db_configuration_fixture(
    empty=False,
    with_users=False,
    with_clusters=False,
    job_patch=None,
):
    @pytest.fixture(scope="function")
    def fixture(request):
        m = hashlib.md5()
        m.update(request.node.nodeid.encode())
        db_name = f"test-db-{m.hexdigest()}"
        with custom_db_config(db_name):
            db = config().mongo.database_instance
            clear_db(db)
            if not empty:
                fill_db(
                    db,
                    with_users=with_users,
                    with_clusters=with_clusters,
                    job_patch=job_patch,
                )
            yield db_name

    return fixture


empty_read_write_db_config_object = create_db_configuration_fixture(empty=True)

read_write_db_config_object = create_db_configuration_fixture(with_clusters=True)

read_only_db_config_object = create_db_configuration_fixture()

read_only_db_with_many_cpu_jobs_config_object = create_db_configuration_fixture(
    job_patch={
        "allocated": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
        "requested": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
    }
)

read_only_db_with_users_config_object = create_db_configuration_fixture(
    with_users=True,
    with_clusters=True,
)


@pytest.fixture
def empty_read_write_db(empty_read_write_db_config_object):
    with custom_db_config(empty_read_write_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_write_db(read_write_db_config_object):
    with custom_db_config(read_write_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db(read_only_db_config_object):
    with custom_db_config(read_only_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db_with_many_cpu_jobs(read_only_db_with_many_cpu_jobs_config_object):
    with custom_db_config(read_only_db_with_many_cpu_jobs_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db_with_users(read_only_db_with_users_config_object):
    with custom_db_config(read_only_db_with_users_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def account_matches():
    """
    Returns a structure of accounts with their corresponding values
    in "mila_ldap", "drac_members" and "drac_roles".

    This can be used for testing the matching of accounts,
    but it can also be used to populate the "users" collection
    in the database.

    Some sub-sub-fields like "department" are not used anywhere,
    but they were included to reproduce the actual structure in
    the real data sources.
    """

    #    "john.appleseed@mila.quebec"     is a straighforward match with 3 fields
    #    "nur.ali@mila.quebec"            is a match with 2 fields,
    #                                     but the name is in a different order,
    #                                     and with one letter difference
    #    "pikachu.pigeon@mila.quebec"     is not a match
    #    "ignoramus.mikey@mila.quebec"    is specifically meant to be ignored
    #    "overrido.dudette@mila.quebec"   has a forced match to username "duddirov" on CC

    # This is equivalent to config values
    #     mila_emails_to_ignore = ["ignoramus.mikey@mila.quebec"]
    #     override_matches_mila_to_cc = {"overrido.dudette@mila.quebec": "duddirov"}

    DLD_account_matches = {
        "john.appleseed@mila.quebec": {
            "mila_ldap": {
                "mila_email_username": "john.appleseed@mila.quebec",
                "mila_cluster_username": "appleseedj",
                "mila_cluster_uid": "1000",
                "mila_cluster_gid": "1000",
                "display_name": "John Appleseed",
                "status": "enabled",
            },
            "drac_members": {
                "rapi": "jvb-000-ag",
                "groupname": "rrg-bengioy-ad",
                "name": "John Appleseed",
                "position": "Étudiant au doctorat",
                "institution": "Un. de Montréal",
                "department": "Informatique Et Recherche Opérationnelle",
                "sponsor": "Yoshua Bengio",
                "permission": "Member",
                "activation_status": "activated",
                "username": "appjohn",
                "ccri": "abc-002-01",
                "email": "johnnyapple@umontreal.ca",
                "member_since": "2018-10-10 10:10:10 -0400",
            },
            "drac_roles": {
                "status": "Activated",
                "lastname": "Appleseed",
                "username": "appjohn",
                "ccri": "abc-002-01",
                "nom": "John Appleseed",
                "email": "johnnyapple@umontreal.ca",
                "statut": "étudiant au doctorat",
                "institution": "Un. de Montréal",
                "département": "Informatique Et Recherche Opérationnelle",
                "État du compte": "activé dernier renouvellement le 2021-05-05 10:00:00 EDT -0400",
            },
        },
        "nur.ali@mila.quebec": {
            "mila_ldap": {
                "mila_email_username": "nur.ali@mila.quebec",
                "mila_cluster_username": "nurali",
                "mila_cluster_uid": "1001",
                "mila_cluster_gid": "1001",
                "display_name": "Nur al-Din Ali",
                "status": "enabled",
            },
            "drac_members": {
                "rapi": "jvb-000-ag",
                "groupname": "rrg-bengioy-ad",
                "name": "Nur Alialdin",  # name spelling is different
                "position": "Étudiant au doctorat",
                "institution": "Un. de Montréal",
                "department": "Informatique Et Recherche Opérationnelle",
                "sponsor": "Yoshua Bengio",
                "permission": "Member",
                "activation_status": "activated",
                "username": "aldinur",
                "ccri": "abc-002-04",
                "email": "master_of_chaos@astalavista.box.sk",
                "member_since": "2018-10-10 10:10:10 -0400",
            },
            "drac_roles": None,
        },
        "pikachu.pigeon@mila.quebec": {
            "mila_ldap": {
                "mila_email_username": "pikachu.pigeon@mila.quebec",
                "mila_cluster_username": "pigeopika",
                "mila_cluster_uid": "1002",
                "mila_cluster_gid": "1002",
                "display_name": "pikachu pigeon",
                "status": "enabled",
            },
            "drac_members": None,
            "drac_roles": None,
        },
        "ignoramus.mikey@mila.quebec": {
            "mila_ldap": {
                "mila_email_username": "ignoramus.mikey@mila.quebec",
                "mila_cluster_username": "mikeignor",
                "mila_cluster_uid": "1003",
                "mila_cluster_gid": "1003",
                "display_name": "Michelangelo the Ignoramus",
                "status": "enabled",
            },
            "drac_members": None,
            "drac_roles": None,
        },
        "overrido.dudette@mila.quebec": {
            "mila_ldap": {
                "mila_email_username": "overrido.dudette@mila.quebec",
                "mila_cluster_username": "vopeach",
                "mila_cluster_uid": "1003",
                "mila_cluster_gid": "1003",
                "display_name": "Peach von Overrido",
                "status": "enabled",
            },
            "drac_members": {
                "rapi": "jvb-000-ag",
                "groupname": "rrg-bengioy-ad",
                # name is impossible to match automatically
                "name": "Pachelbel Ethelberg von Overrido",
                "position": "Étudiant au doctorat",
                "institution": "Un. de Montréal",
                "department": "Informatique Et Recherche Opérationnelle",
                "sponsor": "Yoshua Bengio",
                "permission": "Member",
                "activation_status": "activated",
                "username": "duddirov",
                "ccri": "abc-002-06",
                "email": "peachpalace@yahoo.fr",
                "member_since": "2018-10-10 10:10:10 -0400",
            },
            "drac_roles": {
                "status": "Activated",
                "lastname": "Ethelberg von Overrido",
                "username": "duddirov",
                "ccri": "abc-002-06",
                "nom": "P.",
                "email": "peachpalace@yahoo.fr",
                "statut": "étudiant au doctorat",
                "institution": "Un. de Montréal",
                "département": "Informatique Et Recherche Opérationnelle",
                "État du compte": "activé dernier renouvellement le 2021-05-05 10:00:00 EDT -0400",
            },
        },
    }

    return DLD_account_matches


@pytest.fixture
def freeport():
    import socket

    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def admin_client(freeport):
    from pymongo import MongoClient

    return MongoClient(f"mongodb://admin:admin_pass@localhost:{freeport}")


@pytest.fixture
def mongodb(tmp_path, freeport, test_config_path):
    """Initialize a running mongodb instance.
    Can run in parallel
    """

    with MongoInstance(
        str(tmp_path / "db"), freeport, sarc_config=str(test_config_path.absolute())
    ) as dbproc:
        # Populate the database with data

        db = admin_client(freeport).sarc

        fill_db(db)

        db.sercrest.insert_one({"mypassword": 123})

        # return the process
        yield dbproc


@contextmanager
def using_mongo_uri(uri):
    with gifnoc.overlay(
        {"sarc.mongo.connection_string": uri, "sarc.mongo.database_name": "sarc"}
    ):
        yield


@pytest.fixture
def admin_setup(mongodb, scraping_mode, freeport):
    """MongoDB admin user, can do anything."""
    with using_mongo_uri(f"mongodb://admin:admin_pass@localhost:{freeport}"):
        yield


@pytest.fixture
def write_setup(mongodb, scraping_mode, freeport):
    """SARC write user, can only write to sarc database.
    Have access to secrets
    """
    with using_mongo_uri(f"mongodb://write_name:write_pass@localhost:{freeport}/sarc"):
        yield


@pytest.fixture
def read_setup(mongodb, scraping_mode, freeport):
    """SARC read user, can only read to sarc database.
    Does not have access to secrets
    """
    with using_mongo_uri(f"mongodb://user_name:user_pass@localhost:{freeport}/sarc"):
        yield
