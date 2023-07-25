from __future__ import annotations

import pytest

from sarc.config import using_config

from .allocations.factory import create_allocations
from .diskusage.factory import create_diskusages
from .jobs.factory import create_jobs


@pytest.fixture
def db_allocations():
    return create_allocations()


@pytest.fixture
def db_jobs():
    return create_jobs()


def custom_db_config(cfg, db_name):
    assert "test" in db_name
    new_cfg = cfg.replace(mongo=cfg.mongo.replace(database_name=db_name))
    db = new_cfg.mongo.database_instance
    # Ensure we do not use and thus wipe the production database
    assert db.name == db_name
    return new_cfg


def clear_db(db):
    db.allocations.drop()
    db.jobs.drop()
    db.diskusage.drop()
    db.users.drop()
    db.clusters.drop()


def fill_db(db):
    db.allocations.insert_many(create_allocations())
    db.jobs.insert_many(create_jobs())
    db.diskusage.insert_many(create_diskusages())


def create_db_configuration_fixture(db_name, empty=False, scope="function"):
    @pytest.fixture(scope=scope)
    def fixture(standard_config_object):
        cfg = custom_db_config(standard_config_object, db_name)
        db = cfg.mongo.database_instance
        clear_db(db)
        if not empty:
            fill_db(db)
        yield

    return fixture


empty_read_write_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-write-test",
    empty=True,
    scope="function",
)


read_write_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-write-test",
    scope="function",
)


read_only_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-only-test",
    scope="session",
)


@pytest.fixture
def empty_read_write_db(standard_config, empty_read_write_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-write-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.database_instance


@pytest.fixture
def read_write_db(standard_config, read_write_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-write-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.database_instance


@pytest.fixture
def read_only_db(standard_config, read_only_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-only-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.database_instance


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
