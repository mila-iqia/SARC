from datetime import datetime

import pytest

from sarc.config import config


@pytest.fixture
def init_empty_db():
    db = config().mongo.get_database()
    db.allocations.drop()
    yield db


@pytest.fixture
def db_allocations():
    return [
        {
            "start": datetime(year=2017, month=4, day=1),
            "end": datetime(year=2018, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 100,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2017, month=4, day=1),
            "end": datetime(year=2018, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "50TB",
                    "project_inodes": "5e6",
                    "nearline": "15TB",
                },
            },
        },
        {
            "start": datetime(year=2018, month=4, day=1),
            "end": datetime(year=2019, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 100,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2018, month=4, day=1),
            "end": datetime(year=2019, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "70TB",
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2019, month=4, day=1),
            "end": datetime(year=2020, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 190,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2019, month=4, day=1),
            "end": datetime(year=2020, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "90TB",
                    "project_inodes": "5e6",
                    "nearline": "90TB",
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 130,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "30TB",
                    "project_inodes": "5e6",
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-compute",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": 219,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 200,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "70TB",
                    "project_inodes": "5e6",
                    "nearline": "80TB",
                },
            },
        },
    ]


@pytest.fixture
def init_db_with_allocations(init_empty_db, db_allocations):
    db = init_empty_db
    db.allocations.insert_many(db_allocations)


@pytest.fixture
def account_matches():
    """
    Returns a structure of accounts with their corresponding values
    in "mila_ldap", "cc_members" and "cc_roles".

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
        "john.appleseed@mila.quebec":
            {"mila_ldap": {
                "mila_email_username": "john.appleseed@mila.quebec",
                "mila_cluster_username": "appleseedj",
                "mila_cluster_uid": "1000",
                "mila_cluster_gid": "1000",
                "display_name": "John Appleseed",
                "status": "enabled"},
            "cc_members": {
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
                "member_since": "2018-10-10 10:10:10 -0400"
                },
            "cc_roles": {
                "status": "Activated",
                "lastname": "Appleseed",
                "username": "appjohn",
                "ccri": "abc-002-01",
                "nom": "John Appleseed",
                "email": "johnnyapple@umontreal.ca",
                "statut": "étudiant au doctorat",
                "institution": "Un. de Montréal",
                "département": "Informatique Et Recherche Opérationnelle",
                "État du compte": "activé dernier renouvellement le 2021-05-05 10:00:00 EDT -0400"
                }},
        "nur.ali@mila.quebec":
            {"mila_ldap": {
                "mila_email_username": "nur.ali@mila.quebec",
                "mila_cluster_username": "nurali",
                "mila_cluster_uid": "1001",
                "mila_cluster_gid": "1001",
                "display_name": "Nur al-Din Ali",
                "status": "enabled"},
            "cc_members": {
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
                "member_since": "2018-10-10 10:10:10 -0400"
                },
            "cc_roles": None},
        "pikachu.pigeon@mila.quebec":
            {"mila_ldap": {
                "mila_email_username": "pikachu.pigeon@mila.quebec",
                "mila_cluster_username": "pigeopika",
                "mila_cluster_uid": "1002",
                "mila_cluster_gid": "1002",
                "display_name": "pikachu pigeon",
                "status": "enabled"},
            "cc_members": None, "cc_roles": None},
        "ignoramus.mikey@mila.quebec":
            {"mila_ldap": {
                "mila_email_username": "ignoramus.mikey@mila.quebec",
                "mila_cluster_username": "mikeignor",
                "mila_cluster_uid": "1003",
                "mila_cluster_gid": "1003",
                "display_name": "Michelangelo the Ignoramus",
                "status": "enabled"},
            "cc_members": None, "cc_roles": None},
        "overrido.dudette@mila.quebec":
            {"mila_ldap": {
                "mila_email_username": "overrido.dudette@mila.quebec",
                "mila_cluster_username": "vopeach",
                "mila_cluster_uid": "1003",
                "mila_cluster_gid": "1003",
                "display_name": "Peach von Overrido",
                "status": "enabled"},
            "cc_members": {
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
                "member_since": "2018-10-10 10:10:10 -0400"
                },
            "cc_roles": {
                "status": "Activated",
                "lastname": "Ethelberg von Overrido",
                "username": "duddirov",
                "ccri": "abc-002-06",
                "nom": "P.",
                "email": "peachpalace@yahoo.fr",
                "statut": "étudiant au doctorat",
                "institution": "Un. de Montréal",
                "département": "Informatique Et Recherche Opérationnelle",
                "État du compte": "activé dernier renouvellement le 2021-05-05 10:00:00 EDT -0400"
                }
            },
    }

    return DLD_account_matches