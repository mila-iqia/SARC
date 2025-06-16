from dataclasses import dataclass
from typing import Literal

import pymongo
from pymongo.database import Database
from simple_parsing import choice

from sarc.allocations.allocations import AllocationsRepository
from sarc.client.job import SlurmJobRepository
from sarc.config import config
from sarc.storage.diskusage import ClusterDiskUsageRepository


@dataclass
class DbInit:
    url: str | None
    database: str | None

    username: str | None
    password: str | None
    account: Literal["admin", "write", "read"] | None = choice("admin", "write", "read")

    def execute(self) -> int:
        cfg = config()
        url = cfg.mongo.connection_string if self.url is None else self.url
        self.database = (
            cfg.mongo.database_name if self.database is None else self.database
        )

        client: pymongo.MongoClient = pymongo.MongoClient(url)
        db = client.get_database(self.database)

        self.create_readonly_role(db)

        self.create_acount(client, db)

        create_clusters(db)

        # There is no need to create the other collections first, they will be
        # created when the index is created.

        # NOTE: Compound indices with mongodb provide subsets of compound index using prefix.
        #       Ex: For the compound index (a, b, c), we also get compound indices (a, b) and (a)
        #       But we don't get (b, c) or (c), which we must create explicitly if needed.

        create_clusters_indices(db)

        create_jobs_indices(db)

        create_storages_indices(db)

        create_allocations_indices(db)

        create_users_indices(db)

        create_gpu_billing_indices(db)

        create_node_gpu_mapping_indices(db)

        return 0

    def create_acount(self, client: pymongo.MongoClient, db: Database) -> None:
        if self.username is None or self.password is None:
            return

        if self.account == "admin":
            client.admin.command(
                "createUser",
                self.username,
                pwd=self.password,
                roles=[
                    {"role": "userAdminAnyDatabase", "db": "admin"},
                    {"role": "readWriteAnyDatabase", "db": "admin"},
                ],
            )

        if self.account == "read":
            db.command(
                "createUser",
                self.username,
                pwd=self.password,
                roles=[{"role": f"{self.database}ReadOnly", "db": self.database}],
            )

        if self.account == "write":
            db.command(
                "createUser",
                self.username,
                pwd=self.password,
                roles=[{"role": "readWrite", "db": self.database}],
            )

    def create_readonly_role(self, db: Database) -> None:
        collections = [
            "allocations",
            "diskusage",
            "users",
            "jobs",
            "clusters",
            "gpu_billing",
            "node_gpu_mapping",
        ]

        try:
            db.command(
                "createRole",
                f"{self.database}ReadOnly",
                privileges=[
                    {
                        "actions": ["find"],
                        "resource": {"db": self.database, "collection": coll},
                    }
                    for coll in collections
                ],
                roles=[],
            )
        except pymongo.errors.OperationFailure as err:
            if "already exists" not in str(err):
                raise


def create_clusters(db: Database) -> None:
    db_cluster = db.clusters
    # populate the db with default starting dates for each cluster
    clusters = config("scraping").clusters
    for cluster_name, cluster in clusters.items():
        db_cluster.update_one(
            {"cluster_name": cluster_name},
            {
                "$setOnInsert": {
                    "start_date": cluster.start_date,
                    "end_date": None,
                    "billing_is_gpu": cluster.billing_is_gpu,
                }
            },
            upsert=True,
        )


def create_clusters_indices(db: Database) -> None:
    db_collection = db.clusters
    db_collection.create_index([("cluster_name", pymongo.ASCENDING)], unique=True)


def create_users_indices(db: Database) -> None:
    # db_collection = _UserRepository(database=db).get_collection()
    db_collection = db.users

    db_collection.create_index([("mila_ldap.mila_email_username", pymongo.ASCENDING)])
    db_collection.create_index([("mila_ldap.mila_cluster_username", pymongo.ASCENDING)])
    db_collection.create_index(
        [
            ("drac_roles.username", pymongo.ASCENDING),
            ("drac_members.username", pymongo.ASCENDING),
        ]
    )


def create_gpu_billing_indices(db: Database) -> None:
    db_collection = db.gpu_billing
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("since", pymongo.ASCENDING),
        ],
        unique=True,
    )


def create_node_gpu_mapping_indices(db: Database) -> None:
    db_collection = db.node_gpu_mapping
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("since", pymongo.ASCENDING),
        ],
        unique=True,
    )


def create_allocations_indices(db: Database) -> None:
    db_collection = AllocationsRepository(database=db).get_collection()

    # Index most useful for querying allocations for a given cluster
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("start", pymongo.ASCENDING),
            ("end", pymongo.ASCENDING),
        ],
    )

    # Index most useful for querying allocations for any cluster
    db_collection.create_index(
        [
            ("start", pymongo.ASCENDING),
            ("end", pymongo.ASCENDING),
        ],
    )


def create_storages_indices(db: Database) -> None:
    db_collection = ClusterDiskUsageRepository(database=db).get_collection()

    # Index most useful for querying diskusages for a given cluster and a given group
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("groups.group_name", pymongo.ASCENDING),
            ("timestamp", pymongo.ASCENDING),
        ],
    )

    # Index most useful for querying diskusages for a given cluster and any group
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("timestamp", pymongo.ASCENDING),
        ],
    )

    # Index most useful for querying diskusages for any cluster and any group
    db_collection.create_index(
        [
            ("timestamp", pymongo.ASCENDING),
        ],
    )


def create_jobs_indices(db: Database) -> None:
    db_collection = SlurmJobRepository(database=db).get_collection()

    # Index most useful for querying single jobs.
    db_collection.create_index(
        [
            ("job_id", pymongo.ASCENDING),
            ("cluster_name", pymongo.ASCENDING),
            ("submit_time", pymongo.ASCENDING),
        ],
        unique=True,
    )

    # Index most useful for querying jobs for a given cluster and a given state
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("job_state", pymongo.ASCENDING),
            ("submit_time", pymongo.ASCENDING),
            ("end_time", pymongo.ASCENDING),
        ],
    )

    # Index most useful for querying jobs for a given cluster and any state
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("submit_time", pymongo.ASCENDING),
            ("end_time", pymongo.ASCENDING),
        ],
    )

    # Index most useful for querying jobs on all clusters for a given state
    db_collection.create_index(
        [
            ("job_state", pymongo.ASCENDING),
            ("submit_time", pymongo.ASCENDING),
            ("end_time", pymongo.ASCENDING),
        ],
    )

    # Index most useful for querying jobs of any state on any cluster
    db_collection.create_index(
        [
            ("submit_time", pymongo.ASCENDING),
            ("end_time", pymongo.ASCENDING),
        ],
    )
