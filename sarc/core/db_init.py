import pymongo
from pymongo.database import Database

from sarc.allocations.allocations import AllocationsRepository
from sarc.client.job import SlurmJobRepository
from sarc.config import config
from sarc.core.models.runstate import set_parsed_date
from sarc.core.models.validators import START_TIME
from sarc.storage.diskusage import ClusterDiskUsageRepository

CURRENT_SCHEMA_VERSION = 1


def db_upgrade(db: Database) -> None:
    v = db.version.find_one()
    if v is not None:
        if v["value"] == CURRENT_SCHEMA_VERSION:
            return
        elif v["value"] > CURRENT_SCHEMA_VERSION:
            raise RuntimeError("Database schema is newer than the code")

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
    create_healthcheck_indices(db)
    create_runstate(db)

    db.version.replace_one({}, {"value": CURRENT_SCHEMA_VERSION}, upsert=True)


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
                    "end_time_sacct": None,
                    "end_time_prometheus": None,
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

    db_collection.create_index([("uuid", pymongo.ASCENDING)], unique=True)
    db_collection.create_index([("matching_ids.$**", pymongo.ASCENDING)])

    # For user sorting in REST API
    db_collection.create_index(
        [("email", pymongo.ASCENDING), ("uuid", pymongo.ASCENDING)]
    )


def create_gpu_billing_indices(db: Database) -> None:
    db_collection = db.gpu_billing
    db_collection.create_index(
        [("cluster_name", pymongo.ASCENDING), ("since", pymongo.ASCENDING)], unique=True
    )


def create_node_gpu_mapping_indices(db: Database) -> None:
    db_collection = db.node_gpu_mapping
    db_collection.create_index(
        [("cluster_name", pymongo.ASCENDING), ("since", pymongo.ASCENDING)], unique=True
    )


def create_healthcheck_indices(db: Database) -> None:
    """Create indices for the healthcheck collection."""
    db_collection = db.healthcheck
    db_collection.create_index([("check.name", pymongo.ASCENDING)], unique=True)


def create_runstate(db: Database) -> None:
    """Create the runstate collection."""

    db_collection = db.runstate
    # RunStateCollection(database=db).get_collection()
    db_collection.create_index([("name", pymongo.ASCENDING)], unique=True)

    # create the default parsed dates
    set_parsed_date(db, "jobs", START_TIME)
    set_parsed_date(db, "users", START_TIME)


def create_allocations_indices(db: Database) -> None:
    db_collection = AllocationsRepository(database=db).get_collection()

    # Index most useful for querying allocations for a given cluster
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("start", pymongo.ASCENDING),
            ("end", pymongo.ASCENDING),
        ]
    )

    # Index most useful for querying allocations for any cluster
    db_collection.create_index(
        [("start", pymongo.ASCENDING), ("end", pymongo.ASCENDING)]
    )


def create_storages_indices(db: Database) -> None:
    db_collection = ClusterDiskUsageRepository(database=db).get_collection()

    # Index most useful for querying diskusages for a given cluster and a given group
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("groups.group_name", pymongo.ASCENDING),
            ("timestamp", pymongo.ASCENDING),
        ]
    )

    # Index most useful for querying diskusages for a given cluster and any group
    db_collection.create_index(
        [("cluster_name", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)]
    )

    # Index most useful for querying diskusages for any cluster and any group
    db_collection.create_index([("timestamp", pymongo.ASCENDING)])


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
        ]
    )

    # Index most useful for querying jobs for a given cluster and any state
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("submit_time", pymongo.ASCENDING),
            ("end_time", pymongo.ASCENDING),
        ]
    )

    # Index most useful for querying jobs on all clusters for a given state
    db_collection.create_index(
        [
            ("job_state", pymongo.ASCENDING),
            ("submit_time", pymongo.ASCENDING),
            ("end_time", pymongo.ASCENDING),
        ]
    )

    # Index most useful for querying jobs of any state on any cluster
    db_collection.create_index(
        [("submit_time", pymongo.ASCENDING), ("end_time", pymongo.ASCENDING)]
    )

    # Index most useful for querying jobs for a given cluster and scraping period
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("latest_scraped_start", pymongo.ASCENDING),
            ("latest_scraped_end", pymongo.ASCENDING),
        ]
    )

    # Indexes most useful for querying jobs with potential prometheus data
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("allocated.gpu_type", pymongo.ASCENDING),
        ],
        name="idx_stats_not_none",
        partialFilterExpression={"stored_statistics": {"$type": "object"}},
    )
    db_collection.create_index(
        [
            ("cluster_name", pymongo.ASCENDING),
            ("allocated.gpu_type", pymongo.ASCENDING),
        ],
        name="idx_gpu_type_not_none",
        partialFilterExpression={"allocated.gpu_type": {"$type": "string"}},
    )

    # Index most useful for job sorting in REST API
    db_collection.create_index(
        [("submit_time", pymongo.DESCENDING), ("_id", pymongo.ASCENDING)]
    )
