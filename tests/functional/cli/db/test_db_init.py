import pymongo
import pytest

from sarc.config import config
from sarc.core.models.runstate import get_parsed_date
from sarc.core.models.validators import START_TIME


@pytest.mark.usefixtures("empty_read_write_db")
def test_db_init(cli_main):
    db = config().mongo.database_instance

    # NOTE: Use instead these clases to get the collections once `users` is harmonized with the others.
    # from sarc.allocations.allocations import AllocationsRepository
    # from sarc.config import config
    # from sarc.jobs.job import SlurmJobRepository
    # from sarc.storage.diskusage import ClusterDiskUsageRepository

    for collection_name in [
        "jobs",
        "allocations",
        "diskusage",
        "users",
        "clusters",
        "gpu_billing",
        "node_gpu_mapping",
        "runstate",
    ]:
        collection = db[collection_name]
        assert not collection.index_information()

    cli_main(["db", "init"])

    for collection_name in [
        "jobs",
        "allocations",
        "diskusage",
        "users",
        "clusters",
        "gpu_billing",
        "node_gpu_mapping",
        "runstate",
    ]:
        collection = db[collection_name]
        assert collection.index_information()

    parsed_date_jobs = get_parsed_date(db, "jobs")
    parsed_date_users = get_parsed_date(db, "users")
    assert parsed_date_jobs == START_TIME
    assert parsed_date_users == START_TIME

    scraped_time_index = [
        index
        for index in db["jobs"].index_information().values()
        if index["key"]
        == [
            ("cluster_name", pymongo.ASCENDING),
            ("latest_scraped_start", pymongo.ASCENDING),
            ("latest_scraped_end", pymongo.ASCENDING),
        ]
    ]
    assert len(scraped_time_index) == 1
