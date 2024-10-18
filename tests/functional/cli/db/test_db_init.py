import pytest

from sarc.config import config


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
        "rgu_billing",
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
        "rgu_billing",
    ]:
        collection = db[collection_name]
        assert collection.index_information()
