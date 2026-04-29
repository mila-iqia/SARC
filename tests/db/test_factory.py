import pytest
from sqlmodel import select

from sarc.db.allocation import AllocationDB
from sarc.db.cluster import GPUBillingDB, SlurmClusterDB
from sarc.db.diskusage import DiskUsageDB, DiskUsageGroupDB, DiskUsageUserDB
from sarc.db.job import SlurmJobDB
from sarc.db.users import UserDB

from .factory import create_diskusages


@pytest.mark.parametrize(
    "datatype",
    [
        SlurmJobDB,
        UserDB,
        SlurmClusterDB,
        GPUBillingDB,
        AllocationDB,
        DiskUsageUserDB,
        DiskUsageGroupDB,
        DiskUsageDB,
    ],
)
def test_factory_tables(read_only_db, results_regression, datatype):
    q = select(datatype)
    results = read_only_db.exec(q).all()
    assert len(results) > 0
    results_regression(results)


@pytest.mark.parametrize("trial", [1, 2])
def test_rw(empty_read_write_db, trial):
    """Verify that commits to empty_read_write_db don't persist between tests."""
    q = select(DiskUsageDB)
    assert len(empty_read_write_db.exec(q).all()) == 0
    usages = create_diskusages()
    empty_read_write_db.add_all(usages)
    empty_read_write_db.commit()
    assert len(empty_read_write_db.exec(q).all()) > 0
