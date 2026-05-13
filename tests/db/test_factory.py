from datetime import UTC, datetime

import pytest
from sqlmodel import select

from sarc.db.allocation import AllocationDB
from sarc.db.cluster import GPUBillingDB, SlurmClusterDB
from sarc.db.diskusage import DiskUsageDB
from sarc.db.job import SlurmJobDB
from sarc.db.users import (
    CredentialsDB,
    GithubUsernameDB,
    GoogleScholarDB,
    MatchingID,
    MemberTypeDB,
    SupervisorsDB,
    UserDB,
)

from .factory import create_diskusages


@pytest.mark.parametrize(
    "datatype",
    [
        SlurmJobDB,
        UserDB,
        SlurmClusterDB,
        GPUBillingDB,
        AllocationDB,
        # These next two are implicitely tested by the DiskUsageDB dump
        # DiskUsageUserDB,
        # DiskUsageGroupDB,
        DiskUsageDB,
        CredentialsDB,
        MemberTypeDB,
        SupervisorsDB,
        GithubUsernameDB,
        GoogleScholarDB,
        MatchingID,
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
    empty_read_write_db.add(
        DiskUsageDB(id=1, cluster_id=1, timestamp=datetime.now(UTC))
    )
    empty_read_write_db.commit()
    assert len(empty_read_write_db.exec(q).all()) > 0
