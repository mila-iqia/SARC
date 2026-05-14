import json

import pytest
import sqlmodel

from sarc.db.cluster import GPUBillingDB, SlurmClusterDB
from tests.functional.job_series.rgu_utils import (
    ExampleData,
    _billings_dump,
    _check_rgu_columns,
    _gen_fake_rgus,
    cluster_gpu_billing_is_gpu,
    cluster_gpu_billing_many_dates,
    cluster_gpu_billing_one_date,
    cluster_no_gpu_billing,
)
from tests.functional.job_series.test_func_load_job_series import (
    sql_load_job_series as load_job_series,
)


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_clusters_gpu_billings(rgu_db):
    """Sanity check: GPUBillings populated as expected for each test cluster."""

    def billings_count(cluster_name):
        return rgu_db.exec(
            sqlmodel.select(sqlmodel.func.count(GPUBillingDB.id))
            .join(SlurmClusterDB)
            .where(SlurmClusterDB.name == cluster_name)
        ).one()

    assert billings_count(cluster_no_gpu_billing) == 0
    assert billings_count(cluster_gpu_billing_one_date) == 1
    assert billings_count(cluster_gpu_billing_many_dates) > 1


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_job_series_rgu_one_date(rgu_db, file_regression):
    """Concrete test with a cluster which has 1 billing date."""
    data = ExampleData(cluster=cluster_gpu_billing_one_date)
    data.populate(rgu_db)

    frame = load_job_series(rgu_db)
    assert frame.shape[0] == len(data.data)

    _check_rgu_columns(frame, data, rgu_db)

    file_regression.check(
        f"===================================================================================\n"
        f"Example data with expected RGU information [main cluster: {cluster_gpu_billing_one_date} (1 billing date)]:\n"
        f"===================================================================================\n\n"
        f"----------\n"
        f"RGU values\n"
        f"----------\n"
        f"{json.dumps(_gen_fake_rgus(), indent=1)}\n\n"
        f"------------------\n"
        f"GPU billing values\n"
        f"------------------\n"
        f"{json.dumps(_billings_dump(rgu_db, [cluster_gpu_billing_one_date]), indent=1)}\n\n"
        f"----\n"
        f"Data\n"
        f"----\n"
        f"{data}\n"
    )


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_job_series_rgu_with_many_dates(rgu_db, file_regression):
    """Concrete test with a cluster which has many billing dates."""
    data = ExampleData(cluster=cluster_gpu_billing_many_dates)
    data.populate(rgu_db)

    frame = load_job_series(rgu_db)
    assert frame.shape[0] == len(data.data)

    _check_rgu_columns(frame, data, rgu_db)

    billings_dump = _billings_dump(rgu_db, [cluster_gpu_billing_many_dates])
    nb_billings = len(billings_dump[cluster_gpu_billing_many_dates])
    file_regression.check(
        f"===================================================================================\n"
        f"Example data with expected RGU information [main cluster: {cluster_gpu_billing_many_dates} ({nb_billings} billing dates)]:\n"
        f"===================================================================================\n\n"
        f"----------\n"
        f"RGU values\n"
        f"----------\n"
        f"{json.dumps(_gen_fake_rgus(), indent=1)}\n\n"
        f"------------------\n"
        f"GPU billing values\n"
        f"------------------\n"
        f"{json.dumps(billings_dump, indent=1)}\n\n"
        f"----\n"
        f"Data\n"
        f"----\n"
        f"{data}\n"
    )


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_job_series_rgu_billing_is_gpu(rgu_db, file_regression):
    """Concrete test with a cluster where billing_is_gpu is True."""
    data = ExampleData(cluster=cluster_gpu_billing_is_gpu)
    data.populate(rgu_db)

    frame = load_job_series(rgu_db)
    assert frame.shape[0] == len(data.data)

    _check_rgu_columns(frame, data, rgu_db)

    file_regression.check(
        f"==================================================================================\n"
        f"Example data with expected RGU information [main cluster: {cluster_gpu_billing_is_gpu} (no billing date)]:\n"
        f"==================================================================================\n\n"
        f"----------\n"
        f"RGU values\n"
        f"----------\n"
        f"{json.dumps(_gen_fake_rgus(), indent=1)}\n\n"
        f"----\n"
        f"Data\n"
        f"----\n"
        f"{data}\n"
    )
