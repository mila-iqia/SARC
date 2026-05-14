import json
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta

import pandas
import pytest
import sqlmodel

from sarc.config import UTC
from sarc.db.cluster import GPUBillingDB, SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from sarc.models.job import SlurmState
from tests.common.dateutils import MTL
from tests.db.factory import create_gpu_billings
from tests.functional.job_series.test_func_load_job_series import (
    helper_load_job_series as load_job_series,
)

cluster_no_gpu_billing = "hyrule"
cluster_gpu_billing_one_date = "raisin"
cluster_gpu_billing_many_dates = "patate"
cluster_gpu_billing_is_gpu = "mila"


def _gen_fake_rgus():
    """Synthetic RGU values for the GPU types referenced by ExampleData.

    These names are not in IGUANE; tests insert them into GpuRguDB via the
    `local_db` fixture so the JobSeriesDB view can join on them.
    """
    return {
        "A100": 3.21,
        "raisin_gpu_with_rgu_no_billing": 1.5,
        "raisin_gpu_with_rgu_with_billing": 2.5,
        "patate_gpu_with_rgu_no_billing": 3.5,
        "patate_gpu_with_rgu_with_billing": 4.5,
        "mila_gpu_no_rgu_no_billing": 7,
        "mila_gpu_no_rgu_with_billing": 2 * 7,
        "mila_gpu_with_rgu_no_billing": 3 * 7,
        "mila_gpu_with_rgu_with_billing": 4 * 7,
    }


@pytest.fixture
def local_db(empty_read_write_db):
    clusters = empty_read_write_db.exec(sqlmodel.select(SlurmClusterDB)).all()
    empty_read_write_db.add_all(create_gpu_billings(clusters=clusters))
    empty_read_write_db.add_all(
        [GpuRguDB(name=name, rgu=rgu) for name, rgu in _gen_fake_rgus().items()]
    )
    # Insert a test user, required as foreign key target for any SlurmJobDB.sarc_user_id.
    empty_read_write_db.add(UserDB(display_name="test", email="test@mila.quebec"))
    empty_read_write_db.commit()
    return empty_read_write_db


@dataclass
class Expected:
    """Helper class to store expected RGU information results for a job."""

    gres_rgu: float
    gpu_type_rgu: float
    gpu_billing_found: float | None


class Row:
    """Helper class to represent relevant RGU data for a synthetic job."""

    def __init__(
        self, cluster_name: str, start_time: str, job_billing: int, gpu_type: str
    ):
        self.cluster_name = cluster_name
        self.start_time = datetime.combine(
            datetime.strptime(start_time, "%Y-%m-%d"), time.min
        ).replace(tzinfo=MTL)
        self.job_billing: int = job_billing
        self.gpu_type = gpu_type
        self.expected: Expected | None = None

    def json(self):
        """Return row as a JSON dict, for debugging and file regression."""
        return {
            "cluster_name": self.cluster_name,
            "start_time": self.start_time.strftime("%Y-%m-%d"),
            "job_billing": self.job_billing,
            "gpu_type": self.gpu_type,
            "expected": (
                None
                if self.expected is None
                else {
                    "gres_rgu": self.expected.gres_rgu,
                    "gpu_type_rgu": self.expected.gpu_type_rgu,
                    "gpu_billing_found": self.expected.gpu_billing_found,
                }
            ),
        }


class ExampleData:
    """Helper to generate a set of synthetic jobs for RGU testing."""

    def __init__(self, cluster: str, cluster_without_billing: str = cluster_no_gpu_billing):
        """
        Parameters
        ----------
            cluster
                A cluster with GPU billings (or billing_is_gpu).
                Will be used to generate many jobs.
            cluster_without_billing
                A cluster with no GPU billing.
                Will be used to generate 1 job.
        """
        self.data = [
            # before 2023-02-15
            Row(cluster, "2023-02-12", 1, f"{cluster}_gpu_no_rgu_no_billing"),
            Row(cluster, "2023-02-12", 2, f"{cluster}_gpu_no_rgu_with_billing"),
            Row(cluster, "2023-02-13", 3, f"{cluster}_gpu_with_rgu_no_billing"),
            Row(cluster, "2023-02-14", 4, f"{cluster}_gpu_with_rgu_with_billing"),
            Row(cluster, "2023-02-14", 5, "A100"),
            # since 2023-02-15
            Row(cluster, "2023-02-15", 1000, f"{cluster}_gpu_no_rgu_no_billing"),
            Row(cluster, "2023-02-15", 120 * 150, f"{cluster}_gpu_no_rgu_with_billing"),
            Row(cluster, "2023-02-16", 1000, f"{cluster}_gpu_with_rgu_no_billing"),
            Row(cluster, "2023-02-17", 90 * 50, f"{cluster}_gpu_with_rgu_with_billing"),
            Row(cluster, "2023-02-17", 400, "A100"),
            # since 2023-02-18
            Row(cluster, "2023-02-18", 1000, f"{cluster}_gpu_no_rgu_no_billing"),
            Row(cluster, "2023-02-19", 120 * 150, f"{cluster}_gpu_no_rgu_with_billing"),
            Row(cluster, "2023-02-20", 1000, f"{cluster}_gpu_with_rgu_no_billing"),
            Row(cluster, "2023-02-21", 90 * 50, f"{cluster}_gpu_with_rgu_with_billing"),
            Row(cluster, "2023-02-22", 400, "A100"),
            # other clusters
            Row(
                cluster_without_billing,
                "2023-02-21",
                5678,
                f"{cluster_without_billing}_gpu_9",
            ),
        ]

    def __repr__(self):
        """Return data as a JSON string, for debugging and file regression."""
        return json.dumps([row.json() for row in self.data], indent=1)

    def populate(self, sess) -> None:
        """Insert this data as SlurmJobDB rows into the given session.

        submit_time is set to row.start_time so that the JobSeriesDB view's
        billing lookup (which uses submit_time) selects the appropriate
        GPUBilling for the period each row represents.
        """
        clusters = {c.name: c for c in sess.exec(sqlmodel.select(SlurmClusterDB)).all()}
        user = sess.exec(sqlmodel.select(UserDB)).one()
        elapsed_seconds = 10
        elapsed_timedelta = timedelta(seconds=elapsed_seconds)
        for i, row in enumerate(self.data):
            start_time = row.start_time.astimezone(UTC)
            sess.add(
                SlurmJobDB(
                    cluster_id=clusters[row.cluster_name].id,
                    sarc_user_id=user.id,
                    submit_time=start_time,
                    start_time=start_time,
                    end_time=start_time + elapsed_timedelta,
                    elapsed_time=elapsed_seconds,
                    allocated_gres_gpu=row.job_billing,
                    allocated_billing=row.job_billing,
                    allocated_gpu_type=row.gpu_type,
                    requested_gres_gpu=row.job_billing,
                    account="account",
                    job_id=i,
                    name="name",
                    cluster_user="user",
                    group="group",
                    partition="partition",
                    job_state=SlurmState.RUNNING,
                    nodes=[],
                    work_dir="work_dir",
                    submit_line=None,
                )
            )
        sess.commit()

    def get_expected(self, sess):
        """Compute expected RGU values matching the JobSeriesDB view's rgu_expr.

        Mirrors the three branches of the CASE expression in sarc/db/job_series.py:
          A. cluster.billing_is_gpu          -> gpu_count_raw * rgu
          B. gpu_unit_billing IS NULL        -> gpu_count_raw * rgu
             (no applicable billing record, OR gpu_type missing from mapping)
          C. otherwise                       -> (gpu_count_raw / unit_billing) * rgu

        NB: Case B differs from old Mongo semantics. Old code: "no billing for
        this cluster" or "no billing for this GPU type" -> NaN. SQL view: treat
        as if billing == GPU count, so rgu_value = gpu_count * rgu.
        """
        cluster_by_name = {
            c.name: c for c in sess.exec(sqlmodel.select(SlurmClusterDB)).all()
        }

        expected_rgu = []
        expected_gpu_type_rgu = []

        for row in self.data:
            cluster = cluster_by_name[row.cluster_name]
            # Find applicable billing: most recent with since <= row.start_time
            # (datetime_utc columns require UTC-aware comparison values).
            billing = sess.exec(
                sqlmodel.select(GPUBillingDB)
                .where(GPUBillingDB.cluster_id == cluster.id)
                .where(GPUBillingDB.since <= row.start_time.astimezone(UTC))
                .order_by(sqlmodel.col(GPUBillingDB.since).desc())
                .limit(1)
            ).first()

            rgu = _gen_fake_rgus().get(row.gpu_type, math.nan)
            # gpu_count_raw = max(allocated_billing, requested_gres_gpu); here both
            # are set to row.job_billing, so it simplifies.
            gpu_count_raw = row.job_billing

            gpu_billing_found = None
            if cluster.billing_is_gpu:
                rgu_value = gpu_count_raw * rgu
            elif billing is None or row.gpu_type not in billing.gpu_to_billing:
                rgu_value = gpu_count_raw * rgu
            else:
                unit_billing = billing.gpu_to_billing[row.gpu_type]
                rgu_value = (gpu_count_raw / unit_billing) * rgu
                gpu_billing_found = unit_billing

            row.expected = Expected(
                gres_rgu=rgu_value,
                gpu_type_rgu=rgu,
                gpu_billing_found=gpu_billing_found,
            )
            expected_rgu.append(rgu_value)
            expected_gpu_type_rgu.append(rgu)

        return expected_rgu, expected_gpu_type_rgu


def _billings_dump(sess, cluster_names) -> dict:
    """Serialize the GPUBillings of the named clusters for file regression output."""
    name_to_cluster = {
        c.name: c for c in sess.exec(sqlmodel.select(SlurmClusterDB)).all()
    }
    return {
        cluster_name: [
            {
                "since": billing.since.astimezone(MTL).strftime("%Y-%m-%d"),
                "gpu_to_billing": billing.gpu_to_billing,
            }
            for billing in sess.exec(
                sqlmodel.select(GPUBillingDB)
                .where(GPUBillingDB.cluster_id == name_to_cluster[cluster_name].id)
                .order_by(GPUBillingDB.since)
            ).all()
        ]
        for cluster_name in cluster_names
    }


def _series_equals(actual, expected):
    """Compare two lists element-wise, treating NaN/None as equal."""
    assert len(actual) == len(expected), (len(actual), len(expected))
    for i, (a, e) in enumerate(zip(actual, expected)):
        if pandas.isna(e):
            assert pandas.isna(a), (i, a, e)
        else:
            assert a == e, (i, a, e)


def _check_rgu_columns(frame, data: ExampleData, sess):
    """Assert frame's rgu / gpu_type_rgu match expectations."""
    expected_rgu, expected_gpu_type_rgu = data.get_expected(sess)
    _series_equals(frame["rgu"].tolist(), expected_rgu)
    _series_equals(frame["gpu_type_rgu"].tolist(), expected_gpu_type_rgu)


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_clusters_gpu_billings(local_db):
    """Sanity check: GPUBillings populated as expected for each test cluster."""

    def billings_count(cluster_name):
        return local_db.exec(
            sqlmodel.select(sqlmodel.func.count(GPUBillingDB.id))
            .join(SlurmClusterDB)
            .where(SlurmClusterDB.name == cluster_name)
        ).one()

    assert billings_count(cluster_no_gpu_billing) == 0
    assert billings_count(cluster_gpu_billing_one_date) == 1
    assert billings_count(cluster_gpu_billing_many_dates) > 1


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_update_job_series_rgu_one_date(local_db, file_regression):
    """Concrete test with a cluster which has 1 billing date."""
    data = ExampleData(cluster=cluster_gpu_billing_one_date)
    data.populate(local_db)

    frame = load_job_series(local_db)
    assert frame.shape[0] == len(data.data)

    _check_rgu_columns(frame, data, local_db)

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
        f"{json.dumps(_billings_dump(local_db, [cluster_gpu_billing_one_date]), indent=1)}\n\n"
        f"----\n"
        f"Data\n"
        f"----\n"
        f"{data}\n"
    )


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_update_job_series_rgu_with_many_dates(local_db, file_regression):
    """Concrete test with a cluster which has many billing dates."""
    data = ExampleData(cluster=cluster_gpu_billing_many_dates)
    data.populate(local_db)

    frame = load_job_series(local_db)
    assert frame.shape[0] == len(data.data)

    _check_rgu_columns(frame, data, local_db)

    billings_dump = _billings_dump(local_db, [cluster_gpu_billing_many_dates])
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
def test_update_job_series_rgu_billing_is_gpu(local_db, file_regression):
    """Concrete test with a cluster where billing_is_gpu is True."""
    data = ExampleData(cluster=cluster_gpu_billing_is_gpu)
    data.populate(local_db)

    frame = load_job_series(local_db)
    assert frame.shape[0] == len(data.data)

    _check_rgu_columns(frame, data, local_db)

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
