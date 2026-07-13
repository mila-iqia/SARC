import json
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta

import pandas
import sqlmodel
from sqlmodel import Session

from sarc.config import UTC
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from sarc.models.job import SlurmState
from tests.common.dateutils import MTL

cluster_no_gpu_billing = "hyrule"
cluster_gpu_billing_one_date = "raisin"


def _get_rgus(sess: Session) -> dict[str, float]:
    return {gpu.name: gpu.rgu for gpu in sess.exec(sqlmodel.select(GpuRguDB)).all()}


@dataclass
class Expected:
    """Helper class to store expected RGU information results for a job."""

    gres_rgu: float
    gpu_type_rgu: float


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
                }
            ),
        }


class ExampleData:
    """Helper to generate a set of synthetic jobs for RGU testing."""

    def __init__(
        self, cluster: str, cluster_without_billing: str = cluster_no_gpu_billing
    ):
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
        """Insert this data as SlurmJobDB rows into the given session."""
        existing_harmonized_names = {
            harmonized_name
            for harmonized_name in sess.exec(sqlmodel.select(GpuRguDB.name))
        }

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
                    harmonized_gpu_type=row.gpu_type
                    if row.gpu_type in existing_harmonized_names
                    else None,
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

        rgu = coalesce(requested_gres_gpu, 0) * gpu_type_rgu. Since populate()
        always sets requested_gres_gpu = row.job_billing, this simplifies to
        row.job_billing * rgu(gpu_type). rgu(gpu_type) is NaN when the GPU
        type has no matching GpuRguDB row.
        """
        expected_rgu = []
        expected_gpu_type_rgu = []
        rgus = _get_rgus(sess)

        for row in self.data:
            rgu = rgus.get(row.gpu_type, math.nan)
            rgu_value = row.job_billing * rgu

            row.expected = Expected(gres_rgu=rgu_value, gpu_type_rgu=rgu)
            expected_rgu.append(rgu_value)
            expected_gpu_type_rgu.append(rgu)

        return expected_rgu, expected_gpu_type_rgu


def _series_equals(actual, expected):
    """Compare two lists element-wise, treating NaN/None as equal."""
    assert len(actual) == len(expected), (len(actual), len(expected))
    for i, (a, e) in enumerate(zip(actual, expected)):
        if pandas.isna(e):
            assert pandas.isna(a), (i, a, e)
        else:
            assert a == e, (i, a, e)


def _check_rgu_columns(frame: pandas.DataFrame, data: ExampleData, sess):
    """Assert frame's rgu / gpu_type_rgu match expectations."""
    expected_rgu, expected_gpu_type_rgu = data.get_expected(sess)
    _series_equals(frame["rgu"].tolist(), expected_rgu)
    _series_equals(frame["gpu_type_rgu"].tolist(), expected_gpu_type_rgu)
