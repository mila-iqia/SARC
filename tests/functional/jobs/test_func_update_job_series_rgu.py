import json
import math
from dataclasses import dataclass
from datetime import datetime, time

import numpy as np
import pandas
import pytest

from sarc.client import get_available_clusters
from sarc.client.gpumetrics import GPUBilling, get_cluster_gpu_billings
from sarc.client.series import (
    load_job_series,
    update_cluster_job_series_rgu,
    update_job_series_rgu,
)
from sarc.config import MTL

from .test_func_load_job_series import MOCK_TIME


def _gen_fake_rgus():
    """Mock for sarc.client.gpumetrics.get_rgus()"""
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


@dataclass
class Expected:
    """Helper class to store expected RGU information results for a job."""

    gres_gpu: float
    gres_rgu: float
    gpu_type_rgu: float


class Row:
    """Helper class to represent relevant RGU data for a job in a dataframe."""

    def __init__(
        self, cluster_name: str, start_time: str, gres_gpu: float, gpu_type: str
    ):
        self.cluster_name = cluster_name
        self.start_time = datetime.combine(
            datetime.strptime(start_time, "%Y-%m-%d"), time.min
        ).replace(tzinfo=MTL)
        self.gres_gpu = gres_gpu
        self.gpu_type = gpu_type
        self.expected = None

    def expect_before(self, billing: GPUBilling):
        """Compute expected RGU information if job ran before given billing date."""
        rgu = _gen_fake_rgus().get(self.gpu_type, math.nan)
        self.expected = Expected(
            gres_gpu=self.gres_gpu, gres_rgu=self.gres_gpu * rgu, gpu_type_rgu=rgu
        )

    def expect_after(self, billing: GPUBilling):
        """Compute expected RGU information if job run since or after given billing date."""
        rgu = _gen_fake_rgus().get(self.gpu_type, math.nan)
        gpu_billing = billing.gpu_to_billing.get(self.gpu_type, math.nan)
        self.expected = Expected(
            gres_gpu=self.gres_gpu / gpu_billing,
            gres_rgu=self.gres_gpu / gpu_billing * rgu,
            gpu_type_rgu=rgu,
        )

    def json(self):
        """Return row as a JSON, for debugging and file regression."""
        return {
            "cluster_name": self.cluster_name,
            "start_time": self.start_time.strftime("%Y-%m-%d"),
            "gres_gpu": self.gres_gpu,
            "gpu_type": self.gpu_type,
            "expected": (
                None
                if self.expected is None
                else {
                    "gres_gpu": self.expected.gres_gpu,
                    "gres_rgu": self.expected.gres_rgu,
                    "gpu_type_rgu": self.expected.gpu_type_rgu,
                }
            ),
        }


class ExampleData:
    """Helper class to generate a testing dataframe."""

    def __init__(
        self,
        cluster: str,
        cluster_without_billing: str = "hyrule",
    ):
        """
        Initialize.

        Parameters
        ----------
            cluster
                A cluster with GPU billings.
                Will be used to generate many jobs.
            cluster_without_billing
                A cluster with no GPU billing.
                WIll be used to generate 1 job.
        """
        self.data = [
            # before 2023-02-15
            Row(
                cluster_name=cluster,
                start_time="2023-02-12",
                gres_gpu=1,
                gpu_type=f"{cluster}_gpu_no_rgu_no_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-12",
                gres_gpu=2,
                gpu_type=f"{cluster}_gpu_no_rgu_with_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-13",
                gres_gpu=3,
                gpu_type=f"{cluster}_gpu_with_rgu_no_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-14",
                gres_gpu=4,
                gpu_type=f"{cluster}_gpu_with_rgu_with_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-14",
                gres_gpu=5,
                gpu_type="A100",
            ),
            # since 2023-02-15
            Row(
                cluster_name=cluster,
                start_time="2023-02-15",
                gres_gpu=1000,
                gpu_type=f"{cluster}_gpu_no_rgu_no_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-15",
                gres_gpu=120 * 150,
                gpu_type=f"{cluster}_gpu_no_rgu_with_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-16",
                gres_gpu=1000,
                gpu_type=f"{cluster}_gpu_with_rgu_no_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-17",
                gres_gpu=90 * 50,
                gpu_type=f"{cluster}_gpu_with_rgu_with_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-17",
                gres_gpu=400,
                gpu_type="A100",
            ),
            # since 2023-02-18
            Row(
                cluster_name=cluster,
                start_time="2023-02-18",
                gres_gpu=1000,
                gpu_type=f"{cluster}_gpu_no_rgu_no_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-19",
                gres_gpu=120 * 150,
                gpu_type=f"{cluster}_gpu_no_rgu_with_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-20",
                gres_gpu=1000,
                gpu_type=f"{cluster}_gpu_with_rgu_no_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-21",
                gres_gpu=90 * 50,
                gpu_type=f"{cluster}_gpu_with_rgu_with_billing",
            ),
            Row(
                cluster_name=cluster,
                start_time="2023-02-22",
                gres_gpu=400,
                gpu_type="A100",
            ),
            # other clusters
            Row(
                cluster_name=cluster_without_billing,
                start_time="2023-02-21",
                gres_gpu=5678,
                gpu_type=f"{cluster_without_billing}_gpu_9",
            ),
        ]

    def __repr__(self):
        """Return data as a JSON string, for debugging and file regression."""
        return json.dumps([row.json() for row in self.data], indent=1)

    def frame(self) -> pandas.DataFrame:
        """Generate data frame."""
        return pandas.DataFrame(
            [
                {
                    "cluster_name": row.cluster_name,
                    "start_time": row.start_time,
                    "allocated.gres_gpu": row.gres_gpu,
                    "allocated.gpu_type": row.gpu_type,
                }
                for row in self.data
            ]
        )

    def get_expected(self):
        """Compute expected RGU values."""
        clusters = {
            cluster.cluster_name: cluster for cluster in get_available_clusters()
        }

        expected_values = []
        for row in self.data:
            cluster = clusters[row.cluster_name]
            billings = get_cluster_gpu_billings(row.cluster_name)
            if cluster.billing_is_gpu:
                # If cluster billing is GPU, then
                # RGU is computed exactly as if there is no RGU billing
                # (i.e. gpu_count * RGU; no billing involved).
                row.expect_before(None)
            elif billings:
                # Get billing associated to this job, if possible.
                # Iterate billings in reverse order to find a billing
                # whom billing date <= job date.
                for curr_billing in reversed(billings):
                    if curr_billing.since <= row.start_time:
                        fn_expect = row.expect_after
                        billing = curr_billing
                        break
                else:
                    # If no billing found, we assume job ran before the oldest billing date.
                    fn_expect = row.expect_before
                    billing = billings[0]
                # Compute expected values.
                # Will be saved in field Row.expected
                fn_expect(billing)
            else:
                # If no billing available,
                # we expect gres_gpu unchanged, and RGU values to NaN.
                row.expected = Expected(
                    gres_gpu=row.gres_gpu, gres_rgu=math.nan, gpu_type_rgu=math.nan
                )
            expected_values.append(row.expected)

        # Then extract RGU-related column values.
        expected_gres_gpu = [expected.gres_gpu for expected in expected_values]
        expected_gres_rgu = [expected.gres_rgu for expected in expected_values]
        expected_gres_gpu_type_rgu = [
            expected.gpu_type_rgu for expected in expected_values
        ]
        return expected_gres_gpu, expected_gres_rgu, expected_gres_gpu_type_rgu


@pytest.fixture
def cluster_no_gpu_billing():
    return "hyrule"


@pytest.fixture
def cluster_no_gpu_billing_2():
    return "gerudo"


@pytest.fixture
def cluster_gpu_billing_one_date():
    return "raisin"


@pytest.fixture
def cluster_gpu_billing_many_dates():
    return "patate"


@pytest.fixture
def cluster_gpu_billing_is_gpu():
    return "mila"


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_clusters_gpu_billings(
    cluster_no_gpu_billing,
    cluster_no_gpu_billing_2,
    cluster_gpu_billing_one_date,
    cluster_gpu_billing_many_dates,
):
    """Just check available GPU Billings per tested clusters."""
    assert get_cluster_gpu_billings(cluster_no_gpu_billing) == []
    assert get_cluster_gpu_billings(cluster_no_gpu_billing_2) == []
    assert len(get_cluster_gpu_billings(cluster_gpu_billing_one_date)) == 1
    assert len(get_cluster_gpu_billings(cluster_gpu_billing_many_dates)) > 1


def test_get_rgus_mocked(monkeypatch):
    """Just check that we can correctly mock get_rgus() for testing."""
    monkeypatch.setattr("sarc.client.gpumetrics.get_rgus", _gen_fake_rgus)

    from sarc.client.gpumetrics import get_rgus

    assert get_rgus() == _gen_fake_rgus()


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_data_frame_output_size(
    cluster_no_gpu_billing,
    cluster_no_gpu_billing_2,
    cluster_gpu_billing_one_date,
    monkeypatch,
):
    """Check that nothing is computed if cluster does not have GPU billing."""
    monkeypatch.setattr("sarc.client.series.get_rgus", _gen_fake_rgus)

    frame = ExampleData(cluster_gpu_billing_one_date).frame()
    nb_rows = frame.shape[0]
    assert nb_rows

    nans = pandas.Series([np.nan] * nb_rows)
    assert frame.shape == (nb_rows, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    update_cluster_job_series_rgu(frame, cluster_no_gpu_billing)
    assert frame.shape == (nb_rows, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    update_cluster_job_series_rgu(frame, cluster_no_gpu_billing_2)
    assert frame.shape == (nb_rows, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    # Then, with full config, we should have updates.
    update_cluster_job_series_rgu(frame, cluster_gpu_billing_one_date)
    assert frame.shape == (nb_rows, 6)
    assert not frame["allocated.gres_rgu"].equals(nans)
    assert not frame["allocated.gpu_type_rgu"].equals(nans)


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_update_job_series_rgu_one_date(
    cluster_gpu_billing_one_date, monkeypatch, file_regression
):
    """Concrete test with a cluster which does have 1 billing date."""
    monkeypatch.setattr("sarc.client.series.get_rgus", _gen_fake_rgus)

    data = ExampleData(cluster=cluster_gpu_billing_one_date)
    frame = data.frame()
    nb_rows = frame.shape[0]
    assert nb_rows

    assert frame.shape == (nb_rows, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_job_series_rgu(frame)
    assert frame is returned_frame
    assert frame.shape == (nb_rows, 6)
    assert "allocated.gres_rgu" in frame.columns
    assert "allocated.gpu_type_rgu" in frame.columns

    (
        expected_gres_gpu,
        expected_gres_rgu,
        expected_gpu_type_rgu,
    ) = data.get_expected()
    assert frame["allocated.gres_gpu"].equals(
        pandas.Series(expected_gres_gpu, dtype=float)
    )
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))

    all_gpu_billings = {
        cluster_name: [
            {
                "since": billing.since.strftime("%Y-%m-%d"),
                "gpu_to_billing": billing.gpu_to_billing,
            }
            for billing in get_cluster_gpu_billings(cluster_name)
        ]
        for cluster_name in [cluster_gpu_billing_one_date]
    }
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
        f"{json.dumps(all_gpu_billings, indent=1)}\n\n"
        f"----\n"
        f"Data\n"
        f"----\n"
        f"{data}\n"
    )


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_update_job_series_rgu_with_many_dates(
    cluster_gpu_billing_many_dates, file_regression, monkeypatch
):
    """Concrete test with a cluster which does have many billing dates."""
    monkeypatch.setattr("sarc.client.series.get_rgus", _gen_fake_rgus)

    data = ExampleData(cluster=cluster_gpu_billing_many_dates)
    frame = data.frame()
    nb_rows = frame.shape[0]
    assert nb_rows

    assert frame.shape == (nb_rows, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_job_series_rgu(frame)
    assert frame is returned_frame
    assert frame.shape == (nb_rows, 6)
    assert "allocated.gres_rgu" in frame.columns
    assert "allocated.gpu_type_rgu" in frame.columns

    (
        expected_gres_gpu,
        expected_gres_rgu,
        expected_gpu_type_rgu,
    ) = data.get_expected()
    assert frame["allocated.gres_gpu"].equals(
        pandas.Series(expected_gres_gpu, dtype=float)
    )
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))

    all_gpu_billings = {
        cluster_name: [
            {
                "since": billing.since.strftime("%Y-%m-%d"),
                "gpu_to_billing": billing.gpu_to_billing,
            }
            for billing in get_cluster_gpu_billings(cluster_name)
        ]
        for cluster_name in [cluster_gpu_billing_many_dates]
    }
    nb_billings = len(all_gpu_billings[cluster_gpu_billing_many_dates])
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
        f"{json.dumps(all_gpu_billings, indent=1)}\n\n"
        f"----\n"
        f"Data\n"
        f"----\n"
        f"{data}\n"
    )


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_update_job_series_rgu_billing_is_gpu(
    cluster_gpu_billing_is_gpu, monkeypatch, file_regression
):
    """Concrete test with a cluster where billing_is_gpu is True."""
    monkeypatch.setattr("sarc.client.series.get_rgus", _gen_fake_rgus)

    data = ExampleData(cluster=cluster_gpu_billing_is_gpu)
    frame = data.frame()
    nb_rows = frame.shape[0]
    assert nb_rows

    assert frame.shape == (nb_rows, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_job_series_rgu(frame)
    assert frame is returned_frame
    assert frame.shape == (nb_rows, 6)
    assert "allocated.gres_rgu" in frame.columns
    assert "allocated.gpu_type_rgu" in frame.columns

    (
        expected_gres_gpu,
        expected_gres_rgu,
        expected_gpu_type_rgu,
    ) = data.get_expected()
    assert frame["allocated.gres_gpu"].equals(
        pandas.Series(expected_gres_gpu, dtype=float)
    )
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))

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


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_update_job_series_rgu_with_read_only_db(
    cluster_gpu_billing_one_date, file_regression, monkeypatch
):
    """Concrete tests with jobs from read_only_db"""
    monkeypatch.setattr("sarc.client.series.get_rgus", _gen_fake_rgus)

    def _df_to_pretty_str_before(df: pandas.DataFrame) -> str:
        fields = [
            "job_id",
            "cluster_name",
            "start_time",
            "allocated.gpu_type",
            "allocated.gres_gpu",
        ]
        return df[fields].to_markdown()

    def _df_to_pretty_str(df: pandas.DataFrame) -> str:
        fields = [
            "job_id",
            "cluster_name",
            "start_time",
            "allocated.gpu_type",
            "allocated.gres_gpu",
            "allocated.gres_rgu",
            "allocated.gpu_type_rgu",
        ]
        return df[fields].to_markdown()

    frame = load_job_series()

    str_frame_before = _df_to_pretty_str_before(frame)
    update_cluster_job_series_rgu(frame, cluster_gpu_billing_one_date)
    str_frame_after = _df_to_pretty_str(frame)

    all_gpu_billings = {
        cluster_name: [
            {
                "since": billing.since.strftime("%Y-%m-%d"),
                "gpu_to_billing": billing.gpu_to_billing,
            }
            for billing in get_cluster_gpu_billings(cluster_name)
        ]
        for cluster_name in ("raisin",)
    }

    file_regression.check(
        f"=======================================\n"
        f"Update RGU with read_only_db test jobs:\n"
        f"=======================================\n"
        f"(only GPU `A100` is meaningful here)\n"
        f"====================================\n\n"
        f"----------\n"
        f"RGU values\n"
        f"----------\n"
        f"{json.dumps(_gen_fake_rgus(), indent=1)}\n\n"
        f"----------------------------------------\n"
        f"GPU billing values for cluster `raisin`:\n"
        f"----------------------------------------\n"
        f"{json.dumps(all_gpu_billings, indent=1)}\n\n"
        f"------------------\n"
        f"Data before update\n"
        f"------------------\n"
        f"{str_frame_before}\n"
        f"-----------------\n"
        f"Data after update\n"
        f"-----------------\n"
        f"{str_frame_after}\n"
    )
