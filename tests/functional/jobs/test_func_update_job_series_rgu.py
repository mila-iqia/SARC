import json
from datetime import datetime
from pprint import pformat
from typing import Dict

import numpy as np
import pandas
import pytest

from sarc.config import MTL, ClusterConfig, config
from sarc.jobs.series import (
    load_job_series,
    update_cluster_job_series_rgu,
    update_job_series_rgu,
)

from .test_func_load_job_series import MOCK_TIME


def _gen_data_frame(
    cluster_names: list, start_times=[], gres_gpu: list = [], gpu_type: list = []
):
    """Generate a data frame suited for RGU tests."""
    assert len(cluster_names) == len(start_times) == len(gres_gpu) == len(gpu_type)
    rows = [
        {
            "cluster_name": cluster_name,
            "start_time": start_time,
            "allocated.gres_gpu": gres_gpu,
            "allocated.gpu_type": gpu_type,
        }
        for cluster_name, start_time, gres_gpu, gpu_type in zip(
            cluster_names, start_times, gres_gpu, gpu_type
        )
    ]
    frame = pandas.DataFrame(rows)
    assert frame.shape == (len(gres_gpu), 4 if len(gres_gpu) else 0)
    return frame


def _read_json(filename):
    with open(filename, "r", encoding="utf-8") as file:
        return json.load(file)


# Below, we generate fixtures for cluster configs used in these tests.
# There are 5 clusters:
# - no rgu date, no RGU mapping
# - no rgu date, only RGU mapping
# - only rgu date, no RGU mapping
# - rgu date, empty RGU mapping
# - rgu date, RGU mapping
# With 4 first configs, frame should not be updated,
# as either rgu date is missing or RGU mapping is missing or empty.
# With 5th config, frame should be updated, as all required data are available.


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.fixture
def clusters_config():
    clusters: Dict[str, ClusterConfig] = config().clusters
    return clusters


@pytest.fixture
def cluster_no_rgu(clusters_config):
    return clusters_config["hyrule"]


@pytest.fixture
def cluster_only_rgu_start_date(clusters_config):
    return clusters_config["local"]


@pytest.fixture
def cluster_only_rgu_billing(clusters_config):
    return clusters_config["patate"]


@pytest.fixture
def cluster_full_rgu_empty_billing(clusters_config):
    return clusters_config["gerudo"]


@pytest.fixture
def cluster_full_rgu(clusters_config):
    return clusters_config["raisin"]


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_clusters_rgu_config(
    cluster_no_rgu,
    cluster_only_rgu_start_date,
    cluster_only_rgu_billing,
    cluster_full_rgu_empty_billing,
    cluster_full_rgu,
):
    """Just check clusters config."""
    assert cluster_no_rgu.rgu_start_date is None
    assert cluster_no_rgu.gpu_to_rgu_billing is None

    assert cluster_only_rgu_start_date.rgu_start_date is not None
    assert cluster_only_rgu_start_date.gpu_to_rgu_billing is None

    assert cluster_only_rgu_billing.rgu_start_date is None
    assert cluster_only_rgu_billing.gpu_to_rgu_billing is not None

    assert cluster_full_rgu_empty_billing.rgu_start_date is not None
    assert cluster_full_rgu_empty_billing.gpu_to_rgu_billing is not None
    assert _read_json(cluster_full_rgu_empty_billing.gpu_to_rgu_billing) == {}

    assert cluster_full_rgu.rgu_start_date is not None
    assert cluster_full_rgu.gpu_to_rgu_billing is not None
    gpu_to_rgu_billing = _read_json(cluster_full_rgu.gpu_to_rgu_billing)
    assert isinstance(gpu_to_rgu_billing, dict)
    assert len(gpu_to_rgu_billing)


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_data_frame_output_size(
    cluster_no_rgu,
    cluster_only_rgu_start_date,
    cluster_only_rgu_billing,
    cluster_full_rgu_empty_billing,
    cluster_full_rgu,
):
    """
    Check that nothing is computed if cluster does not have both
    RGU start time and non-empty RGU/GPU ratio JSON file.
    """
    cluster_names = ["raisin"] * 5
    start_times = [
        datetime.strptime(date, "%Y-%m-%d").astimezone(MTL)
        for date in (
            "2023-02-14",
            "2023-02-15",
            "2023-02-16",
            "2023-02-17",
            "2023-02-18",
        )
    ]
    gres_gpu = [1, 2, 3, 4, 5]
    gpu_type = [
        "raisin_gpu_1",
        "raisin_gpu_2",
        "raisin_gpu_3",
        "raisin_gpu_4",
        "raisin_gpu_5",
    ]

    nans = pandas.Series([np.nan] * 5)

    frame = _gen_data_frame(cluster_names, start_times, gres_gpu, gpu_type)
    assert frame.shape == (5, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    update_cluster_job_series_rgu(frame, cluster_no_rgu)
    assert frame.shape == (5, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    update_cluster_job_series_rgu(frame, cluster_only_rgu_start_date)
    assert frame.shape == (5, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    update_cluster_job_series_rgu(frame, cluster_only_rgu_billing)
    assert frame.shape == (5, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    update_cluster_job_series_rgu(frame, cluster_full_rgu_empty_billing)
    assert frame.shape == (5, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    # Then, with full config, we should have updates.
    update_cluster_job_series_rgu(frame, cluster_full_rgu)
    assert frame.shape == (5, 6)
    assert not frame["allocated.gres_rgu"].equals(nans)
    assert not frame["allocated.gpu_type_rgu"].equals(nans)


def _gen_complex_data_frame():
    cluster_names = (["raisin"] * 9) + ["fromage", "patate", "fromage"]
    start_times = [
        datetime.strptime(date, "%Y-%m-%d").astimezone(MTL)
        for date in (
            "2023-02-12",
            "2023-02-13",
            "2023-02-14",
            "2023-02-15",
            "2023-02-16",
            "2023-02-17",
            "2023-02-18",
            "2023-02-19",
            "2023-02-20",
            "2023-02-21",  # job belongs to cluster fromage
            "2023-02-21",  # job belongs to cluster patate
            "2023-02-22",  # job belongs to cluster fromage
        )
    ]
    gres_gpu = [1, 2, 3, 4, 5000, 6000, 7000, 8000, 9000, 123, 5678, 91011]
    gpu_type = [
        "raisin_gpu_unknown_1",
        "raisin_gpu_unknown_2",
        "raisin_gpu_3",
        "raisin_gpu_4",
        "raisin_gpu_5",
        "raisin_gpu_unknown_6",
        "A100",
        "raisin_gpu_unknown_8",
        "raisin_gpu_unknown_9",
        "fromage_gpu_1",  # job belongs to cluster fromage
        "patate_gpu_9",  # job belongs to cluster patate
        "fromage_gpu_2",  # job belongs to cluster fromage
    ]
    return _gen_data_frame(cluster_names, start_times, gres_gpu, gpu_type)


def _get_expected_columns_with_cluster_raisin():
    """
    Return expected columns when complex data frame is updated using only cluster raisin.
    """
    expected_gres_gpu = [
        1.0,  # before 2023-02-16, should not change (even if GPU type is unknown)
        2.0,  # before 2023-02-16, should not change (even if GPU type is unknown)
        3.0,  # before 2023-02-16, should not change
        4.0,  # before 2023-02-16, should not change
        5000 / 500,  # from 2023-12-16, should be divided by RGU/GPU ratio
        np.nan,  # from 2023-12-16, unknown GPU type, should be nan
        7000 / 700,  # from 2023-12-16, should be divided by RGU/GPU ratio
        np.nan,  # from 2023-12-16, unknown GPU type, should be nan
        np.nan,  # from 2023-12-16, unknown GPU type, should be nan
        123,  # job does not belong to cluster raisin, then should not change
        5678,  # job does not belong to cluster raisin, then should not change
        91011,  # job does not belong to cluster raisin, then should not change
    ]
    expected_gres_rgu = [
        np.nan,  # before 2023-12-16, unknown GPU type, should be nan
        np.nan,  # before 2023-12-16, unknown GPU type, should be nan
        3 * 300.0,  # before 2023-12-16, should be gres_gpu * RGU/GPU ratio
        4 * 400.0,  # before 2023-12-16, should be gres_gpu * RGU/GPU ratio
        5000.0,  # from 2023-12-16, should be gres_gpu
        6000.0,  # from 2023-12-16, should be gres_gpu (even if GPU type is unknown)
        7000.0,  # from 2023-12-16, should be gres_gpu
        8000.0,  # from 2023-12-16, should be gres_gpu (even if GPU type is unknown)
        9000.0,  # from 2023-12-16, should be gres_gpu (even if GPU type is unknown)
        np.nan,  # job does not belong to cluster raisin, then should have nan here
        np.nan,  # job does not belong to cluster raisin, then should have nan here
        np.nan,  # job does not belong to cluster raisin, then should have nan here
    ]
    expected_gpu_type_rgu = [
        np.nan,  # GPU type unknown, should be nan
        np.nan,  # GPU type unknown, should be nan
        300,  # GPU type exists in RGU map, should be copied here
        400,  # GPU type exists in RGU map, should be copied here
        500,  # GPU type exists in RGU map, should be copied here
        np.nan,  # GPU type unknown, should be nan
        700,  # GPU type exists in RGU map, should be copied here
        np.nan,  # GPU type unknown, should be nan
        np.nan,  # GPU type unknown, should be nan
        np.nan,  # job does not belong to cluster raisin, then should have nan here
        np.nan,  # job does not belong to cluster raisin, then should have nan here
        np.nan,  # job does not belong to cluster raisin, then should have nan here
    ]

    return expected_gres_gpu, expected_gres_rgu, expected_gpu_type_rgu


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_update_cluster_job_series_rgu(cluster_full_rgu):
    """Concrete test for 1 cluster with a generated frame."""
    assert cluster_full_rgu.rgu_start_date == "2023-02-16"
    frame = _gen_complex_data_frame()
    assert frame.shape == (12, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_cluster_job_series_rgu(frame, cluster_full_rgu)
    assert frame is returned_frame
    assert frame.shape == (12, 6)
    assert "allocated.gres_rgu" in frame.columns
    assert "allocated.gpu_type_rgu" in frame.columns

    (
        expected_gres_gpu,
        expected_gres_rgu,
        expected_gpu_type_rgu,
    ) = _get_expected_columns_with_cluster_raisin()
    assert frame["allocated.gres_gpu"].equals(pandas.Series(expected_gres_gpu))
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_update_job_series_rgu():
    """Concrete test for all clusters with a generated frame."""
    frame = _gen_complex_data_frame()
    assert frame.shape == (12, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_job_series_rgu(frame)
    assert frame is returned_frame
    assert frame.shape == (12, 6)
    assert "allocated.gres_rgu" in frame.columns
    assert "allocated.gpu_type_rgu" in frame.columns

    (
        expected_gres_gpu,
        expected_gres_rgu,
        expected_gpu_type_rgu,
    ) = _get_expected_columns_with_cluster_raisin()
    expected_gres_gpu[-3:] = [
        123.0,  # job belongs to cluster fromage before RGU, should not change
        5678.0,  # job belongs to cluster patate, no RGU, then no change
        91011 / 200,  # job belongs to cluster fromage after RGU, divided by RGU/GPU
    ]
    expected_gres_rgu[-3:] = [
        123 * 100.0,  # job from to cluster fromage before RGU: gres_gpu * RGU/GPU ratio
        np.nan,  # job belongs to cluster patate, no RGU, then should have nan here
        91011.0,  # job belongs to cluster fromage after RGU, should be gres_gpu
    ]
    expected_gpu_type_rgu[-3:] = [
        100.0,  # job belongs to cluster fromage, GPU type should be copied here
        np.nan,  # job belongs to cluster patate, no RGU, then should have nan here
        200.0,  # job belongs to cluster fromage, GPU type should be copied here
    ]
    assert frame["allocated.gres_gpu"].equals(pandas.Series(expected_gres_gpu))
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_update_job_series_rgu_with_real_test_data(cluster_full_rgu, file_regression):
    """Concrete tests with jobs from read_only_db"""
    frame = load_job_series()
    update_cluster_job_series_rgu(frame, cluster_full_rgu)

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

    file_regression.check(
        f"Update job series RGU for {frame.shape[0]} job(s):\n\n"
        f"RGU start date: {cluster_full_rgu.rgu_start_date}\n\n"
        f"gpu_to_rgu_billing:\n{pformat(_read_json(cluster_full_rgu.gpu_to_rgu_billing))}\n\n"
        f"{_df_to_pretty_str(frame)}"
    )
