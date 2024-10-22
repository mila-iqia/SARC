import json
from datetime import datetime

import numpy as np
import pandas
import pytest

from sarc.client.rgu import get_cluster_rgus
from sarc.client.series import (
    load_job_series,
    update_cluster_job_series_rgu,
    update_job_series_rgu,
)
from sarc.config import MTL

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


@pytest.fixture
def cluster_no_rgu():
    return "hyrule"


@pytest.fixture
def cluster_no_rgu_2():
    return "gerudo"


@pytest.fixture
def cluster_full_rgu_one_date():
    return "raisin"


@pytest.fixture
def cluster_full_rgu_many_dates():
    return "patate"


@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
def test_clusters_rgu_config(
    cluster_no_rgu,
    cluster_no_rgu_2,
    cluster_full_rgu_one_date,
    cluster_full_rgu_many_dates,
):
    """Just check clusters config."""
    assert get_cluster_rgus(cluster_no_rgu) == []
    assert get_cluster_rgus(cluster_no_rgu_2) == []
    assert len(get_cluster_rgus(cluster_full_rgu_one_date)) == 1
    assert len(get_cluster_rgus(cluster_full_rgu_many_dates)) > 1


@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
def test_data_frame_output_size(
    cluster_no_rgu,
    cluster_no_rgu_2,
    cluster_full_rgu_one_date,
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

    update_cluster_job_series_rgu(frame, cluster_no_rgu_2)
    assert frame.shape == (5, 6)
    assert frame["allocated.gres_rgu"].equals(nans)
    assert frame["allocated.gpu_type_rgu"].equals(nans)

    # Then, with full config, we should have updates.
    update_cluster_job_series_rgu(frame, cluster_full_rgu_one_date)
    assert frame.shape == (5, 6)
    assert not frame["allocated.gres_rgu"].equals(nans)
    assert not frame["allocated.gpu_type_rgu"].equals(nans)


def _gen_complex_data_frame():
    cluster_names = (["raisin"] * 9) + ["fromage", "hyrule", "fromage"]
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
            "2023-02-21",  # job belongs to cluster hyrule
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
        "hyrule_gpu_9",  # job belongs to cluster hyrule
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


@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
def test_update_cluster_job_series_rgu(cluster_full_rgu_one_date):
    """Concrete test for 1 cluster with a generated frame."""
    frame = _gen_complex_data_frame()
    assert frame.shape == (12, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_cluster_job_series_rgu(frame, cluster_full_rgu_one_date)
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


@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
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
        123
        * 100.0,  # job belongs to cluster fromage before RGU: gres_gpu * RGU/GPU ratio
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


def _gen_complex_data_frame_with_many_dates():
    cluster_names = (["patate"] * 9) + ["fromage", "hyrule", "fromage"]
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
            "2023-02-21",  # job belongs to cluster hyrule (no RGU available)
            "2023-02-22",  # job belongs to cluster fromage
        )
    ]
    gres_gpu = [1, 2, 3, 4, 5000, 6000, 7000, 8000, 9000, 123, 5678, 91011]
    gpu_type = [
        "patate_gpu_unknown_0",
        "patate_gpu_2",
        "patate_gpu_3",
        "patate_gpu_1",
        "patate_gpu_unknown",
        "patate_gpu_2",
        "A100",
        "patate_gpu_unknown_8",
        "patate_gpu_1",
        "fromage_gpu_1",  # job belongs to cluster fromage
        "patate_gpu_9",  # job belongs to cluster patate
        "fromage_gpu_2",  # job belongs to cluster fromage
    ]
    return _gen_data_frame(cluster_names, start_times, gres_gpu, gpu_type)


def _get_expected_columns_with_cluster_patate_and_many_dates():
    """
    Return expected columns when complex data frame is updated using cluster patate
    """
    expected_gres_gpu = [
        1.0,  # before 2023-02-15, no RGU, should not change
        2.0,  # before 2023-02-15, no RGU, should not change
        3.0,  # before 2023-02-15, no RGU, should not change
        4 / 400,  # In [2023-12-15, 2023-12-18), should be divided by RGU/GPU ratio
        np.nan,  # from 2023-12-15, unknown GPU type, should be nan
        6000 / 700,  # In [2023-12-15, 2023-12-18), should be divided by RGU/GPU ratio
        np.nan,  # from 2023-12-18, unknown GPU type, shoud be nan
        np.nan,  # from 2023-12-18, unknown GPU type, should be nan
        9000 / 440,  # from 2023-12-18, should be divided by RGU/GPU ratio
        123,  # job does not belong to cluster raisin, then should not change
        5678,  # job does not belong to cluster raisin, then should not change
        91011,  # job does not belong to cluster raisin, then should not change
    ]
    expected_gres_rgu = [
        np.nan,  # before 2023-12-15, unknown GPU type, shoud be nan
        2.0
        * 700,  # before 2023-12-15, should be gres_gpu * RGU/GPU ratio from 2023-12-15 ratios
        3.0
        * 300,  # before 2023-12-15, should be gres_gpu * RGU/GPU ratio from 2023-12-15 ratios
        4,  # [2023-12-15, 2023-02-18), should be gres_gpu
        5000.0,  # from 2023-12-15, should be gres_gpu (event if GPU type is unknown)
        6000.0,  # [2023-12-15, 2023-02-18), should be gres_gpu
        7000.0,  # from 2023-12-18, should be gres_gpu (even if GPU type is unknown)
        8000.0,  # from 2023-12-18, should be gres_gpu (even if GPU type is unknown)
        9000.0,  # from 2023-12-18, should be gres_gpu
        np.nan,  # job does not belong to cluster raisin, then should have nan here
        np.nan,  # job does not belong to cluster raisin, then should have nan here
        np.nan,  # job does not belong to cluster raisin, then should have nan here
    ]
    expected_gpu_type_rgu = [
        np.nan,  # GPU type unknown from 2023-02-15, shoud be nan
        700,  # RGU/GPU from 2023-02-15
        300,  # RGU/GPU from 2023-02-15
        400,
        np.nan,  # GPU type unknown in [2023-02-15, 2023-02-18), should be nan
        700,
        np.nan,  # GPU type unknown in [2023-02-18, ...), should be nan
        np.nan,  # GPU type unknown in [2023-02-18, ...), should be nan
        440,
        np.nan,  # job does not belong to cluster patate, then should have nan here
        np.nan,  # job does not belong to cluster patate, then should have nan here
        np.nan,  # job does not belong to cluster patate, then should have nan here
    ]

    return expected_gres_gpu, expected_gres_rgu, expected_gpu_type_rgu


@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
def test_update_cluster_job_series_rgu_with_many_dates(cluster_full_rgu_many_dates):
    """Concrete test for 1 cluster with a generated frame and many RGU dates."""
    frame = _gen_complex_data_frame_with_many_dates()
    assert frame.shape == (12, 4)
    assert "allocated.gres_rgu" not in frame.columns
    assert "allocated.gpu_type_rgu" not in frame.columns

    returned_frame = update_cluster_job_series_rgu(frame, cluster_full_rgu_many_dates)
    assert frame is returned_frame
    assert frame.shape == (12, 6)
    assert "allocated.gres_rgu" in frame.columns
    assert "allocated.gpu_type_rgu" in frame.columns

    (
        expected_gres_gpu,
        expected_gres_rgu,
        expected_gpu_type_rgu,
    ) = _get_expected_columns_with_cluster_patate_and_many_dates()
    assert frame["allocated.gres_gpu"].equals(pandas.Series(expected_gres_gpu))
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))


@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
def test_update_job_series_rgu_with_many_dates():
    """Concrete test for all clusters with a generated frame and many RGU dates."""
    frame = _gen_complex_data_frame_with_many_dates()
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
    ) = _get_expected_columns_with_cluster_patate_and_many_dates()
    expected_gres_gpu[-3:] = [
        123.0,  # job belongs to cluster fromage before RGU, should not change
        5678.0,  # job belongs to cluster hyrule, no RGU, then no change
        91011 / 200,  # job belongs to cluster fromage after RGU, divided by RGU/GPU
    ]
    expected_gres_rgu[-3:] = [
        123
        * 100.0,  # job belongs to cluster fromage before RGU: gres_gpu * RGU/GPU ratio
        np.nan,  # job belongs to cluster hyrule, no RGU, then should have nan here
        91011.0,  # job belongs to cluster fromage after RGU, should be gres_gpu
    ]
    expected_gpu_type_rgu[-3:] = [
        100.0,  # job belongs to cluster fromage, GPU type should be copied here
        np.nan,  # job belongs to cluster hyrule, no RGU, then should have nan here
        200.0,  # job belongs to cluster fromage, GPU type should be copied here
    ]
    assert frame["allocated.gres_gpu"].equals(pandas.Series(expected_gres_gpu))
    assert frame["allocated.gres_rgu"].equals(pandas.Series(expected_gres_rgu))
    assert frame["allocated.gpu_type_rgu"].equals(pandas.Series(expected_gpu_type_rgu))


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db_with_users_client", "tzlocal_is_mtl")
def test_update_job_series_rgu_with_real_test_data(
    cluster_full_rgu_one_date, file_regression
):
    """Concrete tests with jobs from read_only_db"""
    frame = load_job_series()
    update_cluster_job_series_rgu(frame, cluster_full_rgu_one_date)

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
        f"{_df_to_pretty_str(frame)}"
    )
