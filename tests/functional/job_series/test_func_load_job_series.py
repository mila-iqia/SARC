"""
Test load_job_series

NB:
To freeze time, we use time_machine instead of pytest-freezegun
to prevent complains from pydantic about datetime formats
in REST API derived tests.
pytest-freezegun uses a fake datetime class, causing issues,
while time_machine seems to keep using the real datetime class.
"""

import math
from datetime import datetime
from typing import Protocol

import pandas
import pytest
import time_machine
from pandas import DataFrame
from sqlmodel import Session, col, or_, select

from sarc.config import UTC
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.job_series import JobSeriesDB
from sarc.jobs.series import compute_job_statistics
from tests.common.dateutils import MTL
from tests.functional.common import MOCK_TIME, generate_fake_timeseries

ALL_COLUMNS = sorted(
    [
        "CLEAR_SCHEDULING",
        "STARTED_ON_BACKFILL",
        "STARTED_ON_SCHEDULE",
        "STARTED_ON_SUBMIT",
        "account",
        "allocated_billing",
        "allocated_cpu",
        "allocated_gpu_type",
        "allocated_gres_gpu",
        "allocated_mem",
        "allocated_node",
        "array_job_id",
        "cluster_id",
        "cluster_name",
        "constraints",
        "cpu_utilization",
        "elapsed_time",
        "end_time",
        "exit_code",
        "gpu_memory",
        "gpu_power",
        "gpu_utilization",
        "group",
        "job_db_id",
        "job_id",
        "job_state",
        "name",
        "nodes",
        "partition",
        "priority",
        "qos",
        "requested_billing",
        "requested_cpu",
        "requested_gpu_type",
        "requested_gres_gpu",
        "requested_mem",
        "requested_node",
        "signal",
        "start_time",
        "statistics",
        "submit_line",
        "submit_time",
        "system_memory",
        "task_id",
        "time_limit",
        "cluster_user",
        "sarc_user_id",
        "work_dir",
        "rgu",
        "gpu_type_rgu",
        "cpu_cost",
        "cpu_equivalent_cost",
        "cpu_overbilling_cost",
        "cpu_waste",
        "cpu_equivalent_waste",
        "gpu_cost",
        "gpu_equivalent_cost",
        "gpu_overbilling_cost",
        "gpu_waste",
        "gpu_equivalent_waste",
    ]
)

# For testing, we still check expected columns.
# If attributes in User class change, we may need to update this list.
USER_COLUMNS = sorted(["email", "display_name", "supervisors", "member_type"])

# For file regression tests, we will save data frame into a CSV.
# We won't include job `id` because it changes from a call to another
# (note that `job_id`, on the contrary, does not change).
CSV_COLUMNS = ALL_COLUMNS + USER_COLUMNS


def _parse_dt(value: datetime | str) -> datetime:
    if isinstance(value, str):
        return datetime.fromisoformat(value).astimezone()
    return value


def _apply_job_filters(
    query,
    *,
    cluster: str | None = None,
    job_state: str | None = None,
    job_id: int | list[int] | None = None,
    user: str | None = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
):
    if cluster is not None:
        query = query.join(SlurmClusterDB).where(SlurmClusterDB.name == cluster)
    if job_state is not None:
        query = query.where(SlurmJobDB.job_state == job_state)
    if job_id is not None:
        if isinstance(job_id, list):
            query = query.where(col(SlurmJobDB.job_id).in_(job_id))
        else:
            query = query.where(SlurmJobDB.job_id == job_id)
    if user is not None:
        query = query.where(SlurmJobDB.cluster_user == user)
    if end is not None:
        query = query.where(col(SlurmJobDB.submit_time) < _parse_dt(end))
    if start is not None:
        dt = _parse_dt(start)
        query = query.where(
            or_(col(SlurmJobDB.end_time).is_(None), col(SlurmJobDB.end_time) > dt)
        )
    return query


def helper_get_jobs(sess: Session, **kwargs) -> list[SlurmJobDB]:
    query = _apply_job_filters(select(SlurmJobDB).order_by(SlurmJobDB.id), **kwargs)
    return list(sess.exec(query).all())


class LoadJobSeriesFn(Protocol):
    def __call__(self, sess: Session, **kwargs) -> pandas.DataFrame: ...


def _check_load_job_series_frame(data_frame, file_regression):
    assert isinstance(data_frame, pandas.DataFrame)
    if data_frame.shape[0]:
        assert sorted(data_frame.keys().tolist()) == sorted(ALL_COLUMNS + USER_COLUMNS)
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n"
            f"\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
        )
    else:
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
        )


class BaseTestLoadJobSeries:
    """
    Base class to test_load_job_series.
    Use to test both MongoDB and REST API implementations.
    """

    # If True, tests with writing operations (e.g: writing job statistics) will be skipped.
    client_only = False

    @pytest.fixture
    def fn_load_job_series(self) -> LoadJobSeriesFn:
        """
        Abstract fixture.

        Must return a method load_job_series, wrapping JobSeriesDB in a Pandas dataframe.
        """
        raise NotImplementedError("Must implement fn_load_job_series fixture")

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_no_params(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db)
        assert data_frame.shape[0] > 0
        assert data_frame.shape[0] == len(helper_get_jobs(read_only_db))
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_cluster_str(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, cluster="patate")
        assert data_frame.shape[0] > 0
        assert data_frame["cluster_name"].unique().tolist() == ["patate"]
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_job_state(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, job_state="COMPLETED")
        assert data_frame.shape[0] > 0
        assert data_frame["job_state"].unique().tolist() == ["COMPLETED"]
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_one_job(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, job_id=10)
        assert data_frame.shape[0] == 1
        assert data_frame["job_id"].tolist() == [10]
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_one_job_wrong_cluster(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, job_id=10, cluster="patate")
        assert data_frame.shape[0] == 0
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_many_jobs(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, job_id=[8, 9])
        assert data_frame.shape[0] == 2
        assert sorted(data_frame["job_id"].tolist()) == [8, 9]
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_no_jobs(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, job_id=[])
        assert data_frame.shape[0] == 0
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_start_only(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        start = datetime(2023, 2, 19, tzinfo=MTL)
        data_frame = fn_load_job_series(read_only_db, start=start)
        assert data_frame.shape[0] > 0
        assert (data_frame["end_time"] > start).all()
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_end_only(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        end = datetime(2023, 2, 16, tzinfo=MTL)
        data_frame = fn_load_job_series(read_only_db, end=end)
        assert data_frame.shape[0] > 0
        assert (data_frame["submit_time"] < end).all()
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_start_str_only(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        # str input must be interpreted as a local-MTL date at 00:00.
        data_frame = fn_load_job_series(read_only_db, start="2023-02-19")
        assert data_frame.shape[0] > 0
        assert (data_frame["end_time"] > datetime(2023, 2, 19, tzinfo=MTL)).all()
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_end_str_only(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        # str input must be interpreted as a local-MTL date at 00:00.
        data_frame = fn_load_job_series(read_only_db, end="2023-02-16")
        assert data_frame.shape[0] > 0
        assert (data_frame["submit_time"] < datetime(2023, 2, 16, tzinfo=MTL)).all()
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_start_and_end(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        start = datetime(2023, 2, 15, tzinfo=MTL)
        end = datetime(2023, 2, 18, tzinfo=MTL)
        data_frame = fn_load_job_series(read_only_db, start=start, end=end)
        assert data_frame.shape[0] > 0
        assert (data_frame["end_time"] > start).all()
        assert (data_frame["submit_time"] < end).all()
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_user(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, user="beaubonhomme")
        assert data_frame.shape[0] > 0
        assert data_frame["email"].unique().tolist() == ["beaubonhomme@mila.quebec"]
        _check_load_job_series_frame(data_frame, file_regression)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_resubmitted(
        self, read_only_db, file_regression, fn_load_job_series
    ):
        data_frame = fn_load_job_series(read_only_db, job_id=1_000_000)
        assert data_frame.shape[0] == 2
        assert data_frame["job_id"].tolist() == [1_000_000, 1_000_000]
        _check_load_job_series_frame(data_frame, file_regression)

    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_check_end_times(self, read_only_db, fn_load_job_series):
        # Get jobs
        jobs = list(helper_get_jobs(read_only_db))
        # Get a data frame
        frame_1 = fn_load_job_series(read_only_db)
        # Get a data frame again
        frame_2 = fn_load_job_series(read_only_db)
        frame_1_end_times = []
        frame_2_end_times = []
        for i, job in enumerate(jobs):
            if job.end_time is None:
                frame_1_end_times.append(frame_1["end_time"][i])
                frame_2_end_times.append(frame_2["end_time"][i])
                # End time won't be None in data frames, because
                # load_job_series() will have set it to current time.
                assert frame_1["end_time"][i]
                assert frame_2["end_time"][i]
                # As frame_2 is generated after frame_1,
                # end times in frame 2 will be set to a current time more recent
                # than in frame 1.
                assert frame_2["end_time"][i] > frame_1["end_time"][i]
            else:
                # End time won't be changed.
                assert job.end_time == frame_1["end_time"][i]
                assert job.end_time == frame_2["end_time"][i]
        assert len(frame_1_end_times) > 1
        assert len(frame_2_end_times) > 1
        # All missing end times set by a call to load_job_series() must have same value.
        assert len(set(frame_1_end_times)) == 1
        assert len(set(frame_2_end_times)) == 1

    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_with_statistics(self, read_write_db, fn_load_job_series):
        if self.client_only:
            pytest.skip(
                "Test with writing operations not supported in client-only mode."
            )

        # List of job indices with no statistics initially,
        # then with statistic after call to job.statistics().
        job_indices = [
            1,
            2,
            3,
            4,
            5,
            6,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            23,
            1000000,
        ]
        params = {"job_id": job_indices}

        jobs = list(helper_get_jobs(read_write_db, **params))
        frame = fn_load_job_series(read_write_db, **params)
        assert jobs
        for job in jobs:
            assert not job.statistics
        for label in [
            "gpu_utilization",
            "cpu_utilization",
            "gpu_memory",
            "gpu_power",
            "system_memory",
        ]:
            assert all(math.isnan(value) for value in frame[label])

        # Save job statistics.
        for job in jobs:
            job.statistics = compute_job_statistics(job, generate_fake_timeseries(job))
            read_write_db.merge(job)
            read_write_db.commit()
            assert job.statistics

        # Generate new data frame. Relevant fields must not contain nan anymore.
        re_jobs = list(helper_get_jobs(read_write_db, **params))
        re_frame = fn_load_job_series(read_write_db, **params)
        assert re_jobs
        for i, re_job in enumerate(re_jobs):
            stats = re_job.statistics
            assert re_frame["system_memory"][i] == stats["system_memory"].max
            assert re_frame["gpu_memory"][i] == stats["gpu_memory"].max
            assert re_frame["gpu_utilization"][i] == stats["gpu_utilization"].median
            assert re_frame["cpu_utilization"][i] == stats["cpu_utilization"].median
            assert re_frame["gpu_power"][i] == stats["gpu_power"].median

        for label in [
            "gpu_utilization",
            "cpu_utilization",
            "gpu_memory",
            "gpu_power",
            "system_memory",
        ]:
            assert all(not math.isnan(value) for value in re_frame[label])

    @pytest.mark.usefixtures("tzlocal_is_mtl")
    def test_load_job_series_with_bad_gpu_utilization(
        self, read_write_db, file_regression, fn_load_job_series
    ):
        """Check that gpu_utilization > 1 is replaced with nan in job series."""
        if self.client_only:
            pytest.skip(
                "Test with writing operations not supported in client-only mode."
            )

        # Check default situation: gpu_utilization is None
        jobs = list(helper_get_jobs(read_write_db))
        frame = fn_load_job_series(read_write_db)
        assert jobs
        for job in jobs:
            assert not job.statistics
        assert all(math.isnan(value) for value in frame["gpu_utilization"])

        # Save job statistics with gpu_utilization manually set.
        for i, job in enumerate(jobs):
            job.statistics = dict(
                gpu_utilization=JobStatisticDB(
                    name="gpu_utilization",
                    median=2 * (i + 1) / len(jobs),
                    mean=0,
                    std=0,
                    q05=0,
                    q25=0,
                    q75=0,
                    max=0,
                    unused=0,
                )
            )
            read_write_db.merge(job)
            read_write_db.commit()

        # Generate new data frame.
        re_jobs = list(helper_get_jobs(read_write_db))
        re_frame = fn_load_job_series(read_write_db)

        # String representation for jobs
        jobs_markdown = pandas.DataFrame(
            {
                "cluster_name": [job.cluster.name for job in re_jobs],
                "job_id": [job.job_id for job in re_jobs],
                "gpu_utilization": [
                    job.statistics["gpu_utilization"].median for job in re_jobs
                ],
            }
        ).to_markdown()

        # String representation for job series.
        series_markdown = re_frame[
            ["cluster_name", "job_id", "gpu_utilization"]
        ].to_markdown()

        # For jobs, we expect values in gpu_utilization column.
        # For job series, we expect nan for any gpu_utilization > 1.
        file_regression.check(
            f"gpu_utilization:\n"
            f"================\n\n"
            f"Jobs:\n"
            f"{jobs_markdown}\n\n"
            f"Job series:\n"
            f"{series_markdown}\n"
        )


_STAT_LABELS = (
    "gpu_utilization",
    "cpu_utilization",
    "gpu_memory",
    "gpu_power",
    "system_memory",
)


def _apply_view_filters(
    query,
    *,
    cluster: str | None = None,
    job_state: str | None = None,
    job_id: int | list[int] | None = None,
    user: str | None = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
):
    if cluster is not None:
        query = query.where(JobSeriesDB.cluster_name == cluster)
    if job_state is not None:
        query = query.where(JobSeriesDB.job_state == job_state)
    if job_id is not None:
        if isinstance(job_id, list):
            query = query.where(col(JobSeriesDB.job_id).in_(job_id))
        else:
            query = query.where(JobSeriesDB.job_id == job_id)
    if user is not None:
        query = query.where(JobSeriesDB.cluster_user == user)
    if end is not None:
        query = query.where(col(JobSeriesDB.submit_time) < _parse_dt(end))
    if start is not None:
        dt = _parse_dt(start)
        query = query.where(
            or_(col(JobSeriesDB.end_time).is_(None), col(JobSeriesDB.end_time) > dt)
        )
    return query


def _flatten_stat(label: str, stats: dict | None) -> float:
    if not stats:
        return math.nan
    stat = stats.get(label)
    if not stat:
        return math.nan
    if label in ("system_memory", "gpu_memory"):
        return stat["max"]
    return stat["median"]


def helper_load_job_series(sess: Session, **kwargs) -> DataFrame:
    """Query JobSeriesDB view and return a dataframe."""
    query = _apply_view_filters(
        select(JobSeriesDB).order_by(JobSeriesDB.job_db_id), **kwargs
    )
    rows = sess.exec(query).all()
    now = datetime.now(tz=UTC)
    records = []
    for row in rows:
        d = row.model_dump()
        # end_time to now if None
        if d.get("end_time") is None:
            d["end_time"] = now
        # For each stat, add a flattened column, using same logic as in old load_job_series
        # Individual stats (mean, median, etc.) are still in data
        stats = d.get("statistics") or {}
        for label in _STAT_LABELS:
            d[label] = _flatten_stat(label, stats)
        # If gpu_utilization > 1, set it to NaN (same logic as in old load_job_series)
        gpu_util = d["gpu_utilization"]
        if gpu_util is not None and not math.isnan(gpu_util) and gpu_util > 1:
            d["gpu_utilization"] = math.nan
        records.append(d)
    return DataFrame(records)


class TestSqlLoadJobSeries(BaseTestLoadJobSeries):
    """Tests for SQL load_job_series"""

    @pytest.fixture
    def fn_load_job_series(self) -> LoadJobSeriesFn:
        return helper_load_job_series
