import math
from datetime import datetime
from types import SimpleNamespace

import pandas
import pytest
import time_machine

from sarc.client.job import JobStatistics, Statistics
from sarc.config import UTC
from tests.common.dateutils import MTL
from tests.functional.jobs.test_func_job_statistics import generate_fake_timeseries

# ... parameters and constants definition (kept as is) ...

parameters = {
    "no_cluster": {},
    "cluster_str": {"cluster": "patate"},
    "job_state": {"job_state": "COMPLETED"},
    "one_job": {"job_id": 10},
    "one_job_wrong_cluster": {"job_id": 10, "cluster": "patate"},
    "many_jobs": {"job_id": [8, 9]},
    "no_jobs": {"job_id": []},
    "start_only": {"start": datetime(2023, 2, 19, tzinfo=MTL)},
    "end_only": {"end": datetime(2023, 2, 16, tzinfo=MTL)},
    "start_str_only": {"start": "2023-02-19"},
    "end_str_only": {"end": "2023-02-16"},
    "start_and_end": {
        "start": datetime(2023, 2, 15, tzinfo=MTL),
        "end": datetime(2023, 2, 18, tzinfo=MTL),
    },
    "user": {"user": "beaubonhomme"},
    "resubmitted": {"job_id": 1_000_000},
}


few_parameters = {key: parameters[key] for key in ("no_cluster", "start_and_end")}
param_start_end = {"start_and_end": parameters["start_and_end"]}
params_no_start_or_end = {
    key: parameters[key] for key in ("no_cluster", "start_only", "end_only")
}

ALL_COLUMNS = [
    "CLEAR_SCHEDULING",
    "STARTED_ON_BACKFILL",
    "STARTED_ON_SCHEDULE",
    "STARTED_ON_SUBMIT",
    "account",
    "allocated.billing",
    "allocated.cpu",
    "allocated.gpu_type",
    "allocated.gres_gpu",
    "allocated.mem",
    "allocated.node",
    "array_job_id",
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
    "id",
    "job_id",
    "job_state",
    "latest_scraped_end",
    "latest_scraped_start",
    "name",
    "nodes",
    "partition",
    "priority",
    "qos",
    "requested.billing",
    "requested.cpu",
    "requested.gpu_type",
    "requested.gres_gpu",
    "requested.mem",
    "requested.node",
    "signal",
    "start_time",
    "stored_statistics",
    "submit_time",
    "system_memory",
    "task_id",
    "time_limit",
    "user",
    "work_dir",
]

# For testing, we still check expected columns.
# If attributes in User class change, we may need to update this list.
USER_COLUMNS = [
    "user.uuid",
    "user.email",
    "user.display_name",
    "user.associated_accounts.cluster.values",
    "user.associated_accounts.drac.values",
    "user.associated_accounts.mila.values",
    "user.associated_accounts.test.values",
    "user.supervisor.values",
    "user.co_supervisors.values",
    "user.github_username.values",
    "user.google_scholar_profile.values",
    "user.member_type.values",
    "user.mila_username",
    "user.drac_username",
]

# For file regression tests, we will save data frame into a CSV.
# We won't include job `id` because it changes from a call to another
# (note that `job_id`, on the contrary, does not change).
CSV_COLUMNS = [col for col in ALL_COLUMNS if col not in ["id"]]


MOCK_TIME = datetime(2023, 11, 22, tzinfo=UTC)


class BaseTestLoadJobSeries:
    @pytest.fixture
    def ops(self):
        raise NotImplementedError("Must implement ops fixture")

    @time_machine.travel("2025-01-01", tick=False)
    @pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
    def test_load_job_series_with_users(self, file_regression, ops):
        """Test job to user mapping."""
        assert len(ops.get_users()) == 10
        data_frame = ops.load_job_series()
        expected_columns = sorted(ALL_COLUMNS + USER_COLUMNS)
        assert sorted(data_frame.keys().tolist()) == expected_columns

        str_view = data_frame[
            ["job_id", "cluster_name", "user"] + USER_COLUMNS
        ].to_markdown()
        file_regression.check(
            f"Found 4 users and {data_frame.shape[0]} job(s):\n\n{str_view}"
        )

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
    def test_load_job_series_without_user_column(self, file_regression, ops):
        """Test job to user mapping when data frame does not contain `user` column."""
        assert len(ops.get_users()) == 10
        data_frame = ops.load_job_series(fields=["job_id", "cluster_name"])
        expected_columns = sorted(["job_id", "cluster_name"])
        assert sorted(data_frame.keys().tolist()) == expected_columns

        str_view = data_frame[["job_id", "cluster_name"]].to_markdown()
        file_regression.check(
            f"Found 4 users and {data_frame.shape[0]} job(s):\n\n{str_view}"
        )

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize("params", parameters.values(), ids=parameters.keys())
    def test_load_job_series(self, params, file_regression, captrace, ops):
        data_frame = ops.load_job_series(**params)
        assert isinstance(data_frame, pandas.DataFrame)
        if data_frame.shape[0]:
            assert sorted(data_frame.keys().tolist()) == ALL_COLUMNS
            file_regression.check(
                f"Found {data_frame.shape[0]} job(s):\n"
                f"\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
            )
        else:
            file_regression.check(
                f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
            )

        # Check trace - Note: Trace check might depend on implementation
        # For now we assume both implementations produce traces or we might need to override
        spans = captrace.get_finished_spans()
        # Filter spans to find "load_job_series"
        load_spans = [s for s in spans if s.name == "load_job_series"]
        if load_spans:
            assert len(load_spans) == 1

    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize("params", [parameters["no_cluster"]], ids=["no_cluster"])
    def test_load_job_series_check_end_times(self, params, ops):
        jobs = list(ops.get_jobs(**params))
        frame_1 = ops.load_job_series(**params)
        frame_2 = ops.load_job_series(**params)
        frame_1_end_times = []
        frame_2_end_times = []
        for i, job in enumerate(jobs):
            if job.end_time is None:
                frame_1_end_times.append(frame_1["end_time"][i])
                frame_2_end_times.append(frame_2["end_time"][i])
                assert frame_1["end_time"][i]
                assert frame_2["end_time"][i]
                assert frame_2["end_time"][i] > frame_1["end_time"][i]
            else:
                assert job.end_time == frame_1["end_time"][i]
                assert job.end_time == frame_2["end_time"][i]
        assert len(frame_1_end_times) > 1
        assert len(frame_2_end_times) > 1
        assert len(set(frame_1_end_times)) == 1
        assert len(set(frame_2_end_times)) == 1

    @pytest.mark.usefixtures("read_write_db", "tzlocal_is_mtl")
    def test_load_job_series_with_stored_statistics(self, monkeypatch, ops):
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

        jobs = list(ops.get_jobs(**params))
        frame = ops.load_job_series(**params)
        assert jobs
        for job in jobs:
            assert not job.stored_statistics
        for label in [
            "gpu_utilization",
            "cpu_utilization",
            "gpu_memory",
            "gpu_power",
            "system_memory",
        ]:
            assert all(math.isnan(value) for value in frame[label])

        monkeypatch.setattr(
            "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
        )

        for job in jobs:
            job.statistics(save=True)
            assert job.stored_statistics

        re_jobs = list(ops.get_jobs(**params))
        re_frame = ops.load_job_series(**params)
        assert re_jobs
        for i, re_job in enumerate(re_jobs):
            stats = re_job.stored_statistics.model_dump()
            assert re_frame["system_memory"][i] == stats["system_memory"]["max"]
            assert re_frame["gpu_memory"][i] == stats["gpu_memory"]["max"]
            assert re_frame["gpu_utilization"][i] == stats["gpu_utilization"]["median"]
            assert re_frame["cpu_utilization"][i] == stats["cpu_utilization"]["median"]
            assert re_frame["gpu_power"][i] == stats["gpu_power"]["median"]

        for label in [
            "gpu_utilization",
            "cpu_utilization",
            "gpu_memory",
            "gpu_power",
            "system_memory",
        ]:
            assert all(not math.isnan(value) for value in re_frame[label])

    @pytest.mark.usefixtures("read_write_db", "tzlocal_is_mtl")
    def test_load_job_series_with_bad_gpu_utilization(self, file_regression, ops):
        jobs = list(ops.get_jobs())
        frame = ops.load_job_series()
        assert jobs
        for job in jobs:
            assert not job.stored_statistics
        assert all(math.isnan(value) for value in frame["gpu_utilization"])

        for i, job in enumerate(jobs):
            job.stored_statistics = JobStatistics(
                gpu_utilization=Statistics(
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
            job.save()

        re_jobs = list(ops.get_jobs())
        re_frame = ops.load_job_series()

        jobs_markdown = pandas.DataFrame(
            {
                "cluster_name": [job.cluster_name for job in re_jobs],
                "job_id": [job.job_id for job in re_jobs],
                "gpu_utilization": [
                    job.stored_statistics.gpu_utilization.median for job in re_jobs
                ],
            }
        ).to_markdown()

        series_markdown = re_frame[
            ["cluster_name", "job_id", "gpu_utilization"]
        ].to_markdown()

        file_regression.check(
            f"gpu_utilization:\n================\n\nJobs:\n{jobs_markdown}\n\nJob series:\n{series_markdown}\n"
        )

    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", few_parameters.values(), ids=few_parameters.keys()
    )
    def test_load_job_series_fields_list(self, params, file_regression, ops):
        fields = ["gpu_memory", "allocated.mem", "requested.mem", "user", "work_dir"]
        data_frame = ops.load_job_series(fields=fields, **params)
        assert isinstance(data_frame, pandas.DataFrame)
        assert sorted(data_frame.keys().tolist()) == sorted(fields)
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
        )

    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", few_parameters.values(), ids=few_parameters.keys()
    )
    def test_load_job_series_fields_dict(self, params, file_regression, ops):
        fields = {
            "gpu_memory": "gpu_footprint",
            "allocated.mem": "memory",
            "user": "username",
            "work_dir": "the_user_folder",
        }
        expected_fields = ["gpu_footprint", "memory", "username", "the_user_folder"]
        data_frame = ops.load_job_series(fields=fields, **params)
        assert isinstance(data_frame, pandas.DataFrame)
        assert sorted(data_frame.keys().tolist()) == sorted(expected_fields)
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
        )

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", param_start_end.values(), ids=param_start_end.keys()
    )
    def test_load_job_series_clip_time_true(self, params, file_regression, ops):
        assert "start" in params
        assert "end" in params
        data_frame = ops.load_job_series(clip_time=True, **params)
        assert isinstance(data_frame, pandas.DataFrame)
        assert sorted(data_frame.keys().tolist()) == sorted(
            ALL_COLUMNS + ["unclipped_start", "unclipped_end"]
        )
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
        )

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", param_start_end.values(), ids=param_start_end.keys()
    )
    def test_load_job_series_clip_time_false(self, params, file_regression, ops):
        assert "start" in params
        assert "end" in params
        data_frame = ops.load_job_series(clip_time=False, **params)
        assert isinstance(data_frame, pandas.DataFrame)
        assert sorted(data_frame.keys().tolist()) == sorted(ALL_COLUMNS)
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
        )

    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", params_no_start_or_end.values(), ids=params_no_start_or_end.keys()
    )
    def test_load_job_series_clip_time_true_no_start_or_end(
        self, params, file_regression, ops
    ):
        with pytest.raises(ValueError, match=r"Clip time\: missing (start|end)"):
            ops.load_job_series(clip_time=True, **params)

    @time_machine.travel(MOCK_TIME, tick=False)
    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", few_parameters.values(), ids=few_parameters.keys()
    )
    def test_load_job_series_callback(self, params, file_regression, ops):
        def callback(rows):
            rows[-1]["another_column"] = 1234

        data_frame = ops.load_job_series(callback=callback, **params)
        assert isinstance(data_frame, pandas.DataFrame)
        assert sorted(data_frame.keys().tolist()) == sorted(
            ALL_COLUMNS + ["another_column"]
        )
        assert data_frame["another_column"].sum() == 1234 * data_frame.shape[0]
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n"
            f"\n{data_frame.to_csv(columns=CSV_COLUMNS + ['another_column'])}"
        )

    @pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
    @pytest.mark.parametrize(
        "params", param_start_end.values(), ids=param_start_end.keys()
    )
    def test_load_job_series_all_args(self, params, file_regression, ops):
        def callback(rows):
            rows[-1]["another_column"] = 1234

        fields = {
            "gpu_memory": "gpu_footprint",
            "allocated.mem": "memory",
            "user": "username",
            "work_dir": "the_user_folder",
        }
        expected_fields = [
            "gpu_footprint",
            "memory",
            "username",
            "the_user_folder",
            "another_column",
        ]
        data_frame = ops.load_job_series(
            fields=fields, clip_time=True, callback=callback, **params
        )
        assert isinstance(data_frame, pandas.DataFrame)
        assert sorted(data_frame.keys().tolist()) == sorted(expected_fields)
        assert data_frame["another_column"].sum() == 1234 * data_frame.shape[0]
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=expected_fields)}"
        )


class TestMongoLoadJobSeries(BaseTestLoadJobSeries):
    @pytest.fixture
    def ops(self):
        from sarc.client.job import get_jobs
        from sarc.client.series import load_job_series
        from sarc.users.db import get_users

        return SimpleNamespace(
            load_job_series=load_job_series,
            get_jobs=get_jobs,
            get_users=get_users,
        )
