from datetime import datetime

import pandas
import pytest

from sarc.config import MTL
from sarc.jobs.job import get_jobs
from sarc.jobs.series import load_job_series

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
    "allocated",
    "array_job_id",
    "cluster_name",
    "constraints",
    "cpu",
    "cpu_utilization",
    "duration",
    "elapsed_time",
    "end",
    "end_time",
    "exit_code",
    "gpu_allocated",
    "gpu_memory",
    "gpu_power",
    "gpu_requested",
    "gpu_utilization",
    "group",
    "id",
    "job_id",
    "job_state",
    "mem",
    "name",
    "nodes",
    "partition",
    "priority",
    "qos",
    "requested",
    "signal",
    "start",
    "start_time",
    "stored_statistics",
    "submit",
    "submit_time",
    "system_memory",
    "task_id",
    "time_limit",
    "user",
    "work_dir",
]


# For file regression tests, we will save data frame into a CSV.
# We won't include job id because it changes from a call to another.
# We won't include time-related fields because they can depend on start_time.
# which may be set by load_job_series() to current time (datetime.now()) if initially None.
CSV_COLUMNS = [
    col
    for col in ALL_COLUMNS
    if col
    not in ("id", "start_time", "end_time", "elapsed_time", "start", "end", "duration")
]


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", parameters.values(), ids=parameters.keys())
def test_get_jobs(params, file_regression):
    data_frame = load_job_series(**params)
    assert isinstance(data_frame, pandas.DataFrame)
    # Check columns
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


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", [parameters["no_cluster"]], ids=["no_cluster"])
def test_get_jobs_check_end_times(params):
    # Get jobs
    jobs = list(get_jobs(**params))
    # Get a data frame
    frame_1 = load_job_series(**params)
    # Get a data frame again
    frame_2 = load_job_series(**params)
    nb_no_end_times = 0
    for i, job in enumerate(jobs):
        if job.end_time is None:
            nb_no_end_times += 1
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
    # Check we really got raw jobs with no end time.
    assert nb_no_end_times


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", few_parameters.values(), ids=few_parameters.keys())
def test_get_jobs_fields_list(params, file_regression):
    fields = ["gpu_memory", "mem", "user", "work_dir"]
    data_frame = load_job_series(fields=fields, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(fields)
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", few_parameters.values(), ids=few_parameters.keys())
def test_get_jobs_fields_dict(params, file_regression):
    fields = {
        "gpu_memory": "gpu_footprint",
        "mem": "memory",
        "user": "username",
        "work_dir": "the_user_folder",
    }
    expected_fields = ["gpu_footprint", "memory", "username", "the_user_folder"]
    data_frame = load_job_series(fields=fields, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(expected_fields)
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", param_start_end.values(), ids=param_start_end.keys())
def test_get_jobs_clip_time_true(params, file_regression):
    assert "start" in params
    assert "end" in params
    data_frame = load_job_series(clip_time=True, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(
        ALL_COLUMNS + ["unclipped_start", "unclipped_end"]
    )
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", param_start_end.values(), ids=param_start_end.keys())
def test_get_jobs_clip_time_false(params, file_regression):
    assert "start" in params
    assert "end" in params
    data_frame = load_job_series(clip_time=False, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(ALL_COLUMNS)
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params", params_no_start_or_end.values(), ids=params_no_start_or_end.keys()
)
def test_get_jobs_clip_time_true_no_start_or_end(params, file_regression):
    with pytest.raises(ValueError, match="Clip time\: missing (start|end)"):
        load_job_series(clip_time=True, **params)


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", few_parameters.values(), ids=few_parameters.keys())
def test_get_jobs_callback(params, file_regression):
    def callback(rows):
        rows[-1]["another_column"] = 1234

    data_frame = load_job_series(callback=callback, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(
        ALL_COLUMNS + ["another_column"]
    )
    assert data_frame["another_column"].sum() == 1234 * data_frame.shape[0]
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n"
        f"\n{data_frame.to_csv(columns=CSV_COLUMNS + ['another_column'])}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", param_start_end.values(), ids=param_start_end.keys())
def test_get_jobs_all_args(params, file_regression):
    def callback(rows):
        rows[-1]["another_column"] = 1234

    fields = {
        "gpu_memory": "gpu_footprint",
        "mem": "memory",
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
    data_frame = load_job_series(
        fields=fields, clip_time=True, callback=callback, **params
    )
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(expected_fields)
    assert data_frame["another_column"].sum() == 1234 * data_frame.shape[0]
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=expected_fields)}"
    )
