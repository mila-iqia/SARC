from datetime import datetime

import pytest

from sarc.config import MTL, config
from sarc.jobs.sacct import SAcctScraper

from .factory import JsonJobFactory, create_jobs, json_raw


@pytest.fixture
def json_job(request):
    return JsonJobFactory().create_job(**request.param)


@pytest.fixture
def json_jobs():
    job_factory = JsonJobFactory()
    create_jobs(job_factory)
    job_factory.add_job(group=None)
    return job_factory.jobs


@pytest.fixture
def sacct_json(json_jobs):
    tmp_json_raw = copy.deepcopy(json_raw)
    tmp_json_raw["jobs"] = json_jobs
    return json.dumps(tmp_json_raw)


parameters = {
    "user": {"user": "longbonhomme"},
    "job_state": {"state": {"current": "OUT_OF_MEMORY", "reason": "None"}},
    "signal": {
        "exit_code": {
            "status": "SIGNALED",
            "return_code": None,
            "signal": {"signal_id": 9, "name": "Killed"},
        }
    },
    "exit_code": {"exit_code": {"return_code": 1, "status": "FAILED"}},
    "time_limit": {"time": {"limit": 12345}},
    "submit_time": {
        "time": {"submit": int(datetime(2023, 2, 14).timestamp().timestamp())}
    },
    "dont_trust_start_time": {"time": {"start": 1}},
    "end_time": {"time": {"end": int(datetime(2023, 2, 15).timestamp().timestamp())}},
    "no_end_time": {"time": {"end": None}},
    "nodes": {"nodes": "node1,node[3-5]"},
    "flags": {
        "flags": ["CLEAR_SCHEDULING", "STARTED_ON_SUBMIT"],
    },
    "tres": {
        "tres": {
            "allocated": [
                {"count": 4, "id": 1, "name": None, "type": "cpu"},
                {"count": 16384, "id": 2, "name": None, "type": "mem"},
                {"count": 1, "id": 4, "name": None, "type": "node"},
                {"count": 1, "id": 5, "name": None, "type": "billing"},
                {"count": 1, "id": 1001, "name": "gpu", "type": "gres"},
                {"count": 1, "id": 1002, "name": "gpu:p100", "type": "gres"},
            ],
            "requested": [
                {"count": 4, "id": 1, "name": None, "type": "cpu"},
                {"count": 16384, "id": 2, "name": None, "type": "mem"},
                {"count": 1, "id": 4, "name": None, "type": "node"},
                {"count": 1, "id": 5, "name": None, "type": "billing"},
                {"count": 1, "id": 1001, "name": "gpu", "type": "gres"},
                {"count": 1, "id": 1002, "name": "gpu:p100", "type": "gres"},
            ],
        }
    },
    "array": {
        "array": {
            "job_id": 29036715,
            "limits": {"max": {"running": {"tasks": 0}}},
            "task": None,
            "task_id": 10,
        },
        "job_id": 29036725,
    },
}


@pytest.fixture
def scraper():
    return SAcctScraper(cluster=config().clusters["raisin"], day=datetime(2023, 2, 14))


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_job", parameters.values(), ids=parameters.keys(), indirect=True
)
def test_parse_json_job(json_job, scraper, file_regression):
    file_regression.check(scraper.convert(json_job).json(indent=4))


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_job",
    [{}],
    indirect=True,
)
def test_parse_malformed_jobs(json_job, scraper, capsys, file_regression):
    json_job["tres"]["allocated"].append(
        {"requested": {"quossé ça fait icitte ça?": "ché pas"}}
    )
    scraper.results = {"jobs": [json_job]}
    assert list(scraper) == []
    file_regression.check(capsys.readouterr().err)


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_job",
    [{"group": None}],
    indirect=True,
)
def test_parse_no_group_jobs(json_job, scraper, caplog, file_regression):
    scraper.results = {"jobs": [json_job]}
    with caplog.at_level("DEBUG"):
        assert list(scraper) == []
    file_regression.check(caplog.text)


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_job",
    [{"cluster": "patate"}],
    indirect=True,
)
def test_scrape_lost_job_on_wrong_cluster(json_job, scraper, caplog, file_regression):
    scraper.results = {"jobs": [json_job]}
    with caplog.at_level("WARNING"):
        jobs = list(scraper)

    assert len(jobs) == 1
    assert scraper.cluster.name == "raisin"
    assert jobs[0].cluster_name == "raisin"
    file_regression.check(caplog.text)


@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_scraper_with_cache(scraper, sacct_json, db_jobs):
    assert str(scraper.cachefile).startswith("/tmp/pytest")

    with open(scraper.cachefile, "w") as f:
        f.write(sacct_json)

    skipped = 0
    for i, (scraped_job, db_job) in enumerate(zip(scraper, db_jobs)):
        db_job = SlurmJob(**db_job)
        if db_job.cluster_name != scraper.cluster.name:
            skipped += 1
            continue
        assert scraped_job.dict() == db_job.dict()

    assert skipped == 2


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "patate"}}}], indirect=True
)
def test_scraper_with_malformed_cache(
    test_config, remote, scraper, caplog, file_regression
):
    assert str(scraper.cachefile).startswith("/tmp/pytest")

    with open(scraper.cachefile, "w") as f:
        f.write("I am malformed!! :'(")

    channel = remote.expect(
        host="patate",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-14T00:00' -E '2023-02-15T00:00' --json",
        out=b"{}",
    )

    with caplog.at_level("WARNING"):
        assert len(scraper.get_raw()) == 0

    file_regression.check(caplog.text)


# Test updating the DB

{"cluster_tz": {"cluster": "patate"}}  # Patate is in Vancouver!
