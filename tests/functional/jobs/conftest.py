import copy
import json
from pathlib import Path

import pytest
from serieux.features.partial import merge as _merge
from sqlmodel import select

from sarc.db.job import SlurmJobDB


@_merge.variant(priority=10)
def merge(xs: list, ys: list):
    return ys


here = Path(__file__).parent


datadir = here.parent.parent / "data"


@pytest.fixture(scope="session")
def base_jobs():
    return json.loads((datadir / "sacct" / "raisin_1714003200.json").read_text())


@pytest.fixture(scope="session")
def base_job(base_jobs):
    return base_jobs["jobs"][0]


@pytest.fixture
def create_sacct_json(base_jobs, base_job):
    def generate(mods):
        tmp_json_raw = copy.deepcopy(base_jobs)
        tmp_json_raw["jobs"] = [merge(base_job, mod) for mod in mods]
        return json.dumps(tmp_json_raw)

    return generate


@pytest.fixture
def patched_job(base_job, request):
    assert isinstance(request.param, dict)
    return merge(base_job, request.param)


@pytest.fixture
def json_jobs(base_job, request):
    if isinstance(request.param, dict):
        request.param = [request.param]

    return [merge(base_job, adj) for adj in request.param]


@pytest.fixture
def sacct_json(base_jobs, json_jobs):
    tmp_json_raw = copy.deepcopy(base_jobs)
    tmp_json_raw["jobs"] = json_jobs
    return json.dumps(tmp_json_raw)


@pytest.fixture
def get_jobs(jobless_read_write_db):
    def get():
        q = select(SlurmJobDB)
        return jobless_read_write_db.exec(q).all()

    return get


@pytest.fixture
def slurm_version(base_jobs):
    return base_jobs["meta"]["slurm"]["version"]
