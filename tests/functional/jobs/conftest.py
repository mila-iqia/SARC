from __future__ import annotations

import copy
import json

import pytest

from .factory import json_raw, create_json_jobs


@pytest.fixture
def json_jobs(request):
    if isinstance(request.param, dict):
        request.param = [request.param]

    return create_json_jobs(request.param)


@pytest.fixture
def sacct_json(json_jobs):
    tmp_json_raw = copy.deepcopy(json_raw)
    tmp_json_raw["jobs"] = json_jobs
    return json.dumps(tmp_json_raw)
