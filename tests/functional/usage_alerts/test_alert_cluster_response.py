import re
from datetime import timedelta

import pytest

from sarc.alerts.usage_alerts.cluster_response import check_cluster_response
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

PARAMETERS = {
    "default": dict(),  # default is 7 days
    **{
        f"{days}-days": dict(time_interval=timedelta(days=days))
        for days in [365, 283, 282, 281, 280, 279]
    },
}


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db_with_users", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", PARAMETERS.values(), ids=PARAMETERS.keys())
def test_check_cluster_response(params, caplog, file_regression):
    check_cluster_response(**params)
    file_regression.check(
        re.sub(
            r"ERROR +sarc\.alerts\.usage_alerts\.cluster_response:cluster_response.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
