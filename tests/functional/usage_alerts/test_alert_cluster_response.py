import re

import pytest
import time_machine

from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

PARAMETERS = {
    "default": "cluster_response_default",  # default is 7 days
    **{
        f"{days}-days": f"cluster_response_{days}_days"
        for days in [365, 283, 282, 281, 280, 279]
    },
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db_with_users", "health_config")
@pytest.mark.parametrize("check_name", PARAMETERS.values(), ids=PARAMETERS.keys())
def test_check_cluster_response(caplog, file_regression, cli_main, check_name):
    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(
        re.sub(
            r"ERROR +sarc\.alerts\.usage_alerts\.cluster_response:cluster_response.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
