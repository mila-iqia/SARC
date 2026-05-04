import re
from datetime import datetime, UTC

import pytest
import time_machine

MOCK_TIME = datetime(2023, 11, 22, tzinfo=UTC)

PARAMETERS = {
    "default": "cluster_response_default",  # default is 7 days
    **{
        f"{days}-days": f"cluster_response_{days}_days"
        for days in [365, 283, 282, 281, 280, 279]
    },
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db", "health_config")
@pytest.mark.parametrize("check_name", PARAMETERS.values(), ids=PARAMETERS.keys())
def test_check_cluster_response(caplog, file_regression, cli_main, check_name):
    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(re.sub(r"ERROR +.+\.py:[0-9]+ +", "", caplog.text))
