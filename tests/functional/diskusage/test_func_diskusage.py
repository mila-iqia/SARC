from datetime import date

import pytest

from sarc.storage.diskusage import get_diskusages

parameters = {
    "name_only": {"cluster_name": "totk"},
    "start_only": {"cluster_name": "totk", "start": date(2022, 2, 1)},
    "end_only": {"cluster_name": "totk", "end": date(2022, 1, 1)},
    "start_and_end": {
        "cluster_name": "totk",
        "start": date(2018, 4, 1),
        "end": date(2023, 5, 2),
    },
    "name_list_start_and_end": {
        "cluster_name": ["totk", "botw"],
        "start": date(2018, 4, 1),
        "end": date(2022, 1, 1),
    },
    "another_name": {"cluster_name": "botw"},
}


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("params,", parameters.values(), ids=parameters.keys())
def test_get_diskusage(params, data_regression):
    data = get_diskusages(**params)

    data_regression.check(
        [allocation.model_dump(exclude={"id": True}) for allocation in data]
    )
