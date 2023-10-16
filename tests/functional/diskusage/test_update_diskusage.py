import os

import pytest

import sarc.storage.drac
from sarc.storage.diskusage import get_diskusages

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def mock_drac_fetch(monkeypatch):
    def mock_fetch(cluster, *args):
        path = os.path.join(FOLDER, f"drac_reports/report_{cluster.name}.txt")
        with open(path, "r") as f:
            return f.readlines()

    monkeypatch.setattr(sarc.storage.drac, "_fetch_diskusage_report", mock_fetch)


@pytest.mark.usefixtures("mock_drac_fetch")
@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_one(file_regression, cli_main):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "gerudo",
        ]
    )

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 1
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))


@pytest.mark.usefixtures("mock_drac_fetch")
@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_two(file_regression, cli_main):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []
    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "gerudo",
        ]
    )
    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "hyrule",
        ]
    )
    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 2
    data_json = "\n".join(
        [
            data[0].json(exclude={"id": True}, indent=4),
            data[1].json(exclude={"id": True}, indent=4),
        ]
    )
    file_regression.check(data_json)


@pytest.mark.usefixtures("mock_drac_fetch")
@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_no_duplicate(file_regression, cli_main):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "gerudo",
        ]
    )
    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "gerudo",
        ]
    )

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 1
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))
