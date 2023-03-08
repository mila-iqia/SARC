import os

import pytest

from sarc.config import config
from sarc.storage.diskusage import get_diskusage_collection, get_diskusages
from sarc.storage.drac import convert_parsed_report_to_diskusage, parse_diskusage_report

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_one(file_regression, cli_main):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    cli_main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_gerudo.txt"),
            "-c",
            "gerudo",
        ]
    )

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 1
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_two(file_regression, cli_main):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []
    cli_main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_gerudo.txt"),
            "-c",
            "gerudo",
        ]
    )
    cli_main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_hyrule.txt"),
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


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_no_duplicate(file_regression, cli_main):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    cli_main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_gerudo.txt"),
            "-c",
            "gerudo",
        ]
    )
    cli_main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_gerudo.txt"),
            "-c",
            "gerudo",
        ]
    )

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 1
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))
