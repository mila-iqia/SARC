import os

import pytest

from sarc.cli import main
from sarc.storage.diskusage import get_diskusage_collection, get_diskusages
from sarc.storage.drac import convert_parsed_report_to_diskusage, parse_diskusage_report

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_one(file_regression):
    assert get_diskusages(cluster_name=["narval", "beluga"]) == []

    main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_narval.txt"),
            "-c",
            "narval",
        ]
    )

    data = get_diskusages(cluster_name=["narval", "beluga"])
    assert len(data) == 1
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_two(file_regression):
    assert get_diskusages(cluster_name=["narval", "beluga"]) == []
    main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_narval.txt"),
            "-c",
            "narval",
        ]
    )
    main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_beluga.txt"),
            "-c",
            "beluga",
        ]
    )
    data = get_diskusages(cluster_name=["narval", "beluga"])
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
def test_update_drac_diskusage_no_duplicate(file_regression):
    assert get_diskusages(cluster_name=["narval", "beluga"]) == []

    main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_narval.txt"),
            "-c",
            "narval",
        ]
    )
    main(
        [
            "acquire",
            "storages",
            "--file",
            os.path.join(FOLDER, "drac_reports/report_narval.txt"),
            "-c",
            "narval",
        ]
    )

    data = get_diskusages(cluster_name=["narval", "beluga"])
    assert len(data) == 1
    file_regression.check(data[0].json(exclude={"id": True}, indent=4))
