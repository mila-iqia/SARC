import os
from pathlib import Path

import pytest
from fabric.testing.base import Command, Session

from sarc.storage.diskusage import get_diskusages

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_one(file_regression, cli_main, remote):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    # Load the expected report content
    report_path = Path(FOLDER) / "drac_reports/report_gerudo.txt"
    with open(report_path, "r", encoding="utf-8") as f:
        raw_report = f.read()

    # Mock the SSH command using remote fixture
    remote.expect(
        host="gerudo",
        cmd="diskusage_report --project --all_users",
        out=str.encode(raw_report),
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
    file_regression.check(data[0].model_dump_json(exclude={"id": True}, indent=4))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_two(file_regression, cli_main, remote):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    # Load both report contents
    gerudo_report_path = Path(FOLDER) / "drac_reports/report_gerudo.txt"
    hyrule_report_path = Path(FOLDER) / "drac_reports/report_hyrule.txt"

    with open(gerudo_report_path, "r", encoding="utf-8") as f:
        gerudo_report = f.read()
    with open(hyrule_report_path, "r", encoding="utf-8") as f:
        hyrule_report = f.read()

    # Mock both SSH commands
    remote.expect_sessions(
        Session(
            host="gerudo",
            cmd="diskusage_report --project --all_users",
            out=str.encode(gerudo_report),
        ),
        Session(
            host="hyrule",
            cmd="diskusage_report --project --all_users",
            out=str.encode(hyrule_report),
        ),
    )

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
            data[0].model_dump_json(exclude={"id": True}, indent=4),
            data[1].model_dump_json(exclude={"id": True}, indent=4),
        ]
    )
    file_regression.check(data_json)


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_update_drac_diskusage_no_duplicate(file_regression, cli_main, remote):
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    # Load the expected report content
    report_path = Path(FOLDER) / "drac_reports/report_gerudo.txt"
    with open(report_path, "r", encoding="utf-8") as f:
        raw_report = f.read()

    # Mock both SSH commands
    remote.expect(
        host="gerudo",
        commands=[
            Command(
                cmd="diskusage_report --project --all_users", out=str.encode(raw_report)
            ),
            Command(
                cmd="diskusage_report --project --all_users", out=str.encode(raw_report)
            ),
        ],
    )

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
    file_regression.check(data[0].model_dump_json(exclude={"id": True}, indent=4))
