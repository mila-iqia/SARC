import os
from pathlib import Path

import pytest
from fabric.testing.base import Command
from sqlmodel import Session, select

from sarc.cli import main
from sarc.db.cluster import SlurmClusterDB
from sarc.db.diskusage import DiskUsageDB
from sarc.scraping.diskusage import DiskUsage

FOLDER = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.usefixtures("enabled_cache", "no_pkey")
@pytest.mark.time_machine("2023-05-12T00:00+00:00", tick=False)
def test_update_drac_diskusage_one(
    file_regression, remote, empty_read_write_db: Session
):
    assert (
        len(
            empty_read_write_db.exec(
                select(DiskUsageDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["gerudo", "hyrule"]))
            ).all()
        )
        == 0
    )

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
    main(["fetch", "diskusage", "-c", "gerudo"])
    main(["parse", "diskusage", "--from", "2023-05-11"])

    data = empty_read_write_db.exec(
        select(DiskUsageDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["gerudo", "hyrule"]))
    ).all()
    assert len(data) == 1
    file_regression.check(data[0].model_dump_json(exclude={"id"}, indent=4))


@pytest.mark.usefixtures("enabled_cache", "no_pkey")
@pytest.mark.time_machine("2023-05-12T00:00+00:00")
def test_update_drac_diskusage_two(file_regression, remote, empty_read_write_db):
    assert (
        len(
            empty_read_write_db.exec(
                select(DiskUsageDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["gerudo", "hyrule"]))
            ).all()
        )
        == 0
    )

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

    main(["fetch", "diskusage", "-c", "gerudo"])
    main(["fetch", "diskusage", "-c", "hyrule"])
    main(["parse", "diskusage", "--from", "2023-05-11"])
    data = empty_read_write_db.exec(
        select(DiskUsageDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["gerudo", "hyrule"]))
    ).all()
    assert len(data) == 2
    data_json = "\n".join(
        [
            data[0].model_dump_json(exclude={"id": True}, indent=4),
            data[1].model_dump_json(exclude={"id": True}, indent=4),
        ]
    )
    file_regression.check(data_json)


@pytest.mark.usefixtures("enabled_cache", "no_pkey")
@pytest.mark.time_machine("2023-05-12T00:00+00:00")
def test_update_drac_diskusage_no_duplicate(
    file_regression, remote, empty_read_write_db
):
    assert (
        len(
            empty_read_write_db.exec(
                select(DiskUsageDB)
                .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
                .where(SlurmClusterDB.name.in_(["gerudo", "hyrule"]))
            ).all()
        )
        == 0
    )

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

    main(["fetch", "diskusage", "-c", "gerudo"])
    main(["fetch", "diskusage", "-c", "gerudo"])
    main(["parse", "diskusage", "--from", "2023-05-11"])

    data = empty_read_write_db.exec(
        select(DiskUsageDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
        .where(SlurmClusterDB.name.in_(["gerudo", "hyrule"]))
    ).all()
    assert len(data) == 1
    file_regression.check(data[0].model_dump_json(exclude={"id": True}, indent=4))
