from datetime import UTC, datetime
from pathlib import Path

import pytest
from fabric.testing.base import Command

import sarc.storage.beegfs
from sarc.config import config
from sarc.core.models.users import Credentials, UserData
from sarc.core.models.validators import END_TIME, START_TIME, ValidTag
from sarc.core.scraping.diskusage import get_diskusage_scraper


def mock_get_users():
    return [
        UserData(
            display_name="user1",
            email="@",
            associated_accounts={
                "mila": Credentials(
                    values=[
                        ValidTag(
                            value="user1", valid_start=START_TIME, valid_end=END_TIME
                        )
                    ]
                )
            },
            matching_ids={},
        ),
        UserData(
            display_name="user2",
            email="@",
            associated_accounts={
                "mila": Credentials(
                    values=[
                        ValidTag(
                            value="user2", valid_start=START_TIME, valid_end=END_TIME
                        )
                    ]
                )
            },
            matching_ids={},
        ),
        UserData(
            display_name="user3",
            email="@",
            associated_accounts={
                "mila": Credentials(
                    values=[
                        ValidTag(
                            value="user3", valid_start=START_TIME, valid_end=END_TIME
                        )
                    ]
                )
            },
            matching_ids={},
        ),
        UserData(
            display_name="user4",
            email="@",
            associated_accounts={
                "mila": Credentials(
                    values=[
                        ValidTag(
                            value="none",
                            valid_start=START_TIME,
                            valid_end=datetime(2023, 6, 30, tzinfo=UTC),
                        )
                    ]
                )
            },
            matching_ids={},
        ),
    ]


@pytest.mark.freeze_time("2023-07-25")
def test_beegfs_fetch_report(monkeypatch, remote, file_regression):
    cluster = config("scraping").clusters["mila"]
    diskusages = cluster.diskusage
    assert diskusages is not None
    assert len(diskusages) == 1
    diskusage = diskusages[0]
    assert diskusage.name == "beegfs"
    scraper = get_diskusage_scraper(diskusage.name)
    dconfig = scraper.validate_config(diskusage.params)
    with open(
        Path(__file__).parent / "mila_reports/report_user1.txt",
        "rb",
    ) as f:
        report1 = f.read()
    with open(
        Path(__file__).parent / "mila_reports/report_user2.txt",
        "rb",
    ) as f:
        report2 = f.read()
    with open(
        Path(__file__).parent / "mila_reports/report_user3.txt",
        "rb",
    ) as f:
        report3 = f.read()

    assert len(dconfig.config_files) == 1
    cfile = next(iter(dconfig.config_files.values()))

    remote.expect(
        host=cluster.host,
        commands=[
            Command(
                cmd=f"{dconfig.beegfs_ctl_path} --cfgFile={cfile} --getquota --uid user1 --csv",
                out=report1,
            ),
            Command(
                cmd=f"{dconfig.beegfs_ctl_path} --cfgFile={cfile} --getquota --uid user2 --csv",
                out=report2,
            ),
            Command(
                cmd=f"{dconfig.beegfs_ctl_path} --cfgFile={cfile} --getquota --uid user3 --csv",
                out=report3,
            ),
        ],
    )
    monkeypatch.setattr(sarc.storage.beegfs, "get_users", mock_get_users)

    report = scraper.get_diskusage_report(cluster.ssh, "mila", dconfig)
    file_regression.check(report.decode())


@pytest.mark.freeze_time("2023-07-25")
def test_beegfs_parse_report(file_regression):
    with open(Path(__file__).parent / "mila_reports/report_all.txt", "rb") as f:
        raw_report = f.read()

    scraper = get_diskusage_scraper("beegfs")
    result = scraper.parse_diskusage_report(raw_report)

    file_regression.check(result.model_dump_json(exclude={"id"}, indent=4))
