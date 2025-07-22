from pathlib import Path

import pytest
from fabric.testing.base import Command

import sarc.storage.beegfs
from sarc.client.users.api import Credentials, User
from sarc.config import config
from sarc.core.scraping.diskusage import get_diskusage_scraper
from sarc.storage.diskusage import get_diskusages


def mock_get_users():
    return [
        User(
            name="user1",
            mila=Credentials(username="user1", email="@", active=True),
            drac=None,
            mila_ldap=dict(),
            drac_members=None,
            drac_roles=None,
        ),
        User(
            name="user2",
            mila=Credentials(username="user2", email="@", active=True),
            drac=None,
            mila_ldap=dict(),
            drac_members=None,
            drac_roles=None,
        ),
        User(
            name="user3",
            mila=Credentials(username="user3", email="@", active=True),
            drac=None,
            mila_ldap=dict(),
            drac_members=None,
            drac_roles=None,
        ),
        User(
            name="user4",
            mila=Credentials(username="none", email="@", active=False),
            drac=None,
            mila_ldap=dict(),
            drac_members=None,
            drac_roles=None,
        ),
    ]


@pytest.mark.freeze_time("2023-07-25")
def test_beegfs_fetch_report(monkeypatch, remote, cli_main, file_regression):
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
        "r",
        encoding="utf-8",
    ) as f:
        report1 = f.read()
    with open(
        Path(__file__).parent / "mila_reports/report_user2.txt",
        "r",
        encoding="utf-8",
    ) as f:
        report2 = f.read()
    with open(
        Path(__file__).parent / "mila_reports/report_user3.txt",
        "r",
        encoding="utf-8",
    ) as f:
        report3 = f.read()

    assert len(dconfig.config_files) == 1
    cfile = next(iter(dconfig.config_files.values()))

    remote.expect(
        host=cluster.host,
        commands=[
            Command(
                cmd=f"{dconfig.beegfs_ctl_path} --cfgFile={cfile} --getquota --uid user1 --csv",
                out=str.encode(report1),
            ),
            Command(
                cmd=f"{dconfig.beegfs_ctl_path} --cfgFile={cfile} --getquota --uid user2 --csv",
                out=str.encode(report2),
            ),
            Command(
                cmd=f"{dconfig.beegfs_ctl_path} --cfgFile={cfile} --getquota --uid user3 --csv",
                out=str.encode(report3),
            ),
        ],
    )
    monkeypatch.setattr(sarc.storage.beegfs, "get_users", mock_get_users)

    report = scraper.get_diskusage_report(cluster.ssh, dconfig)
    file_regression.check(report)


@pytest.mark.freeze_time("2023-07-25")
def test_beegfs_parse_report(file_regression):
    with open(Path(__file__).parent / "mila_reports/report_all.txt", "r") as f:
        raw_report = f.read()

    scraper = get_diskusage_scraper("beegfs")
    config = scraper.validate_config(
        {"config_files": {"default": "/etc/beegfs/scratch.d/beegfs-client.conf"}}
    )
    result = scraper.parse_diskusage_report(config, "mila", raw_report)

    file_regression.check(result.model_dump_json(exclude={"id"}, indent=4))
