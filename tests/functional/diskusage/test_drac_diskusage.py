import json
from pathlib import Path

import pytest
from sqlmodel import select

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.diskusage import DiskUsageDB
from sarc.scraping.diskusage import get_diskusage_scraper


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.time_machine("2023-07-25T00:00+00:00", tick=False)
def test_drac_fetch_report(remote, file_regression):
    cluster = config.clusters["gerudo"]
    diskusages = cluster.diskusage
    assert diskusages is not None
    assert len(diskusages) == 1
    diskusage = diskusages[0]
    assert diskusage.name == "drac"
    scraper = get_diskusage_scraper(diskusage.name)
    dconfig = scraper.validate_config(diskusage.params)
    raw_report = None
    with open(Path(__file__).parent / "drac_reports/report_gerudo.txt", "rb") as f:
        raw_report = f.read()
    assert raw_report

    remote.expect(
        host=cluster.host,
        cmd=f"{dconfig.diskusage_path} --project --all_users",
        out=raw_report,
    )

    report = scraper.get_diskusage_report(cluster.ssh, "gerudo", dconfig)
    file_regression.check(report.decode())


# Test DRAC scraper parsing functionality through the proper interface
def test_drac_parse_report(file_regression):
    with open(
        Path(__file__).parent / "drac_reports/cached_report_hyrule.txt", "rb"
    ) as f:
        raw_report = f.read()

    scraper = get_diskusage_scraper("drac")
    result = scraper.parse_diskusage_report(raw_report)

    file_regression.check(result.model_dump_json(exclude={"timestamp", "id"}, indent=2))


@pytest.mark.usefixtures("enabled_cache", "no_pkey")
@pytest.mark.time_machine("2023-05-12T00:00+00:00", tick=False)
def test_drac_acquire_storages(remote, cli_main, file_regression, empty_read_write_db):
    cluster = config.clusters["hyrule"]
    diskusages = cluster.diskusage
    assert diskusages is not None
    assert len(diskusages) == 1
    diskusage = diskusages[0]
    assert diskusage.name == "drac"
    scraper = get_diskusage_scraper(diskusage.name)
    dconfig = scraper.validate_config(diskusage.params)
    raw_report = None
    with open(Path(__file__).parent / "drac_reports/report_hyrule.txt", "rb") as f:
        raw_report = f.read()
    assert raw_report

    remote.expect(
        host=cluster.host,
        cmd=f"{dconfig.diskusage_path} --project --all_users",
        out=raw_report,
    )

    cli_main(["fetch", "diskusage", "-c", "hyrule"])
    cli_main(["parse", "diskusage", "--from", "2023-05-11"])

    data = empty_read_write_db.exec(
        select(DiskUsageDB)
        .join(SlurmClusterDB, SlurmClusterDB.id == DiskUsageDB.cluster_id)
        .where(SlurmClusterDB.name == "hyrule")
    ).all()
    assert len(data) == 1
    file_regression.check(
        json.dumps(
            data[0].model_dump(mode="json", exclude={"id"}), indent=4, sort_keys=True
        )
    )
