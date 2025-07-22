from pathlib import Path

import pytest

import sarc.storage  # Import storage module to register scrapers
from sarc.config import config
from sarc.core.scraping.diskusage import get_diskusage_scraper
from sarc.storage.diskusage import get_diskusages


def test_drac_fetch_report(remote, file_regression):
    cluster = config("scraping").clusters["gerudo"]
    diskusages = cluster.diskusage
    assert diskusages is not None
    assert len(diskusages) == 1
    diskusage = diskusages[0]
    assert diskusage.name == "drac"
    scraper = get_diskusage_scraper(diskusage.name)
    dconfig = scraper.validate_config(diskusage.params)
    raw_report = None
    with open(
        Path(__file__).parent / "drac_reports/report_gerudo.txt",
        "r",
        encoding="utf-8",
    ) as f:
        raw_report = f.read()
    assert raw_report

    remote.expect(
        host=cluster.host,
        cmd=f"{dconfig.diskusage_path} --project --all_users",
        out=str.encode(raw_report),
    )

    report = scraper.get_diskusage_report(cluster.ssh, dconfig)
    file_regression.check(report)


# Test DRAC scraper parsing functionality through the proper interface
def test_drac_parse_report(file_regression):
    with open(Path(__file__).parent / "drac_reports/report_hyrule.txt", "r") as f:
        raw_report = f.read()

    scraper = get_diskusage_scraper("drac")
    config = scraper.validate_config({})  # Use default config
    result = scraper.parse_diskusage_report(config, "hyrule", raw_report)

    file_regression.check(result.model_dump_json(exclude={"timestamp", "id"}, indent=2))


@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.freeze_time("2023-05-12")
def test_drac_acquire_storages(remote, cli_main, file_regression):
    cluster = config("scraping").clusters["hyrule"]
    diskusages = cluster.diskusage
    assert diskusages is not None
    assert len(diskusages) == 1
    diskusage = diskusages[0]
    assert diskusage.name == "drac"
    scraper = get_diskusage_scraper(diskusage.name)
    dconfig = scraper.validate_config(diskusage.params)
    raw_report = None
    with open(
        Path(__file__).parent / "drac_reports/report_hyrule.txt",
        "r",
        encoding="utf-8",
    ) as f:
        raw_report = f.read()
    assert raw_report

    remote.expect(
        host=cluster.host,
        cmd=f"{dconfig.diskusage_path} --project --all_users",
        out=str.encode(raw_report),
    )

    cli_main(
        [
            "acquire",
            "storages",
            "-c",
            "hyrule",
        ]
    )

    data = get_diskusages(cluster_name=["hyrule"])
    assert len(data) == 1
    file_regression.check(data[0].model_dump_json(exclude={"id": True}, indent=4))
