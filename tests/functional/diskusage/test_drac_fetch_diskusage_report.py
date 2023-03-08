"""
tests the scrapping of disk usage report on DRAC clusters
"""
from pathlib import Path

import pytest
from fabric.testing.base import Command, MockRemote, Session

from sarc.cli import main
from sarc.storage.diskusage import get_diskusages
from sarc.storage.drac import fetch_diskusage_report


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"hyrule": {"host": "hyrule"}}}], indirect=True
)
def test_drac_fetch_diskusage_report(test_config, remote, file_regression):
    cluster = test_config.clusters["hyrule"]
    raw_report = None
    with open(
        Path(__file__).parent / "drac_reports/report_hyrule.txt", "r", encoding="utf-8"
    ) as f:
        raw_report = f.read()
    assert raw_report
    channel = remote.expect(
        host=cluster.host,
        cmd=cluster.diskusage_report_command,
        out=str.encode(raw_report),
    )

    report = fetch_diskusage_report(cluster=test_config.clusters["hyrule"])
    file_regression.check("\n".join(report))


# @pytest.mark.parametrize(
#     "test_config", [{"clusters": {"narval": {"host": "narval.computecanada.ca"}}}], indirect=True
# )
# @pytest.mark.usefixtures("empty_read_write_db")
# @pytest.mark.freeze_time("2023-05-12")
# def test_drac_acquire_storages(test_config, remote, cli_main, file_regression):
#     cluster = test_config.clusters["narval"]
#     raw_report = None
#     with open(
#         Path(__file__).parent / "drac_reports/report_narval.txt", "r", encoding="utf-8"
#     ) as f:
#         raw_report = f.read()
#     assert raw_report
#     channel = remote.expect(
#         host=cluster.host,
#         cmd="diskusage_report --project --all_users",
#         out=str.encode(raw_report),
#     )

#     cli_main(
#         [
#             "acquire",
#             "storages",
#             "-c",
#             "narval",
#         ]
#     )
#     data = get_diskusages(cluster_name=["test"])
#     assert len(data) == 1
#     file_regression.check(data[0].json(exclude={"id": True}, indent=4))
