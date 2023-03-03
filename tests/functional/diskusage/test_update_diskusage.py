import os

import pytest

from sarc.storage.diskusage import get_diskusage_collection, get_diskusages
from sarc.storage.drac import convert_parsed_report_to_diskusage, parse_diskusage_report

FOLDER = os.path.dirname(os.path.abspath(__file__))


def import_file_to_db(clustername, filename):
    """
    utility function that inserts a diskusage text output of DRAC into mongo
    """
    f = open(os.path.join(FOLDER, filename), "r")
    report = f.readlines()
    f.close()
    header, body = parse_diskusage_report(report)
    collection = get_diskusage_collection()
    collection.add(convert_parsed_report_to_diskusage(clustername, body))


@pytest.mark.usefixtures("empty_read_write_db")
def test_update_drac_diskusage_one():
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    import_file_to_db("gerudo", "drac_reports/report_gerudo.txt")

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 1


@pytest.mark.usefixtures("empty_read_write_db")
def test_update_drac_diskusage_two():
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    import_file_to_db("gerudo", "drac_reports/report_gerudo.txt")
    import_file_to_db("hyrule", "drac_reports/report_hyrule.txt")

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 2


@pytest.mark.usefixtures("empty_read_write_db")
def test_update_drac_diskusage_no_duplicate():
    assert get_diskusages(cluster_name=["gerudo", "hyrule"]) == []

    import_file_to_db("gerudo", "drac_reports/report_gerudo.txt")
    import_file_to_db("gerudo", "drac_reports/report_gerudo.txt")

    data = get_diskusages(cluster_name=["gerudo", "hyrule"])
    assert len(data) == 1
