import os
from pathlib import Path

from sarc.storage.drac import _parse_body as parse_body
from sarc.storage.drac import _parse_header_summary as parse_header_summary


# cedar, beluga and graham style
def test_header_00(file_regression):
    f = open(Path(__file__).parent / "drac_reports/report_hyrule.txt", "r")
    report = f.readlines()
    f.close()
    file_regression.check("\n".join(map(str, parse_header_summary(report))))


# narval style
def test_header_01(file_regression):
    f = open(Path(__file__).parent / "drac_reports/report_gerudo.txt", "r")
    report = f.readlines()
    f.close()
    file_regression.check("\n".join(map(str, parse_header_summary(report))))


def test_parse_body(file_regression):
    f = open(Path(__file__).parent / "drac_reports/report_hyrule.txt", "r")
    report = f.readlines()
    f.close()
    file_regression.check(str(parse_body(report)))
