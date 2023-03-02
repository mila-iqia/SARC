import os

from sarc.storage.drac import _parse_body as parse_body
from sarc.storage.drac import _parse_header_summary as parse_header_summary

FOLDER = os.path.dirname(os.path.abspath(__file__))


# cedar, beluga and graham style
def test_header_00():
    f = open(os.path.join(FOLDER, "drac_reports/report_gerudo.txt"), "r")
    report = f.readlines()
    f.close()
    L_results = parse_header_summary(report)
    L_results_expected = [
        {"group": "def-bengioy", "space": "971G/1000G", "nbr_files": "791k/1005k"},
        {"group": "rpp-bengioy", "space": "31T/2048k", "nbr_files": "3626k/1025"},
    ]

    assert len(L_results) == len(L_results_expected)

    for a, b in zip(L_results, L_results_expected):
        assert a == b


# narval style
def test_header_01():
    f = open(os.path.join(FOLDER, "drac_reports/report_hyrule.txt"), "r")
    report = f.readlines()
    f.close()
    L_results = parse_header_summary(report)
    L_results_expected = [
        {"group": "rrg-bengioy-ad", "space": "39T/75T", "nbr_files": "1316k/5000k"},
        {"group": "def-bengioy", "space": "956G/1000G", "nbr_files": "226k/500k"},
    ]

    assert len(L_results) == len(L_results_expected)

    for a, b in zip(L_results, L_results_expected):
        assert a == b


def test_parse_body():
    f = open(os.path.join(FOLDER, "drac_reports/report_gerudo.txt"), "r")
    report = f.readlines()
    f.close()

    DLD_results = parse_body(report)
    assert set(list(DLD_results.keys())) == set(["def-bengioy", "rpp-bengioy"])

    assert DLD_results["def-bengioy"] == [
        {"username": "revali", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "urbosa", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "daruk", "nbr_files": 50, "size": (13.49, "GiB")},
        {"username": "mipha", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "Total", "nbr_files": 696928, "size": (877.51, "GiB")},
    ]

    assert DLD_results["rpp-bengioy"] == [
        {"username": "riju", "nbr_files": 47085, "size": (4.20, "GiB")},
        {"username": "grosaillieh", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "bourgette", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "kohga", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "rhoam", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "Total", "nbr_files": 3626455, "size": (30009.08, "GiB")},
    ]
