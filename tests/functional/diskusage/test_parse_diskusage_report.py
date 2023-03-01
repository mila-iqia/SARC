from sarc.storage.drac import _parse_body as parse_body
from sarc.storage.drac import (
    _parse_header_summary as parse_header_summary,
)


def test_header_00():
    L_lines = """
                                Description                Space           # of files
                /project (group kjsfsd78)              0/2048k               0/1025
                /project (group def-bengioy)           971G/1000G           791k/1005k
                /project (group rpp-bengioy)            31T/2048k           3626k/1025
            /project (group rrg-bengioy-ad)              54T/75T          1837k/5005k
        """.split(
        "\n"
    )

    L_results = parse_header_summary(L_lines)
    L_results_expected = [
        {"group": "kjsfsd78", "space": "0/2048k", "nbr_files": "0/1025"},
        {"group": "def-bengioy", "space": "971G/1000G", "nbr_files": "791k/1005k"},
        {"group": "rpp-bengioy", "space": "31T/2048k", "nbr_files": "3626k/1025"},
        {"group": "rrg-bengioy-ad", "space": "54T/75T", "nbr_files": "1837k/5005k"},
    ]

    assert len(L_results) == len(L_results_expected)

    for a, b in zip(L_results, L_results_expected):
        assert a == b


def test_parse_body_00():
    L_lines = """
    
    Breakdown for project def-bengioy (Last update: 2022-10-25 14:01:28)
            User      File count                 Size             Location
    -------------------------------------------------------------------------
       k0000000               2             0.00 GiB              On disk
       k1111111               2             0.00 GiB              On disk
         k22222              50            13.49 GiB              On disk
         k33333               2             0.00 GiB              On disk
          Total          696928           877.51 GiB              On disk    

Breakdown for project rpp-bengioy (Last update: 2022-10-25 13:09:53)
           User      File count                 Size             Location
-------------------------------------------------------------------------
         aa0000           47085             4.20 GiB              On disk
       ab111111               2             0.00 GiB              On disk
         ab2222               2             0.00 GiB              On disk
       a4444444               2             0.00 GiB              On disk
        a555555               2             0.00 GiB              On disk    
          Total         3626455         30009.08 GiB              On disk

    """.split(
        "\n"
    )

    DLD_results = parse_body(L_lines)
    assert set(list(DLD_results.keys())) == set(["def-bengioy", "rpp-bengioy"])

    assert DLD_results["def-bengioy"] == [
        {"username": "k0000000", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "k1111111", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "k22222", "nbr_files": 50, "size": (13.49, "GiB")},
        {"username": "k33333", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "Total", "nbr_files": 696928, "size": (877.51, "GiB")},
    ]

    assert DLD_results["rpp-bengioy"] == [
        {"username": "aa0000", "nbr_files": 47085, "size": (4.20, "GiB")},
        {"username": "ab111111", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "ab2222", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "a4444444", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "a555555", "nbr_files": 2, "size": (0.0, "GiB")},
        {"username": "Total", "nbr_files": 3626455, "size": (30009.08, "GiB")},
    ]
