from sarc.storage.drac import _parse_body as parse_body
from sarc.storage.drac import (
    _parse_header_summary as parse_header_summary,
)


test_output_drac_00 = """
                                Description                Space           # of files
                /project (group kjsfsd78)              0/2048k               0/1025
                /project (group def-bengioy)           971G/1000G           791k/1005k
                /project (group rpp-bengioy)            31T/2048k           3626k/1025
            /project (group rrg-bengioy-ad)              54T/75T          1837k/5005k
    
    Breakdown for project def-bengioy (Last update: 2022-10-25 14:01:28)
            User      File count                 Size             Location
    -------------------------------------------------------------------------
         revali               2             0.00 GiB              On disk
         urbosa               2             0.00 GiB              On disk
          daruk              50            13.49 GiB              On disk
          mipha               2             0.00 GiB              On disk
          Total          696928           877.51 GiB              On disk    

Breakdown for project rpp-bengioy (Last update: 2022-10-25 13:09:53)
           User      File count                 Size             Location
-------------------------------------------------------------------------
           riju           47085             4.20 GiB              On disk
    grosaillieh               2             0.00 GiB              On disk
      bourgette               2             0.00 GiB              On disk
          kohga               2             0.00 GiB              On disk
          rhoam               2             0.00 GiB              On disk    
          Total         3626455         30009.08 GiB              On disk

""".split(
    "\n"
)

test_output_drac_01 = """
                             Description                Space           # of files
       /project (project rrg-bengioy-ad)              39T/75T          1316k/5000k
          /project (project def-bengioy)           956G/1000G            226k/500k
    
    Breakdown for project def-bengioy (Last update: 2022-10-25 14:01:28)
            User      File count                 Size             Location
    -------------------------------------------------------------------------
         revali               2             0.00 GiB              On disk
         urbosa               2             0.00 GiB              On disk
          daruk              50            13.49 GiB              On disk
          mipha               2             0.00 GiB              On disk
          Total          696928           877.51 GiB              On disk    

Breakdown for project rpp-bengioy (Last update: 2022-10-25 13:09:53)
           User      File count                 Size             Location
-------------------------------------------------------------------------
           riju           47085             4.20 GiB              On disk
    grosaillieh               2             0.00 GiB              On disk
      bourgette               2             0.00 GiB              On disk
          kohga               2             0.00 GiB              On disk
          rhoam               2             0.00 GiB              On disk    
          Total         3626455         30009.08 GiB              On disk

""".split(
    "\n"
)

# cedar, beluga and graham style
def test_header_00():
    L_results = parse_header_summary(test_output_drac_00)
    L_results_expected = [
        {"group": "kjsfsd78", "space": "0/2048k", "nbr_files": "0/1025"},
        {"group": "def-bengioy", "space": "971G/1000G", "nbr_files": "791k/1005k"},
        {"group": "rpp-bengioy", "space": "31T/2048k", "nbr_files": "3626k/1025"},
        {"group": "rrg-bengioy-ad", "space": "54T/75T", "nbr_files": "1837k/5005k"},
    ]

    assert len(L_results) == len(L_results_expected)

    for a, b in zip(L_results, L_results_expected):
        assert a == b
        
# narval style
def test_header_01():
    L_results = parse_header_summary(test_output_drac_01)
    L_results_expected = [
        {"group": "rrg-bengioy-ad", "space": "39T/75T", "nbr_files": "1316k/5000k"},
        {"group": "def-bengioy", "space": "956G/1000G", "nbr_files": "226k/500k"},
    ]

    assert len(L_results) == len(L_results_expected)

    for a, b in zip(L_results, L_results_expected):
        assert a == b




def test_parse_body_00():

    DLD_results = parse_body(test_output_drac_00)
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
