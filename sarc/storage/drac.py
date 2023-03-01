"""
Fetching and parsing code specific to DRAC clusters
"""

import logging

from sarc.config import ClusterConfig
import re


def _parse_fraction(s):
    """
    Something like
    0/2048k    971G/1000G   3626k/1025   791k/1005k

    Note sure if there's anything better to do than to just return it as is.
    """
    return s


def _parse_header_summary(L_lines: list[str]):
    """
    beluga, cedar and graham format :
                        Description                Space           # of files
        /project (group kdf900)                  0/2048k               0/1025
        /project (group def-bengioy)           971G/1000G           791k/1005k
        /project (group rpp-bengioy)            31T/2048k           3626k/1025
        /project (group rrg-bengioy-ad)           54T/75T          1837k/5005k

    narval format :
                        Description                Space           # of files
        /project (project kdf900)                  0/2048k               0/1025
        /project (project def-bengioy)           971G/1000G           791k/1005k
        /project (project rpp-bengioy)            31T/2048k           3626k/1025
        /project (project rrg-bengioy-ad)           54T/75T          1837k/5005k
    """
    L_results = []
    inside_segment = False
    for line in L_lines:
        if re.match(r"\s+Description\s+Space.*", line):
            inside_segment = True
            continue
        elif m := re.match(
            r"\s*/project \(project\s(.*?)\)\s+(.+?)\s+(.+)", line
        ) or re.match(r".*/project \(group\s(.*?)\)\s+(.+?)\s+(.+)", line):
            if inside_segment:
                L_results.append(
                    {
                        "group": m.group(1),
                        "space": _parse_fraction(m.group(2)),
                        "nbr_files": _parse_fraction(m.group(3)),
                    }
                )
                # print(f"Header: {L_results[-1]}")

            else:
                # we don't expect this branch to ever be taken
                continue
        else:
            inside_segment = False

    return L_results


def _parse_body(L_lines: list[str], DLD_results=None):
    """
    Breakdown for project def-bengioy (Last update: 2022-10-25 14:01:28)
            User      File count                 Size             Location
    -------------------------------------------------------------------------
       kfsdfsdf               2             0.00 GiB              On disk
       k000f0ds               2             0.00 GiB              On disk
         kdf900              50            13.49 GiB              On disk
         k349ff               2             0.00 GiB              On disk
          Total          696928           877.51 GiB              On disk
    """

    if DLD_results is None:
        DLD_results = {}
    # DLD_results indexed by project name, contains a list of dict entries per user

    project = None
    LD_results = []
    inside_segment = False
    for n, line in enumerate(L_lines):
        if not inside_segment and re.match(
            r"^\s*$", line
        ):  # skip empty line when outside of segment
            continue
        elif m := re.match(r"^\s*Breakdown\sfor\sproject\s(.+?)\s.*$", line):
            inside_segment = True
            project = m.group(1)
            continue
        elif re.match(r"^\s*\-+\s*$", line):  # line with only -----
            continue
        elif re.match(
            r"^\s*User\s*File\scount\s*Size\s*Location\s*$", line
        ):  # line with column names
            continue
        elif inside_segment and re.match(r"^\s*$", line):  # empty line marks the end
            # accumulate into the dict to return before recursive call
            assert project
            assert LD_results
            DLD_results[project] = LD_results
            # print(f"Going into recursive call from n {n}.")
            return _parse_body(L_lines[n:], DLD_results)
        elif inside_segment:
            # omitting the "On Disk" part of the line
            m = re.match(r"^\s*([\w\.]+)\s+(\d+)\s+([\d\.]+)\s(\w+)\s*", line)
            assert m, f"If this line doesn't match, we've got a problem.\n{line}"
            username = m.group(1)
            nbr_files = int(m.group(2))
            size = (float(m.group(3)), m.group(4))
            LD_results.append(
                {"username": username, "nbr_files": nbr_files, "size": size}
            )

    # this gets returned like that only on the last recursive call
    # print(DLD_results)
    return DLD_results


def parse_diskusage_report(L_lines: list[str]):
    """
    Parses the output of fetch_diskusage_report
    """
    header = _parse_header_summary(L_lines)
    body = _parse_body(L_lines)
    return header, body


def fetch_diskusage_report(cluster: ClusterConfig):
    """
        Get the output of the command diskusage_report --project --all_users on the wanted cluster

        The output is something like this:

                                 Description                Space           # of files
           /project (project rrg-bengioy-ad)              39T/75T          1226k/5000k
              /project (project def-bengioy)           956G/1000G            226k/500k

    Breakdown for project rrg-bengioy-ad (Last update: 2023-02-27 23:04:29)
               User      File count                 Size             Location
    -------------------------------------------------------------------------
             user01               2             0.00 GiB              On disk
             user02           14212           223.99 GiB              On disk
    (...)
             user99               4           819.78 GiB              On disk
              Total          381818         36804.29 GiB              On disk


    Breakdown for project def-bengioy (Last update: 2023-02-27 23:00:57)
               User      File count                 Size             Location
    -------------------------------------------------------------------------
             user01               2             0.00 GiB              On disk
             user02           14212           223.99 GiB              On disk
    (...)
             user99               4           819.78 GiB              On disk
              Total          381818         36804.29 GiB              On disk


    Disk usage can be explored using the following commands:
    diskusage_explorer /project/rrg-bengioy-ad 	 (Last update: 2023-02-27 20:06:27)
    diskusage_explorer /project/def-bengioy 	 (Last update: 2023-02-27 19:59:41)
    """
    cmd = "diskusage_report --project --all_users"
    # print(f"{cluster.name} $ {cmd}")
    results = cluster.ssh.run(cmd, hide=True)
    return results.stdout.split("\n")  # break this long string into a list of lines


def drac_mongodb_import(cluster: ClusterConfig):
    """
    All-in-one function to :
    - fetch the disk usage statistics on the specified cluster
    - parse it
    - import it in MongoDB
    """
    logging.error("Not yet implemented")
    return
