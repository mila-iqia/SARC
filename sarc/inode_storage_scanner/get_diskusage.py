"""
This is not a complete implementation.
It is just a stub, notes for what needs to be done.

Some of the configuration was added to the `sarc-dev.json` file
so as to have information about the commands specific to our clusters.

Because this project is currently under reorganization,
it is not clear how the configuration will be loaded,
how to run commands through ssh or where this script
will run.


Basically, this script will go through all the clusters
found in the config file and it will look for those that
have the keys "duc_inodes_command", "duc_storage_command", "diskusage_report_command".
If the "cluster_name" argument is specific at command-line,
it will only process that one.

The commands found will be run on the cluster and the output
will be parsed and stored in the database.
As of now, we think that "diskusage_report_command" contains
all the information and we don't need to run the other two commands.
Nevertheless, some sanity check is in order.

It is also recommended to add an option to simply dump
the contents of the files on the disk instead of committing
to the database. This will allow us to check the output for
debugging, and also to generate properly anonymized equivalents
meant for testing.
"""

# from pymongo import MongoClient, UpdateOne
import argparse

# from sarc.common.config import get_config
import json
from ..config import ClusterConfig

parser = argparse.ArgumentParser(
    description="Fetch the diskusage from a compute cluster and commit the information to our database."
)
parser.add_argument(
    "--ADD_PROPER_ARGUMENTS_HERE",
    type=str,
    default="",
    help="",
)


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
    print(f"{cluster.name} $ {cmd}")
    results = cluster.ssh.run(cmd, hide=True)
    return results.stdout.split('\n')
    

