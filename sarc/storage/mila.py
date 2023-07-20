"""
Fetching and parsing code specific to the mila cluster
"""

import re
from datetime import datetime

from sarc.config import ClusterConfig
from sarc.storage.diskusage import DiskUsage, DiskUsageGroup, DiskUsageUser


beegfs_header = "name,id,size,hard,files,hard"


def parse_beegfs_csv(output):
    documents = []

    started = False
    # find header
    for line in output.splitlines():
        if beegfs_header in line:
            started = True
            continue

        if started:
            name, id, size, hard, files, limit = line.split(",")

            documents.append(DiskUsageUser(name, files, size))

    return DiskUsageGroup(group_name="mila", users=documents)


def fetch_diskusage_report(cluster: ClusterConfig):
    """Get the output of the command beegfs-ctl on the wanted cluster

    Notes
    -----

    ``beegfs-ctl`` has a ``--all`` option but it does not work

    Example
    -------

    .. code-block::

        $ beegfs-ctl --cfgFile=/etc/beegfs/home.d/beegfs-client.conf --getquota --uid $USER --csv

        Quota information for storage pool Default (ID: 1):

        name,id,size,hard,files,hard
        delaunap,1500000082,51046633472,107374182400,201276,1000000
    """
    cmd = cluster.diskusage_report_command

    results = cluster.ssh.run(cmd, hide=True)

    group = parse_beegfs_csv(results.stdout)

    return DiskUsage(
        cluster_name=cluster.name, groups=[group], timestamp=datetime.utcnow()
    )
