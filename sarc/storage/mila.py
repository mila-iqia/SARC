"""
Fetching and parsing code specific to the mila cluster
"""
from datetime import datetime

from sarc.config import ClusterConfig
from sarc.storage.diskusage import DiskUsage, DiskUsageGroup, DiskUsageUser
from sarc.ldap.api import get_users


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

    Examples
    --------

    .. code-block::

        $ beegfs-ctl --cfgFile=/etc/beegfs/home.d/beegfs-client.conf --getquota --uid $USER --csv

        Quota information for storage pool Default (ID: 1):

        name,id,size,hard,files,hard
        delaunap,1500000082,51046633472,107374182400,201276,1000000
    
    """
    cmd = cluster.diskusage_report_command

    users = get_users()
    
    usage = []
    main_group=DiskUsageGroup(group_name="default", users=usage)
    
    # Note: --all in beegfs does not work so we have to do it one by one
    for user in users:
        results = cluster.ssh.run(cmd.replace('$USER', user.mila.username), hide=True)
        group = parse_beegfs_csv(results.stdout)
        usage.extend(group.users)

    return DiskUsage(
        cluster_name=cluster.name, groups=[main_group], timestamp=datetime.utcnow()
    )
