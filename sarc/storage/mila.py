"""
Fetching and parsing code specific to the mila cluster
"""
from datetime import datetime

from sarc.config import ClusterConfig
from sarc.ldap.api import get_users
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
            name, _, size, _, files, _ = line.split(",")
            documents.append(DiskUsageUser(user=name, nbr_files=files, size=size))

    if len(documents) < 1:
        print("Beegfs output was empty")

    return DiskUsageGroup(group_name="mila", users=documents)


def fetch_diskusage_report(cluster: ClusterConfig, retries: int = 3):
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

    users = get_users()[:10]
    assert len(users) > 0

    usage = []
    errors = []
    failures = []

    # Note: --all in beegfs does not work so we have to do it one by one
    connection = cluster.ssh
    for user in users:
        cmd_exec = cmd.replace("$USER", user.mila.username)

        for _ in range(retries):
            try:
                results = connection.run(cmd_exec, hide=True)
                break
            # pylint: disable=broad-exception-caught
            except Exception as err:
                errors.append(err)
        else:
            failures.append(user)

        group = parse_beegfs_csv(results.stdout)
        usage.extend(group.users)

    print(f"Error Count: {len(errors)}")
    print(f"Failures: {len(failures)}")
    print(f"    Details: {failures}")

    return DiskUsage(
        cluster_name=cluster.name,
        groups=[DiskUsageGroup(group_name="default", users=usage)],
        timestamp=datetime.utcnow(),
    )
