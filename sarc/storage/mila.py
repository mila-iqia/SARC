"""
Fetching and parsing code specific to the mila cluster
"""

from datetime import datetime

from tqdm import tqdm

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


def _fetch_diskusage_report(connection, command, retries):
    errors = []

    for _ in range(retries):
        try:
            result = connection.run(command, hide=True)
            return result.stdout, errors

        # pylint: disable=broad-exception-caught
        except Exception as err:
            errors.append(err)

    return None, errors


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

    users = get_users()
    assert len(users) > 0

    usage = []
    errors = []
    failures = []

    # Note: --all in beegfs does not work so we have to do it one by one
    connection = cluster.ssh
    for user in tqdm(users):
        if not user.mila.active:
            continue

        cmd_exec = cmd.replace("$USER", user.mila.username)
        result, err = _fetch_diskusage_report(connection, cmd_exec, retries)

        if result is None:
            failures.append(user)

        if err:
            errors.extend(err)

        if result is not None:
            group = parse_beegfs_csv(result)
            usage.extend(group.users)

    print(f"Error Count: {len(errors)}")
    print(f"Failures: {len(failures)}")
    print(f"    Details: {failures}")

    return DiskUsage(
        cluster_name=cluster.name,
        groups=[DiskUsageGroup(group_name="default", users=usage)],
        timestamp=datetime.utcnow(),
    )
