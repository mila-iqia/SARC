from sarc.config import config
from sarc.inode_storage_scanner.get_diskusage import *

# Get disk usage of DRAC clusters

# drac_clusters=["narval", "beluga", "cedar", "graham"]
drac_clusters=["narval"]

for cluster_name in drac_clusters:
    cluster = config().clusters[cluster_name]
    print (f"fetching data from cluster {cluster.host} ...")
    report = fetch_diskusage_report(cluster)
    print (report[0:34])

