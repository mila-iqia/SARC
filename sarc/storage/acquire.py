import sys
import logging

from sarc.config import config
from sarc.storage.drac import drac_mongodb_import

drac_clusters = ['narval','beluga','cedar','graham']

def main(cluster_name):
    cfg = config()
    cluster = cfg.clusters[cluster_name]
    if cluster_name in drac_clusters and cluster:
        drac_mongodb_import(cluster)
    else
        # we don't support this sluster... (yet)
        logging.error(f"Cluster unknown/unsupported: {cluster_name}")

if __name__ == "__main__":
    main(sys.argv[1])
