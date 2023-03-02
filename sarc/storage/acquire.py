"""
What this script does:

It collects disk usage informations on the specified cluster,
and injects it to the mongoDB database.
Meant to be cluster-agnostic.

To be called each hour by the automation.

usage example: 
$ python sarc/storage/acquire.py narval
"""

# import logging
# import sys

# from sarc.config import config
# from sarc.storage.drac import diskusage_drac_mongodb_import

# drac_clusters = ["narval", "beluga", "cedar", "graham"]


# def main(cluster_name):
#     cfg = config()
#     cluster = cfg.clusters.get(cluster_name, None)
#     if not cluster:
#         logging.error(f"Cluster unknown : {cluster_name}")
#     elif cluster_name in drac_clusters:
#         diskusage_drac_mongodb_import(cluster)
#     else:
#         # we don't support this cluster... (yet)
#         logging.error(f"Cluster not yet supported : {cluster_name}")
#         return -1


# if __name__ == "__main__":
#     main(sys.argv[1])
