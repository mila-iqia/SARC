from sarc.config import config
from sarc.storage.drac import *

# Get disk usage of DRAC clusters

drac_clusters=["narval", "beluga" , "cedar", "graham"]
# drac_clusters=["narval"]

drac_reports={}

for cluster_name in drac_clusters:
    print(f"Parsing {cluster_name} report...")

    # cluster = config().clusters[cluster_name]
    # print (f"fetching data from cluster {cluster.host} ...")
    # report = fetch_diskusage_report(cluster)

    f=open(f"report_{cluster_name}.txt","r")
    report = f.readlines()
    f.close()

    # f=open(f"report_{cluster_name}.txt","w")
    # for line in report:
    #     f.write(f"{line}\n")
    # f.close()

    header,body = parse_diskusage_report(report)
    # print(header)
    for k in body:
        print (k)
        print (body[k])
    # print (body)

