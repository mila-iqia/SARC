import sys
from datetime import datetime

from sarc.config import config
from sarc.jobs.db import sacct_mongodb_import


def main():
    cluster_name, date_string = sys.argv[1:]
    cfg = config()
    mc = cfg.clusters[cluster_name]
    date = datetime.strptime(date_string, "%Y-%m-%d")
    print(f"Acquire data for date: {date}")
    sacct_mongodb_import(mc, date)


if __name__ == "__main__":
    main()
