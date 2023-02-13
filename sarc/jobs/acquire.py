import sys
from datetime import datetime

from sarc.config import config
from sarc.jobs.sacct import sacct_mongodb_import


def main():
    cluster_name, *date_strings = sys.argv[1:]
    cfg = config()
    mc = cfg.clusters[cluster_name]
    for date_string in date_strings:
        date = datetime.strptime(date_string, "%Y-%m-%d")
        print(f"Acquire data for date: {date}")
        sacct_mongodb_import(mc, date)


if __name__ == "__main__":
    main()
