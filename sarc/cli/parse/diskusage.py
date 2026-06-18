import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.diskusage import DiskUsageDB, DiskUsageGroupDB, DiskUsageUserDB
from sarc.scraping.diskusage import get_diskusage_scraper

logger = logging.getLogger(__name__)


@dataclass
class ParseDiskUsage:
    from_: str = field(
        alias="--from",
        help="Start parsing the cache from the specified date. "
        "NB: Naive date will be interpreted as UTC.",
    )

    def execute(self) -> int:
        ts = datetime.fromisoformat(self.from_)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        cache = Cache("disk_usage")
        with config.db.session() as sess:
            for ce in cache.read_from(ts):
                for item in ce.items():
                    scraper = get_diskusage_scraper(item[0])
                    obj = scraper.parse_diskusage_report(item[1])
                    cluster_name = obj.cluster_name
                    db_obj = DiskUsageDB(
                        cluster_id=SlurmClusterDB.id_by_name(sess, cluster_name),  # ty:ignore[invalid-argument-type]
                        timestamp=obj.timestamp,
                    )

                    groups = []
                    for group in obj.groups:
                        db_group = DiskUsageGroupDB(group_name=group.group_name)
                        users = []
                        for user in group.users:
                            db_user = DiskUsageUserDB(
                                user=user.user, nbr_files=user.nbr_files, size=user.size
                            )
                            users.append(db_user)
                        db_group._users = users
                        groups.append(db_group)
                    db_obj._groups = groups

                    sess.add(db_obj)

                sess.flush()
            sess.commit()

        return 0
