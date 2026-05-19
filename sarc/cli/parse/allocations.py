import csv
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import config
from sarc.db.allocation import AllocationDB
from sarc.db.cluster import SlurmClusterDB

logger = logging.getLogger(__name__)


@dataclass
class ParseAllocations:
    since: str = field(
        help="Start parsing the cache from the specified date. "
        "NB: Naive date will be interpreted as UTC."
    )

    def execute(self) -> int:
        cache = Cache(subdirectory="allocations")

        ts = datetime.fromisoformat(self.since)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        with config().db.session() as sess:
            for ce in cache.read_from(ts):
                for _, value in ce.items():  # noqa: PERF102
                    reader = csv.DictReader(
                        value.decode("utf-8").split("\n"),
                        skipinitialspace=True,
                        restkey="garbage",
                        restval="",
                    )
                    for row in reader:
                        row.pop("garbage", None)

                        for key in list(row.keys()):
                            if row[key].strip(" ") == "":
                                row[key] = None

                        cluster_name = row.pop("cluster_name")
                        cluster_id = SlurmClusterDB.id_by_name(sess, cluster_name)
                        if cluster_id is None:
                            logger.error(
                                "Can't find cluster % for allocation, skipping row",
                                cluster_name,
                            )
                            continue
                        row["timestamp"] = datetime.now(UTC)
                        row["cluster_id"] = cluster_id
                        try:
                            allocation = AllocationDB.get_or_create(sess, **row)
                            logger.info(f"Adding allocation: {allocation}")
                        except Exception as e:
                            logger.exception(f"Skipping row: {row}", exc_info=e)
                            continue

                    sess.flush()
                sess.commit()

        return 0
