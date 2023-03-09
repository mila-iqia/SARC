from dataclasses import dataclass
from typing import Optional

from sarc.config import config


@dataclass
class DbInit:
    url: Optional[str]
    database: Optional[str]

    def execute(self) -> int:
        import pymongo

        cfg = config()
        url = cfg.mongo.url if self.url is None else self.url
        database = cfg.mongo.database if self.database is None else self.database

        client = pymongo.MongoClient(url)
        db = client.get_database(database)

        # There is no need to create the collections first, they will be
        # created when the index is created.
        db.jobs.create_index(
            [
                ("cluster_name", pymongo.ASCENDING),
                ("submit_time", pymongo.ASCENDING),
                ("end_time", pymongo.ASCENDING),
            ],
        )

        return 0
