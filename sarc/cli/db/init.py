from dataclasses import dataclass
from typing import Literal

import pymongo
from pymongo.database import Database
from simple_parsing import choice

from sarc.config import config


@dataclass
class DbInit:
    url: str | None
    database: str | None

    username: str | None
    password: str | None
    account: Literal["admin", "write", "read"] | None = choice("admin", "write", "read")

    def execute(self) -> int:
        cfg = config()
        url = cfg.mongo.connection_string if self.url is None else self.url
        self.database = (
            cfg.mongo.database_name if self.database is None else self.database
        )

        client: pymongo.MongoClient = pymongo.MongoClient(url)
        db = client.get_database(self.database)

        self.create_readonly_role(db)
        self.create_acount(client, db)

        return 0

    def create_acount(self, client: pymongo.MongoClient, db: Database) -> None:
        if self.username is None or self.password is None:
            return

        if self.account == "admin":
            client.admin.command(
                "createUser",
                self.username,
                pwd=self.password,
                roles=[
                    {"role": "userAdminAnyDatabase", "db": "admin"},
                    {"role": "readWriteAnyDatabase", "db": "admin"},
                ],
            )

        if self.account == "read":
            db.command(
                "createUser",
                self.username,
                pwd=self.password,
                roles=[{"role": f"{self.database}ReadOnly", "db": self.database}],
            )

        if self.account == "write":
            db.command(
                "createUser",
                self.username,
                pwd=self.password,
                roles=[{"role": "readWrite", "db": self.database}],
            )

    def create_readonly_role(self, db: Database) -> None:
        collections = [
            "allocations",
            "diskusage",
            "users",
            "jobs",
            "clusters",
            "gpu_billing",
            "node_gpu_mapping",
            "healthcheck",
        ]

        try:
            db.command(
                "createRole",
                f"{self.database}ReadOnly",
                privileges=[
                    {
                        "actions": ["find"],
                        "resource": {"db": self.database, "collection": coll},
                    }
                    for coll in collections
                ],
                roles=[],
            )
        except pymongo.errors.OperationFailure as err:
            if "already exists" not in str(err):
                raise
