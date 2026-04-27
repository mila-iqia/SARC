from sqlalchemy import Engine
from sqlmodel import SQLModel, text

from . import allocation, cluster, diskusage, job, users  # noqa: F401


def db_upgrade(engine: Engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gist"))

        # This will work for now, but should use proper migrations eventually
        SQLModel.metadata.create_all(conn, checkfirst=True)

        # TODO: do we want to also insert the clusters from the config here?
        # Probably not, but we will have to see what the consequences are.

        conn.commit()
