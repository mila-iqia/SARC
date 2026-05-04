from sqlalchemy import Engine
from sqlalchemy.dialects import postgresql
from sqlmodel import Session, select, text

from sarc.config import config

from . import cluster
from .sqlmodel import SQLModel


def db_upgrade(engine: Engine):
    # We need to import those to register the tables
    from . import (  # noqa: F401
        allocation,
        cluster,
        diskusage,
        job,
        job_series,
        support,
        users,
    )

    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gist"))

        # This will work for now, but should use proper migrations eventually
        tables = [
            t
            for n, t in SQLModel.metadata.tables
            if n != job_series.JobSeries.__tablename__
        ]
        SQLModel.metadata.create_all(conn, tables, checkfirst=True)

        compiled_query = job_series.JobSeries.__sql_view__.compile(
            dialect=postgresql.dialect, compile_kwargs={"literal_binds": True}
        )

        # This is kinda bad for performance, so we will have to take care of it with migrations
        conn.execute(
            text(
                f"CREATE OR REPLACE VIEW {job_series.JobSeries.__tablename__} AS {compiled_query};"
            )
        )

        conn.commit()

        with Session(conn) as sess:
            insert_clusters(sess)
            sess.commit()


def insert_clusters(sess: Session) -> None:
    # populate the db with default starting dates for each cluster
    clusters = config("scraping").clusters
    for cluster_name, clust in clusters.items():
        db_cluster = sess.exec(
            select(cluster.SlurmClusterDB).where(
                cluster.SlurmClusterDB.name == cluster_name
            )
        ).one_or_none()
        if db_cluster is None:
            db_cluster = cluster.SlurmClusterDB(
                name=cluster_name,
                domain=clust.user_domain,
                start_date=clust.start_date,
                billing_is_gpu=clust.billing_is_gpu,
            )
            sess.add(db_cluster)
        else:
            db_cluster.domain = clust.user_domain
            db_cluster.start_date = clust.start_date
            db_cluster.billing_is_gpu = clust.billing_is_gpu
        sess.flush()
