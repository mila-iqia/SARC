from sqlalchemy import Engine
from sqlmodel import Session, select, text

from sarc.config import config

# We need to import those to register the tables
from . import allocation, cluster, diskusage, job, users  # noqa: F401
from .sqlmodel import SQLModel


def db_upgrade(engine: Engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gist"))

        # This will work for now, but should use proper migrations eventually
        SQLModel.metadata.create_all(conn, checkfirst=True)

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
                cluster.SlurmClusterDB.cluster_name == cluster_name
            )
        ).one_or_none()
        if db_cluster is None:
            db_cluster = cluster.SlurmClusterDB(
                cluster_name=cluster_name,
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
