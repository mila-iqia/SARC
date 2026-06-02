from alembic_utils.pg_extension import PGExtension
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

from alembic import context
from sarc.config import config
from sarc.db import get_meta, job_series

compiled_query = job_series.JobSeriesDB.__sql_view__.compile(
    dialect=PGDialect_psycopg(), compile_kwargs={"literal_binds": True}
)
job_series_view = PGView(
    schema="public", signature="job_series_view", definition=f"{compiled_query}"
)
btree_gist = PGExtension(schema="public", signature="btree_gist")

register_entities([btree_gist, job_series_view])

target_metadata = get_meta()


def include_object(object, name, type_: str, reflected: bool, compare_to):  # noqa: ARG001
    if type_ == "table" and name == job_series.JobSeriesDB.__tablename__:
        return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    with config("scraping").db.engine.connect() as conn:
        context.configure(
            connection=conn,
            target_metadata=target_metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    with config("scraping").db.engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
