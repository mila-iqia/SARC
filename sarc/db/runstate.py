from datetime import datetime

from sqlmodel import Field, Session, col, select, update

from sarc.core.models.validators import datetime_utc

from .sqlmodel import SQLModel


class ParseDates(SQLModel, table=True):
    name: str = Field(primary_key=True)
    date: datetime_utc


def get_parsed_date(sess: Session, value_name: str) -> datetime | None:
    """Get the parsed date for a given value name (jobs or users, for example)."""
    return sess.exec(
        select(ParseDates.date).where(ParseDates.name == value_name)
    ).one_or_none()


def set_parsed_date(sess: Session, value_name: str, value: datetime) -> None:
    """Set the parsed date for a given value name (jobs or users, for example)."""
    sess.exec(
        update(ParseDates).where(col(ParseDates.name) == value_name).values(date=value)
    )
