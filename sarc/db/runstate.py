from datetime import datetime

from sqlmodel import Field, Session, SQLModel, select, update

from sarc.core.models.validators import datetime_utc


class ParseDates(SQLModel, table=True):
    name: str = Field(primary_key=True)
    date: datetime_utc


def get_parsed_date(sess: Session, value_name: str) -> datetime:
    """Get the parsed date for a given value name (jobs or users, for example)."""
    return sess.exec(select(ParseDates).where(ParseDates.name == value_name)).one().date


def set_parsed_date(sess: Session, value_name: str, value: datetime) -> None:
    """Set the parsed date for a given value name (jobs or users, for example)."""
    sess.exec(
        update(ParseDates).where(ParseDates.name == value_name).values(date=value)
    )
