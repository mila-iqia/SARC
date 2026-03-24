from datetime import datetime

from pymongo.database import Database

from sarc.config import UTC


def get_parsed_date(db: Database, value_name: str) -> datetime:
    """Get the parsed date for a given value name (jobs or users, for example)."""
    parsed_date = db.runstate.find_one({"name": "parsed_date"})
    assert parsed_date is not None
    assert value_name in parsed_date
    value_str = parsed_date[value_name]
    assert value_str is not None
    return datetime.fromisoformat(value_str).replace(tzinfo=UTC)


def set_parsed_date(db: Database, value_name: str, value: datetime) -> None:
    """Set the parsed date for a given value name (jobs or users, for example)."""
    db.runstate.update_one(
        {"name": "parsed_date"},
        {"$set": {value_name: value.isoformat("milliseconds")}},
        upsert=True,
    )
