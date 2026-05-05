from datetime import datetime
from typing import Any

from pydantic_core import PydanticUndefined as Undefined
from sqlalchemy import DateTime
from sqlalchemy.types import TypeDecorator
from sqlmodel import Field
from sqlmodel.main import SQLModel as SQLModelBase, finish_init, is_table_model_class

from sarc.core.models.validators import UTCOFFSET


class UTCDateTime(TypeDecorator):
    """TIMESTAMPTZ that rejects naive or non-UTC datetimes.

    Defense in depth over the ``datetime_utc`` Pydantic validator: catches
    paths that bypass Pydantic, e.g. raw ``insert()``/``update()`` calls
    or attribute mutation after construction.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def _expect_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(f"naive datetime not allowed: {value!r}")
        if value.utcoffset() != UTCOFFSET:
            raise ValueError(f"datetime must be in UTC timezone: {value!r}")
        return value

    def process_bind_param(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        return self._expect_utc(value)

    def process_result_value(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        # We also expect UTC when reading from database.
        return self._expect_utc(value)


def datetime_utc_field(**kwargs: Any) -> Any:
    """SQLModel field for tz-aware UTC datetimes stored as TIMESTAMPTZ.

    Plain ``datetime`` annotations map to ``TIMESTAMP WITHOUT TIME ZONE`` in
    PostgreSQL, which drops the tzinfo on round-trip. Use this helper so the
    column is created as ``TIMESTAMPTZ`` and tz-aware values survive reads.
    """
    return Field(sa_type=UTCDateTime, **kwargs)

# This code is lifted from this PR: https://github.com/fastapi/sqlmodel/pull/1823
# If that is eventually merged, we can revert to just using the base class.


def sqlmodel_init(*, self: SQLModel, data: dict[str, Any]) -> None:
    old_dict = self.__dict__.copy()
    self.__pydantic_validator__.validate_python(data, self_instance=self)
    if not is_table_model_class(self.__class__):
        object.__setattr__(self, "__dict__", {**old_dict, **self.__dict__})
    else:
        fields_set = self.__pydantic_fields_set__.copy()
        for key, value in {**old_dict, **self.__dict__}.items():
            setattr(self, key, value)
        object.__setattr__(self, "__pydantic_fields_set__", fields_set)
        for key in self.__sqlmodel_relationships__:
            value = data.get(key, Undefined)
            if value is not Undefined:
                setattr(self, key, value)


class SQLModel(SQLModelBase):
    def __init__(__pydantic_self__, **data: Any) -> None:
        if finish_init.get():
            sqlmodel_init(self=__pydantic_self__, data=data)
