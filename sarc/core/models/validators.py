from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Callable

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


@dataclass(frozen=True)
class DatetimeUTCValidator:
    def validate_tz_utc(self, value: datetime, handler: Callable):
        assert value.tzinfo == UTC, "date is not in UTC time"

        return handler(value)

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_wrap_validator_function(
            self.validate_tz_utc, handler(source_type)
        )


type datetime_utc = Annotated[datetime, DatetimeUTCValidator()]
