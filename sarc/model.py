import zoneinfo
from datetime import date, datetime
from functools import cached_property
from typing import Any

import tzlocal
from bson import ObjectId
from pydantic import BaseModel as _BaseModel
from pydantic import Extra

MTL = zoneinfo.ZoneInfo("America/Montreal")
PST = zoneinfo.ZoneInfo("America/Vancouver")
UTC = zoneinfo.ZoneInfo("UTC")
TZLOCAL = zoneinfo.ZoneInfo(tzlocal.get_localzone_name())


class BaseModel(_BaseModel):
    class Config:
        # Forbid extra fields that are not explicitly defined
        extra = Extra.forbid
        # Ignore cached_property, this avoids errors with serialization
        keep_untouched = (cached_property,)
        # Serializer for mongo's object ids
        json_encoders = {ObjectId: str}
        # Allow types like ZoneInfo
        arbitrary_types_allowed = True

    def dict(self, *args, **kwargs) -> dict[str, Any]:
        d = super().dict(*args, **kwargs)
        for k, v in list(d.items()):
            if isinstance(getattr(type(self), k, None), cached_property):
                del d[k]
                continue

        for k, v in d.items():
            if isinstance(v, date) and not isinstance(v, datetime):
                d[k] = datetime(
                    year=v.year,
                    month=v.month,
                    day=v.day,
                )
        return d

    def replace(self, **replacements):
        new_arguments = {**self.dict(), **replacements}
        return type(self)(**new_arguments)
