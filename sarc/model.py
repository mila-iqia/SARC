from datetime import date, datetime
from functools import cached_property
from typing import Any

from pydantic import BaseModel as _BaseModel


class BaseModel(_BaseModel):
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
