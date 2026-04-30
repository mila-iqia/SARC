from typing import Any

from pydantic_core import PydanticUndefined as Undefined
from sqlmodel.main import SQLModel as SQLModelBase
from sqlmodel.main import finish_init, is_table_model_class

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
