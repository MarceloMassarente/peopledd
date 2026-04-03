from __future__ import annotations

import copy
from datetime import datetime
from typing import Any


def Field(default: Any = None, default_factory=None, **kwargs):  # noqa: N802
    if default_factory is not None:
        return default_factory()
    return default


class BaseModel:
    def __init__(self, **data: Any) -> None:
        annotations = getattr(self.__class__, "__annotations__", {})
        for name in annotations:
            if name in data:
                value = data[name]
            else:
                value = copy.deepcopy(getattr(self.__class__, name, None))
            setattr(self, name, value)

    def model_dump(self, mode: str | None = None) -> dict[str, Any]:
        def _dump(value: Any):
            if isinstance(value, BaseModel):
                return value.model_dump(mode=mode)
            if isinstance(value, list):
                return [_dump(v) for v in value]
            if isinstance(value, dict):
                return {k: _dump(v) for k, v in value.items()}
            if isinstance(value, datetime):
                return value.isoformat()
            return value

        return {k: _dump(v) for k, v in self.__dict__.items()}

    def model_copy(self, deep: bool = False, update: dict[str, Any] | None = None):
        obj = copy.deepcopy(self) if deep else copy.copy(self)
        if update:
            for k, v in update.items():
                setattr(obj, k, v)
        return obj
