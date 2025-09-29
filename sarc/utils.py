from typing import Any, Mapping


def flatten(d: Mapping[str, Any]) -> dict[str, Any]:
    res = {}

    def _flatten(d: Mapping[str, Any], parent: str | None = None):
        for k, v in d.items():
            fk = f"{parent}.{k}" if parent else k
            if isinstance(v, Mapping):
                _flatten(v, fk)
            else:
                if fk in res:
                    raise ValueError(f"Key {fk} is duplicated")
                res[fk] = v

    _flatten(d)
    return res
