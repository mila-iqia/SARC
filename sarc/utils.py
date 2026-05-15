from datetime import UTC, datetime
from typing import Any, Mapping

from fabric import Connection


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


def ensure_utc(d: datetime) -> datetime:
    assert d.tzinfo is not None
    return d.astimezone(UTC)


def run_command(
    connection: Connection, command: str, retries: int
) -> tuple[str | None, list[Exception]]:
    errors: list[Exception] = []

    for _ in range(retries):
        try:
            result = connection.run(command, hide=True)
            return result.stdout, errors

        # pylint: disable=broad-exception-caught
        except Exception as err:
            errors.append(err)

    return None, errors
