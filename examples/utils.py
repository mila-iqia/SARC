from __future__ import annotations

import dataclasses
import functools
import hashlib
import inspect
import logging
import pickle
import subprocess
import typing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import simple_parsing
from typing_extensions import Sequence

from sarc.config import MTL

logger = logging.getLogger(__name__)


def midnight(dt: datetime) -> datetime:
    """Returns the start of the given day (hour 00:00)."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class Filter:
    """Configuration options for this script."""

    start: datetime = simple_parsing.field(
        default=(midnight(datetime.now(tz=MTL)) - timedelta(days=30)),
        type=lambda d: datetime.fromisoformat(d).astimezone(MTL),
    )
    """ Start date. """

    end: datetime = simple_parsing.field(
        default=midnight(datetime.now(tz=MTL)),
        type=lambda d: datetime.fromisoformat(d).astimezone(MTL),
    )
    """ End date. """

    user: Sequence[str] = dataclasses.field(default_factory=tuple)
    """ Which user(s) to query information for. Leave blank to get a global compute profile."""

    clusters: Sequence[str] = dataclasses.field(default_factory=tuple)
    """ Which clusters to query information for. Leave blank to get data from all clusters."""

    def get_users(self, assume_mila_email: bool = False) -> list[str]:
        users = self.user
        user_emails = []
        for user in users:
            if "@" in user:
                user_emails.append(user.strip())
            elif assume_mila_email:
                user_emails.append(user.strip() + "@mila.quebec")
            else:
                raise ValueError(
                    f"User '{user}' does not contain an email address. "
                    "Please provide a valid email address or set `assume_mila_email=True`."
                )
        return sorted(user_emails)


def cache_results_to_file[**P, OutT](
    cache_dir: Path,
) -> Callable[[Callable[P, OutT]], Callable[P, OutT]]:
    """Caches the results of calling this function in a given cache dir.

    See also:
    - `_get_cache_file_name`: Gets the cache file for a function given its signature and passed arguments.
    - `_hash`: Gets the string representation of an argument value.
    """
    return functools.partial(wrapper, cache_dir=cache_dir)


def wrapper[**P, OutT](fn: Callable[P, OutT], cache_dir: Path) -> Callable[P, OutT]:
    """Wraps a function, caching the results to a file based on the pretty hash of the function name and arguments."""
    assert cache_dir and cache_dir.exists() and cache_dir.is_dir()

    if inspect.iscoroutinefunction(fn):
        raise NotImplementedError("Can't cache result of coroutines just yet.")

    @functools.wraps(fn)
    def _wrapper(*args: P.args, **kwargs: P.kwargs) -> OutT:
        """Decorator to cache the results of a function."""
        assert cache_dir and cache_dir.exists() and cache_dir.is_dir()
        cache_file = cache_dir / get_cache_file_name(fn, *args, **kwargs)
        if cache_file.exists():
            logger.info(f"Loading result of {fn.__name__} from {cache_file}")
            if cache_file.suffix == ".txt":
                result = cache_file.read_text()
                # the function returns a string (`OutT` is `str`)
                return typing.cast(OutT, result)
            else:
                result = pickle.loads(cache_file.read_bytes())
            # if inspect.iscoroutinefunction(fn):
            #     # If the function is a coroutine, we need to return an awaitable.
            #     return AwaitableWrapper(result)
            # return AwaitableWrapper(result)
            return result
        else:
            logger.debug(f"Cache miss for {fn.__name__} at {cache_file}")
            result = fn(*args, **kwargs)
            # if inspect.iscoroutinefunction(fn):
            #     result = result.__await__()
            if cache_file.suffix in (".txt", ".md", ".json", ".yaml", ".yml"):
                if not isinstance(result, str):
                    raise RuntimeError(
                        f"Result should be str (annotation says so!), but got {result}"
                    )
                cache_file.write_text(result)
            else:
                cache_file.write_bytes(pickle.dumps(result))
            logger.info(f"Saved result of computing {fn.__name__} to {cache_file}")
            return result

    return _wrapper


def get_cache_file_name[**P](
    fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs
) -> str:
    # More interpretable than using this:
    # return hashlib.md5(
    #     json.dumps((fn.__name__, args, kwargs), sort_keys=True, default=str).encode()
    # ).hexdigest()

    hashed_args = "-".join(map(_pretty_hash, args)) + "-".join(
        f"{k}-{_pretty_hash(v)}" for k, v in kwargs.items()
    )
    extension = ".pkl"
    try:
        if typing.get_type_hints(fn).get("return") is str:
            extension = ".txt"
    except TypeError:
        pass
    return f"{fn.__name__}-{hashed_args}{extension}"


@functools.singledispatch
def _pretty_hash(v) -> str:
    """Create a human-friendly hash string for an argument value.

    This is used to create cache file names that are more interpretable than raw hashes.
    """
    match v:
        case Filter():
            return "-".join(
                [
                    _pretty_hash(v.get_users()),
                    _pretty_hash(v.start),
                    _pretty_hash(v.end),
                    _pretty_hash(v.clusters),
                ]
            )
        case datetime(hour=0, minute=0, second=0, microsecond=0) as d:
            return d.strftime("%Y-%m-%d")
        case datetime() as v:
            return v.strftime("%Y-%m-%dT%H:%M:%S%z")
        case str() | None:
            return str(v).removesuffix("@mila.quebec")  # no quotes around strings.
        case [str(), *_] if len(v) > 2:
            # If there are more than 3 strings, hash them together.
            return hashlib.md5("+".join(sorted(v)).encode()).hexdigest()[:12]
        case list() | tuple():
            return "+".join(sorted(map(_pretty_hash, v)))
        case None | int() | float():
            return repr(v)
        # case [UserData(), *_]:
        #     return _hash(sorted([student.email for student in v]))
        # case {"$in": list(values)}:
        #     # Special case for MongoDB-like queries.
        #     return _hash(values)
        case _:
            raise NotImplementedError(f"Unsupported arg type: {v} of type {type(v)}")


def setup_sarc_access():
    # Setup access to the SARC dev machine while the SARC API is not yet deployed.

    # Check if there is already an ssh tunnel to the dev server at port 8123
    if "ssh" in subprocess.getoutput(
        # Double-check this `ss` command. Proposed by an LLM.
        "ss --tcp --listening --numeric --processes | grep 8123"
    ):
        logger.info("SSH tunnel to SARC dev server already exists.")
        return
    print("Creating ssh tunnel to dev server...")
    port_from_client_config = 8123  # todo: read and parse from the config file?
    sarc_hostname = "sarc"  # You need to have a config entry for this hostname.
    (Path.home() / ".cache" / "ssh").mkdir(parents=True, exist_ok=True)
    try:
        subprocess.check_call(
            f"ssh -oControlMaster=auto -oControlPersist=yes -oControlPath=~/.cache/ssh/%r@%h:%p -L {port_from_client_config}:localhost:27017 {sarc_hostname} echo 'SSH tunnel established'",
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        logger.error(f"Failed to create SSH tunnel to SARC dev server: {err}")
        logger.error(
            f"Make sure you have an SSH config entry for {sarc_hostname!r} configured."
        )
        raise
