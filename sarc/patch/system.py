from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any, Protocol, overload


@dataclass
class PatchSystem:
    callbacks: dict[str, list[Callable]] = field(default_factory=dict)

    def create(self, name: str) -> None:
        assert name not in self.callbacks
        self.callbacks[name] = list()

    def register(self, name: str, fn: Callable) -> None:
        fn_l = self.callbacks.get(name)
        if fn_l is None:
            raise ValueError(
                f"Attempt to register callback for non-existent endpoint: {name}"
            )
        fn_l.append(fn)

    def call(self, name: str, args: tuple, kwargs: dict[str, Any]) -> None:
        fn_l = self.callbacks.get(name)
        if fn_l is None:
            raise ValueError(
                f"Attempt to call callbacks for non-existent endpoint: {name}"
            )
        for fn in fn_l:
            fn(*args, **kwargs)


system = PatchSystem()


class _Fn[**P](Protocol):
    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> None: ...


def declare_patch[**P](fn: _Fn[P]) -> Callable[P, None]:
    name = fn.__name__
    system.create(name)

    @wraps(fn)
    def do_call(*args: P.args, **kwargs: P.kwargs) -> None:
        system.call(name, args, kwargs)

    return do_call


@overload
def register(
    name_or_fn: Callable[..., None], fn: None = None
) -> Callable[..., None]: ...


@overload
def register(
    name_or_fn: str, fn: None = None
) -> Callable[[Callable[..., None]], Callable[..., None]]: ...


@overload
def register(name_or_fn: str, fn: Callable[..., None]) -> None: ...


def register(
    name_or_fn: str | Callable[..., None], fn: Callable[..., None] | None = None
) -> Callable[[Callable[..., None]], Callable[..., None]] | Callable[..., None] | None:
    if callable(name_or_fn):
        system.register(name_or_fn.__name__, name_or_fn)  # ty:ignore[unresolved-attribute]
        return name_or_fn  # ty:ignore[invalid-return-type]
    elif fn is not None:
        system.register(name_or_fn, fn)
        return fn
    else:

        def decorator(f: Callable[..., None]) -> Callable[..., None]:
            system.register(name_or_fn, f)
            return f

        return decorator


def load(dir: Path) -> None:
    for path in sorted(dir.rglob("*.py")):
        exec(compile(path.read_text(), path, "exec"), {"__file__": str(path)})
