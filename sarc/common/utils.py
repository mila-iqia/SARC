from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
import re
from tqdm import tqdm as std_tqdm

from apischema import ValidationError, deserializer, serializer
from dateutil import parser as dateparser


gui_page = ContextVar("gui_page", default=None)


@serializer
def _serialize_timedelta(td: timedelta) -> str:
    """Serialize timedelta as Xs (seconds) or Xus (microseconds)."""
    seconds = int(td.total_seconds())
    if td.microseconds:
        return f"{seconds}{td.microseconds:06}us"
    else:
        return f"{seconds}s"


@deserializer
def _deserialize_timedelta(s: str) -> timedelta:
    """Deserialize a combination of days, hours, etc. as a timedelta."""
    units = {
        "d": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
        "ms": "milliseconds",
        "us": "microseconds",
    }
    sign = 1
    if s.startswith("-"):
        s = s[1:]
        sign = -1
    kw = {}
    parts = re.split(string=s, pattern="([a-z ]+)")
    if parts[-1] != "":
        raise ValidationError("timedelta representation must end with a unit")
    for i in range(len(parts) // 2):
        n = parts[i * 2]
        unit = parts[i * 2 + 1].strip()
        if unit not in units:
            raise ValidationError(f"'{unit}' is not a valid timedelta unit")
        try:
            kw[units[unit]] = float(n)
        except ValueError as err:
            raise ValidationError(
                f"Could not convert '{n}' ({units[unit]}) to float"
            ) from err
    return sign * timedelta(**kw)


@deserializer
def _deserialize_date(s: str) -> datetime:
    """This is mostly so that things work with Python <3.11."""
    return dateparser.parse(s)


@contextmanager
def display_on_gui(element):
    token = gui_page.set(element)
    try:
        yield
    finally:
        gui_page.reset(token)


class tqdm(std_tqdm):
    def __init__(self, *args, **kwargs):
        self.element = gui_page.get()
        if self.element is None:
            super().__init__(*args, **kwargs)
        else:
            from hrepr import H

            self.H = H
            super().__init__(*args, **kwargs, gui=True)
            self.element.set(
                H.div["progress"](
                    H.div["progress_desc"](self.desc),
                    elem_bar := H.div["progress_bar_holder"]().ensure_id(),
                    elem_n := H.div["progress_n"](self.format_dict["n"]).ensure_id(),
                    H.div("/"),
                    elem_total := H.div["progress_total"](
                        self.format_dict["total"]
                    ).ensure_id(),
                )
            )
            self.elem_bar = self.element[elem_bar]
            self.elem_n = self.element[elem_n]
            self.elem_total = self.element[elem_total]

    def display(self, msg=None, pos=None):
        if self.element is None:
            super().display(msg, pos)
        else:
            n = self.format_dict["n"]
            total = self.format_dict["total"]
            pct = int(100 * n / total)
            self.elem_bar.set(self.H.div["progress_bar"](style={"width": f"{pct}%"}))
            self.elem_n.set(str(n))

    def close(self):
        if self.element is None:
            return super().close()
        else:
            self.display()

    def clear(self):
        if self.element is None:
            return super().clear()
        else:
            self.element.clear()
