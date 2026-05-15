from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Callable, Self

from pydantic import BaseModel, Field, GetCoreSchemaHandler
from pydantic.functional_validators import model_validator
from pydantic_core import CoreSchema, core_schema

UTCOFFSET = timedelta(0)


@dataclass(frozen=True)
class DatetimeUTCValidator:
    def validate_tz_utc(self, value: datetime, handler: Callable):
        val = handler(value)
        assert val.tzinfo is not None, "date is not tz-aware"
        assert val.utcoffset() == UTCOFFSET, "date is not in UTC timezone"

        return val

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_wrap_validator_function(
            self.validate_tz_utc, handler(source_type)
        )


datetime_utc = Annotated[datetime, DatetimeUTCValidator()]


def _max_upper(a: datetime | None, b: datetime | None) -> datetime | None:
    """Max of two upper bounds where None represents +infinity."""
    return None if (a is None or b is None) else max(a, b)


def _lower_gt_upper(lower: datetime | None, upper: datetime | None) -> bool:
    """lower (None=-∞) > upper (None=+∞). Always False if either is None."""
    return lower is not None and upper is not None and lower > upper


def _lower_eq_upper(lower: datetime | None, upper: datetime | None) -> bool:
    """lower == upper as a touching-point check. False if either is None."""
    return lower is not None and upper is not None and lower == upper


def _lower_ge(a: datetime | None, b: datetime | None) -> bool:
    """a >= b for lower bounds (None = -∞)."""
    return b is None or (a is not None and a >= b)


def _lower_gt(a: datetime | None, b: datetime | None) -> bool:
    """a > b for lower bounds (None = -∞)."""
    return a is not None and (b is None or a > b)


def _upper_lt(a: datetime | None, b: datetime | None) -> bool:
    """a < b for upper bounds (None = +∞)."""
    return a is not None and (b is None or a < b)


class DateRange(BaseModel):
    lower: datetime_utc | None
    upper: datetime_utc | None

    @model_validator(mode="after")
    def range_order(self) -> Self:
        if self.lower is not None and self.upper is not None:
            assert self.lower <= self.upper
        return self

    @property
    def bounds(self) -> str:
        return "[)"

    def contains(self, date: datetime) -> bool:
        """True if date is in [lower, upper), with None bounds meaning unbounded."""
        if self.lower is not None and date < self.lower:
            return False
        if self.upper is not None and date >= self.upper:
            return False
        return True

    def overlaps(self, other: DateRange) -> bool:
        """True if two [lower, upper) ranges share any point."""
        if (
            self.upper is not None
            and other.lower is not None
            and self.upper <= other.lower
        ):
            return False
        if (
            other.upper is not None
            and self.lower is not None
            and other.upper <= self.lower
        ):
            return False
        return True

    def ends_before_start_of(self, other: DateRange) -> bool:
        """True if self ends at or before other starts (self.upper <= other.lower)."""
        return (
            self.upper is not None
            and other.lower is not None
            and self.upper <= other.lower
        )

    def with_lower(self, lower: datetime | None) -> DateRange:
        return DateRange(lower=lower, upper=self.upper)

    def with_upper(self, upper: datetime | None) -> DateRange:
        return DateRange(lower=self.lower, upper=upper)


class ValidTag[V](BaseModel):
    value: V
    valid: DateRange


class DateOverlapError(Exception):
    def __init__(self, tag1: ValidTag, tag2: ValidTag):
        super().__init__(
            f"""Overlapping validity with different values:
{tag1.value}: {tag1.valid.lower} - {tag1.valid.upper}
{tag2.value}: {tag2.valid.lower} - {tag2.valid.upper}"""
        )


class DateMatchError(Exception):
    def __init__(self, date: datetime):
        super().__init__(f"No valid value for date: {date}")


class ValidField[V](BaseModel):
    """
    Class to maintain a list of date-scoped values.

    You should never access the list directly, but use the accessor methods
    instead.

    The list is kept ordered in order of validity with the most recent entry
    first. We do not expect frequent changes to the values or to have a humongous
    number of values.
    """

    values: list[ValidTag[V]] = Field(default_factory=list)

    def insert(
        self, value: V, start: datetime | None = None, end: datetime | None = None
    ) -> None:
        """Add a value with optional validity bounds.

        The validity bounds may not overlap with a different value. If the
        values are the same (checked with ==) and the bounds overlap, the
        entries will be merged otherwise a DateOverlapError is raised.

        As a special case, if you add a value that starts later than all
        exisiting values and the end bound of the most recent value is unbounded,
        instead of an overlapping error, the bound of the most recent value will
        be adjusted to end at the start of the new value.
        """
        assert start is None or start.tzinfo is not None
        assert end is None or end.tzinfo is not None

        start = start.astimezone(UTC) if start is not None else None
        end = end.astimezone(UTC) if end is not None else None
        tag = ValidTag(value=value, valid=DateRange(lower=start, upper=end))
        self._insert_tag(tag, truncate=False)

    def _insert_tag(self, tag: ValidTag[V], truncate) -> None:
        """Insert a tag in its proper position, possibly modifying bounds around it."""
        if len(self.values) == 0:
            self.values.append(tag)
            return

        viter = iter(enumerate(self.values))
        i, ltag = next(viter)
        try:
            while tag.valid.ends_before_start_of(ltag.valid):
                i, ltag = next(viter)
        except StopIteration:
            # The new tag ends before the last tag starts so just add it at the end
            self.values.append(tag)
            return
        # Here we know that ltag is the first tag in the list to have any
        # possible overlap with tag. Since the list is kept in "most recent
        # first" order that means that any tag that is after ltag is fully
        # after tag too ("after" in time, not in list order).
        if _lower_gt_upper(tag.valid.lower, ltag.valid.upper):
            # The new tag is fully before ltag so we just insert it
            self.values.insert(i, tag)
        elif _lower_eq_upper(tag.valid.lower, ltag.valid.upper):
            # The new tag starts just after ltag so we merge/insert depending if
            # the value is the same
            if tag.value == ltag.value:
                ltag.valid = ltag.valid.with_upper(tag.valid.upper)
            else:
                self.values.insert(i, tag)
        elif tag.value == ltag.value:
            # The new tag overlaps ltag
            if _lower_ge(tag.valid.lower, ltag.valid.lower):
                # Since the values are the same and tag starts after ltag,
                # we can just merge into ltag and be done
                ltag.valid = ltag.valid.with_upper(
                    _max_upper(ltag.valid.upper, tag.valid.upper)
                )
            else:
                # tag starts before ltag so we need to check previous
                # tag(s). to do it, we merge ltag into tag, remove ltag from
                # the list and try to insert again. This can happen
                # recursively, but since we do not expect the list to grow
                # large, it is fine for now
                tag.valid = tag.valid.with_upper(
                    _max_upper(ltag.valid.upper, tag.valid.upper)
                )
                self.values.pop(i)
                try:
                    return self._insert_tag(tag, truncate=truncate)
                except DateOverlapError:
                    # if there is an overlap, we restore the removed tag to
                    # leave the list as it was
                    self.values.insert(i, ltag)
                    raise

        # exception for new "current" value as described in insert()
        elif (
            i == 0
            and ltag.valid.upper is None
            and _lower_gt(tag.valid.lower, ltag.valid.lower)
        ):
            ltag.valid = ltag.valid.with_upper(tag.valid.lower)
            self.values.insert(i, tag)
        elif not truncate:
            # We have an overlap and the values differ
            raise DateOverlapError(tag, ltag)
        elif _lower_ge(tag.valid.lower, ltag.valid.lower):
            # Clip tag to start where ltag ends; only insert if the result is non-empty.
            # Non-empty means ltag has a finite end that is strictly before tag's end.
            if ltag.valid.upper is not None and (
                tag.valid.upper is None or ltag.valid.upper < tag.valid.upper
            ):
                tag.valid = tag.valid.with_lower(ltag.valid.upper)
                self.values.insert(i, tag)
            ### from here we know that tag starts before ltag ###
        elif _upper_lt(tag.valid.upper, ltag.valid.upper):
            # If tag ends within ltag, clip the end and retry.
            # If ltag has no lower bound, tag is fully swallowed so we drop it.
            if ltag.valid.lower is not None:
                tag.valid = tag.valid.with_upper(ltag.valid.lower)
                self._insert_tag(tag, truncate=truncate)
        else:
            # tag fully overlaps ltag, so we split tag in two, add the part
            # that goes after ltag, and try again for the part that goes
            # before.
            orig_lower = tag.valid.lower
            if ltag.valid.upper is not None:
                tag.valid = tag.valid.with_lower(ltag.valid.upper)
                self.values.insert(i, tag)
            if ltag.valid.lower is not None:
                tag2 = ValidTag(
                    value=tag.value,
                    valid=DateRange(lower=orig_lower, upper=ltag.valid.lower),
                )
                self._insert_tag(tag2, truncate=truncate)

    def get_value(self, date: datetime | None = None) -> V:
        """Get the valid value at specified time.

        If date is None, we use the current time as the date.
        If there was no value as the specified date, raises DateMatchError.
        """
        assert date is None or date.tzinfo is not None

        if date is None:
            date = datetime.now(UTC)

        date = date.astimezone(UTC)

        for tag in self.values:
            if tag.valid.contains(date):
                return tag.value
        raise DateMatchError(date)

    def values_in_range(self, start: datetime_utc, end: datetime_utc) -> list[V]:
        """Get values in a range

        The range starts at `start` and ends just before `end`.  This means that
        start is included, but end is not.
        """
        query = DateRange(lower=start.astimezone(UTC), upper=end.astimezone(UTC))
        return [tag.value for tag in self.values if tag.valid.overlaps(query)]

    def merge_with(self, other: Self, truncate=False) -> None:
        """Insert all the values in other in self.

        If truncate=True, inserted values will have their validity periods
        reduced to fit around existing values.
        """
        for tag in other.values:
            self._insert_tag(tag, truncate=truncate)
