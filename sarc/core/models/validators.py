from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Callable, Self

from pydantic import BaseModel, Field, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

UTCOFFSET = timedelta(0)
START_TIME = datetime(year=2000, month=1, day=1, tzinfo=UTC)
END_TIME = datetime(year=3000, month=1, day=1, tzinfo=UTC)


@dataclass(frozen=True)
class DatetimeUTCValidator:
    def validate_tz_utc(self, value: datetime, handler: Callable):
        assert value.tzinfo is not None, "date is not tz-aware"
        assert value.utcoffset() == UTCOFFSET, "date is not in UTC timezone"

        return handler(value)

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_wrap_validator_function(
            self.validate_tz_utc, handler(source_type)
        )


type datetime_utc = Annotated[datetime, DatetimeUTCValidator()]


class ValidTag[V](BaseModel):
    value: V
    valid_start: datetime_utc
    valid_end: datetime_utc


class DateOverlapError(Exception):
    def __init__(self, tag1: ValidTag, tag2: ValidTag):
        super().__init__(
            f"""Overlapping validity with different values:
{tag1.value}: {tag1.valid_start} - {tag1.valid_end}
{tag2.value}: {tag2.valid_start} - {tag2.valid_end}"""
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
    last. We do not expect frequent changes to the values or to have a humongous
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
        exisiting values and the end bound of the most recent value is END_TIME,
        instead of an overlapping error, the bound of the most recent value will
        be adjusted to end at the start of the new value.
        """
        assert start is None or start.tzinfo is not None
        assert end is None or end.tzinfo is not None

        if start is not None and end is not None:
            assert start < end
        if start is None or start < START_TIME:
            start = START_TIME
        if end is None or end > END_TIME:
            end = END_TIME
        start = start.astimezone(UTC)
        end = end.astimezone(UTC)
        tag = ValidTag(value=value, valid_start=start, valid_end=end)
        self._insert_tag(tag, truncate=False)

    def _insert_tag(self, tag: ValidTag[V], truncate) -> None:
        """Insert a tag in its proper position, possibly modifying bounds around it."""
        if len(self.values) == 0:
            self.values.append(tag)
            return

        viter = iter(enumerate(self.values))
        i, ltag = next(viter)
        try:
            while tag.valid_end <= ltag.valid_start:
                i, ltag = next(viter)
        except StopIteration:
            # The new tag ends before the last tag starts so just add it at the end
            self.values.append(tag)
            return
        # Here we know that ltag is the first tag in the list to have any
        # possible overlap with tag. Since the list is kept in "most recent
        # first" order that means that any tag that is after ltag is fully
        # after tag too ("after" in time, not in list order).
        if tag.valid_start > ltag.valid_end:
            # The new tag is fully before ltag so we just insert it
            self.values.insert(i, tag)
        elif tag.valid_start == ltag.valid_end:
            # The new tag starts just after ltag so we merge/insert depending if
            # the value is the same
            if tag.value == ltag.value:
                ltag.valid_end = tag.valid_end
            else:
                self.values.insert(i, tag)
        elif tag.value == ltag.value:
            # The new tag overlaps ltag
            if tag.valid_start >= ltag.valid_start:
                # Since the values are the same and tag starts after ltag,
                # we can just merge into ltag and be done
                ltag.valid_end = max(ltag.valid_end, tag.valid_end)
            else:
                # tag starts before ltag so we need to check previous
                # tag(s). to do it, we merge ltag into tag, remove ltag from
                # the list and try to insert again. This can happen
                # recursively, but since we do not expect the list to grow
                # large, it is fine for now
                tag.valid_end = max(ltag.valid_end, tag.valid_end)
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
            i == 0 and ltag.valid_end == END_TIME and tag.valid_start > ltag.valid_start
        ):
            ltag.valid_end = tag.valid_start
            self.values.insert(i, tag)
        elif not truncate:
            # We have an overlap and the values differ
            raise DateOverlapError(tag, ltag)
        elif tag.valid_start >= ltag.valid_start:
            # If the tag starts after ltag, we can just clip its starting point
            tag.valid_start = ltag.valid_end
            # if that makes the tag start after its end, it means that it
            # fully overlaps with ltag and so we just drop it otherwise we
            # add the trucated tag.
            if tag.valid_start < tag.valid_end:
                self.values.insert(i, tag)
            ### from here we know that tag starts before ltag ###
        elif tag.valid_end < ltag.valid_end:
            # If tag ends within ltag, clip the end and retry
            tag.valid_end = ltag.valid_start
            self._insert_tag(tag, truncate=truncate)
        else:
            # tag fully overlaps ltag, so we split tag in two, add the part
            # that goes after ltag, and try again for the part that goes
            # before.
            tag2 = ValidTag(
                value=tag.value,
                valid_start=tag.valid_start,
                valid_end=ltag.valid_start,
            )
            tag.valid_start = ltag.valid_end
            self.values.insert(i, tag)
            self._insert_tag(tag2, truncate=truncate)

    def get_value(self, date: datetime | None = None) -> V:
        """Get the valid value at specified time.

        If date is None, we the the current time as the date.
        If there was no value as the specified date, raises DateMatchError.
        """
        assert date is None or date.tzinfo is not None

        if date is None:
            date = datetime.now(UTC)

        date = date.astimezone(UTC)

        for tag in self.values:
            if date >= tag.valid_start and date < tag.valid_end:
                return tag.value
        raise DateMatchError(date)

    def values_in_range(self, start: datetime_utc, end: datetime_utc) -> list[V]:
        """Get values in a range

        The range starts at `start` and ends just before `end`.  This means that
        start is included, but end is not.
        """
        res = list[V]()
        start = start.astimezone(UTC)
        end = end.astimezone(UTC)

        for tag in self.values:
            if not (end <= tag.valid_start or start >= tag.valid_end):
                res.append(tag.value)

        return res

    def merge_with(self, other: Self, truncate=False) -> None:
        """Insert all the values in other in self.

        If truncate=True, inserted values will have their validity periods
        reduced to fit around existing values.
        """
        for tag in other.values:
            self._insert_tag(tag, truncate=truncate)
