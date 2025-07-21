import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import chain

logger = logging.getLogger(__name__)

universities = {
    "mcgill",
    "udem",
    "poly",
    "ets",
    "concordia",
    "ulaval",
    "hec",
}


class MultipleSupervisor(Exception):
    pass


def extract_groups(
    member_of: list[str],
) -> tuple[list[str], list[str], str | None, bool, bool]:
    supervisors = []
    groups = []
    is_student = False
    is_core = False
    university = None

    for e in member_of:
        if m := re.match(r"^cn=(.+?)-students.*", e):
            if m.group(1) in universities:
                university = m.group(1)
                is_student = True
                continue

            supervisors.append(m.group(1))
            is_student = True
            continue

        if m := re.match(r"^cn=(.+?),.*", e):
            if m.group(1) in ["mila-core-profs", "mila-profs", "core-academic-member"]:
                is_core = True
                is_student = False

            groups.append(m.group(1))
            continue

    return supervisors, groups, university, is_student, is_core


@dataclass
class Result:
    ldap: dict
    is_prof: bool
    is_core: bool
    is_student: bool
    supervisors: list[str]
    cn_groups: list[str]
    university: str | None


def _student_or_prof(
    person: dict[str, list[str]], S_profs: set[str], exceptions: dict[str, list[str]]
) -> Result | None:
    # the most straightforward way to determine if a person is a prof,
    # because you can't trust the cn_groups "core-profs" where
    # the mila directors are also listed
    university = None
    is_prof = person["mail"][0] in S_profs
    (
        cn_groups_of_supervisors,
        cn_groups,
        university,
        is_student,
        is_core,
    ) = extract_groups(person["memberOf"])

    if person["suspended"][0] == "true":
        return None

    if person["mail"][0] in exceptions.get("not_student", []):
        is_student = False
        is_prof = True

    elif person["mail"][0] in exceptions.get("not_teacher", []):
        is_prof = False
        is_student = True

    if is_prof:
        return Result(
            person,
            supervisors=[],
            university=university,
            is_core=is_core,
            is_prof=is_prof,
            is_student=is_student,
            cn_groups=list(set(cn_groups)),
        )

    return Result(
        person,
        is_prof,
        is_core,
        is_student,
        cn_groups_of_supervisors,
        cn_groups,
        university,
    )


@dataclass
class SupervisorMatchingErrors:
    no_supervisors: list[Result] = field(default_factory=list)
    too_many_supervisors: list[Result] = field(default_factory=list)
    unknown_supervisors: list[str] = field(default_factory=list)
    unknown_group: list[str] = field(default_factory=list)
    prof_and_student: list[Result] = field(default_factory=list)

    def errors(self) -> Iterable[Result | str]:
        return chain(
            self.no_supervisors,
            self.too_many_supervisors,
            self.unknown_supervisors,
            self.unknown_group,
            self.prof_and_student,
        )

    def error_count(self) -> int:
        return len(list(self.errors()))

    def has_errors(self) -> bool:
        return self.error_count() > 0

    def show(self) -> None:
        def make_list(errors: list[Result]) -> list[str]:
            return [person.ldap["mail"][0] for person in errors]

        def show_error(msg: str, array: list[Result]) -> None:
            unique_values = sorted(set(make_list(array)))
            if len(unique_values) > 0:
                logger.error(f"{msg} {unique_values}")

        show_error("     Missing supervisors:", self.no_supervisors)
        show_error("    Too many supervisors:", self.too_many_supervisors)
        show_error("        Prof and Student:", self.prof_and_student)

        if self.unknown_supervisors:
            logger.warning(
                f"     Unknown supervisors: {sorted(set(self.unknown_supervisors))}"
            )

        if self.unknown_group:
            logger.warning(
                f"           Unknown group: {sorted(set(self.unknown_group))}"
            )


def _extract_supervisors_from_groups(
    person: Result,
    group_to_prof: dict[str, str],
    errors: SupervisorMatchingErrors,
    index: dict[str, Result],
) -> list[str]:
    supervisors: list[str] = []

    for group in person.supervisors:
        prof = group_to_prof.get(group)

        if prof is None:
            errors.unknown_group.append(group)
        else:
            p = index.get(prof)

            if p is None:
                errors.unknown_supervisors.append(prof)

            supervisors.append(prof)

    # We need to sort them, make the core prof index 0
    def sortkey(x: str) -> int:
        person = index.get(x)
        if person:
            return int(person.is_core)
        return 0

    return sorted(supervisors, key=sortkey, reverse=True)


# pylint: disable=too-many-branches
def resolve_supervisors(
    ldap_people: list[dict],
    group_to_prof: dict[str, str],
    exceptions: dict[str, list[str]],
) -> SupervisorMatchingErrors:
    index: dict[str, Result] = {}
    people: list[Result] = []
    S_profs = set(group_to_prof.values())
    errors = SupervisorMatchingErrors()

    # Build the index for supervisor resolution
    for ldap_person in ldap_people:
        result = _student_or_prof(
            ldap_person,
            S_profs,
            exceptions,
        )

        if result is None:
            continue

        if result.is_prof and result.is_student:
            errors.prof_and_student.append(result)
            continue

        index[result.ldap["mail"][0]] = result
        people.append(result)

    for person in people:
        if person.is_student:
            person.ldap["is_student"] = True

            supervisors = _extract_supervisors_from_groups(
                person, group_to_prof, errors, index
            )

            if len(supervisors) == 0:
                person.ldap["supervisor"] = []
                errors.no_supervisors.append(person)

            elif len(supervisors) == 1:
                person.ldap["supervisor"] = supervisors[0]

            elif len(supervisors) == 2:
                person.ldap["supervisor"] = supervisors[0]
                person.ldap["co_supervisor"] = supervisors[1]

            else:
                errors.too_many_supervisors.append(person)

    return errors
