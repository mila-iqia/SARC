import re
from dataclasses import dataclass, field
from itertools import chain

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


def extract_groups(member_of: list[str]):
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
    supervisors: list
    cn_groups: list
    university: str


def _student_or_prof(person: dict, S_profs: set[str], exceptions: dict) -> Result:
    if exceptions is None:
        exceptions = {}

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
            cn_groups=set(cn_groups),
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
    no_supervisors: list = field(default_factory=list)
    too_many_supervisors: list = field(default_factory=list)
    unknown_supervisors: list = field(default_factory=list)
    unknown_group: list = field(default_factory=list)
    prof_and_student: list = field(default_factory=list)

    def errors(self):
        return chain(
            self.no_supervisors,
            self.too_many_supervisors,
            self.unknown_supervisors,
            self.unknown_group,
            self.prof_and_student,
        )

    def error_count(self):
        return len(list(self.errors()))

    def has_errors(self):
        return self.error_count() > 0

    def show(self):
        def make_list(errors):
            return [person.ldap["mail"][0] for person in errors]

        def show_error(msg, array):
            if len(array) > 0:
                print(f"{msg} {make_list(array)}")

        show_error("     Missing supervisors:", self.no_supervisors)
        show_error("    Too many supervisors:", self.too_many_supervisors)
        show_error("        Prof and Student:", self.prof_and_student)

        if self.unknown_supervisors:
            print(f"     Unknown supervisors: {self.unknown_supervisors}")

        if self.unknown_group:
            print(f"           Unknown group: {self.unknown_group}")


def _extract_supervisors_from_groups(
    person: Result, group_to_prof: dict, errors: SupervisorMatchingErrors, index: dict
) -> list:
    supervisors = []

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
    def sortkey(x):
        person = index.get(x)
        if person:
            return int(person.is_core)
        return 0

    return sorted(supervisors, key=sortkey, reverse=True)


def resolve_supervisors(
    ldap_people: list[dict], group_to_prof: dict, exceptions: dict
) -> SupervisorMatchingErrors:
    index = {}
    people = []
    S_profs = set(group_to_prof.values())
    errors = SupervisorMatchingErrors()

    # Build the index for supervisor resolution
    for person in ldap_people:
        result = _student_or_prof(
            person,
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
        # if there is a supervisors override, use it whatever the student status may be
        if person.ldap["mail"][0] in exceptions.get("supervisors_overrides", []):
            supervisors = exceptions["supervisors_overrides"][person.ldap["mail"][0]]
            person.ldap["supervisor"] = None
            person.ldap["co_supervisor"] = None
            if len(supervisors) >= 1:
                person.ldap["supervisor"] = supervisors[0]
            else:
                person.ldap["supervisor"] = None
            if len(supervisors) >= 2:
                person.ldap["co_supervisor"] = supervisors[1]
            else:
                person.ldap["co_supervisor"] = None
        elif person.is_student:
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
