import re
from dataclasses import dataclass, field

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


def extract_supervisors(member_of):
    supervisors = []
    groups = []
    is_student = False
    is_core = False
    university = None

    for e in member_of:
        # "memberOf": [
        #     "cn=c.pal-students,ou=Groups,dc=mila,dc=quebec",
        #     "cn=clusterusers,ou=Groups,dc=mila,dc=quebec",
        #     "cn=d.nowrouzezahrai-students,ou=Groups,dc=mila,dc=quebec",
        #     "cn=edi.survey.students,ou=Groups,dc=mila,dc=quebec",
        #     "cn=mcgill-students,ou=Groups,dc=mila,dc=quebec",
        #     "cn=mila_acces_special,ou=Groups,dc=mila,dc=quebec",
        #     "cn=phd,ou=Groups,dc=mila,dc=quebec"
        # ],
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


def _student_or_prof(person, S_profs, exceptions):
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
    ) = extract_supervisors(person["memberOf"])

    if person["suspended"][0] == "true":
        return None

    if person["mail"][0] in exceptions.get("not_student", []):
        # For some reason, Christopher Pal and Yue Li are on their own students lists.
        # Mirco Ravanelli is an ex postdoc of Yoshua but appears to be an associate member now.
        # Let's make exceptions.
        is_student = False
        is_prof = True

    elif person["mail"][0] in exceptions.get("not_teacher", []):
        # Maxime Gasse is a postdoc with Andrea Lodi but also appears to co-supervise someone.
        is_prof = False
        is_student = True

    # Someone can't be prof AND student, apart with the two above exceptions.
    assert not (
        is_prof and is_student
    ), f"Person {person['givenName'][0]} {person['sn'][0]} is both a student and a prof."

    # because it's stupid to wait for the LDAP to be updated for that one
    prefered_name = exceptions.get("rename", {}).get(person["mail"][0])
    if prefered_name is not None:
        person["givenName"][0] = prefered_name

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

    if is_student:
        return Result(
            person,
            is_prof,
            is_core,
            is_student,
            cn_groups_of_supervisors,
            cn_groups,
            university,
        )

    return None


@dataclass
class SupervisorMatchingErrors:
    no_supervisors: list = field(default_factory=list)
    too_many_supervisors: list = field(default_factory=list)
    no_core_supervisors: list = field(default_factory=list)
    unknown_supervisors: list = field(default_factory=list)
    unknown_group: list = field(default_factory=list)

    def has_errors(self):
        return (
            len(self.no_supervisors) > 0
            or len(self.no_core_supervisors) > 0
            or len(self.too_many_supervisors) > 0
            or len(self.unknown_supervisors) > 0
            or len(self.unknown_group) > 0
        )

    def show(self):
        def make_list(errors):
            return [person.ldap["mail"][0] for person in errors]

        def show_error(msg, array):
            if len(array) > 0:
                print(f"{msg} {make_list(array)}")

        show_error("     Missing supervisors:", self.no_supervisors)
        show_error("Missing core supervisors:", self.no_core_supervisors)
        show_error("    Too many supervisors:", self.too_many_supervisors)

        if self.unknown_supervisors:
            print(f"     Unknown supervisors: {self.unknown_supervisors}")

        if self.unknown_group:
            print(f"           Unknown group: {self.unknown_group}")


def _extract_supervisors_from_groups(person, group_to_prof, errors, index):
    has_core_supervisor = False
    supervisors = []

    for group in person.supervisors:
        prof = group_to_prof.get(group)

        if prof is None:
            errors.unknown_group.append(group)
        else:
            p = index.get(prof)
            if p is None:
                errors.unknown_supervisors.append(prof)
            else:
                has_core_supervisor = has_core_supervisor or p.is_core

            supervisors.append(prof)

    if not has_core_supervisor:
        errors.no_core_supervisors.append(person)

    # We need to sort them, make the core prof index 0
    def sortkey(x):
        person = index.get(x)
        if person:
            return int(person.is_core)
        return 0

    return sorted(supervisors, key=sortkey, reverse=True)


def resolve_supervisors(ldap_people, group_to_prof, exceptions):
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

        index[result.ldap["mail"][0]] = result
        people.append(result)

    for person in people:
        if person.is_student:
            person.ldap["is_student"] = True

            supervisors = _extract_supervisors_from_groups(
                person, group_to_prof, errors, index
            )

            print(supervisors)

            if len(supervisors) == 0:
                person.ldap["supervisor"] = []
                errors.no_supervisors.append(person)

            elif len(supervisors) == 1:
                person.ldap["supervisor"] = supervisors[0]

            elif len(supervisors) == 2:
                person.ldap["supervisor"] = (
                    supervisors[0] if len(supervisors) >= 1 else None
                )
                person.ldap["co_supervisor"] = (
                    supervisors[1] if len(supervisors) > 1 else None
                )

            else:
                errors.too_many_supervisors.append(person)

    return errors
