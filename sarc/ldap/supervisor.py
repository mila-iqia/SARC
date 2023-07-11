from dataclasses import dataclass
import re

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
                continue

            supervisors.append(m.group(1))
            is_student = True
            continue

        if m := re.match(r"^cn=(.+?),.*", e):
            if m.group(1) in ["mila-core-profs", "mila-profs", "core-academic-member"]:
                 is_core = True
            
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


def _student_or_prof(person, group_to_prof, exceptions):
    if exceptions is None:
        exceptions = dict()
        
    # the most straightforward way to determine if a person is a prof,
    # because you can't trust the cn_groups "core-profs" where
    # the mila directors are also listed

    S_profs = set(group_to_prof.values())

    university = None
    is_prof = person["mail"][0] in S_profs
    
    cn_groups_of_supervisors, cn_groups, university, is_student, is_core = \
        extract_supervisors(
            person["memberOf"]
        )
        
    if person["suspended"][0] == "true":
        return None

    if person["mail"][0] in exceptions.get("not_student", []):
        # For some reason, Christopher Pal and Yue Li are on their own students lists.
        # Mirco Ravanelli is an ex postdoc of Yoshua but appears to be an associate member now.
        # Let's make exceptions.
        is_student = False

    elif person["mail"][0] in exceptions.get("not_teacher", []):
        # Maxime Gasse is a postdoc with Andrea Lodi but also appears to co-supervise someone.
        is_prof = False

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
        supervisors = [
            group_to_prof[prof_short_name]
            for prof_short_name in cn_groups_of_supervisors
        ]
        
        return Result(
            person,
            is_prof,
            is_core,
            is_student,
            supervisors,
            cn_groups,
            university,
        )
        

def resolve_supervisors(ldap_people, group_to_prof, exceptions):
    index = dict()
    people = []
    
    # Build the index for supervisor resolution
    for person in ldap_people:
        result = _student_or_prof(
            person,
            group_to_prof,
            exceptions,
        )
        
        if result is None:
            continue
        
        index[result.ldap["mail"][0]] = result
        people.append(result)


    no_supervisors = []
    
    for person in people:
        if person.is_student:
            if len(person.supervisors) == 0:
                no_supervisors.append(person)
                
            elif len(person.supervisors) == 1:
                person.ldap["supervisor"] = person.supervisors[0]
                
            # We need to sort them, make the core prof index 0
            elif len(person.supervisors) == 2:
                supervisors = list(
                    sorted(person.supervisors, key=lambda x: int(index[x].is_core), reverse=True)
                )
                person.ldap["supervisor"] = supervisors[0]
                person.ldap["co_supervisor"] = supervisors[1]
                
            else:
                raise MultipleSupervisor(person)
                
    return no_supervisors

