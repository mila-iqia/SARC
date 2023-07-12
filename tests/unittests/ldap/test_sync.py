from collections import namedtuple

import sarc.ldap.read_mila_ldap
from sarc.ldap.read_mila_ldap import resolve_supervisors, run
from sarc.ldap.supervisor import _student_or_prof, extract_supervisors


class CollectionMock:
    def __init__(self) -> None:
        self.documents = []

    def find(self, *args, **kwargs):
        return []

    def bulk_write(self, write_ops):
        self.documents = write_ops
        return namedtuple("Result", ["bulk_api_result"])(len(write_ops))


def make_person(name, suspended=False):
    return {
        "mail": [f"{name}@email.com"],
        "memberOf": [],
        "suspended": ["true" if suspended else "false"],
        "posixUid": [f"{name}"],
        "uidNumber": [f"{name}"],
        "gidNumber": [f"{name}"],
        "displayName": [f"{name}"],
        "googleUid": [f"{name}"],
        "uid": [f"{name}"],
        "givenName": [f"{name}"],
        "sn": [f"{name}"],
    }


def make_core(name):
    person = make_person(name, False)
    person["memberOf"] = ["cn=mila-core-profs,ou=Groups,dc=mila,dc=quebec"]
    return person


def make_student(name, supervisors, suspended=False):
    person = make_person(name, suspended)
    members = []
    if supervisors:
        for s in supervisors:
            members.append(f"cn={s}-students,ou=Groups,dc=mila,dc=quebec")

    person["memberOf"] = members
    return person


def ldap_mock(*args, **kwargs):
    return [
        make_student("good", ["mcgill", "co.supervisor", "supervisor"]),
        make_person("co.supervisor"),
        make_core("supervisor"),
    ]


def ldap_mock_no_supervisor(*args, **kwargs):
    return [
        make_student("good", ["mcgill"]),
        make_person("co.supervisor"),
        make_core("supervisor"),
    ]


def ldap_mock_too_many_supervisor(*args, **kwargs):
    return [
        make_student("good", ["mcgill", "co.supervisor", "supervisor", "metoo"]),
        make_person("co.supervisor"),
        make_core("supervisor"),
        make_core("metoo"),
    ]


def ldap_mock_nocore_supervisor(*args, **kwargs):
    return [
        make_student("good", ["mcgill", "co.supervisor", "supervisor"]),
        make_person("co.supervisor"),
        make_person("supervisor"),
    ]


def ldap_mock_missing_supervisor(*args, **kwargs):
    """Supervisor is not in LDAP"""
    return [
        make_student("good", ["mcgill", "co.supervisor", "supervisor"]),
        make_person("co.supervisor"),
    ]


def ldap_mock_missing_supervisor_mapping(*args, **kwargs):
    """Cannot find supervisor from group; missing mapping"""
    return [
        make_student("good", ["mcgill", "co.supervisor", "supervisoR"]),
        make_person("co.supervisor"),
        make_core("supervisor"),
    ]


def group_to_prof(*args):
    return {
        "supervisor": "supervisor@email.com",
        "co.supervisor": "co.supervisor@email.com",
    }


def ldap_exception(*args):
    return {}


def test_extract_supervisors_student_no_supervisor():
    supervisors, groups, university, is_student, is_core = extract_supervisors(
        make_student("ok", ["mcgill"])["memberOf"]
    )

    assert university == "mcgill"
    assert is_student is True
    assert is_core is False

    # The supervisors are extracted as is and not yet sorted
    assert supervisors == []
    assert groups == []


def test_extract_supervisors_student():
    ldap_people = ldap_mock()

    supervisors, groups, university, is_student, is_core = extract_supervisors(
        ldap_people[0]["memberOf"]
    )

    assert university == "mcgill"
    assert is_student is True
    assert is_core is False

    # The supervisors are extracted as is and not yet sorted
    assert supervisors == ["co.supervisor", "supervisor"]
    assert groups == []


def test_extract_supervisors_not_core():
    ldap_people = ldap_mock()

    supervisors, groups, university, is_student, is_core = extract_supervisors(
        ldap_people[1]["memberOf"]
    )

    assert university is None
    assert is_student is False
    assert is_core is False

    # The supervisors are extracted as is and not yet sorted
    assert supervisors == []
    assert groups == []


def test_extract_supervisors():
    ldap_people = ldap_mock()

    supervisors, groups, university, is_student, is_core = extract_supervisors(
        ldap_people[2]["memberOf"]
    )

    assert university is None
    assert is_student is False
    assert is_core is True
    assert supervisors == []
    assert groups == ["mila-core-profs"]


def test_resolve_supervisors():
    ldap_people = ldap_mock()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    assert errors.has_errors() is False

    # The supervisors got sorted
    assert ldap_people[0]["supervisor"] == "supervisor@email.com"
    assert ldap_people[0]["co_supervisor"] == "co.supervisor@email.com"


def test_resolve_no_supervisors():
    ldap_people = ldap_mock_no_supervisor()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    errors.show()

    assert errors.has_errors() is True
    assert len(errors.no_supervisors) == 1


def test_resolve_too_many_supervisors():
    ldap_people = ldap_mock_too_many_supervisor()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    errors.show()
    assert errors.has_errors() is True
    assert len(errors.too_many_supervisors) == 1


def test_resolve_nocore_supervisors():
    ldap_people = ldap_mock_nocore_supervisor()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    errors.show()
    assert errors.has_errors() is True
    assert len(errors.no_core_supervisors) == 1


def test_resolve_missing_supervisors():
    ldap_people = ldap_mock_missing_supervisor()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    errors.show()
    assert errors.has_errors() is True
    assert len(errors.unknown_supervisors) == 1


def test_resolve_missing_supervisors_mapping():
    ldap_people = ldap_mock_missing_supervisor_mapping()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    errors.show()
    assert errors.has_errors() is True
    assert len(errors.unknown_group) == 1


def test_resolve_supervisors():
    ldap_people = ldap_mock()

    errors = resolve_supervisors(ldap_people, group_to_prof(), exceptions=None)

    assert errors.has_errors() is False

    # The supervisors got sorted
    assert ldap_people[0]["supervisor"] == "supervisor@email.com"
    assert ldap_people[0]["co_supervisor"] == "co.supervisor@email.com"


def test_student_or_prof_exception_rename_student():
    result = _student_or_prof(
        make_student("good", ["mcgill"]),
        dict(),
        exceptions=dict(
            not_student=[], not_teacher=[], rename={"good@email.com": "newName"}
        ),
    )
    assert result.ldap["givenName"][0] == "newName"


def test_student_or_prof_exception_student_is_prof():
    result = _student_or_prof(
        make_student("good", ["mcgill"]),
        dict(),
        exceptions=dict(not_student=["good@email.com"], not_teacher=[], rename=dict()),
    )
    assert result.is_prof is True


def test_student_or_prof_exception_prof_is_student():
    result = _student_or_prof(
        make_person("good", False),
        dict(),
        exceptions=dict(not_student=[], not_teacher=["good@email.com"], rename=dict()),
    )
    assert result is not None
    assert result.is_prof is False


def test_ldap_simple_sync(monkeypatch):
    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "_query_and_dump", ldap_mock)
    monkeypatch.setattr(
        sarc.ldap.read_mila_ldap, "load_ldap_exceptions", ldap_exception
    )
    monkeypatch.setattr(
        sarc.ldap.read_mila_ldap, "load_group_to_prof_mapping", group_to_prof
    )

    collection = CollectionMock()

    run(
        ldap=None,
        mongodb_collection=collection,
    )

    docs = collection.documents

    for d in docs:
        if d._doc["$set"]["mila_ldap"]["mila_email_username"].startswith("good"):
            break

    student = d._doc["$set"]["mila_ldap"]
    assert student["supervisor"] == "supervisor@email.com"
    assert student["co_supervisor"] == "co.supervisor@email.com"
