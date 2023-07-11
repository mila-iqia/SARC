from collections import namedtuple

import sarc.ldap.read_mila_ldap
from sarc.ldap.read_mila_ldap import run, resolve_supervisors
from sarc.ldap.supervisor import extract_supervisors


class CollectionMock:
    def __init__(self) -> None:
        self.documents = []
        
    def find(self, *args, **kwargs):
        return []
    
    def bulk_write(self, write_ops):
        self.documents = write_ops
        return namedtuple("Result", ["bulk_api_result"])(len(write_ops))


def ldap_mock(*args, **kwargs):
    return [
        {
            "mail": ["student@email.com"],
            "memberOf": [
                "cn=mcgill-students,ou=Groups,dc=mila,dc=quebec",
                "cn=co.supervisor-students,ou=Groups,dc=mila,dc=quebec",
                "cn=supervisor-students,ou=Groups,dc=mila,dc=quebec",
            ],
            "suspended": ["false"],
            "posixUid": ["student"],
            "uidNumber": ["student"],
            "gidNumber": ["student"],
            "displayName": ["student"],
            "googleUid": ["student"],
            "uid": ["student"]
        },
        {
            "mail": ["supervisor@email.com"],
            "memberOf": [
                "cn=mila-core-profs,ou=Groups,dc=mila,dc=quebec",
            ],
            "suspended": ["false"],
            "posixUid": ["supervisor"],
            "uidNumber": ["supervisor"],
            "gidNumber": ["supervisor"],
            "displayName": ["supervisor"],
            "googleUid": ["supervisor"],
            "uid": ["supervisor"],
        },
        {
            "mail": ["co.supervisor@email.com"],
            "memberOf": [
            ],
            "suspended": ["false"],
            "posixUid": ["cosupervisor"],
            "uidNumber": ["cosupervisor"],
            "gidNumber": ["cosupervisor"],
            "displayName": ["cosupervisor"],
            "googleUid": ["co.supervisor"],
            "uid": ["co.supervisor"],
        }
    ]
    

def group_to_prof():
    return  {
        "supervisor": "supervisor@email.com",
        "co.supervisor": "co.supervisor@email.com",
    }


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


def test_extract_supervisors_core():
    ldap_people = ldap_mock()
    
    supervisors, groups, university, is_student, is_core = extract_supervisors(
        ldap_people[1]["memberOf"]
    )
    
    assert university is None
    assert is_student is False
    assert is_core is True
    
    # The supervisors are extracted as is and not yet sorted
    assert supervisors == []
    assert groups == ['mila-core-profs']


def test_extract_supervisors_notcode():
    ldap_people = ldap_mock()
    
    supervisors, groups, university, is_student, is_core = extract_supervisors(
        ldap_people[2]["memberOf"]
    )
    
    assert university is None
    assert is_student is False
    assert is_core is False
    assert supervisors == []
    assert groups == []


def test_resolve_supervisors():
    
    ldap_people = ldap_mock()
    
    errors = resolve_supervisors(
        ldap_people,
        group_to_prof(),
        exceptions=None
    )
    
    assert errors.has_errors() is False
    
    # The supervisors got sorted 
    assert ldap_people[0]['supervisor'] == "supervisor@email.com"
    assert ldap_people[0]['co_supervisor'] == "co.supervisor@email.com"


def test_sync(monkeypatch):
    monkeypatch.setattr(sarc.ldap.read_mila_ldap, "_query_and_dump", ldap_mock)
    collection = CollectionMock()
    
    run(
        ldap=None, 
        mongodb_collection=collection,
        group_to_prof=group_to_prof(),
        exceptions=None
    )
    
    docs = collection.documents
    
    student = docs[0]._doc['$set']['mila_ldap']
    assert student['supervisor'] == "supervisor@email.com"
    assert student['co_supervisor'] == "co.supervisor@email.com"
    