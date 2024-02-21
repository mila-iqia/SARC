from copy import deepcopy
from datetime import date

import pandas as pd

_membership_types = [
    # Student
    [
        "Research intern",
        "",
    ],
    # Prof
    [
        "Permanent HQP",
        "Visiting Researcher",
        "Collaborating Researcher",
    ],
]
_affiliation_types = [
    # Prof
    [
        "Collaborating Alumni",
        "Collaborating researcher",
        "HQP - Postdoctorate",
        "visiting researcher",
        "",
    ],
    # Student
    [
        "HQP - DESS",
        "HQP - Master's Research",
        "HQP - PhD",
        "HQP - Professional Master's",
        "HQP - Undergraduate",
        "Research Intern",
        "",
    ],
]
_supervisors = [
    # Student
    [f"John Smith{i:03d}" for i in range(5)],
    # Prof
    [""],
]
_current_university_title = [
    # Student
    [
        "",
    ],
    # Prof
    [
        "Canada Research Chair (Tier 2) and Assistant Professor",
        "Assistant Professor, School of Computer Science",
        "Professeur sous octrois agrégé / Associate Research Professor",
        "",
    ],
]


mymila_template = {
    "Profile Type": "",
    "Applicant Type": "",
    "internal id": "",
    "Mila Number": "",
    "Membership Type": "",
    "Affiliation type": "",
    "Assistant email": "",
    "Preferred email": "",
    "Faculty affiliated": "",
    "Department affiliated": "",
    "ID affiliated": "",
    "Affiliated university 2": "",
    "Second affiliated university": "",
    "Affiliated university 3": "",
    "Third affiliated university": "",
    "Program of study": "",
    "GitHub username": "",
    "Google Scholar profile": "",
    "Cluster access": "",
    "Access privileges": "",
    "Status": "",
    "Membership Type.1": "",
    "Affiliation type.1": "",
    "Last Name": "",
    "First Name": "",
    "Preferred First Name": "",
    "Email": "email",
    "Supervisor Principal": "",
    "Co-Supervisor": "",
    "Start date of studies": "",
    "End date of studies": "",
    "Start date of visit-internship": "",
    "End date of visit-internship": "",
    "Affiliated university": "",
    "Current university title": "",
    "Start date of academic nomination": "",
    "End date of academic nomination": "",
    "Alliance-DRAC account": "",
    "MILA Email": "email",
    "Start Date with MILA": "",
    "End Date with MILA": "",
    "Type of membership": "",
}


def dictset(dictionnary: dict, operation: dict):
    result = deepcopy(dictionnary)

    for path, value in operation.items():
        current = result
        frags = path.split(".")

        for frag in frags[:-1]:
            current = current.setdefault(frag, dict())

        current[frags[-1]] = value

    return result


def fake_mymila_data(nbr_users=10, nbr_profs=5):
    entry_ctor = mymila_entry_builder(nbr_profs)

    return pd.DataFrame(list([entry_ctor(i) for i in range(nbr_users)]))


def fake_mymila_data_with_history(nbr_users=10, nbr_profs=5):
    pass


def mymila_entry_builder(nbr_profs=5):
    """
    Return a deterministically-generated list of fake MyMila users just as
    they would be returned by the function `load_mymila` (yet to be developped).
    This is used for mocking the reading of a CSV file, since we don't expect
    being able to read directly from the database itself in the short term.

    Records must have some matching points with the fake LDAP data, to allow
    for matching to be tested.

    Returns a list of dictionaries, easy to convert to a dataframe.
    """

    faculty_affiliated = [
        "Computer Science and Operations Research",
        "Electrical and Computer Engineering",
        "Mathematics and Statistics",
        "Physics",
        "Psychology",
        "Other",
        "",
    ]
    program_of_study = [
        "Computer Science",
        "Doctorat en Informatique",
        "",
    ]
    status = [
        "Active",
        "Inactive",
        "",
    ]
    affiliated_university = [
        "McGill",
        "UdeM",
        "Samsung SAIT",
        "",
    ]

    # by convention, first 'nbr_profs' names will be professors and the rest students
    def mymila_entry(i: int):
        # 2 different types of entries: prof and student
        is_prof = i < nbr_profs

        membership_types = _membership_types[int(is_prof)]
        affiliation_types = _affiliation_types[int(is_prof)]
        supervisors = _supervisors[int(is_prof)]
        current_university_title = _current_university_title[int(is_prof)]
        uni_title = current_university_title[i % len(current_university_title)]
        first_name = "John"
        last_name = f"Smith{i:03d}"
        email = f"john.smith{i:03d}@mila.quebec"

        return dictset(
            mymila_template,
            {
                "Status": status[i % len(status)],
                "Last Name": first_name,
                "First Name": last_name,
                "Preferred First Name": first_name,
                "MILA Email": email,
                "Start Date with MILA": date(2022, 1, 1),
                "End Date with MILA": [date(2027, 12, 31), None][i % 2],
                "Supervisor Principal": supervisors[i % len(supervisors)],
                "Co-Supervisor": supervisors[(i + 1) % len(supervisors)],
                # Optional
                "Membership Type": membership_types[i % len(membership_types)],
                "Affiliation type": affiliation_types[i % len(affiliation_types)],
                "Faculty affiliated": faculty_affiliated[i % len(faculty_affiliated)],
                "Program of study": program_of_study[i % len(program_of_study)],
                "Start date of studies": date(year=2022, month=1, day=1),
                "End date of studies": date(year=2027, month=12, day=31),
                "Affiliated university": affiliated_university[
                    i % len(affiliated_university)
                ],
                "Current university title": uni_title,
                "Start date of academic nomination": date(2022, 1, 1),
                "End date of academic nomination": [date(2027, 12, 31), None][i % 2],
                "Email": email,
            },
        )

    return mymila_entry


def fake_member_of(index, count):
    member_of_config = {
        # Core prof
        0: ["cn=mila-core-profs,ou=Groups,dc=mila,dc=quebec"],
        # Student
        1: [
            "cn=mcgill-students,ou=Groups,dc=mila,dc=quebec",
            "cn=supervisor000-students,ou=Groups,dc=mila,dc=quebec",
        ],
        # Not core prof, not student
        2: [],
    }
    return member_of_config.get(index, [])


def fake_raw_ldap_data(nbr_users=10):
    """
    Return a deterministically-generated list of fake LDAP users just as
    they would be returned by the function `query_ldap`.
    This is used for mocking the LDAP server.
    """
    return list(
        [
            {
                "apple-generateduid": ["AF54098F-29AE-990A-B1AC-F63F5A89B89"],
                "cn": [f"john.smith{i:03d}", f"John Smith{i:03d}"],
                "departmentNumber": [],
                "displayName": [f"John Smith the {i:03d}rd"],
                "employeeNumber": [],
                "employeeType": [],
                "gecos": [""],
                "gidNumber": [str(1500000001 + i)],
                "givenName": ["John"],
                "googleUid": [f"john.smith{i:03d}"],
                "homeDirectory": [f"/home/john.smith{i:03d}"],
                "loginShell": ["/bin/bash"],
                "mail": [f"john.smith{i:03d}@mila.quebec"],
                "memberOf": fake_member_of(i, nbr_users),
                "objectClass": [
                    "top",
                    "person",
                    "organizationalPerson",
                    "inetOrgPerson",
                    "posixAccount",
                ],
                "physicalDeliveryOfficeName": [],
                "posixUid": [f"smithj{i:03d}"],
                "sn": [f"Smith {i:03d}"],
                "suspended": ["false"],
                "telephoneNumber": [],
                "title": [],
                "uid": [f"john.smith{i:03d}"],
                "uidNumber": [str(1500000001 + i)],
            }
            for i in range(nbr_users)
        ]
    )
