from copy import deepcopy
from datetime import date

import pandas as pd

_profile_type = [
    # Student
    "Student",
    # Prof
    "Professor",
]
_membership_types = [
    # Student
    [
        "Collaborating Researcher",
        "Permanent HQP",
        "Research intern",
        "Visiting Researcher",
        "",
    ],
    # Prof
    [
        "Associate academic member",
        "Associate industry member",
        "Collaborating Researcher",
        "Core Academic Member",
        "Core industry member",
        "External affiliate member",
        "Permanent HQP",
        "Visiting Researcher",
        "",
    ],
]
_affiliation_types = [
    # Student
    [
        "Collaborating Alumni",
        "Collaborating researcher",
        "HQP - DESS",
        "HQP - Master's Research",
        "HQP - PhD",
        "HQP - Professional Master's",
        "HQP - Undergraduate",
        "Research Intern",
        "visiting researcher",
        "",
    ],
    # Prof
    [
        "Collaborating Alumni",
        "Collaborating researcher",
        "HQP - Postdoctorate",
        "visiting researcher",
        "",
    ],
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
    "in1touch_id": "100",
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


def fake_mymila_data(nbr_users=10, nbr_profs=5, hardcoded_values_by_user={}):
    entry_ctor = mymila_entry_builder(nbr_profs, hardcoded_values_by_user)
    return list([entry_ctor(i) for i in range(nbr_users)])


def fake_mymila_data_with_history(nbr_users=10, nbr_profs=5):
    pass


def mymila_entry_builder(nbr_profs=5, hardcoded_values_by_user={}):
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
        # Reduce de proportion of deactivated accounts (2/3)
        *(["Active"] * (nbr_profs - 3)),
    ]
    affiliated_university = [
        "McGill",
        "UdeM",
        "Samsung SAIT",
        "",
    ]
    _supervisors = [
        # Student
        [100 + i for i in range(nbr_profs)],
        # Prof
        [""],
    ]

    # by convention, first 'nbr_profs' names will be professors and the rest students
    def mymila_entry(i: int):
        # 2 different types of entries: prof and student
        is_prof = i < nbr_profs

        profile_type = _profile_type[int(is_prof)]
        membership_types = _membership_types[int(is_prof)]
        affiliation_types = _affiliation_types[int(is_prof)]
        supervisors = _supervisors[int(is_prof)]
        current_university_title = _current_university_title[int(is_prof)]
        uni_title = current_university_title[i % len(current_university_title)]
        first_name = "John"
        last_name = f"MM Smith{i:03d}"
        email = f"john.smith{i:03d}@mila.quebec"

        def fdate(year, month, day):
            return date(year, month, day).strftime("%Y-%m-%d")

        def _define_value(i, key, default_value):
            """
            Retrieve the hardcoded value to associate to a key for a user, if any,
            or use the default value given as parameter.

            Parameters:
                i               Index of the user in the list we want to generate
                key             The key of the element we want to check if there is a hardcoded value for
                default_value   The value to associate to the user and key if no hardcoded value
                                is associated to it
            Returns:
                The hardcoded value defined for the user, if any.
                The default value given as parameter otherwise.
            """
            return define_value(i, key, default_value, hardcoded_values_by_user)

        return dictset(
            mymila_template,
            {
                "Profile Type": _define_value(i, "Profile Type", profile_type),
                "Status": _define_value(i, "Status", status[i % len(status)]),
                "Last Name": _define_value(i, "Last Name", last_name),
                "First Name": _define_value(i, "First Name", first_name),
                "Preferred First Name": _define_value(
                    i, "Preferred First Name", first_name
                ),
                "MILA Email": _define_value(i, "MILA Email", email),
                "Start Date with MILA": _define_value(
                    i, "Start Date with MILA", fdate(2022, 1, 1)
                ),
                "End Date with MILA": _define_value(
                    i, "End Date with MILA", [fdate(2027, 12, 31), None][i % 2]
                ),
                "Supervisor Principal": _define_value(
                    i, "Supervisor Principal", supervisors[i % len(supervisors)]
                ),
                "Co-Supervisor": _define_value(
                    i, "Co-Supervisor", supervisors[(i + 1) % len(supervisors)]
                ),
                # Optional
                "Membership Type": _define_value(
                    i, "Membership Type", membership_types[i % len(membership_types)]
                ),
                "Affiliation type": _define_value(
                    i, "Affiliation type", affiliation_types[i % len(affiliation_types)]
                ),
                "Faculty affiliated": _define_value(
                    i,
                    "Faculty affiliated",
                    faculty_affiliated[i % len(faculty_affiliated)],
                ),
                "Program of study": _define_value(
                    i, "Program of study", program_of_study[i % len(program_of_study)]
                ),
                "Start date of studies": _define_value(
                    i, "Start date of studies", fdate(year=2022, month=1, day=1)
                ),
                "End date of studies": _define_value(
                    i, "End date of studies", fdate(year=2027, month=12, day=31)
                ),
                "Affiliated university": _define_value(
                    i,
                    "Affiliated university",
                    affiliated_university[i % len(affiliated_university)],
                ),
                "Current university title": _define_value(
                    i, "Current university title", uni_title
                ),
                "Start date of academic nomination": _define_value(
                    i, "Start date of academic nomination", fdate(2022, 1, 1)
                ),
                "End date of academic nomination": _define_value(
                    i,
                    "End date of academic nomination",
                    [fdate(2027, 12, 31), None][i % 2],
                ),
                "Email": _define_value(i, "Email", email),
                "in1touch_id": _define_value(i, "in1touch_id", i + 100),
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


def define_value(i, key, default_value, hardcoded_values_by_user):
    """ """

    return (
        hardcoded_values_by_user[i][key]
        if i in hardcoded_values_by_user and key in hardcoded_values_by_user[i]
        else default_value
    )


def fake_raw_ldap_data(nbr_users=10, hardcoded_values_by_user={}):
    """
    Return a deterministically-generated list of fake LDAP users just as
    they would be returned by the function `query_ldap`.
    This is used for mocking the LDAP server.

    Parameters:
        nbr_users                   The number of users we want to generate
        hardcoded_values_by_user    Dictionary associating the index of a user to
                                    a dictionary of the values we want to hardcode
    """

    def _define_value(i, key, default_value):
        """
        Retrieve the hardcoded value to associate to a key for a user, if any,
        or use the default value given as parameter.

        Parameters:
            i               Index of the user in the list we want to generate
            key             The key of the element we want to check if there is a hardcoded value for
            default_value   The value to associate to the user and key if no hardcoded value
                            is associated to it
        Returns:
            The hardcoded value defined for the user, if any.
            The default value given as parameter otherwise.
        """
        return define_value(i, key, default_value, hardcoded_values_by_user)

    return list(
        [
            {
                "apple-generateduid": _define_value(
                    i, "apple-generateduid", ["AF54098F-29AE-990A-B1AC-F63F5A89B89"]
                ),
                "cn": _define_value(
                    i, "cn", [f"john.smith{i:03d}", f"John Smith{i:03d}"]
                ),
                "departmentNumber": _define_value(i, "departmentNumber", []),
                "displayName": _define_value(
                    i, "displayName", [f"John Smith the {i:03d}rd"]
                ),
                "employeeNumber": _define_value(i, "employeeNumber", []),
                "employeeType": _define_value(i, "employeeType", []),
                "gecos": _define_value(i, "gecos", [""]),
                "gidNumber": _define_value(i, "gidNumber", [str(1500000001 + i)]),
                "givenName": _define_value(i, "givenName", ["John"]),
                "googleUid": _define_value(i, "googleUid", [f"john.smith{i:03d}"]),
                "homeDirectory": _define_value(
                    i, "homeDirectory", [f"/home/john.smith{i:03d}"]
                ),
                "loginShell": _define_value(i, "loginShell", ["/bin/bash"]),
                "mail": _define_value(i, "mail", [f"john.smith{i:03d}@mila.quebec"]),
                "memberOf": _define_value(i, "memberOf", fake_member_of(i, nbr_users)),
                "objectClass": _define_value(
                    i,
                    "objectClass",
                    [
                        "top",
                        "person",
                        "organizationalPerson",
                        "inetOrgPerson",
                        "posixAccount",
                    ],
                ),
                "physicalDeliveryOfficeName": _define_value(
                    i, "physicalDeliveryOfficeName", []
                ),
                "posixUid": _define_value(i, "posixUid", [f"smithj{i:03d}"]),
                "sn": _define_value(i, "sn", [f"Smith {i:03d}"]),
                "suspended": _define_value(i, "suspended", ["false"]),
                "telephoneNumber": _define_value(i, "telephoneNumber", []),
                "title": _define_value(i, "title", []),
                "uid": _define_value(i, "uid", [f"john.smith{i:03d}"]),
                "uidNumber": _define_value(i, "uidNumber", [str(1500000001 + i)]),
                "supervisor": _define_value(i, "supervisor", None),
                "co_supervisor": _define_value(i, "co_supervisor", None),
            }
            for i in range(nbr_users)
        ]
    )
