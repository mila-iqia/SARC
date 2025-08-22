from copy import deepcopy
from datetime import date
from typing import Any, Callable

from sarc.users.mymila import Headers

MYMILA_HEADERS = (
    "Affiliated_university",
    "Affiliation_type",
    "Alliance-DRAC_account",
    "Co-Supervisor_Membership_Type",
    "Co-Supervisor__MEMBER_NAME_",
    "Co-Supervisor__MEMBER_NUM_",
    "Department_affiliated",
    "End_date_of_academic_nomination",
    "End_date_of_studies",
    "End_date_of_visit-internship",
    "Faculty_affiliated",
    "First_Name",
    "GitHub_username",
    "Google_Scholar_profile",
    "Last_Name",
    "MILA_Email",
    "Membership_Type",
    "Mila_Number",
    "Preferred_First_Name",
    "Profile_Type",
    "Start_Date_with_MILA",
    "Start_date_of_academic_nomination",
    "Start_date_of_studies",
    "Start_date_of_visit-internship",
    "End_Date_with_MILA",
    "Status",
    "Supervisor_Principal_Membership_Type",
    "Supervisor_Principal__MEMBER_NAME_",
    "Supervisor_Principal__MEMBER_NUM_",
    "internal_id",
    "Co-Supervisor_CCAI_Chair_CIFAR",
    "Supervisor_Principal_CCAI_Chair_CIFAR",
    "CCAI_Chair_CIFAR",
    "_MEMBER_NUM_",
)


_membership_types: list[list[str]] = [
    # Student
    [
        "Permanent HQP",
        "Collaborating Researcher",
    ],
    # Prof
    [
        "Associate academic member",
        "Core Academic Member",
    ],
]
_affiliation_types: list[list[Any]] = [
    # Student
    [
        "HQP - DESS",
        "HQP - Master's Research",
        "HQP - PhD",
        "HQP - Professional Master's",
        "HQP - Undergraduate",
        "Research Intern",
        None,
    ],
    # Prof
    [
        "Core Academic Member",
        "Associate academic member",
    ],
]


def dictset(dictionnary: dict, operation: dict) -> dict:
    result = deepcopy(dictionnary)

    for path, value in operation.items():
        current = result
        frags = path.split(".")

        for frag in frags[:-1]:
            current = current.setdefault(frag, dict())

        current[frags[-1]] = value

    return result


def fake_mymila_data(
    nbr_users: int = 10,
    nbr_profs: int = 5,
    hardcoded_values_by_user: dict[int, dict[str, Any]] = {},
) -> tuple[list[tuple], tuple]:
    records: list[tuple] = []
    headers = MYMILA_HEADERS
    entry_ctor = mymila_entry_builder(records, nbr_profs, hardcoded_values_by_user)
    for i in range(nbr_users):
        records.append(entry_ctor(i))
    return records, headers


def mymila_entry_builder(
    records: list[tuple],
    nbr_profs: int,
    hardcoded_values_by_user: dict[int, dict[str, Any]],
) -> Callable[[int], tuple]:
    """
    Return a deterministically-generated list of fake MyMila users just as
    they would be returned by the function `_query_mymila`.
    """

    faculty_affiliated = [
        "Computer Science and Operations Research",
        "Electrical and Computer Engineering",
        "Mathematics and Statistics",
        "Physics",
    ]
    departement_affiliated = [
        "Informatique et de recherche opérationnelle",
        "Mathématique",
    ]
    program_of_study = [
        "Computer Science",
        "Doctorat en Informatique",
        None,
    ]
    affiliated_university = [
        "McGill",
        "UdeM",
        "Samsung SAIT",
        None,
    ]

    # first 'nbr_profs' names will be professors and the rest students
    def mymila_entry(i: int) -> tuple:
        is_prof = i < nbr_profs
        is_employee = i == nbr_profs
        is_applicant = i % 5 == 0 and not is_employee
        is_inactive = i % 5 == 4
        drac_account = [f"abc-{i:03d}", f"abc-{i:03d}-01", "test123", None][i % 4]

        supervisor = None if is_prof or is_employee else i % nbr_profs
        co_supervisor = None if is_prof or is_employee or i % 3 else (i + 1) % nbr_profs

        membership_types = _membership_types[int(is_prof)]
        affiliation_types = _affiliation_types[int(is_prof)]
        first_name = "John"
        last_name = f"Smith{i:03d}"
        email = f"john.smith{i:03d}@mila.quebec"
        # Theses are all value formats that i've seen in the real data even if it doesn't really make sense
        github_usernames = [
            f"jsmith{i:d}",
            None,
            f"https://github.com/jsmith{i:d}",
            f"john.smith{i:d}@example.com",
        ]
        scholar_profiles = [
            f"https://scholar.google.com/citations?user=PataTe_{i:03d}AJ&hl=en",
            "https://portal.mila.quebec/site/forms/prof-candidate",
            f"https://bit.ly/john-smith-{i:d}-googlescholar",
            None,
        ]

        def _define_value[T](i: int, key: str, default_value: T) -> T:
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

        return (
            _define_value(
                i,
                "Affiliated_university",
                None
                if is_applicant or is_inactive or is_employee
                else affiliated_university[i % len(affiliated_university)],
            ),
            _define_value(
                i,
                "Affiliation_type",
                None
                if is_applicant or is_employee
                else affiliation_types[i % len(affiliation_types)],
            ),
            _define_value(
                i, "Alliance-DRAC_account", None if is_employee else drac_account
            ),
            # These are not _define_values because they depend on the id
            (
                None
                if co_supervisor is None
                else records[co_supervisor][Headers.Membership_Type]
            ),  # "Co-Supervisor_Membership_Type"
            (
                None
                if co_supervisor is None
                else (
                    records[co_supervisor][Headers.Last_Name]
                    + " "
                    + (
                        records[co_supervisor][Headers.Preferred_First_Name]
                        or records[co_supervisor][Headers.First_Name]
                    )
                )
            ),  # "Co-Supervisor__MEMBER_NAME_"
            _define_value(i, "Co-Supervisor__MEMBER_NUM_", co_supervisor),
            _define_value(
                i,
                "Department_affiliated",
                None
                if is_employee or (is_applicant and not is_prof)
                else departement_affiliated[i % len(departement_affiliated)],
            ),
            _define_value(i, "End_date_of_academic_nomination", None),
            _define_value(
                i,
                "End_date_of_studies",
                None if is_employee or is_prof or i % 3 != 0 else date(2026, 5, 31),
            ),
            _define_value(i, "End_date_of_visit-internship", None),
            _define_value(
                i,
                "Faculty_affiliated",
                None
                if is_employee or (is_applicant and not is_prof) or is_inactive
                else faculty_affiliated[i % len(faculty_affiliated)],
            ),
            _define_value(i, "First_Name", first_name),
            _define_value(
                i,
                "GitHub_username",
                None if is_employee else github_usernames[i % len(github_usernames)],
            ),
            _define_value(
                i,
                "Google_Scholar_profile",
                None if is_employee else scholar_profiles[i % len(scholar_profiles)],
            ),
            _define_value(i, "Last_Name", last_name),
            _define_value(i, "MILA_Email", email),
            _define_value(
                i,
                "Membership_Type",
                None
                if is_employee or is_applicant
                else membership_types[i % len(membership_types)],
            ),
            _define_value(
                i,
                "Mila_Number",
                None
                if is_employee or is_applicant
                else (f"PR-{i:04d}" if is_prof else f"ST-{i:04d}"),
            ),
            _define_value(i, "Preferred_First_Name", None if i % 2 else "Jane"),
            _define_value(
                i,
                "Profile_Type",
                "Professor" if is_prof else ("Employee" if is_employee else "Student"),
            ),
            _define_value(
                i,
                "Start_Date_with_MILA",
                None if is_employee or is_applicant else date(2023, 9, 1),
            ),
            _define_value(
                i,
                "Start_date_of_academic_nomination",
                None
                if not is_prof or is_employee or is_inactive
                else date(2022, 4, 20),
            ),
            _define_value(
                i,
                "Start_date_of_studies",
                None if is_employee or is_prof or i % 3 != 0 else date(2023, 8, 1),
            ),
            _define_value(i, "Start_date_of_visit-internship", None),
            _define_value(
                i,
                "End_Date_with_MILA",
                None
                if is_employee or is_applicant
                else (date(2024, 9, 1) if is_inactive else date(2025, 9, 1)),
            ),
            _define_value(
                i,
                "Status",
                "Applicant"
                if is_applicant
                else ("Inactive" if is_inactive else "Active"),
            ),
            (
                None
                if supervisor is None
                else records[supervisor][Headers.Membership_Type]
            ),  # "Supervisor_Principal_Membership_Type"
            (
                None
                if supervisor is None
                else (
                    records[supervisor][Headers.Last_Name]
                    + " "
                    + (
                        records[supervisor][Headers.Preferred_First_Name]
                        or records[supervisor][Headers.First_Name]
                    )
                )
            ),  # "Supervisor_Principal__MEMBER_NAME_"
            _define_value(i, "Supervisor_Principal__MEMBER_NUM_", supervisor),
            _define_value(i, "internal_id", str(i)),
            (
                None
                if co_supervisor is None
                else records[co_supervisor][Headers.CCAI_Chair_CIFAR]
            ),  # "Co-Supervisor_CCAI_Chair_CIFAR"
            (
                None
                if supervisor is None
                else records[supervisor][Headers.CCAI_Chair_CIFAR]
            ),  # "Supervisor_Principal_CCAI_Chair_CIFAR"
            _define_value(
                i,
                "CCAI_Chair_CIFAR",
                None if (not is_prof) or is_applicant else ["Yes", "No"][i % 2],
            ),
            _define_value(i, "_MEMBER_NUM_", i),
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


def define_value[T](
    i: int,
    key: str,
    default_value: T,
    hardcoded_values_by_user: dict[int, dict[str, T | Any]],
) -> T:
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
