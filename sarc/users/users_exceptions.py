from sarc.config import LDAPConfig
from sarc.users.read_mila_ldap import load_ldap_exceptions


def apply_users_delegation_exceptions(DD_persons, ldap_config: LDAPConfig, span):
    """
    Apply manual exceptions to users;
    these exceptions are defined in the exceptions.json file refered in the LDAPConfig.
    """
    span.add_event("Applying users delegation exceptions ...")
    # Load exceptions
    exceptions = load_ldap_exceptions(ldap_config)

    for _, user in DD_persons.items():
        if (
            exceptions
            and user["mila_ldap"]["mila_email_username"] in exceptions["delegations"]
        ):
            span.add_event(
                f"Applying delegation exception for {user['mila_ldap']['mila_email_username']} ..."
            )
            user["teacher_delegations"] = exceptions["delegations"][
                user["mila_ldap"]["mila_email_username"]
            ]


def apply_users_supervisor_exceptions(DD_persons, ldap_config: LDAPConfig, span):
    """
    Apply manual exceptions to users;
    these exceptions are defined in the exceptions.json file refered in the LDAPConfig.
    """
    span.add_event("Applying users supervisor exceptions ...")
    # Load exceptions
    exceptions = load_ldap_exceptions(ldap_config)

    for _, user in DD_persons.items():
        # if there is a supervisors override, use it whatever the student status may be
        if exceptions and user["mila_ldap"]["mila_email_username"] in exceptions.get(
            "supervisors_overrides", []
        ):
            span.add_event(
                f"Applying supervisor exception for {user['mila_ldap']['mila_email_username']} ..."
            )
            supervisors = exceptions["supervisors_overrides"][
                user["mila_ldap"]["mila_email_username"]
            ]
            user["mila_ldap"]["supervisor"] = None
            user["mila_ldap"]["co_supervisor"] = None
            if len(supervisors) >= 1:
                user["mila_ldap"]["supervisor"] = supervisors[0]
            else:
                user["mila_ldap"]["supervisor"] = None
            if len(supervisors) >= 2:
                user["mila_ldap"]["co_supervisor"] = supervisors[1]
            else:
                user["mila_ldap"]["co_supervisor"] = None
