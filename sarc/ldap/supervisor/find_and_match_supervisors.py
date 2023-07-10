"""
This produces a list of all the students and their supervisors.

Some students have multiple supervisors, and some supervisors are not affiliated with Mila.
We refer to people with their mila email address, but we have to make exceptions
when profs are not affiliated with Mila.
"""

import csv
import json
import re
from datetime import datetime

from names_matching import find_exact_bag_of_words_matches

# hugo.Larochelle avec une majuscule?
# guillaume.dumas@mila.quebec avec aucun étudiant? je ne vois pas de g.dumas
# Mirko Ravanelli étant un étudiant de yoshua même s'il est membre académique?
# on a un étudiant de Will Hamilton mais lui-même n'est pas là
# rioult et g.rioult, mais pas de prof avec un nom de famille "rioult"
# a.tapp sans que Alain Tapp ne soit un prof
# On dirait que Brice Rauby est supervisé par Maxime Gasse, mais Maxime Gasse était un postdoc avec Andrea Lodi. Il n'est pas prof associé à Mila, mais Brice Rauby est encore son étudiant et il a un compte actif chez nous?
# w.hamilton 1
# g.rioult 1
# rioult 1
# Deux fois siamak
#    "siamak_" : "siamak.ravanbakhsh@mila.quebec" # ??
#    "s.ravanbakhsh" : "siamak.ravanbakhsh@mila.quebec",
# Christopher Pal fait partie de sa propre liste "c.pal-students"
# Quentin BERTRAND a un nom de famille en majuscules.
# JONAS NGNAWE
# DOHA, HWANG
# Yue Li is on her own mailing list y.li-students
# PerreaultLevasseur avec un nom de famille sans trait d'union
# AbbasgholizadehRahimi avec un nom de famille sans trait d'union
# qqch de bizarre avec Sarath Chandar dont "Sarath Chandar" semble être seulement le prénom,
#     mais le site du Mila suggère quasiment que c'est son prénom et son nom de famille
# EbrahimiKahou devrait être Ebrahimi-Kahou



def get_filename(filename):
    # FIXME resolve to the expected path
    import os 
    return os.path.join("/home/newton/work/SARC/secrets", filename)


def load_python_dict(file):
    # Maybe convert to json
    # note that this is a safe eval
    #
    # > The string or node provided may only consist of the following
    # > Python literal structures: strings, numbers, tuples, lists, dicts, booleans,
    # > and None.
    #
    import ast
    
    with open(file, 'r') as f:
        return ast.literal_eval(f.read())


#
#   Load Files
#

mapping_group_to_prof = load_python_dict(get_filename("group_to_prof.py"))

mapping_prof_mila_email_to_academic_email = \
    load_python_dict(get_filename("mapping_prof_mila_email_to_academic_email.py"))

mapping_academic_email_to_drac_info = {}

with open(get_filename("big_csv_str.csv"), "r") as file:
    big_csv_str = file.read()

with open(get_filename("not_students.csv"), "r") as f:
    not_sudents = set(f.read().split('\n'))

with open(get_filename("not_prof.csv"), "r") as f:
    not_prof = set(f.read().split('\n'))
    
with open(get_filename("rename.json"), "r") as f:
    rename = json.load(f)
    
#
#   ===
#
    
S_profs = {v for v in mapping_group_to_prof.values() if v is not None}

for e in big_csv_str.split("\n"):
    if len(e) > 10:
        academic_email, ccri, def_account = e.split(",")
        academic_email = academic_email.strip()
        if "deactivated" in ccri.lower() or "none" in ccri.lower():
            ccri = None
        if "none" in def_account.lower():
            def_account = None
        mapping_academic_email_to_drac_info[academic_email] = (ccri, def_account)


"""
"cn=core-academic-member,ou=Groups,dc=mila,dc=quebec",
            "cn=downscaling,ou=Groups,dc=mila,dc=quebec",
            "cn=drolnick-ml4climate,ou=Groups,dc=mila,dc=quebec",
            "cn=ecosystem-embeddings,ou=Groups,dc=mila,dc=quebec",
            "cn=internal,ou=Groups,dc=mila,dc=quebec",
            "cn=mila-core-profs,ou=Groups,dc=mila,dc=quebec",
            "cn=mila-profs,ou=Groups,dc=mila,dc=quebec",
"""

S_profs_cn_groups = {
    "mila-core-profs",
    "mila-profs",
    "core-academic-member",
    "core-industry-member",
    "associate-member-academic",
    "external-associate-members",
    "associate-member-industry",
}

# "aishwarya_lab" is basically the same as "a.agrawal" but it also contains
# "Yash" who works at Samsung downstairs.
# Since he's not a student, we don't want to have the "aishwarya_lab" mention
# in the students.
# There's 'ioannis.collab' and 'gidel.lab that I'm not sure about.
# "linclab_users" et "linclab" ?
S_cn_groups_to_ignore = {
    "deactivation-2022",
    "fall2022",
    "covid19-app-dev",
    "internal",
    "townhall",
    "aishwarya_lab",
    "edi.survey.students",
    "computer-vision-rg",
    "2fa-reminder",
    "overleaf_renewal",
    "gflownet",
    "neural-ai-rg",
    "winter2022-associate",
    "downscaling",
    "overleaf_renewal",
    "onboarding-cluster",
    "ecosystem-embeddings",
    "ccai",
    "covid19-helpers",
    "thisclimate",
    "vicc",
    "aiphysim_users",
}
# statcan.dialogue.dataset, te_dk_xm (not sure what this is),


class Prof:
    def __init__(self, first_name, last_name, mila_email_username, cn_groups: set):
        # when we have a Mila prof, there needs to be some way of
        # placing them in some of the cn_groups that makes them profs
        if mila_email_username is not None:
            assert set(cn_groups).intersection(
                S_profs_cn_groups
            ), f"Prof {first_name} {last_name} ({mila_email_username}) doesn't belong to the right groups."

        self.first_name = first_name.replace(" ", "")
        self.last_name = last_name.replace(" ", "")
        self.mila_email_username = mila_email_username.lower()
        self.academic_email = mapping_prof_mila_email_to_academic_email[
            self.mila_email_username
        ]
        self.cn_groups = cn_groups.difference(S_cn_groups_to_ignore)
        # let's give an opportunity to override with the constructor
        if self.academic_email in mapping_academic_email_to_drac_info:
            self.ccri, self.def_account = mapping_academic_email_to_drac_info[
                self.academic_email
            ]
        else:
            self.ccri, self.def_account = None, None


class Student:
    def __init__(
        self,
        first_name,
        last_name,
        mila_email_username,
        cn_groups: set,
        supervisor=None,
        co_supervisor=None,
        university=None,
    ):
        self.first_name = first_name
        self.last_name = last_name
        self.mila_email_username = mila_email_username
        self.cn_groups = cn_groups.difference(S_cn_groups_to_ignore)
        self.supervisor = supervisor
        self.co_supervisor = co_supervisor
        self.university = university


def read_population_mila_csv(input_path):
    """
    Process each line with a dictionary reader.
    Resolve the conflits when there is more than
    one entry for a given person because they were
    at Mila at different times.
    """

    today = datetime.now()
    formatted_today = today.strftime("%Y-%m-%d")

    L_excel_students = []
    with open(input_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        # LD_members = [line for line in csv_reader]
        for person in csv_reader:
            if person["EndDate"] <= formatted_today:
                continue
            elif person["StartDate"] >= formatted_today:
                continue

            L_excel_students.append(
                {
                    "first_name": person["FirstName"],
                    "last_name": person["LastName"],
                    "supervisor": person["Supervisor"],
                    "cosupervisor": person["CoSupervisor"],
                }
            )
    return L_excel_students


def read_mila_raw_ldap_json(input_path):
    """
    Read the JSON file.
    """

    # def process(e):
    #    if re.match()

    L_profs = []
    L_students = []

    with open(input_path, "r") as json_file:
        json_data = json.load(json_file)

        for person in json_data:
            # the most straightforward way to determine if a person is a prof,
            # because you can't trust the cn_groups "core-profs" where
            # the mila directors are also listed
            is_prof = person["mail"][0] in S_profs

            is_student = False
            cn_groups_of_supervisors = []
            cn_groups = []
            university = None

            if person["suspended"][0] == "true":
                continue

            for e in person["memberOf"]:
                if m := re.match(r"^cn=(.+?)-students.*", e):
                    if m.group(1) in [
                        "mcgill",
                        "udem",
                        "poly",
                        "ets",
                        "concordia",
                        "ulaval",
                        "hec",
                    ]:
                        university = m.group(1)
                        continue
                    cn_groups_of_supervisors.append(m.group(1))
                    is_student = True
                    continue
                if m := re.match(r"^cn=(.+?),.*", e):
                    # if m.group(1) in ["mila-core-profs", "mila-profs", "core-academic-member"]:
                    #     is_prof = True
                    cn_groups.append(m.group(1))
                    continue

            if person["mail"][0] in not_sudents:
                # For some reason, Christopher Pal and Yue Li are on their own students lists.
                # Mirco Ravanelli is an ex postdoc of Yoshua but appears to be an associate member now.
                # Let's make exceptions.
                is_student = False
            elif person["mail"][0] in not_prof:
                # Maxime Gasse is a postdoc with Andrea Lodi but also appears to co-supervise someone.
                is_prof = False

            # Someone can't be prof AND student, apart with the two above exceptions.
            assert not (
                is_prof and is_student
            ), f"Person {person['givenName'][0]} {person['sn'][0]} is both a student and a prof."

            # because it's stupid to wait for the LDAP to be updated for that one
            prefered_name = rename.get(person["mail"][0])
            if prefered_name is not None:
                person["givenName"][0] = prefered_name

            if is_prof:
                L_profs.append(
                    Prof(
                        person["givenName"][0],
                        person["sn"][0],
                        person["mail"][0],
                        set(cn_groups),
                    )
                )

            if is_student:
                L = [
                    mapping_group_to_prof[prof_short_name]
                    for prof_short_name in cn_groups_of_supervisors
                ]
                if len(L) == 1:
                    # this is not true for students with a principal supervisor
                    # outside of Mila, but it's the best that we can do with the
                    # data from the Mila LDAP
                    supervisor, co_supervisor = L[0], None
                elif len(L) == 2:
                    supervisor, co_supervisor = L
                else:
                    raise ValueError(f"More than two supervisors for a student: {L}.")
                L_students.append(
                    Student(
                        person["givenName"][0],
                        person["sn"][0],
                        person["mail"][0],
                        set(cn_groups),
                        supervisor=supervisor,
                        co_supervisor=co_supervisor,
                        university=university,
                    )
                )

    return L_profs, L_students

    """
            "memberOf": [
                "cn=c.pal-students,ou=Groups,dc=mila,dc=quebec",
                "cn=clusterusers,ou=Groups,dc=mila,dc=quebec",
                "cn=d.nowrouzezahrai-students,ou=Groups,dc=mila,dc=quebec",
                "cn=edi.survey.students,ou=Groups,dc=mila,dc=quebec",
                "cn=mcgill-students,ou=Groups,dc=mila,dc=quebec",
                "cn=mila_acces_special,ou=Groups,dc=mila,dc=quebec",
                "cn=phd,ou=Groups,dc=mila,dc=quebec"
            ],
    """


"""
Last Name,First Name,Status,Full Mila,Left Mila,School,Supervisor,Co-Supervisor,Start Date,End Date
Zumer,Jeremie,MSc,Full,Left,UdeM,Aaron Courville,n/a,2017-03-28,2019-03-30
"""


def update_students_from_excel_source(L_students, L_excel_students):
    """
    Mutates `L_students` but leaves `L_excel_students` unchanged.
    """
    L_names_A = [s.first_name + s.last_name for s in L_students]
    L_names_B = [s["first_name"] + s["last_name"] for s in L_excel_students]
    LP_results = find_exact_bag_of_words_matches(
        L_names_A, L_names_B, delta_threshold=1
    )
    # see if that makes sense
    # print(LP_results)

    DP_results = {}  # the name to which it matches, and its distance
    for a, b, delta in LP_results:
        # If we have nothing for that name,
        # or if we have something but found an even better match,
        # then update it.
        if a not in DP_results or delta < DP_results[a][1]:
            DP_results[a] = (b, delta)

    D_name_A_to_student = {s.first_name + s.last_name: s for s in L_students}
    D_name_B_to_excel_student = {
        s["first_name"] + s["last_name"]: s for s in L_excel_students
    }

    for name_A, (name_B, delta) in DP_results.items():
        student = D_name_A_to_student[name_A]
        excel_student = D_name_B_to_excel_student[name_B]
        # Now we want to update the student with the information from the excel_student.
        # Basically, all that we care about is the supervisor and the co-supervisor.

        # What's not fun, though, is that we're left with `student` having emails
        # to identity the supervisor+cosupervisor, but the excel_student has the
        # full names, and sometimes this even corresponds to non-mila profs.

        # TODO : If you don't have something here, then this function does nothing at all.


def run(population_mila_csv_input_path, mila_raw_ldap_json_input_path, verbose=True):
    L_profs, L_students = read_mila_raw_ldap_json(mila_raw_ldap_json_input_path)

    L_excel_students = read_population_mila_csv(population_mila_csv_input_path)
    update_students_from_excel_source(
        L_students, L_excel_students
    )  # mutates `L_students`

    if verbose:
        print("Profs:")
        for prof in L_profs:
            # print(prof.first_name, prof.last_name, prof.mila_email_username, prof.cn_groups)
            print(
                f"{prof.first_name}, {prof.last_name}, {prof.mila_email_username}, {prof.ccri}, {prof.def_account}"
            )  # , {prof.cn_groups}")

        print("Students:")
        for student in L_students:
            #    print(student.first_name, student.last_name, student.mila_email_username, student.cn_groups,
            #          student.supervisor, student.co_supervisor)
            print(
                f"{student.first_name}, {student.last_name}, {student.mila_email_username}, {student.supervisor}, {student.co_supervisor}"
            )

    with open("profs_and_students.json", "w") as f_out:
        json.dump(
            {
                "profs": [
                    {
                        "first_name": prof.first_name,
                        "last_name": prof.last_name,
                        "mila_email_username": prof.mila_email_username,
                        "academic_email": prof.academic_email,
                        "ccri": prof.ccri,
                        "def_account": prof.def_account,
                    }
                    for prof in L_profs
                ],
                "students": [
                    {
                        "first_name": student.first_name,
                        "last_name": student.last_name,
                        "mila_email_username": student.mila_email_username,
                        "supervisor": student.supervisor,
                        "co_supervisor": student.co_supervisor,
                        "university": student.university,
                    }
                    for student in L_students
                ],
            },
            f_out,
            indent=4,
            ensure_ascii=False,
        )


if __name__ == "__main__":
    
    run(
        population_mila_csv_input_path=get_filename("population_mila.csv"),
        mila_raw_ldap_json_input_path=get_filename("mila_raw_users.json"),
    )
