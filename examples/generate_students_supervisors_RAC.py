import csv

from sarc.ldap.api import get_users

## list of wanted supervisors:

supervisors = [
    "yoshua.bengio@mila.quebec",  # Yoshua Bengio
    "courvila@mila.quebec",  # Aaron Courville
    "aishwarya.agrawal@mila.quebec",  # Aishwarya Agrawal
    "blake.richards@mila.quebec",  # Blake Richards
    "christopher.pal@mila.quebec",  # Chris Pal ("Christopher Pal" dans le LDAP)
    "drolnick@mila.quebec",  # David Rolnick
    "gidelgau@mila.quebec",  # Gauthier Gidel
    "glen.berseth@mila.quebec",  # Glen Berseth
    "guillaume.lajoie@mila.quebec",  # Guillaume Lajoie
    "rabussgu@mila.quebec",  # Guillaume Rabusseau
    "wolfguy@mila.quebec",  # Guy Wolf
    "ioannis@mila.quebec",  # Ioannis Mitliagkas
    "tangjian@mila.quebec",  # Jian Tang
    "lcharlin@mila.quebec",  # Laurent Charlin
    "sarath.chandar@mila.quebec",  # Sarath Chandar ("Sarath Chandar Anbil Parthipan" dans le LDAP)
    "siamak.ravanbakhsh@mila.quebec",  # Siamak Ravanbakhsh
    "slacoste@mila.quebec",  # Simon Lacoste-Julien
    "siva.reddy@mila.quebec",  # Siva Reddy
    "odonnelt@mila.quebec",  # Timothy J O’Donnell
    "farnadig@mila.quebec",  # Golnoosh Farnadi
    "dhanya.sridhar@mila.quebec",  # Dhanya Sridhar
    "jpineau@mila.quebec",  # Joelle Pineau
    "cheungja@mila.quebec",  # Jackie Cheung
    "reihaneh.rabbany@mila.quebec",  # Reihaneh Rabbany
    "derek@mila.quebec",  # Derek Nowrouzezahrai
    "irina.rish@mila.quebec",  # Irina Rish
    # "plcbacon@mila.quebec", # Pierre-Luc Bacon (doublon / homonyme)
    "pierre-luc.bacon@mila.quebec",  # Pierre-Luc Bacon
    "chengzhi.mao@mila.quebec",  # Chengzhi Mao
]


users = get_users()
users = [u for u in users if u.mila.active]
print(f"Number of active users: {len(users)}")

dl_supervisors_to_students = {}
for supervisor in supervisors:
    dl_supervisors_to_students[supervisor] = []

# this will be our output data: {"student": [supervisors]}
dl_students_to_supervisors = {}
nb_supervised_students = 0

for user in users:
    user_email = user.mila.email
    user_supervisors = [user.mila_ldap["supervisor"], user.mila_ldap["co_supervisor"]]
    if user_supervisors[0]:
        nb_supervised_students += 1
    if user_supervisors[0] in supervisors or user_supervisors[1] in supervisors:
        dl_students_to_supervisors[user_email] = user_supervisors
        for s in user_supervisors:
            if s in dl_supervisors_to_students:
                dl_supervisors_to_students[s].append(user_email)
        # print(f"{user_email} is supervised by {user_supervisors}")

print(f"Number of supervised students: {nb_supervised_students}")
print(f"Number of supervised students for RAC: {len(dl_students_to_supervisors)}\n")
print("Number of students per professor:\n")
for supervisor in dl_supervisors_to_students:
    print(f"{supervisor}: {len(dl_supervisors_to_students[supervisor])}")

# write the CSV
with open("students_supervisors_RAC.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["student email", "supervisor email", "co-supervisor email"])
    for student in dl_students_to_supervisors:
        writer.writerow([student] + dl_students_to_supervisors[student])
