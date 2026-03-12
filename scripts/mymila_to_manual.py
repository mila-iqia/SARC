import json
import re
import sys

with open(sys.argv[1]) as f:
    data, headers = json.load(f)

# headers
drac = headers.index("Alliance-DRAC_account")
email = headers.index("MILA_Email")

CCI_RE = re.compile(r"[a-z]{3}-\d{3}")
CCRI_RE = re.compile(r"[a-z]{3}-\d{3}-\d{2}")

id = 0
print("id_pairs:")  # noqa: T201
for d in data[2:]:
    if d[drac] is not None and d[email] is not None:
        drac_data = d[drac].strip()
        if (m := CCI_RE.search(drac_data)) is not None:
            cci = m.group(0)
        else:
            continue

        print(  # noqa: T201
            f"""  a{str(id)}:\n    - name: mila_ldap\n      mid: {d[email]}\n    - name: drac_member\n      mid: {cci}\n    - name: drac_role\n      mid: {cci}\n"""
        )
        id += 1
