#!/usr/bin/env python3
"""Génère la liste des membres des allocations Slurm aip-* des profs Mila sur tamia.

Entrée : mila_profs.csv (colonnes email,name,CCI — le CCI peut être vide).

Le script :
  1. se connecte à tamia via un control master SSH (réutilise ~/.ssh/cm-tamia si
     actif ; sinon le lance et demande le mot de passe + MFA dans le terminal) ;
  2. récupère toutes les allocations aip-* de tamia et le nom complet (gecos) de
     chaque titulaire ;
  3. associe les profs de la liste CSV aux allocations en comparant les noms ;
  4. liste les membres (sacctmgr list assoc) de ces allocations ;
  5. enrichit les emails si possible depuis la cache CCDB (drac_members.csv) et
     les dumps MyMila (mymila*.json) — sinon email vide ;
  6. écrit le CSV : Username,Nom,Email,Allocations,PI.

Usage :
    python generate_drac_users_aip.py [--profs mila_profs.csv] [--out tamia_aip_members.csv]
                                      [--host tamia] [--socket ~/.ssh/cm-tamia]
"""

import argparse
import csv
import json
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def norm(s: str) -> str:
    """minuscules, sans accents, ponctuation -> espaces"""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def ssh(socket: str, host: str, script: str) -> str:
    """Exécute un script bash sur le cluster via le control master, renvoie stdout."""
    r = subprocess.run(  # noqa: PLW1510
        ["ssh", "-S", socket, host, "bash -s"],
        input=script.encode(),
        capture_output=True,
    )
    if r.returncode != 0:
        sys.exit(
            f"ERREUR ssh ({r.returncode}) : {r.stderr.decode(errors='replace').strip()}"
        )
    return r.stdout.decode(errors="replace")


def ensure_master(socket: str, host: str) -> None:
    """Réutilise le control master s'il répond, sinon se connecte (une seule
    invite mot de passe + MFA ; les commandes suivantes réutilisent le canal)."""
    check = subprocess.run(  # noqa: PLW1510
        ["ssh", "-S", socket, "-O", "check", host], capture_output=True
    )
    if check.returncode == 0:
        log(f"control master réutilisé : {socket}")
        return
    Path(socket).unlink(missing_ok=True)
    log(f"connexion ssh {host} — saisis ton mot de passe + MFA si demandé...")
    r = subprocess.run(  # noqa: PLW1510
        ["ssh", "-M", "-S", socket, "-o", "ControlPersist=60m", host, "true"]
    )
    if r.returncode != 0:
        sys.exit(f"ERREUR : connexion à {host} impossible")
    log(f"canal établi : {socket}")


def remote_dump_pis(socket: str, host: str) -> dict:
    """username->gecos pour tous les titulaires de comptes aip-*."""
    out = ssh(
        socket,
        host,
        """
for u in $(sacctmgr -nP list account Format=Account | sed -n "s/^aip-//p"); do
  g=$(getent passwd "$u" | cut -d: -f5)
  echo "PI|$u|$g"
done
""",
    )
    pis = {}
    for line in out.splitlines():
        parts = line.split("|", 2)
        if len(parts) == 3:
            pis[parts[1]] = parts[2]
    return pis


def match_allocations(profs: list, pis: dict) -> tuple:
    """Associe chaque prof de la liste aux comptes aip-* dont le gecos correspond.

    Retourne (CCI->username PI, username PI sans correspondance, profs sans compte).
    """
    by_cci, unmatched_profs = {}, []
    used = set()
    for prof in profs:
        toks = set(norm(prof["name"]).split()) - {""}
        if not toks:
            continue
        # le dernier morceau du nom (peut être multiple : Lacoste-Julien, O'Donnell)
        surname_toks = set(norm(prof["name"].split()[-1]).split())
        matches = []
        for user, gecos in pis.items():
            if user in used or not gecos:
                continue
            gtoks = set(norm(gecos).split())
            # tous les tokens du prof dans le gecos, OU (nom de famille commun
            # ET au moins un autre token commun)
            if toks <= gtoks or (surname_toks & gtoks and toks & gtoks):
                matches.append(user)
        # privilégie le candidat qui partage le plus de tokens
        if len(matches) > 1:
            matches.sort(key=lambda u: -len(toks & set(norm(pis[u]).split())))
            log(f"  AMBIGU {prof['name']}: {matches} -> garde {matches[0]}")
        if matches:
            used.add(matches[0])
            by_cci[prof["name"]] = matches[0]
        else:
            unmatched_profs.append(prof)
    return by_cci, unmatched_profs


def remote_dump_members(socket: str, host: str, accts: list) -> tuple:
    """(username->set(accounts), username->gecos) pour les membres des allocations."""
    script = (
        'accts="%s"\n' % " ".join(accts)
        + """
for a in $accts; do
  sacctmgr -nP list assoc Where Account=$a Format=User 2>/dev/null | tail -n +2 \\
    | while read -r u; do [ -n "$u" ] && [ "$u" != "None" ] && echo "ASSOC|$a|$u"; done
done > /tmp/aip_members.$$
cut -d"|" -f3 /tmp/aip_members.$$ | sort -u | while read -r u; do
  g=$(getent passwd "$u" | cut -d: -f5)
  echo "GECOS|$u|$g"
done
cat /tmp/aip_members.$$
rm -f /tmp/aip_members.$$
"""
    )
    out = ssh(socket, host, script)
    members, gecos = {}, {}
    for line in out.splitlines():
        parts = line.split("|", 2)
        if parts[0] == "ASSOC" and len(parts) == 3:
            members.setdefault(parts[2], set()).add(parts[1])
        elif parts[0] == "GECOS" and len(parts) == 3:
            gecos[parts[1]] = parts[2]
    return members, gecos


def email_lookups(mymila_glob: str, ccdb: Path) -> tuple:
    """Retourne (username->email depuis CCDB, nom_normalise->email depuis MyMila)."""
    by_user = {}
    if ccdb.exists():
        with open(ccdb, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                u = (row.get("username") or "").strip().lower()
                e = (row.get("Email") or "").strip()
                if u and e:
                    by_user[u] = e
    by_name = {}
    for path in sorted(HERE.glob(mymila_glob)):
        try:
            d = json.load(open(path, encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            log(f"  dump MyMila ignoré ({path.name}) : {exc}")
            continue
        if not (isinstance(d, list) and len(d) == 2 and isinstance(d[1][0], str)):
            continue
        rows, hdr = d[0], d[1]
        ix = {h: i for i, h in enumerate(hdr)}
        need = ("First_Name", "Last_Name", "MILA_Email")
        if any(c not in ix for c in need):
            continue
        for r in rows:
            e = str(r[ix["MILA_Email"]] or "").strip()
            if not e:
                continue
            name = norm(f"{r[ix['First_Name']] or ''} {r[ix['Last_Name']] or ''}")
            if name:
                prev = by_name.get(name)
                if prev is not None and prev != e:
                    by_name[name] = None  # homonyme : on ne tranche pas
                elif name not in by_name:
                    by_name[name] = e
    return by_user, by_name


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--profs", default=str(HERE / "mila_profs.csv"))
    ap.add_argument("--out", default=str(HERE / "tamia_aip_members.csv"))
    ap.add_argument("--host", default="tamia")
    ap.add_argument("--socket", default=str(Path.home() / ".ssh" / "cm-tamia"))
    ap.add_argument(
        "--mymila",
        default="../mymila*.json",
        help="glob des dumps MyMila pour les emails",
    )
    ap.add_argument("--ccdb", default=str(HERE.parent.parent / "drac_members.csv"))
    ap.add_argument(
        "--close-master", action="store_true", help="ferme le control master à la fin"
    )
    args = ap.parse_args()

    with open(args.profs, newline="", encoding="utf-8") as f:
        profs = [r for r in csv.DictReader(f) if (r.get("name") or "").strip()]
    log(f"{len(profs)} profs lus dans {args.profs}")

    socket = str(Path(args.socket).expanduser())
    ensure_master(socket, args.host)

    log("liste des allocations aip-* + titulaires...")
    pis = remote_dump_pis(socket, args.host)
    log(f"{len(pis)} comptes aip-* sur {args.host}")

    by_cci, unmatched = match_allocations(profs, pis)
    log(f"{len(by_cci)} profs associés à une allocation, {len(unmatched)} sans")
    for p in unmatched:
        log(f"  sans compte : {p['name']} ({p.get('CCI') or 'sans CCI'})")
    if not by_cci:
        sys.exit("aucune association prof <-> allocation, abandon")

    accts = [f"aip-{u}" for u in sorted(set(by_cci.values()))]
    log(f"membres de {len(accts)} allocations...")
    members, gecos = remote_dump_members(socket, args.host, accts)
    log(f"{len(members)} users distincts")

    ccdb_mail, mymila_mail = email_lookups(args.mymila, Path(args.ccdb))
    # email des PIs : celui de la liste profs, rattaché via l'association trouvée
    pi_mail = {}
    for pname, user in by_cci.items():
        e = next((p.get("email", "").strip() for p in profs if p["name"] == pname), "")
        if e:
            pi_mail[user] = e
    pi_users = set(by_cci.values())

    rows = []
    for u in sorted(members):
        name = gecos.get(u, "")
        email = (
            pi_mail.get(u) or ccdb_mail.get(u) or (mymila_mail.get(norm(name)) or "")
        )
        rows.append(
            {
                "Slurm_Username": u,
                "Nom": name,
                "Email": email,
                "Allocations": ";".join(sorted(members[u])),
                "PI": "oui" if u in pi_users else "",
            }
        )

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f, fieldnames=["Slurm_Username", "Nom", "Email", "Allocations", "PI"]
        )
        w.writeheader()
        w.writerows(rows)
    n_mail = sum(1 for r in rows if r["Email"])
    log(f"OK : {args.out} — {len(rows)} users, email trouvé pour {n_mail}")

    if args.close_master:
        subprocess.run(  # noqa: PLW1510
            ["ssh", "-S", socket, "-O", "exit", args.host], capture_output=True
        )
        log("control master fermé")
    else:
        log(
            f"(control master laissé actif ; pour le fermer : "
            f"ssh -S {socket} -O exit {args.host})"
        )


if __name__ == "__main__":
    main()
