#!/usr/bin/env python3
"""Pré-génère un fichier de user-matching pour scripts/usermatch/main.py.

Les deux pools sont ceux de l'interface « Match DRAC » :
  - les utilisateurs qui ont un compte DRAC (username) mais pas de compte Mila LDAP ;
  - les utilisateurs Mila LDAP qui n'ont pas de compte DRAC.

Critère de rapprochement : le **nom** de la personne, recoupé avec le groupe
auquel elle appartient :

  - le nom des profs (PI) des allocations tamia ``aip-*`` dont le compte DRAC est
    membre — récupéré en direct sur tamia (``sacctmgr`` pour les allocations et
    les membres, ``getent passwd`` pour le nom complet du titulaire) ;
  - les superviseurs SARC du compte LDAP (tables ``user_supervisors`` /
    ``supervisorshelper``, alimentées par MyMila).

Le recoupement « prof du groupe » <-> « superviseur » se fait d'abord au niveau
des ids SARC (le username du titulaire est retrouvé dans ``credentialsdb``), à
défaut au niveau des noms. Un compte DRAC titulaire de sa propre allocation
(``aip-<username>``) est donc apparié avec le compte LDAP sans superviseur qui
porte le même nom.

Un premier passage apparie les comptes qui partagent le même email.

Seules les paires **non ambiguës** (meilleure candidature dans les deux sens)
sont écrites dans le fichier. Le reste (ex-aequo, candidats non réciproques,
indices isolés) est réservé au rapport ``--report``, à trancher à la main dans
l'interface.

Le format de sortie est exactement celui que télécharge ``scripts/usermatch/main.py``
(fichier ``usermatches.json``) ; il se recharge avec le bouton « Load from file… » :

    [
      {
        "matching_id": { "name": "drac_username", "mid": "jdoe" },
        "known_matches": [{ "name": "mila_ldap", "mid": "john.doe@mila.quebec" }]
      }
    ]

Usage :
    SARC_CONFIG=cloudsql_db.yaml uv run python scripts/usermatch/prematch.py \\
        --out usermatches_prematch.json --report prematch.md
"""

import argparse
import json
import re
import subprocess
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

from sqlmodel import Session, col, func, select
from tqdm import tqdm

from sarc.config import config
from sarc.db.users import CredentialsDB, SupervisorsDB, SupervisorsHelper, UserDB

AIP_PREFIX = "aip-"

# scores de similarite de nom
NAME_EXACT = 3  # noms normalises identiques
NAME_SUBSET = 2  # tous les mots du nom le plus court retrouves
NAME_LOOSE = 1  # un mot non apparie, mais une simple initiale

# scores de recoupement groupe (prof de l'allocation aip-* / superviseur SARC)
GROUP_ID = 3  # le prof et le superviseur sont la meme ligne de la table users
GROUP_PI_SELF = 3  # compte DRAC titulaire de son allocation, LDAP sans superviseur
GROUP_NAME = 2  # nom du prof == nom d'un superviseur, sans correspondance en base
EMAIL_SCORE = 10  # appariement direct, sans condition de groupe


# ---------------------------------------------------------------------------
# noms : normalisation et similarite
# ---------------------------------------------------------------------------


def norm(s: str) -> str:
    """minuscules, sans accents, ponctuation -> espaces"""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


@cache
def tokens(name: str) -> tuple[str, ...]:
    return tuple(norm(name).split())


def edit_distance(a: str, b: str) -> int:
    """Distance de Levenshtein (les noms sont courts, un DP suffit)."""
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        current = [i]
        for j, cb in enumerate(b, start=1):
            current.append(
                min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (ca != cb))
            )
        previous = current
    return previous[-1]


@cache
def token_similar(a: str, b: str) -> bool:
    """Deux mots de nom equivalents : identiques, initiale, ou legere faute."""
    if a == b:
        return True
    if len(a) <= 1 or len(b) <= 1:
        return (len(a) == 1 and b.startswith(a)) or (len(b) == 1 and a.startswith(b))
    shortest = min(len(a), len(b))
    # une faute toleree des 4 lettres, deux pour les noms longs ; les mots courts
    # (initiales, particules) doivent eux correspondre exactement
    allowed = 2 if shortest >= 7 else 1 if shortest >= 4 else 0
    return edit_distance(a, b) <= allowed


def match_words(short: tuple[str, ...], long: tuple[str, ...]) -> set[int]:
    """Indices dans `short` des mots retrouvables, chacun sur un mot different de
    `long` : les equivalences exactes d'abord, pour ne pas griller les candidats
    du flou."""
    used: set[int] = set()
    matched: set[int] = set()
    for exact_round in (True, False):
        for i, word in enumerate(short):
            if i in matched:
                continue
            for j, other in enumerate(long):
                if j in used:
                    continue
                if (word == other) if exact_round else token_similar(word, other):
                    used.add(j)
                    matched.add(i)
                    break
    return matched


def shares_surname(a: str, b: str) -> bool:
    """Le nom de famille (dernier mot) se retrouve des deux cotes."""
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return False
    return token_similar(ta[-1], tb[-1]) or (
        len(ta) > 1
        and len(tb) > 1
        and token_similar(ta[-1], tb[0])
        and token_similar(ta[0], tb[-1])
    )


def name_score(drac_name: str, ldap_name: str) -> int | None:
    """Score de similarite (NAME_*), ou None si les noms ne correspondent pas.

    Le nom de famille doit etre commun ET au moins un prenom aussi : le seul nom
    de famille commun ne suffit jamais, les groupes comptent beaucoup d'homonymes.
    """
    ta, tb = tokens(drac_name), tokens(ldap_name)
    if ta and ta == tb:
        return NAME_EXACT
    if not ta or not tb or not shares_surname(drac_name, ldap_name):
        return None
    short, long = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    hit = match_words(short, long)
    if len(hit) < 2 or (len(short) - 1) not in hit:  # nom de famille + un prenom
        return None
    unmatched = [word for i, word in enumerate(short) if i not in hit]
    if not unmatched:
        return NAME_SUBSET
    if len(unmatched) == 1 and len(unmatched[0]) <= 2:  # un second prenom/une initiale
        return NAME_LOOSE
    return None


def same_person(a: str, b: str) -> bool:
    """Deux noms designent-ils la meme personne (nom de famille + un prenom) ?

    Exige le niveau strict : sert a verifier qu'un prof d'allocation aip-* et un
    superviseur SARC sont la meme personne, pas a apparier deux comptes.
    """
    score = name_score(a, b)
    return score is not None and score >= NAME_SUBSET


# ---------------------------------------------------------------------------
# collecte des allocations aip-* sur tamia
# ---------------------------------------------------------------------------

# Un seul aller-retour : titulaires des aip-*, membres de ces allocations, puis
# le nom complet (gecos) de tous les usernames rencontres.
REMOTE_DUMP = """
accts=$(sacctmgr -nP list account Format=Account | sed -n "s/^aip-//p")
for u in $accts; do
  echo "PI|$u|$(getent passwd "$u" | cut -d: -f5)"
done
f=/tmp/aip_assoc.$$
sacctmgr -nP list assoc Format=Account,User 2>/dev/null \\
  | awk -F"|" 'NR>1 && $1 ~ /^aip-/ && $2 != "" && $2 != "None" {print "ASSOC|"$1"|"$2}' > "$f"
if [ ! -s "$f" ]; then
  # requete globale indisponible : une requete par allocation
  for u in $accts; do
    a="aip-$u"
    sacctmgr -nP list assoc Where Account=$a Format=User 2>/dev/null | tail -n +2 \\
      | while read -r m; do [ -n "$m" ] && [ "$m" != "None" ] && echo "ASSOC|$a|$m"; done
  done > "$f"
fi
cat "$f"
{ cut -d"|" -f3 "$f"; printf '%s\\n' $accts; } | sed '/^$/d' | sort -u | while read -r m; do
  echo "GECOS|$m|$(getent passwd "$m" | cut -d: -f5)"
done
rm -f "$f"
"""


@dataclass
class Tamia:
    """Allocations aip-*, leurs titulaires et leurs membres."""

    pis: dict[str, str] = field(default_factory=dict)  # username PI -> gecos
    gecos: dict[str, str] = field(default_factory=dict)  # username -> nom complet
    allocations: dict[str, set[str]] = field(default_factory=dict)  # username -> aip-*


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
        print(f"control master réutilisé : {socket}", file=sys.stderr)
        return
    Path(socket).unlink(missing_ok=True)
    print(
        f"connexion ssh {host} — saisis ton mot de passe + MFA si demandé...",
        file=sys.stderr,
    )
    r = subprocess.run(  # noqa: PLW1510
        ["ssh", "-M", "-S", socket, "-o", "ControlPersist=60m", host, "true"]
    )
    if r.returncode != 0:
        sys.exit(f"ERREUR : connexion à {host} impossible")
    print(f"canal établi : {socket}", file=sys.stderr)


def fetch_tamia(host: str, socket: str, close_master: bool) -> Tamia:
    """Allocations aip-*, titulaires et membres, en un seul aller-retour SSH."""
    socket = str(Path(socket).expanduser())
    ensure_master(socket, host)
    out = ssh(socket, host, REMOTE_DUMP)
    if close_master:
        subprocess.run(  # noqa: PLW1510
            ["ssh", "-S", socket, "-O", "exit", host], capture_output=True
        )
        print("control master fermé", file=sys.stderr)

    tamia = Tamia()
    for line in out.splitlines():
        parts = line.split("|", 2)
        if len(parts) != 3:
            continue
        kind, a, b = parts
        if kind == "PI":
            tamia.pis[a.lower()] = b
        elif kind == "GECOS":
            tamia.gecos[b.lower()] = a
        elif kind == "ASSOC":
            tamia.allocations.setdefault(b.lower(), set()).add(a)
    if not tamia.pis:
        sys.exit(f"aucune allocation {AIP_PREFIX}* récupérée sur {host}, abandon")
    print(
        f"{len(tamia.pis)} allocations {AIP_PREFIX}* sur {host}, "
        f"{len(tamia.allocations)} membres",
        file=sys.stderr,
    )
    return tamia


def pi_name(tamia: Tamia, pi_username: str) -> str:
    """Nom complet du titulaire d'une allocation aip-<pi_username>."""
    key = pi_username.lower()
    return tamia.pis.get(key) or tamia.gecos.get(key) or ""


# ---------------------------------------------------------------------------
# pools : comptes DRAC seuls et comptes LDAP seuls (mêmes règles que main.py)
# ---------------------------------------------------------------------------


@dataclass
class DracAccount:
    user_id: int
    display_name: str
    email: str
    username: str
    cci: str | None
    allocations: set[str] = field(default_factory=set)
    pi_ids: set[int] = field(default_factory=set)  # ids SARC des PI des allocations
    pis: dict[str, str] = field(default_factory=dict)  # username PI -> nom (gecos)
    is_pi: bool = False  # titulaire de sa propre allocation aip-<username>

    def matching_id(self) -> dict[str, str]:
        return {"name": "drac_username", "mid": self.username}

    def label(self) -> str:
        return f"{self.display_name} [{self.username}]"


@dataclass
class LdapAccount:
    user_id: int
    display_name: str
    email: str
    ldap_id: str
    supervisors: dict[int, str] = field(default_factory=dict)  # id SARC -> nom

    def label(self) -> str:
        return f"{self.display_name} [{self.ldap_id}]"


def load_drac_usernames(sess: Session) -> dict[int, str]:
    """Map user ids to their most recent DRAC username (same rule as main.py)."""
    stmt = (
        select(CredentialsDB.user_id, CredentialsDB.username)
        .where(CredentialsDB.domain == "drac")
        .order_by(func.lower(CredentialsDB.valid))
    )
    # Later validity periods overwrite earlier ones.
    return {user_id: username for user_id, username in sess.exec(stmt)}


def load_current_supervisors(sess: Session) -> dict[int, list[int]]:
    """{id de l'etudiant: [ids SARC des superviseurs]}, listes valides maintenant."""
    rows = sess.exec(
        select(SupervisorsDB.user_id, SupervisorsHelper.supervisor)
        .join(
            SupervisorsHelper, col(SupervisorsHelper.list_id) == col(SupervisorsDB.id)
        )
        .where(SupervisorsDB.valid.contains(datetime.now(UTC)))
        .order_by(col(SupervisorsDB.user_id), col(SupervisorsHelper.pos))
    ).all()
    supervisors: dict[int, list[int]] = {}
    for user_id, supervisor_id in rows:
        supervisors.setdefault(user_id, []).append(supervisor_id)
    return supervisors


def load_pools(tamia: Tamia) -> tuple[list[DracAccount], list[LdapAccount], dict]:
    """Construit les deux pools et y rattache les groupes (allocations, superviseurs)."""
    with config.db.session() as sess:
        drac_usernames = load_drac_usernames(sess)
        supervisors = load_current_supervisors(sess)
        # matching_ids se charge paresseusement : tout lire avant de fermer la session
        users: list[tuple[int, str, str, dict[str, str]]] = []
        for u in tqdm(
            sess.exec(select(UserDB)), desc="Chargement des users", unit="user"
        ):
            if u.id is None:
                continue
            users.append((u.id, u.display_name, u.email or "", dict(u.matching_ids)))

    # username -> ids SARC ; un username rencontre plusieurs fois n'est pas une
    # identite fiable, on ne s'en sert pas pour le recoupement par id
    ids_by_username: dict[str, set[int]] = {}
    for user_id, username in drac_usernames.items():
        ids_by_username.setdefault(username.lower(), set()).add(user_id)
    names = {uid: display_name for uid, display_name, _, _ in users}

    drac_pool: list[DracAccount] = []
    ldap_pool: list[LdapAccount] = []
    for uid, display_name, email, matching_ids in users:
        username = drac_usernames.get(uid)
        ldap_id = matching_ids.get("mila_ldap")
        if username and not ldap_id:
            acc = DracAccount(
                user_id=uid,
                display_name=display_name,
                email=email,
                username=username,
                cci=matching_ids.get("drac_member"),
                allocations=set(tamia.allocations.get(username.lower(), ())),
            )
            for allocation in sorted(acc.allocations):
                pi = allocation.removeprefix(AIP_PREFIX).lower()
                if pi == username.lower():
                    acc.is_pi = True
                acc.pis[pi] = pi_name(tamia, pi)
                pi_ids = ids_by_username.get(pi)
                if pi_ids and len(pi_ids) == 1:
                    acc.pi_ids |= pi_ids
            drac_pool.append(acc)
        elif ldap_id and not username:
            ldap_pool.append(
                LdapAccount(
                    user_id=uid,
                    display_name=display_name,
                    email=email,
                    ldap_id=ldap_id,
                    supervisors={
                        sid: names.get(sid, "") for sid in supervisors.get(uid, ())
                    },
                )
            )

    stats = {
        "drac_only": len(drac_pool),
        "ldap_only": len(ldap_pool),
        "drac_with_aip": sum(1 for d in drac_pool if d.allocations),
        "drac_with_pi": sum(1 for d in drac_pool if d.pi_ids or any(d.pis.values())),
        "ldap_with_supervisors": sum(1 for a in ldap_pool if a.supervisors),
    }
    return drac_pool, ldap_pool, stats


# ---------------------------------------------------------------------------
# appariement
# ---------------------------------------------------------------------------


@dataclass(eq=False)
class Candidate:
    drac: DracAccount
    ldap: LdapAccount
    score: int
    reason: str
    note: str = ""  # pour les candidats non retenus


def group_evidence(drac: DracAccount, ldap: LdapAccount) -> tuple[int, str] | None:
    """Le prof de l'allocation aip-* du compte DRAC est-il un superviseur du LDAP ?"""
    shared_ids = drac.pi_ids & ldap.supervisors.keys()
    if shared_ids:
        who = ", ".join(
            filter(None, (ldap.supervisors[sid] for sid in sorted(shared_ids)))
        )
        return GROUP_ID, f"superviseur SARC commun ({who or sorted(shared_ids)})"
    for pi in drac.pis.values():
        for supervisor in ldap.supervisors.values():
            if pi and supervisor and same_person(pi, supervisor):
                return GROUP_NAME, f"prof « {pi} » = superviseur « {supervisor} »"
    if drac.is_pi and not ldap.supervisors:
        return GROUP_PI_SELF, "titulaire de l'allocation, aucun superviseur côté LDAP"
    return None


def build_candidates(
    drac_pool: list[DracAccount], ldap_pool: list[LdapAccount]
) -> tuple[list[Candidate], list[Candidate]]:
    """Candidats retenables (nom + groupe), et candidats a un seul indice pour le rapport."""
    candidates: list[Candidate] = []
    single_clue: list[Candidate] = []
    for drac in drac_pool:
        for ldap in ldap_pool:
            if not shares_surname(drac.display_name, ldap.display_name):
                continue
            group = group_evidence(drac, ldap)
            score = name_score(drac.display_name, ldap.display_name)
            if group and score:
                candidates.append(
                    Candidate(
                        drac, ldap, group[0] + score, f"{group[1]}, nom compatible"
                    )
                )
            elif group:
                single_clue.append(
                    Candidate(
                        drac,
                        ldap,
                        group[0],
                        group[1],
                        "noms trop différents pour être appariés automatiquement "
                        "(seul le nom de famille est commun)",
                    )
                )
            elif score:
                single_clue.append(
                    Candidate(
                        drac,
                        ldap,
                        score,
                        "noms compatibles",
                        "aucun prof de groupe en commun connu",
                    )
                )
    return candidates, single_clue


def select_unique(
    candidates: list[Candidate],
) -> tuple[list[Candidate], list[Candidate]]:
    """Garde les paires qui sont la meilleure candidature dans les deux sens."""
    by_drac: dict[int, list[Candidate]] = {}
    by_ldap: dict[int, list[Candidate]] = {}
    for c in candidates:
        by_drac.setdefault(c.drac.user_id, []).append(c)
        by_ldap.setdefault(c.ldap.user_id, []).append(c)

    def tops(groups: dict[int, list[Candidate]]) -> dict[int, list[Candidate]]:
        return {
            owner: [c for c in cs if c.score == max(x.score for x in cs)]
            for owner, cs in groups.items()
        }

    top_drac, top_ldap = tops(by_drac), tops(by_ldap)

    accepted, ambiguous = [], []
    for c in candidates:
        rivals_drac = [x for x in top_drac[c.drac.user_id] if x.ldap is not c.ldap]
        rivals_ldap = [x for x in top_ldap[c.ldap.user_id] if x.drac is not c.drac]
        if rivals_drac:
            rival = max(rivals_drac, key=lambda x: x.score)
            if rival.score == c.score:
                c.note = (
                    f"{c.drac.label()} est aussi bien apparié avec "
                    f"{rival.ldap.label()} (même score {c.score})"
                )
            else:
                c.note = (
                    f"{c.drac.label()} est mieux apparié avec {rival.ldap.label()} "
                    f"(score {rival.score} > {c.score})"
                )
            ambiguous.append(c)
        elif rivals_ldap:
            rival = max(rivals_ldap, key=lambda x: x.score)
            if rival.score == c.score:
                c.note = (
                    f"{c.ldap.label()} est aussi bien apparié avec "
                    f"{rival.drac.label()} (même score {c.score})"
                )
            else:
                c.note = (
                    f"{c.ldap.label()} est mieux apparié avec {rival.drac.label()} "
                    f"(score {rival.score} > {c.score})"
                )
            ambiguous.append(c)
        else:
            accepted.append(c)
    return accepted, ambiguous


def pair_by_email(
    drac_pool: list[DracAccount], ldap_pool: list[LdapAccount]
) -> tuple[list[Candidate], list[DracAccount], list[LdapAccount]]:
    """Premier passage : emails identiques, quand il n'y a qu'un seul candidat."""
    ldap_by_email: dict[str, list[LdapAccount]] = {}
    for ldap in ldap_pool:
        if ldap.email:
            ldap_by_email.setdefault(ldap.email.lower(), []).append(ldap)

    pairs: list[Candidate] = []
    used_drac: set[int] = set()
    used_ldap: set[int] = set()
    for drac in drac_pool:
        if not drac.email:
            continue
        matches = ldap_by_email.get(drac.email.lower(), [])
        if len(matches) != 1:
            continue
        pairs.append(
            Candidate(drac, matches[0], EMAIL_SCORE, f"email identique ({drac.email})")
        )
        used_drac.add(drac.user_id)
        used_ldap.add(matches[0].user_id)
    rest_drac = [d for d in drac_pool if d.user_id not in used_drac]
    rest_ldap = [a for a in ldap_pool if a.user_id not in used_ldap]
    return pairs, rest_drac, rest_ldap


# ---------------------------------------------------------------------------
# sortie
# ---------------------------------------------------------------------------


def to_json(pairs: list[Candidate]) -> list[dict]:
    """Format usermatches.json de scripts/usermatch/main.py."""
    return [
        {
            "matching_id": c.drac.matching_id(),
            "known_matches": [{"name": "mila_ldap", "mid": c.ldap.ldap_id}],
        }
        for c in sorted(pairs, key=lambda c: c.drac.username)
    ]


def _rows(
    pairs: list[Candidate], *, use_note: bool = False, limit: int = 0
) -> list[str]:
    if not pairs:
        return ["_Aucun._"]
    out = []
    ordered = sorted(pairs, key=lambda c: (-c.score, c.drac.username))
    if limit:
        ordered = ordered[:limit]
    for c in ordered:
        cells = [
            f"{c.drac.display_name} `{c.drac.username}`",
            f"{c.ldap.display_name} `{c.ldap.ldap_id}`",
            str(c.score),
            c.note if use_note else c.reason,
        ]
        out.append("| " + " | ".join(cells) + " |")
    return out


def report(
    accepted: list[Candidate],
    ambiguous: list[Candidate],
    email_pairs: list[Candidate],
    single_clue: list[Candidate],
    stats: dict,
) -> str:
    lines = [
        "# Pré-matching DRAC ↔ Mila LDAP",
        "",
        f"- Généré le {datetime.now(UTC).astimezone().strftime('%Y-%m-%d %H:%M %Z')}",
        f"- Comptes DRAC sans LDAP : {stats['drac_only']} "
        f"({stats['drac_with_aip']} membres d'allocations {AIP_PREFIX}*, "
        f"{stats['drac_with_pi']} avec un prof identifié)",
        f"- Comptes LDAP sans DRAC : {stats['ldap_only']} "
        f"({stats['ldap_with_supervisors']} avec des superviseurs connus)",
        f"- Paires par email : {len(email_pairs)}",
        f"- Paires par nom + recoupement prof/superviseur : {len(accepted)}",
        f"- Cas ambigus (non écrits dans le JSON) : {len(ambiguous)}",
        f"- Candidats à un seul indice (nom ou groupe) : {len(single_clue)}",
        "",
        "## Paires écrites dans le fichier",
        "",
        "| DRAC | LDAP | score | critère |",
        "| --- | --- | --- | --- |",
    ]
    lines += _rows(email_pairs + accepted)
    lines += [
        "",
        "## Cas ambigus — à trancher dans l'interface",
        "",
        "| DRAC | LDAP | score | pourquoi ce n'est pas automatique |",
        "| --- | --- | --- | --- |",
    ]
    lines += _rows(ambiguous, use_note=True)
    lines += [
        "",
        "## Candidats à un seul indice",
        "",
        "Nom compatible sans recoupement de groupe, ou groupe commun mais seul le "
        "nom de famille est commun. Les 40 mieux classés sont listés.",
        "",
        "| DRAC | LDAP | score | ce qui manque |",
        "| --- | --- | --- | --- |",
    ]
    lines += _rows(single_clue, use_note=True, limit=40)
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--out",
        default="usermatches_prematch.json",
        help="fichier de matching à charger dans scripts/usermatch/main.py "
        "(défaut : %(default)s)",
    )
    parser.add_argument(
        "--report", default=None, help="écrire aussi un rapport markdown à ce chemin"
    )
    parser.add_argument(
        "--host", default="tamia", help="hôte SSH du cluster (défaut : %(default)s)"
    )
    parser.add_argument(
        "--socket",
        default=str(Path.home() / ".ssh" / "cm-tamia"),
        help="control master SSH (défaut : %(default)s)",
    )
    parser.add_argument(
        "--close-master", action="store_true", help="ferme le control master à la fin"
    )
    args = parser.parse_args()

    tamia = fetch_tamia(args.host, args.socket, args.close_master)
    drac_pool, ldap_pool, stats = load_pools(tamia)
    print(
        f"{stats['drac_only']} comptes DRAC sans LDAP, {stats['ldap_only']} comptes "
        f"LDAP sans DRAC ({stats['drac_with_aip']} DRAC membres d'allocations "
        f"{AIP_PREFIX}*, {stats['ldap_with_supervisors']} LDAP avec superviseurs)",
        file=sys.stderr,
    )

    email_pairs, rest_drac, rest_ldap = pair_by_email(drac_pool, ldap_pool)
    candidates, single_clue = build_candidates(rest_drac, rest_ldap)
    accepted, ambiguous = select_unique(candidates)

    print(
        f"{len(email_pairs)} paires par email, {len(accepted)} paires par nom + "
        f"recoupement prof/superviseur, {len(ambiguous)} cas ambigus, "
        f"{len(single_clue)} candidats à un seul indice",
        file=sys.stderr,
    )
    for c in sorted(email_pairs + accepted, key=lambda c: c.drac.username):
        print(
            f"  {c.drac.label():38} -> {c.ldap.label():42} ({c.reason})",
            file=sys.stderr,
        )
    if not accepted and not email_pairs:
        print("aucune paire trouvée", file=sys.stderr)

    pairs = email_pairs + accepted
    out = Path(args.out)
    out.write_text(
        json.dumps(to_json(pairs), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"{out} écrit ({len(pairs)} paires)")

    if args.report:
        Path(args.report).write_text(
            report(accepted, ambiguous, email_pairs, single_clue, stats),
            encoding="utf-8",
        )
        print(
            f"{args.report} écrit ({len(ambiguous)} cas ambigus, "
            f"{len(single_clue)} candidats à un seul indice)"
        )


if __name__ == "__main__":
    main()
