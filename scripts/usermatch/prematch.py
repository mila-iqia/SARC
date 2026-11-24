#!/usr/bin/env python3
"""Pre-generate a user-matching file for scripts/usermatch/main.py.

The two pools are those of the "Match DRAC" interface:
  - users who have a DRAC account (username) but no Mila LDAP account;
  - Mila LDAP users who have no DRAC account.

Matching criterion: the person's **name**, cross-checked with the group
they belong to:

  - the name of the professors (PIs) of the tamia ``aip-*`` allocations the DRAC
    account is a member of — fetched live from tamia (``sacctmgr`` for the
    allocations and members, ``getent passwd`` for the owner's full name);
  - the SARC supervisors of the LDAP account (``user_supervisors`` /
    ``supervisorshelper`` tables, fed by MyMila).

The "group professor" <-> "supervisor" cross-check is first done on SARC ids
(the owner's username is looked up in ``credentialsdb``), and otherwise on
names. A DRAC account that owns its own allocation (``aip-<username>``) is
thus matched with the LDAP account without supervisor that has the same name.

A first pass matches accounts sharing the same email.

Only **unambiguous** pairs (best candidate in both directions) are written to
the file. The rest (ties, non-reciprocal candidates, single clues) is left for
the ``--report`` report, to be decided by hand in the interface.

The output format is exactly the one downloaded by ``scripts/usermatch/main.py``
(``usermatches.json`` file); it can be reloaded with the "Load from file…" button:

    [
      {
        "matching_id": { "name": "drac_username", "mid": "jdoe" },
        "known_matches": [{ "name": "mila_ldap", "mid": "john.doe@mila.quebec" }]
      }
    ]

Usage:
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

# name similarity scores
NAME_EXACT = 3  # identical normalized names
NAME_SUBSET = 2  # all words of the shorter name found
NAME_LOOSE = 1  # one unmatched word, but only an initial

# group cross-check scores (aip-* allocation professor / SARC supervisor)
GROUP_ID = 3  # the professor and the supervisor are the same row of the users table
GROUP_PI_SELF = 3  # DRAC account owns its allocation, LDAP has no supervisor
GROUP_NAME = 2  # professor name == a supervisor name, without a match in the db
EMAIL_SCORE = 10  # direct match, without group condition


# ---------------------------------------------------------------------------
# names: normalization and similarity
# ---------------------------------------------------------------------------


def norm(s: str) -> str:
    """lowercase, no accents, punctuation -> spaces"""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


@cache
def tokens(name: str) -> tuple[str, ...]:
    return tuple(norm(name).split())


def edit_distance(a: str, b: str) -> int:
    """Levenshtein distance (names are short, a DP is enough)."""
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
    """Two equivalent name words: identical, initial, or small typo."""
    if a == b:
        return True
    if len(a) <= 1 or len(b) <= 1:
        return (len(a) == 1 and b.startswith(a)) or (len(b) == 1 and a.startswith(b))
    shortest = min(len(a), len(b))
    # one typo allowed from 4 letters, two for long names; short words
    # (initials, particles) must match exactly
    allowed = 2 if shortest >= 7 else 1 if shortest >= 4 else 0
    return edit_distance(a, b) <= allowed


def match_words(short: tuple[str, ...], long: tuple[str, ...]) -> set[int]:
    """Indices in `short` of the words that can be found, each on a different word
    of `long`: exact equivalences first, so as not to use up candidates for
    fuzzy matching."""
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
    """The surname (last word) is found on both sides."""
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
    """Similarity score (NAME_*), or None if the names don't match.

    The surname must be shared AND at least one first name too: a shared surname
    alone is never enough, groups have many namesakes.
    """
    ta, tb = tokens(drac_name), tokens(ldap_name)
    if ta and ta == tb:
        return NAME_EXACT
    if not ta or not tb or not shares_surname(drac_name, ldap_name):
        return None
    short, long = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    hit = match_words(short, long)
    if len(hit) < 2 or (len(short) - 1) not in hit:  # surname + one first name
        return None
    unmatched = [word for i, word in enumerate(short) if i not in hit]
    if not unmatched:
        return NAME_SUBSET
    if len(unmatched) == 1 and len(unmatched[0]) <= 2:  # a middle name/an initial
        return NAME_LOOSE
    return None


def same_person(a: str, b: str) -> bool:
    """Do two names refer to the same person (surname + one first name)?

    Requires the strict level: used to check that an aip-* allocation professor
    and a SARC supervisor are the same person, not to match two accounts.
    """
    score = name_score(a, b)
    return score is not None and score >= NAME_SUBSET


# ---------------------------------------------------------------------------
# collecting aip-* allocations on tamia
# ---------------------------------------------------------------------------

# A single round trip: aip-* owners, members of these allocations, then
# the full name (gecos) of all the usernames encountered.
REMOTE_DUMP = """
accts=$(sacctmgr -nP list account Format=Account | sed -n "s/^aip-//p")
for u in $accts; do
  echo "PI|$u|$(getent passwd "$u" | cut -d: -f5)"
done
f=/tmp/aip_assoc.$$
sacctmgr -nP list assoc Format=Account,User 2>/dev/null \\
  | awk -F"|" 'NR>1 && $1 ~ /^aip-/ && $2 != "" && $2 != "None" {print "ASSOC|"$1"|"$2}' > "$f"
if [ ! -s "$f" ]; then
  # global query unavailable: one query per allocation
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
    """aip-* allocations, their owners and their members."""

    pis: dict[str, str] = field(default_factory=dict)  # PI username -> gecos
    gecos: dict[str, str] = field(default_factory=dict)  # username -> full name
    allocations: dict[str, set[str]] = field(default_factory=dict)  # username -> aip-*


def ssh(socket: str, host: str, script: str) -> str:
    """Run a bash script on the cluster through the control master, return stdout."""
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
    """Reuse the control master if it responds, otherwise connect (a single
    password + MFA prompt; following commands reuse the channel)."""
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
    """aip-* allocations, owners and members, in a single SSH round trip."""
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
    """Full name of the owner of an aip-<pi_username> allocation."""
    key = pi_username.lower()
    return tamia.pis.get(key) or tamia.gecos.get(key) or ""


# ---------------------------------------------------------------------------
# pools: DRAC-only accounts and LDAP-only accounts (same rules as main.py)
# ---------------------------------------------------------------------------


@dataclass
class DracAccount:
    user_id: int
    display_name: str
    email: str
    username: str
    cci: str | None
    allocations: set[str] = field(default_factory=set)
    pi_ids: set[int] = field(default_factory=set)  # SARC ids of the allocations PIs
    pis: dict[str, str] = field(default_factory=dict)  # PI username -> name (gecos)
    is_pi: bool = False  # owns its own aip-<username> allocation

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
    supervisors: dict[int, str] = field(default_factory=dict)  # SARC id -> name

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
    """{student id: [SARC ids of supervisors]}, lists valid now."""
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
    """Build the two pools and attach the groups to them (allocations, supervisors)."""
    with config.db.session() as sess:
        drac_usernames = load_drac_usernames(sess)
        supervisors = load_current_supervisors(sess)
        # matching_ids is lazy-loaded: read everything before closing the session
        users: list[tuple[int, str, str, dict[str, str]]] = []
        for u in tqdm(
            sess.exec(select(UserDB)), desc="Chargement des users", unit="user"
        ):
            if u.id is None:
                continue
            users.append((u.id, u.display_name, u.email or "", dict(u.matching_ids)))

    # username -> SARC ids; a username found several times is not a reliable
    # identity, it is not used for the cross-check by id
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
# matching
# ---------------------------------------------------------------------------


@dataclass(eq=False)
class Candidate:
    drac: DracAccount
    ldap: LdapAccount
    score: int
    reason: str
    note: str = ""  # for candidates not kept


def group_evidence(drac: DracAccount, ldap: LdapAccount) -> tuple[int, str] | None:
    """Is the professor of the DRAC account's aip-* allocation a supervisor of the LDAP account?"""
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
    """Eligible candidates (name + group), and single-clue candidates for the report."""
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
    """Keep the pairs that are the best candidate in both directions."""
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
    """First pass: identical emails, when there is only one candidate."""
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
# output
# ---------------------------------------------------------------------------


def to_json(pairs: list[Candidate]) -> list[dict]:
    """usermatches.json format of scripts/usermatch/main.py."""
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
