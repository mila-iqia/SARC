"""
Generate manual plugin YAML config by matching Mila LDAP users to DRAC
accounts using a name-distance heuristic.

Output (stdout):
    YAML id_pairs block for the manual user scraper config.
"""

import argparse
import csv
import json
from collections import defaultdict
from typing import Iterable


def main():
    parser = argparse.ArgumentParser(
        description="Generate manual plugin YAML config from DRAC/Mila LDAP name matching."
    )
    parser.add_argument(
        "--mila-ldap",
        required=True,
        help="JSON array of LDAP attribute dicts (as cached by MilaLDAPScraper)",
    )
    parser.add_argument(
        "--drac-members",
        required=True,
        help="DRAC members CSV (columns: Name, Email, Username, CCRI, ...)",
    )
    parser.add_argument(
        "--drac-roles", help="DRAC roles CSV (columns: Nom, Email, CCRI, ...)"
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=2,
        help="Name distance threshold for matching (default: 2)",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive/deactivated DRAC accounts (default: only activated)",
    )
    args = parser.parse_args()

    with open(args.mila_ldap) as f:
        ldap_raw = json.load(f)

    try:
        mila_users = [
            {"display_name": entry["displayName"][0], "email": entry["mail"][0]}
            for entry in ldap_raw
        ]
    except KeyError:
        # Try to parse old mila_ldap JSON files used with previous account matching heuristic
        mila_users = [
            {
                "display_name": entry["display_name"],
                "email": entry["mila_email_username"],
            }
            for entry in ldap_raw
        ]

    # (scraper name, name field in CSV, list of entries)
    drac_sources: list[tuple[str, str, list[dict[str, str]]]] = []

    with open(args.drac_members) as f:
        members = [{k.lower(): v for k, v in d.items()} for d in csv.DictReader(f)]
        if not args.include_inactive:
            members = [
                d
                for d in members
                if d.get("activation_status", "").lower() == "activated"
            ]
        drac_sources.append(("drac_member", "name", members))

    if args.drac_roles:
        with open(args.drac_roles) as f:
            roles = [{k.lower(): v for k, v in d.items()} for d in csv.DictReader(f)]
            if not args.include_inactive:
                roles = [d for d in roles if d.get("status", "").lower() == "activated"]
            drac_sources.append(("drac_role", "nom", roles))

    idx = 0
    print("id_pairs:")  # noqa: T201
    for source_name, name_field, drac_entries in drac_sources:
        best_matches = find_best_word_matches(
            [u["display_name"] for u in mila_users],
            [e[name_field] for e in drac_entries],
            nb_best_matches=10,
        )

        for mila_name, candidates in best_matches:
            under_threshold = [m for m in candidates if m[0] <= args.threshold]
            if len(under_threshold) != 1:
                continue

            drac_name = under_threshold[0][1]
            mila_user = next(u for u in mila_users if u["display_name"] == mila_name)
            drac_entry = next(e for e in drac_entries if e[name_field] == drac_name)
            cci = drac_entry["ccri"][:-3]

            drac_email = drac_entry.get("email", "")
            print(  # noqa: T201
                f"  a{idx}:  # [mila] {mila_name} <-> [drac] {drac_name} ({drac_email})\n"
                f"    - name: mila_ldap\n"
                f"      mid: {mila_user['email']}\n"
                f"    - name: {source_name}\n"
                f"      mid: {cci}\n"
            )
            idx += 1


#################### Matching functions from old heuristic, used to match accounts in main function ####################


def find_best_word_matches(
    L_names_A: Iterable[str], L_names_B: Iterable[str], nb_best_matches: int = 10
) -> list[tuple[str, list[tuple[int, str]]]]:
    """Get the `nb_best_matches` values from L_names_B closest to values in `L_names_A`.

    Return a list of couples, each with format:
        (value_from_A, best_comparisons)

        `best_comparisons` is a sorted list of `nb_best_matches` couples
        with format (threshold, value_from_B)
    """
    # NB: in next line, L_names_A is sorted to make matching pipeline more predictable.
    LP_names_A = [(a, bag_of_words_projection(a)) for a in sorted(L_names_A)]
    LP_names_B = [(b, bag_of_words_projection(b)) for b in L_names_B]
    LP_results: list[tuple[str, list[tuple[int, str]]]] = []
    for a, bow_A in LP_names_A:
        comparisons = sorted(
            ((bow_distance(bow_A, bow_B), b) for b, bow_B in LP_names_B)
        )
        LP_results.append((a, comparisons[:nb_best_matches]))
    return LP_results


def bag_of_words_projection(name: str) -> dict[str, int]:
    letter_counts: dict[str, int] = defaultdict(int)
    for c in name:
        if c in " -":
            # skipping spaces and hyphens
            pass
        c = c.lower()
        if c in "éèëê":
            c = "e"
        if c in "ç":
            c = "c"
        if c in "îï":
            c = "i"
        letter_counts[c] += 1
    return letter_counts


def bow_distance(bow_A: dict[str, int], bow_B: dict[str, int]) -> int:
    """
    For each letter, add how much the counts differ.
    Return the total.
    """
    distance = 0
    all_letters = set(bow_A.keys()) | set(bow_B.keys())
    for k in all_letters:
        distance += abs(bow_A[k] - bow_B[k])
    return distance


if __name__ == "__main__":
    main()
