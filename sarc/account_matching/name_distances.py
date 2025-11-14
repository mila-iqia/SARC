from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy

def bag_of_words_projection(name: str) -> dict[str, int]:
    word_counts: dict[str, int] = defaultdict(int)
    translation_table = str.maketrans({
        "-": " ",
        "é": "e",
        "è": "e",
        "ë": "e",
        "ê": "e",
        "ç": "c",
        "î": "i",
        "ï": "i",
        "à": "a",
        "â": "a",
        "ä": "a",
        "ô": "o",
        "ö": "o",
        "û": "u",
        "ü": "u",
        "ù": "u",
        "ú": "u",
        "ý": "y",
        "ÿ": "y",
        "œ": "oe",
        "æ": "ae",
        "ø": "o",
        "å": "a"})
    name = name.lower().translate(translation_table)
    words = name.split()
    for w in words:
        word_counts[w] += 1
    return word_counts


def bow_distance(bow_A: dict[str, int], bow_B: dict[str, int]) -> int:
    """
    For each letter, add how much the counts differ.
    Return the total.
    """
    bow_A = deepcopy(bow_A)
    bow_B = deepcopy(bow_B)
    distance = 0
    all_words = set(bow_A.keys()) | set(bow_B.keys())
    for k in all_words:
        distance += abs(bow_A[k] - bow_B[k])
    return distance


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
            ((bow_distance(bow_A, bow_B), b) for b, bow_B in LP_names_B),
        )
        LP_results.append((a, comparisons[:nb_best_matches]))
    return LP_results
