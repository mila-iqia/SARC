from collections import defaultdict


def bag_of_words_projection(name):
    letter_counts = defaultdict(int)
    for c in name:
        if c in " -":
            # skipping spaces and hiphens
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


def bow_distance(bow_A, bow_B):
    """
    For each letter, add how much the counts differ.
    Return the total.
    """
    distance = 0
    all_letters = set(bow_A.keys()) | set(bow_B.keys())
    for k in all_letters:
        distance += abs(bow_A[k] - bow_B[k])
    return distance


def find_exact_bag_of_words_matches(L_names_A, L_names_B, delta_threshold=1):
    """
    Find pairs of names that are close enough to be considered the same person.
    Comparisons are done by counting the occurrences of letters in each name,
    ignoring spaces, hyphens, accents and capitalization.
    Some `delta_threshold` value is used to determine the cutoff.
    """

    LP_names_A = [(a, bag_of_words_projection(a)) for a in L_names_A]
    LP_names_B = [(b, bag_of_words_projection(b)) for b in L_names_B]

    # O(N^2) is wasteful, but maybe it's fine with small quantities,
    # or if we run this occasionally for new accounts only,
    # or if we rule out the already-established associations
    LP_results = []
    for a, bow_A in LP_names_A:
        for b, bow_B in LP_names_B:
            delta = bow_distance(bow_A, bow_B)
            if delta <= delta_threshold:
                # print(f"{a}, {b}")
                LP_results.append((a, b, delta))

    # We can't do that like this because we're invaliding
    # the matching that we did with some tolerance.
    # If "Amirjohn Appleseed" and "Amir John Appleseed" match,
    # then we can't compare the exact sets and be shocked
    # when there's a discrepancy.
    #
    # S_names_A = set(list(zip(*LP_results))[0])
    # S_names_B = set(list(zip(*LP_results))[1])
    # for missing_name in S_names_A.difference(S_names_B):
    #    print(f"We have name {missing_name} missing from one.")
    # for missing_name in S_names_B.difference(S_names_A):
    #    print(f"We have name {missing_name} missing from one.")
    #
    # assert len(set(list(zip(*LP_results))[0])) == len(set(list(zip(*LP_results))[1])), (
    #    "We have a big problem in the name matching because some name matched to more than one.\n"
    #    "This should really be a one-to-one correspondance, or otherwise we shouldn't be doing "
    #    "this matching.\n"
    #    "We should be more careful and omit those multiple matches by altering this function."
    # )
    return LP_results


def find_best_word_matches(L_names_A, L_names_B, nb_best_matches=10):
    """Get the `nb_best_matches` values from L_names_B closest to values in `L_names_A`.
    Use same comparison function as in `find_exact_bag_of_words_matches`.

    Return a list of couples, each with format:
        (value_from_A, best_comparisons)

        `best_comparisons` is a sorted list of `nb_best_matches` couples
        with format (threshold, value_from_B)
    """
    # NB: in next line, L_names_A is sorted to make matching pipeline more predictable.
    LP_names_A = [(a, bag_of_words_projection(a)) for a in sorted(L_names_A)]
    LP_names_B = [(b, bag_of_words_projection(b)) for b in L_names_B]
    LP_results = []
    for a, bow_A in LP_names_A:
        comparisons = sorted(
            ((bow_distance(bow_A, bow_B), b) for b, bow_B in LP_names_B),
        )
        LP_results.append((a, comparisons[:nb_best_matches]))
    return LP_results
