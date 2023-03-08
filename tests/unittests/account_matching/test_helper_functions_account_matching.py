from sarc.account_matching.name_distances import (
    bag_of_words_projection,
    bow_distance,
    find_exact_bag_of_words_matches,
)


def test_bag_of_words_nrur_al_din_ali():

    bow_distance(
        bag_of_words_projection("Nur al-Din Ali"),
        bag_of_words_projection("Nur Alialdin"),
    ) == 0
    # find_exact_bag_of_words_matches(L_names_A, L_names_B)


# def test_nrur_al_din_ali():
