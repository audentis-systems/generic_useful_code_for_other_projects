import numpy as np

def int_to_str_with_left_zero_padding(an_integer : int, number_of_decimal_places = 5):
    max_per_given_number_of_decimal_points = np.int64(''.join(['9'] * number_of_decimal_places))
    assert an_integer <= max_per_given_number_of_decimal_points
    zeros_to_pad = ''.join(['0'] * (number_of_decimal_places - len(str(an_integer))))
    return zeros_to_pad + str(an_integer)
