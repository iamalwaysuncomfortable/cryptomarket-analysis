import pytest
from custom_utils.type_validation import validate_type

def test_validate_single_erroneous_type_comparison():
    with pytest.raises(TypeError):
        validate_type("a", int)

def test_validate_multiple_erroneous_type_comparisons():
    with pytest.raises(TypeError):
        validate_type({1, 2}, (list, tuple))

def test_validate_multiple_successful_type_comparisons():
    validate_type(1, (int, str, list))
    validate_type((1,2), (int, str, tuple, list))