import pytest
from custom_utils.type_validation import validate_type
from custom_utils.datautils import boldprint

def test_validate_single_erroneous_type_comparison():
    boldprint("Ensure type comparison throws error if 2 different types are compared")
    with pytest.raises(TypeError):
        validate_type("a", int)

def test_validate_multiple_erroneous_type_comparisons():
    boldprint("Ensure type comparison throws error if input type doesn't match multiple types specified")
    with pytest.raises(TypeError):
        validate_type({1, 2}, (list, tuple))

def test_validate_multiple_successful_type_comparisons():
    boldprint("Ensure type specified matches one of input types")
    validate_type(1, (int, str, list))
    validate_type((1,2), (int, str, tuple, list))