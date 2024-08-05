```python

from typing import Any, List

def validate_list(arg: Any) -> None:
    """
    Validate that the input is a non-empty list.
    
    Args:
        arg (Any): The argument to validate.
    
    Raises:
        TypeError: If the input is not a list.
        ValueError: If the list is empty.
    """
    if not isinstance(arg, list):
        raise TypeError(f"Expected list, got {type(arg).__name__}")
    if not arg:
        raise ValueError("List is empty")

def validate_boolean(arg: Any) -> None:
    """
    Validate that the input is a boolean.
    
    Args:
        arg (Any): The argument to validate.
    
    Raises:
        TypeError: If the input is not a boolean.
    """
    if not isinstance(arg, bool):
        raise TypeError(f"Expected boolean, got {type(arg).__name__}")

# Test functions
def test_validate_list():
    # Test valid list
    try:
        validate_list([1, 2, 3])
    except Exception as e:
        assert False, f"Unexpected exception raised: {e}"

    # Test invalid type
    try:
        validate_list({"key": "value"})
        assert False, "TypeError not raised for non-list input"
    except TypeError as e:
        assert str(e) == "Expected list, got dict"

    # Test empty list
    try:
        validate_list([])
        assert False, "ValueError not raised for empty list"
    except ValueError as e:
        assert str(e) == "List is empty"

def test_validate_boolean():
    # Test valid boolean
    try:
        validate_boolean(True)
        validate_boolean(False)
    except Exception as e:
        assert False, f"Unexpected exception raised: {e}"

    # Test invalid type
    try:
        validate_boolean(1)
        assert False, "TypeError not raised for non-boolean input"
    except TypeError as e:
        assert str(e) == "Expected boolean, got int"

    # Test with string 'True'
    try:
        validate_boolean("True")
        assert False, "TypeError not raised for string 'True'"
    except TypeError as e:
        assert str(e) == "Expected boolean, got str"

# Run tests
if __name__ == "__main__":
    test_validate_list()
    test_validate_boolean()
    print("All tests passed!")


```

