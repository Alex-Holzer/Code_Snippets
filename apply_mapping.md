```python

from datetime import datetime, date
from typing import Union, Any
import numbers

def validate_number(value: Any) -> None:
    """
    Validate that the input is a number (int or float).

    Args:
        value (Any): The value to validate.

    Raises:
        TypeError: If the value is not a number.
    """
    if not isinstance(value, numbers.Number):
        raise TypeError(f"Expected a number, got {type(value).__name__}")

def validate_timestamp(value: Any) -> None:
    """
    Validate that the input is a valid timestamp.

    Args:
        value (Any): The value to validate.

    Raises:
        TypeError: If the value is not a datetime object or a string.
        ValueError: If the string cannot be parsed as a timestamp.
    """
    if isinstance(value, datetime):
        return
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {value}")
    else:
        raise TypeError(f"Expected datetime or string, got {type(value).__name__}")

def validate_date(value: Any) -> None:
    """
    Validate that the input is a valid date.

    Args:
        value (Any): The value to validate.

    Raises:
        TypeError: If the value is not a date object or a string.
        ValueError: If the string cannot be parsed as a date.
    """
    if isinstance(value, date):
        return
    if isinstance(value, str):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {value}. Expected format: YYYY-MM-DD")
    else:
        raise TypeError(f"Expected date or string, got {type(value).__name__}")

def validate_negative(value: Union[int, float]) -> None:
    """
    Validate that the input is a negative number.

    Args:
        value (Union[int, float]): The value to validate.

    Raises:
        TypeError: If the value is not a number.
        ValueError: If the number is not negative.
    """
    if not isinstance(value, numbers.Number):
        raise TypeError(f"Expected a number, got {type(value).__name__}")
    if value >= 0:
        raise ValueError(f"Expected a negative number, got {value}")

# Test functions
def test_validate_number():
    validate_number(42)
    validate_number(3.14)
    try:
        validate_number("42")
        assert False, "TypeError not raised for string input"
    except TypeError as e:
        assert str(e) == "Expected a number, got str"

def test_validate_timestamp():
    validate_timestamp(datetime.now())
    validate_timestamp("2023-05-17T14:30:00")
    try:
        validate_timestamp("2023-05-17")
        assert False, "ValueError not raised for invalid timestamp format"
    except ValueError as e:
        assert str(e) == "Invalid timestamp format: 2023-05-17"
    try:
        validate_timestamp(42)
        assert False, "TypeError not raised for non-datetime/string input"
    except TypeError as e:
        assert str(e) == "Expected datetime or string, got int"

def test_validate_date():
    validate_date(date.today())
    validate_date("2023-05-17")
    try:
        validate_date("17-05-2023")
        assert False, "ValueError not raised for invalid date format"
    except ValueError as e:
        assert str(e) == "Invalid date format: 17-05-2023. Expected format: YYYY-MM-DD"
    try:
        validate_date(42)
        assert False, "TypeError not raised for non-date/string input"
    except TypeError as e:
        assert str(e) == "Expected date or string, got int"

def test_validate_negative():
    validate_negative(-42)
    validate_negative(-3.14)
    try:
        validate_negative(0)
        assert False, "ValueError not raised for zero"
    except ValueError as e:
        assert str(e) == "Expected a negative number, got 0"
    try:
        validate_negative(42)
        assert False, "ValueError not raised for positive number"
    except ValueError as e:
        assert str(e) == "Expected a negative number, got 42"
    try:
        validate_negative("42")
        assert False, "TypeError not raised for string input"
    except TypeError as e:
        assert str(e) == "Expected a number, got str"

# Run tests
if __name__ == "__main__":
    test_validate_number()
    test_validate_timestamp()
    test_validate_date()
    test_validate_negative()
    print("All tests passed!")

```

