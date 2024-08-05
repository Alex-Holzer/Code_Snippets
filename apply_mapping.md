```python

from pyspark.sql import DataFrame
from typing import Any, Callable

def validate_dictionary(func: Callable) -> Callable:
    """
    Decorator to validate that the input is a non-empty dictionary.
    
    Args:
        func (Callable): The function to be decorated.
    
    Returns:
        Callable: The wrapped function.
    """
    def wrapper(arg: Any) -> None:
        if not isinstance(arg, dict):
            raise TypeError(f"Expected dictionary, got {type(arg).__name__}")
        if not arg:
            raise ValueError("Dictionary is empty")
        return func(arg)
    return wrapper

def validate_pyspark_dataframe(func: Callable) -> Callable:
    """
    Decorator to validate that the input is a non-empty PySpark DataFrame.
    
    Args:
        func (Callable): The function to be decorated.
    
    Returns:
        Callable: The wrapped function.
    """
    def wrapper(arg: Any) -> None:
        if not isinstance(arg, DataFrame):
            raise TypeError(f"Expected PySpark DataFrame, got {type(arg).__name__}")
        if arg.rdd.isEmpty():
            raise ValueError("DataFrame is empty")
        return func(arg)
    return wrapper

# Test functions
def test_validate_dictionary():
    @validate_dictionary
    def dummy_dict_func(d):
        return "Valid dictionary"

    # Test valid dictionary
    assert dummy_dict_func({"key": "value"}) == "Valid dictionary"

    # Test invalid type
    try:
        dummy_dict_func([1, 2, 3])
    except TypeError as e:
        assert str(e) == "Expected dictionary, got list"
    else:
        assert False, "TypeError not raised"

    # Test empty dictionary
    try:
        dummy_dict_func({})
    except ValueError as e:
        assert str(e) == "Dictionary is empty"
    else:
        assert False, "ValueError not raised"

def test_validate_pyspark_dataframe():
    class MockDataFrame:
        def __init__(self, is_empty=False):
            self.rdd = type('MockRDD', (), {'isEmpty': lambda: is_empty})()

    @validate_pyspark_dataframe
    def dummy_df_func(df):
        return "Valid DataFrame"

    # Test valid DataFrame
    assert dummy_df_func(MockDataFrame()) == "Valid DataFrame"

    # Test invalid type
    try:
        dummy_df_func([1, 2, 3])
    except TypeError as e:
        assert str(e) == "Expected PySpark DataFrame, got list"
    else:
        assert False, "TypeError not raised"

    # Test empty DataFrame
    try:
        dummy_df_func(MockDataFrame(is_empty=True))
    except ValueError as e:
        assert str(e) == "DataFrame is empty"
    else:
        assert False, "ValueError not raised"

# Run tests
if __name__ == "__main__":
    test_validate_dictionary()
    test_validate_pyspark_dataframe()
    print("All tests passed!")

```

