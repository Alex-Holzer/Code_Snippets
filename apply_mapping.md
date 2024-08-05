```python

import unittest
from pyspark.sql import DataFrame
from unittest.mock import Mock
from typing import Any, Dict

# Import the functions to be tested
from your_module import validate_pyspark_dataframe, validate_dictionary, validate_args

# Mock DataFrame for testing
class MockDataFrame(DataFrame):
    def __init__(self, is_empty=False):
        self.is_empty = is_empty

    @property
    def rdd(self):
        return Mock(isEmpty=lambda: self.is_empty)

class TestValidationFunctions(unittest.TestCase):
    def test_validate_pyspark_dataframe_valid(self):
        df = MockDataFrame()
        try:
            validate_pyspark_dataframe(df)
        except Exception as e:
            self.fail(f"validate_pyspark_dataframe raised {type(e).__name__} unexpectedly!")

    def test_validate_pyspark_dataframe_invalid_type(self):
        with self.assertRaises(TypeError) as context:
            validate_pyspark_dataframe([1, 2, 3])
        self.assertIn("Expected PySpark DataFrame", str(context.exception))

    def test_validate_pyspark_dataframe_empty(self):
        df = MockDataFrame(is_empty=True)
        with self.assertRaises(ValueError) as context:
            validate_pyspark_dataframe(df)
        self.assertEqual(str(context.exception), "DataFrame is empty")

    def test_validate_dictionary_valid(self):
        d = {"key": "value"}
        try:
            validate_dictionary(d)
        except Exception as e:
            self.fail(f"validate_dictionary raised {type(e).__name__} unexpectedly!")

    def test_validate_dictionary_invalid_type(self):
        with self.assertRaises(TypeError) as context:
            validate_dictionary([1, 2, 3])
        self.assertIn("Expected dictionary", str(context.exception))

    def test_validate_dictionary_empty(self):
        with self.assertRaises(ValueError) as context:
            validate_dictionary({})
        self.assertEqual(str(context.exception), "Dictionary is empty")

    def test_validate_args_decorator(self):
        @validate_args(validate_pyspark_dataframe, validate_dictionary)
        def dummy_function(df: DataFrame, params: Dict[str, Any]) -> str:
            return "Success"

        df = MockDataFrame()
        params = {"key": "value"}

        # Test valid inputs
        self.assertEqual(dummy_function(df, params), "Success")

        # Test invalid DataFrame
        with self.assertRaises(ValueError) as context:
            dummy_function([1, 2, 3], params)
        self.assertIn("Validation failed for argument", str(context.exception))

        # Test invalid dictionary
        with self.assertRaises(ValueError) as context:
            dummy_function(df, [1, 2, 3])
        self.assertIn("Validation failed for argument", str(context.exception))

        # Test with keyword arguments
        self.assertEqual(dummy_function(df=df, params=params), "Success")

        # Test partial application
        with self.assertRaises(ValueError) as context:
            dummy_function(df)
        self.assertIn("Validation failed for argument", str(context.exception))

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)


```

