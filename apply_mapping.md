```python

import unittest
from pyspark.sql import DataFrame
from unittest.mock import Mock
from typing import Any, Dict

# Import the functions to be tested
from your_module import validate_pyspark_dataframe, validate_dictionary, validate_args

class MockDataFrame(DataFrame):
    def __init__(self, is_empty=False):
        self.is_empty = is_empty

    @property
    def rdd(self):
        return Mock(isEmpty=lambda: self.is_empty)

class TestValidateArgsDecorator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        @validate_args(validate_pyspark_dataframe, validate_dictionary)
        def dummy_function(df: DataFrame, params: Dict[str, Any]) -> str:
            return "Success"
        cls.dummy_function = dummy_function

    def test_valid_inputs(self):
        df = MockDataFrame()
        params = {"key": "value"}
        self.assertEqual(self.dummy_function(df, params), "Success")

    def test_invalid_dataframe(self):
        params = {"key": "value"}
        with self.assertRaises(ValueError) as context:
            self.dummy_function([1, 2, 3], params)
        self.assertIn("Validation failed for argument", str(context.exception))
        self.assertIn("Expected PySpark DataFrame", str(context.exception))

    def test_invalid_dictionary(self):
        df = MockDataFrame()
        with self.assertRaises(ValueError) as context:
            self.dummy_function(df, [1, 2, 3])
        self.assertIn("Validation failed for argument", str(context.exception))
        self.assertIn("Expected dictionary", str(context.exception))

    def test_with_keyword_arguments(self):
        df = MockDataFrame()
        params = {"key": "value"}
        self.assertEqual(self.dummy_function(df=df, params=params), "Success")

    def test_partial_application(self):
        df = MockDataFrame()
        with self.assertRaises(TypeError) as context:
            self.dummy_function(df)
        self.assertIn("missing 1 required positional argument", str(context.exception))

    def test_empty_dataframe(self):
        df = MockDataFrame(is_empty=True)
        params = {"key": "value"}
        with self.assertRaises(ValueError) as context:
            self.dummy_function(df, params)
        self.assertIn("DataFrame is empty", str(context.exception))

    def test_empty_dictionary(self):
        df = MockDataFrame()
        with self.assertRaises(ValueError) as context:
            self.dummy_function(df, {})
        self.assertIn("Dictionary is empty", str(context.exception))

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
```

