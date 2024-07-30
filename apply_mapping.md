```python

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Tuple

def get_unique_values(df: DataFrame, column: str) -> List:
    """
    Get unique values from a specific column in a DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame
        column (str): Column name to get unique values from
    
    Returns:
        List: List of unique values
    """
    return [row[column] for row in df.select(column).distinct().collect()]

def find_missing_values(df1_values: List, df2_values: List) -> List:
    """
    Find values that are in df2 but not in df1.
    
    Args:
        df1_values (List): List of values from first DataFrame
        df2_values (List): List of values from second DataFrame
    
    Returns:
        List: List of missing values
    """
    return list(set(df2_values) - set(df1_values))

def format_output_message(missing_values: List) -> Tuple[bool, str]:
    """
    Format the output message based on missing values.
    
    Args:
        missing_values (List): List of missing values
    
    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating test pass/fail and the formatted message
    """
    if not missing_values:
        return True, "🎉 Great news! All values from the second DataFrame are present in the first. Test passed! 🎉"
    else:
        examples = missing_values[:5]
        return False, (f"❌ Oops! We found some values in the second DataFrame that are missing from the first. "
                       f"Here are up to 5 examples: {examples} ❌")

def compare_dataframe_columns(df1: DataFrame, df2: DataFrame, col1: str, col2: str) -> Tuple[bool, str]:
    """
    Compare column values between two DataFrames.
    
    Args:
        df1 (DataFrame): First DataFrame
        df2 (DataFrame): Second DataFrame
        col1 (str): Column name in first DataFrame
        col2 (str): Column name in second DataFrame
    
    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating test pass/fail and the comparison result message
    """
    df1_values = get_unique_values(df1, col1)
    df2_values = get_unique_values(df2, col2)
    missing_values = find_missing_values(df1_values, df2_values)
    return format_output_message(missing_values)



def compare_dataframe_columns(df1: DataFrame, df2: DataFrame, col1: str, col2: str) -> Tuple[bool, str]:
    """
    Compare column values between two DataFrames to check if all values from the second DataFrame's column
    are present in the first DataFrame's column.

    This function is useful for data validation, ensuring data completeness, or checking for discrepancies
    between two datasets. It compares unique values in the specified columns of both DataFrames.

    Args:
        df1 (DataFrame): The first PySpark DataFrame. This is considered the "master" or "complete" dataset.
        df2 (DataFrame): The second PySpark DataFrame. This is the dataset being checked against df1.
        col1 (str): The name of the column in df1 to compare. Must exist in df1.
        col2 (str): The name of the column in df2 to compare. Must exist in df2.

    Returns:
        Tuple[bool, str]: A tuple containing two elements:
            - bool: True if all values in df2[col2] are present in df1[col1], False otherwise.
            - str: A formatted message describing the result of the comparison.
              If successful, it returns a celebratory message.
              If there are missing values, it returns an error message with up to 5 examples of missing values.

    Raises:
        ValueError: If either col1 is not in df1 or col2 is not in df2.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> 
        >>> # Create sample DataFrames
        >>> df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value"])
        >>> df2 = spark.createDataFrame([(1, "A"), (2, "B"), (4, "D")], ["id", "value"])
        >>> 
        >>> # Compare 'value' columns
        >>> result, message = compare_dataframe_columns(df1, df2, "value", "value")
        >>> print(f"Comparison result: {result}")
        >>> print(f"Message: {message}")
        Comparison result: False
        Message: ❌ Oops! We found some values in the second DataFrame that are missing from the first. Here are up to 5 examples: ['D'] ❌

    Note:
        - This function only compares the presence of values, not their frequency or order.
        - The comparison is case-sensitive for string values.
        - The function assumes that the columns contain comparable data types.
        - Large DataFrames may impact performance, as the function collects all unique values to the driver.

    See Also:
        get_unique_values: Helper function to extract unique values from a DataFrame column.
        find_missing_values: Helper function to identify values present in one list but not another.
        format_output_message: Helper function to create user-friendly output messages.
    """
    if col1 not in df1.columns:
        raise ValueError(f"Column '{col1}' not found in the first DataFrame.")
    if col2 not in df2.columns:
        raise ValueError(f"Column '{col2}' not found in the second DataFrame.")

    df1_values = get_unique_values(df1, col1)
    df2_values = get_unique_values(df2, col2)
    missing_values = find_missing_values(df1_values, df2_values)
    return format_output_message(missing_values)

```
