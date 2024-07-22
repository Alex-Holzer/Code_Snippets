```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any

def add_mapped_column(
    df: DataFrame, 
    source_column: str, 
    target_column: str, 
    mapping_dict: Dict[Any, Any]
) -> DataFrame:
    """
    Add a new column to a DataFrame based on mapping values from a source column using a dictionary.
    If a value is not in the dictionary, it will be set to None in the new column.

    Args:
        df (DataFrame): The input DataFrame.
        source_column (str): The name of the column to map from.
        target_column (str): The name of the new column to create.
        mapping_dict (Dict[Any, Any]): A dictionary mapping source values to target values.

    Returns:
        DataFrame: A new DataFrame with the additional mapped column.

    Example:
        >>> df = spark.createDataFrame([('A',), ('B',), ('C',)], ['letter'])
        >>> mapping_dict = {'A': 1, 'B': 2}
        >>> result_df = add_mapped_column(df, 'letter', 'letter_mapped', mapping_dict)
        >>> result_df.show()
        +------+-------------+
        |letter|letter_mapped|
        +------+-------------+
        |     A|            1|
        |     B|            2|
        |     C|         null|
        +------+-------------+
    """
    # Create a list of when-otherwise conditions
    conditions = [F.when(F.col(source_column) == F.lit(k), F.lit(v)) for k, v in mapping_dict.items()]
    
    # Add a final otherwise condition to set values not in the dict to None
    otherwise = F.when(F.lit(True), F.lit(None))
    
    # Apply the mapping to create a new column
    return df.withColumn(target_column, F.coalesce(*conditions, otherwise))

# Example usage
if __name__ == "__main__":
    # Create a sample DataFrame
    data = [('A',), ('B',), ('C',), ('D',)]
    df = spark.createDataFrame(data, ['letter'])

    # Define a mapping dictionary
    mapping_dict = {'A': 1, 'B': 2, 'C': 3}

    # Apply the mapping to create a new column
    result_df = add_mapped_column(df, 'letter', 'letter_mapped', mapping_dict)

    # Show the result
    result_df.show()

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Union

def validate_input(df: DataFrame, columns: Union[str, List[str]], from_sep: str, to_sep: str) -> None:
    """
    Validate input parameters for the replace_decimal_separator function.
    
    Args:
        df (DataFrame): Input DataFrame.
        columns (Union[str, List[str]]): Column(s) to process.
        from_sep (str): Current decimal separator.
        to_sep (str): New decimal separator.
    
    Raises:
        ValueError: If input parameters are invalid.
    """
    if not isinstance(df, DataFrame):
        raise ValueError("Input must be a PySpark DataFrame.")
    
    if isinstance(columns, str):
        columns = [columns]
    elif not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise ValueError("Columns must be a string or a list of strings.")
    
    if not set(columns).issubset(df.columns):
        raise ValueError("Specified columns not found in the DataFrame.")
    
    if from_sep not in {',', '.'} or to_sep not in {',', '.'} or from_sep == to_sep:
        raise ValueError("Invalid separator values. Use ',' or '.' and ensure they are different.")

def replace_in_column(column: str, from_sep: str, to_sep: str) -> F.Column:
    """
    Create a column expression to replace the decimal separator in a single column.
    
    Args:
        column (str): Name of the column to process.
        from_sep (str): Current decimal separator.
        to_sep (str): New decimal separator.
    
    Returns:
        F.Column: Transformed column expression.
    """
    return F.regexp_replace(F.col(column), f"\\{from_sep}", to_sep).alias(column)

def replace_decimal_separator(df: DataFrame, columns: Union[str, List[str]], from_sep: str = ',', to_sep: str = '.') -> DataFrame:
    """
    Replace decimal separator in specified columns of a DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame.
        columns (Union[str, List[str]]): Column(s) to process.
        from_sep (str, optional): Current decimal separator. Defaults to ','.
        to_sep (str, optional): New decimal separator. Defaults to '.'.
    
    Returns:
        DataFrame: Transformed DataFrame with updated decimal separators.
    """
    validate_input(df, columns, from_sep, to_sep)
    
    if isinstance(columns, str):
        columns = [columns]
    
    def transform_columns(df: DataFrame) -> DataFrame:
        select_expr = [
            replace_in_column(col, from_sep, to_sep) if col in columns else F.col(col)
            for col in df.columns
        ]
        return df.select(*select_expr)
    
    return df.transform(transform_columns)

# Example usage
def example_usage(spark):
    # Create a sample DataFrame
    data = [("1,5", "2.7"), ("3,14", "4,2")]
    df = spark.createDataFrame(data, ["col1", "col2"])
    
    # Apply the transformation
    df_transformed = replace_decimal_separator(df, ["col1", "col2"], from_sep=",", to_sep=".")
    
    df_transformed.show()

# Note: The example_usage function is just for demonstration and should be removed in production code.


```
