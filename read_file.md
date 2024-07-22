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


```
