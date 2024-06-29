from pyspark.sql import DataFrame
from pyspark.sql.functions import col, create_map, lit
from pyspark.sql.types import StringType
from typing import List, Union

def create_case_key_lookup(df: DataFrame, case_key_col: str = "_CASE_KEY", hashed_key_col: str = "hashed_case_key") -> DataFrame:
    """
    Create a lookup DataFrame for reverse mapping hashed _CASE_KEYs to original _CASE_KEYs.

    This function creates a DataFrame with a map column that allows for efficient lookup
    of original _CASE_KEYs based on their hashed values.

    Args:
        df (DataFrame): Input DataFrame containing the _CASE_KEY and hashed_case_key columns.
        case_key_col (str, optional): Name of the case key column. Defaults to "_CASE_KEY".
        hashed_key_col (str, optional): Name of the hashed key column. Defaults to "hashed_case_key".

    Returns:
        DataFrame: A DataFrame with a single column 'lookup_map' containing a map from hashed keys to original keys.

    Example:
        >>> input_df = spark.createDataFrame([("A-123", 123), ("B-456", 456)], ["_CASE_KEY", "hashed_case_key"])
        >>> lookup_df = create_case_key_lookup(input_df)
        >>> lookup_df.show(truncate=False)
        +---------------------------------------+
        |lookup_map                             |
        +---------------------------------------+
        |{123 -> A-123, 456 -> B-456}           |
        +---------------------------------------+
    """
    return df.select(create_map(col(hashed_key_col), col(case_key_col)).alias("lookup_map"))

def lookup_original_case_keys(lookup_df: DataFrame, hashed_keys: Union[List[int], DataFrame]) -> DataFrame:
    """
    Look up original _CASE_KEYs based on their hashed values.

    This function takes a lookup DataFrame created by create_case_key_lookup()
    and a list of hashed keys or a DataFrame containing hashed keys, and returns
    the corresponding original _CASE_KEYs.

    Args:
        lookup_df (DataFrame): Lookup DataFrame created by create_case_key_lookup().
        hashed_keys (Union[List[int], DataFrame]): List of hashed keys or DataFrame containing hashed keys.

    Returns:
        DataFrame: A DataFrame with 'hashed_case_key' and 'original_case_key' columns.

    Example:
        >>> lookup_df = create_case_key_lookup(input_df)
        >>> hashed_keys = [123, 456, 789]
        >>> result_df = lookup_original_case_keys(lookup_df, hashed_keys)
        >>> result_df.show()
        +----------------+------------------+
        |hashed_case_key |original_case_key |
        +----------------+------------------+
        |123             |A-123             |
        |456             |B-456             |
        |789             |null              |
        +----------------+------------------+
    """
    # Get the lookup map from the lookup DataFrame
    lookup_map = lookup_df.select("lookup_map").first()["lookup_map"]

    # Create a DataFrame from the input hashed keys if a list is provided
    if isinstance(hashed_keys, list):
        hashed_keys_df = lookup_df.spark.createDataFrame([(k,) for k in hashed_keys], ["hashed_case_key"])
    else:
        hashed_keys_df = hashed_keys

    # Use the lookup map to get the original case keys
    return hashed_keys_df.withColumn("original_case_key", 
                                     lookup_map.getItem(col("hashed_case_key")).cast(StringType()))

# Usage example
def main(spark):
    # Sample data
    data = [
        ("5-ta", 1234567),
        ("A-123", 7654321),
        ("F-569", 9876543)
    ]
    
    columns = ["_CASE_KEY", "hashed_case_key"]
    
    df = spark.createDataFrame(data, columns)
    
    # Create the lookup DataFrame
    lookup_df = create_case_key_lookup(df)
    
    # Example: Look up original keys for a list of hashed keys
    hashed_keys_to_lookup = [1234567, 7654321, 9999999]
    result_df = lookup_original_case_keys(lookup_df, hashed_keys_to_lookup)
    
    # Show results
    lookup_df.show(truncate=False)
    result_df.show()

# Note: The main function is provided for demonstration purposes.
# In a real Databricks notebook, you would call these functions directly on your DataFrame.
