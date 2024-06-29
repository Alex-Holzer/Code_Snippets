from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hash, monotonically_increasing_id

def hash_case_key(df: DataFrame, case_key_col: str = "_CASE_KEY") -> DataFrame:
    """
    Hash the _CASE_KEY column in the input DataFrame using an efficient hashing method.

    This function is optimized for large datasets with over 100 million distinct _CASE_KEYs.
    It uses PySpark's built-in hash function, which is efficient and distributes well.

    Args:
        df (DataFrame): Input DataFrame containing the _CASE_KEY column.
        case_key_col (str, optional): Name of the case key column. Defaults to "_CASE_KEY".

    Returns:
        DataFrame: DataFrame with an additional column 'hashed_case_key' containing the hashed values.

    Example:
        >>> input_df = spark.createDataFrame([("A-123",), ("B-456",)], ["_CASE_KEY"])
        >>> result_df = hash_case_key(input_df)
        >>> result_df.show()
        +--------+------------------+
        |_CASE_KEY|hashed_case_key   |
        +--------+------------------+
        |A-123   |-1234567890123456 |
        |B-456   |9876543210987654  |
        +--------+------------------+
    """
    return df.withColumn("hashed_case_key", hash(col(case_key_col)))

def store_hashed_keys(df: DataFrame, case_key_col: str = "_CASE_KEY", hashed_key_col: str = "hashed_case_key") -> DataFrame:
    """
    Store the hashed values and corresponding keys in a separate DataFrame.

    This function creates a new DataFrame with unique _CASE_KEYs and their corresponding hash values.
    It's optimized for performance by using DataFrame operations and avoiding Python UDFs.

    Args:
        df (DataFrame): Input DataFrame containing the _CASE_KEY and hashed_case_key columns.
        case_key_col (str, optional): Name of the case key column. Defaults to "_CASE_KEY".
        hashed_key_col (str, optional): Name of the hashed key column. Defaults to "hashed_case_key".

    Returns:
        DataFrame: DataFrame with unique _CASE_KEYs and their corresponding hash values.

    Example:
        >>> input_df = spark.createDataFrame([("A-123", 123), ("B-456", 456), ("A-123", 123)], ["_CASE_KEY", "hashed_case_key"])
        >>> result_df = store_hashed_keys(input_df)
        >>> result_df.show()
        +--------+------------------+
        |_CASE_KEY|hashed_case_key   |
        +--------+------------------+
        |A-123   |123               |
        |B-456   |456               |
        +--------+------------------+
    """
    return (df
            .select(case_key_col, hashed_key_col)
            .distinct()
            .orderBy(hashed_key_col)
            .withColumn("id", monotonically_increasing_id())
            .cache())

# Usage example
def main(spark):
    # Sample data
    data = [
        ("5-ta", "Beitragsüberbrückung", "Allgemeiner ÄnderungsdialogVSL", "START", "2023-07-27T13:18:12.039+0000"),
        ("A-123", "Stundung", "Inkasso", "AENDERUNG", "2023-07-27T13:14:12.039+0000"),
        ("F-569", "VN-Wechsel", "Juristische Änderung", "PLANMAESSIG_WEITERLEITEN_VON", "2023-01-26T15:36:17.678+0000")
    ]
    
    columns = ["_CASE_KEY", "DIMENSION 1", "DIMENSION 2", "ACTIVITY", "EVENTTIME"]
    
    df = spark.createDataFrame(data, columns)
    
    # Hash the _CASE_KEY
    df_hashed = hash_case_key(df)
    
    # Store the hashed keys
    hashed_keys_df = store_hashed_keys(df_hashed)
    
    # Show results
    df_hashed.show(truncate=False)
    hashed_keys_df.show(truncate=False)

# Note: The main function is provided for demonstration purposes.
# In a real Databricks notebook, you would call these functions directly on your DataFrame.
