from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rlike

def filter_by_patterns(df: DataFrame, column_name: str, patterns: List[str]) -> DataFrame:
    """
    Filters the DataFrame to retain rows where the specified column contains
    any of the provided string patterns.

    Parameters
    ----------
    df : DataFrame
        The input PySpark DataFrame to be filtered.
    column_name : str
        The name of the column in which to search for the patterns.
    patterns : List[str]
        A list of string patterns to filter rows by.

    Returns
    -------
    DataFrame
        A filtered DataFrame containing only the rows where the column contains any of the patterns.
    """
    # Combine all patterns into a single regex expression joined by "|"
    combined_pattern = "|".join(patterns)

    # Apply filter using rlike to match any of the patterns
    return df.filter(col(column_name).rlike(combined_pattern))

# Example usage:

# Assuming 'spark' is your SparkSession and 'df' is your DataFrame
patterns_list = ["pattern1", "pattern2", "pattern3"]
filtered_df = filter_by_patterns(df, "your_column_name", patterns_list)

# To display or collect the results:
filtered_df.show()