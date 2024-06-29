from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional

def analyze_activity_patterns_ceteris_paribus(
    df: DataFrame,
    target_dimension_col: str,
    ceteris_paribus_cols: List[str],
    case_key_col: str = "_CASE_KEY",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    top_n_patterns: int = 5,
    min_cases: int = 5
) -> DataFrame:
    """
    Analyze activity patterns for a target dimension while keeping other dimensions constant.

    This function identifies the most common activity sequences for each unique value of the target dimension,
    while controlling for the effects of other specified dimensions (ceteris paribus approach).

    Args:
        df (DataFrame): Input DataFrame containing process data.
        target_dimension_col (str): Column name for the dimension to analyze.
        ceteris_paribus_cols (List[str]): List of column names to keep constant during analysis.
        case_key_col (str): Column name for the case key. Default is "_CASE_KEY".
        activity_col (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_col (str): Column name for the event timestamp. Default is "EVENTTIME".
        top_n_patterns (int): Number of top activity patterns to return for each dimension combination. Default is 5.
        min_cases (int): Minimum number of cases required for a dimension combination to be included. Default is 5.

    Returns:
        DataFrame: A DataFrame containing the ceteris paribus activity pattern analysis results with columns:
            - [ceteris_paribus_cols]: Columns representing the constant dimensions
            - target_dimension: The unique value from the target dimension column
            - activity_pattern: The sequence of activities
            - pattern_frequency: The number of cases following this pattern
            - pattern_percentage: The percentage of cases following this pattern

    Raises:
        ValueError: If input validation fails.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> pattern_results = analyze_activity_patterns_ceteris_paribus(
        ...     df, 
        ...     target_dimension_col="DIMENSION 1",
        ...     ceteris_paribus_cols=["DIMENSION 2", "DIMENSION 3"],
        ...     top_n_patterns=3
        ... )
        >>> pattern_results.show(truncate=False)

    Note:
        - The function filters out dimension combinations with fewer cases than the specified min_cases.
        - This approach helps isolate the effect of the target dimension on activity patterns.
        - Activity patterns are represented as comma-separated strings of activity names.
        - The analysis focuses on the most common patterns, which can help identify typical process flows
          and how they vary across different values of the target dimension.
    """
    # Validate input
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")
    
    required_columns = [case_key_col, target_dimension_col, activity_col, timestamp_col] + ceteris_paribus_cols
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Define window specifications
    case_window = Window.partitionBy(case_key_col).orderBy(timestamp_col)
    dimension_window = Window.partitionBy(ceteris_paribus_cols + [target_dimension_col])

    # Create activity sequences for each case
    df_with_sequences = df.withColumn(
        "activity_order", F.row_number().over(case_window)
    ).groupBy(case_key_col, *ceteris_paribus_cols, target_dimension_col).agg(
        F.concat_ws(",", F.collect_list(F.struct(F.col("activity_order"), activity_col)).over(case_window))
        .alias("activity_sequence")
    )

    # Count pattern frequencies
    pattern_frequencies = df_with_sequences.groupBy(*ceteris_paribus_cols, target_dimension_col, "activity_sequence").count()

    # Calculate total cases for each dimension combination
    total_cases = pattern_frequencies.groupBy(*ceteris_paribus_cols, target_dimension_col).agg(
        F.sum("count").alias("total_cases")
    )

    # Join frequencies with totals and calculate percentages
    patterns_with_percentages = pattern_frequencies.join(
        total_cases, on=ceteris_paribus_cols + [target_dimension_col]
    ).withColumn(
        "pattern_percentage", (F.col("count") / F.col("total_cases")) * 100
    )

    # Rank patterns and select top N for each dimension combination
    ranked_patterns = patterns_with_percentages.withColumn(
        "pattern_rank", F.row_number().over(dimension_window.orderBy(F.desc("count")))
    ).filter(
        (F.col("pattern_rank") <= top_n_patterns) & (F.col("total_cases") >= min_cases)
    )

    # Prepare final result
    final_result = ranked_patterns.select(
        *ceteris_paribus_cols,
        target_dimension_col,
        F.col("activity_sequence").alias("activity_pattern"),
        F.col("count").alias("pattern_frequency"),
        F.round("pattern_percentage", 2).alias("pattern_percentage")
    ).orderBy(ceteris_paribus_cols + [target_dimension_col, "pattern_frequency"])

    return final_result
