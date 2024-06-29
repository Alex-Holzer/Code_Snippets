from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional

def analyze_bottlenecks_ceteris_paribus(
    df: DataFrame,
    target_dimension_col: str,
    ceteris_paribus_cols: List[str],
    case_key_col: str = "_CASE_KEY",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    time_unit: str = "hours",
    top_n_transitions: int = 5,
    min_cases: int = 5
) -> DataFrame:
    """
    Analyze potential bottlenecks by measuring inter-activity durations for a target dimension
    while keeping other dimensions constant.

    This function identifies the transitions between activities that take the most time,
    potentially indicating bottlenecks in the process. It does this for each unique value 
    of the target dimension, while controlling for other specified dimensions (ceteris paribus approach).

    Args:
        df (DataFrame): Input DataFrame containing process data.
        target_dimension_col (str): Column name for the dimension to analyze.
        ceteris_paribus_cols (List[str]): List of column names to keep constant during analysis.
        case_key_col (str): Column name for the case key. Default is "_CASE_KEY".
        activity_col (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_col (str): Column name for the event timestamp. Default is "EVENTTIME".
        time_unit (str): Unit for duration calculation. 
            Options are "seconds", "minutes", "hours", "days". Default is "hours".
        top_n_transitions (int): Number of top time-consuming transitions to return. Default is 5.
        min_cases (int): Minimum number of cases required for a dimension combination to be included. Default is 5.

    Returns:
        DataFrame: A DataFrame containing the bottleneck analysis results with columns:
            - [ceteris_paribus_cols]: Columns representing the constant dimensions
            - target_dimension: The unique value from the target dimension column
            - from_activity: The activity at the start of the transition
            - to_activity: The activity at the end of the transition
            - avg_duration: The average duration of this transition
            - median_duration: The median duration of this transition
            - max_duration: The maximum duration of this transition
            - transition_frequency: The number of times this transition occurs
            - transition_percentage: The percentage of cases with this transition

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> bottleneck_results = analyze_bottlenecks_ceteris_paribus(
        ...     df, 
        ...     target_dimension_col="DIMENSION 1",
        ...     ceteris_paribus_cols=["DIMENSION 2", "DIMENSION 3"],
        ...     time_unit="hours",
        ...     top_n_transitions=3
        ... )
        >>> bottleneck_results.show(truncate=False)

    Note:
        - The function filters out dimension combinations with fewer cases than the specified min_cases.
        - This approach helps isolate the effect of the target dimension on potential bottlenecks.
        - Transitions are represented as pairs of activities (from_activity -> to_activity).
        - The analysis focuses on the most time-consuming transitions, which can help identify
          bottlenecks in the process and how they vary across different values of the target dimension.
    """
    # Validate input
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")
    
    required_columns = [case_key_col, target_dimension_col, activity_col, timestamp_col] + ceteris_paribus_cols
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Validate and set time unit multiplier
    time_unit_multipliers = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400
    }
    if time_unit not in time_unit_multipliers:
        raise ValueError(f"Invalid time unit. Choose from: {', '.join(time_unit_multipliers.keys())}")
    time_multiplier = time_unit_multipliers[time_unit]

    # Define window specifications
    case_window = Window.partitionBy(case_key_col).orderBy(timestamp_col)
    dimension_window = Window.partitionBy(ceteris_paribus_cols + [target_dimension_col])

    # Calculate durations between activities
    df_with_next = df.withColumn(
        "next_activity", F.lead(activity_col).over(case_window)
    ).withColumn(
        "next_timestamp", F.lead(timestamp_col).over(case_window)
    ).withColumn(
        "duration",
        (F.col("next_timestamp").cast("long") - F.col(timestamp_col).cast("long")) / time_multiplier
    )

    # Group by dimensions and transitions, calculate statistics
    transition_stats = df_with_next.groupBy(
        *ceteris_paribus_cols, target_dimension_col, activity_col, "next_activity"
    ).agg(
        F.avg("duration").alias("avg_duration"),
        F.expr("percentile(duration, 0.5)").alias("median_duration"),
        F.max("duration").alias("max_duration"),
        F.count("*").alias("transition_frequency")
    )

    # Calculate total cases for each dimension combination
    total_cases = transition_stats.groupBy(*ceteris_paribus_cols, target_dimension_col).agg(
        F.sum("transition_frequency").alias("total_transitions")
    )

    # Join with total cases and calculate percentages
    transitions_with_percentages = transition_stats.join(
        total_cases, on=ceteris_paribus_cols + [target_dimension_col]
    ).withColumn(
        "transition_percentage", (F.col("transition_frequency") / F.col("total_transitions")) * 100
    )

    # Rank transitions by average duration and select top N for each dimension combination
    ranked_transitions = transitions_with_percentages.withColumn(
        "duration_rank", F.row_number().over(dimension_window.orderBy(F.desc("avg_duration")))
    ).filter(
        (F.col("duration_rank") <= top_n_transitions) & (F.col("total_transitions") >= min_cases)
    )

    # Prepare final result
    final_result = ranked_transitions.select(
        *ceteris_paribus_cols,
        target_dimension_col,
        F.col(activity_col).alias("from_activity"),
        F.col("next_activity").alias("to_activity"),
        F.round("avg_duration", 2).alias("avg_duration"),
        F.round("median_duration", 2).alias("median_duration"),
        F.round("max_duration", 2).alias("max_duration"),
        "transition_frequency",
        F.round("transition_percentage", 2).alias("transition_percentage")
    ).orderBy(ceteris_paribus_cols + [target_dimension_col, "avg_duration"])

    return final_result
