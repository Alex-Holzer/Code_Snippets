from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List


def analyze_transition_probabilities(
    df: DataFrame,
    dimension_cols: List[str],
    case_key_col: str = "_CASE_KEY",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    end_activities: List[str] = ["AUTOMATISCH_ENDE", "ARCH", "STORNIERT"],
    min_cases: int = 20,
    min_probability: float = 0.01,
) -> DataFrame:
    """
    Analyze transition probabilities between activities across multiple dimensions for completed processes.

    This function calculates the probability of moving from one activity to another,
    considering different dimension combinations. It focuses on completed processes only.

    Args:
        df (DataFrame): Input DataFrame containing process data.
        dimension_cols (List[str]): List of column names representing dimensions to analyze.
        case_key_col (str): Column name for the case key. Default is "_CASE_KEY".
        activity_col (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_col (str): Column name for the event timestamp. Default is "EVENTTIME".
        end_activities (List[str]): List of activities that mark the end of a process.
            Default is ["AUTOMATISCH_ENDE", "ARCH", "STORNIERT"].
        min_cases (int): Minimum number of cases required for a dimension combination to be included. Default is 20.
        min_probability (float): Minimum transition probability to include in the results. Default is 0.01 (1%).

    Returns:
        DataFrame: A DataFrame containing the transition probability analysis results with columns:
            - dimension_combination: The combination of dimension values
            - from_activity: The activity at the start of the transition
            - to_activity: The activity at the end of the transition
            - transition_count: The number of times this transition occurs
            - total_from_activity: Total number of transitions from the 'from_activity'
            - transition_probability: The probability of this transition
            - is_end_activity: Boolean indicating if 'to_activity' is an end activity

    Raises:
        ValueError: If input validation fails.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> transition_probs = analyze_transition_probabilities(
        ...     df,
        ...     dimension_cols=["DIMENSION 1", "DIMENSION 2"],
        ...     min_probability=0.05
        ... )
        >>> transition_probs.show(truncate=False)

    Note:
        - Only completed processes (those that reach an end activity) are considered in the analysis.
        - Transitions with probabilities lower than min_probability are filtered out.
        - This function provides insights into common process flows and how they vary across
          different dimension combinations, helping to identify typical and atypical process paths.
    """
    # Validate input
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")

    required_columns = [case_key_col, activity_col, timestamp_col] + dimension_cols
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Define window specifications
    case_window = Window.partitionBy(case_key_col).orderBy(timestamp_col)
    dimension_window = Window.partitionBy(dimension_cols + [F.col(activity_col)])

    # Identify completed cases
    completed_cases = (
        df.groupBy(case_key_col)
        .agg(
            F.max(
                F.when(F.col(activity_col).isin(end_activities), 1).otherwise(0)
            ).alias("is_completed")
        )
        .filter(F.col("is_completed") == 1)
    )

    # Filter for completed cases and add next activity
    df_completed = df.join(completed_cases, on=case_key_col).withColumn(
        "next_activity", F.lead(activity_col).over(case_window)
    )

    # Calculate transition counts
    transition_counts = df_completed.groupBy(
        *dimension_cols, activity_col, "next_activity"
    ).count()

    # Calculate total transitions from each activity
    total_from_activity = transition_counts.groupBy(*dimension_cols, activity_col).agg(
        F.sum("count").alias("total_from_activity")
    )

    # Calculate transition probabilities
    transition_probs = (
        transition_counts.join(total_from_activity, on=dimension_cols + [activity_col])
        .withColumn(
            "transition_probability", F.col("count") / F.col("total_from_activity")
        )
        .withColumn(
            "is_end_activity",
            F.when(F.col("next_activity").isin(end_activities), True).otherwise(False),
        )
    )

    # Filter based on minimum cases and probability
    filtered_probs = (
        transition_probs.groupBy(*dimension_cols)
        .agg(F.sum("count").alias("total_cases"))
        .join(transition_probs, on=dimension_cols)
        .filter(
            (F.col("total_cases") >= min_cases)
            & (F.col("transition_probability") >= min_probability)
        )
    )

    # Prepare final result
    final_result = filtered_probs.select(
        F.concat_ws(
            ", ", *[F.concat(F.lit(dim + ": "), F.col(dim)) for dim in dimension_cols]
        ).alias("dimension_combination"),
        F.col(activity_col).alias("from_activity"),
        F.col("next_activity").alias("to_activity"),
        F.col("count").alias("transition_count"),
        "total_from_activity",
        F.round("transition_probability", 4).alias("transition_probability"),
        "is_end_activity",
    ).orderBy(dimension_cols + [activity_col, F.desc("transition_probability")])

    return final_result
