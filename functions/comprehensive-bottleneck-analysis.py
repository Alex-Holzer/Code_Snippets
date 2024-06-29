from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List

def analyze_comprehensive_bottlenecks(
    df: DataFrame,
    dimension_cols: List[str],
    case_key_col: str = "_CASE_KEY",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    time_unit: str = "hours",
    end_activities: List[str] = ["AUTOMATISCH_ENDE", "ARCH", "STORNIERT"],
    top_n_bottlenecks: int = 10,
    min_cases: int = 20
) -> DataFrame:
    """
    Perform a comprehensive bottleneck analysis across multiple dimensions for completed processes.

    This function identifies the strongest bottlenecks in the process, considering interactions
    between dimensions and focusing on completed processes only. It calculates both absolute
    and relative impact of transitions between activities.

    Args:
        df (DataFrame): Input DataFrame containing process data.
        dimension_cols (List[str]): List of column names representing dimensions to analyze.
        case_key_col (str): Column name for the case key. Default is "_CASE_KEY".
        activity_col (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_col (str): Column name for the event timestamp. Default is "EVENTTIME".
        time_unit (str): Unit for duration calculation. 
            Options are "seconds", "minutes", "hours", "days". Default is "hours".
        end_activities (List[str]): List of activities that mark the end of a process.
            Default is ["AUTOMATISCH_ENDE", "ARCH", "STORNIERT"].
        top_n_bottlenecks (int): Number of top bottlenecks to return. Default is 10.
        min_cases (int): Minimum number of cases required for a dimension combination to be included. Default is 20.

    Returns:
        DataFrame: A DataFrame containing the comprehensive bottleneck analysis results with columns:
            - dimension_combination: The combination of dimension values
            - from_activity: The activity at the start of the transition
            - to_activity: The activity at the end of the transition
            - avg_duration: The average duration of this transition
            - median_duration: The median duration of this transition
            - max_duration: The maximum duration of this transition
            - transition_frequency: The number of times this transition occurs
            - transition_percentage: The percentage of cases with this transition
            - avg_relative_impact: The average percentage of total case duration this transition represents
            - bottleneck_score: A composite score indicating the strength of the bottleneck

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> bottleneck_results = analyze_comprehensive_bottlenecks(
        ...     df, 
        ...     dimension_cols=["DIMENSION 1", "DIMENSION 2"],
        ...     time_unit="hours",
        ...     top_n_bottlenecks=5
        ... )
        >>> bottleneck_results.show(truncate=False)

    Note:
        - Only completed processes (those that reach an end activity) are considered in the analysis.
        - The bottleneck_score is a composite measure considering duration, frequency, and relative impact.
        - This function provides insights into bottlenecks across different dimension combinations,
          helping to identify both global and context-specific process inefficiencies.
    """
    # Validate input
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")
    
    required_columns = [case_key_col, activity_col, timestamp_col] + dimension_cols
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
    dimension_window = Window.partitionBy(dimension_cols)

    # Identify completed cases
    completed_cases = df.groupBy(case_key_col).agg(
        F.max(F.when(F.col(activity_col).isin(end_activities), 1).otherwise(0)).alias("is_completed")
    ).filter(F.col("is_completed") == 1)

    # Filter for completed cases and add case duration
    df_completed = df.join(completed_cases, on=case_key_col).withColumn(
        "case_duration",
        (F.max(timestamp_col).over(case_window).cast("long") - 
         F.min(timestamp_col).over(case_window).cast("long")) / time_multiplier
    )

    # Calculate durations between activities
    df_with_next = df_completed.withColumn(
        "next_activity", F.lead(activity_col).over(case_window)
    ).withColumn(
        "next_timestamp", F.lead(timestamp_col).over(case_window)
    ).withColumn(
        "transition_duration",
        (F.col("next_timestamp").cast("long") - F.col(timestamp_col).cast("long")) / time_multiplier
    ).withColumn(
        "relative_impact",
        F.col("transition_duration") / F.col("case_duration") * 100
    )

    # Group by dimensions and transitions, calculate statistics
    transition_stats = df_with_next.groupBy(
        *dimension_cols, activity_col, "next_activity"
    ).agg(
        F.avg("transition_duration").alias("avg_duration"),
        F.expr("percentile(transition_duration, 0.5)").alias("median_duration"),
        F.max("transition_duration").alias("max_duration"),
        F.count("*").alias("transition_frequency"),
        F.avg("relative_impact").alias("avg_relative_impact")
    )

    # Calculate total cases for each dimension combination
    total_cases = transition_stats.groupBy(*dimension_cols).agg(
        F.sum("transition_frequency").alias("total_transitions")
    )

    # Join with total cases and calculate percentages
    transitions_with_percentages = transition_stats.join(
        total_cases, on=dimension_cols
    ).withColumn(
        "transition_percentage", (F.col("transition_frequency") / F.col("total_transitions")) * 100
    ).filter(F.col("total_transitions") >= min_cases)

    # Calculate bottleneck score
    bottlenecks = transitions_with_percentages.withColumn(
        "bottleneck_score",
        (F.col("avg_duration") * F.col("transition_percentage") * F.col("avg_relative_impact")) / 100
    )

    # Rank bottlenecks and select top N
    ranked_bottlenecks = bottlenecks.withColumn(
        "bottleneck_rank", F.row_number().over(dimension_window.orderBy(F.desc("bottleneck_score")))
    ).filter(F.col("bottleneck_rank") <= top_n_bottlenecks)

    # Prepare final result
    final_result = ranked_bottlenecks.select(
        F.concat_ws(", ", *[F.concat(F.lit(dim + ": "), F.col(dim)) for dim in dimension_cols]).alias("dimension_combination"),
        F.col(activity_col).alias("from_activity"),
        F.col("next_activity").alias("to_activity"),
        F.round("avg_duration", 2).alias("avg_duration"),
        F.round("median_duration", 2).alias("median_duration"),
        F.round("max_duration", 2).alias("max_duration"),
        "transition_frequency",
        F.round("transition_percentage", 2).alias("transition_percentage"),
        F.round("avg_relative_impact", 2).alias("avg_relative_impact"),
        F.round("bottleneck_score", 2).alias("bottleneck_score")
    ).orderBy(F.desc("bottleneck_score"))

    return final_result
