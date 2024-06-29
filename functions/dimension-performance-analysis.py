from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional

def analyze_dimension_performance(
    df: DataFrame,
    case_key_col: str = "_CASE_KEY",
    dimension_col: str = "DIMENSION 1",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    start_activities: Optional[List[str]] = None,
    end_activities: Optional[List[str]] = None,
    time_unit: str = "hours"
) -> DataFrame:
    """
    Analyze performance metrics for each dimension in the process data.

    This function calculates various performance indicators for each unique dimension,
    helping to identify low-performing dimensions across all cases.

    Args:
        df (DataFrame): Input DataFrame containing process data.
        case_key_col (str): Column name for the case key. Default is "_CASE_KEY".
        dimension_col (str): Column name for the dimension to analyze. Default is "DIMENSION 1".
        activity_col (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_col (str): Column name for the event timestamp. Default is "EVENTTIME".
        start_activities (Optional[List[str]]): List of activities that mark the start of a process.
            If None, the first event for each case is considered the start. Default is None.
        end_activities (Optional[List[str]]): List of activities that mark the end of a process.
            If None, the last event for each case is considered the end. Default is None.
        time_unit (str): Unit for duration calculation. 
            Options are "seconds", "minutes", "hours", "days". Default is "hours".

    Returns:
        DataFrame: A DataFrame containing the performance analysis results with columns:
            - dimension: The unique value from the dimension column
            - avg_duration: The average duration for completed cases in this dimension
            - median_duration: The median duration for completed cases in this dimension
            - min_duration: The minimum duration for completed cases in this dimension
            - max_duration: The maximum duration for completed cases in this dimension
            - std_dev_duration: The standard deviation of durations for completed cases in this dimension
            - total_cases: The total number of cases for this dimension
            - completed_cases: The number of completed cases for this dimension
            - completion_rate: The rate of completed cases (completed_cases / total_cases)

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> dimension_performance = analyze_dimension_performance(
        ...     df, 
        ...     dimension_col="DIMENSION 2", 
        ...     start_activities=["START", "AENDERUNG"],
        ...     end_activities=["AUTOMATISCH_ENDE", "ARCH"],
        ...     time_unit="days"
        ... )
        >>> dimension_performance.show()

    Note:
        - A case is considered completed if it has at least one activity from the end_activities list.
        - If end_activities is None, all cases are considered completed.
        - Low-performing dimensions can be identified by comparing metrics such as
          avg_duration, median_duration, completion_rate, etc., across dimensions.
        - A high std_dev_duration might indicate inconsistent performance within a dimension.
        - Low completion_rate might suggest process bottlenecks or issues in that dimension.
        - Duration statistics are calculated only for completed cases.
        - Time unit conversion is applied at the end of the calculation to maintain precision.
    """
    # Validate input
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")
    
    required_columns = [case_key_col, dimension_col, activity_col, timestamp_col]
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

    # Identify start and end events
    if start_activities:
        df = df.withColumn(
            "is_start",
            F.when(F.col(activity_col).isin(start_activities), 1).otherwise(0)
        )
    else:
        df = df.withColumn("is_start", F.lit(1))

    if end_activities:
        df = df.withColumn(
            "is_end",
            F.when(F.col(activity_col).isin(end_activities), 1).otherwise(0)
        )
    else:
        df = df.withColumn("is_end", F.lit(1))

    # Find first start and last end for each case
    df_with_boundaries = df.withColumn(
        "start_time",
        F.first(F.when(F.col("is_start") == 1, F.col(timestamp_col))).over(case_window)
    ).withColumn(
        "end_time",
        F.last(F.when(F.col("is_end") == 1, F.col(timestamp_col))).over(case_window)
    )

    # Mark completed cases and calculate duration
    df_with_completion = df_with_boundaries.withColumn(
        "is_completed",
        F.max(F.col("is_end")).over(case_window)
    ).withColumn(
        "duration",
        F.when(
            F.col("is_completed") == 1,
            (F.col("end_time").cast("long") - F.col("start_time").cast("long")) / time_multiplier
        )
    ).dropDuplicates([case_key_col])  # Keep one row per case

    # Calculate performance metrics for each dimension
    dimension_performance = df_with_completion.groupBy(dimension_col).agg(
        F.avg("duration").alias("avg_duration"),
        F.expr("percentile(duration, 0.5)").alias("median_duration"),
        F.min("duration").alias("min_duration"),
        F.max("duration").alias("max_duration"),
        F.stddev("duration").alias("std_dev_duration"),
        F.count("*").alias("total_cases"),
        F.sum("is_completed").alias("completed_cases")
    ).withColumn(
        "completion_rate",
        F.col("completed_cases") / F.col("total_cases")
    )

    # Round numeric columns for readability
    numeric_columns = ["avg_duration", "median_duration", "min_duration", "max_duration", 
                       "std_dev_duration", "completion_rate"]
    for col in numeric_columns:
        dimension_performance = dimension_performance.withColumn(col, F.round(col, 2))

    # Reorder columns for better readability
    final_columns = [dimension_col, "avg_duration", "median_duration", "min_duration", "max_duration", 
                     "std_dev_duration", "total_cases", "completed_cases", "completion_rate"]
    
    return dimension_performance.select(final_columns)
