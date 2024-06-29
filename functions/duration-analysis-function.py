from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional, Tuple

def analyze_process_duration_stats(
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
    Analyze the duration statistics of processes for a specific dimension column.

    This function calculates the average, minimum, and maximum time to complete the process
    for each unique value in the specified dimension column. It offers flexibility in defining
    process boundaries and time units for analysis.

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
        DataFrame: A DataFrame containing the analysis results with columns:
            - dimension: The unique value from the dimension column
            - avg_duration: Average duration of the process in the specified time unit
            - min_duration: Minimum duration of the process in the specified time unit
            - max_duration: Maximum duration of the process in the specified time unit

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> result = analyze_process_duration_stats(
        ...     df, 
        ...     dimension_col="DIMENSION 2", 
        ...     start_activities=["START", "AENDERUNG"],
        ...     end_activities=["AUTOMATISCH_ENDE", "ARCH"],
        ...     time_unit="days"
        ... )
        >>> result.show()

    Note:
        - If start_activities and end_activities are not provided, the function will use
          the first and last events for each case to calculate duration.
        - The function handles cases where start or end events are missing by excluding
          those cases from the duration calculations.
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

    # Define window specification
    window_spec = Window.partitionBy(case_key_col).orderBy(timestamp_col)

    # Identify start and end events
    if start_activities and end_activities:
        df_with_start_end = df.withColumn(
            "is_start",
            F.when(F.col(activity_col).isin(start_activities), 1).otherwise(0)
        ).withColumn(
            "is_end",
            F.when(F.col(activity_col).isin(end_activities), 1).otherwise(0)
        )
    else:
        df_with_start_end = df.withColumn("is_start", F.lit(1)).withColumn("is_end", F.lit(1))

    # Find first start and last end for each case
    df_with_boundaries = df_with_start_end.withColumn(
        "start_time",
        F.first(F.when(F.col("is_start") == 1, F.col(timestamp_col))).over(window_spec)
    ).withColumn(
        "end_time",
        F.last(F.when(F.col("is_end") == 1, F.col(timestamp_col))).over(window_spec)
    )

    # Calculate duration for each case
    df_with_duration = df_with_boundaries.withColumn(
        "duration",
        F.when(
            (F.col("start_time").isNotNull()) & (F.col("end_time").isNotNull()),
            (F.col("end_time").cast("long") - F.col("start_time").cast("long")) / time_multiplier
        )
    )

    # Group by dimension and calculate statistics
    result = df_with_duration.groupBy(dimension_col).agg(
        F.avg("duration").alias("avg_duration"),
        F.min("duration").alias("min_duration"),
        F.max("duration").alias("max_duration")
    ).select(
        F.col(dimension_col).alias("dimension"),
        F.round("avg_duration", 2).alias("avg_duration"),
        F.round("min_duration", 2).alias("min_duration"),
        F.round("max_duration", 2).alias("max_duration")
    )

    return result
