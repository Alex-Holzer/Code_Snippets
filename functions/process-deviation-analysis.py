from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional


def analyze_process_deviation(
    df: DataFrame,
    case_key_col: str = "_CASE_KEY",
    dimension_col: str = "DIMENSION 1",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    start_activities: Optional[List[str]] = None,
    end_activities: Optional[List[str]] = None,
    time_unit: str = "hours",
    deviation_threshold: float = 0.1,
    below_average: bool = True,
) -> DataFrame:
    """
    Analyze process duration deviations from the mean for a specific dimension.

    This function calculates the percentage deviation of each case's duration from the mean
    duration of its dimension group. It then identifies cases that deviate from the mean
    by more than the specified threshold, either below or above average.

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
        deviation_threshold (float): The threshold for identifying significant deviations,
            expressed as a decimal (e.g., 0.1 for 10%). Default is 0.1.
        below_average (bool): If True, identify cases below average (slower).
            If False, identify cases above average (faster). Default is True.

    Returns:
        DataFrame: A DataFrame containing the deviation analysis results with columns:
            - dimension: The unique value from the dimension column
            - case_key: The case key of the deviating case
            - duration: The duration of the process for this case
            - avg_duration: The average duration for this dimension
            - deviation_percentage: The percentage deviation from the mean

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> slow_cases = analyze_process_deviation(
        ...     df,
        ...     dimension_col="DIMENSION 2",
        ...     start_activities=["START", "AENDERUNG"],
        ...     end_activities=["AUTOMATISCH_ENDE", "ARCH"],
        ...     time_unit="days",
        ...     deviation_threshold=0.1,
        ...     below_average=True
        ... )
        >>> slow_cases.show()

    Note:
        - The deviation_percentage is calculated as (case_duration - avg_duration) / avg_duration.
        - A positive deviation_percentage indicates the case is slower than average.
        - A negative deviation_percentage indicates the case is faster than average.
        - The function handles cases where start or end events are missing by excluding
          those cases from the calculations.
        - Time unit conversion is applied at the end of the calculation to maintain precision.

    Interpreting the deviation_percentage:
        1. When deviation_threshold = 0.1 and below_average = True:
           - The function returns cases where deviation_percentage > 0.1
           - These cases are at least 10% slower than the average
           - Example: A deviation_percentage of 0.15 means the case is 15% slower than average

        2. When deviation_threshold = 0.1 and below_average = False:
           - The function returns cases where deviation_percentage < -0.1
           - These cases are at least 10% faster than the average
           - Example: A deviation_percentage of -0.15 means the case is 15% faster than average

        3. General interpretation:
           - deviation_percentage of 0.2 means 20% slower than average
           - deviation_percentage of -0.2 means 20% faster than average
           - deviation_percentage of 0 means exactly average
           - deviation_percentage of 1.0 means twice as slow as average
           - deviation_percentage of -0.5 means half as long (twice as fast) as average

    """
    # Validate input
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")

    required_columns = [case_key_col, dimension_col, activity_col, timestamp_col]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Validate and set time unit multiplier
    time_unit_multipliers = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}
    if time_unit not in time_unit_multipliers:
        raise ValueError(
            f"Invalid time unit. Choose from: {', '.join(time_unit_multipliers.keys())}"
        )
    time_multiplier = time_unit_multipliers[time_unit]

    # Define window specifications
    case_window = Window.partitionBy(case_key_col).orderBy(timestamp_col)
    dimension_window = Window.partitionBy(dimension_col)

    # Identify start and end events
    if start_activities and end_activities:
        df_with_start_end = df.withColumn(
            "is_start",
            F.when(F.col(activity_col).isin(start_activities), 1).otherwise(0),
        ).withColumn(
            "is_end", F.when(F.col(activity_col).isin(end_activities), 1).otherwise(0)
        )
    else:
        df_with_start_end = df.withColumn("is_start", F.lit(1)).withColumn(
            "is_end", F.lit(1)
        )

    # Find first start and last end for each case
    df_with_boundaries = df_with_start_end.withColumn(
        "start_time",
        F.first(F.when(F.col("is_start") == 1, F.col(timestamp_col))).over(case_window),
    ).withColumn(
        "end_time",
        F.last(F.when(F.col("is_end") == 1, F.col(timestamp_col))).over(case_window),
    )

    # Calculate duration for each case
    df_with_duration = df_with_boundaries.withColumn(
        "duration",
        F.when(
            (F.col("start_time").isNotNull()) & (F.col("end_time").isNotNull()),
            (F.col("end_time").cast("long") - F.col("start_time").cast("long"))
            / time_multiplier,
        ),
    ).dropDuplicates(
        [case_key_col]
    )  # Keep one row per case

    # Calculate average duration for each dimension
    df_with_avg = df_with_duration.withColumn(
        "avg_duration", F.avg("duration").over(dimension_window)
    )

    # Calculate percentage deviation from mean
    df_with_deviation = df_with_avg.withColumn(
        "deviation_percentage",
        (F.col("duration") - F.col("avg_duration")) / F.col("avg_duration"),
    )

    # Filter cases based on deviation threshold and direction
    if below_average:
        deviating_cases = df_with_deviation.where(
            F.col("deviation_percentage") > deviation_threshold
        )
    else:
        deviating_cases = df_with_deviation.where(
            F.col("deviation_percentage") < -deviation_threshold
        )

    # Prepare final result
    result = deviating_cases.select(
        dimension_col,
        F.col(case_key_col).alias("case_key"),
        F.round("duration", 2).alias("duration"),
        F.round("avg_duration", 2).alias("avg_duration"),
        F.round("deviation_percentage", 4).alias("deviation_percentage"),
    )

    return result
