from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional

def identify_process_outliers(
    df: DataFrame,
    case_key_col: str = "_CASE_KEY",
    dimension_col: str = "DIMENSION 1",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    start_activities: Optional[List[str]] = None,
    end_activities: Optional[List[str]] = None,
    time_unit: str = "hours",
    iqr_multiplier: float = 1.5
) -> DataFrame:
    """
    Identify outliers in process durations for a specific dimension column using the IQR method.

    This function calculates the process duration for each case and identifies outliers
    based on the Interquartile Range (IQR) method. It provides flexibility in defining
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
        iqr_multiplier (float): Multiplier for IQR to determine outlier boundaries. Default is 1.5.

    Returns:
        DataFrame: A DataFrame containing the outlier analysis results with columns:
            - dimension: The unique value from the dimension column
            - case_key: The case key of the outlier
            - duration: The duration of the process for this case
            - q1: The first quartile of durations for this dimension
            - q3: The third quartile of durations for this dimension
            - iqr: The interquartile range for this dimension
            - lower_bound: The lower bound for outlier detection
            - upper_bound: The upper bound for outlier detection

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> outliers = identify_process_outliers(
        ...     df, 
        ...     dimension_col="DIMENSION 2", 
        ...     start_activities=["START", "AENDERUNG"],
        ...     end_activities=["AUTOMATISCH_ENDE", "ARCH"],
        ...     time_unit="days",
        ...     iqr_multiplier=2.0
        ... )
        >>> outliers.show()

    Note:
        - Outliers are defined as cases with durations below Q1 - (IQR * iqr_multiplier) or 
          above Q3 + (IQR * iqr_multiplier), where Q1 is the first quartile, Q3 is the third quartile, 
          and IQR is the interquartile range.
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

    # Define window specifications
    case_window = Window.partitionBy(case_key_col).orderBy(timestamp_col)
    dimension_window = Window.partitionBy(dimension_col)

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
        F.first(F.when(F.col("is_start") == 1, F.col(timestamp_col))).over(case_window)
    ).withColumn(
        "end_time",
        F.last(F.when(F.col("is_end") == 1, F.col(timestamp_col))).over(case_window)
    )

    # Calculate duration for each case
    df_with_duration = df_with_boundaries.withColumn(
        "duration",
        F.when(
            (F.col("start_time").isNotNull()) & (F.col("end_time").isNotNull()),
            (F.col("end_time").cast("long") - F.col("start_time").cast("long")) / time_multiplier
        )
    ).dropDuplicates([case_key_col])  # Keep one row per case

    # Calculate quartiles and IQR for each dimension
    quartiles = df_with_duration.groupBy(dimension_col).agg(
        F.expr("percentile(duration, array(0.25, 0.75))").alias("quartiles")
    ).select(
        dimension_col,
        F.col("quartiles")[0].alias("q1"),
        F.col("quartiles")[1].alias("q3")
    ).withColumn("iqr", F.col("q3") - F.col("q1"))

    # Join quartiles back to the main DataFrame and identify outliers
    outliers = df_with_duration.join(quartiles, dimension_col).withColumn(
        "lower_bound", F.col("q1") - (F.col("iqr") * iqr_multiplier)
    ).withColumn(
        "upper_bound", F.col("q3") + (F.col("iqr") * iqr_multiplier)
    ).where(
        (F.col("duration") < F.col("lower_bound")) | (F.col("duration") > F.col("upper_bound"))
    ).select(
        dimension_col,
        F.col(case_key_col).alias("case_key"),
        F.round("duration", 2).alias("duration"),
        F.round("q1", 2).alias("q1"),
        F.round("q3", 2).alias("q3"),
        F.round("iqr", 2).alias("iqr"),
        F.round("lower_bound", 2).alias("lower_bound"),
        F.round("upper_bound", 2).alias("upper_bound")
    )

    return outliers
