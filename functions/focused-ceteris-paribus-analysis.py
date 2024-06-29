from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional

def analyze_kpis_ceteris_paribus(
    df: DataFrame,
    target_dimension_col: str,
    ceteris_paribus_cols: List[str],
    case_key_col: str = "_CASE_KEY",
    activity_col: str = "ACTIVITY",
    timestamp_col: str = "EVENTTIME",
    start_activities: Optional[List[str]] = None,
    end_activities: Optional[List[str]] = None,
    time_unit: str = "hours",
    min_cases: int = 5
) -> DataFrame:
    """
    Analyze key performance indicators (KPIs) for a target dimension while keeping other dimensions constant.

    This function calculates the number of cases, completed cases, completion rate, and average completion time
    for each unique value of the target dimension, while controlling for the effects of other specified dimensions 
    (ceteris paribus approach).

    Args:
        df (DataFrame): Input DataFrame containing process data.
        target_dimension_col (str): Column name for the dimension to analyze.
        ceteris_paribus_cols (List[str]): List of column names to keep constant during analysis.
        case_key_col (str): Column name for the case key. Default is "_CASE_KEY".
        activity_col (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_col (str): Column name for the event timestamp. Default is "EVENTTIME".
        start_activities (Optional[List[str]]): List of activities that mark the start of a process.
            If None, the first event for each case is considered the start. Default is None.
        end_activities (Optional[List[str]]): List of activities that mark the end of a process.
            If None, the last event for each case is considered the end. Default is None.
        time_unit (str): Unit for duration calculation. 
            Options are "seconds", "minutes", "hours", "days". Default is "hours".
        min_cases (int): Minimum number of cases required for a dimension combination to be included.
            Default is 5.

    Returns:
        DataFrame: A DataFrame containing the ceteris paribus KPI analysis results with columns:
            - [ceteris_paribus_cols]: Columns representing the constant dimensions
            - target_dimension: The unique value from the target dimension column
            - total_cases: The total number of cases
            - completed_cases: The number of completed cases
            - completion_rate: The rate of completed cases (completed_cases / total_cases)
            - avg_completion_time: The average completion time for completed cases

    Raises:
        ValueError: If input validation fails or if an invalid time unit is provided.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> kpi_results = analyze_kpis_ceteris_paribus(
        ...     df, 
        ...     target_dimension_col="DIMENSION 1",
        ...     ceteris_paribus_cols=["DIMENSION 2", "DIMENSION 3"],
        ...     start_activities=["START", "AENDERUNG"],
        ...     end_activities=["AUTOMATISCH_ENDE", "ARCH"],
        ...     time_unit="days"
        ... )
        >>> kpi_results.show()

    Note:
        - A case is considered completed if it has at least one activity from the end_activities list.
        - If end_activities is None, all cases are considered completed.
        - The function filters out dimension combinations with fewer cases than the specified min_cases.
        - This approach helps isolate the effect of the target dimension on the specified KPIs.
        - Time unit conversion is applied at the end of the calculation to maintain precision.
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

    # Group by all dimensions (target + ceteris paribus)
    all_dimensions = ceteris_paribus_cols + [target_dimension_col]

    # Calculate KPIs
    kpi_results = df_with_completion.groupBy(*all_dimensions).agg(
        F.count("*").alias("total_cases"),
        F.sum("is_completed").alias("completed_cases"),
        F.avg(F.when(F.col("is_completed") == 1, F.col("duration"))).alias("avg_completion_time")
    ).withColumn(
        "completion_rate",
        F.col("completed_cases") / F.col("total_cases")
    ).filter(F.col("total_cases") >= min_cases)  # Filter out combinations with too few cases

    # Round numeric columns for readability
    numeric_columns = ["completion_rate", "avg_completion_time"]
    for col in numeric_columns:
        kpi_results = kpi_results.withColumn(col, F.round(col, 2))

    # Reorder columns for better readability
    final_columns = ceteris_paribus_cols + [target_dimension_col, "total_cases", "completed_cases", 
                                            "completion_rate", "avg_completion_time"]
    
    return kpi_results.select(final_columns)
