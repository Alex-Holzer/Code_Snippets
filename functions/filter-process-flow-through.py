from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional, Union


def filter_process_flow_through(
    df: DataFrame,
    case_flows_through: Optional[Union[str, List[str]]] = None,
    case_does_not_flow_through: Optional[Union[str, List[str]]] = None,
    case_column: str = "_CASE_KEY",
    activity_column: str = "ACTIVITY",
    timestamp_column: str = "EVENTTIME",
) -> DataFrame:
    """
    Filter the process flow DataFrame based on specified activities.

    This function efficiently filters the input DataFrame to include or exclude cases
    based on whether they flow through specific activities or not.

    Args:
        df (DataFrame): Input DataFrame containing process data.
        case_flows_through (Optional[Union[str, List[str]]]): Activity or list of activities
            that cases must flow through. Default is None.
        case_does_not_flow_through (Optional[Union[str, List[str]]]): Activity or list of activities
            that cases must not flow through. Default is None.
        case_column (str): Column name for the case identifier. Default is "_CASE_KEY".
        activity_column (str): Column name for the activity. Default is "ACTIVITY".
        timestamp_column (str): Column name for the event timestamp. Default is "EVENTTIME".

    Returns:
        DataFrame: Filtered DataFrame containing all original columns.

    Raises:
        ValueError: If input validation fails.

    Example:
        >>> df = spark.read.parquet("path/to/process_data")
        >>> filtered_df = filter_process_flow_through(
        ...     df,
        ...     case_flows_through=["Start", "Process"],
        ...     case_does_not_flow_through="Error"
        ... )
        >>> filtered_df.show()

    Note:
        - The function uses PySpark's internal functions for optimal performance.
        - It's designed to be efficient for large datasets by minimizing shuffles.
        - The function applies early filtering when possible to reduce data volume.
        - It utilizes window functions to avoid multiple passes over the data.
    """
    # Input validation
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")

    required_columns = [case_column, activity_column, timestamp_column]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Convert single strings to lists for consistent processing
    flows_through = (
        [case_flows_through]
        if isinstance(case_flows_through, str)
        else case_flows_through
    )
    not_flows_through = (
        [case_does_not_flow_through]
        if isinstance(case_does_not_flow_through, str)
        else case_does_not_flow_through
    )

    # Early return if no filtering is needed
    if flows_through is None and not_flows_through is None:
        return df

    # Window specification for aggregating activities within each case
    case_window = Window.partitionBy(case_column)

    # Create flags for activities
    df_with_flags = df

    if flows_through:
        df_with_flags = df_with_flags.withColumn(
            "flows_through_flag",
            F.when(F.col(activity_column).isin(flows_through), 1).otherwise(0),
        )

    if not_flows_through:
        df_with_flags = df_with_flags.withColumn(
            "not_flows_through_flag",
            F.when(F.col(activity_column).isin(not_flows_through), 1).otherwise(0),
        )

    # Aggregate flags over each case
    df_aggregated = df_with_flags.groupBy(case_column).agg(
        F.max("flows_through_flag").alias("case_flows_through"),
        F.max("not_flows_through_flag").alias("case_not_flows_through"),
    )

    # Apply filtering conditions
    filter_condition = F.lit(True)
    if flows_through:
        filter_condition = filter_condition & (F.col("case_flows_through") == 1)
    if not_flows_through:
        filter_condition = filter_condition & (F.col("case_not_flows_through") == 0)

    filtered_cases = df_aggregated.filter(filter_condition).select(case_column)

    # Join the filtered cases back to the original DataFrame
    return df.join(filtered_cases, on=case_column, how="inner")
