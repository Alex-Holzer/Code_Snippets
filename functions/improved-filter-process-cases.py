from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Union, List, Optional
from functools import reduce


def filter_process_cases(
    df: DataFrame,
    case_flows_through: Optional[Union[str, List[str]]] = None,
    case_does_not_flow_through: Optional[Union[str, List[str]]] = None,
    case_starts_with: Optional[Union[str, List[str]]] = None,
    case_ends_with: Optional[Union[str, List[str]]] = None,
    case_column: str = "_CASE_KEY",
    activity_column: str = "ACTIVITY",
    timestamp_column: str = "EVENTTIME",
) -> DataFrame:
    """
    High-performance function to filter process cases based on specified flow conditions
    for large datasets (case-insensitive).

    Args:
        df (DataFrame): Input DataFrame with process mining data.
        case_flows_through (Optional[Union[str, List[str]]]): Activity or list of activities that cases must flow through.
        case_does_not_flow_through (Optional[Union[str, List[str]]]): Activity or list of activities that cases must not flow through.
        case_starts_with (Optional[Union[str, List[str]]]): Activity or list of activities that cases must start with.
        case_ends_with (Optional[Union[str, List[str]]]): Activity or list of activities that cases must end with.
        case_column (str): Name of the case column. Defaults to "_CASE_KEY".
        activity_column (str): Name of the activity column. Defaults to "ACTIVITY".
        timestamp_column (str): Name of the timestamp column. Defaults to "EVENTTIME".

    Returns:
        DataFrame: Filtered DataFrame containing only the cases that meet all specified conditions.

    Example:
        >>> filtered_df = filter_process_cases(
        ...     df,
        ...     case_flows_through=["Middle", "Review"],
        ...     case_does_not_flow_through="Reject",
        ...     case_starts_with="Start",
        ...     case_ends_with=["End", "Complete"]
        ... )
    """

    # Helper function to convert input to lowercase list
    def to_lower_list(x):
        if x is None:
            return []
        return [s.lower() for s in (x if isinstance(x, list) else [x])]

    # Convert inputs to lowercase lists
    flows_through = to_lower_list(case_flows_through)
    not_flows_through = to_lower_list(case_does_not_flow_through)
    starts_with = to_lower_list(case_starts_with)
    ends_with = to_lower_list(case_ends_with)

    # If no filtering conditions are provided, return the original DataFrame
    if not any([flows_through, not_flows_through, starts_with, ends_with]):
        return df

    # Create a window spec for each case
    case_window = Window.partitionBy(case_column).orderBy(timestamp_column)

    # Optimize by selecting only necessary columns and applying lowercase once
    df_optimized = df.select(
        F.col(case_column),
        F.lower(F.col(activity_column)).alias("activity_lower"),
        F.col(timestamp_column),
    )

    # Calculate first and last activities efficiently
    df_with_endpoints = df_optimized.withColumn(
        "is_first", F.row_number().over(case_window) == 1
    ).withColumn(
        "is_last",
        F.row_number().over(case_window.orderBy(F.desc(timestamp_column))) == 1,
    )

    # Aggregate case information efficiently
    case_summary = df_with_endpoints.groupBy(case_column).agg(
        F.collect_set("activity_lower").alias("activities"),
        F.first(F.when(F.col("is_first"), F.col("activity_lower"))).alias(
            "first_activity"
        ),
        F.first(F.when(F.col("is_last"), F.col("activity_lower"))).alias(
            "last_activity"
        ),
    )

    # Prepare filter conditions
    filter_conditions = []

    if flows_through:
        filter_conditions.extend(
            [
                F.array_contains(F.col("activities"), F.lit(activity))
                for activity in flows_through
            ]
        )

    if not_flows_through:
        filter_conditions.extend(
            [
                ~F.array_contains(F.col("activities"), F.lit(activity))
                for activity in not_flows_through
            ]
        )

    if starts_with:
        filter_conditions.append(F.col("first_activity").isin(starts_with))

    if ends_with:
        filter_conditions.append(F.col("last_activity").isin(ends_with))

    # Apply all filter conditions at once
    if filter_conditions:
        combined_filter = reduce(lambda x, y: x & y, filter_conditions)
        case_summary = case_summary.filter(combined_filter)

    # Get the list of cases that meet all conditions
    valid_cases = case_summary.select(case_column)

    # Filter the original DataFrame
    return df.join(F.broadcast(valid_cases), case_column, "inner")
