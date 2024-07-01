from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    first,
    last,
    collect_set,
    array_contains,
    lit,
)
from pyspark.sql.window import Window
from typing import Union, List, Optional


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
    Highly optimized function to filter process cases based on specified flow conditions
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

    # Precompute case summary
    case_summary = df.groupBy(case_column).agg(
        collect_set(lower(col(activity_column))).alias("activities"),
        first(lower(col(activity_column))).alias("first_activity"),
        last(lower(col(activity_column))).alias("last_activity"),
    )

    # Apply filters
    if flows_through:
        case_summary = case_summary.filter(
            *[
                array_contains(col("activities"), lit(activity))
                for activity in flows_through
            ]
        )

    if not_flows_through:
        case_summary = case_summary.filter(
            *[
                ~array_contains(col("activities"), lit(activity))
                for activity in not_flows_through
            ]
        )

    if starts_with:
        case_summary = case_summary.filter(col("first_activity").isin(starts_with))

    if ends_with:
        case_summary = case_summary.filter(col("last_activity").isin(ends_with))

    # Get the list of cases that meet all conditions
    valid_cases = case_summary.select(case_column)

    # Filter the original DataFrame
    return df.join(valid_cases, case_column, "inner")
