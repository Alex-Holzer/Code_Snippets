from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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
    cache_input: bool = False,
    repartition_by_case: bool = False,
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """
    Highly optimized function to filter process cases based on specified flow conditions for large datasets.
    Includes options for caching and repartitioning to further improve performance.

    This function efficiently filters cases in a process mining dataset based on various conditions:
    - Cases that flow through specific activities
    - Cases that do not flow through specific activities
    - Cases that start with specific activities
    - Cases that end with specific activities

    The function is optimized for maximum performance on large datasets and returns the complete
    original DataFrame (with all columns) for cases that meet the specified conditions.

    Args:
        df (DataFrame): Input DataFrame with process mining data.
        case_flows_through (Optional[Union[str, List[str]]]): Activity or list of activities that cases must flow through.
        case_does_not_flow_through (Optional[Union[str, List[str]]]): Activity or list of activities that cases must not flow through.
        case_starts_with (Optional[Union[str, List[str]]]): Activity or list of activities that cases must start with.
        case_ends_with (Optional[Union[str, List[str]]]): Activity or list of activities that cases must end with.
        case_column (str): Name of the case column. Defaults to "_CASE_KEY".
        activity_column (str): Name of the activity column. Defaults to "ACTIVITY".
        timestamp_column (str): Name of the timestamp column. Defaults to "EVENTTIME".
        cache_input (bool): Whether to cache the input DataFrame. Defaults to False.
        repartition_by_case (bool): Whether to repartition the DataFrame by case_column. Defaults to False.
        num_partitions (Optional[int]): Number of partitions to use if repartitioning. If None, uses Spark's default.

    Returns:
        DataFrame: Filtered DataFrame containing all original columns for cases that meet the specified conditions.

    Example:
        >>> filtered_df = filter_process_cases(
        ...     df,
        ...     case_flows_through=["Middle", "Review"],
        ...     case_does_not_flow_through="Reject",
        ...     case_starts_with="Start",
        ...     case_ends_with=["End", "Complete"],
        ...     cache_input=True,
        ...     repartition_by_case=True,
        ...     num_partitions=200
        ... )
    """
    # If no filtering conditions are provided, return the original DataFrame
    if not any(
        [
            case_flows_through,
            case_does_not_flow_through,
            case_starts_with,
            case_ends_with,
        ]
    ):
        return df

    # Optimize the input DataFrame if requested
    if repartition_by_case:
        if num_partitions:
            df = df.repartition(num_partitions, case_column)
        else:
            df = df.repartition(case_column)

    if cache_input:
        df.cache()
        df.count()  # Force caching

    # Convert single string inputs to lists
    def to_list(x):
        return x if isinstance(x, list) else [x] if x is not None else []

    flows_through = to_list(case_flows_through)
    not_flows_through = to_list(case_does_not_flow_through)
    starts_with = to_list(case_starts_with)
    ends_with = to_list(case_ends_with)

    # Create window specifications
    case_window = Window.partitionBy(case_column).orderBy(timestamp_column)

    # Prepare the case summary DataFrame
    case_summary = df.select(
        F.col(case_column),
        F.col(activity_column),
        F.first(F.col(activity_column)).over(case_window).alias("first_activity"),
        F.last(F.col(activity_column)).over(case_window).alias("last_activity"),
    )

    # Collect set of activities for each case
    case_summary = case_summary.groupBy(case_column).agg(
        F.collect_set(activity_column).alias("activities"),
        F.first("first_activity").alias("first_activity"),
        F.first("last_activity").alias("last_activity"),
    )

    # Build filter conditions
    filter_conditions = []

    if flows_through:
        filter_conditions.append(
            F.size(F.array_intersect(F.col("activities"), F.array(*flows_through)))
            == len(flows_through)
        )

    if not_flows_through:
        filter_conditions.append(
            F.size(F.array_intersect(F.col("activities"), F.array(*not_flows_through)))
            == 0
        )

    if starts_with:
        filter_conditions.append(F.col("first_activity").isin(starts_with))

    if ends_with:
        filter_conditions.append(F.col("last_activity").isin(ends_with))

    # Apply all filter conditions
    if filter_conditions:
        case_summary = case_summary.filter(
            F.reduce(lambda x, y: x & y, filter_conditions)
        )

    # Get the list of cases that meet all conditions
    valid_cases = case_summary.select(case_column)

    # Join with the original DataFrame to get all columns for valid cases
    result = df.join(F.broadcast(valid_cases), case_column, "inner")

    # Unpersist the cached DataFrame if it was cached
    if cache_input:
        df.unpersist()

    return result
