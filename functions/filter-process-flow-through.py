from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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
        >>> df = spark.createDataFrame([
        ...     ("case1", "Start", "2023-01-01"),
        ...     ("case1", "Process", "2023-01-02"),
        ...     ("case1", "End", "2023-01-03"),
        ...     ("case2", "Start", "2023-01-01"),
        ...     ("case2", "Error", "2023-01-02"),
        ...     ("case2", "End", "2023-01-03"),
        ... ], ["_CASE_KEY", "ACTIVITY", "EVENTTIME"])
        >>> filtered_df = filter_process_flow_through(
        ...     df,
        ...     case_flows_through="Process",
        ...     case_does_not_flow_through="Error"
        ... )
        >>> filtered_df.show()
        +_CASE_KEY|ACTIVITY|EVENTTIME
        +---------+--------+----------
        |    case1|   Start|2023-01-01
        |    case1| Process|2023-01-02
        |    case1|     End|2023-01-03

    Note:
        - The function uses PySpark's internal functions for optimal performance.
        - It's designed to be efficient for large datasets by minimizing shuffles.
        - The function applies early filtering when possible to reduce data volume.
        - It utilizes efficient aggregation to avoid multiple passes over the data.
    """
    # Input validation
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")

    required_columns = [case_column, activity_column]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Convert single strings to lists for consistent processing
    flows_through = (
        [case_flows_through]
        if isinstance(case_flows_through, str)
        else case_flows_through or []
    )
    not_flows_through = (
        [case_does_not_flow_through]
        if isinstance(case_does_not_flow_through, str)
        else case_does_not_flow_through or []
    )

    # Early return if no filtering is needed
    if not flows_through and not not_flows_through:
        return df

    # Create aggregation conditions
    agg_conditions = []

    if flows_through:
        agg_conditions.append(
            F.max(
                F.when(F.col(activity_column).isin(flows_through), 1).otherwise(0)
            ).alias("flows_through_flag")
        )

    if not_flows_through:
        agg_conditions.append(
            F.min(
                F.when(F.col(activity_column).isin(not_flows_through), 0).otherwise(1)
            ).alias("not_flows_through_flag")
        )

    # Aggregate flags over each case
    df_aggregated = df.groupBy(case_column).agg(*agg_conditions)

    # Apply filtering conditions
    filter_condition = F.lit(True)
    if flows_through:
        filter_condition = filter_condition & (F.col("flows_through_flag") == 1)
    if not_flows_through:
        filter_condition = filter_condition & (F.col("not_flows_through_flag") == 1)

    filtered_cases = df_aggregated.filter(filter_condition).select(case_column)

    # Join the filtered cases back to the original DataFrame
    return df.join(filtered_cases, on=case_column, how="inner")


# Test the function
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("TestFilterProcessFlow").getOrCreate()

# Create a sample DataFrame
sample_data = [
    ("case1", "Start", "2023-01-01"),
    ("case1", "Process", "2023-01-02"),
    ("case1", "End", "2023-01-03"),
    ("case2", "Start", "2023-01-01"),
    ("case2", "Error", "2023-01-02"),
    ("case2", "End", "2023-01-03"),
    ("case3", "Start", "2023-01-01"),
    ("case3", "Process", "2023-01-02"),
    ("case3", "Error", "2023-01-03"),
    ("case3", "End", "2023-01-04"),
]
df = spark.createDataFrame(sample_data, ["_CASE_KEY", "ACTIVITY", "EVENTTIME"])

# Test cases
print("Original DataFrame:")
df.show()

print("\nTest 1: Filter cases that flow through 'Process'")
result1 = filter_process_flow_through(df, case_flows_through="Process")
result1.show()

print("\nTest 2: Filter cases that do not flow through 'Error'")
result2 = filter_process_flow_through(df, case_does_not_flow_through="Error")
result2.show()

print(
    "\nTest 3: Filter cases that flow through 'Process' and do not flow through 'Error'"
)
result3 = filter_process_flow_through(
    df, case_flows_through="Process", case_does_not_flow_through="Error"
)
result3.show()

print("\nTest 4: Filter cases that flow through ['Start', 'End']")
result4 = filter_process_flow_through(df, case_flows_through=["Start", "End"])
result4.show()

# Clean up
spark.stop()
