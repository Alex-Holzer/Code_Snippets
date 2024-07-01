from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Literal

def filter_process_flow(
    df: DataFrame,
    first_activity: str,
    second_activity: str,
    relationship: Literal[
        "directly_followed",
        "followed_anytime_by",
        "not_directly_followed",
        "never_followed_by",
    ],
    case_column: str = "_CASE_KEY",
    activity_column: str = "ACTIVITY",
    timestamp_column: str = "EVENTTIME",
) -> DataFrame:
    """
    Filter the process flow based on the relationship between two specified activities.

    Args:
        df (DataFrame): Input DataFrame with process mining data.
        first_activity (str): The first activity in the relationship.
        second_activity (str): The second activity in the relationship.
        relationship (Literal["directly_followed", "followed_anytime_by", "not_directly_followed", "never_followed_by"]):
            The type of relationship between the two activities.
        case_column (str): Name of the case column. Defaults to "_CASE_KEY".
        activity_column (str): Name of the activity column. Defaults to "ACTIVITY".
        timestamp_column (str): Name of the timestamp column. Defaults to "EVENTTIME".

    Returns:
        DataFrame: Filtered DataFrame containing only the cases that meet the specified relationship.

    Raises:
        ValueError: If an invalid relationship is provided.

    Example:
        >>> filtered_df = filter_process_flow(
        ...     df,
        ...     first_activity="Start",
        ...     second_activity="Review",
        ...     relationship="directly_followed"
        ... )
    """
    # Validate the relationship parameter
    valid_relationships = [
        "directly_followed",
        "followed_anytime_by",
        "not_directly_followed",
        "never_followed_by",
    ]
    if relationship not in valid_relationships:
        raise ValueError(
            f"Invalid relationship. Must be one of: {', '.join(valid_relationships)}"
        )

    # Create a window spec for each case, ordered by timestamp
    case_window = Window.partitionBy(case_column).orderBy(timestamp_column)

    # Convert activities to lowercase for case-insensitive matching
    df = df.withColumn(activity_column, F.lower(F.col(activity_column)))
    first_activity = first_activity.lower()
    second_activity = second_activity.lower()

    if relationship == "directly_followed":
        # Add a column with the next activity
        df_with_next = df.withColumn(
            "next_activity", F.lead(F.col(activity_column)).over(case_window)
        )

        # Filter cases where first_activity is directly followed by second_activity
        filtered_cases = (
            df_with_next.filter(
                (F.col(activity_column) == first_activity)
                & (F.col("next_activity") == second_activity)
            )
            .select(case_column)
            .distinct()
        )

    elif relationship == "followed_anytime_by":
        # Collect all activities for each case
        case_activities = df.groupBy(case_column).agg(
            F.collect_list(activity_column).alias("activities")
        )

        # Filter cases where first_activity appears before second_activity
        filtered_cases = case_activities.filter(
            (F.array_contains(F.col("activities"), first_activity))
            & (F.array_contains(F.col("activities"), second_activity))
            & (
                F.array_position(F.col("activities"), first_activity)
                < F.array_position(F.col("activities"), second_activity)
            )
        ).select(case_column)

    elif relationship == "not_directly_followed":
        # Add a column with the next activity
        df_with_next = df.withColumn(
            "next_activity", F.lead(F.col(activity_column)).over(case_window)
        )

        # Filter cases where first_activity is not directly followed by second_activity
        filtered_cases = (
            df_with_next.filter(
                (F.col(activity_column) == first_activity)
                & (F.col("next_activity") != second_activity)
            )
            .select(case_column)
            .distinct()
        )

    else:  # never_followed_by
        # Collect all activities for each case
        case_activities = df.groupBy(case_column).agg(
            F.collect_list(activity_column).alias("activities")
        )

        # Filter cases where first_activity appears but is never followed by second_activity
        filtered_cases = case_activities.filter(
            (F.array_contains(F.col("activities"), first_activity))
            & (
                ~F.array_contains(F.col("activities"), second_activity)
                | (
                    F.array_position(F.col("activities"), first_activity)
                    > F.array_position(F.col("activities"), second_activity)
                )
            )
        ).select(case_column)

    # Join the filtered cases back to the original DataFrame
    return df.join(filtered_cases, case_column, "inner")
