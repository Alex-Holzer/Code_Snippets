```python

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Union

def get_first_values_per_partition(
    df: DataFrame,
    partition_by_column: str = "_CASE_KEY",
    order_by_column: str = "EVENTTIME",
    order_direction: str = "DESC",
    target_columns: Union[str, List[str]] = None,
) -> DataFrame:
    """
    Get the first values (ignoring nulls) for specified columns for each distinct partition.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        partition_by_column (str, optional): Column to partition by. Defaults to "_CASE_KEY".
        order_by_column (str, optional): Column to order by within each partition. Defaults to "EVENTTIME".
        order_direction (str, optional): Order direction ('ASC' or 'DESC'). Defaults to "DESC".
        target_columns (Union[str, List[str]], optional): Column(s) to get first values for. 
                                                          If None, all columns except partition_by_column are used.

    Returns:
        DataFrame: DataFrame with distinct partition values and first non-null values for specified columns.

    Example:
        >>> df = spark.createDataFrame([
        ...     ("case1", "2023-01-01", None, "value1"),
        ...     ("case1", "2023-01-02", "value2", None),
        ...     ("case2", "2023-01-01", "value3", "value4"),
        ... ], ["_CASE_KEY", "EVENTTIME", "Value1", "Value2"])
        >>> result = get_first_values_per_partition(df, target_columns=["Value1", "Value2"])
        >>> result.show()
        +--------+-------+-------+
        |_CASE_KEY|Value1 |Value2 |
        +--------+-------+-------+
        |   case1|value2 |value1 |
        |   case2|value3 |value4 |
        +--------+-------+-------+
    """
    # Input validation
    if not isinstance(df, DataFrame):
        raise ValueError("Input must be a PySpark DataFrame")
    
    if order_direction.upper() not in ["ASC", "DESC"]:
        raise ValueError("order_direction must be either 'ASC' or 'DESC'")
    
    # Ensure all specified columns exist in the DataFrame
    all_columns = set(df.columns)
    if partition_by_column not in all_columns:
        raise ValueError(f"Partition column '{partition_by_column}' not found in DataFrame")
    if order_by_column not in all_columns:
        raise ValueError(f"Order by column '{order_by_column}' not found in DataFrame")

    # Handle target_columns
    if target_columns is None:
        target_columns = list(all_columns - {partition_by_column})
    elif isinstance(target_columns, str):
        target_columns = [target_columns]
    elif not isinstance(target_columns, list):
        raise ValueError("target_columns must be a string, list of strings, or None")

    missing_columns = set(target_columns) - all_columns
    if missing_columns:
        raise ValueError(f"The following columns are not present in the DataFrame: {missing_columns}")

    # Create window specification
    window_spec = Window.partitionBy(partition_by_column).orderBy(
        F.col(order_by_column).asc() if order_direction.upper() == "ASC" else F.col(order_by_column).desc()
    )

    # Select first non-null values for each column
    select_expr = [
        F.first(col, ignorenulls=True).over(window_spec).alias(col)
        for col in target_columns
    ]

    # Add partition column to select expression
    select_expr.append(F.col(partition_by_column))

    # Apply the window functions and group by partition column to ensure uniqueness
    result_df = df.select(*select_expr).groupBy(partition_by_column).agg(
        *[F.first(col).alias(col) for col in target_columns]
    )

    return result_df
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Union


def collect_values_per_partition(
    df: DataFrame,
    partition_by_column: str = "_CASE_KEY",
    order_by_column: str = "EVENTTIME",
    order_direction: str = "DESC",
    target_columns: Union[str, List[str]] = None,
    preserve_order: bool = True,
) -> DataFrame:
    """
    Collect values for specified columns for each distinct partition.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        partition_by_column (str, optional): Column to partition by. Defaults to "_CASE_KEY".
        order_by_column (str, optional): Column to order by within each partition. Defaults to "EVENTTIME".
        order_direction (str, optional): Order direction ('ASC' or 'DESC'). Defaults to "DESC".
        target_columns (Union[str, List[str]], optional): Column(s) to collect values for.
                                                          If None, all columns except partition_by_column are used.
        preserve_order (bool, optional): If True, use collect_list to preserve order.
                                         If False, use collect_set for unique values. Defaults to True.

    Returns:
        DataFrame: DataFrame with distinct partition values and collected values for specified columns.

    Example:
        >>> df = spark.createDataFrame([
        ...     ("case1", "2023-01-01", "value1", "A"),
        ...     ("case1", "2023-01-02", "value2", "B"),
        ...     ("case2", "2023-01-01", "value3", "C"),
        ...     ("case2", "2023-01-02", "value4", "C"),
        ... ], ["_CASE_KEY", "EVENTTIME", "Value1", "Value2"])
        >>> result = collect_values_per_partition(df, target_columns=["Value1", "Value2"], preserve_order=True)
        >>> result.show(truncate=False)
        +--------+------------------+------------+
        |_CASE_KEY|Value1            |Value2      |
        +--------+------------------+------------+
        |case1   |[value2, value1]   |[B, A]      |
        |case2   |[value4, value3]   |[C, C]      |
        +--------+------------------+------------+
    """
    # Input validation
    if not isinstance(df, DataFrame):
        raise ValueError("Input must be a PySpark DataFrame")

    if order_direction.upper() not in ["ASC", "DESC"]:
        raise ValueError("order_direction must be either 'ASC' or 'DESC'")

    # Ensure all specified columns exist in the DataFrame
    all_columns = set(df.columns)
    if partition_by_column not in all_columns:
        raise ValueError(
            f"Partition column '{partition_by_column}' not found in DataFrame"
        )
    if order_by_column not in all_columns:
        raise ValueError(f"Order by column '{order_by_column}' not found in DataFrame")

    # Handle target_columns
    if target_columns is None:
        target_columns = list(all_columns - {partition_by_column})
    elif isinstance(target_columns, str):
        target_columns = [target_columns]
    elif not isinstance(target_columns, list):
        raise ValueError("target_columns must be a string, list of strings, or None")

    missing_columns = set(target_columns) - all_columns
    if missing_columns:
        raise ValueError(
            f"The following columns are not present in the DataFrame: {missing_columns}"
        )

    # Create window specification for ordering (if needed)
    window_spec = Window.partitionBy(partition_by_column).orderBy(
        F.col(order_by_column).asc()
        if order_direction.upper() == "ASC"
        else F.col(order_by_column).desc()
    )

    # Prepare the aggregation expressions
    agg_expr = []
    for col in target_columns:
        if preserve_order:
            # Use collect_list to preserve order
            agg_expr.append(F.collect_list(F.col(col).over(window_spec)).alias(col))
        else:
            # Use collect_set for unique values (order not preserved)
            agg_expr.append(F.collect_set(F.col(col)).alias(col))

    # Add partition column to aggregation expression
    agg_expr.append(F.col(partition_by_column))

    # Apply the aggregation
    result_df = df.select(*agg_expr).distinct()

    return result_df

```


