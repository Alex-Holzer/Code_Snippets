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
    delimiter: str = ","
) -> DataFrame:
    """
    Collect and combine values for specified columns for each distinct partition and count the values in each collected column.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        partition_by_column (str, optional): Column to partition by. Defaults to "_CASE_KEY".
        order_by_column (str, optional): Column to order by within each partition. Defaults to "EVENTTIME".
        order_direction (str, optional): Order direction ('ASC' or 'DESC'). Defaults to "DESC".
        target_columns (Union[str, List[str]], optional): Column(s) to collect values for.
                                                          If None, all columns except partition_by_column are used.
        preserve_order (bool, optional): If True, use collect_list to preserve order.
                                         If False, use collect_set for unique values. Defaults to True.
        delimiter (str, optional): Delimiter to use when combining collected values. Defaults to ",".

    Returns:
        DataFrame: DataFrame with distinct partition values, combined values for specified columns,
                   and count of values for each collected column.

    Example:
        >>> df = spark.createDataFrame([
        ...     ("case1", "2023-01-01", "value1", "A"),
        ...     ("case1", "2023-01-02", "value2", "B"),
        ...     ("case2", "2023-01-01", "value3", "C"),
        ...     ("case2", "2023-01-02", "value4", "C"),
        ... ], ["_CASE_KEY", "EVENTTIME", "Value1", "Value2"])
        >>> result = collect_values_per_partition(df, target_columns=["Value1", "Value2"], preserve_order=True, delimiter=";")
        >>> result.show(truncate=False)
        +--------+---------------+---------+-------------+-------------+
        |_CASE_KEY|Value1         |Value2   |Value1_count |Value2_count |
        +--------+---------------+---------+-------------+-------------+
        |case1   |value2;value1   |B;A      |2            |2            |
        |case2   |value4;value3   |C;C      |2            |1            |
        +--------+---------------+---------+-------------+-------------+
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

    # Create window specification for ordering
    window_spec = Window.partitionBy(partition_by_column).orderBy(
        F.col(order_by_column).asc() if order_direction.upper() == "ASC" else F.col(order_by_column).desc()
    )

    # Add row numbers within each partition
    df_with_row_num = df.withColumn("row_num", F.row_number().over(window_spec))

    # Prepare the aggregation expressions
    agg_expr = []
    for col in target_columns:
        if preserve_order:
            # Use collect_list to preserve order, then join with delimiter
            agg_expr.append(F.concat_ws(delimiter, F.collect_list(F.col(col))).alias(col))
        else:
            # Use collect_set for unique values, then join with delimiter
            agg_expr.append(F.concat_ws(delimiter, F.collect_set(F.col(col))).alias(col))
        
        # Add count column for each target column
        agg_expr.append(F.count(F.col(col)).alias(f"{col}_count"))

    # Apply the aggregation and ensure distinct partition values
    result_df = df_with_row_num.groupBy(partition_by_column).agg(*agg_expr)

    return result_df

from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name
from typing import List, Optional

def get_combined_csv_dataframe(folder_path: str, recursive: bool = False, file_extension: str = "csv", **kwargs) -> DataFrame:
    """
    Retrieves all CSV files from a specified folder in Databricks and combines them into a single DataFrame.

    This function is optimized for use in Databricks, utilizing dbutils for file listing and leveraging
    the pre-existing SparkSession. It retrieves CSV files, combines them using unionByName, and is designed 
    to handle large datasets efficiently and scalably.

    Args:
        folder_path (str): The path to the folder containing CSV files. Can be a Databricks FileStore path or a mounted path.
        recursive (bool, optional): If True, searches for files recursively in subfolders. Defaults to False.
        file_extension (str, optional): The file extension to filter by. Defaults to "csv".
        **kwargs: Additional keyword arguments to pass to spark.read.csv().
                  These can include options like 'header', 'inferSchema', 'sep', etc.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the combined data from all CSV files.

    Raises:
        ValueError: If no files with the specified extension are found in the given path.

    Example:
        >>> folder_path = "/mnt/data/csv_files"
        >>> df = get_combined_csv_dataframe(folder_path, recursive=True, header=True, inferSchema=True)
        >>> df.show()
    """
    # Use dbutils to list files
    if recursive:
        files = dbutils.fs.ls(folder_path)
        csv_files = [f.path for f in files if f.path.endswith(f'.{file_extension}')]
    else:
        files = dbutils.fs.ls(folder_path)
        csv_files = [f.path for f in files if f.name.endswith(f'.{file_extension}')]

    if not csv_files:
        raise ValueError(f"No .{file_extension} files found in the specified folder: {folder_path}")

    # Read and union all CSV files
    df = spark.read.csv(csv_files[0], **kwargs)
    
    for file in csv_files[1:]:
        df = df.unionByName(
            spark.read.csv(file, **kwargs),
            allowMissingColumns=True
        )

    # Add a column with the source file name
    df = df.withColumn("source_file", input_file_name())

    return df

# Example usage
# df = get_combined_csv_dataframe("/mnt/data/csv_files", recursive=True, header=True, inferSchema=True)
# df.show()


```


