from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable
from typing import Optional, List
import time

def store_dataframe_efficiently(
    df: DataFrame,
    output_path: str,
    case_key_col: str = "_CASE_KEY",
    timestamp_col: str = "EVENTTIME",
    partition_columns: Optional[List[str]] = None,
    num_buckets: int = 200,
    mode: str = "overwrite",
    z_order_cols: Optional[List[str]] = None
) -> None:
    """
    Efficiently store a DataFrame using partitioning, bucketing, and Delta format.

    This function optimizes the storage of large DataFrames with many unique case keys
    by partitioning on both case key and time, using bucketing, and leveraging the
    Delta format for improved query performance and data management.

    Args:
        df (DataFrame): The input DataFrame to be stored.
        output_path (str): The path where the DataFrame will be stored.
        case_key_col (str): The name of the case key column. Default is "_CASE_KEY".
        timestamp_col (str): The name of the timestamp column. Default is "EVENTTIME".
        partition_columns (Optional[List[str]]): Additional columns to partition by.
            Default is None.
        num_buckets (int): The number of buckets to use for the case key column.
            Default is 200.
        mode (str): The save mode to use ("overwrite" or "append"). Default is "overwrite".
        z_order_cols (Optional[List[str]]): Columns to use for Z-Ordering optimization.
            Default is None.

    Raises:
        ValueError: If input validation fails.

    Example:
        >>> df = spark.read.parquet("path/to/input_data")
        >>> store_dataframe_efficiently(
        ...     df,
        ...     "path/to/optimized_data",
        ...     partition_columns=["DIMENSION 1"],
        ...     num_buckets=300,
        ...     z_order_cols=["_CASE_KEY", "EVENTTIME"]
        ... )
    """
    start_time = time.time()

    # Input validation
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame")
    if case_key_col not in df.columns:
        raise ValueError(f"Column '{case_key_col}' not found in DataFrame")
    if timestamp_col not in df.columns:
        raise ValueError(f"Column '{timestamp_col}' not found in DataFrame")
    if partition_columns and not set(partition_columns).issubset(df.columns):
        raise ValueError("One or more partition columns not found in DataFrame")
    if mode not in ["overwrite", "append"]:
        raise ValueError("Mode must be either 'overwrite' or 'append'")
    if z_order_cols and not set(z_order_cols).issubset(df.columns):
        raise ValueError("One or more Z-Order columns not found in DataFrame")

    # Cache the DataFrame for improved performance
    df.cache()
    df.count()  # Materialize the cache

    # Add year and month columns for partitioning
    df_with_partitions = df.withColumn(
        "year", F.year(F.col(timestamp_col))
    ).withColumn(
        "month", F.month(F.col(timestamp_col))
    )

    # Prepare partition columns
    partition_cols = ["year", "month"]
    if partition_columns:
        partition_cols.extend(partition_columns)

    # Write the DataFrame using Delta format with partitioning and bucketing
    (df_with_partitions.write
     .format("delta")
     .partitionBy(*partition_cols)
     .bucketBy(num_buckets, case_key_col)
     .sortBy(case_key_col, timestamp_col)
     .mode(mode)
     .option("overwriteSchema", "true")
     .save(output_path))

    # Optimize the Delta table
    delta_table = DeltaTable.forPath(spark, output_path)
    
    # Perform Z-Ordering if specified
    if z_order_cols:
        delta_table.optimize().executeZOrder(z_order_cols)
    else:
        delta_table.optimize().executeCompaction()

    # Unpersist the cached DataFrame
    df.unpersist()

    end_time = time.time()
    duration = end_time - start_time
    
    print(f"🚀 DataFrame storage optimization complete! 🎉")
    print(f"📊 Data stored at: {output_path}")
    print(f"⏱️ Process completed in {duration:.2f} seconds")
    print(f"🔍 Optimizations applied: Partitioning, Bucketing, Delta format")
    if z_order_cols:
        print(f"🔀 Z-Ordering applied on: {', '.join(z_order_cols)}")
    print("📈 Your data is now supercharged for lightning-fast queries! ⚡")

# Example usage:
# df = spark.read.parquet("path/to/input_data")
# store_dataframe_efficiently(
#     df, 
#     "path/to/optimized_data", 
#     partition_columns=["DIMENSION 1"],
#     z_order_cols=["_CASE_KEY", "EVENTTIME"]
# )
