from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, to_timestamp
from delta.tables import DeltaTable

def optimize_and_store_dataframe(df: DataFrame, 
                                 storage_path: str, 
                                 partition_cols: list = ['year', 'month'],
                                 case_key_col: str = "_CASE_KEY",
                                 event_time_col: str = "EVENTTIME") -> None:
    """
    Optimize and store a DataFrame efficiently for later process mining analysis.

    This function performs the following optimizations:
    1. Adds year and month columns for partitioning
    2. Repartitions the DataFrame by _CASE_KEY
    3. Writes the data in Delta format with optimized settings
    4. Optimizes the table and computes statistics for query optimization

    Args:
        df (DataFrame): Input DataFrame to be stored
        storage_path (str): Path where the optimized data will be stored
        partition_cols (list): Columns to use for partitioning (default: ['year', 'month'])
        case_key_col (str): Name of the case key column (default: "_CASE_KEY")
        event_time_col (str): Name of the event time column (default: "EVENTTIME")

    Returns:
        None

    Example:
        >>> df = spark.read.parquet("path/to/original/data")
        >>> df_hashed = hash_case_key(df)
        >>> optimize_and_store_dataframe(df_hashed, "dbfs:/path/to/optimized/data")
    """
    # Add year and month columns for partitioning
    df_optimized = df.withColumn("year", year(to_timestamp(event_time_col))) \
                     .withColumn("month", month(to_timestamp(event_time_col)))

    # Repartition the DataFrame by _CASE_KEY
    num_partitions = df_optimized.rdd.getNumPartitions()
    df_optimized = df_optimized.repartition(num_partitions, case_key_col)

    # Write the data in Delta format with optimized settings
    (df_optimized.write
        .format("delta")
        .partitionBy(partition_cols)
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .save(storage_path))

    # Optimize the Delta table
    delta_table = DeltaTable.forPath(spark, storage_path)
    delta_table.optimize().executeCompaction()

    # Compute statistics for query optimization
    delta_table.generate("SYMLINK_FORMAT_MANIFEST")
    delta_table.vacuum()

    print(f"DataFrame optimized and stored successfully at {storage_path}")

# Usage example
def main(spark):
    # Assume df_hashed is your DataFrame with hashed _CASE_KEY
    storage_path = "dbfs:/path/to/optimized/process_mining_data"
    optimize_and_store_dataframe(df_hashed, storage_path)

    # To read the optimized data later:
    optimized_df = spark.read.format("delta").load(storage_path)
    optimized_df.show()

# Note: The main function is provided for demonstration purposes.
# In a real Databricks notebook, you would call this function directly on your DataFrame.
