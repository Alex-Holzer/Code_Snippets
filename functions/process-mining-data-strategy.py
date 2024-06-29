from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, to_timestamp, lag, unix_timestamp
from delta.tables import DeltaTable
from typing import List

def prepare_and_cache_process_data(spark, delta_path: str, dimensions: List[str]) -> DataFrame:
    """
    Prepare and cache the process mining data for multiple calculations.
    
    Args:
        spark: SparkSession object
        delta_path (str): Path to the Delta table
        dimensions (List[str]): List of dimension columns to include
    
    Returns:
        DataFrame: Cached DataFrame ready for process mining calculations
    """
    # Optimize Delta table
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.optimize().executeCompaction()
    
    # Read data
    df = spark.read.format("delta").load(delta_path)
    
    # Prepare DataFrame for process mining
    prepared_df = (df
        .withColumn("EVENTTIME", to_timestamp(col("EVENTTIME")))
        .withColumn("prev_EVENTTIME", lag("EVENTTIME").over(Window.partitionBy("_CASE_KEY").orderBy("EVENTTIME")))
        .withColumn("duration_seconds", 
                    unix_timestamp("EVENTTIME") - unix_timestamp("prev_EVENTTIME"))
    )
    
    # Select relevant columns
    columns_to_select = ["_CASE_KEY", "ACTIVITY", "EVENTTIME", "duration_seconds"] + dimensions
    prepared_df = prepared_df.select(columns_to_select)
    
    # Cache the prepared DataFrame
    prepared_df.cache()
    
    # Materialize the cache
    prepared_df.count()
    
    return prepared_df

def calculate_activity_frequency(df: DataFrame) -> DataFrame:
    """Calculate activity frequency."""
    return df.groupBy("ACTIVITY").count().orderBy(col("count").desc())

def calculate_duration_between_activities(df: DataFrame, dimensions: List[str]) -> DataFrame:
    """Calculate average duration between activities for given dimensions."""
    group_cols = ["ACTIVITY"] + dimensions
    return (df
        .groupBy(group_cols)
        .agg({"duration_seconds": "avg"})
        .orderBy(group_cols)
    )

def main(spark):
    delta_path = "dbfs:/path/to/your/delta/table"
    dimensions = ["DIMENSION 1", "DIMENSION 2"]
    
    # Prepare and cache data
    cached_df = prepare_and_cache_process_data(spark, delta_path, dimensions)
    
    # Perform calculations
    activity_freq = calculate_activity_frequency(cached_df)
    duration_between_activities = calculate_duration_between_activities(cached_df, dimensions)
    
    # Show results
    activity_freq.show()
    duration_between_activities.show()
    
    # Perform other calculations as needed...
    
    # When all calculations are done, unpersist the cached DataFrame
    cached_df.unpersist()

# Note: In a Databricks notebook, you would call main(spark) to execute this code.
