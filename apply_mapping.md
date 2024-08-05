```python

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from typing import List, Optional

def save_delta_table(
    df: DataFrame,
    path: str,
    partition_by: Optional[List[str]] = None,
    z_order_by: Optional[List[str]] = None,
    mode: str = "overwrite"
) -> None:
    """
    Saves a DataFrame as a Delta table, performs optimization, and applies Z-Ordering.

    Args:
        df (DataFrame): The DataFrame to be saved.
        path (str): The path where the Delta table should be saved.
        partition_by (Optional[List[str]]): List of columns for partitioning.
        z_order_by (Optional[List[str]]): List of columns for Z-Ordering.
        mode (str): Write mode ("overwrite", "append", etc.). Defaults to "overwrite".

    Returns:
        None

    Example:
        >>> df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        >>> save_delta_table(df, "/path/to/delta", partition_by=["id"], z_order_by=["value"])
    """
    _write_delta_table(df, path, partition_by, mode)
    _optimize_delta_table(path, z_order_by)
    print(f"Delta table has been saved and optimized: {path}")

def _write_delta_table(
    df: DataFrame,
    path: str,
    partition_by: Optional[List[str]] = None,
    mode: str = "overwrite"
) -> None:
    """Helper function to write the DataFrame as a Delta table."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)

def _optimize_delta_table(
    path: str,
    z_order_by: Optional[List[str]] = None
) -> None:
    """Helper function to optimize the Delta table and apply Z-Ordering if specified."""
    delta_table = DeltaTable.forPath(spark, path)
    if z_order_by:
        z_order_cols = ", ".join(z_order_by)
        delta_table.optimize().executeZOrderBy(z_order_cols)
    else:
        delta_table.optimize().executeCompaction()

# Example usage
def transform_and_save_data(input_df: DataFrame, output_path: str) -> None:
    """
    Example of how to use the save_delta_table function in a data processing pipeline.
    
    Args:
        input_df (DataFrame): Input DataFrame to be processed.
        output_path (str): Path to save the processed Delta table.
    """
    processed_df = (input_df
        .transform(clean_data)
        .transform(enrich_data)
        .transform(aggregate_data)
    )
    
    save_delta_table(
        df=processed_df,
        path=output_path,
        partition_by=["date"],
        z_order_by=["id", "category"],
        mode="overwrite"
    )

# Helper functions for the data processing pipeline
def clean_data(df: DataFrame) -> DataFrame:
    """Clean the input DataFrame."""
    # Implement data cleaning logic
    return df

def enrich_data(df: DataFrame) -> DataFrame:
    """Enrich the DataFrame with additional information."""
    # Implement data enrichment logic
    return df

def aggregate_data(df: DataFrame) -> DataFrame:
    """Perform aggregations on the DataFrame."""
    # Implement aggregation logic
    return df

```

