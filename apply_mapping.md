```python

from pyspark.sql import DataFrame
from typing import List, Optional, Union
from pyspark.sql.functions import col

def save_delta_table(
    df: DataFrame,
    table_name: str,
    path: str,
    mode: str = "overwrite",
    format: str = "delta",
    partition_by: Optional[Union[str, List[str]]] = None,
    z_order_by: Optional[Union[str, List[str]]] = None,
    database: Optional[str] = None
) -> None:
    """
    Save a DataFrame as a Delta table in Azure Data Lake Storage and persist it in the Hive metastore.

    This function writes the DataFrame to the specified path in Delta format and creates or updates
    a table in the Hive metastore. It supports partitioning and Z-Ordering for optimized performance.

    Args:
        df (DataFrame): The DataFrame to be saved.
        table_name (str): The name of the table to be created or updated in the Hive metastore.
        path (str): The ABFSS path where the Delta table will be stored.
        mode (str, optional): The save mode (e.g., "overwrite", "append"). Defaults to "overwrite".
        format (str, optional): The file format. Defaults to "delta".
        partition_by (Union[str, List[str]], optional): Column(s) to partition the data by.
        z_order_by (Union[str, List[str]], optional): Column(s) to Z-Order the data by.
        database (str, optional): The database name where the table should be created. 
                                  If None, the default database will be used.

    Raises:
        ValueError: If an invalid mode or format is provided.

    Example:
        >>> df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        >>> save_delta_table(
        ...     df,
        ...     "my_table",
        ...     "abfss://container@account.dfs.core.windows.net/path/to/table",
        ...     partition_by="id",
        ...     z_order_by="value",
        ...     database="my_database"
        ... )
    """
    if format.lower() != "delta":
        raise ValueError("This function only supports Delta format.")

    if mode.lower() not in ["overwrite", "append", "ignore", "error"]:
        raise ValueError(f"Invalid mode: {mode}. Supported modes are 'overwrite', 'append', 'ignore', and 'error'.")

    # Prepare the writer
    writer = df.write.format(format).mode(mode)

    # Apply partitioning if specified
    if partition_by:
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        writer = writer.partitionBy(*partition_by)

    # Write the data
    writer.save(path)

    # Apply Z-Ordering if specified
    if z_order_by:
        if isinstance(z_order_by, str):
            z_order_by = [z_order_by]
        z_order_cols = ", ".join(z_order_by)
        spark.sql(f"OPTIMIZE '{path}' ZORDER BY ({z_order_cols})")

    # Create or replace the table in the Hive metastore
    full_table_name = f"{database}.{table_name}" if database else table_name
    spark.sql(f"""
    CREATE OR REPLACE TABLE {full_table_name}
    USING DELTA
    LOCATION '{path}'
    """)

    print(f"✅ Table '{full_table_name}' has been successfully saved and persisted.")


```

