```python
from pyspark.sql import DataFrame
from typing import Optional
from pyspark.sql.utils import AnalysisException

# Constants
DEFAULT_BASE_NAME = "prod_uc_analyticsuw_uc1"
DEFAULT_BASE_PATH = "abfss://prod@eudldeg1koprod1.dfs.core.windows.net/PROD/usecases/AnalyticsUW_UC1"

def construct_delta_table_path(
    table_name: str,
    base_name: str = DEFAULT_BASE_NAME,
    base_path: str = DEFAULT_BASE_PATH
) -> str:
    """
    Construct the full path for a Delta table.
    
    Args:
        table_name (str): The name of the table.
        base_name (str): The base name for the table. Defaults to DEFAULT_BASE_NAME.
        base_path (str): The base path for the table. Defaults to DEFAULT_BASE_PATH.
    
    Returns:
        str: The full path for the Delta table.
    """
    return f"{base_path}/{base_name}/{table_name}"

def check_table_exists(spark, table_name: str) -> bool:
    """
    Check if a Delta table exists.
    
    Args:
        spark: The SparkSession object.
        table_name (str): The name of the table to check.
    
    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        spark.table(table_name)
        return True
    except AnalysisException:
        return False


from pyspark.sql import DataFrame
from typing import Optional

def write_delta_table(
    df: DataFrame,
    table_name: str,
    base_name: str = DEFAULT_BASE_NAME,
    base_path: str = DEFAULT_BASE_PATH,
    overwrite_schema: bool = False
) -> None:
    """
    Write a DataFrame to a Delta table with specified options.
    
    Args:
        df (DataFrame): The DataFrame to write.
        table_name (str): The name of the table.
        base_name (str): The base name for the table. Defaults to DEFAULT_BASE_NAME.
        base_path (str): The base path for the table. Defaults to DEFAULT_BASE_PATH.
        overwrite_schema (bool): Whether to overwrite the schema. Defaults to False.
    """
    full_table_name = f"{base_name}.{table_name}"
    full_table_path = construct_delta_table_path(table_name, base_name, base_path)
    
    write_options = {
        "format": "delta",
        "overwriteSchema": str(overwrite_schema).lower(),
        "delta.columnMapping.mode": "name",
        "mode": "overwrite"
    }
    
    if check_table_exists(df.sparkSession, full_table_name):
        df.write.options(**write_options).saveAsTable(full_table_name)
    else:
        df.write.options(**write_options).saveAsTable(full_table_name, path=full_table_path)


```

