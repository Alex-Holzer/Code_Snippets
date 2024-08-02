```python
from pyspark.sql import DataFrame
from typing import Optional
import dbutils

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

def check_path_exists(path: str) -> bool:
    """
    Check if a path exists in Databricks file system.
    
    Args:
        path (str): The path to check.
    
    Returns:
        bool: True if the path exists, False otherwise.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


from pyspark.sql import DataFrame
from typing import Optional

from pyspark.sql import DataFrame
from typing import Optional

def write_delta_table(
    df: DataFrame,
    table_name: str,
    base_name: str = DEFAULT_BASE_NAME,
    base_path: str = DEFAULT_BASE_PATH,
    overwrite_schema: bool = False,
    overwrite_table: bool = False
) -> None:
    """
    Write a DataFrame to a Delta table in Databricks with specified options.
    
    Args:
        df (DataFrame): The DataFrame to write.
        table_name (str): The name of the table.
        base_name (str): The base name for the table. Defaults to DEFAULT_BASE_NAME.
        base_path (str): The base path for the table. Defaults to DEFAULT_BASE_PATH.
        overwrite_schema (bool): Whether to overwrite the schema. Defaults to False.
        overwrite_table (bool): Whether to overwrite the entire table. Defaults to False.
    """
    full_table_name = f"{base_name}.{table_name}"
    full_table_path = construct_delta_table_path(table_name, base_name, base_path)
    
    write_options = {
        "format": "delta",
        "delta.columnMapping.mode": "name",
    }
    
    if overwrite_table:
        write_options["mode"] = "overwrite"
        write_options["overwriteSchema"] = "true"
    elif overwrite_schema:
        write_options["mode"] = "overwrite"
        write_options["overwriteSchema"] = "true"
    else:
        write_options["mode"] = "append"
        write_options["mergeSchema"] = "true"
    
    if check_path_exists(full_table_path):
        # Table already exists
        df.write.options(**write_options).saveAsTable(full_table_name)
    else:
        # Table doesn't exist, specify the path
        df.write.options(**write_options).saveAsTable(full_table_name, path=full_table_path)


```

