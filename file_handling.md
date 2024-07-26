```python

def list_files_in_folder(folder_path):
    """
    List all files in a specified folder using Databricks' dbutils.

    Args:
        folder_path (str): The full path to the folder in the data lake storage.

    Returns:
        list: A list of file paths in the specified folder.

    Raises:
        ValueError: If the folder_path is empty or None.

    Example:
        >>> folder_path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW/Rohdaten"
        >>> files = list_files_in_folder(folder_path)
        >>> for file in files:
        ...     print(file)
    """
    if not folder_path:
        raise ValueError("folder_path cannot be empty or None")

    try:
        # Use dbutils.fs.ls to list files and directories in the specified path
        file_list = dbutils.fs.ls(folder_path)
        
        # Filter out directories and return only file paths
        return [file.path for file in file_list if not file.isDir()]
    except Exception as e:
        print(f"An error occurred while listing files: {str(e)}")
        return []

# Example usage
# folder_path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW/Rohdaten"
# files = list_files_in_folder(folder_path)
# for file in files:
#     print(file)



def list_directories_in_path(path):
    """
    List all directories in a specified path using Databricks' dbutils.

    Args:
        path (str): The full path to the directory in the data lake storage.

    Returns:
        list: A list of directory paths in the specified path.

    Raises:
        ValueError: If the path is empty or None.

    Example:
        >>> path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW"
        >>> directories = list_directories_in_path(path)
        >>> for directory in directories:
        ...     print(directory)
    """
    if not path:
        raise ValueError("path cannot be empty or None")

    try:
        # Use dbutils.fs.ls to list files and directories in the specified path
        all_items = dbutils.fs.ls(path)
        
        # Filter out files and return only directory paths
        return [item.path for item in all_items if item.isDir()]
    except Exception as e:
        print(f"An error occurred while listing directories: {str(e)}")
        return []

# Example usage
# path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW"
# directories = list_directories_in_path(path)
# for directory in directories:
#     print(directory)



from typing import List, Dict

def recursive_directory_listing(path: str) -> List[Dict[str, str]]:
    """
    Recursively list all files and directories under a given path.

    Args:
        path (str): The full path to the directory in the data lake storage.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing 'path' and 'type' 
        ('file' or 'directory') for each item found.

    Raises:
        ValueError: If the path is empty or None.

    Example:
        >>> path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW"
        >>> items = recursive_directory_listing(path)
        >>> for item in items:
        ...     print(f"{item['type']}: {item['path']}")
    """
    if not path:
        raise ValueError("path cannot be empty or None")

    def _recursive_list(current_path: str) -> List[Dict[str, str]]:
        try:
            items = dbutils.fs.ls(current_path)
            result = []
            for item in items:
                if item.isDir():
                    result.append({"path": item.path, "type": "directory"})
                    result.extend(_recursive_list(item.path))
                else:
                    result.append({"path": item.path, "type": "file"})
            return result
        except Exception as e:
            print(f"An error occurred while listing {current_path}: {str(e)}")
            return []

    return _recursive_list(path)

# Example usage
# path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW"
# items = recursive_directory_listing(path)
# for item in items:
#     print(f"{item['type']}: {item['path']}")



def check_path_exists(path: str) -> bool:
    """
    Check if a file or directory exists at the specified path.

    Args:
        path (str): The full path to the file or directory in the data lake storage.

    Returns:
        bool: True if the path exists, False otherwise.

    Raises:
        ValueError: If the path is empty or None.

    Example:
        >>> path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW/example.csv"
        >>> exists = check_path_exists(path)
        >>> print(f"The path {'exists' if exists else 'does not exist'}")
    """
    if not path:
        raise ValueError("path cannot be empty or None")

    try:
        return dbutils.fs.ls(path) is not None
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return False
        else:
            print(f"An error occurred while checking path existence: {str(e)}")
            return False

# Example usage
# path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW/example.csv"
# exists = check_path_exists(path)
# print(f"The path {'exists' if exists else 'does not exist'}")


import fnmatch
from typing import List, Dict

def list_files_by_pattern(directory: str, pattern: str) -> List[Dict[str, str]]:
    """
    List all files in a specified directory that match a given pattern.

    Args:
        directory (str): The full path to the directory in the data lake storage.
        pattern (str): The pattern to match against file names. Supports wildcards (* and ?).

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing 'path' and 'name' 
        for each file that matches the pattern.

    Raises:
        ValueError: If the directory or pattern is empty or None.

    Example:
        >>> directory = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW"
        >>> pattern = "*.csv"
        >>> files = list_files_by_pattern(directory, pattern)
        >>> for file in files:
        ...     print(f"Name: {file['name']}, Path: {file['path']}")
    """
    if not directory or not pattern:
        raise ValueError("directory and pattern cannot be empty or None")

    def _match_pattern(name: str, pattern: str) -> bool:
        return fnmatch.fnmatch(name.lower(), pattern.lower())

    try:
        all_files = dbutils.fs.ls(directory)
        matched_files = [
            {"name": file.name, "path": file.path}
            for file in all_files
            if not file.isDir() and _match_pattern(file.name, pattern)
        ]
        return matched_files
    except Exception as e:
        print(f"An error occurred while listing files: {str(e)}")
        return []

# Example usage
# directory = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW"
# pattern = "*.csv"
# files = list_files_by_pattern(directory, pattern)
# for file in files:
#     print(f"Name: {file['name']}, Path: {file['path']}")


import pandas as pd
from typing import Optional, Any
from pyspark.sql import DataFrame
import tempfile
import os

def extract_xlsx_to_dataframe(file_path: str, sheet_name: Optional[str] = None, **kwargs: Any) -> Optional[DataFrame]:
    """
    Extract data from a specified sheet of an XLSX file in ABFS and convert it to a PySpark DataFrame.

    Args:
        file_path (str): The full path to the XLSX file in the ABFS storage.
        sheet_name (str, optional): The name or index of the sheet to extract. 
                                    If None, the first sheet is used.
        **kwargs: Additional keyword arguments to pass to pd.read_excel().
                  Common options include:
                  - header (int, list of int, default 0): Row (0-indexed) to use for the column labels.
                  - names (array-like, optional): List of column names to use.
                  - usecols (str, list-like, or callable, optional): Columns to read.
                  - skiprows (list-like, int, or callable, optional): Line numbers to skip.
                  - nrows (int, optional): Number of rows to read.

    Returns:
        DataFrame or None: A PySpark DataFrame containing the data from the specified sheet,
                           or None if an error occurs.

    Raises:
        ValueError: If the file_path is empty or None.

    Example:
        >>> file_path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW/data.xlsx"
        >>> df = extract_xlsx_to_dataframe(file_path, sheet_name="Sheet1", header=0, usecols="A:C")
        >>> if df is not None:
        ...     df.show()
    """
    if not file_path:
        raise ValueError("file_path cannot be empty or None")

    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            temp_path = temp_file.name

        # Copy the XLSX file from ABFS to the temporary file
        dbutils.fs.cp(file_path, f"file:{temp_path}")

        # Read the XLSX file into a pandas DataFrame
        pdf = pd.read_excel(temp_path, sheet_name=sheet_name, **kwargs)

        # Convert pandas DataFrame to PySpark DataFrame
        df = spark.createDataFrame(pdf)

        # Clean up the temporary file
        os.unlink(temp_path)
        
        return df
    except Exception as e:
        print(f"An error occurred while extracting XLSX file: {str(e)}")
        # Clean up the temporary file in case of an error
        if 'temp_path' in locals():
            os.unlink(temp_path)
        return None

# Example usage
# file_path = "abfss://prod@eudldegikoproddl.dfs.core.windows.net/PROD/usecases/AnalyticsUW/data.xlsx"
# df = extract_xlsx_to_dataframe(file_path, sheet_name="Sheet1", header=0, usecols="A:C")
# if df is not None:
#     df.show()




-------- DELTA SAVE -------

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from typing import Optional, List, Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_save_params(
    df: DataFrame, 
    path: str, 
    mode: str, 
    options: Dict[str, Any],
    partition_by: Optional[List[str]] = None,
    optimize: bool = False,
    zorder_by: Optional[List[str]] = None
) -> None:
    """
    Validate the input parameters for the save_delta_format function.
    
    Args:
        df (DataFrame): The PySpark DataFrame to be saved.
        path (str): The path where the DataFrame should be saved in ADLS.
        mode (str): The save mode ('overwrite', 'append', 'ignore', or 'error').
        options (Dict[str, Any]): Additional options for saving the Delta table.
        partition_by (Optional[List[str]]): Columns to partition the data by.
        optimize (bool): Whether to run OPTIMIZE command after saving.
        zorder_by (Optional[List[str]]): List of columns to ZORDER by.

    Raises:
        ValueError: If any of the input parameters are invalid.
    """
    if not isinstance(df, DataFrame):
        raise ValueError("Input 'df' must be a PySpark DataFrame.")
    
    if not isinstance(path, str) or not path.strip():
        raise ValueError("'path' must be a non-empty string.")
    
    valid_modes = {'overwrite', 'append', 'ignore', 'error'}
    if mode not in valid_modes:
        raise ValueError(f"Invalid mode. Must be one of: {', '.join(valid_modes)}")
    
    if not isinstance(options, dict):
        raise ValueError("'options' must be a dictionary.")
    
    if partition_by is not None:
        if not isinstance(partition_by, list) or not all(isinstance(col, str) for col in partition_by):
            raise ValueError("'partition_by' must be None or a list of strings.")
        if not set(partition_by).issubset(df.columns):
            raise ValueError("All columns in 'partition_by' must exist in the DataFrame.")
    
    if not isinstance(optimize, bool):
        raise ValueError("'optimize' must be a boolean value.")
    
    if zorder_by is not None:
        if not isinstance(zorder_by, list) or not all(isinstance(col, str) for col in zorder_by):
            raise ValueError("'zorder_by' must be None or a list of strings.")
        if not set(zorder_by).issubset(df.columns):
            raise ValueError("All columns in 'zorder_by' must exist in the DataFrame.")

def save_delta_format(
    df: DataFrame, 
    path: str, 
    mode: str = "overwrite", 
    options: Dict[str, Any] = {},
    partition_by: Optional[List[str]] = None,
    optimize: bool = False,
    zorder_by: Optional[List[str]] = None,
    optimize_options: Dict[str, Any] = {}
) -> None:
    """
    Save a PySpark DataFrame in Delta format to Azure Data Lake Storage with advanced options.

    This function saves the input DataFrame in Delta format to the specified path in Azure Data Lake Storage.
    It supports partitioning, custom save options, and optional OPTIMIZE and ZORDER operations.

    Args:
        df (DataFrame): The PySpark DataFrame to be saved.
        path (str): The path where the DataFrame should be saved in ADLS.
        mode (str): The save mode ('overwrite', 'append', 'ignore', or 'error'). Default is 'overwrite'.
        options (Dict[str, Any]): Additional options for saving the Delta table (e.g., {'overwriteSchema': 'true'}).
        partition_by (Optional[List[str]]): Columns to partition the data by.
        optimize (bool): Whether to run OPTIMIZE command after saving. Default is False.
        zorder_by (Optional[List[str]]): List of columns to ZORDER by.
        optimize_options (Dict[str, Any]): Additional options for the OPTIMIZE command.

    Returns:
        None

    Example:
        >>> df = spark.createDataFrame([(1, "Alice", "2021-01-01"), (2, "Bob", "2021-01-02")], ["id", "name", "date"])
        >>> save_delta_format(
        ...     df, 
        ...     "abfss://container@storage.dfs.core.windows.net/path/to/delta",
        ...     mode="overwrite",
        ...     options={"overwriteSchema": "true"},
        ...     partition_by=["date"],
        ...     optimize=True,
        ...     zorder_by=["id", "name"],
        ...     optimize_options={"maxFilesPerTxn": 2}
        ... )
    """
    try:
        # Validate input parameters
        validate_save_params(df, path, mode, options, partition_by, optimize, zorder_by)

        # Prepare the writer
        writer = df.write.format("delta").mode(mode)

        # Apply partitioning if specified
        if partition_by:
            writer = writer.partitionBy(partition_by)

        # Apply additional options
        for key, value in options.items():
            writer = writer.option(key, value)

        # Save the DataFrame in Delta format
        writer.save(path)
        logger.info(f"Successfully saved DataFrame in Delta format to: {path}")

        # Perform OPTIMIZE and ZORDER if requested
        if optimize:
            delta_table = DeltaTable.forPath(spark, path)
            optimize_builder = delta_table.optimize()

            # Apply custom optimize options
            for key, value in optimize_options.items():
                optimize_builder = optimize_builder.option(key, value)

            if zorder_by:
                logger.info(f"Running OPTIMIZE and ZORDER BY {', '.join(zorder_by)}...")
                optimize_builder.executeZOrderBy(zorder_by)
            else:
                logger.info("Running OPTIMIZE...")
                optimize_builder.executeCompaction()
            
            logger.info("Optimization completed successfully.")

    except Exception as e:
        logger.error(f"Error saving DataFrame: {str(e)}")
        raise

def save_delta_wrapper(
    path: str, 
    mode: str = "overwrite", 
    options: Dict[str, Any] = {},
    partition_by: Optional[List[str]] = None,
    optimize: bool = False,
    zorder_by: Optional[List[str]] = None,
    optimize_options: Dict[str, Any] = {}
):
    """
    Wrapper function to use save_delta_format with DataFrame.transform().

    Args:
        path (str): The path where the DataFrame should be saved in ADLS.
        mode (str): The save mode ('overwrite', 'append', 'ignore', or 'error'). Default is 'overwrite'.
        options (Dict[str, Any]): Additional options for saving the Delta table.
        partition_by (Optional[List[str]]): Columns to partition the data by.
        optimize (bool): Whether to run OPTIMIZE command after saving. Default is False.
        zorder_by (Optional[List[str]]): List of columns to ZORDER by.
        optimize_options (Dict[str, Any]): Additional options for the OPTIMIZE command.

    Returns:
        Callable: A function that can be used with DataFrame.transform().

    Example:
        >>> df_transformed = (df
        ...     .transform(some_transformation)
        ...     .transform(save_delta_wrapper(
        ...         "abfss://container@storage.dfs.core.windows.net/path/to/delta",
        ...         mode="overwrite",
        ...         options={"overwriteSchema": "true"},
        ...         partition_by=["date"],
        ...         optimize=True,
        ...         zorder_by=["id", "name"],
        ...         optimize_options={"maxFilesPerTxn": 2}
        ...     ))
        ... )
    """
    def _save_delta(df: DataFrame) -> DataFrame:
        save_delta_format(df, path, mode, options, partition_by, optimize, zorder_by, optimize_options)
        return df
    return _save_delta


--- delta tables  --------

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from typing import Optional, List, Union

def validate_input(database: str, table_name: str, columns: Optional[List[str]] = None) -> None:
    """
    Validate the input parameters for the Delta table extraction.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        columns (Optional[List[str]]): List of column names to extract.

    Raises:
        ValueError: If inputs are invalid.
    """
    if not isinstance(database, str) or not database.strip():
        raise ValueError("Database name must be a non-empty string.")
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Table name must be a non-empty string.")
    if columns is not None:
        if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
            raise ValueError("Columns must be a list of strings.")

def construct_table_path(database: str, table_name: str) -> str:
    """
    Construct the full table path for the Delta table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.

    Returns:
        str: The full table path.
    """
    return f"{database}.{table_name}"

def get_delta_table(database: str, table_name: str, columns: Optional[List[str]] = None) -> Optional[DataFrame]:
    """
    Get a Delta table or specific columns from the Hive metastore in Databricks.

    This function validates the input, constructs the table path,
    and reads the Delta table or specified columns from the Hive metastore
    using the built-in spark session in Databricks.

    Args:
        database (str): The name of the database containing the table.
        table_name (str): The name of the table to extract.
        columns (Optional[List[str]]): List of column names to extract. 
                                       If None, all columns are extracted.

    Returns:
        Optional[DataFrame]: The extracted Delta table (or specified columns) as a DataFrame,
                             or None if an error occurs.

    Example:
        >>> # Get entire table
        >>> df = get_delta_table("my_database", "my_table")
        >>> # Get specific columns
        >>> df = get_delta_table("my_database", "my_table", ["column1", "column2"])
        >>> if df is not None:
        ...     df.show()
    """
    try:
        validate_input(database, table_name, columns)
        full_table_path = construct_table_path(database, table_name)
        
        if columns:
            return spark.table(full_table_path).select(*columns)
        else:
            return spark.table(full_table_path)
    
    except ValueError as e:
        print(f"Input validation error: {e}")
        return None
    except AnalysisException as e:
        print(f"Error reading Delta table: {e}")
        return None

# Example usage
# df = get_delta_table("hive_metastore", "my_table", ["column1", "column2"])
# if df is not None:
#     df.show()


```
