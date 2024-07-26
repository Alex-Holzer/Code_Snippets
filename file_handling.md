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
from typing import Optional, List

def validate_save_params(
    df: DataFrame, 
    path: str, 
    mode: str, 
    metastore_path: Optional[str] = None,
    optimize: bool = False,
    zorder_by: Optional[List[str]] = None
) -> None:
    """
    Validate the input parameters for the save_delta_format function.

    Args:
        df (DataFrame): The PySpark DataFrame to be saved.
        path (str): The path where the DataFrame should be saved in ADLS.
        mode (str): The save mode ('overwrite', 'append', 'ignore', or 'error').
        metastore_path (Optional[str]): The path for persisting in the Hive metastore.
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
    
    if metastore_path is not None and (not isinstance(metastore_path, str) or not metastore_path.strip()):
        raise ValueError("'metastore_path' must be None or a non-empty string.")
    
    if not isinstance(optimize, bool):
        raise ValueError("'optimize' must be a boolean value.")
    
    if zorder_by is not None:
        if not isinstance(zorder_by, list) or not all(isinstance(col, str) for col in zorder_by):
            raise ValueError("'zorder_by' must be None or a list of strings.")
        if not set(zorder_by).issubset(df.columns):
            raise ValueError("All columns in 'zorder_by' must exist in the DataFrame.")


from pyspark.sql import DataFrame
from typing import Optional, List
from delta.tables import DeltaTable

def save_delta_format(
    df: DataFrame, 
    path: str, 
    mode: str, 
    metastore_path: Optional[str] = None,
    optimize: bool = False,
    zorder_by: Optional[List[str]] = None
) -> None:
    """
    Save a PySpark DataFrame in Delta format to Azure Data Lake Storage with optional optimization.

    This function saves the input DataFrame in Delta format to the specified path in Azure Data Lake Storage.
    Optionally, it can also persist the data in the Hive metastore and perform OPTIMIZE and ZORDER operations.

    Args:
        df (DataFrame): The PySpark DataFrame to be saved.
        path (str): The path where the DataFrame should be saved in ADLS.
        mode (str): The save mode ('overwrite', 'append', 'ignore', or 'error').
        metastore_path (Optional[str]): The path for persisting in the Hive metastore.
        optimize (bool): Whether to run OPTIMIZE command after saving.
        zorder_by (Optional[List[str]]): List of columns to ZORDER by.

    Returns:
        None

    Example:
        >>> df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> save_delta_format(df, "abfss://container@storage.dfs.core.windows.net/path/to/delta", 
        ...                   "overwrite", optimize=True, zorder_by=["id"])
    """
    try:
        # Validate input parameters
        validate_save_params(df, path, mode, metastore_path, optimize, zorder_by)

        # Save the DataFrame in Delta format
        (df.write
         .format("delta")
         .mode(mode)
         .save(path))

        print(f"Successfully saved DataFrame in Delta format to: {path}")

        # Persist in Hive metastore if metastore_path is provided
        if metastore_path:
            table_name = metastore_path.split('.')[-1]
            
            # Create or replace the table in the metastore
            df.write.format("delta").mode("overwrite").saveAsTable(metastore_path)
            
            # Ensure the table is pointing to the correct ADLS location
            spark.sql(f"""
            ALTER TABLE {metastore_path}
            SET LOCATION '{path}'
            """)
            
            print(f"Successfully persisted DataFrame in Hive metastore: {metastore_path}")

        # Perform OPTIMIZE and ZORDER if requested
        if optimize:
            delta_table = DeltaTable.forPath(spark, path)
            if zorder_by:
                print(f"Running OPTIMIZE and ZORDER BY {', '.join(zorder_by)}...")
                delta_table.optimize().executeZOrderBy(zorder_by)
            else:
                print("Running OPTIMIZE...")
                delta_table.optimize().executeCompaction()
            print("Optimization completed successfully.")

    except Exception as e:
        print(f"❌ Error saving DataFrame: {str(e)}")
        raise

def save_delta_wrapper(
    path: str, 
    mode: str, 
    metastore_path: Optional[str] = None,
    optimize: bool = False,
    zorder_by: Optional[List[str]] = None
):
    def _save_delta(df: DataFrame) -> DataFrame:
        save_delta_format(df, path, mode, metastore_path, optimize, zorder_by)
        return df
    return _save_delta








```
