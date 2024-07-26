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



from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from typing import Union, List

def is_scientific_notation(value):
    """Check if a string is in scientific notation."""
    return F.regexp_extract(value, r'^-?\d+(\.\d+)?[eE][+-]?\d+$', 0) != ''

def convert_scientific_to_standard(value):
    """Convert scientific notation to standard notation."""
    return F.when(
        is_scientific_notation(value),
        F.regexp_replace(
            F.format_number(F.cast(value, DoubleType()), 0),
            r'[,.]',
            ''
        )
    ).otherwise(value)

def handle_non_string_column(df, column):
    """Cast non-string column to string."""
    return df.withColumn(column, F.col(column).cast(StringType()))

def convert_scientific_notation(df, columns: Union[str, List[str]]):
    """
    Convert scientific notation to standard notation in specified column(s).
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
        columns (str or list of str): Name(s) of the column(s) to process
    
    Returns:
        pyspark.sql.DataFrame: DataFrame with converted column(s)
    """
    if isinstance(columns, str):
        columns = [columns]
    
    for column in columns:
        df = (df
            .transform(lambda df: handle_non_string_column(df, column))
            .withColumn(column, convert_scientific_to_standard(F.col(column)))
        )
    
    return df

# Example usage in a transformation pipeline
def process_dataframe(df, columns_to_convert):
    """
    Example of using the convert_scientific_notation function in a transformation pipeline.
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
        columns_to_convert (str or list of str): Column(s) to convert
    
    Returns:
        pyspark.sql.DataFrame: Processed DataFrame
    """
    return (df
        .transform(lambda df: convert_scientific_notation(df, columns_to_convert))
        # Add more transformations as needed
    )

# Test the function
if __name__ == "__main__":
    # Create a sample DataFrame
    data = [
        ("1.23E+05", "9.9923E+13", "not_scientific"),
        ("2.5E-02", "1.0", "3.14E+00"),
        ("normal", "1.23E-10", "6.022E+23")
    ]
    df = spark.createDataFrame(data, ["column1", "column2", "column3"])
    
    # Process the DataFrame
    columns_to_convert = ["column1", "column2", "column3"]
    result_df = process_dataframe(df, columns_to_convert)
    
    # Show the result
    result_df.show(truncate=False)






```
