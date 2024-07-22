```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict, Any, Optional
import logging
import os

def list_csv_files(folder_path: str, recursive: bool, file_extension: str) -> List[str]:
    """
    List all CSV files in the specified folder using dbutils.
    
    Args:
        folder_path (str): Path to the folder containing CSV files.
        recursive (bool): Whether to search recursively.
        file_extension (str): File extension to filter by.
    
    Returns:
        List[str]: List of CSV file paths.
    
    Raises:
        ValueError: If no matching files are found.
    """
    try:
        if recursive:
            files = dbutils.fs.ls(folder_path)
            csv_files = [f.path for f in files if f.path.endswith(f'.{file_extension}')]
        else:
            files = dbutils.fs.ls(folder_path)
            csv_files = [f.path for f in files if f.name.endswith(f'.{file_extension}')]
        
        if not csv_files:
            raise ValueError(f"No .{file_extension} files found in the specified folder: {folder_path}")
        
        return csv_files
    except Exception as e:
        logging.error(f"Error listing CSV files: {str(e)}")
        raise

def read_csv_file(file_path: str, options: Dict[str, Any], columns: Optional[List[str]] = None) -> DataFrame:
    """
    Read a single CSV file into a DataFrame, select specified columns, and add the file name as a column.
    
    Args:
        file_path (str): Path to the CSV file.
        options (Dict[str, Any]): Options for reading CSV.
        columns (Optional[List[str]]): List of columns to select. If None, all columns are selected.
    
    Returns:
        DataFrame: The read DataFrame with selected columns and an additional 'source_file' column.
    """
    df = spark.read.options(**options).csv(file_path)
    
    if columns:
        df = df.select(*columns)
    
    file_name = os.path.basename(file_path)
    return df.withColumn("source_file", F.lit(file_name))

def get_combined_csv_dataframe(
    folder_path: str,
    recursive: bool = False,
    file_extension: str = "csv",
    columns: Optional[List[str]] = None,
    **kwargs
) -> DataFrame:
    """
    Retrieve and combine CSV files from a specified folder into a single DataFrame in Databricks.

    This function is optimized for use in Databricks, utilizing dbutils for file listing and the
    pre-existing SparkSession. It retrieves CSV files, selects specified columns for each file,
    and combines them using unionByName. It is designed to handle large datasets efficiently and scalably.

    Args:
        folder_path (str): The path to the folder containing CSV files.
        recursive (bool, optional): If True, searches for files recursively in subfolders. Defaults to False.
        file_extension (str, optional): The file extension to filter by. Defaults to "csv".
        columns (Optional[List[str]], optional): List of columns to select from each file. If None, all columns are selected.
        **kwargs: Additional keyword arguments to pass to spark.read.csv().

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the combined data from all CSV files, 
                               with selected columns and an additional 'source_file' column.

    Raises:
        ValueError: If no files with the specified extension are found in the given path.

    Example:
        >>> folder_path = "/mnt/data/csv_files"
        >>> columns = ["id", "name", "value"]
        >>> df = get_combined_csv_dataframe(folder_path, recursive=True, header=True, columns=columns)
        >>> df.show()
    """
    logging.info(f"Reading CSV files from: {folder_path}")
    
    # Set default options
    options = {
        "sep": ";",
        "header": "true",
        "ignoreLeadingWhiteSpace": "true",
        "ignoreTrailingWhiteSpace": "true",
        "encoding": "UTF-8"
    }
    options.update(kwargs)
    
    try:
        csv_files = list_csv_files(folder_path, recursive, file_extension)
        
        # Read all CSV files individually, selecting specified columns
        dataframes = [read_csv_file(file, options, columns) for file in csv_files]
        
        # Combine all DataFrames using unionByName
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        
        return combined_df
    
    except Exception as e:
        logging.error(f"Error in get_combined_csv_dataframe: {str(e)}")
        raise

# Example usage
# folder_path = "/mnt/data/csv_files"
# columns = ["id", "name", "value"]
# df = get_combined_csv_dataframe(folder_path, recursive=True, header=True, columns=columns)
# df.show()

```
