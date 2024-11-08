```python


# Todo: include that empty files are skipped
def _list_excel_files(folder_path: str, recursive: bool, file_extension: str) -> List[str]:
    """
    List all excel files in the specified folder using dbutils.
    
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




def _read_excel_to_spark_df(file_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Read an Excel file from a given path and convert it to a Spark DataFrame.

    Args:
    file_path (str): The path to the Excel file.
    spark (SparkSession): An active Spark session.

    Returns:
    pyspark.sql.DataFrame: A Spark DataFrame containing the Excel data.
    """
    def pd_dfs_from_excel_rdd(rdd_record):
        file_path, file_contents = rdd_record[0], rdd_record[1]
        return file_path, file_contents

    excel_files_rdd = sc.binaryFiles(file_path)
    parsed_excel_sheets = excel_files_rdd.flatMap(pd_dfs_from_excel_rdd)
    
    file_contents = parsed_excel_sheets.collect()[1]
    file_like_obj = io.BytesIO(file_contents)
    
    pandas_df = pd.read_excel(file_like_obj)
    spark_df = spark.createDataFrame(pandas_df)

    if columns:
      spark_df = spark_df.select(*columns)

    file_name = os.path.basename(file_path)
    
    return spark_df.withColumn("source_file", F.lit(file_name))
  
def get_combined_excel_dataframe(
    folder_path: str,
    columns: Optional[List[str]] = None,
    recursive: bool = False,
    file_extension: str = "xlsx",
) -> DataFrame:
    """
    Retrieve and combine xlsx files from a specified folder into a single DataFrame in Databricks.
    
    It retrieves xlsx files, selects specified columns for each file,
    and combines them using unionByName. It is designed to handle large datasets efficiently and scalably.

    Args:
        folder_path (str): The path to the folder containing xlsx files.
        recursive (bool, optional): If True, searches for files recursively in subfolders. Defaults to False.
        file_extension (str, optional): The file extension to filter by. Defaults to "xlsx".
        columns (Optional[List[str]], optional): List of columns to select from each file. If None, all columns are selected.
        **kwargs: Additional keyword arguments to pass to spark.read.csv().

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the combined data from all xlsx files, 
                               with selected columns and an additional 'source_file' column.

    Raises:
        ValueError: If no files with the specified extension are found in the given path.

    Example:
        >>> folder_path = "/mnt/data/csv_files"
        >>> columns = ["id", "name", "value"]
        >>> df = get_combined_csv_dataframe(folder_path, recursive=True, header=True, columns=columns)
        >>> df.show()
    """
    logging.info(f"Reading xlsx files from: {folder_path}")
    
    try:
        excel_files = _list_excel_files(folder_path, recursive, file_extension)
        
        # Read all xlsx files individually, selecting specified columns
        dataframes = [_read_excel_to_spark_df(file, columns) for file in excel_files]
        
        # Combine all DataFrames using unionByName
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        
        return combined_df
    
    except Exception as e:
        logging.error(f"Error in get_combined_excel_dataframe: {str(e)}")
        raise


```


```python

%pip install adlfs openpyxl


import pandas as pd
import io
import os
import logging
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import adlfs

# Adjusted function to list Excel files
def _list_excel_files(folder_path: str, recursive: bool, file_extension: str) -> List[str]:
    """
    List all Excel files in the specified folder using dbutils.
    """
    try:
        all_files = []
        paths = [folder_path]
        while paths:
            current_path = paths.pop()
            files = dbutils.fs.ls(current_path)
            for f in files:
                if f.isDir() and recursive:
                    paths.append(f.path)
                elif f.path.endswith(f'.{file_extension}'):
                    all_files.append(f.path)
        if not all_files:
            raise ValueError(f"No .{file_extension} files found in the specified folder: {folder_path}")
        return all_files
    except Exception as e:
        logging.error(f"Error listing Excel files: {str(e)}")
        raise

# Adjusted function to read Excel files and convert to Spark DataFrames
def _read_excel_to_spark_df(file_path: str, columns: Optional[List[str]] = None) -> Optional[DataFrame]:
    """
    Read an Excel file from a given path and convert it to a Spark DataFrame.
    """
    try:
        # Extract storage account and container information from the file path
        # Example file_path: 'abfss://container@storage_account.dfs.core.windows.net/path/to/file.xlsx'
        path_parts = file_path.replace('abfss://', '').split('@')
        container = path_parts[0]
        storage_account = path_parts[1].split('.dfs.core.windows.net')[0]
        relative_path = '/'.join(path_parts[1].split('.dfs.core.windows.net/')[1:])

        # Create an instance of Azure Datalake FileSystem
        fs = adlfs.AzureBlobFileSystem(account_name=storage_account)

        # Full path within the filesystem
        fs_path = f"{container}/{relative_path}"

        # Open the file using the filesystem
        with fs.open(fs_path, 'rb') as f:
            pandas_df = pd.read_excel(
                f,
                engine='openpyxl',   # Ensure compatibility with .xlsx files
                sheet_name=0,        # Read the first worksheet
                dtype=str            # Read all columns as strings
            )

        if pandas_df.empty:
            return None  # Skip empty workbooks

        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)

        if columns:
            spark_df = spark_df.select(*columns)

        file_name = os.path.basename(file_path)
        return spark_df.withColumn("source_file", F.lit(file_name))
    except Exception as e:
        logging.error(f"Error reading Excel file {file_path}: {str(e)}")
        return None  # Treat exceptions as empty workbooks

# Adjusted main function to combine DataFrames and handle empty workbooks
def get_combined_excel_dataframe(
    folder_path: str,
    columns: Optional[List[str]] = None,
    recursive: bool = False,
    file_extension: str = "xlsx",
) -> Optional[DataFrame]:
    """
    Retrieve and combine Excel files from a specified folder into a single DataFrame in Databricks.
    """
    logging.info(f"Reading Excel files from: {folder_path}")
    empty_workbooks = []
    dataframes = []
    try:
        excel_files = _list_excel_files(folder_path, recursive, file_extension)
        
        # Read all Excel files individually, selecting specified columns
        for file in excel_files:
            df = _read_excel_to_spark_df(file, columns)
            if df is None or df.rdd.isEmpty():
                empty_workbooks.append(file)
                continue
            dataframes.append(df)

        if not dataframes:
            logging.warning("No dataframes to combine.")
            return None
        
        # Combine all DataFrames using unionByName
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        
        if empty_workbooks:
            print("Empty workbooks skipped:")
            for wb in empty_workbooks:
                print(wb)
        
        return combined_df
        
    except Exception as e:
        logging.error(f"Error in get_combined_excel_dataframe: {str(e)}")
        raise


```
