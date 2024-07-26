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



```
