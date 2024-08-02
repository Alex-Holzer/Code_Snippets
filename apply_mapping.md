```python

from typing import Dict

def database_exists(database_name: str) -> bool:
    """
    Check if a database exists in Databricks.
    
    Args:
        database_name (str): The name of the database to check.
    
    Returns:
        bool: True if the database exists, False otherwise.
    """
    result = spark.sql(f"SHOW DATABASES LIKE '{database_name}'").collect()
    return len(result) > 0

def create_database(
    database_name: str,
    database_directory: str,
    database_properties: Dict[str, str],
    database_comment: str
) -> None:
    """
    Create a new database with the given parameters.
    
    Args:
        database_name (str): The name of the database to create.
        database_directory (str): The DBFS path for the database.
        database_properties (Dict[str, str]): Additional properties for the database.
        database_comment (str): A comment describing the database.
    """
    properties_str = ', '.join([f"'{k}' = '{v}'" for k, v in database_properties.items()])
    create_db_sql = f"""
    CREATE DATABASE IF NOT EXISTS {database_name}
    LOCATION '{database_directory}'
    WITH DBPROPERTIES ({properties_str})
    COMMENT '{database_comment}'
    """
    spark.sql(create_db_sql)

def generate_message(database_name: str, exists: bool) -> str:
    """
    Generate an appropriate message based on whether the database exists.
    
    Args:
        database_name (str): The name of the database.
        exists (bool): Whether the database already exists.
    
    Returns:
        str: A message indicating the result of the operation.
    """
    if exists:
        return f"🚫 Database '{database_name}' already exists. No action taken."
    else:
        return f"✅ Database '{database_name}' has been successfully created!"


from typing import Dict, Optional

def create_database_if_not_exists(
    database_name: str,
    database_directory: str,
    database_properties: Optional[Dict[str, str]] = None,
    database_comment: str = ""
) -> str:
    """
    Create a database if it doesn't exist in Databricks.

    This function checks if a database with the given name exists. If it doesn't,
    it creates the database with the specified parameters. If it already exists,
    it returns a message indicating so.

    Args:
        database_name (str): The name of the database to create.
        database_directory (str): The DBFS path for the database.
        database_properties (Optional[Dict[str, str]]): Additional properties for the database.
            Defaults to None.
        database_comment (str): A comment describing the database. Defaults to an empty string.

    Returns:
        str: A message indicating the result of the operation.

    Example:
        >>> result = create_database_if_not_exists(
        ...     "my_new_database",
        ...     "dbfs:/user/hive/warehouse/my_new_database.db",
        ...     {"creator": "data_team", "purpose": "analytics"},
        ...     "Database for analytics project"
        ... )
        >>> print(result)
    """
    if database_properties is None:
        database_properties = {}

    if database_exists(database_name):
        return generate_message(database_name, True)
    
    create_database(database_name, database_directory, database_properties, database_comment)
    return generate_message(database_name, False)


```

