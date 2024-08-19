```python


from pyspark.sql.functions import col, explode, expr, from_json
from pyspark.sql.types import StructType, ArrayType

def flatten_json(df, column_name):
    """
    Recursively flatten a nested JSON structure in a PySpark DataFrame.
    
    :param df: PySpark DataFrame
    :param column_name: Name of the column containing the JSON structure
    :return: Flattened DataFrame
    """
    # Get the schema of the JSON column
    schema = df.schema[column_name].dataType

    # Base case: if the column is not a struct or array type, return the DataFrame
    if not isinstance(schema, (StructType, ArrayType)):
        return df

    # Flatten struct types
    if isinstance(schema, StructType):
        expanded = [col(f"{column_name}.{field.name}").alias(f"{column_name}_{field.name}") 
                    for field in schema.fields]
        df = df.select("*", *expanded).drop(column_name)

    # Flatten array types
    elif isinstance(schema, ArrayType):
        df = df.withColumn(column_name, explode(col(column_name)))

    # Get all columns of the DataFrame
    columns = df.columns

    # Recursively flatten any nested structures in the expanded columns
    for column in columns:
        if column.startswith(f"{column_name}_") or column == column_name:
            df = flatten_json(df, column)

    return df

def unpack_json_cell(df, json_column='data'):
    """
    Unpack a JSON string cell into multiple columns in a PySpark DataFrame.
    
    :param df: PySpark DataFrame with a JSON string column
    :param json_column: Name of the column containing the JSON data (default: 'data')
    :return: Flattened DataFrame
    """
    # Infer the schema from the JSON string
    sample_json = df.select(json_column).first()[0]
    schema = expr(f"schema_of_json('{sample_json}')")
    
    # Parse the JSON string and flatten the structure
    parsed_df = df.withColumn("parsed", from_json(col(json_column), schema))
    flattened_df = flatten_json(parsed_df, "parsed")
    
    # Drop the original JSON column and the intermediate parsed column
    flattened_df = flattened_df.drop(json_column, "parsed")
    
    return flattened_df

# Example usage:
# Assuming 'df' is your original DataFrame with a column named 'data' containing the JSON string
# flattened_df = unpack_json_cell(df)
# flattened_df.show()



----
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, expr, from_json
from pyspark.sql.types import StructType, ArrayType
from typing import List

def _flatten_struct(df: DataFrame, column_name: str) -> DataFrame:
    """
    Flatten a struct type column in a PySpark DataFrame.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        column_name (str): Name of the column containing the struct.

    Returns:
        DataFrame: DataFrame with the specified struct column flattened.
    """
    schema = df.schema[column_name].dataType
    expanded = [
        col(f"{column_name}.{field.name}").alias(field.name)
        for field in schema.fields
    ]
    return df.select("*", *expanded).drop(column_name)

def _flatten_array(df: DataFrame, column_name: str) -> DataFrame:
    """
    Flatten an array type column in a PySpark DataFrame.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        column_name (str): Name of the column containing the array.

    Returns:
        DataFrame: DataFrame with the specified array column exploded.
    """
    return df.withColumn(column_name, explode(col(column_name)))

def _flatten_json_recursive(df: DataFrame, column_name: str) -> DataFrame:
    """
    Recursively flatten a nested JSON structure in a PySpark DataFrame.

    This function handles both struct and array types, recursively flattening
    the nested structure until all columns are of primitive types.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        column_name (str): Name of the column containing the nested JSON structure.

    Returns:
        DataFrame: Flattened DataFrame with all nested structures expanded.
    """
    schema = df.schema[column_name].dataType

    if isinstance(schema, StructType):
        df = _flatten_struct(df, column_name)
    elif isinstance(schema, ArrayType):
        df = _flatten_array(df, column_name)
    else:
        return df

    columns_to_check = [col for col in df.columns if col.startswith(f"{column_name}_") or col == column_name]
    
    for column in columns_to_check:
        df = _flatten_json_recursive(df, column)

    return df

def unpack_json_cell(df: DataFrame, json_column: str = 'data') -> DataFrame:
    """
    Unpack a JSON string cell into multiple columns in a PySpark DataFrame.

    This function takes a DataFrame with a JSON string column, infers its schema,
    and flattens the nested structure into separate columns. It handles nested
    objects and arrays recursively.

    Args:
        df (DataFrame): Input PySpark DataFrame with a JSON string column.
        json_column (str, optional): Name of the column containing the JSON data. 
                                     Defaults to 'data'.

    Returns:
        DataFrame: A new DataFrame with the JSON structure flattened into separate columns.

    Example:
        >>> json_df = spark.createDataFrame([('{"name": "Alice", "age": 30}',)], ['data'])
        >>> flattened_df = json_df.transform(unpack_json_cell)
        >>> flattened_df.show()
        +-----+---+
        | name|age|
        +-----+---+
        |Alice| 30|
        +-----+---+
    """
    sample_json = df.select(json_column).first()[0]
    schema = expr(f"schema_of_json('{sample_json}')")
    
    parsed_df = df.withColumn("parsed", from_json(col(json_column), schema))
    flattened_df = _flatten_json_recursive(parsed_df, "parsed")
    
    return flattened_df.drop(json_column, "parsed")

# Example usage:
# df_flattened = df.transform(unpack_json_cell)
# df_flattened.show()


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, expr, from_json
from pyspark.sql.types import StructType, ArrayType
from typing import List
import json

def _flatten_json_recursive(df: DataFrame, column_name: str) -> DataFrame:
    """
    Recursively flatten a nested JSON structure in a PySpark DataFrame.

    This function handles both struct and array types, recursively flattening
    the nested structure until all columns are of primitive types.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        column_name (str): Name of the column containing the nested JSON structure.

    Returns:
        DataFrame: Flattened DataFrame with all nested structures expanded.
    """
    while True:
        schema = df.schema[column_name].dataType
        if isinstance(schema, StructType):
            expanded = [
                col(f"{column_name}.{field.name}").alias(field.name)
                for field in schema.fields
            ]
            df = df.select("*", *expanded).drop(column_name)
        elif isinstance(schema, ArrayType):
            df = df.withColumn(column_name, explode(col(column_name)))
        else:
            return df

        columns_to_check = [col for col in df.columns if col.startswith(f"{column_name}_") or col == column_name]
        if not columns_to_check:
            return df
        column_name = columns_to_check[0]

def unpack_json_cell(df: DataFrame, json_column: str = 'data') -> DataFrame:
    """
    Unpack a JSON string cell into multiple columns in a PySpark DataFrame.

    This function takes a DataFrame with a JSON string column, infers its schema,
    and flattens the nested structure into separate columns. It handles nested
    objects and arrays recursively.

    Args:
        df (DataFrame): Input PySpark DataFrame with a JSON string column.
        json_column (str, optional): Name of the column containing the JSON data. 
                                     Defaults to 'data'.

    Returns:
        DataFrame: A new DataFrame with the JSON structure flattened into separate columns.

    Raises:
        ValueError: If the specified JSON column is not found in the DataFrame.
        json.JSONDecodeError: If the sample JSON is invalid.

    Example:
        >>> json_df = spark.createDataFrame([('{"name": "Alice", "age": 30}',)], ['data'])
        >>> flattened_df = json_df.transform(unpack_json_cell)
        >>> flattened_df.show()
        +-----+---+
        | name|age|
        +-----+---+
        |Alice| 30|
        +-----+---+
    """
    if json_column not in df.columns:
        raise ValueError(f"Column '{json_column}' not found in the DataFrame.")

    try:
        sample_json = df.select(json_column).first()[0]
        json.loads(sample_json)  # Validate JSON
        schema = expr(f"schema_of_json('{sample_json}')")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in column '{json_column}': {str(e)}", e.doc, e.pos)
    
    parsed_df = df.withColumn("parsed", from_json(col(json_column), schema))
    flattened_df = _flatten_json_recursive(parsed_df, "parsed")
    
    return flattened_df.drop(json_column, "parsed")

# Example usage:
# df_flattened = df.transform(unpack_json_cell)
# df_flattened.show()







```
