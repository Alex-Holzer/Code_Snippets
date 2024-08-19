```python

from pyspark.sql.functions import col, explode, expr
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

# Usage example
def unpack_json_cell(df, json_column='json_column'):
    """
    Unpack a JSON cell into multiple columns in a PySpark DataFrame.
    
    :param df: PySpark DataFrame with a JSON column
    :param json_column: Name of the column containing the JSON data (default: 'json_column')
    :return: Flattened DataFrame
    """
    # Ensure the JSON column is properly parsed
    df = df.withColumn(json_column, expr(f"from_json({json_column}, schema_of_json(element_at({json_column}, 1)))"))
    
    # Flatten the JSON structure
    flattened_df = flatten_json(df, json_column)
    
    return flattened_df

# Example usage:
# Assuming 'df' is your original DataFrame with a column named 'json_column' containing the JSON data
# flattened_df = unpack_json_cell(df)
# flattened_df.show()


```
