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
# flattened_df.show()```
