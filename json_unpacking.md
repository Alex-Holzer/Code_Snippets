```python

from pyspark.sql.functions import schema_of_json, from_json, col, lit
from pyspark.sql.types import StructType
from typing import Tuple, Union

def infer_json_schema(
    df: "pyspark.sql.DataFrame",
    json_column: str
) -> Tuple[StructType, "pyspark.sql.DataFrame"]:
    """
    Infers the schema of JSON data in a specified column of a PySpark DataFrame
    and returns both the inferred schema and a new DataFrame with the JSON data parsed.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing the JSON column.
        json_column (str): Name of the column containing JSON data.

    Returns:
        Tuple[StructType, pyspark.sql.DataFrame]: A tuple containing:
            - The inferred JSON schema as a StructType
            - A new DataFrame with an additional column 'parsed_json' containing the parsed JSON data

    Raises:
        ValueError: If the specified JSON column doesn't exist in the DataFrame.

    Example:
        >>> df = spark.createDataFrame([("1", '{"name": "John", "age": 30}')], ["id", "json_data"])
        >>> json_schema, parsed_df = infer_json_schema(df, "json_data")
        >>> print(json_schema)
        >>> parsed_df.show()
    """
    if json_column not in df.columns:
        raise ValueError(f"Column '{json_column}' not found in the DataFrame.")

    # Get a sample JSON string from the specified column
    sample_json = df.select(json_column).first()[0]

    # Infer the schema from the sample JSON
    json_schema = schema_of_json(lit(sample_json))

    # Parse the JSON data using the inferred schema
    parsed_df = df.withColumn("parsed_json", from_json(col(json_column), json_schema))

    return json_schema, parsed_df

# Helper function to print schema in a more readable format
def print_schema_tree(schema: Union[StructType, str], level: int = 0):
    """
    Prints the schema tree in a more readable format.

    Args:
        schema (Union[StructType, str]): The schema to print, either as a StructType or a string representation.
        level (int): The current indentation level (used for recursive calls).

    Example:
        >>> json_schema, _ = infer_json_schema(df, "json_data")
        >>> print_schema_tree(json_schema)
    """
    if isinstance(schema, str):
        schema = StructType.fromJson(eval(schema))
    
    for field in schema.fields:
        print("  " * level + f"- {field.name} ({field.dataType})")
        if isinstance(field.dataType, StructType):
            print_schema_tree(field.dataType, level + 1)

# Example usage
# json_schema, parsed_df = infer_json_schema(df, "data")
# print_schema_tree(json_schema)
# parsed_df.select("id", "data", "parsed_json.*").show(truncate=False)


```
