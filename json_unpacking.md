```python

from pyspark.sql.functions import schema_of_json, lit
from pyspark.sql.types import StructType

def infer_json_schema(df, json_column):
    """
    Infers the schema of JSON data in a specified column of a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing the JSON column.
        json_column (str): Name of the column containing JSON data.

    Returns:
        pyspark.sql.types.StructType: The inferred JSON schema.

    Raises:
        ValueError: If the specified JSON column doesn't exist in the DataFrame.

    Example:
        >>> df = spark.createDataFrame([("1", '{"name": "John", "age": 30}')], ["id", "json_data"])
        >>> json_schema = infer_json_schema(df, "json_data")
        >>> print(json_schema)
    """
    if json_column not in df.columns:
        raise ValueError(f"Column '{json_column}' not found in the DataFrame.")

    # Get a sample JSON string from the specified column
    sample_json = df.select(json_column).first()[0]

    # Infer the schema from the sample JSON
    return schema_of_json(lit(sample_json))



from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType

def parse_json_column(df, json_column, schema, output_column=None):
    """
    Parses a JSON column in a PySpark DataFrame using a provided schema and adds the result as a new column.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing the JSON column.
        json_column (str): Name of the column containing JSON data.
        schema (pyspark.sql.types.StructType): The schema to use for parsing the JSON data.
        output_column (str, optional): Name of the output column. If None, defaults to f"{json_column}_parsed".

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with an additional column containing the parsed JSON data.

    Raises:
        ValueError: If the specified JSON column doesn't exist in the DataFrame.

    Example:
        >>> df = spark.createDataFrame([("1", '{"name": "John", "age": 30}')], ["id", "json_data"])
        >>> json_schema = infer_json_schema(df, "json_data")  # Assuming this function is available
        >>> parsed_df = parse_json_column(df, "json_data", json_schema)
        >>> parsed_df.show(truncate=False)
    """
    if json_column not in df.columns:
        raise ValueError(f"Column '{json_column}' not found in the DataFrame.")

    if not isinstance(schema, StructType):
        raise ValueError("The provided schema must be a StructType.")

    if output_column is None:
        output_column = f"{json_column}_parsed"

    return df.withColumn(output_column, from_json(col(json_column), schema))


```
