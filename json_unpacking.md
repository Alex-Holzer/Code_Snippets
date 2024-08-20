```python

from pyspark.sql.functions import schema_of_json, from_json, col, lit

def infer_and_parse_json_column(df, json_column, output_column="parsed_json"):
    """
    Infers the schema of a JSON column and then parses it, adding the result as a new column.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing the JSON column.
        json_column (str): Name of the column containing JSON data.
        output_column (str, optional): Name of the output column. Defaults to "parsed_json".

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with an additional column containing the parsed JSON data.

    Raises:
        ValueError: If the specified JSON column doesn't exist in the DataFrame.

    Example:
        >>> df = spark.createDataFrame([("1", '{"name": "John", "age": 30}')], ["id", "json_data"])
        >>> parsed_df = infer_and_parse_json_column(df, "json_data")
        >>> parsed_df.show(truncate=False)
    """
    if json_column not in df.columns:
        raise ValueError(f"Column '{json_column}' not found in the DataFrame.")

    # Get a sample JSON string from the specified column
    sample_json = df.select(json_column).first()[0]

    # Infer the schema from the sample JSON
    json_schema = schema_of_json(lit(sample_json))

    # Parse the JSON data using the inferred schema
    return df.withColumn(output_column, from_json(col(json_column), json_schema))
```
