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


```
