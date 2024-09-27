```python

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

def extract_json_to_columns(df: DataFrame, column_names: list) -> DataFrame:
    """
    Extract JSON-like strings from specified columns and create separate columns for each key.

    This function takes a PySpark DataFrame and a list of column names containing JSON-like strings.
    It extracts the keys (val, name, enum) and values from these strings and creates new columns
    for each key, prefixed with the original column name.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        column_names (list): List of column names containing JSON-like strings.

    Returns:
        DataFrame: DataFrame with additional columns extracted from the JSON-like strings.

    Raises:
        ValueError: If any specified column does not exist in the DataFrame.

    Example:
        >>> data = [
        ...     (1, '{"val":"0", "name": "nicht relevant", "enum":"AnrechnungsArt"}', '{"val":"1", "name": "active", "enum":"Status"}'),
        ...     (2, '{"val":"52", "name": "Zurich Dt. Herold Leben AG", "enum":"VertragsGesellschaft"}', '{"val":"2", "name": "inactive", "enum":"Status"}')
        ... ]
        >>> df = spark.createDataFrame(data, ["id", "data1", "data2"])
        >>> result_df = extract_json_to_columns(df, ["data1", "data2"])
        >>> result_df.show(truncate=False)
        +---+-----------------------------+------------------+----------+---------------------------+--------------------+--------+----------+--------+
        |id |data1                        |data2             |data1_val |data1_name                 |data1_enum          |data2_val|data2_name|data2_enum|
        +---+-----------------------------+------------------+----------+---------------------------+--------------------+--------+----------+--------+
        |1  |{"val":"0", "name": "nicht relevant", "enum":"AnrechnungsArt"}|{"val":"1", "name": "active", "enum":"Status"}|0         |nicht relevant             |AnrechnungsArt      |1       |active    |Status   |
        |2  |{"val":"52", "name": "Zurich Dt. Herold Leben AG", "enum":"VertragsGesellschaft"}|{"val":"2", "name": "inactive", "enum":"Status"}|52        |Zurich Dt. Herold Leben AG |VertragsGesellschaft|2       |inactive  |Status   |
        +---+-----------------------------+------------------+----------+---------------------------+--------------------+--------+----------+--------+
    """
    # Check if all specified columns exist in the DataFrame
    for column_name in column_names:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")

    # Define the schema for the JSON structure
    json_schema = StructType([
        StructField("val", StringType(), True),
        StructField("name", StringType(), True),
        StructField("enum", StringType(), True)
    ])

    # Process each specified column
    for column_name in column_names:
        # Clean up the JSON-like string by replacing single quotes with double quotes
        df = df.withColumn(column_name, regexp_replace(col(column_name), "'", '"'))

        # Convert the JSON-like string to a struct
        df = df.withColumn(column_name, from_json(col(column_name), json_schema))

        # Extract each field from the struct and create new columns
        for field in ["val", "name", "enum"]:
            new_column_name = f"{column_name}_{field}"
            df = df.withColumn(new_column_name, col(f"{column_name}.{field}"))

        # Drop the original JSON column
        df = df.drop(column_name)

    return df

```
