```python

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, StringType

def extract_json_to_columns(df: DataFrame, column_name: str) -> DataFrame:
    """
    Extract JSON-like strings from a specified column and create separate columns for each key.

    This function takes a PySpark DataFrame and a column name containing JSON-like strings.
    It extracts the keys and values from these strings and creates new columns for each key,
    prefixed with the original column name.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        column_name (str): Name of the column containing JSON-like strings.

    Returns:
        DataFrame: DataFrame with additional columns extracted from the JSON-like strings.

    Raises:
        ValueError: If the specified column does not exist in the DataFrame.

    Example:
        >>> data = [
        ...     (1, '{"val":"0", "name": "nicht relevant", "enum":"AnrechnungsArt"}'),
        ...     (2, '{"val":"52", "name": "Zurich Dt. Herold Leben AG", "enum":"VertragsGesellschaft"}')
        ... ]
        >>> df = spark.createDataFrame(data, ["id", "data"])
        >>> result_df = extract_json_to_columns(df, "data")
        >>> result_df.show(truncate=False)
        +---+------------------------------------------------------------------------------+----------+---------------------------+--------------------+
        |id |data                                                                          |data_val  |data_name                  |data_enum           |
        +---+------------------------------------------------------------------------------+----------+---------------------------+--------------------+
        |1  |{"val":"0", "name": "nicht relevant", "enum":"AnrechnungsArt"}                |0         |nicht relevant             |AnrechnungsArt      |
        |2  |{"val":"52", "name": "Zurich Dt. Herold Leben AG", "enum":"VertragsGesellschaft"}|52       |Zurich Dt. Herold Leben AG |VertragsGesellschaft|
        +---+------------------------------------------------------------------------------+----------+---------------------------+--------------------+
    """
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")

    # Clean up the JSON-like string by replacing single quotes with double quotes
    df = df.withColumn(column_name, regexp_replace(col(column_name), "'", '"'))

    # Infer the schema from the JSON-like string
    sample_json = df.select(column_name).first()[0]
    schema = StructType.fromJson(eval(sample_json))

    # Convert the JSON-like string to a struct
    df = df.withColumn(column_name, from_json(col(column_name), schema))

    # Extract each field from the struct and create new columns
    for field in schema.fields:
        new_column_name = f"{column_name}_{field.name}"
        df = df.withColumn(new_column_name, col(f"{column_name}.{field.name}").cast(StringType()))

    return df
```
