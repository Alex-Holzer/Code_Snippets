```python

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, expr
from pyspark.sql.types import StructType, StructField, StringType

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

    # Sample the data to infer the schema
    sample_data = df.select(column_name).limit(100).collect()
    all_keys = set()
    for row in sample_data:
        try:
            json_data = eval(row[column_name])
            all_keys.update(json_data.keys())
        except:
            continue

    # Create a schema based on the keys found
    schema = StructType([StructField(key, StringType(), True) for key in all_keys])

    # Convert the JSON-like string to a struct
    df = df.withColumn(column_name, from_json(col(column_name), schema))

    # Extract each field from the struct and create new columns
    for field in schema.fields:
        new_column_name = f"{column_name}_{field.name}"
        df = df.withColumn(new_column_name, col(f"{column_name}.{field.name}"))

    # Drop the original JSON column if needed
    # df = df.drop(column_name)

    return df





------


def expand_json_column(df, column_name):
    """
    Expands a JSON string column into separate columns for each key-value pair.
    
    Parameters:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str): The name of the column containing JSON strings.
    
    Returns:
    DataFrame: The DataFrame with additional columns for each key in the JSON strings.
    """
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType
    
    # Get a sample JSON string to infer the schema
    sample_json_row = df.select(col(column_name)) \
                        .filter(col(column_name).isNotNull() & (col(column_name) != '')) \
                        .limit(1) \
                        .collect()
    
    if not sample_json_row:
        # If the column has no valid JSON strings, return the original DataFrame
        return df
    
    sample_json_str = sample_json_row[0][column_name]
    
    # Infer the JSON schema from the sample string
    json_schema = spark.read.json(spark.sparkContext.parallelize([sample_json_str])).schema
    
    # Parse the JSON strings into struct columns using the inferred schema
    df_with_struct = df.withColumn(
        f"{column_name}_struct",
        from_json(col(column_name), json_schema)
    )
    
    # Extract field names from the schema
    struct_fields = json_schema.fieldNames()
    
    # Create new columns for each key in the JSON, prefixed with the original column name
    for field in struct_fields:
        df_with_struct = df_with_struct.withColumn(
            f"{column_name}_{field}",
            col(f"{column_name}_struct.{field}")
        )
    
    # Drop the intermediate struct column
    df_final = df_with_struct.drop(f"{column_name}_struct")
    
    return df_final



```
