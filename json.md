```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, explode, posexplode
import json

def infer_schema_from_column(df, column_name, sample_size=100000):
    """
    Infers the schema from a DataFrame column containing JSON data.
    
    :param df: Input DataFrame
    :param column_name: Name of the column containing JSON data
    :param sample_size: Number of rows to sample for schema inference
    :return: Inferred schema as StructType
    """
    # Sample the DataFrame and collect JSON strings
    df_sample = df.select(column_name).limit(sample_size)
    json_strings = [row[column_name] for row in df_sample.collect() if row[column_name] is not None]
    
    if not json_strings:
        raise ValueError(f"No non-null JSON data found in column '{column_name}'")
    
    # Combine all JSON objects into a single array
    combined_json = "[" + ",".join(json_strings) + "]"
    
    # Create a temporary view of the combined JSON
    temp_df = df.sparkSession.read.json(df.sparkSession.sparkContext.parallelize([combined_json]))
    
    return temp_df.schema

def merge_schemas(schema1, schema2):
    """
    Merges two schemas, combining fields and handling conflicts.
    
    :param schema1: First schema
    :param schema2: Second schema
    :return: Merged schema
    """
    if not isinstance(schema1, StructType) or not isinstance(schema2, StructType):
        # If either schema is not a StructType, return the more complex one
        return schema1 if isinstance(schema1, StructType) else schema2
    
    fields1 = {field.name: field for field in schema1.fields}
    fields2 = {field.name: field for field in schema2.fields}
    
    merged_fields = []
    all_keys = set(fields1.keys()) | set(fields2.keys())
    
    for key in all_keys:
        if key in fields1 and key in fields2:
            # If the field is in both schemas, merge them
            merged_field = StructField(key, merge_schemas(fields1[key].dataType, fields2[key].dataType), True)
            merged_fields.append(merged_field)
        elif key in fields1:
            merged_fields.append(fields1[key])
        else:
            merged_fields.append(fields2[key])
    
    return StructType(merged_fields)

inferred_schema = infer_schema_from_column(df, 'data')



def infer_schema_from_json(json_data):
    def infer_type(value):
        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            if value > 2147483647 or value < -2147483648:
                return LongType()
            return IntegerType()
        elif isinstance(value, float):
            return FloatType()
        elif isinstance(value, list):
            if value:
                return ArrayType(infer_type(value[0]))
            else:
                return ArrayType(StringType())  # Default to string for empty arrays
        elif isinstance(value, dict):
            return infer_schema_from_json(value)
        else:
            return StringType()

    fields = []
    for key, value in json_data.items():
        if isinstance(value, dict):
            fields.append(StructField(key, infer_schema_from_json(value), True))
        elif isinstance(value, list):
            if value and isinstance(value[0], dict):
                # Handle nested array of objects
                element_type = infer_schema_from_json(value[0])
                fields.append(StructField(key, ArrayType(element_type), True))
            elif value:
                element_type = infer_type(value[0])
                fields.append(StructField(key, ArrayType(element_type), True))
            else:
                fields.append(StructField(key, ArrayType(StringType()), True))
        else:
            fields.append(StructField(key, infer_type(value), True))
    
    return StructType(fields)



# Parse the JSON column and explode the resulting array
df_parsed = df.withColumn("parsed_json", from_json(col("data"), ArrayType(inferred_schema)))
df_exploded = df_parsed.withColumn("exploded", explode("parsed_json"))

# Create individual columns for each field in the JSON
for field in inferred_schema.fields:
    if isinstance(field.dataType, StructType):
        # For nested objects, create a column with the struct
        df_exploded = df_exploded.withColumn(field.name, col(f"exploded.{field.name}"))
        
        # If you want to flatten the nested object, uncomment the following lines:
        # for nested_field in field.dataType.fields:
        #     df_exploded = df_exploded.withColumn(f"{field.name}_{nested_field.name}", 
        #                                          col(f"exploded.{field.name}.{nested_field.name}"))
    elif isinstance(field.dataType, ArrayType):
        # For arrays, keep them as a single column
        df_exploded = df_exploded.withColumn(field.name, col(f"exploded.{field.name}"))
    else:
        # For simple types, create a column directly
        df_exploded = df_exploded.withColumn(field.name, col(f"exploded.{field.name}"))

# Drop the original JSON column and the intermediate columns
df_result = df_exploded.drop("json_column", "parsed_json", "exploded")
df_result.display()



-------- New--------

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    BooleanType,
    ArrayType,
)
from pyspark.sql.functions import from_json, col, explode
from typing import Optional, Union, Dict, Any, List

def infer_schema_from_column(
    df: DataFrame,
    column_name: str,
    sample_size: int = 100000
) -> StructType:
    """
    Infer the schema from a DataFrame column containing JSON data.

    This function samples the specified column, parses the JSON data,
    and infers the schema based on the structure of the JSON objects.

    Args:
        df (DataFrame): Input DataFrame containing the JSON column.
        column_name (str): Name of the column containing JSON data.
        sample_size (int, optional): Number of rows to sample for schema inference. Defaults to 100000.

    Returns:
        StructType: Inferred schema as a PySpark StructType.

    Raises:
        ValueError: If no non-null JSON data is found in the specified column.

    Example:
        >>> df = spark.createDataFrame([('{"name": "John", "age": 30}',)], ["json_data"])
        >>> inferred_schema = infer_schema_from_column(df, "json_data")
        >>> print(inferred_schema)
        StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))
    """
    df_sample = df.select(column_name).limit(sample_size)
    json_strings = [
        row[column_name] for row in df_sample.collect() if row[column_name] is not None
    ]

    if not json_strings:
        raise ValueError(f"No non-null JSON data found in column '{column_name}'")

    combined_json = "[" + ",".join(json_strings) + "]"
    temp_df = df.sparkSession.read.json(
        df.sparkSession.sparkContext.parallelize([combined_json])
    )

    return temp_df.schema

def merge_schemas(schema1: StructType, schema2: StructType) -> StructType:
    """
    Merge two PySpark StructType schemas, combining fields and handling conflicts.

    This function recursively merges two schemas, preferring the more complex
    data type in case of conflicts.

    Args:
        schema1 (StructType): First schema to merge.
        schema2 (StructType): Second schema to merge.

    Returns:
        StructType: Merged schema.

    Example:
        >>> schema1 = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        >>> schema2 = StructType([StructField("name", StringType()), StructField("score", FloatType())])
        >>> merged_schema = merge_schemas(schema1, schema2)
        >>> print(merged_schema)
        StructType(List(StructField(name,StringType,true),StructField(age,IntegerType,true),StructField(score,FloatType,true)))
    """
    if not isinstance(schema1, StructType) or not isinstance(schema2, StructType):
        return schema1 if isinstance(schema1, StructType) else schema2

    fields1 = {field.name: field for field in schema1.fields}
    fields2 = {field.name: field for field in schema2.fields}

    merged_fields = []
    all_keys = set(fields1.keys()) | set(fields2.keys())

    for key in all_keys:
        if key in fields1 and key in fields2:
            merged_field = StructField(
                key, merge_schemas(fields1[key].dataType, fields2[key].dataType), True
            )
            merged_fields.append(merged_field)
        elif key in fields1:
            merged_fields.append(fields1[key])
        else:
            merged_fields.append(fields2[key])

    return StructType(merged_fields)

def infer_schema_from_json(json_data: Dict[str, Any]) -> StructType:
    """
    Infer a PySpark StructType schema from a JSON object.

    This function recursively infers the schema of a JSON object,
    handling nested structures and arrays.

    Args:
        json_data (Dict[str, Any]): JSON object to infer schema from.

    Returns:
        StructType: Inferred schema as a PySpark StructType.

    Example:
        >>> json_obj = {"name": "John", "age": 30, "scores": [85.5, 90.0]}
        >>> inferred_schema = infer_schema_from_json(json_obj)
        >>> print(inferred_schema)
        StructType(List(StructField(name,StringType,true),StructField(age,IntegerType,true),StructField(scores,ArrayType(FloatType,true),true)))
    """
    def infer_type(value: Any) -> Union[DataType, StructType]:
        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType() if value > 2147483647 or value < -2147483648 else IntegerType()
        elif isinstance(value, float):
            return FloatType()
        elif isinstance(value, list):
            return ArrayType(infer_type(value[0]) if value else StringType())
        elif isinstance(value, dict):
            return infer_schema_from_json(value)
        else:
            return StringType()

    fields = [
        StructField(key, infer_type(value), True)
        for key, value in json_data.items()
    ]

    return StructType(fields)

def parse_json_column(
    df: DataFrame,
    column_name: str,
    schema: Optional[StructType] = None,
    flatten_nested: bool = False
) -> DataFrame:
    """
    Parse a JSON column in a PySpark DataFrame and expand it into individual columns.

    This function parses a JSON column, infers or uses a provided schema,
    and creates new columns for each field in the JSON structure.

    Args:
        df (DataFrame): Input DataFrame containing the JSON column.
        column_name (str): Name of the column containing JSON data.
        schema (StructType, optional): Schema to use for parsing. If None, it will be inferred.
        flatten_nested (bool, optional): Whether to flatten nested structures. Defaults to False.

    Returns:
        DataFrame: DataFrame with JSON data parsed into individual columns.

    Example:
        >>> df = spark.createDataFrame([('{"name": "John", "age": 30}',)], ["json_data"])
        >>> parsed_df = parse_json_column(df, "json_data")
        >>> parsed_df.show()
        +----+---+
        |name|age|
        +----+---+
        |John| 30|
        +----+---+
    """
    if schema is None:
        schema = infer_schema_from_column(df, column_name)

    df_parsed = df.withColumn(
        "parsed_json", from_json(col(column_name), schema)
    )

    for field in schema.fields:
        if isinstance(field.dataType, StructType) and flatten_nested:
            for nested_field in field.dataType.fields:
                df_parsed = df_parsed.withColumn(
                    f"{field.name}_{nested_field.name}",
                    col(f"parsed_json.{field.name}.{nested_field.name}")
                )
        else:
            df_parsed = df_parsed.withColumn(field.name, col(f"parsed_json.{field.name}"))

    return df_parsed.drop(column_name, "parsed_json")

# Helper function to handle array fields
def _explode_array_field(df: DataFrame, field_name: str) -> DataFrame:
    """
    Explode an array field in a DataFrame into separate rows.

    Args:
        df (DataFrame): Input DataFrame.
        field_name (str): Name of the array field to explode.

    Returns:
        DataFrame: DataFrame with the specified array field exploded.
    """
    return df.withColumn(field_name, explode(field_name))


```
