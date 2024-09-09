```python

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, explode, posexplode
import json

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
            if value:
                element_type = infer_type(value[0])
                fields.append(StructField(key, ArrayType(element_type), True))
            else:
                fields.append(StructField(key, ArrayType(StringType()), True))
        else:
            fields.append(StructField(key, infer_type(value), True))
    
    return StructType(fields)


inferred_schema = infer_schema_from_json(sample_json[0])

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


# ----------

from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, ArrayType, StringType

def unpack_json_column(df, json_column, schema=None):
    """
    Unpacks a JSON column in a PySpark DataFrame.
    
    :param df: Input DataFrame
    :param json_column: Name of the column containing JSON data
    :param schema: Optional schema for the JSON data. If None, it will be inferred.
    :return: DataFrame with unpacked JSON columns
    """
    # If schema is not provided, infer it
    if schema is None:
        schema = df.select(json_column).schema[0].dataType
    
    # Check if the column contains an array of JSON objects
    if isinstance(schema, ArrayType):
        df = df.withColumn(json_column, explode(col(json_column)))
        schema = schema.elementType
    
    # Parse the JSON column
    parsed = df.withColumn("parsed", from_json(col(json_column), schema))
    
    # Flatten the structure
    for field in schema:
        if isinstance(field.dataType, StructType):
            for nested_field in field.dataType:
                parsed = parsed.withColumn(f"{field.name}_{nested_field.name}", 
                                           col(f"parsed.{field.name}.{nested_field.name}"))
        elif isinstance(field.dataType, ArrayType):
            parsed = parsed.withColumn(field.name, col(f"parsed.{field.name}"))
        else:
            parsed = parsed.withColumn(field.name, col(f"parsed.{field.name}"))
    
    # Drop the original JSON column and the intermediate parsed column
    return parsed.drop(json_column, "parsed")

# Example usage:
# Assuming 'df' is your DataFrame and 'json_column' is the name of your JSON column
# result_df = unpack_json_column(df, 'json_column')

# To see the result:
# result_df.printSchema()
# result_df.show(truncate=False)

```
