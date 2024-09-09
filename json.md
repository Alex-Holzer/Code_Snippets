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
from pyspark.sql.functions import from_json, col, explode, expr

def infer_schema_from_json(json_data):
    def infer_type(value):
        if value is None:
            return StringType()
        elif isinstance(value, bool):
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
                return ArrayType(StringType())
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

def flatten_schema(schema, prefix=""):
    fields = []
    for field in schema.fields:
        name = prefix + field.name
        if isinstance(field.dataType, StructType):
            fields.extend(flatten_schema(field.dataType, name + "_"))
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            fields.append((name, field.dataType))
            fields.extend(flatten_schema(field.dataType.elementType, name + "_"))
        else:
            fields.append((name, field.dataType))
    return fields

# Assume we have the sample_json and df defined
inferred_schema = infer_schema_from_json(sample_json[0])

# Parse the JSON column
df_parsed = df.withColumn("parsed_json", from_json(col("data"), inferred_schema))

# Flatten the schema
flat_schema = flatten_schema(inferred_schema)

# Create individual columns for each field in the JSON
for name, data_type in flat_schema:
    if isinstance(data_type, ArrayType) and isinstance(data_type.elementType, StructType):
        # For arrays of structs, explode the array and create columns for each field
        df_parsed = df_parsed.withColumn(f"{name}_exploded", explode(col(f"parsed_json.{name}")))
        for subfield in data_type.elementType.fields:
            df_parsed = df_parsed.withColumn(f"{name}_{subfield.name}", col(f"{name}_exploded.{subfield.name}"))
        df_parsed = df_parsed.drop(f"{name}_exploded")
    elif isinstance(data_type, ArrayType):
        # For simple arrays, keep them as a single column
        df_parsed = df_parsed.withColumn(name, col(f"parsed_json.{name}"))
    else:
        # For simple types, create a column directly
        df_parsed = df_parsed.withColumn(name, col(f"parsed_json.{name}"))

# Drop the original and intermediate columns
df_result = df_parsed.drop("data", "parsed_json")

# For debugging purposes
df_result.printSchema()
df_result.show(truncate=False)

```
