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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, explode
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
    
    # Parse each JSON string and infer its schema
    schemas = [infer_schema_from_json(json.loads(js)) for js in json_strings]
    
    # Merge all inferred schemas
    final_schema = schemas[0]
    for schema in schemas[1:]:
        final_schema = merge_schemas(final_schema, schema)
    
    return final_schema

def infer_schema_from_json(data):
    """
    Recursively infers the schema from a JSON object or array.
    
    :param data: JSON object, array, or primitive value
    :return: Inferred schema as StructType, ArrayType, or a primitive PySpark type
    """
    if isinstance(data, list):
        if not data:
            return ArrayType(StringType())  # Default to string for empty arrays
        element_schemas = [infer_schema_from_json(elem) for elem in data]
        return ArrayType(merge_schemas_list(element_schemas))
    elif isinstance(data, dict):
        fields = []
        for key, value in data.items():
            fields.append(StructField(key, infer_schema_from_json(value), True))
        return StructType(fields)
    elif isinstance(data, bool):
        return BooleanType()
    elif isinstance(data, int):
        return LongType() if data > 2147483647 or data < -2147483648 else IntegerType()
    elif isinstance(data, float):
        return FloatType()
    else:
        return StringType()

def merge_schemas(schema1, schema2):
    """
    Merges two schemas, combining fields and handling conflicts.
    
    :param schema1: First schema
    :param schema2: Second schema
    :return: Merged schema
    """
    if isinstance(schema1, ArrayType) and isinstance(schema2, ArrayType):
        return ArrayType(merge_schemas(schema1.elementType, schema2.elementType))
    elif not isinstance(schema1, StructType) or not isinstance(schema2, StructType):
        # If either is not a StructType, return the more complex one
        return schema1 if isinstance(schema1, StructType) else schema2
    
    fields1 = {field.name: field for field in schema1.fields}
    fields2 = {field.name: field for field in schema2.fields}
    
    merged_fields = []
    all_keys = set(fields1.keys()) | set(fields2.keys())
    
    for key in all_keys:
        if key in fields1 and key in fields2:
            merged_field = StructField(key, merge_schemas(fields1[key].dataType, fields2[key].dataType), True)
        elif key in fields1:
            merged_field = fields1[key]
        else:
            merged_field = fields2[key]
        merged_fields.append(merged_field)
    
    return StructType(merged_fields)

def merge_schemas_list(schemas):
    """
    Merges a list of schemas.
    
    :param schemas: List of schemas to merge
    :return: Merged schema
    """
    if not schemas:
        return StringType()  # Default to string if no schemas
    merged = schemas[0]
    for schema in schemas[1:]:
        merged = merge_schemas(merged, schema)
    return merged

# Usage
inferred_schema = infer_schema_from_column(df, 'data')

# Parse the JSON column and explode the resulting array
df_parsed = df.withColumn("parsed_json", from_json(col("data"), ArrayType(inferred_schema)))
df_exploded = df_parsed.withColumn("exploded", explode("parsed_json"))

# Create individual columns for each field in the JSON
for field in inferred_schema.fields:
    df_exploded = df_exploded.withColumn(field.name, col(f"exploded.{field.name}"))

# Drop the original JSON column and the intermediate columns
df_result = df_exploded.drop("data", "parsed_json", "exploded")
df_result.display()


# ---- efficiency improvement ------------#

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, explode
import json
from functools import reduce

def infer_schema_from_column(df, column_name, sample_size=100000):
    """
    Infers the schema from a DataFrame column containing JSON data.
    
    :param df: Input DataFrame
    :param column_name: Name of the column containing JSON data
    :param sample_size: Number of rows to sample for schema inference
    :return: Inferred schema as StructType
    """
    # Sample the DataFrame and collect JSON strings
    df_sample = df.select(column_name).limit(sample_size).rdd.flatMap(lambda x: x).collect()
    
    if not df_sample:
        raise ValueError(f"No non-null JSON data found in column '{column_name}'")
    
    # Parse JSON strings in parallel
    parsed_jsons = df.sparkSession.sparkContext.parallelize(df_sample).map(lambda js: json.loads(js) if js else None).filter(lambda x: x is not None).collect()
    
    # Infer schema from parsed JSON data
    schema = infer_schema_from_json(parsed_jsons)
    
    return schema

def infer_schema_from_json(data):
    """
    Infers the schema from JSON data.
    
    :param data: List of JSON objects or a single JSON object
    :return: Inferred schema as StructType
    """
    if not isinstance(data, list):
        data = [data]
    
    fields = {}
    for item in data:
        for key, value in item.items():
            if key not in fields:
                fields[key] = set()
            fields[key].add(infer_type(value))
    
    return StructType([
        StructField(key, merge_types(types), True)
        for key, types in fields.items()
    ])

def infer_type(value):
    """
    Infers the PySpark SQL type from a Python value.
    
    :param value: Python value
    :return: PySpark SQL type
    """
    if isinstance(value, bool):
        return BooleanType()
    elif isinstance(value, int):
        return LongType() if value > 2147483647 or value < -2147483648 else IntegerType()
    elif isinstance(value, float):
        return FloatType()
    elif isinstance(value, list):
        if not value:
            return ArrayType(StringType())
        return ArrayType(merge_types({infer_type(v) for v in value}))
    elif isinstance(value, dict):
        return infer_schema_from_json(value)
    else:
        return StringType()

def merge_types(types):
    """
    Merges multiple PySpark SQL types into a single type.
    
    :param types: Set of PySpark SQL types
    :return: Merged PySpark SQL type
    """
    if len(types) == 1:
        return next(iter(types))
    
    # If we have a mix of Int and Long, use Long
    if IntegerType() in types and LongType() in types:
        types = {t for t in types if t != IntegerType()}
    
    # If we have a mix of numeric types, use the most general
    numeric_types = {IntegerType(), LongType(), FloatType()}
    if any(t in types for t in numeric_types):
        for t in [FloatType(), LongType(), IntegerType()]:
            if t in types:
                types = {t if isinstance(t, type(n)) else n for n in types}
                break
    
    # If we still have multiple types, use StringType
    if len(types) > 1:
        return StringType()
    
    return next(iter(types))

# Usage
inferred_schema = infer_schema_from_column(df, 'data')

# Parse the JSON column using the inferred schema
df_parsed = df.withColumn("parsed_json", from_json(col("data"), inferred_schema))

# Select all fields from the parsed JSON
selected_columns = [col("parsed_json.*")] + [c for c in df.columns if c != "data"]
df_result = df_parsed.select(*selected_columns)

df_result.display()




```
