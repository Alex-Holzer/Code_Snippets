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

```
