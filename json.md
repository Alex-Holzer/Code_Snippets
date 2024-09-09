```python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import col, from_json, to_json, schema_of_json

def infer_schema_from_json(json_str):
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
                return ArrayType(StringType())
        elif isinstance(value, dict):
            return infer_schema_from_json(json.dumps(value))
        else:
            return StringType()

    parsed = json.loads(json_str)
    fields = []
    for key, value in parsed.items():
        if isinstance(value, dict):
            fields.append(StructField(key, infer_schema_from_json(json.dumps(value)), True))
        elif isinstance(value, list):
            if value:
                element_type = infer_type(value[0])
                fields.append(StructField(key, ArrayType(element_type), True))
            else:
                fields.append(StructField(key, ArrayType(StringType()), True))
        else:
            fields.append(StructField(key, infer_type(value), True))
    
    return StructType(fields)

# Assume 'spark' is your SparkSession and 'df' is your DataFrame with a 'json_column'

# Get a sample of the JSON data from the column
sample_json = df.select("json_column").limit(1).collect()[0][0]

# Infer the schema from the sample
inferred_schema = infer_schema_from_json(sample_json)

# Print the inferred schema
print("Inferred Schema:")
inferred_schema.printTreeString()

# Apply the inferred schema to parse the JSON column
df_parsed = df.withColumn("parsed_json", from_json(col("json_column"), inferred_schema))

# Select all fields from the parsed JSON
df_result = df_parsed.select("parsed_json.*")

# Show the result
print("\nParsed Data:")
df_result.show(truncate=False)

# If you want to work with specific fields:
# df_result = df_parsed.select(
#     col("parsed_json.id"),
#     col("parsed_json.name"),
#     col("parsed_json.age"),
#     col("parsed_json.address.city").alias("city")
# )
# df_result.show(truncate=False)


```
