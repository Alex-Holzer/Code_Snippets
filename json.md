```python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, to_json
import json

def infer_schema_from_json(json_data):
    # ... [Keep the existing infer_schema_from_json function as is] ...

# Create a SparkSession
spark = SparkSession.builder.appName("JSONToMultipleColumns").getOrCreate()

# Sample JSON data (used for schema inference)
sample_json = [
    {
        "id": "12345",
        "name": "John Doe",
        "age": 30,
        "large_number": 9223372036854775807,
        "salary": 50000.50,
        "is_employee": True,
        "hobbies": ["reading", "swimming"],
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "country": "USA"
        },
        "scores": [
            {"subject": "math", "score": 95},
            {"subject": "english", "score": 88}
        ]
    }
]

# Infer the schema from the sample data
inferred_schema = infer_schema_from_json(sample_json[0])

# Print the inferred schema
print("Inferred Schema:")
print(inferred_schema)

# Create a new DataFrame with a JSON column (simulating your existing DataFrame)
df_with_json = spark.createDataFrame([
    (1, '{"name": "Alice", "age": 25, "address": {"city": "London", "country": "UK"}, "hobbies": ["dancing", "painting"]}'),
    (2, '{"name": "Bob", "age": 30, "address": {"city": "Paris", "country": "France"}, "hobbies": ["cooking", "traveling"]}')
], ["id", "json_column"])

# Parse the JSON column
df_parsed = df_with_json.withColumn("parsed_json", from_json(col("json_column"), inferred_schema))

# Create individual columns for each field in the JSON
for field in inferred_schema.fields:
    if isinstance(field.dataType, StructType):
        # For nested objects, create a column with the struct
        df_parsed = df_parsed.withColumn(field.name, col(f"parsed_json.{field.name}"))
        
        # If you want to flatten the nested object, uncomment the following lines:
        # for nested_field in field.dataType.fields:
        #     df_parsed = df_parsed.withColumn(f"{field.name}_{nested_field.name}", 
        #                                      col(f"parsed_json.{field.name}.{nested_field.name}"))
    elif isinstance(field.dataType, ArrayType):
        # For arrays, keep them as a single column
        df_parsed = df_parsed.withColumn(field.name, col(f"parsed_json.{field.name}"))
    else:
        # For simple types, create a column directly
        df_parsed = df_parsed.withColumn(field.name, col(f"parsed_json.{field.name}"))

# Drop the original JSON column and the intermediate parsed_json column
df_result = df_parsed.drop("json_column", "parsed_json")

# Show the result
print("\nParsed Data with Individual Columns:")
df_result.show(truncate=False)

# If you want to see the schema of the resulting DataFrame
print("\nResulting DataFrame Schema:")
df_result.printSchema()


```
