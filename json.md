```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, to_json, expr
import json

def infer_schema_from_json(json_data):
    # ... [Keep the existing infer_schema_from_json function as is] ...

# Create a SparkSession
spark = SparkSession.builder.appName("ParseJSONDictColumn").getOrCreate()

# Sample JSON data (dictionary with multiple objects)
sample_json = {
    "object1": {
        "id": "12345",
        "name": "John Doe",
        "age": 30
    },
    "object2": {
        "address": {
            "street": "123 Main St",
            "city": "New York"
        }
    },
    "object3": {
        "hobbies": ["reading", "swimming"]
    },
    "object4": {
        "scores": [
            {"subject": "math", "score": 95},
            {"subject": "english", "score": 88}
        ]
    }
}

# Infer the schema from the sample data
inferred_schema = StructType([
    StructField(key, infer_schema_from_json(value))
    for key, value in sample_json.items()
])

# Print the inferred schema
print("Inferred Schema:")
print(inferred_schema)

# Create a DataFrame with a JSON column (simulating your existing DataFrame)
df_with_json = spark.createDataFrame([
    (1, json.dumps(sample_json)),
    (2, json.dumps({
        "object1": {"id": "67890", "name": "Jane Smith", "age": 28},
        "object2": {"address": {"street": "456 Elm St", "city": "Los Angeles"}},
        "object3": {"hobbies": ["painting", "yoga"]},
        "object4": {"scores": [{"subject": "history", "score": 92}, {"subject": "science", "score": 94}]}
    }))
], ["id", "json_column"])

# Parse the JSON column
df_parsed = df_with_json.withColumn("parsed_json", from_json(col("json_column"), inferred_schema))

# Create individual columns for each object in the JSON
for field in inferred_schema.fields:
    df_parsed = df_parsed.withColumn(field.name, col(f"parsed_json.{field.name}"))

# Optionally, if you want to flatten nested structures:
df_flattened = df_parsed
for field in inferred_schema.fields:
    if isinstance(field.dataType, StructType):
        for nested_field in field.dataType.fields:
            df_flattened = df_flattened.withColumn(
                f"{field.name}_{nested_field.name}",
                col(f"{field.name}.{nested_field.name}")
            )
    elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
        for nested_field in field.dataType.elementType.fields:
            df_flattened = df_flattened.withColumn(
                f"{field.name}_{nested_field.name}",
                expr(f"transform({field.name}, x -> x.{nested_field.name})")
            )

# Drop the original JSON column and the intermediate parsed_json column
df_result = df_flattened.drop("json_column", "parsed_json")

# Show the result
print("\nParsed Data with Individual Columns:")
df_result.show(truncate=False)

# If you want to see the schema of the resulting DataFrame
print("\nResulting DataFrame Schema:")
df_result.printSchema()


```
