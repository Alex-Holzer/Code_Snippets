```python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import from_json, col, explode, expr, to_json
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
            if value and isinstance(value[0], dict):
                fields.append(StructField(key, ArrayType(infer_schema_from_json(value[0])), True))
            elif value:
                element_type = infer_type(value[0])
                fields.append(StructField(key, ArrayType(element_type), True))
            else:
                fields.append(StructField(key, ArrayType(StringType()), True))
        else:
            fields.append(StructField(key, infer_type(value), True))
    
    return StructType(fields)

# Create a SparkSession
spark = SparkSession.builder.appName("ParseJSONListWithNestedArrays").getOrCreate()

# Sample JSON data (list of dictionaries with nested array of objects)
sample_json = [
    {
        "id": "12345",
        "name": "John Doe",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "New York"
        },
        "hobbies": ["reading", "swimming"],
        "scores": [
            {"subject": "math", "score": 95},
            {"subject": "english", "score": 88}
        ]
    },
    {
        "id": "67890",
        "name": "Jane Smith",
        "age": 28,
        "address": {
            "street": "456 Elm St",
            "city": "Los Angeles"
        },
        "hobbies": ["painting", "yoga"],
        "scores": [
            {"subject": "history", "score": 92},
            {"subject": "science", "score": 94}
        ]
    }
]

# Infer the schema from the first item in the sample data
inferred_schema = infer_schema_from_json(sample_json[0])

# Print the inferred schema
print("Inferred Schema:")
inferred_schema.printTreeString()

# Create a DataFrame with a JSON column (simulating your existing DataFrame)
df_with_json = spark.createDataFrame([
    (1, json.dumps(sample_json)),
    (2, json.dumps([
        {"id": "11111", "name": "Alice Johnson", "age": 35, "address": {"street": "789 Oak St", "city": "Chicago"}, "hobbies": ["traveling", "cooking"], "scores": [{"subject": "physics", "score": 91}, {"subject": "chemistry", "score": 89}]},
        {"id": "22222", "name": "Bob Williams", "age": 40, "address": {"street": "101 Pine St", "city": "San Francisco"}, "hobbies": ["photography", "hiking"], "scores": [{"subject": "biology", "score": 93}, {"subject": "geography", "score": 90}]}
    ]))
], ["row_id", "json_column"])

# Parse the JSON column and explode the resulting array
df_parsed = df_with_json.withColumn("parsed_json", from_json(col("json_column"), ArrayType(inferred_schema)))
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
        if isinstance(field.dataType.elementType, StructType):
            # For arrays of objects, keep as JSON string
            df_exploded = df_exploded.withColumn(field.name, to_json(col(f"exploded.{field.name}")))
            
            # If you want to extract specific fields from the array of objects:
            # df_exploded = df_exploded.withColumn(
            #     f"{field.name}_extracted",
            #     expr(f"transform({field.name}, x -> struct(x.subject as subject, x.score as score))")
            # )
        else:
            # For simple arrays, keep as is
            df_exploded = df_exploded.withColumn(field.name, col(f"exploded.{field.name}"))
    else:
        # For simple types, create a column directly
        df_exploded = df_exploded.withColumn(field.name, col(f"exploded.{field.name}"))

# Drop the original JSON column and the intermediate columns
df_result = df_exploded.drop("json_column", "parsed_json", "exploded")

# Show the result
print("\nParsed Data with Individual Columns:")
df_result.show(truncate=False)

# If you want to see the schema of the resulting DataFrame
print("\nResulting DataFrame Schema:")
df_result.printSchema()

```
