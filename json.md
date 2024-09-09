```python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import json

def infer_schema_from_json(json_data):
    def infer_type(value):
        if isinstance(value, bool):
            return StringType()  # Treat booleans as strings to avoid type conflicts
        elif isinstance(value, int):
            return IntegerType()
        elif isinstance(value, float):
            return StringType()  # Treat floats as strings for precise representation
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

# Create a SparkSession
spark = SparkSession.builder.appName("ImprovedSchemaInference").getOrCreate()

# Sample JSON data (you can replace this with your actual sample data)
sample_json = [
    {
        "id": "12345",
        "name": "John Doe",
        "age": 30,
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
    },
    {
        "id": "67890",
        "name": "Jane Smith",
        "age": 28,
        "salary": 55000.75,
        "is_employee": False,
        "hobbies": ["painting", "yoga"],
        "address": {
            "street": "456 Elm St",
            "city": "Los Angeles",
            "country": "USA"
        },
        "scores": [
            {"subject": "math", "score": 92},
            {"subject": "english", "score": 96}
        ]
    }
]

# Infer the schema from the sample data
inferred_schema = infer_schema_from_json(sample_json[0])

# Print the inferred schema
print(inferred_schema)

# Create a DataFrame using the inferred schema
df = spark.createDataFrame(sample_json, schema=inferred_schema)

# Show the DataFrame to verify the schema
df.printSchema()
df.show(truncate=False)

# Example of using the inferred schema to parse a JSON column
from pyspark.sql.functions import from_json, col

# Assuming you have a DataFrame 'df_with_json' with a column 'json_column'
# df_parsed = df_with_json.withColumn("parsed_json", from_json(col("json_column"), inferred_schema))
# df_parsed.select("parsed_json.*").show(truncate=False)


```
