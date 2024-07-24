```python

-------- Execution Measure  -----------------
from functools import wraps
import time
from pyspark.sql import DataFrame

def measure_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"Function '{func.__name__}' executed in {execution_time:.4f} seconds")
        
        if isinstance(result, DataFrame):
            print(f"Resulting DataFrame has {result.count()} rows and {len(result.columns)} columns")
        
        return result
    return wrapper

----------------------------- Data set

from pyspark.sql.functions import rand, randn, lit, expr, concat, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
import string
import random

def generate_large_dataset(num_rows=100_000_000):
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("is_customer", BooleanType(), True),
        StructField("registration_date", TimestampType(), True)
    ] + [StructField(f"feature_{i}", DoubleType(), True) for i in range(1, 56)])  # 55 additional numeric features

    # Generate base DataFrame
    df = spark.range(0, num_rows)

    # Add columns
    df = df.withColumn("name", concat(
        lit(random.choice(string.ascii_uppercase)),
        expr("substring(md5(rand()), 1, 9)")  # Generate random string
    ))
    df = df.withColumn("age", (rand() * 80 + 18).cast(IntegerType()))
    df = df.withColumn("salary", (randn() * 20000 + 50000).cast(DoubleType()))
    df = df.withColumn("is_customer", (rand() > 0.5).cast(BooleanType()))
    df = df.withColumn("registration_date", expr("date_sub(current_date(), cast(rand() * 1000 as int))"))

    # Add 55 random numeric features
    for i in range(1, 56):
        df = df.withColumn(f"feature_{i}", randn())

    return df.select(schema.fieldNames())

# Usage example
large_df = generate_large_dataset()
print(f"Generated DataFrame with {large_df.count()} rows and {len(large_df.columns)} columns")
display(large_df.limit(5))  # Using display() instead of show() in Databricks
large_df.printSchema()
```
