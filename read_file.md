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

from pyspark.sql.functions import rand, randn, lit, expr, concat, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
import string
import random

@udf(returnType=StringType())
def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

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
        random_string(lit(9))
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

-------- Broadcast Join
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

def broadcast_join(
    large_df: DataFrame, 
    small_df: DataFrame, 
    on: str | list | callable,
    how: str = "inner"
) -> DataFrame:
    """
    Perform a broadcast join between a large DataFrame and a small DataFrame.

    This function is designed to be used in a Databricks environment where
    the Spark session is already active. It mimics the behavior of the standard
    DataFrame.join() method but automatically broadcasts the smaller DataFrame.

    Args:
        large_df (DataFrame): The larger DataFrame to join.
        small_df (DataFrame): The smaller DataFrame to be broadcasted.
        on (str | list | callable): Columns to join on. Same as the 'on' parameter
                                    in the standard DataFrame.join() method.
        how (str, optional): The type of join to perform. Defaults to "inner".

    Returns:
        DataFrame: The result of the broadcast join.

    Examples:
        >>> large_df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value_large"])
        >>> small_df = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "value_small"])
        >>> result = broadcast_join(large_df, small_df, "id", "left")
        >>> result.show()
        +---+-----------+-----------+
        | id|value_large|value_small|
        +---+-----------+-----------+
        |  1|          A|          X|
        |  2|          B|          Y|
        |  3|          C|       null|
        +---+-----------+-----------+
    """
    if not isinstance(large_df, DataFrame) or not isinstance(small_df, DataFrame):
        raise TypeError("Both large_df and small_df must be PySpark DataFrames")

    if not isinstance(how, str):
        raise TypeError("'how' parameter must be a string")

    # Perform the broadcast join
    return large_df.join(broadcast(small_df), on=on, how=how)

# Example usage
def example_usage():
    # Assuming spark session is already available in Databricks
    large_df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value_large"])
    small_df = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "value_small"])
    
    # Simple join on a single column
    result1 = broadcast_join(large_df, small_df, "id", "left")
    result1.show()
    
    # Join on multiple columns
    large_df2 = large_df.withColumn("category", lambda: "cat1")
    small_df2 = small_df.withColumn("category", lambda: "cat1")
    result2 = broadcast_join(large_df2, small_df2, ["id", "category"], "inner")
    result2.show()
    
    # Join with a complex condition
    result3 = broadcast_join(large_df, small_df, large_df.id == small_df.id, "left")
    result3.show()

# Uncomment the following line to run the example in Databricks
# example_usage()


```
