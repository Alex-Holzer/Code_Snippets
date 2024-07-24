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
from typing import Union, List, Callable

def broadcast_join(
    large_df: DataFrame, 
    small_df: DataFrame, 
    on: Union[str, List[str], Callable],
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
        on (Union[str, List[str], Callable]): Columns to join on. Same as the 'on' 
                                              parameter in the standard DataFrame.join() method.
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
    from pyspark.sql import functions as F
    result3 = broadcast_join(large_df, small_df, F.col("large_df.id") == F.col("small_df.id"), "left")
    result3.show()

# Uncomment the following line to run the example in Databricks
# example_usage()


--- replace string----

from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, lower, upper, col
from typing import Union, List

def replace_string_in_columns(
    df: DataFrame,
    columns: Union[str, List[str]],
    find_string: str,
    replace_string: str,
    case_sensitive: bool = True
) -> DataFrame:
    """
    Perform a configurable case-sensitive or case-insensitive find-and-replace operation on specified columns of a DataFrame.

    This function replaces all occurrences of 'find_string' with 'replace_string' in the specified
    column(s) of the input DataFrame. The operation can be case-sensitive or case-insensitive.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        columns (Union[str, List[str]]): A single column name or a list of column names to perform the replacement on.
        find_string (str): The string to find.
        replace_string (str): The string to replace with.
        case_sensitive (bool, optional): If True, the replacement is case-sensitive. If False, it's case-insensitive. 
                                         Defaults to True.

    Returns:
        DataFrame: A new DataFrame with the specified replacements applied.

    Examples:
        >>> df = spark.createDataFrame([("Billy", 25), ("Sally", 30)], ["Name", "Age"])
        >>> result = replace_string_in_columns(df, "Name", "l", "n", case_sensitive=True)
        >>> result.show()
        +-----+---+
        | Name|Age|
        +-----+---+
        |Binny| 25|
        |Sally| 30|
        +-----+---+

        >>> result = replace_string_in_columns(df, "Name", "L", "n", case_sensitive=False)
        >>> result.show()
        +-----+---+
        | Name|Age|
        +-----+---+
        |Binny| 25|
        |Sanny| 30|
        +-----+---+
    """
    if not isinstance(df, DataFrame):
        raise TypeError("Input 'df' must be a PySpark DataFrame")
    
    if isinstance(columns, str):
        columns = [columns]
    elif not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise TypeError("'columns' must be a string or a list of strings")
    
    if not isinstance(find_string, str) or not isinstance(replace_string, str):
        raise TypeError("'find_string' and 'replace_string' must be strings")
    
    if not isinstance(case_sensitive, bool):
        raise TypeError("'case_sensitive' must be a boolean")
    
    # Validate that all specified columns exist in the DataFrame
    missing_columns = set(columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Columns {missing_columns} not found in the DataFrame")
    
    # Escape special regex characters in find_string
    escaped_find_string = find_string.replace('\\', '\\\\').replace('.', '\\.').replace('*', '\\*')
    escaped_find_string = escaped_find_string.replace('+', '\\+').replace('?', '\\?').replace('|', '\\|')
    escaped_find_string = escaped_find_string.replace('{', '\\{').replace('}', '\\}').replace('(', '\\(')
    escaped_find_string = escaped_find_string.replace(')', '\\)').replace('[', '\\[').replace(']', '\\]')
    escaped_find_string = escaped_find_string.replace('^', '\\^').replace('$', '\\$')
    
    # Apply the replacement to each specified column
    for column in columns:
        if case_sensitive:
            df = df.withColumn(column, regexp_replace(col(column), escaped_find_string, replace_string))
        else:
            # For case-insensitive, we use a regex that matches both upper and lower case
            case_insensitive_regex = ''.join(f'[{c.lower()}{c.upper()}]' for c in escaped_find_string)
            df = df.withColumn(column, regexp_replace(col(column), case_insensitive_regex, replace_string))
    
    return df

# Example usage
def example_usage():
    # Assuming spark session is already available in Databricks
    df = spark.createDataFrame([("Billy", 25), ("Sally", 30)], ["Name", "Age"])
    
    # Case-sensitive replacement: Replace 'l' with 'n' in the 'Name' column
    result1 = replace_string_in_columns(df, "Name", "l", "n", case_sensitive=True)
    print("Example 1: Case-sensitive replacement of 'l' with 'n' in 'Name' column")
    result1.show()
    
    # Case-insensitive replacement: Replace 'L' with 'n' in the 'Name' column
    result2 = replace_string_in_columns(df, "Name", "L", "n", case_sensitive=False)
    print("Example 2: Case-insensitive replacement of 'L' with 'n' in 'Name' column")
    result2.show()
    
    # Multiple columns: Replace 'a' with 'X' in multiple columns (case-sensitive)
    df2 = spark.createDataFrame([("Hello World", "Test"), ("Hello Earth", "Example")], ["Greeting", "Type"])
    result3 = replace_string_in_columns(df2, ["Greeting", "Type"], "e", "X", case_sensitive=True)
    print("Example 3: Case-sensitive replacement of 'e' with 'X' in 'Greeting' and 'Type' columns")
    result3.show()

# Uncomment the following line to run the example in Databricks
# example_usage()

------- Improved ---

from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col
from typing import Union, List
import re

def replace_string_in_columns(
    df: DataFrame,
    columns: Union[str, List[str]],
    find_string: str,
    replace_string: str,
    case_sensitive: bool = True
) -> DataFrame:
    """
    Perform a configurable string replacement operation on specified columns of a DataFrame,
    supporting wildcard patterns with '%'.

    This function replaces all occurrences of 'find_string' with 'replace_string' in the specified
    column(s) of the input DataFrame. The operation can be case-sensitive or case-insensitive and
    supports wildcard patterns using '%' similar to SQL's LIKE operator.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        columns (Union[str, List[str]]): A single column name or a list of column names to perform the replacement on.
        find_string (str): The string pattern to find. Use '%' as a wildcard for any number of characters.
        replace_string (str): The string to replace with.
        case_sensitive (bool, optional): If True, the replacement is case-sensitive. If False, it's case-insensitive. 
                                         Defaults to True.

    Returns:
        DataFrame: A new DataFrame with the specified replacements applied.

    Examples:
        >>> df = spark.createDataFrame([("Binny", 25), ("Sally", 30)], ["Name", "Age"])
        >>> result = replace_string_in_columns(df, "Name", "%in%", "urn", case_sensitive=True)
        >>> result.show()
        +-----+---+
        | Name|Age|
        +-----+---+
        |Burny| 25|
        |Sally| 30|
        +-----+---+

        >>> result = replace_string_in_columns(df, "Name", "%AL%", "el", case_sensitive=False)
        >>> result.show()
        +-----+---+
        | Name|Age|
        +-----+---+
        |Binny| 25|
        |Selly| 30|
        +-----+---+
    """
    if not isinstance(df, DataFrame):
        raise TypeError("Input 'df' must be a PySpark DataFrame")
    
    if isinstance(columns, str):
        columns = [columns]
    elif not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise TypeError("'columns' must be a string or a list of strings")
    
    if not isinstance(find_string, str) or not isinstance(replace_string, str):
        raise TypeError("'find_string' and 'replace_string' must be strings")
    
    if not isinstance(case_sensitive, bool):
        raise TypeError("'case_sensitive' must be a boolean")
    
    # Validate that all specified columns exist in the DataFrame
    missing_columns = set(columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Columns {missing_columns} not found in the DataFrame")
    
    def escape_regex_chars(pattern: str) -> str:
        """Escape special regex characters except '%'."""
        special_chars = r'\.^$*+?{}[]|()'
        return ''.join([f'\\{c}' if c in special_chars else c for c in pattern])
    
    def wildcard_to_regex(pattern: str) -> str:
        """Convert SQL LIKE wildcard pattern to regex pattern."""
        escaped_pattern = escape_regex_chars(pattern)
        return escaped_pattern.replace('%', '.*')
    
    regex_pattern = wildcard_to_regex(find_string)
    
    # Apply the replacement to each specified column
    for column in columns:
        if case_sensitive:
            df = df.withColumn(column, regexp_replace(col(column), regex_pattern, replace_string))
        else:
            # For case-insensitive, we use the (?i) flag in the regex
            df = df.withColumn(column, regexp_replace(col(column), f'(?i){regex_pattern}', replace_string))
    
    return df

# Example usage
def example_usage():
    # Assuming spark session is already available in Databricks
    df = spark.createDataFrame([("Binny", 25), ("Sally", 30), ("Billy", 35)], ["Name", "Age"])
    
    # Case-sensitive replacement with wildcard: Replace 'in' (with any characters around) with 'urn' in the 'Name' column
    result1 = replace_string_in_columns(df, "Name", "%in%", "urn", case_sensitive=True)
    print("Example 1: Case-sensitive replacement of '%in%' with 'urn' in 'Name' column")
    result1.show()
    
    # Case-insensitive replacement with wildcard: Replace 'al' (with any characters around) with 'el' in the 'Name' column
    result2 = replace_string_in_columns(df, "Name", "%AL%", "el", case_sensitive=False)
    print("Example 2: Case-insensitive replacement of '%AL%' with 'el' in 'Name' column")
    result2.show()
    
    # Multiple columns with wildcard: Replace 'i' (with any characters around) with 'X' in multiple columns (case-sensitive)
    df2 = spark.createDataFrame([("Hello World", "Test"), ("Hi Earth", "Example")], ["Greeting", "Type"])
    result3 = replace_string_in_columns(df2, ["Greeting", "Type"], "%i%", "X", case_sensitive=True)
    print("Example 3: Case-sensitive replacement of '%i%' with 'X' in 'Greeting' and 'Type' columns")
    result3.show()

# Uncomment the following line to run the example in Databricks
# example_usage()

--------- improved add_mapped_column------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Column
from typing import Dict, Any, Union

def create_mapping_conditions(mapping_dict: Dict[str, Any], source_column: str):
    """
    Create a list of conditions based on the mapping dictionary.

    This helper function generates conditions for each key in the mapping dictionary,
    supporting both exact matches and wildcard patterns.

    Args:
        mapping_dict (Dict[str, Any]): A dictionary mapping source values (potentially with wildcards) to target values.
        source_column (str): The name of the column to map from.

    Returns:
        list: A list of tuples, each containing a condition and its corresponding value.
    """
    return [
        (F.col(source_column).like(key) if '%' in key else F.col(source_column) == key, F.lit(value))
        for key, value in mapping_dict.items()
    ]

def add_mapped_column(
    df: DataFrame,
    source_column: str,
    target_column: str,
    mapping_dict: Dict[str, Any],
    otherwise: Union[Any, Column]
) -> DataFrame:
    """
    Add a new column to the DataFrame based on a mapping dictionary with wildcard support.

    This function creates a new column in the DataFrame by mapping values
    from a source column using a provided dictionary. It supports wildcard
    matching using '%' similar to SQL LIKE operations. If a value is not
    found in the mapping dictionary, it uses the provided 'otherwise' value,
    which can be a constant or another column.

    Args:
        df (DataFrame): The input DataFrame.
        source_column (str): The name of the column to map from.
        target_column (str): The name of the new column to create.
        mapping_dict (Dict[str, Any]): A dictionary mapping source values (potentially with wildcards) to target values.
        otherwise (Union[Any, Column]): The value or column to use when the source value doesn't match any mapping.

    Returns:
        DataFrame: The original DataFrame with the new mapped column added.

    Example:
        >>> df = spark.createDataFrame([("Apple", "Fruit"), ("Banana", "Fruit"), ("Carrot", "Vegetable")], ["food", "type"])
        >>> mapping = {"%pp%": "Contains pp", "Ban%": "Starts with Ban"}
        >>> result = add_mapped_column(df, "food", "category", mapping, F.col("type"))
        >>> result.show()
        +------+---------+----------------+
        |  food|     type|        category|
        +------+---------+----------------+
        | Apple|    Fruit|    Contains pp |
        |Banana|    Fruit|Starts with Ban |
        |Carrot|Vegetable|     Vegetable  |
        +------+---------+----------------+
    """
    conditions = create_mapping_conditions(mapping_dict, source_column)
    
    mapping_expr = conditions[0][1]
    for condition, value in conditions[1:]:
        mapping_expr = F.when(condition, value).otherwise(mapping_expr)
    
    # Handle the case where 'otherwise' is a column
    if isinstance(otherwise, Column):
        final_expr = F.when(F.expr(' OR '.join([c[0].cast('string') for c in conditions])), mapping_expr).otherwise(otherwise)
    else:
        final_expr = F.when(F.expr(' OR '.join([c[0].cast('string') for c in conditions])), mapping_expr).otherwise(F.lit(otherwise))
    
    return df.withColumn(target_column, final_expr)




from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("TestAddMappedColumn").getOrCreate()

# Import the function (assuming it's in a module named 'transformations')
from transformations import add_mapped_column

# Create a sample DataFrame
data = [
    ("Apple", "Fruit", 1),
    ("Banana", "Fruit", 2),
    ("Carrot", "Vegetable", 3),
    ("Date", "Fruit", 4),
    ("Eggplant", "Vegetable", 5)
]
df = spark.createDataFrame(data, ["food", "type", "id"])

# Test 1: Basic exact match mapping with constant otherwise
def test_exact_match():
    mapping = {"Apple": "Red fruit", "Banana": "Yellow fruit"}
    result = add_mapped_column(df, "food", "category", mapping, "Other")
    result.show()
    assert result.filter(F.col("food") == "Apple").select("category").first()[0] == "Red fruit"
    assert result.filter(F.col("food") == "Carrot").select("category").first()[0] == "Other"

# Test 2: Wildcard mapping with constant otherwise
def test_wildcard_match():
    mapping = {"%pp%": "Contains pp", "Ban%": "Starts with Ban", "%t": "Ends with t"}
    result = add_mapped_column(df, "food", "category", mapping, "No match")
    result.show()
    assert result.filter(F.col("food") == "Apple").select("category").first()[0] == "Contains pp"
    assert result.filter(F.col("food") == "Carrot").select("category").first()[0] == "Ends with t"
    assert result.filter(F.col("food") == "Date").select("category").first()[0] == "No match"

# Test 3: Using another column as otherwise
def test_column_otherwise():
    mapping = {"Apple": "Special fruit", "Carrot": "Orange vegetable"}
    result = add_mapped_column(df, "food", "category", mapping, F.col("type"))
    result.show()
    assert result.filter(F.col("food") == "Apple").select("category").first()[0] == "Special fruit"
    assert result.filter(F.col("food") == "Banana").select("category").first()[0] == "Fruit"

# Test 4: Combining exact and wildcard matches
def test_combined_matching():
    mapping = {"Apple": "Exact match", "%ana": "Ends with ana", "C%": "Starts with C"}
    result = add_mapped_column(df, "food", "category", mapping, "Default")
    result.show()
    assert result.filter(F.col("food") == "Apple").select("category").first()[0] == "Exact match"
    assert result.filter(F.col("food") == "Banana").select("category").first()[0] == "Ends with ana"
    assert result.filter(F.col("food") == "Carrot").select("category").first()[0] == "Starts with C"
    assert result.filter(F.col("food") == "Date").select("category").first()[0] == "Default"

# Test 5: Using the function in a transformation pipeline
def test_in_pipeline():
    def transform_pipeline(df):
        mapping1 = {"%pp%": "Contains pp", "Ban%": "Starts with Ban"}
        mapping2 = {"1": "One", "2": "Two"}
        return (
            df
            .transform(lambda df: add_mapped_column(df, "food", "category1", mapping1, F.col("type")))
            .transform(lambda df: add_mapped_column(df, "id", "category2", mapping2, "Many"))
        )
    
    result = transform_pipeline(df)
    result.show()
    assert result.filter(F.col("food") == "Apple").select("category1").first()[0] == "Contains pp"
    assert result.filter(F.col("food") == "Banana").select("category1").first()[0] == "Starts with Ban"
    assert result.filter(F.col("food") == "Carrot").select("category1").first()[0] == "Vegetable"
    assert result.filter(F.col("id") == 1).select("category2").first()[0] == "One"
    assert result.filter(F.col("id") == 5).select("category2").first()[0] == "Many"

# Run all tests
if __name__ == "__main__":
    test_exact_match()
    test_wildcard_match()
    test_column_otherwise()
    test_combined_matching()
    test_in_pipeline()
    
    print("All tests completed successfully!")

# Clean up
spark.stop()


```
