```python

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType, StringType

def process_data(df, multiplier, columns):
    # Validate DataFrame
    assert isinstance(df, DataFrame), "df must be a PySpark DataFrame"
    
    # Validate multiplier
    assert isinstance(multiplier, (int, float)), "multiplier must be int or float"
    
    # Validate columns
    assert all(isinstance(c, str) for c in columns), "columns must be a list of strings"
    assert all(c in df.columns for c in columns), "all columns must exist in the DataFrame"
    
    # Validate column types
    for c in columns:
        assert isinstance(df.schema[c].dataType, (IntegerType, FloatType)), f"Column {c} must be numeric"
    
    # Function implementation
    return df.select(*[col(c) * multiplier for c in columns])


from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, DateType, TimestampType
from typing import List, Union, Tuple

def validate_dataframe(df: DataFrame) -> None:
    """Validate that the input is a PySpark DataFrame."""
    assert isinstance(df, DataFrame), "Input must be a PySpark DataFrame"

def validate_columns_exist(df: DataFrame, columns: List[str]) -> None:
    """Validate that all specified columns exist in the DataFrame."""
    missing_columns = set(columns) - set(df.columns)
    assert not missing_columns, f"Columns {missing_columns} do not exist in the DataFrame"

def validate_column_types(df: DataFrame, column_types: List[Tuple[str, Union[type, Tuple[type, ...]]]) -> None:
    """Validate that columns have the expected data types."""
    schema = df.schema
    type_mapping = {
        int: IntegerType,
        float: FloatType,
        str: StringType,
        bool: BooleanType
    }
    
    for column, expected_type in column_types:
        assert column in schema, f"Column {column} does not exist in the DataFrame"
        col_type = schema[column].dataType
        expected_spark_types = type_mapping.get(expected_type, expected_type)
        if not isinstance(expected_spark_types, tuple):
            expected_spark_types = (expected_spark_types,)
        assert any(isinstance(col_type, t) for t in expected_spark_types), \
            f"Column {column} has type {col_type}, expected {expected_type}"

def validate_non_null(df: DataFrame, columns: List[str]) -> None:
    """Validate that specified columns do not contain null values."""
    for column in columns:
        null_count = df.filter(df[column].isNull()).count()
        assert null_count == 0, f"Column {column} contains {null_count} null values"

def validate_numeric_range(df: DataFrame, column: str, min_value: float, max_value: float) -> None:
    """Validate that numeric values in a column fall within a specified range."""
    out_of_range = df.filter((df[column] < min_value) | (df[column] > max_value)).count()
    assert out_of_range == 0, f"{out_of_range} values in column {column} are outside the range [{min_value}, {max_value}]"

def validate_string_length(df: DataFrame, column: str, min_length: int, max_length: int) -> None:
    """Validate that string values in a column have a length within the specified range."""
    invalid_length = df.filter((df.length(column) < min_length) | (df.length(column) > max_length)).count()
    assert invalid_length == 0, f"{invalid_length} values in column {column} have length outside the range [{min_length}, {max_length}]"

def validate_unique(df: DataFrame, columns: List[str]) -> None:
    """Validate that specified columns contain only unique values."""
    for column in columns:
        distinct_count = df.select(column).distinct().count()
        total_count = df.count()
        assert distinct_count == total_count, f"Column {column} contains non-unique values"

# Example usage function
def validate_employee_data(df: DataFrame) -> None:
    validate_dataframe(df)
    validate_columns_exist(df, ['employee_id', 'name', 'age', 'salary', 'department'])
    validate_column_types(df, [
        ('employee_id', str),
        ('name', str),
        ('age', int),
        ('salary', (int, float)),
        ('department', str)
    ])
    validate_non_null(df, ['employee_id', 'name', 'department'])
    validate_numeric_range(df, 'age', 18, 65)
    validate_numeric_range(df, 'salary', 0, 1000000)
    validate_string_length(df, 'name', 2, 100)
    validate_unique(df, ['employee_id'])

# Usage example:
# validate_employee_data(employee_df)


from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.rdd import RDD
from typing import Dict, List, Union, Any

def validate_dictionary(d: Dict[Any, Any], required_keys: List[str] = None, value_types: Dict[str, type] = None) -> None:
    """
    Validate a dictionary.
    
    :param d: Dictionary to validate
    :param required_keys: List of keys that must be present in the dictionary
    :param value_types: Dictionary specifying the expected type for each key's value
    """
    assert isinstance(d, dict), "Input must be a dictionary"
    
    if required_keys:
        missing_keys = set(required_keys) - set(d.keys())
        assert not missing_keys, f"Missing required keys: {missing_keys}"
    
    if value_types:
        for key, expected_type in value_types.items():
            if key in d:
                assert isinstance(d[key], expected_type), f"Value for key '{key}' must be of type {expected_type}"

def validate_list(lst: List[Any], expected_type: type = None, min_length: int = None, max_length: int = None) -> None:
    """
    Validate a list.
    
    :param lst: List to validate
    :param expected_type: Expected type of list elements
    :param min_length: Minimum allowed length of the list
    :param max_length: Maximum allowed length of the list
    """
    assert isinstance(lst, list), "Input must be a list"
    
    if expected_type:
        assert all(isinstance(item, expected_type) for item in lst), f"All items must be of type {expected_type}"
    
    if min_length is not None:
        assert len(lst) >= min_length, f"List length must be at least {min_length}"
    
    if max_length is not None:
        assert len(lst) <= max_length, f"List length must not exceed {max_length}"

def validate_number(value: Union[str, float, int], min_value: float = None, max_value: float = None) -> None:
    """
    Validate a number (string, float, or int).
    
    :param value: Value to validate
    :param min_value: Minimum allowed value
    :param max_value: Maximum allowed value
    """
    if isinstance(value, str):
        assert value.replace('.', '').isdigit(), "String must represent a valid number"
        value = float(value)
    
    assert isinstance(value, (int, float)), "Value must be a number (int or float)"
    
    if min_value is not None:
        assert value >= min_value, f"Value must be at least {min_value}"
    
    if max_value is not None:
        assert value <= max_value, f"Value must not exceed {max_value}"

def validate_string(s: str, min_length: int = None, max_length: int = None, allowed_chars: str = None) -> None:
    """
    Validate a string.
    
    :param s: String to validate
    :param min_length: Minimum allowed length of the string
    :param max_length: Maximum allowed length of the string
    :param allowed_chars: String containing all allowed characters
    """
    assert isinstance(s, str), "Input must be a string"
    
    if min_length is not None:
        assert len(s) >= min_length, f"String length must be at least {min_length}"
    
    if max_length is not None:
        assert len(s) <= max_length, f"String length must not exceed {max_length}"
    
    if allowed_chars:
        assert all(char in allowed_chars for char in s), f"String can only contain characters from: {allowed_chars}"

def validate_column_names(df: DataFrame, required_columns: List[str], optional_columns: List[str] = None) -> None:
    """
    Validate column names in a DataFrame.
    
    :param df: DataFrame to validate
    :param required_columns: List of columns that must be present
    :param optional_columns: List of columns that may be present
    """
    assert isinstance(df, DataFrame), "Input must be a PySpark DataFrame"
    
    missing_columns = set(required_columns) - set(df.columns)
    assert not missing_columns, f"Missing required columns: {missing_columns}"
    
    if optional_columns:
        all_allowed_columns = set(required_columns + optional_columns)
        extra_columns = set(df.columns) - all_allowed_columns
        assert not extra_columns, f"Unexpected columns found: {extra_columns}"

def validate_rdd_is_empty(rdd: RDD) -> None:
    """
    Validate that an RDD is empty.
    
    :param rdd: RDD to validate
    """
    assert isinstance(rdd, RDD), "Input must be a PySpark RDD"
    assert rdd.isEmpty(), "RDD is not empty"

# Example usage function
def validate_customer_data(df: DataFrame, config: Dict[str, Any]) -> None:
    validate_dictionary(config, required_keys=['min_age', 'max_age', 'allowed_statuses'])
    validate_column_names(df, 
                          required_columns=['customer_id', 'name', 'age', 'status'],
                          optional_columns=['email', 'phone'])
    
    validate_number(config['min_age'], min_value=0)
    validate_number(config['max_age'], max_value=150)
    validate_list(config['allowed_statuses'], expected_type=str)
    
    # Validate age column
    age_out_of_range = df.filter((df['age'] < config['min_age']) | (df['age'] > config['max_age'])).count()
    assert age_out_of_range == 0, f"{age_out_of_range} customers have age out of range [{config['min_age']}, {config['max_age']}]"
    
    # Validate status column
    invalid_status = df.filter(~df['status'].isin(config['allowed_statuses'])).count()
    assert invalid_status == 0, f"{invalid_status} customers have invalid status"

# Usage example:
# config = {
#     'min_age': 18,
#     'max_age': 100,
#     'allowed_statuses': ['active', 'inactive', 'suspended']
# }
# validate_customer_data(customer_df, config)


---------------
from pyspark.sql import DataFrame
from pyspark.sql.readwriter import DataFrameWriter
from typing import Optional, List

def _validate_input(df: DataFrame, file_path: str) -> None:
    """
    Validate the input DataFrame and file path.

    Args:
        df (DataFrame): The Spark DataFrame to be validated.
        file_path (str): The file path to be validated.

    Raises:
        ValueError: If the DataFrame is empty or if the file_path is invalid.
    """
    if df.rdd.isEmpty():
        raise ValueError("DataFrame is empty. Cannot save an empty DataFrame.")
    if not file_path:
        raise ValueError("Invalid file path. Please provide a valid path.")

def _save_dataframe(
    df: DataFrame,
    file_path: str,
    header: bool,
    separator: str,
    overwrite: bool
) -> None:
    """
    Save the DataFrame as a CSV file.

    Args:
        df (DataFrame): The Spark DataFrame to be saved.
        file_path (str): The path where the CSV file will be saved.
        header (bool): Whether to include a header in the CSV file.
        separator (str): The separator to use in the CSV file.
        overwrite (bool): Whether to overwrite existing files.
    """
    write_mode = "overwrite" if overwrite else "error"
    df.repartition(1).write.format('csv').mode(write_mode).save(
        file_path, 
        header=header, 
        sep=separator
    )

def _list_files(directory: str) -> List[object]:
    """
    List all files in the given directory.

    Args:
        directory (str): The directory to list files from.

    Returns:
        List[object]: A list of file objects in the directory.
    """
    return dbutils.fs.ls(directory)

def _find_csv_file(files: List[object]) -> Optional[object]:
    """
    Find the first CSV file in the list of files.

    Args:
        files (List[object]): A list of file objects to search through.

    Returns:
        Optional[object]: The first CSV file found, or None if no CSV file is found.
    """
    return next((f for f in files if f.name.endswith(".csv")), None)

def _rename_and_replace_file(file_path: str, csv_file: object) -> None:
    """
    Rename the CSV file and replace the original directory.

    Args:
        file_path (str): The original file path.
        csv_file (object): The CSV file object to be renamed and moved.
    """
    temp_path = file_path + "_tmp"
    dbutils.fs.cp(file_path + "/" + csv_file.name, temp_path)
    dbutils.fs.rm(file_path, recurse=True)
    dbutils.fs.mv(temp_path, file_path)

def _ensure_single_file(file_path: str) -> None:
    """
    Ensure only a single CSV file remains in the specified directory.

    Args:
        file_path (str): The path to the directory containing the CSV file.

    Raises:
        ValueError: If no CSV file is found in the specified directory.
    """
    files = _list_files(file_path)
    csv_file = _find_csv_file(files)
    if csv_file:
        _rename_and_replace_file(file_path, csv_file)
    else:
        raise ValueError(f"No CSV file found in {file_path}")

def save_as_single_csv_file(
    df: DataFrame,
    file_path: str,
    header: bool = True,
    separator: str = ";",
    overwrite: bool = True
) -> None:
    """
    Save a Spark DataFrame as a single CSV file.

    This function repartitions the DataFrame to a single partition,
    saves it as a CSV file, and then ensures only one CSV file remains
    in the specified directory.

    Args:
        df (DataFrame): The Spark DataFrame to be saved.
        file_path (str): The path where the CSV file will be saved.
        header (bool, optional): Whether to include a header in the CSV file. Defaults to True.
        separator (str, optional): The separator to use in the CSV file. Defaults to ";".
        overwrite (bool, optional): Whether to overwrite existing files. Defaults to True.

    Returns:
        None

    Raises:
        ValueError: If the DataFrame is empty or if the file_path is invalid.
    """
    _validate_input(df, file_path)
    _save_dataframe(df, file_path, header, separator, overwrite)
    _ensure_single_file(file_path)

# Add the method to DataFrameWriter for backwards compatibility
DataFrameWriter.singleFileSave = save_as_single_csv_file


# Example
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession (in Databricks this is already available as 'spark')
spark = SparkSession.builder.appName("SingleCSVFileExample").getOrCreate()

# Create a sample DataFrame
data = [
    ("Alice", 28, "New York"),
    ("Bob", 35, "San Francisco"),
    ("Charlie", 42, "London"),
    ("Diana", 31, "Paris")
]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# Display the DataFrame
print("Sample DataFrame:")
df.show()

# Define the file path where you want to save the CSV
# Note: In Databricks, you might use a path like "/dbfs/mnt/your-mount-point/your-file.csv"
file_path = "/path/to/your/output/single_file.csv"

# Use the save_as_single_csv_file function
save_as_single_csv_file(
    df=df,
    file_path=file_path,
    header=True,
    separator=",",
    overwrite=True
)

print(f"DataFrame saved as a single CSV file at: {file_path}")

# Optionally, verify the saved file
saved_df = spark.read.csv(file_path, header=True, inferSchema=True)
print("\nContents of the saved CSV file:")
saved_df.show()

# If you're using the DataFrameWriter method (for backwards compatibility)
# df.write.singleFileSave(file_path, header='true', sep=",")


---------

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing import Dict, Any

def _format_variable_info(name: str, value: Any) -> str:
    """
    Format the variable information as a string.
    
    Args:
        name (str): The name of the variable.
        value (Any): The value of the variable.
    
    Returns:
        str: A formatted string with variable information.
    """
    if isinstance(value, DataFrame):
        return f"DataFrame[{', '.join([f'{f.name}: {f.dataType}' for f in value.schema.fields])}]"
    elif callable(value):
        return "function"
    elif isinstance(value, (int, float, str, bool)):
        return f"{type(value).__name__} = {value}"
    else:
        return type(value).__name__

def show_notebook_variables() -> DataFrame:
    """
    Display all defined variables in the current Databricks notebook.
    
    Returns:
        DataFrame: A DataFrame containing variable names and their formatted information.
    
    Example:
        >>> show_notebook_variables().show(truncate=False)
        +---------------+----------------------------------------+
        |variable_name  |variable_info                           |
        +---------------+----------------------------------------+
        |df             |DataFrame[id: bigint]                   |
        |x              |int = 10                                |
        |my_function    |function                                |
        +---------------+----------------------------------------+
    """
    global_vars = globals()
    variable_info = [
        (name, _format_variable_info(name, value))
        for name, value in global_vars.items()
        if not (name.startswith('_') or name in ('show_notebook_variables', '_format_variable_info'))
    ]
    
    return spark.createDataFrame(variable_info, ["variable_name", "variable_info"])

```

