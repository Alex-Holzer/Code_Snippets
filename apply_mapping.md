```python




from functools import wraps
from typing import Any, Callable, Tuple

def validate_args(*validators: Callable[[Any], None]):
    """
    A decorator that applies validation functions to the arguments of the decorated function.

    Args:
        *validators: A variable number of validation functions to be applied to the function arguments.

    Returns:
        Callable: A decorator function.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Validate positional arguments
            for validator, arg in zip(validators, args):
                try:
                    validator(arg)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Validation failed for argument: {arg}. Error: {str(e)}")
            
            # Validate keyword arguments
            func_params = func.__annotations__
            for param_name, param_value in kwargs.items():
                if param_name in func_params:
                    validator_index = list(func_params.keys()).index(param_name)
                    if validator_index < len(validators):
                        try:
                            validators[validator_index](param_value)
                        except (ValueError, TypeError) as e:
                            raise ValueError(f"Validation failed for argument '{param_name}': {param_value}. Error: {str(e)}")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def validate_positive(value: float) -> None:
    if value <= 0:
        raise ValueError("Value must be positive")

def validate_string(value: str) -> None:
    if not isinstance(value, str):
        raise TypeError("Value must be a string")

@validate_args(validate_positive, validate_positive, validate_string)
def calculate_volume(length: float, width: float, unit: str) -> Tuple[float, str]:
    return length * width, unit

# Usage
print(calculate_volume(5, 3, "cm²"))  # Valid: (15, 'cm²')
# print(calculate_volume(-1, 3, "cm²"))  # Raises ValueError
# print(calculate_volume(5, 3, 42))  # Raises TypeError
# print(calculate_volume(length=5, width=3, unit="cm²"))  # Valid: (15, 'cm²')



from pyspark.sql import DataFrame
from typing import Any, Dict

def validate_pyspark_dataframe(df: Any) -> None:
    """
    Validate that the input is a PySpark DataFrame and is not empty.

    Args:
        df (Any): The object to validate.

    Raises:
        TypeError: If the input is not a PySpark DataFrame.
        ValueError: If the DataFrame is empty.
    """
    if not isinstance(df, DataFrame):
        raise TypeError(f"Expected PySpark DataFrame, got {type(df).__name__}")
    
    if df.rdd.isEmpty():
        raise ValueError("DataFrame is empty")

def validate_dictionary(d: Any) -> None:
    """
    Validate that the input is a dictionary and is not empty.

    Args:
        d (Any): The object to validate.

    Raises:
        TypeError: If the input is not a dictionary.
        ValueError: If the dictionary is empty.
    """
    if not isinstance(d, dict):
        raise TypeError(f"Expected dictionary, got {type(d).__name__}")
    
    if not d:
        raise ValueError("Dictionary is empty")

# Example usage with the validate_args decorator
@validate_args(validate_pyspark_dataframe, validate_dictionary)
def process_dataframe_with_params(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Process a PySpark DataFrame using the provided parameters.

    Args:
        df (DataFrame): The input PySpark DataFrame to process.
        params (Dict[str, Any]): A dictionary of parameters for processing.

    Returns:
        DataFrame: The processed DataFrame.
    """
    # Example processing (replace with actual logic)
    for column, value in params.items():
        if column in df.columns:
            df = df.filter(df[column] == value)
    return df

# Usage example (assuming you have a SparkSession named 'spark')
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("ValidationExample").getOrCreate()
# 
# sample_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "category"])
# sample_params = {"category": "A"}
# 
# result = process_dataframe_with_params(sample_df, sample_params)
# result.show()


```

