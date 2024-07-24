```python

-------- Execution Measure  -----------------
from pyspark.sql import DataFrame
from time import time
from typing import Callable, Any
import functools

def time_execution(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator that measures and logs the execution time of a function in Databricks.
    
    Args:
        func (Callable[..., Any]): The function to be timed.
    
    Returns:
        Callable[..., Any]: A wrapper function that times the execution of the input function.
    
    Example:
        @time_execution
        def my_function(df: DataFrame) -> DataFrame:
            # Function logic here
            return df
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time()
        result = func(*args, **kwargs)
        end_time = time()
        execution_time = end_time - start_time
        
        print(f"Function '{func.__name__}' executed in {execution_time:.4f} seconds")
        
        return result
    return wrapper

# Add the transform method to DataFrame if not already present
if not hasattr(DataFrame, 'transform'):
    DataFrame.transform = lambda self, f, *args, **kwargs: f(self, *args, **kwargs)



from pyspark.sql.functions import col

@time_execution
def double_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Double the values in a specified column of a DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame
        column_name (str): Name of the column to be doubled
    
    Returns:
        DataFrame: DataFrame with the specified column doubled
    """
    return df.withColumn(f"{column_name}_doubled", col(column_name) * 2)

# Create a sample DataFrame
data = [(1,), (2,), (3,)]
df = spark.createDataFrame(data, ["value"])

# Apply the timed function
result_df = df.transform(double_column, "value")

result_df.show()

----------------------------- 


```
