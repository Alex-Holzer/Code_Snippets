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
    return wrapper----------------------------- 


```
