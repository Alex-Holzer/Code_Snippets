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


```

