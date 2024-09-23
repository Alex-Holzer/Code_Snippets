```python

from pyspark.sql.types import ArrayType, StructType

# Get all columns that are of type ArrayType or StructType
array_or_struct_cols = [
    field.name for field in flattened_df.schema.fields 
    if isinstance(field.dataType, (ArrayType, StructType))
]

# Display the result
array_or_struct_cols


```
