```python

from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

def unpack_structured_column(df, column_name):
    """
    Recursively unpacks a structured column in a PySpark DataFrame, handling arrays correctly.
    
    Args:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str): The name of the column to unpack.
    
    Returns:
    DataFrame: The DataFrame with the unpacked columns.
    """
    def unpack_struct(df, parent_col):
        struct_fields = df.select(f"{parent_col}.*").columns
        for field in struct_fields:
            col_name = f"{parent_col}.{field}" if parent_col else field
            field_type = df.select(col_name).schema[0].dataType
            
            if isinstance(field_type, StructType):
                df = unpack_struct(df, col_name)
            elif isinstance(field_type, ArrayType):
                if isinstance(field_type.elementType, StructType):
                    exploded_col_name = f"{col_name}_exploded"
                    df = df.withColumn(exploded_col_name, explode_outer(col(col_name)))
                    df = unpack_struct(df, exploded_col_name)
                    # Keep the original array column
                    df = df.drop(exploded_col_name)
                else:
                    df = df.withColumn(col_name, col(col_name))
            else:
                df = df.withColumn(col_name, col(col_name))
        return df
    
    return unpack_struct(df, column_name)

# Usage example:
# Assuming 'df' is your DataFrame and 'parsed_json' is the structured column
# unpacked_df = unpack_structured_column(df, "parsed_json")

# To unpack specific nested structures:
# unpacked_df = unpack_structured_column(df, "parsed_json.keys")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json.lvks")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json.lvv")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json.vtg")


```
