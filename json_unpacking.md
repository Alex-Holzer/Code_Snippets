```python

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, ArrayType

def unpack_structured_column(df, column_name):
    """
    Recursively unpacks a structured column in a PySpark DataFrame, 
    flattening nested structures while preserving array columns.
    
    Args:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str): The name of the column to unpack.
    
    Returns:
    DataFrame: The DataFrame with the unpacked columns.
    """
    def flatten_struct(schema, prefix=""):
        flat_fields = []
        for field in schema.fields:
            name = prefix + field.name
            if isinstance(field.dataType, StructType):
                flat_fields.extend(flatten_struct(field.dataType, name + "."))
            else:
                flat_fields.append(name)
        return flat_fields

    def unpack_struct(df, parent_col):
        field_type = df.select(parent_col).schema[0].dataType
        if isinstance(field_type, StructType):
            flat_fields = flatten_struct(field_type)
            for flat_field in flat_fields:
                full_name = f"{parent_col}.{flat_field}"
                df = df.withColumn(full_name.replace(".", "_"), col(full_name))
        elif isinstance(field_type, ArrayType):
            # For array types, we keep the column as is
            df = df.withColumn(parent_col, col(parent_col))
        else:
            df = df.withColumn(parent_col, col(parent_col))
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

# ---- improved -------

from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, ArrayType

def unpack_structured_column(df, column_name):
    """
    Recursively unpacks a structured column in a PySpark DataFrame,
    flattening nested structures and unpacking the last element of arrays of structs.
    
    Args:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str): The name of the column to unpack.
    
    Returns:
    DataFrame: The DataFrame with the unpacked columns.
    """
    def flatten_struct(schema, prefix=""):
        flat_fields = []
        for field in schema.fields:
            name = prefix + field.name
            if isinstance(field.dataType, StructType):
                flat_fields.extend(flatten_struct(field.dataType, name + "_"))
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                flat_fields.extend(flatten_struct(field.dataType.elementType, name + "_last_"))
                flat_fields.append(name)  # Keep the original array column
            else:
                flat_fields.append(name)
        return flat_fields

    def unpack_struct(df, parent_col):
        field_type = df.select(parent_col).schema[0].dataType
        if isinstance(field_type, StructType):
            flat_fields = flatten_struct(field_type)
            for flat_field in flat_fields:
                full_name = f"{parent_col}.{flat_field.replace('_', '.')}"
                df = df.withColumn(parent_col + "_" + flat_field, col(full_name))
        elif isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType):
            # For array types containing structs, we get the last element and flatten
            flat_fields = flatten_struct(field_type.elementType)
            for flat_field in flat_fields:
                if flat_field.startswith(parent_col + "_last_"):
                    array_expr = f"{parent_col}[size({parent_col}) - 1].{flat_field.split('_last_')[1].replace('_', '.')}"
                    df = df.withColumn(flat_field, expr(array_expr))
        else:
            df = df.withColumn(parent_col, col(parent_col))
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
