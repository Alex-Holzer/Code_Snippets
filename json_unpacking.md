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

from pyspark.sql.functions import col, expr, when, size
from pyspark.sql.types import StructType, ArrayType

def unpack_structured_column(df, column_name):
    """
    Recursively unpacks a structured column in a PySpark DataFrame,
    flattening nested structures and handling arrays generically.
    
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
            elif isinstance(field.dataType, ArrayType):
                flat_fields.append(name)
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
        elif isinstance(field_type, ArrayType):
            # For array types, we create a column for the last element
            df = df.withColumn(
                f"{parent_col}_last",
                when(size(col(parent_col)) > 0, col(parent_col)[size(col(parent_col)) - 1])
            )
            # If the array contains structs, unpack the last element
            if isinstance(field_type.elementType, StructType):
                df = unpack_struct(df, f"{parent_col}_last")
        else:
            df = df.withColumn(parent_col, col(parent_col))
        return df
    
    return unpack_struct(df, column_name)

# Usage example:
# Assuming 'df' is your DataFrame and 'parsed_json' is the structured column
# unpacked_df = unpack_structured_column(df, "parsed_json")

# To unpack specific nested structures:
# unpacked_df = unpack_structured_column(df, "parsed_json_keys")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_lvks")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_lvv")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_vtg")

## ------- test new verions ---------

from pyspark.sql.functions import col, expr, when, size, explode
from pyspark.sql.types import StructType, ArrayType

def unpack_structured_column(df, column_name):
    """
    Recursively unpacks a structured column in a PySpark DataFrame,
    flattening nested structures and handling arrays generically.
    Also unpacks each element of the last array element.
    
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
            elif isinstance(field.dataType, ArrayType):
                flat_fields.append(name)
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
        elif isinstance(field_type, ArrayType):
            # For array types, we create a column for the last element
            df = df.withColumn(
                f"{parent_col}_last",
                when(size(col(parent_col)) > 0, col(parent_col)[size(col(parent_col)) - 1])
            )
            # If the array contains structs, unpack the last element
            if isinstance(field_type.elementType, StructType):
                df = unpack_struct(df, f"{parent_col}_last")
            
            # Unpack each element of the last array element
            df = df.withColumn(f"{parent_col}_last_unpacked", explode(col(f"{parent_col}_last")))
            df = unpack_struct(df, f"{parent_col}_last_unpacked")
        else:
            df = df.withColumn(parent_col, col(parent_col))
        return df
    
    return unpack_struct(df, column_name)

# Usage example:
# Assuming 'df' is your DataFrame and 'parsed_json' is the structured column
# unpacked_df = unpack_structured_column(df, "parsed_json")

# To unpack specific nested structures:
# unpacked_df = unpack_structured_column(df, "parsed_json_keys")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_lvks")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_lvv")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_vtg")

```
