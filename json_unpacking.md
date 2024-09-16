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

from pyspark.sql.functions import col, expr, when, size, explode, array_except
from pyspark.sql.types import StructType, ArrayType

def unpack_structured_column(df, column_name, num_objects=None):
    """
    Recursively unpacks a structured column in a PySpark DataFrame,
    flattening nested structures and handling arrays generically.
    Extracts individual objects from the last array entry as separate columns.
    
    Args:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str): The name of the column to unpack.
    num_objects (int, optional): Number of objects to extract from the end of the array.
                                 If None, all objects are extracted.
    
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
            # For array types, we create columns for individual objects from the end
            array_size = df.select(size(col(parent_col))).first()[0]
            
            if num_objects is None or num_objects > array_size:
                objects_to_extract = array_size
            else:
                objects_to_extract = num_objects
            
            for i in range(objects_to_extract):
                index = -objects_to_extract + i
                df = df.withColumn(
                    f"{parent_col}_obj_{i}",
                    when(size(col(parent_col)) > abs(index), col(parent_col)[index])
                )
                if isinstance(field_type.elementType, StructType):
                    df = unpack_struct(df, f"{parent_col}_obj_{i}")
            
            # Remove extracted objects from the original array
            if num_objects is not None and num_objects < array_size:
                df = df.withColumn(
                    parent_col,
                    array_except(col(parent_col), array(*[col(f"{parent_col}_obj_{i}") for i in range(objects_to_extract)]))
                )
        else:
            df = df.withColumn(parent_col, col(parent_col))
        return df
    
    return unpack_struct(df, column_name)

# Usage example:
# Assuming 'df' is your DataFrame and 'parsed_json' is the structured column
# To extract all objects:
# unpacked_df = unpack_structured_column(df, "parsed_json")
# To extract the last 3 objects:
# unpacked_df = unpack_structured_column(df, "parsed_json", num_objects=3)

# To unpack specific nested structures:
# unpacked_df = unpack_structured_column(df, "parsed_json_keys")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_lvks")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_lvv")
# unpacked_df = unpack_structured_column(unpacked_df, "parsed_json_vtg")



#-------------- Identify Array Structure---------

from pyspark.sql.types import ArrayType, StructType, DataType
from typing import Dict, List, Union

def identify_array_objects(df, column_name: str = None) -> Dict[str, Union[str, List[str]]]:
    """
    Identifies all objects in specified column(s) that are in an array.
    If no column is specified, it checks all columns in the DataFrame.

    Args:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str, optional): The name of the column to check. If None, checks all columns.

    Returns:
    Dict[str, Union[str, List[str]]]: A dictionary where keys are column names and values are either
                                      the type of array elements or a list of struct field names.
    """
    def analyze_datatype(dt: DataType, prefix: str = "") -> Union[str, List[str]]:
        if isinstance(dt, ArrayType):
            if isinstance(dt.elementType, StructType):
                return [f"{prefix}{field.name}" for field in dt.elementType.fields]
            else:
                return str(dt.elementType)
        elif isinstance(dt, StructType):
            return [f"{prefix}{field.name}" for field in dt.fields]
        else:
            return str(dt)

    result = {}
    
    if column_name:
        columns_to_check = [column_name]
    else:
        columns_to_check = df.columns

    for col in columns_to_check:
        field = df.schema[col]
        if isinstance(field.dataType, ArrayType):
            result[col] = analyze_datatype(field.dataType.elementType)

    return result

# Usage example:
# array_objects = identify_array_objects(df)
# print(array_objects)

# To check a specific column:
# array_objects = identify_array_objects(df, "your_array_column")
# print(array_objects)

--- split -------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, posexplode_outer
from typing import List, Dict

def create_dataframes_from_array(df: DataFrame, column_name: str) -> List[DataFrame]:
    """
    Creates individual PySpark DataFrames for each array object in a specific column.

    Args:
    df (DataFrame): The input PySpark DataFrame.
    column_name (str): The name of the column containing the array.

    Returns:
    List[DataFrame]: A list of DataFrames, each representing an object from the array.
    """
    # First, identify if the column is an array and what it contains
    array_info = identify_array_objects(df, column_name)
    
    if column_name not in array_info:
        raise ValueError(f"Column '{column_name}' is not an array type.")

    # Get the size of the array
    array_size = df.select(F.size(col(column_name)).alias("array_size")).agg(F.max("array_size")).collect()[0][0]

    # Create a list to store our DataFrames
    result_dfs = []

    # For each index in the array, create a new DataFrame
    for i in range(array_size):
        # Extract the i-th element of the array
        df_i = df.select(col(f"{column_name}[{i}]").alias(column_name))
        
        # If the array contains structs, we need to unpack it
        if isinstance(array_info[column_name], list):
            for field in array_info[column_name]:
                df_i = df_i.withColumn(field, col(f"{column_name}.{field}"))
            # Drop the original struct column
            df_i = df_i.drop(column_name)
        
        result_dfs.append(df_i)

    return result_dfs

# Usage example:
# Assuming 'df' is your DataFrame and 'array_column' is the column containing the array
# array_dfs = create_dataframes_from_array(df, "array_column")

# To work with individual DataFrames:
# for i, df_i in enumerate(array_dfs):
#     print(f"DataFrame for object {i}:")
#     df_i.show()

# Assuming 'df' is your original DataFrame and 'array_column' is the column containing the array
array_dfs = create_dataframes_from_array(df, "array_column")

# To work with individual DataFrames:
for i, df_i in enumerate(array_dfs):
    print(f"DataFrame for object {i}:")
    df_i.show()


# ---------- Array Structure -------
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructType
from typing import Dict, Union, List

def identify_array_columns(df: DataFrame) -> Dict[str, Union[str, List[str]]]:
    """
    Identifies all columns in a PySpark DataFrame that have an array structure.

    Args:
    df (DataFrame): The input PySpark DataFrame.

    Returns:
    Dict[str, Union[str, List[str]]]: A dictionary where keys are column names with array structure,
                                      and values are either the type of array elements (as a string)
                                      or a list of field names for arrays of structs.
    """
    array_columns = {}

    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            if isinstance(field.dataType.elementType, StructType):
                array_columns[field.name] = [f.name for f in field.dataType.elementType.fields]
            else:
                array_columns[field.name] = str(field.dataType.elementType)

    return array_columns

# Usage example:
# array_cols = identify_array_columns(df)
# for col, content in array_cols.items():
#     print(f"Column '{col}' is an array of: {content}")


```
