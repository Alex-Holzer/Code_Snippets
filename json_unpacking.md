```python

from pyspark.sql.functions import col, explode
from typing import List, Union

def extract_nested_values(
    df: "pyspark.sql.DataFrame",
    json_column: str,
    paths: Union[str, List[str]],
    explode_arrays: bool = False
) -> "pyspark.sql.DataFrame":
    """
    Efficiently extracts values from a nested JSON structure in a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame with parsed JSON data.
        json_column (str): Name of the column containing parsed JSON data.
        paths (Union[str, List[str]]): Dot-notated path(s) to the desired field(s).
        explode_arrays (bool, optional): Whether to explode array fields. Defaults to False.

    Returns:
        pyspark.sql.DataFrame: DataFrame with extracted values.

    Example:
        >>> df = spark.createDataFrame([("1", '{"lvv": {"objektZustand": {"val": "AKTIV"}}}')], ["id", "data"])
        >>> parsed_df = infer_and_parse_json_column(df, "data", "json_data")
        >>> result = extract_nested_values(parsed_df, "json_data", "lvv.objektZustand.val")
        >>> result.show()
    """
    if isinstance(paths, str):
        paths = [paths]

    select_expr = []
    for path in paths:
        full_path = f"{json_column}.{path}"
        if explode_arrays and "[" in path:
            # Handle array fields if explode_arrays is True
            array_path, field_path = path.split("[", 1)
            field_path = field_path.rstrip("]")
            select_expr.append(explode(col(f"{json_column}.{array_path}")).alias(array_path))
            select_expr.append(col(f"{array_path}.{field_path}").alias(path.replace(".", "_")))
        else:
            select_expr.append(col(full_path).alias(path.replace(".", "_")))

    return df.select("*", *select_expr)

# Helper function to get all leaf nodes from the schema
def get_all_leaf_nodes(schema, prefix=""):
    paths = []
    for field in schema.fields:
        new_prefix = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            paths.extend(get_all_leaf_nodes(field.dataType, new_prefix))
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            paths.extend(get_all_leaf_nodes(field.dataType.elementType, f"{new_prefix}[]"))
        else:
            paths.append(new_prefix)
    return paths

# Example usage:
# all_paths = get_all_leaf_nodes(json_schema)
# extracted_df = extract_nested_values(parsed_df, "json_data", all_paths, explode_arrays=True)

```
