```python



from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import Dict, Any, Callable, Optional

def create_condition_function(condition: Any) -> Callable:
    """
    Create a condition function based on the provided condition.
    
    Args:
        condition (Any): The condition to be evaluated.
    
    Returns:
        Callable: A function that evaluates the condition.
    
    Examples:
        >>> create_condition_function("constant")
        <function create_condition_function.<locals>.<lambda>>
        
        >>> create_condition_function(lambda col: col > 5)
        <function create_condition_function.<locals>.<lambda>>
        
        >>> create_condition_function(10)
        <function create_condition_function.<locals>.<lambda>>
    """
    if isinstance(condition, str):
        return lambda col: F.lit(condition)
    elif callable(condition):
        return condition
    else:
        return lambda col: F.lit(condition)

def apply_mapping(
    df: DataFrame, 
    column: str, 
    mapping_dict: Dict[Any, Any], 
    new_column: Optional[str] = None
) -> DataFrame:
    """
    Apply value mapping to a DataFrame column based on a dictionary of conditions.
    
    Args:
        df (DataFrame): The input DataFrame.
        column (str): The name of the column to apply the mapping to.
        mapping_dict (Dict[Any, Any]): A dictionary where keys are the new values and values are the conditions.
        new_column (Optional[str]): If provided, creates a new column with this name instead of modifying the existing column.
    
    Returns:
        DataFrame: The DataFrame with the mapped values.
    
    Examples:
        >>> df = spark.createDataFrame([("apple", "red"), ("banana", "yellow")], ["fruit", "color"])
        >>> mapping_dict = {"red fruit": F.col("color") == "red", "yellow fruit": F.col("color") == "yellow"}
        
        # Modifying existing column
        >>> result1 = apply_mapping(df, "fruit", mapping_dict)
        >>> result1.show()
        +----------+------+
        |     fruit| color|
        +----------+------+
        | red fruit|   red|
        |yellow fruit|yellow|
        +----------+------+
        
        # Creating a new column
        >>> result2 = apply_mapping(df, "fruit", mapping_dict, new_column="category")
        >>> result2.show()
        +------+------+------------+
        | fruit| color|    category|
        +------+------+------------+
        | apple|   red|   red fruit|
        |banana|yellow|yellow fruit|
        +------+------+------------+
    """
    conditions = [
        (create_condition_function(condition)(F.col(column)), F.lit(new_value))
        for new_value, condition in mapping_dict.items()
    ]
    
    mapped_column = F.coalesce(
        F.when(conditions[0][0], conditions[0][1]),
        *[F.when(cond, val) for cond, val in conditions[1:]],
        F.col(column)
    )
    
    if new_column:
        return df.withColumn(new_column, mapped_column)
    else:
        return df.withColumn(column, mapped_column)

def transform_dataframe(df: DataFrame, new_column: Optional[str] = None) -> DataFrame:
    """
    Apply transformations to the DataFrame using the value mapper function.
    
    Args:
        df (DataFrame): The input DataFrame.
        new_column (Optional[str]): If provided, creates a new column with this name instead of modifying the existing column.
    
    Returns:
        DataFrame: The transformed DataFrame.
    
    Examples:
        >>> df = spark.createDataFrame([
        ...     ("Apple", "red", "Fruit description", "USA", "user@gmail.com", 25),
        ...     ("Banana", "yellow", "Yellow fruit", "Canada", "user@yahoo.com", 30)
        ... ], ["name", "color", "description", "country", "email", "age"])
        
        # Modifying existing column
        >>> result1 = transform_dataframe(df)
        >>> result1.show()
        +------+------+------------------+-------+---------------+---+--------+
        |  name| color|       description|country|          email|age|category|
        +------+------+------------------+-------+---------------+---+--------+
        | Apple|   red|Fruit description|    USA| user@gmail.com| 25|   apple|
        |Banana|yellow|     Yellow fruit| Canada|user@yahoo.com| 30|    null|
        +------+------+------------------+-------+---------------+---+--------+
        
        # Creating a new column
        >>> result2 = transform_dataframe(df, new_column="mapped_category")
        >>> result2.show()
        +------+------+------------------+-------+---------------+---+---------------+
        |  name| color|       description|country|          email|age|mapped_category|
        +------+------+------------------+-------+---------------+---+---------------+
        | Apple|   red|Fruit description|    USA| user@gmail.com| 25|          apple|
        |Banana|yellow|     Yellow fruit| Canada|user@yahoo.com| 30|           null|
        +------+------+------------------+-------+---------------+---+---------------+
    """
    mapping_dict = {
        "apple": F.col("description").contains("ap"),
        "pie": F.col("color") == "red",
        "pear": F.col("name").like("A%"),
        "gym": F.col("country").isin("USA", "Canada", "Mexico"),
        "email_match": F.col("email").rlike("@gmail\.com$"),
        "adult": F.col("age").between(18, 65)
    }
    
    return df.transform(lambda d: apply_mapping(d, "category", mapping_dict, new_column))


```
