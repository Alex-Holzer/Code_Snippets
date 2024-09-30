```python

def remove_empty_columns(df: DataFrame) -> DataFrame:
    """
    Remove all columns from a PySpark DataFrame that contain only null or NaN values.

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame from which empty columns need to be removed.

    Returns
    -------
    DataFrame
        A new DataFrame without columns that are entirely null or NaN.
    """
    # Get column names that are not completely empty (i.e., not all null or NaN)
    non_empty_columns = [
        column for column in df.columns
        if df.filter(~(col(column).isNull() | isnan(col(column)))).count() > 0
    ]
    
    # Select only non-empty columns
    return df.select(*non_empty_columns)


```
