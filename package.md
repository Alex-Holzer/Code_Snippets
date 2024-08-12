my_pyspark_package/
│
├── setup.py
├── README.md
│
└── src/
    └── my_pyspark_lib/
        ├── __init__.py
        ├── libraries.py
        └── transformations.py

# File: setup.py
from setuptools import setup, find_packages

setup(
    name="my_pyspark_lib",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.0.0",
    ],
    author="Your Name",
    description="A package for PySpark transformations",
)

# File: README.md
# My PySpark Library

This package contains useful PySpark transformations and utilities.

## Usage

```python
from my_pyspark_lib.transformations import add_column, filter_by_condition, group_and_aggregate

# Use the functions in your PySpark code
```

# File: src/my_pyspark_lib/__init__.py
from .transformations import *

# File: src/my_pyspark_lib/libraries.py
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from typing import List, Dict, Union

# File: src/my_pyspark_lib/transformations.py
from .libraries import *

def add_column(df: DataFrame, column_name: str, value: Union[str, int, float]) -> DataFrame:
    """
    Add a new column to the DataFrame with a constant value.
    """
    return df.withColumn(column_name, F.lit(value))

def filter_by_condition(df: DataFrame, condition: str) -> DataFrame:
    """
    Filter the DataFrame based on a given condition.
    """
    return df.filter(condition)

def group_and_aggregate(df: DataFrame, group_cols: List[str], agg_dict: Dict[str, str]) -> DataFrame:
    """
    Group the DataFrame by specified columns and perform aggregations.
    """
    return df.groupBy(group_cols).agg(agg_dict)

# Add more transformation functions as needed
