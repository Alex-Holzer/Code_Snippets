# PySpark and Databricks Optimization Techniques

## Catalyst Optimizer
- Enable adaptive query execution: `spark.conf.set("spark.sql.adaptive.enabled", "true")`
- Use join hints for small tables: `.hint("broadcast")`
- Push down predicates early in the query

## Tungsten Execution Engine
- Define schemas explicitly when reading data
- Use DataFrame operations instead of RDDs
- Avoid Python UDFs when possible; use Scala or Java UDFs for critical operations

## DataFrame Caching and Persistence
- Use `persist()` with appropriate `StorageLevel`
- Cache frequently accessed DataFrames
- Unpersist when data is no longer needed
- Monitor cache usage with `spark.sparkContext.getPersistentRDDs()`

## Broadcast Joins and Handling Skewed Data
- Use `broadcast()` for small DataFrames in joins
- Repartition data before joining to handle skew
- Consider salting techniques for extremely skewed data

## Partitioning and Shuffling Optimization
- Partition data based on common query patterns (e.g., date-based partitioning)
- Adjust `spark.sql.shuffle.partitions` based on data size
- Use `coalesce()` instead of `repartition()` when reducing partitions
- Implement bucketing for high-cardinality columns used in joins

## Query Optimization
- Use `explain()` to analyze and optimize query plans
- Look for opportunities to reduce shuffling and data movement
- Leverage filter pushdown and column pruning

## Schema Optimization
- Define schemas explicitly when reading data
- Use appropriate data types to minimize memory usage
- Consider using nested structures for related fields

## I/O Optimization
- Use columnar file formats like Parquet or ORC
- Enable predicate pushdown: `spark.sql.parquet.filterPushdown`
- Use appropriate compression codecs

## UDF Optimization
- Minimize UDF usage; prefer built-in functions
- Use Scalar UDFs or Pandas UDFs for better performance
- Vectorize operations when possible

## Resource Management
- Configure executor memory and cores appropriately
- Use dynamic allocation: `spark.dynamicAllocation.enabled`
- Monitor and adjust parallelism based on cluster resources

## Data Skew Handling
- Identify skewed keys using approx_count_distinct()
- Implement salting or separate processing for skewed keys
- Consider using `repartition()` to redistribute data evenly

## Caching and Checkpointing
- Cache intermediate results for iterative algorithms
- Use checkpointing for long lineage chains
- Clear unnecessary cached data to manage memory

## Monitoring and Tuning
- Use Spark UI to identify bottlenecks
- Monitor GC behavior and adjust if necessary
- Implement custom accumulators for fine-grained progress tracking