def flexible_groupby_agg(
    df: F.DataFrame, group_cols: List[str], agg_expressions: Dict[str, F.Column]
) -> F.DataFrame:
    """
    Perform flexible groupBy aggregation on the input DataFrame.

    Args:
        df (DataFrame): Input DataFrame
        group_cols (List[str]): Columns to group by
        agg_expressions (Dict[str, Column]): Dictionary of aggregation expressions

    Returns:
        DataFrame: Aggregated DataFrame
    """
    validate_columns(df, group_cols + list(agg_expressions.keys()))
    return df.groupBy(*group_cols).agg(
        *[expr.alias(name) for name, expr in agg_expressions.items()]
    )


# Usage
result = flexible_groupby_agg(
    df,
    group_cols=["category", "region"],
    agg_expressions={
        "total_sales": F.sum("sales"),
        "avg_price": F.avg("price"),
        "unique_customers": F.countDistinct("customer_id"),
    },
)
