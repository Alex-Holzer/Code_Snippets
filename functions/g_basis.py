from pyspark.sql import functions as F


# Define a single trim function
def trim_col(col_name):
    return F.trim(F.col(col_name))


# Apply trimming to all relevant columns at once
columns_to_trim = [
    "VERTRAGSNUMMER",
    "MOP_ID",
    "VW_NAME",
    "VB_NAME",
    "VS_NAME",
    "AKTION",
    "FACHLICHER_STATUS",
    "FACHLICHER_SCHRITT",
    "GRUND",
    "ORG_EINHEIT_ID",
    "AG_KEY",
]
df_trimmed = df.select(
    *[trim_col(col).alias(col) for col in columns_to_trim],
    *[col for col in df.columns if col not in columns_to_trim]
)

# Define conditions for VW_DESC_MOD
vw_desc_mod_conditions = [
    (F.col("VB_NAME").like("%Post%"), "Postbank"),
    (F.col("MANDANT").isin(["PLAT", "PAT"]), "PAT"),
    (F.col("VS_NAME").like("%noris%"), "Norisbank"),
    (F.col("VW_NAME").isNull(), "Kein Vertriebsweg"),
    (F.col("VW_NAME").isin(["Z-EP", "ZEP"]), "ZEP"),
    (F.col("VW_NAME").isin(["Broker Retail"]), "Makler"),
    (F.col("VW_NAME").isin(["Bank"]), "DB"),
]

# Final DataFrame transformation
df_final = df_trimmed.select(
    F.col("_CASE_KEY_GLIFE").alias("_CASE_KEY"),
    F.when(
        (F.col("VERTRAGSNUMMER").like("%000000%"))
        & (F.col("Vorgangs_Indicator").isNull()),
        F.col("VERTRAGSNUMMER_GLIFE"),
    )
    .otherwise(F.col("VERTRAGSNUMMER"))
    .alias("VERTRAGSNUMMER"),
    F.col("MOP_ID").alias("VORGANGSNUMMER"),
    "ATRG_NR",
    "VW_KEY",
    F.col("VW_NAME").alias("Vertriebsweg"),
    "VB_KEY",
    F.col("VB_NAME").alias("Vertriebsbereich"),
    "VS_KEY",
    F.col("VS_NAME").alias("Vertriebsstelle"),
    F.to_timestamp(F.col("ERFT_TS"), "y.M.d H:m:s").alias("ERFT_TS"),
    "AKTION",
    F.when(F.col("FACHLICHER_STATUS") == "", None).otherwise(
        F.col("FACHLICHER_STATUS")
    ),
    "FACHLICHER_SCHRITT",
    F.when(F.col("GRUND") == "", None)
    .when(
        F.col("GRUND").contains("besonderer Versandhinweis"),
        "besonderer Versandhinweis",
    )
    .otherwise(F.col("GRUND"))
    .alias("GRUND"),
    "ORG_EINHEIT_ID",
    F.coalesce(
        *[F.when(cond, val) for cond, val in vw_desc_mod_conditions], F.lit("Sonstige")
    ).alias("VW_DESC_MOD"),
    "AG_KEY",
    F.lit("VORGANGSDATEN").alias("DATENHERKUNFT"),
).distinct()
