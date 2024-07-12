from pyspark.sql import functions as F

# Define the conditions for VORGANGS_TYP_MOP_TYPE_NAME
conditions = [
    "VORGANGS_TYP_MOP_TYPE_NAME LIKE '%PBLNeuAV%'",
    "VORGANGS_TYP_MOP_TYPE_NAME LIKE '%PBLNeuV%'",
    "VORGANGS_TYP_MOP_TYPE_NAME LIKE '%MLNeuFVLV%'",
    "VORGANGS_TYP_MOP_TYPE_NAME LIKE '%PFNGAUTUEvg%'",
    "VORGANGS_TYP_MOP_TYPE_NAME LIKE '%VSLNGAUTUEvg%'",
]

# Create the DataFrame with optimized operations
df_datalake = spark.sql(
    f"""
    SELECT DISTINCT 
        VERTRAGSNUMMER, 
        trim(MOP_ID) AS MOP_ID, 
        ERFT_TS, 
        AKTION, 
        FACHLICHER_STATUS, 
        GRUND, 
        TARGET_ACTOR_ID, 
        FACHLICHER_SCHRITT,
        ORG_EINHEIT_ID 
    FROM prod_app_degi_zdw_workflow.v_udw80f002t 
    WHERE {" OR ".join(conditions)}
"""
)

# Perform the join operation
GLIFE_BASIS_plus = df_datalake.join(
    glife_basis_plus, df_datalake.MOP_ID == glife_basis_plus.VORGANG_ID, "left"
)

# Cache the resulting DataFrame if it will be used multiple times
GLIFE_BASIS_plus.cache()

# Show the first few rows of the result (optional)
GLIFE_BASIS_plus.show(5)


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the table using spark.table()
df = spark.table("prod_app_degi_zdw_workflow.v_udw80f002t")

# Define the conditions for VORGANGS_TYP_MOP_TYPE_NAME
mop_type_conditions = [
    F.col("VORGANGS_TYP_MOP_TYPE_NAME").like("%PBLNeuAV%"),
    F.col("VORGANGS_TYP_MOP_TYPE_NAME").like("%PBLNeuV%"),
    F.col("VORGANGS_TYP_MOP_TYPE_NAME").like("%MLNeuFVLV%"),
    F.col("VORGANGS_TYP_MOP_TYPE_NAME").like("%PFNGAUTUEvg%"),
    F.col("VORGANGS_TYP_MOP_TYPE_NAME").like("%VSLNGAUTUEvg%"),
]

# Create the DataFrame with optimized operations
df_datalake = df.select(
    "VERTRAGSNUMMER",
    F.trim(F.col("MOP_ID")).alias("MOP_ID"),
    "ERFT_TS",
    "AKTION",
    "FACHLICHER_STATUS",
    "GRUND",
    "TARGET_ACTOR_ID",
    "FACHLICHER_SCHRITT",
    "ORG_EINHEIT_ID",
).where(F.reduce(lambda a, b: a | b, mop_type_conditions))

# Remove duplicates
df_datalake = df_datalake.distinct()

# Perform the join operation
GLIFE_BASIS_plus = df_datalake.join(
    spark.table("glife_basis_plus"), df_datalake.MOP_ID == F.col("VORGANG_ID"), "left"
)

# Cache the resulting DataFrame if it will be used multiple times
GLIFE_BASIS_plus.cache()

# Show the first few rows of the result (optional)
GLIFE_BASIS_plus.show(5)

# Optionally, if you need to optimize the number of partitions:
# GLIFE_BASIS_plus = GLIFE_BASIS_plus.repartition(100)  # Adjust the number based on your cluster size and data volume
