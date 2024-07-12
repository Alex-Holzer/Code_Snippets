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
