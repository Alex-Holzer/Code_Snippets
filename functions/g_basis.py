from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_glife_basis_plus(spark, db_name):
    """
    Create an enhanced GLIFE_BASIS DataFrame with additional information from related tables.

    Args:
        spark (SparkSession): The active Spark session.
        db_name (str): The database name containing the GLIFE_BASIS table.

    Returns:
        DataFrame: Enhanced GLIFE_BASIS DataFrame with joined information.
    """
    # Load base tables
    glife_basis = spark.table(f"{db_name}.GLIFE_BASIS").select(
        F.col("_CASE_KEY").alias("_CASE_KEY_GLIFE"),
        F.col("VTG_VERS_NR_MOD").alias("VERTRAGSNUMMER_GLIFE"),
        "VTG_VERS_NR_VORG", "VNT_VERS_NR_VORG", "DATUM_BEARB_ZP",
        "ATSG_NR", "VORGANG_ID", "VERS_NR_TECHN", "MANDANT"
    )

    v_ude610001t = spark.table("prod_app_degi_zdw_vertrieb.v_ude610001t").select(
        "AR_KEY", F.trim("XP_VSNR").alias("XP_VSNR"), "VS_KEY", "VN_KEY",
        "AR_ROLLE", "AR_GUE_AB_DAT", "AR_GUE_BIS_DAT"
    ).filter(F.col("AR_ROLLE").like("A%"))

    v_ude600001t = spark.table("prod_app_degi_zdw_vertrieb.v_ude600001t").select(
        F.trim("VS_KEY").alias("VS_KEY"), "VS_VST", "VS_NAME", "VS_GUE_AB_DAT", "VS_GUE_BIS_DAT"
    )

    v_ude620002t = spark.table("prod_app_degi_zdw_vertrieb.v_ude620002t").select(
        F.trim("VN_KEY").alias("VN_KEY"), "VN_REV", "VN_NAME"
    )

    v_ude600003t = spark.table("prod_app_degi_zdw_vertrieb.v_ude600003t").select(
        F.trim("VN_KEY").alias("VN_KEY"), "VN_DESC"
    )

    v_ude870021 = spark.table("prod_app_degi_zdw_workflow.v_ude870021").select(
        "NOP_ID", "ENTP_TS", "ACTION", "FACHLICHER_STATUS", "GRUND",
        "TARGET_ACTOR_ID", "FACHLICHER_SCHRITT", "ORG_EINHEIT_ID"
    ).filter(
        F.col("VORGANG_TYP_NOP_TYPE_NAME").isin(
            "WPTLNEUGVAV", "WPTLNEUPVAV", "WPBILGVA", "WPBILANDPVA", "WPBILANDRVA", "WPBILDATDVA"
        )
    )

    # Perform joins
    result = glife_basis.join(
        v_ude610001t,
        (glife_basis.VERTRAGSNUMMER_GLIFE == v_ude610001t.XP_VSNR) |
        (glife_basis.VNT_VERS_NR_VORG == v_ude610001t.XP_VSNR),
        "left"
    ).join(
        v_ude600001t,
        (v_ude600001t.VS_KEY == v_ude610001t.VS_KEY) &
        (v_ude600001t.VS_GUE_AB_DAT <= glife_basis.DATUM_BEARB_ZP) &
        (v_ude600001t.VS_GUE_BIS_DAT > glife_basis.DATUM_BEARB_ZP),
        "left"
    ).join(
        v_ude620002t,
        v_ude620002t.VN_KEY == v_ude610001t.VN_KEY,
        "left"
    ).join(
        v_ude600003t,
        v_ude600003t.VN_KEY == v_ude620002t.VN_KEY,
        "left"
    ).join(
        v_ude870021,
        v_ude870021.NOP_ID == glife_basis.VORGANG_ID,
        "left"
    )

    return result

# Usage
glife_basis_plus = create_glife_basis_plus(spark, "your_db_name")