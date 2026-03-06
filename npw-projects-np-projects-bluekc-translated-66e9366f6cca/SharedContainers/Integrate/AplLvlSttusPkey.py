# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
AplLvlSttusPkey
***************************************************************************************************************************************************************
COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   

PROCESSING:    Used in Evicore, NDBH,Telligen and Facets process




MODIFICATIONS:
Developer           Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      -------------------------
Ravi Singh        10/04/2018                Initial program                                                              MTM-5841       IntergrateDev2	    Abhiram Dasarathy    2018-10-30
"""
# This container is used in:
# Evicore,NDBH and Telligen Appeals
# ......Extr
#
# These programs need to be re-compiled when logic changes
#
# Assign primary surrogate key
#
# This container is used in:
# EvicoreIdsAplLvlExtr
# FctsAplLvlExtr
#
# These programs need to be re-compiled when logic changes
#
# Assign primary surrogate key
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def run_AplLvlSttusPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    # -------------------------------------------------------------
    # Unpack parameters (each exactly once)
    # -------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    # -------------------------------------------------------------
    # Dummy-table lookup (replacement for hash file hf_apl_lvl_sttus)
    # -------------------------------------------------------------
    extract_query = f"""
    SELECT
        SRC_SYS_CD               AS SRC_SYS_CD_lkp,
        APL_ID                   AS APL_ID_lkp,
        APL_LVL_SEQ_NO           AS APL_LVL_SEQ_NO_lkp,
        SEQ_NO                   AS SEQ_NO_lkp,
        CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp,
        APL_LVL_STTUS_SK         AS APL_LVL_STTUS_SK_lkp
    FROM {IDSOwner}.dummy_hf_apl_lvl_sttus
    """
    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # -------------------------------------------------------------
    # Join incoming stream with lookup
    # -------------------------------------------------------------
    join_expr = (
        (col("t.SRC_SYS_CD")      == col("lk.SRC_SYS_CD_lkp")) &
        (col("t.APL_ID")          == col("lk.APL_ID_lkp")) &
        (col("t.APL_LVL_SEQ_NO")  == col("lk.APL_LVL_SEQ_NO_lkp")) &
        (col("t.SEQ_NO")          == col("lk.SEQ_NO_lkp"))
    )
    df_enriched = (
        df_Transform.alias("t")
        .join(df_lkup.alias("lk"), join_expr, "left")
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("lk.APL_LVL_STTUS_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("lk.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )
    # -------------------------------------------------------------
    # Surrogate-key generation (replaces KeyMgtGetNextValueConcurrent)
    # -------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"APL_LVL_STTUS_SK",<schema>,<secret_name>)
    df_enriched = df_enriched.withColumn("SK", col("APL_LVL_STTUS_SK"))
    # -------------------------------------------------------------
    # Build the 'updt' stream (rows missing in lookup)
    # -------------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("lk.APL_LVL_STTUS_SK_lkp").isNull())
        .select(
            col("SRC_SYS_CD"),
            col("APL_ID"),
            col("APL_LVL_SEQ_NO"),
            col("SEQ_NO"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("APL_LVL_STTUS_SK")
        )
    )
    (
        df_updt.write
            .format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", f"{IDSOwner}.dummy_hf_apl_lvl_sttus")
            .mode("append")
            .save()
    )
    # -------------------------------------------------------------
    # Build the 'Key' output stream
    # -------------------------------------------------------------
    df_key = (
        df_enriched.select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            col("SK").alias("APL_LVL_STTUS_SK"),
            "APL_ID",
            "APL_LVL_SEQ_NO",
            "SEQ_NO",
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "APL_LVL_SK",
            "APST_USID",
            "APST_USID_ROUTE",
            "APL_LVL_STTUS_CD",
            "APL_LVL_STTUS_RSN_CD",
            "STTUS_DTM"
        )
    )
    # -------------------------------------------------------------
    # Return the container output link(s)
    # -------------------------------------------------------------
    return df_key