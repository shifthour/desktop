# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName       : AplActvtyPkey
JobType       : Server Job
JobCategory   : DS_Integrate
FolderPath    : Shared Containers
Description   : ***************************************************************************************************************************************************************
COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   

PROCESSING:    Used in Evicore, NDBH,Telligen and Facets process
      



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      -------------------------
Ravi Singh        10/04/2018                Initial program                                                              MTM-5841       Intergrate Dev2	   Abhiram Dasarathy     2018-10-30
"""
# This container is used in:
# Evicore,NDBH and Telligen Appeals
# ......Extr
#
# These programs need to be re-compiled when logic changes
# Assign primary surrogate key
# This container is used in:
# EvicoreIdsAplActvtyExtr
# FctsAplActvtyExtr
#
# These programs need to be re-compiled when logic changes

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


def run_AplActvtyPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Shared-container logic replicated in PySpark.
    Parameters
    ----------
    df_Transform : DataFrame
        Incoming stream corresponding to DataStage link "Transform".
    params : dict
        Runtime parameters & secrets supplied by the calling notebook.

    Returns
    -------
    DataFrame
        Outgoing stream corresponding to DataStage link "Key".
    """

    # -----------------------------------------------------------
    # Unpack required parameters (exactly once)
    # -----------------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    jdbc_url          = params["ids_jdbc_url"]
    jdbc_props        = params["ids_jdbc_props"]
    IDSOwner          = params.get("IDSOwner", "")
    ids_secret_name   = params.get("ids_secret_name", "")

    # -----------------------------------------------------------
    # Read dummy table replacing hash-file hf_apl_actvty
    # -----------------------------------------------------------
    extract_query = """
        SELECT
            SRC_SYS_CD                 AS SRC_SYS_CD_lkp,
            APL_ID                     AS APL_ID_lkp,
            SEQ_NO                     AS SEQ_NO_lkp,
            CRT_RUN_CYC_EXCTN_SK       AS CRT_RUN_CYC_EXCTN_SK_lkp,
            APL_ACTVTY_SK              AS APL_ACTVTY_SK_lkp
        FROM dummy_hf_apl_actvty
    """
    df_hf_apl_actvty = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # -----------------------------------------------------------
    # Perform lookup join
    # -----------------------------------------------------------
    df_joined = (
        df_Transform.alias("t")
        .join(
            df_hf_apl_actvty.alias("l"),
            [
                col("t.SRC_SYS_CD") == col("l.SRC_SYS_CD_lkp"),
                col("t.APL_ID")     == col("l.APL_ID_lkp"),
                col("t.SEQ_NO")     == col("l.SEQ_NO_lkp")
            ],
            "left"
        )
    )

    # -----------------------------------------------------------
    # Derive columns
    # -----------------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn("SK", col("l.APL_ACTVTY_SK_lkp"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("l.APL_ACTVTY_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("l.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # -----------------------------------------------------------
    # Surrogate key generation (replaces KeyMgtGetNextValueConcurrent)
    # -----------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SK',<schema>,<secret_name>)

    # -----------------------------------------------------------
    # Build 'updt' DataFrame (rows where lookup did not find a match)
    # -----------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("l.APL_ACTVTY_SK_lkp").isNull())
        .select(
            col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("t.APL_ID").alias("APL_ID"),
            col("t.SEQ_NO").alias("SEQ_NO"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("APL_ACTVTY_SK")
        )
    )

    # -----------------------------------------------------------
    # Write updates back to dummy table
    # -----------------------------------------------------------
    (
        df_updt.write.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", "dummy_hf_apl_actvty")
        .mode("append")
        .save()
    )

    # -----------------------------------------------------------
    # Build 'Key' output DataFrame
    # -----------------------------------------------------------
    df_key = (
        df_enriched.select(
            col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("t.DISCARD_IN").alias("DISCARD_IN"),
            col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("t.ERR_CT").alias("ERR_CT"),
            col("t.RECYCLE_CT").alias("RECYCLE_CT"),
            col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("SK").alias("APL_ACTVTY_SK"),
            col("t.APL_ID").alias("APL_ID"),
            col("t.SEQ_NO").alias("SEQ_NO"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("t.APL_SK").alias("APL_SK"),
            col("t.APL_RVWR_SK").alias("APL_RVWR_SK"),
            col("t.CRT_USER_SK").alias("CRT_USER_SK"),
            col("t.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
            col("t.APL_ACTVTY_METH_CD_SK").alias("APL_ACTVTY_METH_CD_SK"),
            col("t.APL_ACTVTY_TYP_CD_SK").alias("APL_ACTVTY_TYP_CD_SK"),
            col("t.ACTVTY_DT_SK").alias("ACTVTY_DT_SK"),
            col("t.CRT_DTM").alias("CRT_DTM"),
            col("t.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
            col("t.APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
            col("t.ACTVTY_SUM").alias("ACTVTY_SUM"),
            col("t.APL_RVWR_ID").alias("APL_RVWR_ID")
        )
    )

    return df_key