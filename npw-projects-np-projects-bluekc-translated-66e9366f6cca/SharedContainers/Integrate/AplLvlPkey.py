# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
***************************************************************************************************************************************************************
COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   
PROCESSING:    Used in Evicore, NDBH,Telligen and Facets process



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      -------------------------
Ravi Singh        10/04/2018                Initial program                                                              MTM-5841       IntergrateDev2	  Abhiram Dasarathy	      2018-10-30
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
# COMMAND ----------
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
def run_AplLvlPkey(
    df_Transform: DataFrame,
    params: Dict
) -> DataFrame:
    """
    Shared-container logic translated from DataStage job 'AplLvlPkey'.
    
    Parameters
    ----------
    df_Transform : pyspark.sql.DataFrame
        Incoming stream corresponding to the 'Transform' input link.
    params : dict
        Runtime parameters passed from the calling job/notebook.
    
    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame corresponding to the 'Key' output link.
    """
    # ----- Unpack parameters -----
    CurrRunCycle      = params["CurrRunCycle"]
    jdbc_url          = params["jdbc_url"]
    jdbc_props        = params["jdbc_props"]
    # --------------------------------------------------
    # ----- Read dummy table replacing hashed file -----
    extract_query = """
        SELECT
            SRC_SYS_CD               AS SRC_SYS_CD_lkp,
            APL_ID                   AS APL_ID_lkp,
            SEQ_NO                   AS SEQ_NO_lkp,
            CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp,
            APL_LVL_SK               AS APL_LVL_SK_lkp
        FROM dummy_hf_apl_lvl
    """
    df_lkup = (
        spark.read.format("jdbc")
             .option("url", jdbc_url)
             .options(**jdbc_props)
             .option("query", extract_query)
             .load()
    )
    # --------------------------------------------------
    # ----- Join Transform input with lookup ----------
    join_expr = (
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_lkp")) &
        (F.col("Transform.APL_ID")     == F.col("lkup.APL_ID_lkp"))    &
        (F.col("Transform.SEQ_NO")     == F.col("lkup.SEQ_NO_lkp"))
    )
    df_joined = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_expr, "left")
    )
    # --------------------------------------------------
    # ----- Derive columns ----------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "SK",
            F.col("lkup.APL_LVL_SK_lkp")
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("lkup.APL_LVL_SK_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )
    # ----- Surrogate Key Generation ------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)
    # --------------------------------------------------
    # ----- Prepare 'updt' DataFrame ------------------
    df_updt = (
        df_enriched
        .filter(F.col("lkup.APL_LVL_SK_lkp").isNull())
        .select(
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.APL_ID").alias("APL_ID"),
            F.col("Transform.SEQ_NO").alias("SEQ_NO"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("APL_LVL_SK")
        )
    )
    # ----- Write to dummy table ----------------------
    (
        df_updt.write
        .format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", "dummy_hf_apl_lvl")
        .mode("append")
        .save()
    )
    # --------------------------------------------------
    # ----- Prepare 'Key' output ----------------------
    df_key = (
        df_enriched
        .select(
            "Transform.JOB_EXCTN_RCRD_ERR_SK",
            "Transform.INSRT_UPDT_CD",
            "Transform.DISCARD_IN",
            "Transform.PASS_THRU_IN",
            "Transform.FIRST_RECYC_DT",
            "Transform.ERR_CT",
            "Transform.RECYCLE_CT",
            "Transform.SRC_SYS_CD",
            "Transform.PRI_KEY_STRING",
            F.col("SK").alias("APL_LVL_SK"),
            "Transform.APL_ID",
            "Transform.SEQ_NO",
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "Transform.APL_SK",
            "Transform.CRT_USER_SK",
            "Transform.LAST_UPDT_USER_SK",
            "Transform.PRI_USER_SK",
            "Transform.SEC_USER_SK",
            "Transform.TRTY_USER_SK",
            "Transform.APL_LVL_CD_SK",
            "Transform.APL_LVL_CUR_STTUS_CD_SK",
            "Transform.APL_LVL_DCSN_CD_SK",
            "Transform.APL_LVL_DCSN_RSN_CD_SK",
            "Transform.APL_LVL_DSPT_RSLTN_TYP_CD_SK",
            "Transform.APL_LVL_INITN_METH_CD_SK",
            "Transform.APL_LVL_LATE_DCSN_RSN_CD_SK",
            "Transform.APL_LVL_NTFCTN_CAT_CD_SK",
            "Transform.APL_LVL_NTFCTN_METH_CD_SK",
            "Transform.EXPDTD_IN",
            "Transform.HRNG_IN",
            "Transform.INITN_DT_SK",
            "Transform.CRT_DTM",
            "Transform.CUR_STTUS_DTM",
            "Transform.DCSN_DT_SK",
            "Transform.HRNG_DT_SK",
            "Transform.LAST_UPDT_DTM",
            "Transform.NTFCTN_DT_SK",
            "Transform.CUR_STTUS_SEQ_NO",
            "Transform.LVL_DESC"
        )
    )
    # --------------------------------------------------
    return df_key
# COMMAND ----------