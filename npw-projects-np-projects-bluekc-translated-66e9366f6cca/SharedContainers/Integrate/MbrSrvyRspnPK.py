
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
JobName       : MbrSrvyRspnPK
JobType       : Server Job
FolderPath    : Shared Containers/PrimaryKey
Description   : Copyright 2011 Blue Cross/Blue Shield of Kansas City

CALLED BY:  IdsMbrSrvyRspnExtr, IdsWMbrRspnHraExtr

PROCESSING: Primary Key Shared Container for IDS table MBR_SRVY_RSPN.

MODIFICATIONS:
Developer        Date          Project/Altiris #  Change Description
---------------  ------------  -----------------  -------------------------------------------------------------
Kalyan Neelam    2011-05-13    4673               Initial Programming
Raja Gummadi     2013-11-20    5063               Added 5 new columns to W_MBR_RSPN Table
                                                   MBR_SRVY_RSPN_UPDT_SRC_CD, CMPLTN_DT_SK, STRT_DT_SK,
                                                   UPDT_DT_SK, HRA_ID

Annotations:
1) This container is used in:
   IdsMbrSrvyRspnExtr
   IdsWMbrRspnHraExtr
   These programs need to be re-compiled when logic changes
2) MBR_SRVY_RSPN Primary Key Shared Container
"""

# COMMAND ----------

def run_MbrSrvyRspnPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container logic translated from DataStage container MbrSrvyRspnPK.
    
    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame corresponding to link 'Transform'.
    params : dict
        Runtime parameters dictionary.
    
    Returns
    -------
    DataFrame
        DataFrame corresponding to output link 'Key'.
    """

    # --------------------------------------------------
    # Unpack required parameters exactly once
    # --------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    ids_secret_name    = params["ids_secret_name"]

    # --------------------------------------------------
    # Read dummy table replacing hashed-file hf_mbr_srvy_rspn
    # --------------------------------------------------
    dummy_table = "dummy_hf_mbr_srvy_rspn"
    extract_query = (
        "SELECT "
        "SRC_SYS_CD           AS SRC_SYS_CD_lkp, "
        "MBR_SRVY_TYP_CD      AS MBR_SRVY_TYP_CD_lkp, "
        "MBR_UNIQ_KEY         AS MBR_UNIQ_KEY_lkp, "
        "QSTN_CD_TX           AS QSTN_CD_TX_lkp, "
        "RSPN_DT_SK           AS RSPN_DT_SK_lkp, "
        "SEQ_NO               AS SEQ_NO_lkp, "
        "CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK_lkp, "
        "MBR_SRVY_RSPN_SK     AS MBR_SRVY_RSPN_SK_lkp "
        f"FROM {dummy_table}"
    )

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Join input with lookup
    # --------------------------------------------------
    join_cond = (
        (df_Transform["SRC_SYS_CD"]      == df_lkup["SRC_SYS_CD_lkp"]) &
        (df_Transform["MBR_SRVY_TYP_CD"] == df_lkup["MBR_SRVY_TYP_CD_lkp"]) &
        (df_Transform["MBR_UNIQ_KEY"]    == df_lkup["MBR_UNIQ_KEY_lkp"]) &
        (df_Transform["QSTN_CD_TX"]      == df_lkup["QSTN_CD_TX_lkp"]) &
        (df_Transform["RSPN_DT_SK"]      == df_lkup["RSPN_DT_SK_lkp"]) &
        (df_Transform["SEQ_NO"]          == df_lkup["SEQ_NO_lkp"])
    )

    df_enriched = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_cond, "left")
        .withColumn(
            "MBR_SRVY_RSPN_SK",
            F.col("MBR_SRVY_RSPN_SK_lkp")
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("MBR_SRVY_RSPN_SK_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn(
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle)
        )
    )

    # --------------------------------------------------
    # Surrogate key generation
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)

    # --------------------------------------------------
    # Prepare output link 'Key'
    # --------------------------------------------------
    select_cols_key = [
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "MBR_SRVY_RSPN_SK",
        "SRC_SYS_CD_SK",
        "MBR_SRVY_TYP_CD",
        "MBR_UNIQ_KEY",
        "QSTN_CD_TX",
        "RSPN_DT_SK",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_ID",
        "SYS_CRT_DT_SK",
        "RCRD_CT",
        "RSPN_ANSWER_TX",
        "MBR_SRVY_RSPN_UPDT_SRC_CD",
        "CMPLTN_DT_SK",
        "STRT_DT_SK",
        "UPDT_DT_SK",
        "HRA_ID"
    ]

    df_key = df_enriched.select(*select_cols_key)

    # --------------------------------------------------
    # Prepare update DataFrame for dummy table
    # --------------------------------------------------
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "MBR_SRVY_TYP_CD",
        "MBR_UNIQ_KEY",
        "QSTN_CD_TX",
        "RSPN_DT_SK",
        "SEQ_NO",
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        "MBR_SRVY_RSPN_SK"
    )

    # --------------------------------------------------
    # Write back to dummy table
    # --------------------------------------------------
    (
        df_updt.write.mode("append").format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", dummy_table)
            .save()
    )

    # --------------------------------------------------
    # Return output
    # --------------------------------------------------
    return df_key
