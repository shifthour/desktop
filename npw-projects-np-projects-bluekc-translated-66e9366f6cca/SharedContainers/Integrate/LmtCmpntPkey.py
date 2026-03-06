
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
DataStage Shared-Container: LmtCmpntPkey
Job Type          : Server Job
Category          : DS_Integrate
Folder Path       : Shared Containers

* VC LOGS *
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/27/07 12:10:21 Batch  14606_43899 PROMOTE bckcetl ids20 dsadm bls for on
^1_1 12/27/07 11:57:06 Batch  14606_43029 INIT bckcett testIDS30 dsadm bls for on
^1_1 12/18/07 09:14:10 Batch  14597_33268 PROMOTE bckcett testIDS30 u03651 Steph for Ollie
^1_1 12/18/07 09:13:36 Batch  14597_33218 INIT bckcett devlIDS30 u03651 steffy
"""

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

spark: SparkSession = spark  # type: ignore

# COMMAND ----------

def run_LmtCmpntPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the DataStage shared container ‘LmtCmpntPkey’.

    Parameters
    ----------
    df_Transform : DataFrame
        Input DataFrame mapped to the ‘Transform’ link.
    params : dict
        Dictionary containing all runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        The DataFrame corresponding to the ‘Key’ output link.
    """

    # --------------------------------------------------
    # Parameter unpacking (exactly once)
    # --------------------------------------------------
    CurrRunCycle           = params["CurrRunCycle"]
    IDSOwner               = params["IDSOwner"]
    ids_secret_name        = params["ids_secret_name"]
    ids_jdbc_url           = params["ids_jdbc_url"]
    ids_jdbc_props         = params["ids_jdbc_props"]
    # --------------------------------------------------

    # --------------------------------------------------
    # Dummy-table lookup replacing hashed-file ‘hf_lmt_cmpnt’
    # --------------------------------------------------
    dummy_table = f"{IDSOwner}.dummy_hf_lmt_cmpnt"

    extract_query = f"""
        SELECT
            SRC_SYS_CD                 AS SRC_SYS_CD_lkp,
            LMT_CMPNT_ID               AS LMT_CMPNT_ID_lkp,
            ACCUM_NO                   AS ACCUM_NO_lkp,
            CRT_RUN_CYC_EXCTN_SK       AS CRT_RUN_CYC_EXCTN_SK_lkp,
            LMT_CMPNT_SK               AS LMT_CMPNT_SK_lkp
        FROM {dummy_table}
    """

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
    df_joined = (
        df_Transform.alias("tr")
        .join(
            df_lkup.alias("lkup"),
            (
                col("tr.SRC_SYS_CD")   == col("lkup.SRC_SYS_CD_lkp")
            ) & (
                col("tr.LMT_CMPNT_ID") == col("lkup.LMT_CMPNT_ID_lkp")
            ) & (
                col("tr.ACCUM_NO")     == col("lkup.ACCUM_NO_lkp")
            ),
            "left"
        )
    )

    # --------------------------------------------------
    # Derivations
    # --------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(
                col("lkup.LMT_CMPNT_SK_lkp").isNull(),
                lit(CurrRunCycle)
            ).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn(
            "LMT_CMPNT_SK",
            col("lkup.LMT_CMPNT_SK_lkp")
        )
    )

    # --------------------------------------------------
    # Surrogate-key generation
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"LMT_CMPNT_SK",<schema>,<secret_name>)

    # --------------------------------------------------
    # ‘updt’ DataFrame  (new rows only)
    # --------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("lkup.LMT_CMPNT_SK_lkp").isNull())
        .select(
            col("SRC_SYS_CD"),
            col("LMT_CMPNT_ID"),
            col("ACCUM_NO"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("LMT_CMPNT_SK")
        )
    )

    # Write back to dummy table (append new rows)
    (
        df_updt.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", dummy_table)
        .mode("append")
        .save()
    )

    # --------------------------------------------------
    # ‘Key’ output DataFrame
    # --------------------------------------------------
    df_key = (
        df_enriched
        .select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            "LMT_CMPNT_SK",
            "LMT_CMPNT_ID",
            "ACCUM_NO",
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "EXCD_SK",
            "LMT_CMPNT_ACCUM_CD_SK",
            "LMT_CMPNT_ACCUM_PERD_CD_SK",
            "LMT_CMPNT_INCLD_EXCL_CD_SK",
            "LMT_CMPNT_INCLD_EXCL_TYP_CD_SK",
            "LMT_CMPNT_LVL_CD_SK",
            "LMT_CMPNT_PRCS_CD_SK",
            "LMT_CMPNT_CAROVR_REINST_AMT",
            "LMT_CMPNT_MAX_ACCUM_AMT",
            "LMT_CMPNT_EXCL_DAYS_CT",
            "LMT_CMPNT_DESC",
            "EXCD_ID",
            "LTLT_IX_TYPE"
        )
    )

    # --------------------------------------------------
    # Return the output stream
    # --------------------------------------------------
    return df_key
