
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# +
"""
Job Name       : RiskCatPkey
Job Type       : Server Job
Job Category   : DS_Integrate
Folder Path    : Shared Containers

* VC LOGS *
^1_2 06/18/08 12:12:12 Batch  14780_43935 PROMOTE bckcetl ids20 dsadm BLS FOR SA
^1_2 06/18/08 11:57:41 Batch  14780_43064 INIT bckcett testIDS dsadm bls for sa
^1_2 06/17/08 13:24:03 Batch  14779_48263 PROMOTE bckcett testIDS u03651 Steffs promote
^1_2 06/17/08 12:43:46 Batch  14779_45832 INIT bckcett devlIDS u03651 steffy
^1_1 05/25/08 15:48:27 Batch  14756_56911 INIT bckcett devlIDS u03651 steffy


Primary Key logic for IDS Risk Cat Table.

Modifications                                                                              code walkthru     date
Bhoomi Dasari    05/2008     Created                                         Steph Goddrad   06/17/2008
"""

# Annotation converted from DataStage
# If primary key found, assign surragote key, otherwise get next key and update hash file.
# This container is used in:
# ImpProRiskCatExtr
# McSourceRiskCatExtr
#
# These programs need to be re-compiled when logic changes
# -

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

# COMMAND ----------


def run_RiskCatPkey(
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    PySpark translation of the IBM DataStage shared container ‘RiskCatPkey’.
    Container Inputs
      • Transform   → df_Transform
    Container Outputs
      • Key         → returned DataFrame
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    IDSOwner           = params.get("IDSOwner", "")
    ids_secret_name    = params.get("ids_secret_name", "")

    # ------------------------------------------------------------------
    # Scenario b  –  “Read-modify-write” hashed file → dummy table
    # ------------------------------------------------------------------
    dummy_table_name = (
        f"{IDSOwner}.dummy_hf_risk_cat" if IDSOwner else "dummy_hf_risk_cat"
    )

    extract_query = f"""
        SELECT
            SRC_SYS_CD          AS SRC_SYS_CD_lkp,
            RISK_CAT_ID         AS RISK_CAT_ID_lkp,
            CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK_lkp,
            RISK_CAT_SK         AS RISK_CAT_SK_lkp
        FROM {dummy_table_name}
    """

    df_lkup = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Join incoming stream with lookup table
    # ------------------------------------------------------------------
    df_enriched = (
        df_Transform.alias("Transform")
        .join(
            df_lkup.alias("lkup"),
            (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD_lkp"))
            & (col("Transform.RISK_CAT_ID") == col("lkup.RISK_CAT_ID_lkp")),
            "left",
        )
        .withColumn("SK", col("lkup.RISK_CAT_SK_lkp"))
    )

    # ------------------------------------------------------------------
    # Generate surrogate keys where needed
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "SK",
        <schema>,
        <secret_name>,
    )

    # ------------------------------------------------------------------
    # Compute remaining derived columns
    # ------------------------------------------------------------------
    df_enriched = df_enriched.withColumn(
        "NewCrtRunCycExtcnSk",
        when(col("lkup.RISK_CAT_SK_lkp").isNull(), lit(CurrRunCycle)).otherwise(
            col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp")
        ),
    )

    # ------------------------------------------------------------------
    # Prepare update rows (insert into dummy table)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched.filter(col("lkup.RISK_CAT_SK_lkp").isNull())
        .select(
            col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("Transform.RISK_CAT_ID").alias("RISK_CAT_ID"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("RISK_CAT_SK"),
        )
    )

    if df_updt.take(1):
        (
            df_updt.write.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", dummy_table_name)
            .mode("append")
            .save()
        )

    # ------------------------------------------------------------------
    # Build output link ‘Key’
    # ------------------------------------------------------------------
    df_key = df_enriched.select(
        col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("Transform.ERR_CT").alias("ERR_CT"),
        col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("SK").alias("RISK_CAT_SK"),
        col("Transform.RISK_CAT_ID").alias("RISK_CAT_ID"),
        col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Transform.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
        col("Transform.RISK_CAT_DESC").alias("RISK_CAT_DESC"),
        col("Transform.RISK_CAT_LABEL").alias("RISK_CAT_LABEL"),
        col("Transform.RISK_CAT_LONG_DESC").alias("RISK_CAT_LONG_DESC"),
        col("Transform.RISK_CAT_NM").alias("RISK_CAT_NM"),
    )

    return df_key
