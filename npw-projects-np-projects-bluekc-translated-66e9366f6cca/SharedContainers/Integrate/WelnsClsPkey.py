# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------

# Databricks notebook source
# MAGIC %run ./Routine_Functions

# COMMAND ----------

# Databricks notebook source
"""
Shared Container: WelnsClsPkey
Description: Takes the sorted file and applies the primary key. Prepares the file for the foreign-key lookup process that follows.

* VC LOGS *
^1_1 11/19/09 14:13:03 Batch  15299_51232 PROMOTE bckcett:31540 testIDS u150906 4319-WelnsCls_Judy_testIDS              Maddy
^1_1 11/18/09 13:12:59 Batch  15298_47659 INIT bckcett:31540 devlIDS u150906 4319-WellnsCls_Judy_devlIDS           Maddy
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_1 12/20/07 09:28:41 Batch  14599_34126 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 12/20/07 09:25:27 Batch  14599_33930 INIT bckcett testIDS30 dsadm bls for sa
^1_1 12/17/07 19:49:17 Batch  14596_71364 PROMOTE bckcett testIDS30 u03651 Steph for Sharon
^1_1 12/17/07 19:43:25 Batch  14596_71010 INIT bckcett devlIDS30 u03651 steffy
^1_1 02/26/07 16:10:51 Batch  14302_58257 INIT bckcett devlIDS30 u150129 Laurel for Steph
^1_2 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
^1_1 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
^1_2 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
^1_1 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
^1_2 10/17/05 14:21:21 Batch  13805_51684 INIT bckcett devlIDS30 u03651 steffy
^1_1 08/03/05 09:15:00 Batch  13730_33303 INIT bckcett devlIDS30 u03651 steffy

Container is utilised by: WellLifeWelnsClsExtr
"""

# COMMAND ----------

from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_WelnsClsPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Applies primary-key generation and lookup logic equivalent to the DataStage
    shared container WelnsClsPkey.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming data stream corresponding to the 'Transform' link.
    params : dict
        Runtime parameters, JDBC configurations, and secrets.

    Returns
    -------
    DataFrame
        DataFrame corresponding to the 'Load' output link.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    ids_secret_name     = params["ids_secret_name"]

    # ------------------------------------------------------------------
    # Scenario-b hashed file → replace with dummy table via JDBC
    # ------------------------------------------------------------------
    dummy_table_name = "dummy_hf_welns_cls"

    extract_query = f"""
        SELECT
            SRC_SYS_CD                AS SRC_SYS_CD_lkp,
            MBR_UNIQ_KEY              AS MBR_UNIQ_KEY_lkp,
            WELNS_CLS_NM              AS WELNS_CLS_NM_lkp,
            STRT_DT_SK                AS STRT_DT_SK_lkp,
            CRT_RUN_CYC_EXCTN_SK      AS CRT_RUN_CYC_EXCTN_SK_lkp,
            WELNS_CLS_SK              AS WELNS_CLS_SK_lkp
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
    join_cond = (
        (df_Transform["SRC_SYS_CD"]   == df_lkup["SRC_SYS_CD_lkp"]) &
        (df_Transform["MBR_UNIQ_KEY"] == df_lkup["MBR_UNIQ_KEY_lkp"]) &
        (df_Transform["WELNS_CLS_NM"] == df_lkup["WELNS_CLS_NM_lkp"]) &
        (df_Transform["STRT_DT"]      == df_lkup["STRT_DT_SK_lkp"])
    )

    df_join = (
        df_Transform.alias("t")
        .join(df_lkup.alias("lk"), join_cond, "left")
    )

    # ------------------------------------------------------------------
    # Derive columns SK and NewCrtRunCycExtcnSk
    # ------------------------------------------------------------------
    df_enriched = (
        df_join
        .withColumn(
            "SK",
            F.when(
                F.col("SRC_SYS_CD_lkp").isNull(),
                F.lit(None)
            ).otherwise(F.col("WELNS_CLS_SK_lkp"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("SRC_SYS_CD_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # ------------------------------------------------------------------
    # Surrogate key generation for new rows
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Prepare UPDATE/INSERT set for dummy table (where NOTFOUND)
    # ------------------------------------------------------------------
    df_updt = df_enriched.filter(F.col("SRC_SYS_CD_lkp").isNull())

    df_updt_write = (
        df_updt.select(
            "SRC_SYS_CD",
            "MBR_UNIQ_KEY",
            "WELNS_CLS_NM",
            F.col("STRT_DT").alias("STRT_DT_SK"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("WELNS_CLS_SK")
        )
    )

    (
        df_updt_write.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", dummy_table_name)
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Build output 'Load' link DataFrame
    # ------------------------------------------------------------------
    df_load = (
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
            F.col("SK").alias("WELNS_CLS_SK"),
            "MBR_UNIQ_KEY",
            "WELNS_CLS_NM",
            "STRT_DT",
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "GRP_ID",
            "END_DT",
            "GRAD_IN",
            "DELIVERY_METHOD",
            "CLASS_TYPE",
            "WELNS_CLS_TYP_CD",
            "END_WT_NO",
            "STRT_WT_NO",
            "TOT_SESS_AVLBL_CT",
            "TOT_SESS_CMPL_CT",
            "TOT_WT_LOSS_CT",
            "WELNS_CLS_STTUS_CD",
            "WELNS_CLS_STTUS_RSN_CD",
            "WT_LOSS_PCT"
        )
    )

    return df_load