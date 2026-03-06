# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName      : JrnLEntryTransPkey
JobType      : Server Job
JobCategory  : DS_Integrate
FolderPath   : Shared Containers

Description:
* VC LOGS *
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_2 03/21/07 14:59:13 Batch  14325_53956 INIT bckcetl ids20 dsadm dsadm
^1_1 03/07/07 15:57:28 Batch  14311_57454 INIT bckcetl ids20 dsadm dsadm
^1_2 07/29/05 15:21:56 Batch  13725_55324 INIT bckcetl ids20 dsadm Brent
^1_1 07/29/05 12:44:12 Batch  13725_45860 INIT bckcetl ids20 dsadm Brent
^1_1 07/28/05 10:08:43 Batch  13724_36528 PROMOTE bckcetl ids20 dsadm Gina Parr
^1_1 07/28/05 10:02:54 Batch  13724_36180 INIT bckcett testIDS30 dsadm Gina Parr
^1_1 07/25/05 08:29:54 Batch  13721_30598 INIT bckcett devlIDS30 u03651 steffy

Annotation:
This container is used in:
PSJrnlEntryCapExtr
PSJrnlEntryClmExtr
PSJrnlEntryComsnExtr
PSJrnlEntryDrugExtr
PSJrnlEntryIncmExtr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when


def run_JrnLEntryTransPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Shared-container logic translated from DataStage job 'JrnLEntryTransPkey'.
    Parameters
    ----------
    df_Transform : DataFrame
        Input stream corresponding to link 'Transform'.
    params : dict
        Runtime parameters supplied from the parent Databricks notebook.

    Returns
    -------
    DataFrame
        Output stream corresponding to link 'Key'.
    """

    # --------------------------------------------------
    # Unpack runtime parameters (each exactly once)
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    jdbc_url          = params["ids_jdbc_url"]
    jdbc_props        = params["ids_jdbc_props"]
    # --------------------------------------------------

    # --------------------------------------------------
    # Scenario-b hashed-file replacement : dummy table
    # --------------------------------------------------
    extract_query = """
        SELECT
            SRC_SYS_CD               AS SRC_SYS_CD_lkp,
            SRC_TRANS_CK             AS SRC_TRANS_CK_lkp,
            SRC_TRANS_TYP_CD         AS SRC_TRANS_TYP_CD_lkp,
            SNAP_ACT_DT              AS SNAP_ACT_DT_lkp,
            CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp,
            JRNL_ENTRY_TRANS_SK      AS JRNL_ENTRY_TRANS_SK_lkp
        FROM dummy_hf_jrnl_entry_trans
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Join Transform stream with lookup table
    # --------------------------------------------------
    join_expr = (
        (col("t.SRC_SYS_CD")      == col("lkup.SRC_SYS_CD_lkp")) &
        (col("t.SRC_TRANS_CK")    == col("lkup.SRC_TRANS_CK_lkp")) &
        (col("t.SRC_TRANS_TYP_CD")== col("lkup.SRC_TRANS_TYP_CD_lkp")) &
        (col("t.SNAP_ACT_DT")     == col("lkup.SNAP_ACT_DT_lkp"))
    )

    df_joined = (
        df_Transform.alias("t")
        .join(df_lkup.alias("lkup"), join_expr, "left")
        .withColumn("is_new", col("lkup.JRNL_ENTRY_TRANS_SK_lkp").isNull())
    )

    # --------------------------------------------------
    # Derive transformer columns
    # --------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn("JRNL_ENTRY_TRANS_SK", col("lkup.JRNL_ENTRY_TRANS_SK_lkp"))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("is_new"), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
        # Retain all passthrough columns from the primary stream
        .select(
            col("t.JOB_EXCTN_RCRD_ERR_SK"),
            col("t.INSRT_UPDT_CD"),
            col("t.DISCARD_IN"),
            col("t.PASS_THRU_IN"),
            col("t.FIRST_RECYC_DT"),
            col("t.ERR_CT"),
            col("t.RECYCLE_CT"),
            col("t.SRC_SYS_CD"),
            col("t.PRI_KEY_STRING"),
            col("JRNL_ENTRY_TRANS_SK"),
            col("t.SRC_TRANS_CK"),
            col("t.SRC_TRANS_TYP_CD"),
            col("t.SNAP_ACT_DT"),
            col("CRT_RUN_CYC_EXCTN_SK"),
            col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("t.TRANS_TBL_SK"),
            col("t.FNCL_LOB"),
            col("t.JRNL_ENTRY_TRANS_DR_CR_CD"),
            col("t.DIST_GL_IN"),
            col("t.JRNL_ENTRY_TRANS_AMT"),
            col("t.TRANS_LN_NO"),
            col("t.ACCT_NO"),
            col("t.AFFILIAT_NO"),
            col("t.APPL_JRNL_ID"),
            col("t.BUS_UNIT_GL_NO"),
            col("t.BUS_UNIT_NO"),
            col("t.CC_ID"),
            col("t.JRNL_LN_DESC"),
            col("t.JRNL_LN_REF_NO"),
            col("t.OPR_UNIT_NO"),
            col("t.SUB_ACCT_NO"),
            col("t.TRANS_ID"),
            col("t.PRCS_MAP_1_TX"),
            col("t.PRCS_MAP_2_TX"),
            col("t.PRCS_MAP_3_TX"),
            col("t.PRCS_MAP_4_TX"),
            col("t.PRCS_SUB_SRC_CD"),
            col("t.BILL_ENTY_CK"),
            col("t.GRP_NO"),
            col("t.PROD"),
            col("t.SUBGRP_ID"),
            col("t.CLCL_ID"),
            col("t.PDBL_ID"),
            col("is_new")
        )
    )

    # --------------------------------------------------
    # Surrogate-key population
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"JRNL_ENTRY_TRANS_SK",<schema>,<secret_name>)

    # --------------------------------------------------
    # Prepare rows that must be inserted into dummy table
    # --------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("is_new") == True)
        .select(
            col("SRC_SYS_CD"),
            col("SRC_TRANS_CK"),
            col("SRC_TRANS_TYP_CD"),
            col("SNAP_ACT_DT"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("JRNL_ENTRY_TRANS_SK")
        )
    )

    # --------------------------------------------------
    # Write updates back to dummy table
    # --------------------------------------------------
    (
        df_updt.write
        .format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", "dummy_hf_jrnl_entry_trans")
        .mode("append")
        .save()
    )

    # --------------------------------------------------
    # Finalize output
    # --------------------------------------------------
    df_output = df_enriched.drop("is_new")

    return df_output