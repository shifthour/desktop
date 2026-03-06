# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container : CustSvcTaskLtrPK
Folder           : Shared Containers/PrimaryKey
Description      : 
* VC LOGS *
^1_1 10/21/08 10:37:16 Batch  14905_38245 PROMOTE bckcetl ids20 dsadm rc for brent 
^1_1 10/21/08 10:33:47 Batch  14905_38033 INIT bckcett testIDS dsadm rc for brent
^1_1 10/20/08 12:44:46 Batch  14904_45890 PROMOTE bckcett testIDS u08717 Brent
^1_1 10/20/08 12:41:40 Batch  14904_45702 INIT bckcett devlIDS u08717 Brent
^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
^1_2 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
^1_2 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
^1_1 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent

Copyright 2008 Blue Cross/Blue Shield of Kansas City

CALLED BY:  FctsCustSvcTaskLtrExtr

PROCESSING:   Assign primary surrogate key to input records

MODIFICATIONS:
Developer      Date         Project/Altiris #   Change Description
-------------- ------------ ------------------ ---------------------------------
Brent Leland   2008-03-04   3567 Primary Key    Original Programming.
                                            Changed counter IDS_SK to CUST_SVC_TASK_LTR_SK
"""

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_CustSvcTaskLtrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the CustSvcTaskLtrPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link “AllCol”.
    df_Transform : DataFrame
        Container input link “Transform”.
    params : dict
        Runtime parameters and configuration objects.

    Returns
    -------
    DataFrame
        Container output link “Key”.
    """

    # --------------------------------------------------
    # unpack parameters (exactly once)
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # --------------------------------------------------
    # stage : hf_cust_svc_task_ltr_allcol  (scenario-a hash file)
    # deduplicate on key columns and forward
    # --------------------------------------------------
    key_cols = [
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_LTR_STYLE_CD",
        "LTR_SEQ_NO",
        "LTR_DEST_ID"
    ]
    df_AllColOut = dedup_sort(
        df_AllCol,
        key_cols,
        [(c, "A") for c in key_cols]
    )

    # --------------------------------------------------
    # stage : K_CUST_SVC_TASK_LTR_TEMP  (database insert)
    # --------------------------------------------------
    temp_table = f"{IDSOwner}.K_CUST_SVC_TASK_LTR_TEMP"
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("overwrite")
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CUST_SVC_TASK_LTR_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # --------------------------------------------------
    # stage : K_CUST_SVC_TASK_LTR_TEMP (W_Extract query)
    # --------------------------------------------------
    extract_query = f"""
    SELECT k.CUST_SVC_TASK_LTR_SK,
           w.SRC_SYS_CD_SK,
           w.CUST_SVC_ID,
           w.TASK_SEQ_NO,
           w.CUST_SVC_TASK_LTR_STYLE_CD,
           w.LTR_SEQ_NO,
           w.LTR_DEST_ID,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CUST_SVC_TASK_LTR_TEMP w,
         {IDSOwner}.K_CUST_SVC_TASK_LTR k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.CUST_SVC_ID   = k.CUST_SVC_ID
      AND w.TASK_SEQ_NO   = k.TASK_SEQ_NO
      AND w.CUST_SVC_TASK_LTR_STYLE_CD = k.CUST_SVC_TASK_LTR_STYLE_CD
      AND w.LTR_SEQ_NO    = k.LTR_SEQ_NO
      AND w.LTR_DEST_ID   = k.LTR_DEST_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CUST_SVC_ID,
           w2.TASK_SEQ_NO,
           w2.CUST_SVC_TASK_LTR_STYLE_CD,
           w2.LTR_SEQ_NO,
           w2.LTR_DEST_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CUST_SVC_TASK_LTR_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CUST_SVC_TASK_LTR k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CUST_SVC_ID   = k2.CUST_SVC_ID
          AND w2.TASK_SEQ_NO   = k2.TASK_SEQ_NO
          AND w2.CUST_SVC_TASK_LTR_STYLE_CD = k2.CUST_SVC_TASK_LTR_STYLE_CD
          AND w2.LTR_SEQ_NO    = k2.LTR_SEQ_NO
          AND w2.LTR_DEST_ID   = k2.LTR_DEST_ID
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # stage : PrimaryKey  (transformer logic)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CUST_SVC_TASK_LTR_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "CUST_SVC_TASK_LTR_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("CUST_SVC_TASK_LTR_SK"))
        )
    )

    # surrogate-key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CUST_SVC_TASK_LTR_SK",
        <schema>,
        <secret_name>
    )

    # NewKeys (constraint svInstUpdt = 'I')
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_LTR_STYLE_CD",
            "LTR_SEQ_NO",
            "LTR_DEST_ID",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CUST_SVC_TASK_LTR_SK"
        )
    )

    # Keys (all records)
    df_Keys = (
        df_enriched
        .select(
            "CUST_SVC_TASK_LTR_SK",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_LTR_STYLE_CD",
            "LTR_SEQ_NO",
            "LTR_DEST_ID",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # updt (hash-file output)
    df_updt = (
        df_enriched
        .select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD_SK"),
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            F.col("CUST_SVC_TASK_LTR_STYLE_CD").alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
            "LTR_SEQ_NO",
            "LTR_DEST_ID",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CUST_SVC_TASK_LTR_SK"
        )
    )

    # --------------------------------------------------
    # stage : Merge
    # --------------------------------------------------
    join_cond = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.CUST_SVC_ID")   == F.col("k.CUST_SVC_ID")) &
        (F.col("all.TASK_SEQ_NO")   == F.col("k.TASK_SEQ_NO")) &
        (F.col("all.CUST_SVC_TASK_LTR_STYLE_CD") == F.col("k.CUST_SVC_TASK_LTR_STYLE_CD")) &
        (F.col("all.LTR_SEQ_NO")    == F.col("k.LTR_SEQ_NO")) &
        (F.col("all.LTR_DEST_ID")   == F.col("k.LTR_DEST_ID"))
    )

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), join_cond, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.CUST_SVC_TASK_LTR_SK"),
            F.col("all.CUST_SVC_ID"),
            F.col("all.TASK_SEQ_NO"),
            F.col("all.CUST_SVC_TASK_LTR_STYLE_CD"),
            F.col("all.LTR_SEQ_NO"),
            F.col("all.LTR_DEST_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CRT_BY_USER_SK"),
            F.col("all.CUST_SVC_TASK_SK"),
            F.col("all.LAST_UPDT_USER_SK"),
            F.col("all.CUST_SVC_TASK_LTR_REPRT_STTUS"),
            F.col("all.CUST_SVC_TASK_LTR_TYP_CD_SK"),
            F.col("all.CRT_DT_SK"),
            F.col("all.LAST_UPDT_DT_SK"),
            F.col("all.MAILED_DT_SK"),
            F.col("all.PRTED_DT_SK"),
            F.col("all.RQST_DT_SK"),
            F.col("all.SUBMT_DT_SK"),
            F.col("all.MAILED_IN"),
            F.col("all.PRTED_IN"),
            F.col("all.RQST_IN"),
            F.col("all.SUBMT_IN"),
            F.col("all.RECPNT_NM"),
            F.col("all.RECPNT_ADDR_LN_1"),
            F.col("all.RECPNT_ADDR_LN_2"),
            F.col("all.RECPNT_ADDR_LN_3"),
            F.col("all.RECPNT_CITY_NM"),
            F.col("all.CS_TASK_LTR_RECPNT_ST_CD_SK"),
            F.col("all.RECPNT_POSTAL_CD"),
            F.col("all.RECPNT_CNTY_NM"),
            F.col("all.RECPNT_PHN_NO"),
            F.col("all.RECPNT_PHN_NO_EXT"),
            F.col("all.RECPNT_FAX_NO"),
            F.col("all.REF_NM"),
            F.col("all.REF_ADDR_LN_1"),
            F.col("all.REF_ADDR_LN_2"),
            F.col("all.REF_ADDR_LN_3"),
            F.col("all.REF_CITY_NM"),
            F.col("all.CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
            F.col("all.REF_POSTAL_CD"),
            F.col("all.REF_CNTY_NM"),
            F.col("all.REF_PHN_NO"),
            F.col("all.REF_PHN_NO_EXT"),
            F.col("all.REF_FAX_NO"),
            F.col("all.SEND_NM"),
            F.col("all.SEND_ADDR_LN_1"),
            F.col("all.SEND_ADDR_LN_2"),
            F.col("all.SEND_ADDR_LN_3"),
            F.col("all.SEND_CITY_NM"),
            F.col("all.CUST_SVC_TASK_LTR_SEND_ST_CD_S"),
            F.col("all.SEND_POSTAL_CD"),
            F.col("all.SEND_CNTY_NM"),
            F.col("all.SEND_PHN_NO"),
            F.col("all.SEND_PHN_NO_EXT"),
            F.col("all.SEND_FAX_NO"),
            F.col("all.EXPL_TX"),
            F.col("all.LTR_TX_1"),
            F.col("all.LTR_TX_2"),
            F.col("all.LTR_TX_3"),
            F.col("all.LTR_TX_4"),
            F.col("all.MSG_TX")
        )
    )

    # --------------------------------------------------
    # outputs : sequential file & parquet hash-file
    # --------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_CUST_SVC_TASK_LTR.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    parquet_path = f"{adls_path}/CustSvcTaskLtrPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # return container output(s)
    # --------------------------------------------------
    return df_Key

# COMMAND ----------