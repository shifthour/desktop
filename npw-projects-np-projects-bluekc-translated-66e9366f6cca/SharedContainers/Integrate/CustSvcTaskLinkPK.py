
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
CustSvcTaskLinkPK – Shared Container
------------------------------------
* VC LOGS *
^1_1 10/21/08 10:37:16 Batch  14905_38245 PROMOTE bckcetl ids20 dsadm rc for brent 
^1_1 10/21/08 10:33:47 Batch  14905_38033 INIT bckcett testIDS dsadm rc for brent
^1_1 10/20/08 12:44:46 Batch  14904_45890 PROMOTE bckcett testIDS u08717 Brent
^1_1 10/20/08 12:41:40 Batch  14904_45702 INIT bckcett devlIDS u08717 Brent
^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
^1_2 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
^1_2 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
^1_1 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent

CALLED BY:  FctsCustSvcTaskLinkExtr, NascoCustSvcTaskLinkExtr

PROCESSING: Assign primary surrogate key to input records

MODIFICATIONS:
Developer        Date        Project/Altiris #   Change Description
---------------  ----------  ------------------  -----------------------------------------------------------
Brent Leland     2008-03-04  3567 Primary Key    Original Programming. Changed counter IDS_SK to CUST_SVC_TASK_LINK_SK
"""

# join primary key info with table info
# update primary key table (K_CUST_SVC_TASK_LINK) with new keys created today
# Assign primary surrogate key
# primary key hash file only contains current run keys
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
# Load IDS temp. table
# Used by FctsCustSvcTaskLinkExtr & NascoCustSvcTaskLinkExtr
# IDS Primary Key Container for Customer Service Task Link
# Hash file cleared in calling job

from pyspark.sql import DataFrame, functions as F


def run_CustSvcTaskLinkPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the CustSvcTaskLinkPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Records flowing through link ‘AllCol’.
    df_Transform : DataFrame
        Records flowing through link ‘Transform’.
    params : dict
        All runtime parameters and pre-resolved configuration items.

    Returns
    -------
    DataFrame
        Records flowing out of link ‘Key’.
    """

    # ------------------------------------------------------
    # Parameter unpacking (each key exactly once)
    # ------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    RunID               = params.get("RunID")
    CurrDate            = params.get("CurrDate")
    SrcSysCdSk          = params.get("SrcSysCdSk")
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]

    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------
    # Stage: hf_cust_svc_task_link_allcol  (scenario a)
    # Replace hashed-file with dedup logic
    # ------------------------------------------------------
    partition_cols = [
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_LINK_TYP_CD",
        "LINK_RCRD_ID"
    ]
    sort_cols = []  # no specific ordering required
    df_AllColDedup = dedup_sort(df_AllCol, partition_cols, sort_cols)

    # ------------------------------------------------------
    # Stage: K_CUST_SVC_TASK_LINK_TEMP  (DB2/Azure-SQL)
    # Insert df_Transform into temp table, runstats, then extract
    # ------------------------------------------------------
    truncate_sql = f"TRUNCATE TABLE {IDSOwner}.K_CUST_SVC_TASK_LINK_TEMP"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CUST_SVC_TASK_LINK_TEMP")
        .mode("append")
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CUST_SVC_TASK_LINK_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
        SELECT  k.CUST_SVC_TASK_LINK_SK,
                w.SRC_SYS_CD_SK,
                w.CUST_SVC_ID,
                w.TASK_SEQ_NO,
                w.CUST_SVC_TASK_LINK_TYP_CD,
                w.LINK_RCRD_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_CUST_SVC_TASK_LINK_TEMP w
        JOIN    {IDSOwner}.K_CUST_SVC_TASK_LINK k
              ON w.SRC_SYS_CD_SK           = k.SRC_SYS_CD_SK
             AND w.CUST_SVC_ID             = k.CUST_SVC_ID
             AND w.TASK_SEQ_NO             = k.TASK_SEQ_NO
             AND w.CUST_SVC_TASK_LINK_TYP_CD = k.CUST_SVC_TASK_LINK_TYP_CD
             AND w.LINK_RCRD_ID            = k.LINK_RCRD_ID
        UNION ALL
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.CUST_SVC_ID,
                w2.TASK_SEQ_NO,
                w2.CUST_SVC_TASK_LINK_TYP_CD,
                w2.LINK_RCRD_ID,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_CUST_SVC_TASK_LINK_TEMP w2
        WHERE NOT EXISTS (
                SELECT 1
                FROM   {IDSOwner}.K_CUST_SVC_TASK_LINK k2
                WHERE  w2.SRC_SYS_CD_SK           = k2.SRC_SYS_CD_SK
                  AND  w2.CUST_SVC_ID             = k2.CUST_SVC_ID
                  AND  w2.TASK_SEQ_NO             = k2.TASK_SEQ_NO
                  AND  w2.CUST_SVC_TASK_LINK_TYP_CD = k2.CUST_SVC_TASK_LINK_TYP_CD
                  AND  w2.LINK_RCRD_ID            = k2.LINK_RCRD_ID
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CUST_SVC_TASK_LINK_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "CUST_SVC_TASK_LINK_SK",
            F.when(F.col("CUST_SVC_TASK_LINK_SK") == F.lit(-1), F.lit(None)).otherwise(F.col("CUST_SVC_TASK_LINK_SK"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate Key Generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CUST_SVC_TASK_LINK_SK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------
    # Output link: NewKeys (only Inserts)
    # ------------------------------------------------------
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_LINK_TYP_CD",
            "LINK_RCRD_ID",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CUST_SVC_TASK_LINK_SK"
        )
    )

    # ------------------------------------------------------
    # Output link: Keys (all records)
    # ------------------------------------------------------
    df_Keys = (
        df_enriched.select(
            "CUST_SVC_TASK_LINK_SK",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_LINK_TYP_CD",
            "LINK_RCRD_ID",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------
    # Output link: updt (hashed-file replacement written as parquet)
    # ------------------------------------------------------
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_LINK_TYP_CD",
            "LINK_RCRD_ID",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CUST_SVC_TASK_LINK_SK"
        )
    )

    parquet_path_updt = f"{adls_path}/CustSvcTaskLinkPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------
    # Stage: Merge (Transformer)
    # ------------------------------------------------------
    join_condition = (
        (F.col("a.SRC_SYS_CD") == F.col("k.SRC_SYS_CD")) &
        (F.col("a.CUST_SVC_ID") == F.col("k.CUST_SVC_ID")) &
        (F.col("a.TASK_SEQ_NO") == F.col("k.TASK_SEQ_NO")) &
        (F.col("a.CUST_SVC_TASK_LINK_TYP_CD") == F.col("k.CUST_SVC_TASK_LINK_TYP_CD")) &
        (F.col("a.LINK_RCRD_ID") == F.col("k.LINK_RCRD_ID"))
    )

    df_Key = (
        df_AllColDedup.alias("a")
        .join(df_Keys.alias("k"), join_condition, "left")
        .select(
            F.col("a.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("a.DISCARD_IN"),
            F.col("a.PASS_THRU_IN"),
            F.col("a.FIRST_RECYC_DT"),
            F.col("a.ERR_CT"),
            F.col("a.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("a.PRI_KEY_STRING"),
            F.col("k.CUST_SVC_TASK_LINK_SK"),
            F.col("a.CUST_SVC_ID"),
            F.col("a.TASK_SEQ_NO"),
            F.col("a.CUST_SVC_TASK_LINK_TYP_CD"),
            F.col("a.LINK_RCRD_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("a.APL_ID"),
            F.col("a.CLM_ID"),
            F.col("a.CUST_SVC_TASK_ID"),
            F.col("a.LAST_UPDT_USER_ID"),
            F.col("a.REL_CUST_SVC_ID"),
            F.col("a.UM_ID"),
            F.col("a.CUST_SVC_TASK_LINK_RSN_CD"),
            F.col("a.LAST_UPDT_DTM"),
            F.col("a.LINK_DESC")
        )
    )

    # ------------------------------------------------------
    # Stage: K_CUST_SVC_TASK_LINK (Sequential File write)
    # ------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_CUST_SVC_TASK_LINK.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------
    # Return container output
    # ------------------------------------------------------
    return df_Key
