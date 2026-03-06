# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
IncomeInvcCmpntCtPK
Folder Path: Shared Containers/PrimaryKey
Copyright 2010 Blue Cross and Blue Shield of Kansas City
Called by: FctsIncomeInvcCmpntCtExtr
Processing:
    Creates primary key value for rows with new natural key values.
    Looks up existing primary key value for an existing natural key.
    The hf_invc_cmpnt_ct_allcol hashed file is cleared by the calling job.
Control Job Rerun Information:
    Previous Run Successful: Truncate t K_INVC_CMPNT_CT table
    Previous Run Aborted:    Restart, no other steps necessary
Modifications:
Developer        Date        Altiris#   Change Description            Reviewer  Reviewed
---------------  ----------  ---------  ----------------------------  --------  --------
Hugh Sisson      2010-09-26  3346       Original program

Annotations:
  • IDS Primary Key Container for Income Invoice Component Count
  • Used by FctsIncomeInvcCmpntCtExtr
  • SQL joins temp table with key table to assign known keys
  • Temp table is truncated before load and runstat done after load
  • Load IDS temp. table
  • join primary key info with table info
  • update primary key table (K_INVC_CMPNT_CT) with new keys created today
  • primary key hash file only contains current run keys and is cleared before writing
  • Hash file (hf_invc_cmpnt_ct_allcol) cleared in calling job
  • Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def run_IncomeInvcCmpntCtPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-Container translated function for IncomeInvcCmpntCtPK.
    Parameters
    ----------
    df_AllCol   : DataFrame
        Input stream equivalent to link AllCol.
    df_Transform: DataFrame
        Input stream equivalent to link Transform.
    params      : dict
        Runtime parameters and environment variables.
    Returns
    -------
    DataFrame
        Output stream equivalent to link Key.
    """

    # ------------------------------------------------------------
    # Unpack parameters (each exactly once)
    # ------------------------------------------------------------
    DriverTable       = params["DriverTable"]
    SrcSysCd          = params["SrcSysCd"]
    SrcSysCdSk        = params["SrcSysCdSk"]
    RunID             = params["RunID"]
    CurrDate          = params["CurrDate"]
    CurrRunCycle      = params["CurrRunCycle"]

    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]

    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------
    # Stage: hf_invc_cmpnt_ct_allcol  (Scenario-a intermediate hash)
    # Replace with deduplicate logic on incoming DataFrame
    # ------------------------------------------------------------
    key_cols_hash = [
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        key_cols_hash,
        [("<…>", "A")]  # <…> placeholder for manual remediation
    )

    # ------------------------------------------------------------
    # Stage: K_INVC_CMPNT_CT_TEMP (DB write & read)
    # ------------------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_INVC_CMPNT_CT_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    # Write incoming records to temporary table
    (
        df_Transform
        .write
        .format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_INVC_CMPNT_CT_TEMP")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_INVC_CMPNT_CT_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # Read unioned extract (W_Extract)
    extract_query = f"""
    SELECT
        k.INVC_CMPNT_CT_SK,
        w.BILL_INVC_ID,
        w.CLS_PLN_ID,
        w.PROD_ID,
        w.PROD_BILL_CMPNT_ID,
        w.INVC_CMPNT_CT_DISP_CD,
        w.INVC_CMPNT_CT_PRM_TYP_CD,
        w.INVC_CMPNT_CT_SRC_CD,
        w.SRC_SYS_CD_SK,
        k.CRT_RUN_CYC_EXCTN_SK
    FROM
        {IDSOwner}.K_INVC_CMPNT_CT_TEMP w,
        {IDSOwner}.K_INVC_CMPNT_CT k
    WHERE
        w.BILL_INVC_ID           = k.BILL_INVC_ID AND
        w.CLS_PLN_ID             = k.CLS_PLN_ID   AND
        w.PROD_ID                = k.PROD_ID      AND
        w.PROD_BILL_CMPNT_ID     = k.PROD_BILL_CMPNT_ID AND
        w.INVC_CMPNT_CT_DISP_CD  = k.INVC_CMPNT_CT_DISP_CD AND
        w.INVC_CMPNT_CT_PRM_TYP_CD = k.INVC_CMPNT_CT_PRM_TYP_CD AND
        w.INVC_CMPNT_CT_SRC_CD   = k.INVC_CMPNT_CT_SRC_CD AND
        w.SRC_SYS_CD_SK          = k.SRC_SYS_CD_SK
    UNION
    SELECT
        -1,
        w2.BILL_INVC_ID,
        w2.CLS_PLN_ID,
        w2.PROD_ID,
        w2.PROD_BILL_CMPNT_ID,
        w2.INVC_CMPNT_CT_DISP_CD,
        w2.INVC_CMPNT_CT_PRM_TYP_CD,
        w2.INVC_CMPNT_CT_SRC_CD,
        w2.SRC_SYS_CD_SK,
        {CurrRunCycle}
    FROM
        {IDSOwner}.K_INVC_CMPNT_CT_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.INVC_CMPNT_CT_SK
        FROM {IDSOwner}.K_INVC_CMPNT_CT k2
        WHERE
            w2.BILL_INVC_ID           = k2.BILL_INVC_ID AND
            w2.CLS_PLN_ID             = k2.CLS_PLN_ID   AND
            w2.PROD_ID                = k2.PROD_ID      AND
            w2.PROD_BILL_CMPNT_ID     = k2.PROD_BILL_CMPNT_ID AND
            w2.INVC_CMPNT_CT_DISP_CD  = k2.INVC_CMPNT_CT_DISP_CD AND
            w2.INVC_CMPNT_CT_PRM_TYP_CD = k2.INVC_CMPNT_CT_PRM_TYP_CD AND
            w2.INVC_CMPNT_CT_SRC_CD   = k2.INVC_CMPNT_CT_SRC_CD AND
            w2.SRC_SYS_CD_SK          = k2.SRC_SYS_CD_SK
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("INVC_CMPNT_CT_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn(
            "svSK",
            when(col("INVC_CMPNT_CT_SK") == lit(-1), lit(None)).otherwise(col("INVC_CMPNT_CT_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    )

    # SurrogateKeyGen replacement for KeyMgtGetNextValueConcurrent
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # ------------------------------------------------------------
    # Prepare outputs from PrimaryKey transformer
    # ------------------------------------------------------------
    # updt link
    updt_cols = [
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD",
        "svCrtRunCycExctnSk",
        "svSK"
    ]
    df_updt = df_enriched.select(*updt_cols)

    # NewKeys link (constraint svInstUpdt = 'I')
    newkeys_cols = [
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK",
        "svCrtRunCycExctnSk",
        "svSK"
    ]
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(*newkeys_cols)
    )

    # Keys link
    keys_cols = [
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK",
        "svSK",
        "SRC_SYS_CD",
        "svInstUpdt",
        "svCrtRunCycExctnSk"
    ]
    df_Keys = df_enriched.select(*keys_cols)

    # ------------------------------------------------------------
    # Stage: hf_invc_cmpnt_ct   (Scenario-c => parquet write)
    # ------------------------------------------------------------
    parquet_path_updt = f"{adls_path}/IncomeInvcCmpntCtPK_updt.parquet"
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

    # ------------------------------------------------------------
    # Stage: K_INVC_CMPNT_CT (Sequential file write)
    # ------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_INVC_CMPNT_CT.dat"
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

    # ------------------------------------------------------------
    # Stage: Merge  (Transformer – joins Keys with AllColOut)
    # ------------------------------------------------------------
    join_expr = (
        (df_Keys["BILL_INVC_ID"]            == df_AllCol_dedup["BILL_INVC_ID"]) &
        (df_Keys["CLS_PLN_ID"]              == df_AllCol_dedup["CLS_PLN_ID"])   &
        (df_Keys["PROD_ID"]                 == df_AllCol_dedup["PROD_ID"])      &
        (df_Keys["PROD_BILL_CMPNT_ID"]      == df_AllCol_dedup["PROD_BILL_CMPNT_ID"]) &
        (df_Keys["INVC_CMPNT_CT_DISP_CD"]   == df_AllCol_dedup["INVC_CMPNT_CT_DISP_CD"]) &
        (df_Keys["INVC_CMPNT_CT_PRM_TYP_CD"]== df_AllCol_dedup["INVC_CMPNT_CT_PRM_TYP_CD"]) &
        (df_Keys["INVC_CMPNT_CT_SRC_CD"]    == df_AllCol_dedup["INVC_CMPNT_CT_SRC_CD"]) &
        (df_Keys["SRC_SYS_CD_SK"]           == df_AllCol_dedup["SRC_SYS_CD_SK"])
    )

    df_Key = (
        df_Keys.alias("Keys")
        .join(df_AllCol_dedup.alias("AllColOut"), join_expr, "left")
        .select(
            col("Keys.svInstUpdt").alias("INSRT_UPDT_CD"),
            col("AllColOut.DISCARD_IN"),
            col("AllColOut.PASS_THRU_IN"),
            col("AllColOut.FIRST_RECYC_DT"),
            col("AllColOut.ERR_CT"),
            col("AllColOut.RECYCLE_CT"),
            col("Keys.SRC_SYS_CD"),
            col("AllColOut.PRI_KEY_STRING"),
            col("Keys.svSK").alias("INVC_CMPNT_CT_SK"),
            col("Keys.BILL_INVC_ID"),
            col("Keys.CLS_PLN_ID"),
            col("Keys.PROD_ID"),
            col("Keys.PROD_BILL_CMPNT_ID"),
            col("Keys.INVC_CMPNT_CT_DISP_CD"),
            col("Keys.INVC_CMPNT_CT_PRM_TYP_CD"),
            col("Keys.INVC_CMPNT_CT_SRC_CD"),
            col("Keys.SRC_SYS_CD_SK"),
            col("Keys.svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("AllColOut.BILL_DUE_DT_SK"),
            col("AllColOut.BILL_AMT"),
            col("AllColOut.BILL_ENTY_UNIQ_KEY"),
            col("AllColOut.DPNDT_LVS_CT"),
            col("AllColOut.SUB_LVS_CT"),
            col("AllColOut.BLCT_DISP_CD"),
            col("AllColOut.BLCT_PREM_TYPE"),
            col("AllColOut.BLCT_SOURCE"),
            col("AllColOut.PROD_CMPNT_PFX_ID")
        )
    )

    # ------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------
    return df_Key