# Databricks notebook cell
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

"""
Shared Container: PaymtSumPK
Folder Path    : Shared Containers/PrimaryKey
Description    : IDS Primary Key Container for Payment Summary
                 CALLED BY:  FctsPaymtSumExtr
                 PROCESSING: Use existing primary keys assigned or create new ones for records processed
Annotations    :
    - SQL joins temp table with key table to assign known keys
    - Temp table is truncated before load and runstat done after load
    - primary key hash file only contains current run keys and is cleared before writing
    - update primary key table (K_PAYMT_SUM) with new keys created today
    - join primary key info with table info
    - Hash file cleared in calling job
    - Load IDS temp. table
    - Assign primary surrogate key
"""

# ------------------------------------------------------------------------------
# Main callable
# ------------------------------------------------------------------------------

def run_PaymtSumPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the PaymtSumPK shared-container logic.

    Parameters
    ----------
    df_AllCol   : DataFrame
        Input stream corresponding to link “AllCol”.
    df_Transform: DataFrame
        Input stream corresponding to link “Transform”.
    params      : dict
        Runtime parameters supplied by the calling job/notebook.

    Returns
    -------
    DataFrame
        Output stream corresponding to link “Key”.
    """

    # ------------------------------------------------------------------
    # Parameter unpacking
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    jdbc_url            = params["ids_jdbc_url"]
    jdbc_props          = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_payment_sum_allcol  (Scenario-a intermediate hash file)
    #        Replace with dedup logic
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "CKPY_REF_ID", "LOBD_ID"],
        sort_cols=[("SRC_SYS_CD_SK", "A"), ("CKPY_REF_ID", "A"), ("LOBD_ID", "A")]
    )

    # ------------------------------------------------------------------
    # Stage: K_PAYMT_SUM_TEMP  (DB2 / Azure-SQL connector)
    #        1. Truncate temp table
    #        2. Load df_Transform into temp table
    #        3. Extract W_Extract result set
    # ------------------------------------------------------------------
    temp_table_full = f"{IDSOwner}.K_PAYMT_SUM_TEMP"

    # Delete existing rows in temp table
    execute_dml(f"DELETE FROM {temp_table_full}", jdbc_url, jdbc_props)

    # Load current data
    (
        df_Transform.write.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", temp_table_full)
        .mode("append")
        .save()
    )

    # Build W_Extract query
    extract_query = f"""
SELECT  k.PAYMT_SUM_SK,
        w.SRC_SYS_CD_SK,
        w.PAYMT_REF_ID,
        w.PAYMT_SUM_LOB_CD,
        k.CRT_RUN_CYC_EXCTN_SK
FROM    {IDSOwner}.K_PAYMT_SUM_TEMP w
JOIN    {IDSOwner}.K_PAYMT_SUM       k
      ON w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
     AND w.PAYMT_REF_ID       = k.PAYMT_REF_ID
     AND w.PAYMT_SUM_LOB_CD   = k.PAYMT_SUM_LOB_CD
UNION
SELECT  -1,
        w2.SRC_SYS_CD_SK,
        w2.PAYMT_REF_ID,
        w2.PAYMT_SUM_LOB_CD,
        {CurrRunCycle}
FROM    {IDSOwner}.K_PAYMT_SUM_TEMP w2
WHERE NOT EXISTS (
      SELECT 1
      FROM   {IDSOwner}.K_PAYMT_SUM k2
      WHERE  w2.SRC_SYS_CD_SK    = k2.SRC_SYS_CD_SK
        AND  w2.PAYMT_REF_ID     = k2.PAYMT_REF_ID
        AND  w2.PAYMT_SUM_LOB_CD = k2.PAYMT_SUM_LOB_CD )
"""

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey  (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("PAYMT_SUM_SK") == -1, lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit("FACETS"))
        .withColumn(
            "svSK",
            when(col("PAYMT_SUM_SK") == -1, lit(None)).otherwise(col("PAYMT_SUM_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate-key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # updt link
    df_updt = (
        df_enriched.select(
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("PAYMT_REF_ID").alias("PAYMT_SUM_ID"),
            col("PAYMT_SUM_LOB_CD").alias("PAYMT_SUM_LOB_CD_SK"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("PAYMT_SUM_SK")
        )
    )

    # NewKeys link  (only inserts)
    df_newkeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("PAYMT_REF_ID"),
            col("PAYMT_SUM_LOB_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("PAYMT_SUM_SK")
        )
    )

    # Keys link
    df_keys = (
        df_enriched.select(
            col("svSK").alias("PAYMT_SUM_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("PAYMT_REF_ID"),
            col("PAYMT_SUM_LOB_CD"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: Merge  (Transformer)
    # ------------------------------------------------------------------
    join_expr = (
        (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
        (col("all.CKPY_REF_ID")   == col("k.PAYMT_REF_ID")) &
        (col("all.LOBD_ID")       == col("k.PAYMT_SUM_LOB_CD"))
    )

    df_key = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            lit(0).cast("int").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("all.CKPY_PAYEE_PR_ID").alias("CKPY_PAYEE_PR_ID"),
            col("k.PAYMT_SUM_SK").alias("PAYMT_SUM_SK"),
            col("k.PAYMT_REF_ID").alias("PAYMT_SUM_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("k.PAYMT_SUM_LOB_CD").alias("LOBD_ID"),
            col("all.CKPY_PAYEE_TYPE").alias("CKPY_PAYEE_TYPE"),
            col("all.CKPY_PYMT_TYPE").alias("CKPY_PYMT_TYPE"),
            col("all.CKPY_TYPE").alias("CKPY_TYPE"),
            col("all.CKPY_COMB_IND").alias("CKPY_COMB_IND"),
            col("all.CKPY_PAY_DT").alias("CKPY_PAY_DT"),
            col("all.CKPY_PER_END_DT").alias("CKPY_PER_END_DT"),
            col("all.CKPY_DEDUCT_AMT").alias("CKPY_DEDUCT_AMT"),
            col("all.CKPY_NET_AMT").alias("CKPY_NET_AMT"),
            col("all.CKPY_ORIG_AMT").alias("CKPY_ORIG_AMT"),
            col("all.CKPY_CURR_CKCK_SEQ").alias("CKPY_CURR_CKCK_SEQ")
        )
    )

    # ------------------------------------------------------------------
    # Stage: hf_paymt_sum (Scenario-c hash file → parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/PaymtSumPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True
    )

    # ------------------------------------------------------------------
    # Stage: K_PAYMT_SUM  (Sequential file)
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_PAYMT_SUM.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False
    )

    # ------------------------------------------------------------------
    # Final output
    # ------------------------------------------------------------------
    return df_key