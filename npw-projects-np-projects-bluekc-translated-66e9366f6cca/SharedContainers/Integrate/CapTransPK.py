# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# Databricks notebook source
"""
CapTransPK – Shared container converted from IBM DataStage

DESCRIPTION:
    Shared container used for Primary Keying of Capitation Transaction job

CALLED BY:
    PSCapTransExtr

PROCESSING NOTES (original DataStage annotations):
    - Hash file (hf_cap_trans_allcol) cleared in calling program
    - Join primary key info with table info
    - Update primary key table (K_CAP_TRANS) with new keys created today
    - Primary key hash file only contains current run keys and is cleared before writing
    - SQL joins temp table with key table to assign known keys
    - Temp table is truncated before load and runstat done after load
    - Load IDS temp. table
    - This container is used in: PSCapTransExtr – These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

# COMMAND ----------

def run_CapTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the CapTransPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link “AllCol”.
    df_Transform : DataFrame
        Container input link “Transform”.
    params : dict
        Runtime parameters passed from the calling job / notebook.

    Returns
    -------
    DataFrame
        Container output link “Key”.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    SrcSysCd            = params["SrcSysCd"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Step 1 – K_CAP_TRANS_TEMP table load (truncate → insert)
    # ------------------------------------------------------------------
    temp_table_name = f"{IDSOwner}.K_CAP_TRANS_TEMP"

    execute_dml(f"TRUNCATE TABLE {temp_table_name}", ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table_name)
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {temp_table_name} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Step 2 – Extract W_Extract (df_W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.CAP_TRANS_SK,
                w.SRC_SYS_CD_SK,
                w.CAP_TRANS_CK,
                w.ACCTG_DT_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_CAP_TRANS_TEMP w
        JOIN    {IDSOwner}.K_CAP_TRANS k
              ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
             AND w.CAP_TRANS_CK  = k.CAP_TRANS_CK
             AND w.ACCTG_DT_SK   = k.ACCTG_DT_SK
        UNION
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.CAP_TRANS_CK,
                w2.ACCTG_DT_SK,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_CAP_TRANS_TEMP w2
        WHERE   NOT EXISTS (
                    SELECT 1
                    FROM   {IDSOwner}.K_CAP_TRANS k2
                    WHERE  w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
                      AND  w2.CAP_TRANS_CK  = k2.CAP_TRANS_CK
                      AND  w2.ACCTG_DT_SK   = k2.ACCTG_DT_SK
                )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Step 3 – PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("CAP_TRANS_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn("svCrtRunCycExctnSk",
                    when(col("CAP_TRANS_SK") == -1, lit(CurrRunCycle))
                    .otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
    )

    # Surrogate key generation for new rows
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'CAP_TRANS_SK',<schema>,<secret_name>)
    df_enriched = df_enriched.withColumn("svSK", col("CAP_TRANS_SK"))

    # ------------------------------------------------------------------
    # Split outputs from PrimaryKey transformer
    # ------------------------------------------------------------------
    # updt link
    df_updt = df_enriched.select(
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("CAP_TRANS_CK"),
        col("ACCTG_DT_SK"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("CAP_TRANS_SK")
    )

    # NewKeys link (constraint svInstUpdt = 'I')
    df_NewKeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("CAP_TRANS_CK"),
            col("ACCTG_DT_SK"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("CAP_TRANS_SK")
        )
    )

    # Keys link
    df_Keys = df_enriched.select(
        col("CAP_TRANS_SK"),
        col("SRC_SYS_CD_SK"),
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("CAP_TRANS_CK"),
        col("ACCTG_DT_SK"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Step 4 – Replace intermediate hash file hf_cap_trans_allcol
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CAP_TRANS_CK", "ACCTG_DT_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Step 5 – Merge transformer logic
    # ------------------------------------------------------------------
    df_merge = (
        df_AllColOut.alias("AllColOut")
        .join(
            df_Keys.alias("Keys"),
            ["SRC_SYS_CD_SK", "CAP_TRANS_CK", "ACCTG_DT_SK"],
            "left"
        )
    )

    df_Key = df_merge.select(
        col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
        col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("AllColOut.ERR_CT").alias("ERR_CT"),
        col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
        col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("Keys.CAP_TRANS_SK").alias("CAP_TRANS_SK"),
        col("AllColOut.CAP_TRANS_CK").alias("CAP_TRANS_CK"),
        col("AllColOut.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
        col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("AllColOut.GRP").alias("GRP"),
        col("AllColOut.PROD").alias("PROD"),
        col("AllColOut.FNCL_LOB").alias("FNCL_LOB"),
        col("AllColOut.CAP_TRANS_LOB_CD").alias("CAP_TRANS_LOB_CD"),
        col("AllColOut.CAP_TRANS_PAYMT_METH_CD").alias("CAP_TRANS_PAYMT_METH_CD"),
        col("AllColOut.PD_FROM_DT").alias("PD_FROM_DT"),
        col("AllColOut.PD_THRU_DT").alias("PD_THRU_DT"),
        col("AllColOut.CALC_FUND_AMT").alias("CALC_FUND_AMT"),
        col("AllColOut.FUND_RATE_AMT").alias("FUND_RATE_AMT")
    )

    # ------------------------------------------------------------------
    # Step 6 – Write sequential and parquet outputs
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CAP_TRANS.dat",
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None
    )

    write_files(
        df_updt,
        f"{adls_path}/CapTransPK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key