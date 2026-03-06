
# Databricks notebook cell -----------------------------------------------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
CapFundPK – Shared container converted from IBM DataStage job “CapFundPK”.

VC LOGS
1_1 11/12/08 10:07:57 Batch  14927_36485 PROMOTE bckcetl ids20 dsadm rc for brent
1_1 11/12/08 09:58:58 Batch  14927_35947 INIT bckcett testIDS dsadm rc for brent
1_1 11/06/08 08:57:33 Batch  14921_32258 PROMOTE bckcett testIDS u03651 steph for Brent
1_1 11/06/08 08:52:23 Batch  14921_31946 INIT bckcett devlIDS u03651 steffy

COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION
Shared container used for Primary Keying of Capitation Funding job.

CALLED BY
FctsCapFundExtr

PROCESSING NOTES
• Hash file (hf_cap_fund_allcol) cleared in calling program
• Join primary-key info with table info
• Update primary key table (K_CAP_FUND) with new keys created today
• Primary-key hash file only contains current-run keys and is cleared before writing
• SQL joins temp table with key table to assign known keys
• Temp table is truncated before load and runstats done after load
• Load IDS temporary table
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

# COMMAND ----------

def run_CapFundPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes shared-container logic identical to the original DataStage
    implementation of CapFundPK.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input link “AllCol”.
    df_Transform : DataFrame
        Input link “Transform”.
    params : dict
        Runtime parameters, JDBC configs, secret names, and ADLS paths.

    Returns
    -------
    DataFrame
        Container output link “lnkOut”.
    """

    # --------------------------------------------------
    # Unpack runtime parameters (exactly one reference each)
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
    # Stage : hf_cap_fund_allcol  (scenario a – intermediate hash file)
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CAP_FUND_ID"],
        []
    )

    # --------------------------------------------------
    # Stage : K_CAP_FUND_TEMP  (DB2Connector)
    # --------------------------------------------------
    table_temp = f"{IDSOwner}.K_CAP_FUND_TEMP"
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", table_temp)
        .mode("overwrite")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CAP_FUND_TEMP "
        f"on key columns with distribution on key columns and detailed "
        f"indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
    SELECT k.CAP_FUND_SK,
           w.SRC_SYS_CD_SK,
           w.CAP_FUND_ID,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CAP_FUND_TEMP w,
           {IDSOwner}.K_CAP_FUND k
     WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
       AND w.CAP_FUND_ID   = k.CAP_FUND_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CAP_FUND_ID,
           {CurrRunCycle}
      FROM {IDSOwner}.K_CAP_FUND_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_CAP_FUND k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.CAP_FUND_ID   = k2.CAP_FUND_ID)
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # Stage : PrimaryKey (Transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("CAP_FUND_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # --------------------------------------------------
    # Surrogate-key generation (CAP_FUND_SK)
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CAP_FUND_SK",
        <schema>,
        <secret_name>
    )

    # Post-sequence columns
    df_enriched = df_enriched.withColumn("svSK", col("CAP_FUND_SK"))

    # Output link “updt”  → hf_cap_fund
    df_updt = df_enriched.select(
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("CAP_FUND_ID"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("CAP_FUND_SK")
    )

    # Output link “NewKeys” → sequential file K_CAP_FUND.dat
    df_newKeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("CAP_FUND_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CAP_FUND_SK")
        )
    )

    # Output link “Keys”  → Merge
    df_keys = df_enriched.select(
        col("svSK").alias("CAP_FUND_SK"),
        col("SRC_SYS_CD_SK"),
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("CAP_FUND_ID"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # --------------------------------------------------
    # Stage : Merge
    # --------------------------------------------------
    df_merge = (
        df_AllColOut.alias("all")
        .join(
            df_keys.alias("k"),
            (col("k.SRC_SYS_CD_SK") == col("all.SRC_SYS_CD_SK")) &
            (col("k.CAP_FUND_ID")  == col("all.CAP_FUND_ID")),
            "left"
        )
    )

    df_lnkOut = df_merge.select(
        col("all.JOB_EXCTN_RCRD_ERR_SK"),
        col("k.INSRT_UPDT_CD"),
        col("all.DISCARD_IN"),
        col("all.PASS_THRU_IN"),
        col("all.FIRST_RECYC_DT"),
        col("all.ERR_CT"),
        col("all.RECYCLE_CT"),
        col("k.SRC_SYS_CD"),
        col("all.PRI_KEY_STRING"),
        col("k.CAP_FUND_SK"),
        col("all.CAP_FUND_ID"),
        col("k.CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("all.CAP_FUND_ACCTG_CAT_CD"),
        col("all.CAP_FUND_PAYMT_METH_CD"),
        col("all.CAP_FUND_PRORT_RULE_CD"),
        col("all.CAP_FUND_RATE_CD"),
        col("all.GRP_CAP_MOD_APLD_IN"),
        col("all.MAX_AMT"),
        col("all.MIN_AMT"),
        col("all.FUND_DESC")
    )

    # --------------------------------------------------
    # Stage : hf_cap_fund  (scenario c – write parquet)
    # --------------------------------------------------
    parquet_path = f"{adls_path}/CapFundPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # Stage : K_CAP_FUND (sequential file)
    # --------------------------------------------------
    seq_path = f"{adls_path}/load/K_CAP_FUND.dat"
    write_files(
        df_newKeys,
        seq_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_lnkOut
