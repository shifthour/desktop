# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared-Container  : AlineoAppUserTransPK
DataStage Job     : Server Job – DS_Integrate/Shared Containers/PrimaryKey
Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING
    Primary Key Shared Container for HSTD_APP_USER_TRANS

MODIFICATIONS
Developer                Date         Project/Altiris #        Change Description
-----------------------  -----------  -----------------------  -----------------------------------------
Bhoomi Dasari            2010-07-19   4297/Alineo-2            Original Programming
Abhiram Dasarathy(9)     2013-07-15   5108 – Member360(9)      Added USER_TEL_NO column

ANNOTATIONS
    • SQL joins temp table with key table to assign known keys
    • Temp table is truncated before load and runstats done after load
    • Load IDS temp. table
    • join primary key info with table info
    • update primary key table (K_HSTD_APP_USER_TRANS) with new keys created today
    • primary key hash file only contains current run keys and is cleared before writing
    • Assign primary surrogate key
    • Alineo HSTD_APP_USER_TRANS Primary Key Shared Container
    • This container is used in AlineoHstdAppUserExtr (re-compile on logic change)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_AlineoAppUserTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the AlineoAppUserTransPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream that originally landed in hash-file hf_hstd_appuser_trans_allcol.
    df_Transform : DataFrame
        Input stream destined for K_HSTD_APP_USER_TRANS_TEMP.
    params : dict
        Runtime parameters already populated by the calling job.

    Returns
    -------
    DataFrame
        The ‘Key’ output link as defined in the DataStage design.
    """

    # ------------------------------------------------------------------
    # Unpack needed parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params["SrcSysCd"]
    SrcSysCdSk        = params["SrcSysCdSk"]
    IDSOwner          = params["IDSOwner"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    ids_secret_name   = params["ids_secret_name"]

    # ------------------------------------------------------------------
    # Replace intermediate hash-file (scenario a) with deduplication
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "HSTD_USER_ID", "ROW_EFF_DT_SK"],
        sort_cols=[("<…>", "A")]
    )

    # ------------------------------------------------------------------
    # Load K_HSTD_APP_USER_TRANS_TEMP table
    # ------------------------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_HSTD_APP_USER_TRANS_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.select("SRC_SYS_CD_SK", "HSTD_USER_ID", "ROW_EFF_DT_SK")
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_HSTD_APP_USER_TRANS_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_HSTD_APP_USER_TRANS_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Extract W_Extract stream from temp and key tables
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.HSTD_APP_USER_T_SK,
            w.SRC_SYS_CD_SK,
            w.HSTD_USER_ID,
            w.ROW_EFF_DT_SK,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_HSTD_APP_USER_TRANS_TEMP w
    JOIN {IDSOwner}.K_HSTD_APP_USER_TRANS k
          ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND w.HSTD_USER_ID  = k.HSTD_USER_ID
         AND w.ROW_EFF_DT_SK = k.ROW_EFF_DT_SK
    UNION
    SELECT  -1,
            w2.SRC_SYS_CD_SK,
            w2.HSTD_USER_ID,
            w2.ROW_EFF_DT_SK,
            {CurrRunCycle}
    FROM {IDSOwner}.K_HSTD_APP_USER_TRANS_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_HSTD_APP_USER_TRANS k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.HSTD_USER_ID  = k2.HSTD_USER_ID
          AND w2.ROW_EFF_DT_SK = k2.ROW_EFF_DT_SK
    )
    """

    df_w_extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("HSTD_APP_USER_T_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # Surrogate-key generation (special-format call)
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, key_col="HSTD_APP_USER_T_SK", <schema>, <secret_name>)

    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # ------------------------------------------------------------------
    # Build internal streams: updt, NewKeys, Keys
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "HSTD_USER_ID",
            "ROW_EFF_DT_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "HSTD_APP_USER_T_SK"
        )
    )

    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "HSTD_USER_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "HSTD_APP_USER_T_SK"
        )
    )

    df_keys = (
        df_enriched
        .select(
            "HSTD_APP_USER_T_SK",
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "HSTD_USER_ID",
            "ROW_EFF_DT_SK",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Merge transformer logic
    # ------------------------------------------------------------------
    join_expr = [
        F.col("AllColOut.SRC_SYS_CD_SK") == F.col("Keys.SRC_SYS_CD_SK"),
        F.col("AllColOut.HSTD_USER_ID") == F.col("Keys.HSTD_USER_ID"),
        F.col("AllColOut.ROW_EFF_DT_SK") == F.col("Keys.ROW_EFF_DT_SK")
    ]

    df_merged = (
        df_allcol_dedup.alias("AllColOut")
        .join(df_keys.alias("Keys"), join_expr, "left")
    )

    df_key_output = (
        df_merged
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.HSTD_APP_USER_T_SK").alias("HSTD_APP_USER_T_SK"),
            F.col("AllColOut.HSTD_USER_ID").alias("HSTD_USER_ID"),
            F.col("AllColOut.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.APP_USER_TYP_CD_SK").alias("APP_USER_TYP_CD_SK"),
            F.col("AllColOut.TRANS_SRC_CD_SK").alias("TRANS_SRC_CD_SK"),
            F.col("AllColOut.ACTV_USER_IN").alias("ACTV_USER_IN"),
            F.col("AllColOut.HIPAA_LOG_IN").alias("HIPAA_LOG_IN"),
            F.col("AllColOut.HSTD_USER_CRT_DT_SK").alias("HSTD_USER_CRT_DT_SK"),
            F.col("AllColOut.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
            F.col("AllColOut.EXCH_GRP_ID").alias("EXCH_GRP_ID"),
            F.col("AllColOut.EXCH_GRP_NM").alias("EXCH_GRP_NM"),
            F.col("AllColOut.USER_FIRST_NM").alias("USER_FIRST_NM"),
            F.col("AllColOut.USER_LAST_NM").alias("USER_LAST_NM"),
            F.col("AllColOut.USER_FULL_NM").alias("USER_FULL_NM"),
            F.col("AllColOut.USER_SH_NM").alias("USER_SH_NM"),
            F.col("AllColOut.APPL_USERAPPL_USER_MODIFIED").alias("APPL_USERAPPL_USER_MODIFIED"),
            F.col("AllColOut.USER_TEL_NO").alias("USER_TEL_NO")
        )
    )

    # ------------------------------------------------------------------
    # Write sequential file (K_HSTD_APP_USER_TRANS) – via helper
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_HSTD_APP_USER_TRANS.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Write hash-file replacement parquet (hf_hstd_appuser_trans) – via helper
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/AlineoAppUserTransPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output(s)
    # ------------------------------------------------------------------
    return df_key_output