# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ComsnErnPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    IDS Primary Key Container for Commission Earn
    Used by FctsComsnErnExtr

    DESCRIPTION:
        Shared container used for Primary Keying of Comm Stmnt job

    PROCESSING FLOW (simplified):
        1. Deduplicate incoming “AllCol” rows.
        2. Load/refresh the temporary table K_COMSN_ERN_TEMP.
        3. Extract existing / new keys into W_Extract.
        4. Derive insert/update flags, surrogate keys, and run-cycle values.
        5. Persist:
              – Updated hash-file replacement (parquet) : hf_comsn_ern
              – New-key sequential file               : K_COMSN_ERN.dat
        6. Merge outbound “Key” link for downstream consumption.
    """

    # --------------------------------------------------
    # Unpack parameters (each accessed exactly once)
    # --------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    IDSOwner = params["IDSOwner"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    ids_secret_name = params["ids_secret_name"]

    # --------------------------------------------------
    # 1. Deduplicate AllCol input (intermediate hash-file removal)
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "COMSN_BILL_REL_UNIQ_KEY",
            "COMSN_CALC_LOB_CD",
            "COMSN_SCHD_TIER_UNIQ_KEY",
            "SEQ_NO",
        ],
        sort_cols=[("<…>", "D")],
    )

    # --------------------------------------------------
    # 2. Refresh K_COMSN_ERN_TEMP (truncate & load)
    # --------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_COMSN_ERN_TEMP",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_COMSN_ERN_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_ERN_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    # --------------------------------------------------
    # 3. Extract W_Extract (existing & new surrogate keys)
    # --------------------------------------------------
    extract_query = f"""
    SELECT  k.COMSN_ERN_SK,
            w.SRC_SYS_CD_SK,
            w.COMSN_BILL_REL_UNIQ_KEY,
            w.COMSN_CALC_LOB_CD,
            w.COMSN_SCHD_TIER_UNIQ_KEY,
            w.SEQ_NO,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM    {IDSOwner}.K_COMSN_ERN_TEMP w
            JOIN {IDSOwner}.K_COMSN_ERN k
              ON w.SRC_SYS_CD_SK          = k.SRC_SYS_CD_SK
             AND w.COMSN_BILL_REL_UNIQ_KEY = k.COMSN_BILL_REL_UNIQ_KEY
             AND w.COMSN_CALC_LOB_CD      = k.COMSN_CALC_LOB_CD
             AND w.COMSN_SCHD_TIER_UNIQ_KEY= k.COMSN_SCHD_TIER_UNIQ_KEY
             AND w.SEQ_NO                 = k.SEQ_NO
    UNION
    SELECT  -1,
            w2.SRC_SYS_CD_SK,
            w2.COMSN_BILL_REL_UNIQ_KEY,
            w2.COMSN_CALC_LOB_CD,
            w2.COMSN_SCHD_TIER_UNIQ_KEY,
            w2.SEQ_NO,
            {CurrRunCycle}
    FROM    {IDSOwner}.K_COMSN_ERN_TEMP w2
    WHERE   NOT EXISTS (
                SELECT 1
                FROM   {IDSOwner}.K_COMSN_ERN k2
                WHERE  w2.SRC_SYS_CD_SK           = k2.SRC_SYS_CD_SK
                  AND  w2.COMSN_CALC_LOB_CD       = k2.COMSN_CALC_LOB_CD
                  AND  w2.COMSN_BILL_REL_UNIQ_KEY = k2.COMSN_BILL_REL_UNIQ_KEY
                  AND  w2.COMSN_SCHD_TIER_UNIQ_KEY= k2.COMSN_SCHD_TIER_UNIQ_KEY
                  AND  w2.SEQ_NO                  = k2.SEQ_NO
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
    # 4. Derive flags, surrogate keys, run-cycle values
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract.withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("COMSN_ERN_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn(
            "COMSN_ERN_SK",
            F.when(F.col("COMSN_ERN_SK") == F.lit(-1), F.lit(None)).otherwise(
                F.col("COMSN_ERN_SK")
            ),
        )
        .withColumn("SRC_SYS_CD", F.lit("FACETS"))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )

    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "COMSN_ERN_SK", <schema>, <secret_name>
    )

    # --------------------------------------------------
    # 5a. Hash-file replacement parquet (hf_comsn_ern)
    # --------------------------------------------------
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "COMSN_BILL_REL_UNIQ_KEY",
        "COMSN_CALC_LOB_CD",
        "COMSN_SCHD_TIER_UNIQ_KEY",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "COMSN_ERN_SK",
    )

    write_files(
        df_updt,
        f"{adls_path}/ComsnErnPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    # --------------------------------------------------
    # 5b. Sequential file for new keys (K_COMSN_ERN.dat)
    # --------------------------------------------------
    df_new_keys = df_enriched.filter(F.col("INSRT_UPDT_CD") == F.lit("I")).select(
        "SRC_SYS_CD_SK",
        "COMSN_BILL_REL_UNIQ_KEY",
        "COMSN_CALC_LOB_CD",
        "COMSN_SCHD_TIER_UNIQ_KEY",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "COMSN_ERN_SK",
    )

    write_files(
        df_new_keys,
        f"{adls_path}/load/K_COMSN_ERN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote='"',
        nullValue=None,
    )

    # --------------------------------------------------
    # 6. Merge for outbound “Key” link
    # --------------------------------------------------
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
        & (
            F.col("all.COMSN_BILL_REL_UNIQ_KEY")
            == F.col("k.COMSN_BILL_REL_UNIQ_KEY")
        )
        & (F.col("all.COMSN_CALC_LOB_CD") == F.col("k.COMSN_CALC_LOB_CD"))
        & (
            F.col("all.COMSN_SCHD_TIER_UNIQ_KEY")
            == F.col("k.COMSN_SCHD_TIER_UNIQ_KEY")
        )
        & (F.col("all.SEQ_NO") == F.col("k.SEQ_NO"))
    )

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_enriched.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.COMSN_ERN_SK"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.COMSN_BILL_REL_UNIQ_KEY"),
            F.col("all.COMSN_CALC_LOB_CD_SK").alias("LOBD_ID"),
            F.col("all.COMSN_SCHD_TIER_UNIQ_KEY"),
            F.col("all.SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.FNCL_COMSN_RPTNG_RCS_IN"),
            F.col("all.PD_DT_SK"),
            F.col("all.ADV_ERN_AMT"),
            F.col("all.ERN_COMSN_AMT"),
            F.col("all.FORGO_AMT"),
            F.col("all.NET_COMSN_ERN_AMT"),
            F.col("all.OVRD_AMT"),
            F.col("all.PD_COMSN_AMT"),
            F.col("all.RECON_HOLD_AMT"),
            F.col("all.SRC_INCM_AMT"),
            F.col("all.COCE_ID_PAYEE"),
        )
    )

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_Key