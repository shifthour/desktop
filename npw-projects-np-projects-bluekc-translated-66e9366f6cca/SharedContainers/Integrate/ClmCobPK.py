# MAGIC %run ./Utility

# COMMAND ----------
# MAGIC %run ./Routine_Functions

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ClmCobPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Claim COB Primary Key – Shared container originally implemented in IBM DataStage.

    DESCRIPTION:
        Shared container used for Primary Keying of Claim COB

    PROCESSING STEPS (high-level, mirrored from DataStage design):
        1. Deduplicate incoming AllCol stream on key columns.
        2. Read current keys from {IDSOwner}.K_CLM_COB.
        3. Determine insert vs. update for each record coming from Transform stream.
        4. Generate surrogate keys for new (insert) records.
        5. Persist:
           • Updated key hash (parquet) – ‘updt’ link
           • New keys (sequential file) – ‘NewKeys’ link
        6. Merge new/updated keys with full AllCol detail stream.
        7. Return merged DataFrame through the ‘Key’ output link.

    All helper utilities come from the Utility and Routine_Functions notebooks.
    """
    # --------------------------------------------------
    # Unpack parameters (only once)
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    IDSOwner          = params["IDSOwner"]
    SrcSysCd          = params["SrcSysCd"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    # --------------------------------------------------
    # Step 1 – Deduplicate AllCol stream (intermediate hash file replacement)
    # --------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_COB_TYP_CD"],
        []  # no specific sort precedence supplied in the original job
    )

    # --------------------------------------------------
    # Step 2 – Read current key table
    # --------------------------------------------------
    extract_query = f"""
    SELECT
        CLM_COB_SK,
        SRC_SYS_CD_SK,
        CLM_ID,
        CLM_COB_TYP_CD,
        CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_COB
    """
    df_k_clm_cob = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Step 3 – Determine insert/update (emulating W_Extract)
    # --------------------------------------------------
    join_expr = (
        (df_Transform.SRC_SYS_CD_SK == df_k_clm_cob.SRC_SYS_CD_SK) &
        (df_Transform.CLM_ID        == df_k_clm_cob.CLM_ID) &
        (df_Transform.CLM_COB_TYP_CD == df_k_clm_cob.CLM_COB_TYP_CD)
    )

    df_w_extract = (
        df_Transform.alias("w")
        .join(df_k_clm_cob.alias("k"), join_expr, "left")
        .select(
            F.coalesce(F.col("k.CLM_COB_SK"), F.lit(-1)).alias("CLM_COB_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.CLM_ID"),
            F.col("w.CLM_COB_TYP_CD"),
            F.coalesce(F.col("k.CRT_RUN_CYC_EXCTN_SK"), F.lit(CurrRunCycle)).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Step 4 – Transformer logic (PrimaryKey stage)
    # --------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_COB_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSK", F.col("CLM_COB_SK"))  # temporary, will be overwritten for inserts
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Generate surrogate keys for new rows (svInstUpdt == 'I')
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # --------------------------------------------------
    # Step 5 – Prepare output links from PrimaryKey stage
    # --------------------------------------------------
    # updt link (to hf_clm_cob)
    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_COB_TYP_CD",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_COB_SK")
    )

    # NewKeys link (sequential file) – only inserts
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_COB_TYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_COB_SK")
        )
    )

    # Keys link (for merge)
    df_keys = df_enriched.select(
        F.col("svSK").alias("CLM_COB_SK"),
        "SRC_SYS_CD_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_COB_TYP_CD",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # --------------------------------------------------
    # Step 6 – Persist hashed-file (parquet) and sequential file
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ClmCobPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_COB.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # Step 7 – Merge (AllColOut + Keys) – final Key output link
    # --------------------------------------------------
    merge_join_expr = (
        (df_AllCol_dedup.SRC_SYS_CD == df_keys.SRC_SYS_CD) &
        (df_AllCol_dedup.CLM_ID == df_keys.CLM_ID) &
        (df_AllCol_dedup.CLM_COB_TYP_CD == df_keys.CLM_COB_TYP_CD)
    )

    df_key = (
        df_AllCol_dedup.alias("AllColOut")
        .join(df_keys.alias("Keys"), merge_join_expr, "left")
        .select(
            F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD"),
            "AllColOut.DISCARD_IN",
            "AllColOut.PASS_THRU_IN",
            "AllColOut.FIRST_RECYC_DT",
            "AllColOut.ERR_CT",
            "AllColOut.RECYCLE_CT",
            "Keys.SRC_SYS_CD",
            "AllColOut.PRI_KEY_STRING",
            "Keys.CLM_COB_SK",
            "AllColOut.SRC_SYS_CD_SK",
            "AllColOut.CLM_ID",
            "AllColOut.CLM_COB_TYP_CD",
            "Keys.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "AllColOut.CLM_SK",
            "AllColOut.CLM_COB_LIAB_TYP_CD",
            "AllColOut.ALW_AMT",
            "AllColOut.COPAY_AMT",
            "AllColOut.DEDCT_AMT",
            "AllColOut.DSALW_AMT",
            "AllColOut.MED_COINS_AMT",
            "AllColOut.MNTL_HLTH_COINS_AMT",
            "AllColOut.PD_AMT",
            "AllColOut.SANC_AMT",
            "AllColOut.COB_CAR_RSN_CD_TX",
            "AllColOut.COB_CAR_RSN_TX"
        )
    )

    # --------------------------------------------------
    # Return container output link(s)
    # --------------------------------------------------
    return df_key