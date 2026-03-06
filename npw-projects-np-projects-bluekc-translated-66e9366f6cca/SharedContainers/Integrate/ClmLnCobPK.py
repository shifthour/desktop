# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------



# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# COMMAND ----------
def run_ClmLnCobPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared Container: ClmLnCobPK
    DESCRIPTION:
        Shared container used for Primary Keying of Claim Line COB job

    CALLED BY :
        FctsClmLnCOBExtr, NascoClmLnCOBExtr and FctsClmLnDntlCOBExtr

    PROCESSING NOTES:
        - Removes duplicates from the incoming AllCol stream.
        - Generates surrogate keys for new Claim Line COB rows.
        - Merges newly generated keys with full column detail rows.
        - Persists new keys to a sequential file and updates the hash-file
          representation as a parquet resource.
        - Returns the “Key” stream back to the calling job.
    """

    # ------------------------------------------------------------------
    # Unpack Parameters
    # ------------------------------------------------------------------
    IDSOwner        = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]

    CurrRunCycle    = params["CurrRunCycle"]
    SrcSysCd        = params["SrcSysCd"]

    ids_jdbc_url    = params["ids_jdbc_url"]
    ids_jdbc_props  = params["ids_jdbc_props"]

    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_clm_ln_cob_allcol  (Scenario-a intermediate hashed file)
    # ------------------------------------------------------------------
    key_cols_allcol = [
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_COB_TYP_CD"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        key_cols_allcol,
        []
    )

    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_COB_TEMP  (Logical extraction logic)
    # ------------------------------------------------------------------
    df_k_clm_ln_cob = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", f"{IDSOwner}.K_CLM_LN_COB")
            .load()
    )

    join_cond = (
        (df_Transform["SRC_SYS_CD_SK"] == df_k_clm_ln_cob["SRC_SYS_CD_SK"])
        & (df_Transform["CLM_ID"] == df_k_clm_ln_cob["CLM_ID"])
        & (df_Transform["CLM_LN_SEQ_NO"] == df_k_clm_ln_cob["CLM_LN_SEQ_NO"])
        & (df_Transform["CLM_LN_COB_TYP_CD"] == df_k_clm_ln_cob["CLM_LN_COB_TYP_CD"])
    )

    df_w_extract = (
        df_Transform.alias("w")
        .join(df_k_clm_ln_cob.alias("k"), join_cond, "left")
        .select(
            F.coalesce(F.col("k.CLM_COB_LN_SK"), F.lit(-1)).alias("CLM_COB_LN_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.CLM_ID"),
            F.col("w.CLM_LN_SEQ_NO"),
            F.col("w.CLM_LN_COB_TYP_CD"),
            F.when(
                F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_COB_LN_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn("svClmId", F.trim(F.col("CLM_ID")))
        .withColumn("svClmLnCobTypCd", F.trim(F.col("CLM_LN_COB_TYP_CD")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("svInstUpdt") == "I",
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None).cast("long"))
             .otherwise(F.col("CLM_COB_LN_SK"))
        )
    )

    # Surrogate Key Generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        key_col="svSK",
        <schema>,
        <secret_name>
    )

    # Outputs from PrimaryKey transformer
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svClmId").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("svClmLnCobTypCd").alias("CLM_LN_COB_TYP_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_COB_LN_SK")
    )

    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.col("svClmLnCobTypCd").alias("CLM_LN_COB_TYP_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_COB_LN_SK")
        )
    )

    df_keys = df_enriched.select(
        F.col("svSK").alias("CLM_COB_LN_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svClmId").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("svClmLnCobTypCd").alias("CLM_LN_COB_TYP_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Stage: Merge
    # ------------------------------------------------------------------
    merge_cond = (
        (df_AllCol_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"])
        & (df_AllCol_dedup["CLM_ID"] == df_keys["CLM_ID"])
        & (df_AllCol_dedup["CLM_LN_SEQ_NO"] == df_keys["CLM_LN_SEQ_NO"])
        & (df_AllCol_dedup["CLM_LN_COB_TYP_CD"] == df_keys["CLM_LN_COB_TYP_CD"])
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("keys"), merge_cond, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("keys.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("keys.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("keys.CLM_COB_LN_SK"),
            F.col("all.CLM_ID"),
            F.col("all.CLM_LN_SEQ_NO"),
            F.col("all.CLM_LN_COB_TYP_CD"),
            F.col("keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CLM_LN_COB_CAR_PRORTN_CD"),
            F.col("all.CLM_LN_COB_LIAB_TYP_CD"),
            F.col("all.COB_CAR_ADJ_AMT"),
            F.col("all.ALW_AMT"),
            F.col("all.APLD_AMT"),
            F.col("all.COPAY_AMT"),
            F.col("all.DEDCT_AMT"),
            F.col("all.DSALW_AMT"),
            F.col("all.COINS_AMT"),
            F.col("all.MNTL_HLTH_COINS_AMT"),
            F.col("all.OOP_AMT"),
            F.col("all.PD_AMT"),
            F.col("all.SANC_AMT"),
            F.col("all.SAV_AMT"),
            F.col("all.SUBTR_AMT"),
            F.col("all.COB_CAR_RSN_CD_TX"),
            F.col("all.COB_CAR_RSN_TX")
        )
    )

    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_COB  (Sequential File Output)
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_LN_COB.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )

    # ------------------------------------------------------------------
    # Stage: hf_clm_ln_cob  (Parquet Representation of Hash-File)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ClmLnCobPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True
    )

    # ------------------------------------------------------------------
    # Return the container output stream(s)
    # ------------------------------------------------------------------
    return df_Key


# COMMAND ----------