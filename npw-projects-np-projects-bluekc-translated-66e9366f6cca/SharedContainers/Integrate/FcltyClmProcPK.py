
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_FcltyClmProcPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # unpack parameters
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    RunID                 = params["RunID"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    SrcSysCdSk            = params["SrcSysCdSk"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # hf_fclty_proc_allcol  (scenario a – intermediate hashed file)
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_PROC_ORDNL_CD"],
        []
    )

    # ------------------------------------------------------------------
    # K_FCLTY_CLM_PROC_TEMP  (logical replacement in Spark)
    # ------------------------------------------------------------------
    df_temp = (
        df_Transform
        .select("SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_PROC_ORDNL_CD")
        .dropDuplicates()
    )

    extract_query_existing = f"""
    SELECT 
           FCLTY_CLM_PROC_SK,
           SRC_SYS_CD_SK,
           CLM_ID,
           FCLTY_CLM_PROC_ORDNL_CD,
           CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_FCLTY_CLM_PROC
    """

    df_existing = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_existing)
            .load()
    )

    df_join = (
        df_temp.alias("w")
        .join(
            df_existing.alias("k"),
            [
                F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
                F.col("w.CLM_ID") == F.col("k.CLM_ID"),
                F.col("w.FCLTY_CLM_PROC_ORDNL_CD") == F.col("k.FCLTY_CLM_PROC_ORDNL_CD")
            ],
            "left"
        )
    )

    df_W_Extract = df_join.select(
        F.when(F.col("k.FCLTY_CLM_PROC_SK").isNull(), F.lit(-1))
         .otherwise(F.col("k.FCLTY_CLM_PROC_SK"))
         .alias("FCLTY_CLM_PROC_SK"),
        F.col("w.SRC_SYS_CD_SK"),
        F.col("w.CLM_ID"),
        F.col("w.FCLTY_CLM_PROC_ORDNL_CD"),
        F.when(F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle))
         .otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK"))
         .alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("FCLTY_CLM_PROC_SK") == F.lit(-1), F.lit("I"))
             .otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("FCLTY_CLM_PROC_SK") == F.lit(-1), F.lit(None))
             .otherwise(F.col("FCLTY_CLM_PROC_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("FCLTY_CLM_PROC_SK") == F.lit(-1), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svClmId", F.trim(F.col("CLM_ID")))
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # updt link -> hf_clm_proc  (scenario c – parquet)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svClmId").alias("CLM_ID"),
        F.col("FCLTY_CLM_PROC_ORDNL_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("FCLTY_CLM_PROC_SK")
    )

    parquet_path_updt = f"{adls_path}/FcltyClmProcPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # NewKeys link -> K_FCLTY_CLM_PROC sequential file
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("FCLTY_CLM_PROC_ORDNL_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("FCLTY_CLM_PROC_SK")
        )
    )

    seq_file_path = f"{adls_path}/load/K_FCLTY_CLM_PROC.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Keys link for merge with AllCol
    # ------------------------------------------------------------------
    df_keys_for_merge = df_enriched.select(
        F.col("svSK").alias("FCLTY_CLM_PROC_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svClmId").alias("CLM_ID"),
        F.col("FCLTY_CLM_PROC_ORDNL_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Merge transformer logic
    # ------------------------------------------------------------------
    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(
            df_keys_for_merge.alias("k"),
            [
                F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
                F.col("all.CLM_ID") == F.col("k.CLM_ID"),
                F.col("all.FCLTY_CLM_PROC_ORDNL_CD") == F.col("k.FCLTY_CLM_PROC_ORDNL_CD")
            ],
            "left"
        )
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN"),
        F.col("all.PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT"),
        F.col("all.ERR_CT"),
        F.col("all.RECYCLE_CT"),
        F.col("k.SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING"),
        F.col("k.FCLTY_CLM_PROC_SK").alias("CLM_PROC_SK"),
        F.col("all.CLM_ID"),
        F.col("all.FCLTY_CLM_PROC_ORDNL_CD"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("all.PROC_CD"),
        F.col("all.PROC_DT"),
        F.col("all.PROC_TYPE_CD"),
        F.col("all.PROC_CD_MOD_TX"),
        F.col("all.PROC_CD_CAT_CD")
    )

    return df_Key
