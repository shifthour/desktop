# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName: AlineoIndvBePopHlthPgmReOpenPK
JobType: Server Job
JobCategory: DS_Integrate
FolderPath: Shared Containers/PrimaryKey
Description: Called by: AlineoIndvBePopHlthPgmEnrSeq
Processing:
                    Extracts data from an XML file received from Alineo and creates a crf file in the ../key directory.


Modifications:
                                                                Project/                                                                                                                       Code                   Date
Developer                         Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
-------------------------              -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
Karthik Chintalapani      2015-03-10    5157        Original program                                                                                              Kalyan Neelam    2015-03-17
Annotations:
- join primary key info with table info
- update primary key table (K_PAYMT_SUM) with new keys created today
- Assign primary surrogate key
- primary key hash file only contains current run keys and is cleared before writing
- SQL joins temp table with key table to assign known keys
- This container is used in:
AlineoIdsIndvBePopHlthPgmReOpenTransExtr
These programs need to be re-compiled when logic changes
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_AlineoIndvBePopHlthPgmReOpenPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    jdbc_url = params["ids_jdbc_url"]
    jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_indv_be_pop_hlth_pgm_reopen_trans_allcol (Scenario a)
    # Deduplicate incoming AllCol records on key columns
    # ------------------------------------------------------------------
    key_cols_allcol = [
        "POP_HLTH_PGM_ENR_ID",
        "PGM_REOPEN_ORDER_NO",
        "SRC_SYS_CD_SK",
        "ROW_EFF_DT_SK",
    ]
    df_AllCol_dedup = df_AllCol.dropDuplicates(key_cols_allcol)
    # ------------------------------------------------------------------
    # Stage: K_INDV_BE_POP_HLTH_PGM_REOPEN_TRANS (Existing keys in DB)
    # ------------------------------------------------------------------
    existing_query = f"""
        SELECT 
            INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK,
            POP_HLTH_PGM_ENR_ID,
            PGM_REOPEN_ORDER_NO,
            SRC_SYS_CD_SK,
            ROW_EFF_DT_SK,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_REOPEN_TRANS
    """
    df_existing = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", existing_query)
        .load()
    )
    # ------------------------------------------------------------------
    # Stage logic equivalent to DB2Connector Output (W_Extract)
    # ------------------------------------------------------------------
    df_temp = df_Transform.select(
        "POP_HLTH_PGM_ENR_ID",
        "PGM_REOPEN_ORDER_NO",
        "SRC_SYS_CD_SK",
        "ROW_EFF_DT_SK",
    )
    df_w_extract = (
        df_temp.alias("w")
        .join(
            df_existing.alias("k"),
            (
                (F.col("w.POP_HLTH_PGM_ENR_ID") == F.col("k.POP_HLTH_PGM_ENR_ID"))
                & (F.col("w.PGM_REOPEN_ORDER_NO") == F.col("k.PGM_REOPEN_ORDER_NO"))
                & (F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
                & (F.col("w.ROW_EFF_DT_SK") == F.col("k.ROW_EFF_DT_SK"))
            ),
            "left",
        )
        .select(
            F.col("k.INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK"),
            F.col("w.POP_HLTH_PGM_ENR_ID"),
            F.col("w.PGM_REOPEN_ORDER_NO"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.ROW_EFF_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
        )
    )
    df_w_extract = (
        df_w_extract.withColumn(
            "INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK",
            F.when(
                F.col("INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK").isNull(), F.lit(-1)
            ).otherwise(F.col("INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK")),
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )
    # ------------------------------------------------------------------
    # Stage: PrimaryKey Transformer
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract.withColumn(
            "INSRT_UPDT_CD",
            F.when(
                F.col("INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK") == -1, F.lit("I")
            ).otherwise(F.lit("U")),
        )
        .withColumn("SRC_SYS_CD", F.lit("ALINEO"))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == "I", F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK",
        <schema>,
        <secret_name>,
    )
    # ------------------------------------------------------------------
    # Link: updt (to hashed file → parquet)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "PGM_REOPEN_ORDER_NO",
        "SRC_SYS_CD_SK",
        "ROW_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK",
    )
    parquet_path_updt = f"{adls_path}/AlineoIndvBePopHlthPgmReOpenPK_updt.parquet"
    write_files(df_updt, parquet_path_updt, mode="overwrite", is_pqruet=True)
    # ------------------------------------------------------------------
    # Link: NewKeys (filtered inserts) → sequential file
    # ------------------------------------------------------------------
    df_newkeys = df_enriched.filter(F.col("INSRT_UPDT_CD") == "I").select(
        "POP_HLTH_PGM_ENR_ID",
        "PGM_REOPEN_ORDER_NO",
        "SRC_SYS_CD_SK",
        "ROW_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK",
    )
    seq_file_path = (
        f"{adls_path}/load/K_INDV_BE_POP_HLTH_PGM_REOPEN_TRANS.dat"
    )
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
    )
    # ------------------------------------------------------------------
    # Link: Keys (for merge)
    # ------------------------------------------------------------------
    df_keys = df_enriched.select(
        "INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK",
        "POP_HLTH_PGM_ENR_ID",
        "PGM_REOPEN_ORDER_NO",
        "SRC_SYS_CD_SK",
        "ROW_EFF_DT_SK",
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    )
    # ------------------------------------------------------------------
    # Stage: Merge Transformer -> Output Link: Key
    # ------------------------------------------------------------------
    join_cond_merge = (
        (F.col("all.POP_HLTH_PGM_ENR_ID") == F.col("keys.POP_HLTH_PGM_ENR_ID"))
        & (F.col("all.PGM_REOPEN_ORDER_NO") == F.col("keys.PGM_REOPEN_ORDER_NO"))
        & (F.col("all.SRC_SYS_CD_SK") == F.col("keys.SRC_SYS_CD_SK"))
        & (F.col("all.ROW_EFF_DT_SK") == F.col("keys.ROW_EFF_DT_SK"))
    )
    df_Key_output = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("keys"), join_cond_merge, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("all.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("all.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("keys.INDV_BE_POP_HLTH_PGM_REOPEN_TRANS_SK"),
            F.col("all.POP_HLTH_PGM_ENR_ID"),
            F.col("all.PGM_REOPEN_ORDER_NO"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("all.ROW_EFF_DT_SK"),
            F.col("keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.REOPEN_RSN_CODE"),
            F.col("all.MOST_RECENT_PGM_RCRD_IN"),
            F.col("all.PGM_REOPEN_IN"),
            F.col("all.PGM_REOPEN_LOG_STRT_DT_SK"),
            F.col("all.PGM_REOPEN_LOG_CLOSE_DT_SK"),
            F.col("all.ROW_TERM_DT_SK"),
            F.col("all.INDV_BE_KEY"),
            F.col("all.DAYS_IN_PGM_CT"),
            F.col("all.PGM_GAP_DAYS_CT"),
            F.col("all.REOPEN_BY_USER_ID"),
        )
    )
    # ------------------------------------------------------------------
    # Return final DataFrame (Key output)
    # ------------------------------------------------------------------
    return df_Key_output