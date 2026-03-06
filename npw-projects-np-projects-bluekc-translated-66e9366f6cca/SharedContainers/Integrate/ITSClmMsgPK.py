# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: ITSClmMsgPK
Description: Shared container used for Primary Keying of ITS Claim Message job
Called By: FctsITSClmMsgExtr
Annotations:
- Hash file (hf_its_clm_msg_allcol) cleared in calling program
- join primary key info with table info
- update primary key table (K_ITS_CLM_MSG) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
- SQL joins temp table with key table to assign known keys
- Temp table is tuncated before load and runstat done after load
- Load IDS temp. table
- This container is used in:               FctsITSClmMsgExtr
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ITSClmMsgPK(
    df_Transform: DataFrame,
    df_AllCol: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack runtime parameters
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    SrcSysCd            = params["SrcSysCd"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Load temporary table K_ITS_CLM_MSG_TEMP
    truncate_sql = f"TRUNCATE TABLE {IDSOwner}.K_ITS_CLM_MSG_TEMP"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_ITS_CLM_MSG_TEMP")
        .mode("append")
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_ITS_CLM_MSG_TEMP "
        f"on key columns with distribution on key columns "
        f"and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)
    # ------------------------------------------------------------------
    # Extract W_Extract DataFrame
    extract_query = f"""
    SELECT k.ITS_CLM_MSG_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.ITS_CLM_MSG_FMT_CD,
           w.ITS_CLM_MSG_ID,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_ITS_CLM_MSG_TEMP w,
         {IDSOwner}.K_ITS_CLM_MSG k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.CLM_ID        = k.CLM_ID
      AND w.ITS_CLM_MSG_FMT_CD = k.ITS_CLM_MSG_FMT_CD
      AND w.ITS_CLM_MSG_ID = k.ITS_CLM_MSG_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.ITS_CLM_MSG_FMT_CD,
           w2.ITS_CLM_MSG_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_ITS_CLM_MSG_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.ITS_CLM_MSG_SK
        FROM {IDSOwner}.K_ITS_CLM_MSG k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID        = k2.CLM_ID
          AND w2.ITS_CLM_MSG_FMT_CD = k2.ITS_CLM_MSG_FMT_CD
          AND w2.ITS_CLM_MSG_ID = k2.ITS_CLM_MSG_ID
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
    # Deduplicate AllCol (intermediate hashed file replacement)
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        [
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "ITS_CLM_MSG_FMT_CD",
            "ITS_CLM_MSG_ID"
        ],
        []
    )
    # ------------------------------------------------------------------
    # Primary Key Transformer logic
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("ITS_CLM_MSG_SK") == F.lit(-1), F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn("svSK",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None))
                     .otherwise(F.col("ITS_CLM_MSG_SK")))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svClmId", F.trim(F.col("CLM_ID")))
        .withColumn("svItsClmMsgFmtCd", F.trim(F.col("ITS_CLM_MSG_FMT_CD")))
        .withColumn("svItsClmMsgId", F.trim(F.col("ITS_CLM_MSG_ID")))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>, 'svSK', <schema>, <secret_name>)
    # ------------------------------------------------------------------
    # updt link -> hf_its_clm_msg (hashed file → parquet)
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svClmId").alias("CLM_ID"),
        F.col("svItsClmMsgFmtCd").alias("ITS_CLM_MSG_FMT_CD"),
        F.col("svItsClmMsgId").alias("ITS_CLM_MSG_ID"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("ITS_CLM_MSG_SK")
    )
    write_files(
        df_updt,
        f"{adls_path}/ITSClmMsgPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # NewKeys link (insert only) -> sequential file
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("svItsClmMsgFmtCd").alias("ITS_CLM_MSG_FMT_CD"),
            F.col("svItsClmMsgId").alias("ITS_CLM_MSG_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("ITS_CLM_MSG_SK")
        )
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_ITS_CLM_MSG.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Keys link DataFrame for merging
    df_keys = df_enriched.select(
        F.col("svSK").alias("ITS_CLM_MSG_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svClmId").alias("CLM_ID"),
        F.col("svItsClmMsgFmtCd").alias("ITS_CLM_MSG_FMT_CD"),
        F.col("svItsClmMsgId").alias("ITS_CLM_MSG_ID"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ------------------------------------------------------------------
    # Merge logic to create container output
    join_cond = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.CLM_ID") == F.col("k.CLM_ID")) &
        (F.col("all.ITS_CLM_MSG_FMT_CD") == F.col("k.ITS_CLM_MSG_FMT_CD")) &
        (F.col("all.ITS_CLM_MSG_ID") == F.col("k.ITS_CLM_MSG_ID"))
    )
    df_key_output = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_cond, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.ITS_CLM_MSG_SK"),
            F.col("all.CLM_ID"),
            F.col("all.ITS_CLM_MSG_FMT_CD"),
            F.col("all.ITS_CLM_MSG_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.ITS_CLM_MSG_DESC")
        )
    )
    # ------------------------------------------------------------------
    return df_key_output