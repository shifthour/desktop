# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def run_CustSvcTaskNotePK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # hf_cust_svc_task_note_allcol (scenario a – intermediate hash file)
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        [
            "SYS_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
        ],
        [],
    )
    # ------------------------------------------------------------------
    # K_CUST_SVC_TASK_NOTE_TEMP  (write temp table, then extract)
    execute_dml(
        f"DELETE FROM {IDSOwner}.K_CUST_SVC_TASK_NOTE_TEMP",
        ids_jdbc_url,
        ids_jdbc_props,
    )
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CUST_SVC_TASK_NOTE_TEMP")
        .mode("append")
        .save()
    )
    extract_query = f"""
    SELECT k.CUST_SVC_TASK_NOTE_SK,
           w.SRC_SYS_CD_SK,
           w.CUST_SVC_ID,
           w.TASK_SEQ_NO,
           w.CUST_SVC_TASK_NOTE_LOC_CD,
           w.NOTE_SEQ_NO,
           w.LAST_UPDT_DTM,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CUST_SVC_TASK_NOTE_TEMP w,
           {IDSOwner}.K_CUST_SVC_TASK_NOTE k
     WHERE w.SRC_SYS_CD_SK            = k.SRC_SYS_CD_SK
       AND w.CUST_SVC_ID              = k.CUST_SVC_ID
       AND w.TASK_SEQ_NO              = k.TASK_SEQ_NO
       AND w.CUST_SVC_TASK_NOTE_LOC_CD= k.CUST_SVC_TASK_NOTE_LOC_CD
       AND w.NOTE_SEQ_NO              = k.NOTE_SEQ_NO
       AND w.LAST_UPDT_DTM            = k.LAST_UPDT_DTM
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CUST_SVC_ID,
           w2.TASK_SEQ_NO,
           w2.CUST_SVC_TASK_NOTE_LOC_CD,
           w2.NOTE_SEQ_NO,
           w2.LAST_UPDT_DTM,
           {CurrRunCycle}
      FROM {IDSOwner}.K_CUST_SVC_TASK_NOTE_TEMP w2
     WHERE NOT EXISTS (
        SELECT 1
          FROM {IDSOwner}.K_CUST_SVC_TASK_NOTE k2
         WHERE w2.SRC_SYS_CD_SK             = k2.SRC_SYS_CD_SK
           AND w2.CUST_SVC_ID               = k2.CUST_SVC_ID
           AND w2.TASK_SEQ_NO               = k2.TASK_SEQ_NO
           AND w2.CUST_SVC_TASK_NOTE_LOC_CD = k2.CUST_SVC_TASK_NOTE_LOC_CD
           AND w2.NOTE_SEQ_NO               = k2.NOTE_SEQ_NO
           AND w2.LAST_UPDT_DTM             = k2.LAST_UPDT_DTM
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
    df_enriched = (
        df_w_extract.withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("CUST_SVC_TASK_NOTE_SK") == -1, F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CUST_SVC_TASK_NOTE_SK",
            F.when(F.col("CUST_SVC_TASK_NOTE_SK") == -1, F.lit(None)).otherwise(
                F.col("CUST_SVC_TASK_NOTE_SK")
            ),
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )
    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "CUST_SVC_TASK_NOTE_SK", <schema>, <secret_name>
    )
    # ------------------------------------------------------------------
    # updt (hash file write -> parquet)
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_NOTE_SK",
    )
    write_files(
        df_updt,
        f"{adls_path}/CustSvcTaskNotePK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None,
    )
    # ------------------------------------------------------------------
    # NewKeys sequential file
    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
            "CRT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_TASK_NOTE_SK",
        )
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CUST_SVC_TASK_NOTE.dat",
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None,
    )
    # ------------------------------------------------------------------
    # Keys dataframe for merge
    df_keys = df_enriched.select(
        "CUST_SVC_TASK_NOTE_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK",
    )
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
        & (F.col("all.CUST_SVC_ID") == F.col("k.CUST_SVC_ID"))
        & (F.col("all.TASK_SEQ_NO") == F.col("k.TASK_SEQ_NO"))
        & (
            F.col("all.CUST_SVC_TASK_NOTE_LOC_CD")
            == F.col("k.CUST_SVC_TASK_NOTE_LOC_CD")
        )
        & (F.col("all.NOTE_SEQ_NO") == F.col("k.NOTE_SEQ_NO"))
        & (F.col("all.LAST_UPDT_DTM") == F.col("k.LAST_UPDT_DTM"))
    )
    df_key = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
            F.col("all.CUST_SVC_ID").alias("CUST_SVC_ID"),
            F.col("all.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
            F.col("all.CUST_SVC_TASK_NOTE_LOC_CD").alias("CUST_SVC_TASK_NOTE_LOC_CD"),
            F.col("all.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
            F.col("all.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
            F.col("all.LAST_UPDT_USER").alias("LAST_UPDT_USER"),
            F.col("all.NOTE_SUM_TX").alias("NOTE_SUM_TX"),
        )
    )
    return df_key
# COMMAND ----------