# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, when

# COMMAND ----------
def run_ClmLtrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    IBM DataStage shared container ‘ClmLtrPK’ converted to PySpark.
    
    IDS Primary Key Container for Claim Letter (Used by FctsClmLtrExtr)
    Processing: Loads temp table, finds existing keys, creates new keys for records processed.
    Hash file (hf_clm_ltr_allcol) cleared in the calling program.
    SQL joins temp table with key table to assign known keys.
    Primary key hash file only contains current run keys.
    Update primary key table K_CLM_LTR with new keys created today.
    Assign primary surrogate key.
    """
    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle           = params["CurrRunCycle"]
    IDSOwner               = params["IDSOwner"]
    ids_secret_name        = params["ids_secret_name"]
    ids_jdbc_url           = params["ids_jdbc_url"]
    ids_jdbc_props         = params["ids_jdbc_props"]
    adls_path              = params["adls_path"]
    adls_path_raw          = params["adls_path_raw"]
    adls_path_publish      = params["adls_path_publish"]
    # --------------------------------------------------
    # Step 1 – Persist incoming Transform data to temp table {IDSOwner}.K_CLM_LTR_TEMP
    (
        df_Transform.write.mode("overwrite")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LTR_TEMP")
        .save()
    )
    # --------------------------------------------------
    # Step 2 – Extract existing/new key information (W_Extract)
    extract_query = f"""
    SELECT
        k.CLM_LTR_SK,
        t.SRC_SYS_CD_SK,
        t.CLM_ID,
        t.CLM_LTR_SEQ_NO,
        k.CRT_RUN_CYC_EXCTN_SK
    FROM
        {IDSOwner}.K_CLM_LTR_TEMP t
    JOIN
        {IDSOwner}.K_CLM_LTR k
          ON t.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND t.CLM_ID        = k.CLM_ID
         AND t.CLM_LTR_SEQ_NO= k.CLM_LTR_SEQ_NO
    UNION
    SELECT
        -1,
        t2.SRC_SYS_CD_SK,
        t2.CLM_ID,
        t2.CLM_LTR_SEQ_NO,
        {CurrRunCycle}
    FROM
        {IDSOwner}.K_CLM_LTR_TEMP t2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM_LTR k2
        WHERE  t2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND  t2.CLM_ID        = k2.CLM_ID
          AND  t2.CLM_LTR_SEQ_NO= k2.CLM_LTR_SEQ_NO
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
    # Step 3 – PrimaryKey transformer logic
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    when(col("CLM_LTR_SK") == lit(-1), lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit("FACETS"))
        .withColumn("svSK",
                    when(col("svInstUpdt") == lit("I"), lit(None)).otherwise(col("CLM_LTR_SK")))
        .withColumn("svCrtRunCycExctnSk",
                    when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # --------------------------------------------------
    # Step 4 – Prepare outputs from PrimaryKey transformer
    # updt link
    df_updt = (
        df_enriched.select(
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("CLM_ID"),
            col("CLM_LTR_SEQ_NO"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_LTR_SK")
        )
    )
    # NewKeys link
    df_newkeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
                   .select(
                        col("SRC_SYS_CD_SK"),
                        col("CLM_ID"),
                        col("CLM_LTR_SEQ_NO"),
                        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
                        col("svSK").alias("CLM_LTR_SK")
                    )
    )
    # Keys link
    df_keys = (
        df_enriched.select(
            col("svSK").alias("CLM_LTR_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("CLM_ID"),
            col("CLM_LTR_SEQ_NO"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # Step 5 – hf_clm_ltr_allcol (intermediate hash-file)
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "CLM_ID", "CLM_LTR_SEQ_NO"],
        sort_cols=[]
    )
    # --------------------------------------------------
    # Step 6 – Merge transformer logic
    join_cond = (
        (df_AllCol_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"]) &
        (df_AllCol_dedup["CLM_ID"]        == df_keys["CLM_ID"]) &
        (df_AllCol_dedup["CLM_LTR_SEQ_NO"]== df_keys["CLM_LTR_SEQ_NO"])
    )
    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_cond, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD"),
            col("all.DISCARD_IN"),
            col("all.PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT"),
            col("all.ERR_CT"),
            col("all.RECYCLE_CT"),
            col("all.SRC_SYS_CD"),
            col("all.PRI_KEY_STRING"),
            col("k.CLM_LTR_SK"),
            col("all.CLM_ID"),
            col("all.CLM_LTR_SEQ_NO"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            col("all.LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.CLM_SK"),
            col("all.LAST_UPDT_USER_ID"),
            col("all.CLM_LTR_REPRT_STTUS_CD"),
            col("all.CLM_LTR_TYP_CD"),
            col("all.FLW_UP_IN"),
            col("all.MAIL_IN"),
            col("all.PRT_IN"),
            col("all.RCV_IN"),
            col("all.RQST_IN"),
            col("all.SUBMT_IN"),
            col("all.LAST_UPDT_DT"),
            col("all.MAIL_DT"),
            col("all.PRT_DT"),
            col("all.RCV_DT"),
            col("all.RQST_DT"),
            col("all.SUBMT_DT"),
            col("all.FROM_NM"),
            col("all.LTR_DESC"),
            col("all.REF_NM"),
            col("all.SUBJ_TX"),
            col("all.TO_NM")
        )
    )
    # --------------------------------------------------
    # Step 7 – Write out files/tables
    # hf_clm_letter parquet
    write_files(
        df_updt,
        f"{adls_path}/ClmLtrPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # Sequential file K_CLM_LTR.dat
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_LTR.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    return df_Key
# COMMAND ----------