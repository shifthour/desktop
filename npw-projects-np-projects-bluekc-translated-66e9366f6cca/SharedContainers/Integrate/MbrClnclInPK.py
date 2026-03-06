# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_MbrClnclInPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # -------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            "CLNCL_IN_ID",
        ],
        sort_cols=[],
    )
    # -------------------------------------------------------------------
    temp_table = f"{IDSOwner}.K_MBR_CLNCL_IN_TEMP"
    execute_dml(f"TRUNCATE TABLE {temp_table}", ids_jdbc_url, ids_jdbc_props)
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("append")
        .save()
    )
    extract_query = f"""
        SELECT  k.MBR_CLNCL_IN_SK,
                w.SRC_SYS_CD_SK,
                w.MBR_UNIQ_KEY,
                w.PRCS_YR_MO_SK,
                w.CLNCL_IN_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MBR_CLNCL_IN_TEMP w,
             {IDSOwner}.K_MBR_CLNCL_IN k
        WHERE w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
          AND w.MBR_UNIQ_KEY       = k.MBR_UNIQ_KEY
          AND w.PRCS_YR_MO_SK      = k.PRCS_YR_MO_SK
          AND w.CLNCL_IN_ID        = k.CLNCL_IN_ID
        UNION
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.MBR_UNIQ_KEY,
                w2.PRCS_YR_MO_SK,
                w2.CLNCL_IN_ID,
                {CurrRunCycle}
        FROM {IDSOwner}.K_MBR_CLNCL_IN_TEMP w2
        WHERE NOT EXISTS (
            SELECT k2.MBR_CLNCL_IN_SK
            FROM {IDSOwner}.K_MBR_CLNCL_IN k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
              AND w2.PRCS_YR_MO_SK = k2.PRCS_YR_MO_SK
              AND w2.CLNCL_IN_ID   = k2.CLNCL_IN_ID
        )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # -------------------------------------------------------------------
    df_enriched = (
        df_W_Extract.withColumn(
            "svInstUpdt",
            F.when(F.col("MBR_CLNCL_IN_SK") == -1, F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn(
            "svSK",
            F.when(F.col("MBR_CLNCL_IN_SK") == -1, F.lit(None)).otherwise(
                F.col("MBR_CLNCL_IN_SK")
            ),
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("MBR_CLNCL_IN_SK") == -1, F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
        .withColumn("svClnclId", F.trim(F.col("CLNCL_IN_ID")))
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
    )
    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "svSK", <schema>, <secret_name>
    )
    # -------------------------------------------------------------------
    df_updt = df_enriched.select(
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        F.col("svClnclId").alias("CLNCL_IN_ID"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("MBR_CLNCL_IN_SK"),
    )
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I").select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            F.col("svClnclId").alias("CLNCL_IN_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MBR_CLNCL_IN_SK"),
        )
    )
    df_keys = df_enriched.select(
        F.col("svSK").alias("MBR_CLNCL_IN_SK"),
        "SRC_SYS_CD_SK",
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        F.col("svClnclId").alias("CLNCL_IN_ID"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    )
    # -------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/MbrClnclInPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_MBR_CLNCL_IN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )
    # -------------------------------------------------------------------
    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            [
                F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
                F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY"),
                F.col("all.PRCS_YR_MO_SK") == F.col("k.PRCS_YR_MO_SK"),
                F.col("all.CLNCL_IN_ID") == F.col("k.CLNCL_IN_ID"),
            ],
            "left",
        )
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
            F.col("k.MBR_CLNCL_IN_SK"),
            F.col("all.MBR_UNIQ_KEY"),
            F.col("all.PRCS_YR_MO_SK"),
            F.col("all.CLNCL_IN_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.MBR_MED_MESRS_CD"),
            F.col("all.MIN_DT"),
            F.col("all.MAX_DT"),
            F.col("all.OCCUR_CT"),
        )
    )
    # -------------------------------------------------------------------
    return df_Key
# COMMAND ----------