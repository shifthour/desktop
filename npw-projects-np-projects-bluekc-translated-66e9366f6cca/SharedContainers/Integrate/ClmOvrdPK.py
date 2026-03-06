# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName: ClmOvrdPK
FolderPath: Shared Containers/PrimaryKey
Description: * VC LOGS *
Annotation:
    join primary key info with table info
    Load IDS temp. table
    Temp table is tuncated before load and runstat done after load
    SQL joins temp table with key table to assign known keys
    Assign primary surrogate key
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
# COMMAND ----------
def run_ClmOvrdPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    SrcSysCd            = params["SrcSysCd"]
    SrcSysCdSk          = params["SrcSysCdSk"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # hf_clm_ovrd_allcol (intermediate hash-file -> dedup)
    df_hf_clm_ovrd_allcol = dedup_sort(
        df_AllColl,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_OVRD_ID"],
        []
    )
    # ------------------------------------------------------------------
    # K_CLM_OVRD_TEMP  (load temp table)
    table_temp = f"{IDSOwner}.K_CLM_OVRD_TEMP"
    execute_dml(f"DROP TABLE IF EXISTS {table_temp}", ids_jdbc_url, ids_jdbc_props)
    execute_dml(
        f"""CREATE TABLE {table_temp} (
            SRC_SYS_CD_SK INTEGER NOT NULL,
            CLM_ID VARCHAR(20) NOT NULL,
            CLM_OVRD_ID VARCHAR(20) NOT NULL,
            PRIMARY KEY (SRC_SYS_CD_SK, CLM_ID, CLM_OVRD_ID)
        )""",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform.write.format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", table_temp)
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {table_temp} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ------------------------------------------------------------------
    # W_Extract (query)
    extract_query = f"""
    SELECT k.CLM_OVRD_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_OVRD_ID,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_OVRD_TEMP w,
         {IDSOwner}.K_CLM_OVRD k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.CLM_ID        = k.CLM_ID
      AND w.CLM_OVRD_ID   = k.CLM_OVRD_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_OVRD_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_OVRD_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.CLM_OVRD_SK
        FROM {IDSOwner}.K_CLM_OVRD k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID        = k2.CLM_ID
          AND w2.CLM_OVRD_ID   = k2.CLM_OVRD_ID
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
    # PrimaryKey transformer
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("CLM_OVRD_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn("svSK", when(col("svInstUpdt") == lit("U"), col("CLM_OVRD_SK")).otherwise(lit(None)))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # updt link
    df_updt = (
        df_enriched.select(
            col("svSrcSysCd").alias("SRC_SYS_CD_SK"),
            col("CLM_ID"),
            col("CLM_OVRD_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_OVER_PAYMT_SK")
        )
    )
    write_files(
        df_updt,
        f"{adls_path}/ClmOvrdPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # NewKeys link
    df_newkeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I")).select(
            col("SRC_SYS_CD_SK"),
            col("CLM_ID"),
            col("CLM_OVRD_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_OVER_PAYMT_SK")
        )
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_OVRD.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Keys link
    df_keys = (
        df_enriched.select(
            col("svSK").alias("CLM_OVRD_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("CLM_ID"),
            col("CLM_OVRD_ID"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # ------------------------------------------------------------------
    # Merge transformer
    df_key = (
        df_hf_clm_ovrd_allcol.alias("all")
        .join(
            df_keys.alias("k"),
            ["SRC_SYS_CD_SK", "CLM_ID", "CLM_OVRD_ID"],
            "left"
        )
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.CLM_OVRD_SK").alias("CLM_OVRD_SK"),
            col("all.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            col("all.CLM_ID").alias("CLM_ID"),
            col("all.CLM_OVRD_ID").alias("CLM_OVRD_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.CLM_SK").alias("CLM_SK"),
            col("all.USER_ID").alias("USER_ID"),
            col("all.CLM_OVRD_EXCD").alias("CLM_OVRD_EXCD"),
            col("all.OVERRIDE_DT").alias("OVERRIDE_DT"),
            col("all.OVERRIDE_AMT").alias("OVERRIDE_AMT"),
            col("all.OVERRIDE_VAL_DESC").alias("OVERRIDE_VAL_DESC")
        )
    )
    return df_key
# COMMAND ----------