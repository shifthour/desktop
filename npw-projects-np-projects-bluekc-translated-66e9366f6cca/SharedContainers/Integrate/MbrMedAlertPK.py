# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
JobName        : MbrMedAlertPK
FolderPath     : Shared Containers/PrimaryKey
Description    : Shared container used for Primary Keying of MbrMedAlert job
Called By      : ImpProMbrMedAlertExtr

Annotations
-----------
Used by

ImpProMbrMedAlertExtr
IDS Primary Key Container for ImpProMbrMedAlertExtr
primary key hash file only contains current run keys and is cleared before writing
update primary key table (K_MBR_MED_ALERT) with new keys created today
Assign primary surrogate key
Hash file (hf_mbr_med_alert_allcol) cleared in calling job
join primary key info with table info
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
"""
# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------
def run_MbrMedAlertPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack parameters
    # --------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    # --------------------------------------------------
    # Stage: hf_mbr_med_alert_allcol (scenario a – intermediate hash-file)
    # --------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "PRCS_YR_MO_SK", "MED_ALERT_ID"],
        [("SRC_SYS_CD_SK", "A")]
    )
    # --------------------------------------------------
    # Stage: K_MBR_MED_ALERT_TEMP (DB2Connector)
    # --------------------------------------------------
    (
        df_Transform.write.format("jdbc")
        .mode("overwrite")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MBR_MED_ALERT_TEMP")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MBR_MED_ALERT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    extract_query = f"""
SELECT  k.MBR_MED_ALERT_SK,
        w.SRC_SYS_CD_SK,
        w.MBR_UNIQ_KEY,
        w.PRCS_YR_MO_SK,
        w.MED_ALERT_ID,
        k.CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.K_MBR_MED_ALERT_TEMP w,
     {IDSOwner}.K_MBR_MED_ALERT k
WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
  AND w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
  AND w.PRCS_YR_MO_SK = k.PRCS_YR_MO_SK
  AND w.MED_ALERT_ID  = k.MED_ALERT_ID
UNION
SELECT -1,
       w2.SRC_SYS_CD_SK,
       w2.MBR_UNIQ_KEY,
       w2.PRCS_YR_MO_SK,
       w2.MED_ALERT_ID,
       {CurrRunCycle}
FROM {IDSOwner}.K_MBR_MED_ALERT_TEMP w2
WHERE NOT EXISTS (
      SELECT k2.MBR_MED_ALERT_SK
      FROM {IDSOwner}.K_MBR_MED_ALERT k2
      WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
        AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
        AND w2.PRCS_YR_MO_SK = k2.PRCS_YR_MO_SK
        AND w2.MED_ALERT_ID  = k2.MED_ALERT_ID)
"""
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # --------------------------------------------------
    # Transformer: PrimaryKey
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("MBR_MED_ALERT_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svSK", F.lit(None).cast("long"))
        .withColumn("svCrtRunCycExctnSk", F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svMedAlertId", F.trim(F.col("MED_ALERT_ID")))
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # updt link
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        F.col("svMedAlertId").alias("MED_ALERT_ID"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("MBR_MED_ALERT_SK")
    )
    # NewKeys link
    df_newKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            F.col("svMedAlertId").alias("MED_ALERT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MBR_MED_ALERT_SK")
        )
    )
    # Keys link
    df_keys = df_enriched.select(
        F.col("svSK").alias("MBR_MED_ALERT_SK"),
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        F.col("svMedAlertId").alias("MED_ALERT_ID"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # --------------------------------------------------
    # Transformer: Merge
    # --------------------------------------------------
    join_cond = [
        F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
        F.col("all.MBR_UNIQ_KEY")  == F.col("k.MBR_UNIQ_KEY"),
        F.col("all.PRCS_YR_MO_SK") == F.col("k.PRCS_YR_MO_SK"),
        F.col("all.MED_ALERT_ID")  == F.col("k.MED_ALERT_ID")
    ]
    df_merge = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_cond, "left")
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
            F.col("k.MBR_MED_ALERT_SK").alias("MBR_MED_ALERT_SK"),
            F.col("all.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.col("all.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
            F.col("all.MED_ALERT_ID").alias("MED_ALERT_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.MED_ALERT_SK").alias("MED_ALERT_SK"),
            F.col("all.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK")
        )
    )
    # --------------------------------------------------
    # Stage: K_MBR_MED_ALERT (Sequential File)
    # --------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_MBR_MED_ALERT.dat"
    write_files(
        df_newKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    # Stage: hf_mbr_med_alert (Parquet replacement)
    # --------------------------------------------------
    parquet_file_path_updt = f"{adls_path}/MbrMedAlertPK_updt.parquet"
    write_files(
        df_updt,
        parquet_file_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_merge
# COMMAND ----------