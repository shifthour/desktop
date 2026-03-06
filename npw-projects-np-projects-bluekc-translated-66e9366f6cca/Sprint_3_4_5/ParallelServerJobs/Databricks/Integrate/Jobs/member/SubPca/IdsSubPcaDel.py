# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 12/02/09 12:38:39 Batch  15312_45563 INIT bckcett:31540 devlIDSnew u150906 3556-MbrCDC_Ralph_devlIDSnew       Maddy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  FctsMbrAccumSeq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:   Delete IDS Sub Pca Change Data Capture rows.   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  11/12/2009    3556 CDC                        Originally Programmed                            devlIDSnew                    Steph Goddard            11/23/2009

# MAGIC File created in driver job FctsSubPcaDrvrExtr
# MAGIC Apply Deletions to IDS Sub Pca table,
# MAGIC Copy deletes to EDW file path for EDW jobs to process
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, lit, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1581')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','122')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWFilePath = get_widget_value('EDWFilePath','')

schema_SubPcaDelCrf = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("SBSB_CK", IntegerType(), False),
    StructField("HSAI_ACC_SFX", StringType(), False),
    StructField("SBHS_PLAN_YR_DT", TimestampType(), False),
    StructField("SBHS_EFF_DT", TimestampType(), False),
    StructField("OP", StringType(), False)
])

df_SubPcaDelCrf = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .option("sep", "|")
    .schema(schema_SubPcaDelCrf)
    .load(f"{adls_path}/load/IdsSubPca.del")
)

df_PurgeTrnMbrAccum = df_SubPcaDelCrf

df_IdsDelete = df_PurgeTrnMbrAccum.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    col("HSAI_ACC_SFX").alias("SUB_PCA_ACCUM_PFX_ID"),
    substring(col("SBHS_PLAN_YR_DT").cast("string"), 0, 10).alias("PLN_YR_BEG_DT_SK"),
    substring(col("SBHS_EFF_DT").cast("string"), 0, 10).alias("EFF_DT_SK")
)

df_EdwDeleteFile = df_PurgeTrnMbrAccum.select(
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    col("HSAI_ACC_SFX").alias("SUB_PCA_ACCUM_PFX_ID"),
    substring(col("SBHS_PLAN_YR_DT").cast("string"), 0, 10).alias("PLN_YR_BEG_DT_SK"),
    substring(col("SBHS_EFF_DT").cast("string"), 0, 10).alias("EFF_DT_SK"),
    col("OP").alias("OP")
)

final_df_EdwDeleteFile = df_EdwDeleteFile.select(
    rpad(col("SRC_SYS_CD"), 10, " ").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY"),
    rpad(col("SUB_PCA_ACCUM_PFX_ID"), 4, " ").alias("SUB_PCA_ACCUM_PFX_ID"),
    rpad(col("PLN_YR_BEG_DT_SK"), 10, " ").alias("PLN_YR_BEG_DT_SK"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("OP"), 1, " ").alias("OP")
)

write_files(
    final_df_EdwDeleteFile,
    f"{adls_path}/load/FctsSubPcaDel.EdwSubPcaDel.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_SUB_PCA_write = df_IdsDelete.select(
    col("SRC_SYS_CD_SK"),
    col("SUB_UNIQ_KEY"),
    rpad(col("SUB_PCA_ACCUM_PFX_ID"), 4, " ").alias("SUB_PCA_ACCUM_PFX_ID"),
    rpad(col("PLN_YR_BEG_DT_SK"), 10, " ").alias("PLN_YR_BEG_DT_SK"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsSubPcaDel_SUB_PCA_temp",
    jdbc_url,
    jdbc_props
)

(
    df_SUB_PCA_write.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsSubPcaDel_SUB_PCA_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.SUB_PCA AS T
USING STAGING.IdsSubPcaDel_SUB_PCA_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
   AND T.SUB_PCA_ACCUM_PFX_ID = S.SUB_PCA_ACCUM_PFX_ID
   AND T.PLN_YR_BEG_DT_SK = S.PLN_YR_BEG_DT_SK
   AND T.EFF_DT_SK = S.EFF_DT_SK
WHEN MATCHED THEN
  DELETE;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)