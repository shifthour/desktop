# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  F2FMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Common Extract job for the MBR_SRVY table in IDS. The job processes landing files generated from the Face2Face, NDBH and Healthways File validation jobs and creates the key file to be loaded into IDS MBR_SRVY table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari        2011-04-06               4673                       Original Programming                                                IntegrateWrhsDevl                  SAndrew                  2011-04-20
# MAGIC Kalyan Neelam         2014-10-15               TFS 9558                Added balancing snap shot file                                IntegrateNewDevl                 Bhoomi Dasari          10/20/2014

# MAGIC MBR_SRVY Extract Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1000')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2011-04-06')
InFile = get_widget_value('InFile','')
OutFile = get_widget_value('OutFile','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_Srvy_Common_File = StructType([
    StructField("MBR_SRVY_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=True),
    StructField("MBR_SRVY_TYP_CD", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SRVY_CAT_CD_SK", IntegerType(), nullable=True),
    StructField("MBR_SRVY_TYP_CD_SK", IntegerType(), nullable=True),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("MBR_SRVY_DESC", StringType(), nullable=False),
])

df_Srvy_Common_File = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_Srvy_Common_File)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

df_Transform = df_Srvy_Common_File.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.concat(trim(F.col("SRC_SYS_CD")), F.lit(";"), F.col("MBR_SRVY_TYP_CD").cast(StringType())).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_SRVY_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_CAT_CD_SK").alias("MBR_SRVY_CAT_CD_SK"),
    F.lit(0).alias("MBR_SRVY_TYP_CD_SK"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    F.col("MBR_SRVY_DESC").alias("MBR_SRVY_DESC")
)

df_Snapshot = df_Srvy_Common_File.select(
    F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD")
)

write_files(
    df_Snapshot.select("SRC_SYS_CD_SK","MBR_SRVY_TYP_CD"),
    f"{adls_path}/load/B_MBR_SRVY.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = "SELECT SRC_SYS_CD, MBR_SRVY_TYP_CD, CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_SK FROM IDS.dummy_hf_mbr_srvy"
df_hf_mbr_srvy_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_pkey_in = df_Transform.alias("Transform").join(
    df_hf_mbr_srvy_lkup.alias("lkup"),
    (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) & 
    (F.col("Transform.MBR_SRVY_TYP_CD") == F.col("lkup.MBR_SRVY_TYP_CD")),
    "left"
)

df_enriched = df_pkey_in.withColumn(
    "MBR_SRVY_SK_temp",
    F.when(F.col("lkup.MBR_SRVY_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.MBR_SRVY_SK"))
)

df_enriched = df_enriched.drop("MBR_SRVY_SK")
df_enriched = df_enriched.withColumnRenamed("MBR_SRVY_SK_temp", "MBR_SRVY_SK")
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_SRVY_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("lkup.MBR_SRVY_SK").isNull(), F.col("Transform.CRT_RUN_CYC_EXCTN_SK"))
     .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"))

df_MbrSrvy = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MBR_SRVY_SK").alias("MBR_SRVY_SK"),
    F.col("Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.MBR_SRVY_CAT_CD_SK").alias("MBR_SRVY_CAT_CD_SK"),
    F.col("Transform.MBR_SRVY_TYP_CD_SK").alias("MBR_SRVY_TYP_CD_SK"),
    F.col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("Transform.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Transform.MBR_SRVY_DESC").alias("MBR_SRVY_DESC")
)

write_files(
    df_MbrSrvy,
    f"{adls_path}/key/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_hf_mbr_srvy_updt = df_enriched.select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_SK").alias("MBR_SRVY_SK")
)

temp_table = "STAGING.IdsMbrSrvyExtr_hf_mbr_srvy_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

(
    df_hf_mbr_srvy_updt
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE IDS.dummy_hf_mbr_srvy AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.MBR_SRVY_TYP_CD = S.MBR_SRVY_TYP_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.MBR_SRVY_SK = S.MBR_SRVY_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, MBR_SRVY_TYP_CD, CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_SK)
  VALUES (S.SRC_SYS_CD, S.MBR_SRVY_TYP_CD, S.CRT_RUN_CYC_EXCTN_SK, S.MBR_SRVY_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)