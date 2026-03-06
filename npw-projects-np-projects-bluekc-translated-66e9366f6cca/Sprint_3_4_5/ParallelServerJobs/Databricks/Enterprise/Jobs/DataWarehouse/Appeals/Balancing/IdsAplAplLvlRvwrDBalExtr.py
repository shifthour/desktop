# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/28/07 12:47:07 Batch  14577_46088 PROMOTE bckcetl edw10 dsadm bls for hs
# MAGIC ^1_1 11/28/07 11:24:20 Batch  14577_41063 INIT bckcett testEDW10 dsadm bls for hs
# MAGIC ^1_1 11/27/07 15:06:26 Batch  14576_54393 PROMOTE bckcett testEDW10 u03651 steph for Hugh
# MAGIC ^1_1 11/27/07 14:52:16 Batch  14576_53540 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsEdwMedMgtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/12/2007         3264                              Originally Programmed                            devlEDW10          
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to       devlEDW10                     Steph Goddard             10/18/2007
# MAGIC                                                                                                          Snapshot table extract


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

schema_IdsAplRvwr = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("APL_ID", StringType(), True),
    StructField("APL_LVL_SEQ_NO", IntegerType(), True),
    StructField("APL_RVWR_ID", StringType(), True),
    StructField("EFF_DT_SK", StringType(), True),
    StructField("APL_RVWR_NM", StringType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("TERM_DT_SK", StringType(), True)
])

df_IdsAplRvwr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsAplRvwr)
    .load(f"{adls_path}/balancing/snapshot/IDS_APL_LVL_RVWR.uniq")
)

df_enriched = df_IdsAplRvwr.alias("Snapshot").join(
    df_hf_cdma_codes.alias("SrcSysCdLkup"),
    F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
    "left"
)

src_sys_cd_col = F.when(
    F.col("SrcSysCdLkup.TRGT_CD").isNull() | (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
    F.lit("NA")
).otherwise(F.col("SrcSysCdLkup.TRGT_CD"))

df_enriched = df_enriched.select(
    src_sys_cd_col.alias("SRC_SYS_CD"),
    F.col("Snapshot.APL_ID").alias("APL_ID"),
    F.col("Snapshot.APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("Snapshot.APL_RVWR_ID").alias("APL_RVWR_ID"),
    F.col("Snapshot.EFF_DT_SK").alias("APL_LVL_RVWR_EFF_DT_SK"),
    F.col("Snapshot.APL_RVWR_NM").alias("APL_RVWR_NM"),
    F.col("Snapshot.TERM_DT_SK").alias("APL_LVL_RVWR_TERM_DT_SK"),
    F.col("Snapshot.LAST_UPDT_DTM").alias("LAST_UPDT_DT_SK")
)

df_enriched = df_enriched.withColumn(
    "APL_LVL_RVWR_EFF_DT_SK",
    F.rpad(F.col("APL_LVL_RVWR_EFF_DT_SK"), 10, " ")
).withColumn(
    "APL_LVL_RVWR_TERM_DT_SK",
    F.rpad(F.col("APL_LVL_RVWR_TERM_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_DT_SK",
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ")
)

write_files(
    df_enriched,
    f"{adls_path}/load/B_APL_LVL_RVWR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)