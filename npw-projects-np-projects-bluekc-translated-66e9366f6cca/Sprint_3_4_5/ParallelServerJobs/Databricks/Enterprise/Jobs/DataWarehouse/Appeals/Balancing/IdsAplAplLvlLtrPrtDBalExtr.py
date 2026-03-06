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
# MAGIC CALLED BY:    IdsEdwAplBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/02/2007         3028                              Originally Programmed                            devlEDW10                      Steph Goddard            10/18/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

schema_IdsAplLvlLtrPrt = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("APL_LVL_LTR_STYLE_CD", StringType(), False),
    StructField("LTR_SEQ_NO", IntegerType(), False),
    StructField("LTR_DEST_ID", StringType(), False),
    StructField("PRT_SEQ_NO", IntegerType(), False),
    StructField("PRT_DT_SK", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False)
])

df_IdsAplLvlLtrPrt = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsAplLvlLtrPrt)
    .load(f"{adls_path}/balancing/snapshot/IDS_APL_LVL_LTR_PRT.uniq")
)

df_joined = df_IdsAplLvlLtrPrt.alias("Snapshot").join(
    df_hf_cdma_codes.alias("SrcSysCdLkup"),
    F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
    "left"
)

df_enriched = df_joined.select(
    F.when(
        F.col("SrcSysCdLkup.TRGT_CD").isNull() | (F.length(F.col("SrcSysCdLkup.TRGT_CD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Snapshot.APL_ID").alias("APL_ID"),
    F.col("Snapshot.SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("Snapshot.APL_LVL_LTR_STYLE_CD").alias("APL_LVL_LTR_STYLE_CD"),
    F.col("Snapshot.LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
    F.col("Snapshot.LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
    F.col("Snapshot.PRT_SEQ_NO").alias("APL_LVL_LTR_PRT_SEQ_NO"),
    F.col("Snapshot.PRT_DT_SK").alias("APL_LVL_LTR_PRT_DT_SK"),
    F.date_format(F.col("Snapshot.LAST_UPDT_DTM"), "yyyy-MM-dd").alias("LAST_UPDT_DT_SK")
)

df_final = df_enriched.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("APL_ID"), <...>, " ").alias("APL_ID"),
    F.col("APL_LVL_SEQ_NO"),
    F.rpad(F.col("APL_LVL_LTR_STYLE_CD"), <...>, " ").alias("APL_LVL_LTR_STYLE_CD"),
    F.col("APL_LVL_LTR_SEQ_NO"),
    F.rpad(F.col("APL_LVL_LTR_DEST_ID"), <...>, " ").alias("APL_LVL_LTR_DEST_ID"),
    F.col("APL_LVL_LTR_PRT_SEQ_NO"),
    F.rpad(F.col("APL_LVL_LTR_PRT_DT_SK"), 10, " ").alias("APL_LVL_LTR_PRT_DT_SK"),
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/B_APL_LVL_LTR_PRT_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)