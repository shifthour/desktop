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
# MAGIC Bhoomi Dasari                 10/02/2007         3028                              Originally Programmed                            devlEDW10                      Steph Goddard             10/18/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, length, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

schema_IdsAplCntct = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("APL_CNTCT_ID", StringType(), False)
])

df_IdsAplCntct = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsAplCntct)
    .csv(f"{adls_path}/balancing/snapshot/IDS_APL_CNTCT.uniq")
)

df_transform = (
    df_IdsAplCntct.alias("Snapshot")
    .join(
        df_hf_cdma_codes.alias("SrcSysCdLkup"),
        col("Snapshot.SRC_SYS_CD_SK") == col("SrcSysCdLkup.CD_MPPNG_SK"),
        how="left"
    )
)

df_transform = df_transform.select(
    when(
        (col("SrcSysCdLkup.TRGT_CD").isNull()) | (length(trim(col("SrcSysCdLkup.TRGT_CD"))) == 0),
        lit("NA")
    ).otherwise(col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    col("Snapshot.APL_ID").alias("APL_ID"),
    col("Snapshot.SEQ_NO").alias("APL_CNTCT_SEQ_NO"),
    col("Snapshot.APL_CNTCT_ID").alias("APL_CNTCT_ID")
)

df_transform = df_transform.withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "APL_ID", rpad(col("APL_ID"), <...>, " ")
).withColumn(
    "APL_CNTCT_ID", rpad(col("APL_CNTCT_ID"), <...>, " ")
)

df_transform = df_transform.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_CNTCT_SEQ_NO",
    "APL_CNTCT_ID"
)

write_files(
    df_transform,
    f"{adls_path}/load/B_APL_CNTCT_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)