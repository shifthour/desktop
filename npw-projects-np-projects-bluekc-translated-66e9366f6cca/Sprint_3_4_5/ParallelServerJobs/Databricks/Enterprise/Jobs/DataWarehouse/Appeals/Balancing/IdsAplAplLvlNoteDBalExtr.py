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
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to        devlEDW10                   Steph Goddard              10/18/2007
# MAGIC                                                                                                          Snapshot table extract


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")
df_hf_cdma_codes = df_hf_cdma_codes.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")

schema_IdsAplLvlNote = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("LVL_SEQ_NO", IntegerType(), False),
    StructField("NOTE_SEQ_NO", IntegerType(), False),
    StructField("NOTE_DEST_ID", StringType(), False),
    StructField("APL_LVL_SK", IntegerType(), False)
])

df_IdsAplLvlNote = (
    spark.read.format("csv")
    .schema(schema_IdsAplLvlNote)
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .load(f"{adls_path}/balancing/snapshot/IDS_APL_LVL_NOTE.uniq")
    .select("SRC_SYS_CD_SK","APL_ID","LVL_SEQ_NO","NOTE_SEQ_NO","NOTE_DEST_ID","APL_LVL_SK")
)

df_enriched = (
    df_IdsAplLvlNote.alias("Snapshot")
    .join(
        df_hf_cdma_codes.alias("SrcSysCdLkup"),
        F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.when(
            (F.col("SrcSysCdLkup.TRGT_CD").isNull()) | (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
        F.col("Snapshot.APL_ID").alias("APL_ID"),
        F.col("Snapshot.LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
        F.col("Snapshot.NOTE_SEQ_NO").alias("APL_LVL_NOTE_SEQ_NO"),
        F.col("Snapshot.NOTE_DEST_ID").alias("APL_LVL_NOTE_DEST_ID"),
        F.col("Snapshot.APL_LVL_SK").alias("APL_LVL_SK")
    )
)

df_final = df_enriched.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("APL_ID"), <...>, " ").alias("APL_ID"),
    F.col("APL_LVL_SEQ_NO"),
    F.col("APL_LVL_NOTE_SEQ_NO"),
    F.rpad(F.col("APL_LVL_NOTE_DEST_ID"), <...>, " ").alias("APL_LVL_NOTE_DEST_ID"),
    F.col("APL_LVL_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/B_APL_LVL_NOTE_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)