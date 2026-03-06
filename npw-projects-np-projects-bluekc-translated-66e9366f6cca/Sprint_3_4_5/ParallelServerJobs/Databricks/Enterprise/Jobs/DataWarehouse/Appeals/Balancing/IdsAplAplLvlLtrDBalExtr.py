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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

schema_hf_cdma_codes = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), nullable=False),
    StructField("TRGT_CD", StringType(), nullable=True),
    StructField("TRGT_CD_NM", StringType(), nullable=True)
])

df_hf_cdma_codes = spark.read.schema(schema_hf_cdma_codes).parquet(f"{adls_path}/hf_cdma_codes.parquet")

schema_IdsAplLvlLtr = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("APL_LVL_LTR_STYLE_CD", StringType(), nullable=False),
    StructField("LTR_SEQ_NO", IntegerType(), nullable=False),
    StructField("LTR_DEST_ID", StringType(), nullable=False),
    StructField("RQST_DT_SK", StringType(), nullable=False)
])

df_IdsAplLvlLtr = (
    spark.read
    .schema(schema_IdsAplLvlLtr)
    .option("quote", "\"")
    .option("header", "false")
    .option("sep", ",")
    .csv(f"{adls_path}/balancing/snapshot/IDS_APL_LVL_LTR.uniq")
)

df_Transform_Output = (
    df_IdsAplLvlLtr.alias("snap")
    .join(
        df_hf_cdma_codes.alias("lkp"),
        col("snap.SRC_SYS_CD_SK") == col("lkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        when(
            col("lkp.TRGT_CD").isNull() | (length(trim(col("lkp.TRGT_CD"))) == 0),
            "NA"
        ).otherwise(col("lkp.TRGT_CD")).alias("SRC_SYS_CD"),
        col("snap.APL_ID").alias("APL_ID"),
        col("snap.SEQ_NO").alias("APL_LVL_SEQ_NO"),
        col("snap.APL_LVL_LTR_STYLE_CD").alias("APL_LVL_LTR_STYLE_CD"),
        col("snap.LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
        col("snap.LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
        col("snap.RQST_DT_SK").alias("APL_LVL_LTR_RQST_DT_SK")
    )
)

df_Transform_Output = df_Transform_Output.withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "APL_ID", rpad(col("APL_ID"), <...>, " ")
).withColumn(
    "APL_LVL_LTR_STYLE_CD", rpad(col("APL_LVL_LTR_STYLE_CD"), <...>, " ")
).withColumn(
    "APL_LVL_LTR_DEST_ID", rpad(col("APL_LVL_LTR_DEST_ID"), <...>, " ")
).withColumn(
    "APL_LVL_LTR_RQST_DT_SK", rpad(col("APL_LVL_LTR_RQST_DT_SK"), 10, " ")
)

df_final = df_Transform_Output.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "APL_LVL_LTR_STYLE_CD",
    "APL_LVL_LTR_SEQ_NO",
    "APL_LVL_LTR_DEST_ID",
    "APL_LVL_LTR_RQST_DT_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/B_APL_LVL_LTR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)