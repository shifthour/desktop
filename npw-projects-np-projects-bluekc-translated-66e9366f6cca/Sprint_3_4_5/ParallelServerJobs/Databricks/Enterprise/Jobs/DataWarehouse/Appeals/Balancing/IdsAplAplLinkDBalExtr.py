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
# MAGIC Bhoomi Dasari                 10/02/2007         3028                              Originally Programmed                            devlEDW10                     Steph Goddard             10/18/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

schema_IdsAplLink = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("APL_LINK_TYP_CD", StringType(), nullable=False),
    StructField("APL_LINK_ID", StringType(), nullable=False),
    StructField("APL_LINK_DESC", StringType(), nullable=True),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False)
])

df_IdsAplLink = (
    spark.read.csv(
        f"{adls_path}/balancing/snapshot/IDS_APL_LINK.uniq",
        schema=schema_IdsAplLink,
        sep=",",
        quote='"',
        header=False
    )
)

df_enriched = (
    df_IdsAplLink.alias("Snapshot")
    .join(
        df_hf_cdma_codes.alias("SrcSysCdLkup"),
        F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
        how="left"
    )
)

df_enriched = df_enriched.select(
    F.when(
        (F.col("SrcSysCdLkup.TRGT_CD").isNull()) | (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Snapshot.APL_ID").alias("APL_ID"),
    F.col("Snapshot.APL_LINK_TYP_CD").alias("APL_LINK_TYP_CD"),
    F.col("Snapshot.APL_LINK_ID").alias("APL_LINK_ID"),
    F.col("Snapshot.APL_LINK_DESC").alias("APL_LINK_DESC"),
    F.date_format(F.col("Snapshot.LAST_UPDT_DTM"), "yyyy-MM-dd").alias("LAST_UPDT_DT_SK")
)

df_final = (
    df_enriched
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("APL_ID", F.rpad(F.col("APL_ID"), <...>, " "))
    .withColumn("APL_LINK_TYP_CD", F.rpad(F.col("APL_LINK_TYP_CD"), <...>, " "))
    .withColumn("APL_LINK_ID", F.rpad(F.col("APL_LINK_ID"), <...>, " "))
    .withColumn("APL_LINK_DESC", F.rpad(F.col("APL_LINK_DESC"), <...>, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_final.select(
        "SRC_SYS_CD",
        "APL_ID",
        "APL_LINK_TYP_CD",
        "APL_LINK_ID",
        "APL_LINK_DESC",
        "LAST_UPDT_DT_SK"
    ),
    f"{adls_path}/load/B_APL_LINK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)