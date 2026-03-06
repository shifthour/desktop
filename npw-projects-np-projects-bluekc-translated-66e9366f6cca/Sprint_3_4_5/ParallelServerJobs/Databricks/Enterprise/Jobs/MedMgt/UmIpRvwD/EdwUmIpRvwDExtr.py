# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwUmActvtyDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS UM_IP_RVW to flatfile UM_IP_RVW_D.dat
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 3/31/2009          BICC/3808                  Original Programming.                                                                               devlEDW                  Steph Goddard           04/03/2009
# MAGIC Ralph Tucker                  09/15/2009   TTR-583                         Added EdwRunCycle to last updt run cyc                                             devlEDW                       Steph Goddard           09/16/2009
# MAGIC Sravya Gorla                   10/02/2019     US# 140167                 Updated the datatype of DIAG_SET_CRT_DTM                                   EnterpriseDev2             Jaideep Mankala        10/07/2019
# MAGIC                                                                                                       from Timestamp(26.3) to timestamp(26.6)

# MAGIC Extract IDS Data
# MAGIC Apply business logic.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT UM_IP_RVW_SK,SRC_SYS_CD_SK,UM_REF_ID,UM_IP_RVW_SEQ_NO,CRT_RUN_CYC_EXCTN_SK,"
    f"LAST_UPDT_RUN_CYC_EXCTN_SK,UM_SK,UM_IP_RVW_LOS_EXCPT_CD_SK,UM_IP_RVW_TREAT_CAT_CD_SK,"
    f"DIAG_SET_CRT_DTM,RVW_DT_SK,AUTH_TOT_LOS_NO,AVG_TOT_LOS_NO,NRMTV_TOT_LOS_NO,RQST_TOT_LOS_NO "
    f"FROM {IDSOwner}.UM_IP_RVW "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle} OR LAST_UPDT_RUN_CYC_EXCTN_SK = 0 OR LAST_UPDT_RUN_CYC_EXCTN_SK = 1"
)

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_enriched = (
    df_IDS.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSysCd"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refUmIpRvwLosExcptCdCat"),
        F.col("Extract.UM_IP_RVW_LOS_EXCPT_CD_SK") == F.col("refUmIpRvwLosExcptCdCat.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refUmIpRvwTreatCdCat"),
        F.col("Extract.UM_IP_RVW_TREAT_CAT_CD_SK") == F.col("refUmIpRvwTreatCdCat.CD_MPPNG_SK"),
        "left"
    )
)

df_enriched = df_enriched.withColumn("UM_IP_RVW_SK", F.col("Extract.UM_IP_RVW_SK"))
df_enriched = df_enriched.withColumn(
    "SRC_SYS_CD",
    F.when(
        F.col("refSrcSysCd.TRGT_CD").isNull()
        | (F.length(trim(F.col("refSrcSysCd.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refSrcSysCd.TRGT_CD"))
)
df_enriched = df_enriched.withColumn("UM_REF_ID", F.col("Extract.UM_REF_ID"))
df_enriched = df_enriched.withColumn("UM_IP_RVW_SEQ_NO", F.col("Extract.UM_IP_RVW_SEQ_NO"))
df_enriched = df_enriched.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate))
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate))
df_enriched = df_enriched.withColumn("UM_SK", F.col("Extract.UM_SK"))
df_enriched = df_enriched.withColumn("DIAG_SET_CRT_DTM", F.col("Extract.DIAG_SET_CRT_DTM"))
df_enriched = df_enriched.withColumn("RVW_DT_SK", F.col("Extract.RVW_DT_SK"))
df_enriched = df_enriched.withColumn("UM_IP_RVW_AUTH_TOT_LOS_NO", F.col("Extract.AUTH_TOT_LOS_NO"))
df_enriched = df_enriched.withColumn("UM_IP_RVW_AVG_TOT_LOS_NO", F.col("Extract.AVG_TOT_LOS_NO"))
df_enriched = df_enriched.withColumn(
    "UM_IP_RVW_LOS_EXCPT_CD",
    F.when(
        F.col("refUmIpRvwLosExcptCdCat.TRGT_CD").isNull()
        | (F.length(trim(F.col("refUmIpRvwLosExcptCdCat.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refUmIpRvwLosExcptCdCat.TRGT_CD"))
)
df_enriched = df_enriched.withColumn(
    "UM_IP_RVW_LOS_EXCPT_NM",
    F.when(
        F.col("refUmIpRvwLosExcptCdCat.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refUmIpRvwLosExcptCdCat.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refUmIpRvwLosExcptCdCat.TRGT_CD_NM"))
)
df_enriched = df_enriched.withColumn("UM_IP_RVW_NRMTV_TOT_LOS_NO", F.col("Extract.NRMTV_TOT_LOS_NO"))
df_enriched = df_enriched.withColumn("UM_IP_RVW_RQST_TOT_LOS_NO", F.col("Extract.RQST_TOT_LOS_NO"))
df_enriched = df_enriched.withColumn(
    "UM_IP_RVW_TREAT_CAT_CD",
    F.when(
        F.col("refUmIpRvwTreatCdCat.TRGT_CD").isNull()
        | (F.length(trim(F.col("refUmIpRvwTreatCdCat.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refUmIpRvwTreatCdCat.TRGT_CD"))
)
df_enriched = df_enriched.withColumn(
    "UM_IP_RVW_TREAT_CAT_NM",
    F.when(
        F.col("refUmIpRvwTreatCdCat.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refUmIpRvwTreatCdCat.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refUmIpRvwTreatCdCat.TRGT_CD_NM"))
)
df_enriched = df_enriched.withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("Extract.CRT_RUN_CYC_EXCTN_SK"))
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EdwRunCycle))
df_enriched = df_enriched.withColumn("UM_IP_RVW_LOS_EXCPT_CD_SK", F.col("Extract.UM_IP_RVW_LOS_EXCPT_CD_SK"))
df_enriched = df_enriched.withColumn("UM_IP_RVW_TREAT_CAT_CD_SK", F.col("Extract.UM_IP_RVW_TREAT_CAT_CD_SK"))

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "RVW_DT_SK",
    F.rpad(F.col("RVW_DT_SK"), 10, " ")
)

df_final = df_enriched.select(
    "UM_IP_RVW_SK",
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_IP_RVW_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "UM_SK",
    "DIAG_SET_CRT_DTM",
    "RVW_DT_SK",
    "UM_IP_RVW_AUTH_TOT_LOS_NO",
    "UM_IP_RVW_AVG_TOT_LOS_NO",
    "UM_IP_RVW_LOS_EXCPT_CD",
    "UM_IP_RVW_LOS_EXCPT_NM",
    "UM_IP_RVW_NRMTV_TOT_LOS_NO",
    "UM_IP_RVW_RQST_TOT_LOS_NO",
    "UM_IP_RVW_TREAT_CAT_CD",
    "UM_IP_RVW_TREAT_CAT_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_IP_RVW_LOS_EXCPT_CD_SK",
    "UM_IP_RVW_TREAT_CAT_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/UM_IP_RVW_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)