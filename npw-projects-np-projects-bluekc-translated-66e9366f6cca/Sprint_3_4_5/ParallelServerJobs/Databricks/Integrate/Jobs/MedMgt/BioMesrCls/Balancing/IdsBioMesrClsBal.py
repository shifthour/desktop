# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : BioMesrClsBalSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi              2011-12-28       4765                               Original Programming                           IntegrateCurDevl            Sharon Anderw           2012-01-12

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
ExtrRunCycle = get_widget_value("ExtrRunCycle", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_SrcTrgtComp = f"""
SELECT
BIO_MESR_CLS.BIO_MESR_TYP_CD AS SRC_BIO_MESR_TYP_CD,
BIO_MESR_CLS.GNDR_CD AS SRC_GNDR_CD,
BIO_MESR_CLS.AGE_RNG_MIN_YR_NO AS SRC_AGE_RNG_MIN_YR_NO,
BIO_MESR_CLS.AGE_RNG_MAX_YR_NO AS SRC_AGE_RNG_MAX_YR_NO,
BIO_MESR_CLS.BIO_MESR_RNG_LOW_NO AS SRC_BIO_MESR_RNG_LOW_NO,
BIO_MESR_CLS.BIO_MESR_RNG_HI_NO AS SRC_BIO_MESR_RNG_HI_NO,
BIO_MESR_CLS.EFF_DT_SK AS SRC_EFF_DT_SK,
BIO_MESR_CLS.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_BIO_MESR_CLS.BIO_MESR_TYP_CD AS TRGT_BIO_MESR_TYP_CD,
B_BIO_MESR_CLS.GNDR_CD AS TRGT_GNDR_CD,
B_BIO_MESR_CLS.AGE_RNG_MIN_YR_NO AS TRGT_AGE_RNG_MIN_YR_NO,
B_BIO_MESR_CLS.AGE_RNG_MAX_YR_NO AS TRGT_AGE_RNG_MAX_YR_NO,
B_BIO_MESR_CLS.BIO_MESR_RNG_LOW_NO AS TRGT_BIO_MESR_RNG_LOW_NO,
B_BIO_MESR_CLS.BIO_MESR_RNG_HI_NO AS TRGT_BIO_MESR_RNG_HI_NO,
B_BIO_MESR_CLS.EFF_DT_SK AS TRGT_EFF_DT_SK,
B_BIO_MESR_CLS.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.BIO_MESR_CLS BIO_MESR_CLS
FULL OUTER JOIN {IDSOwner}.B_BIO_MESR_CLS B_BIO_MESR_CLS
ON BIO_MESR_CLS.BIO_MESR_TYP_CD = B_BIO_MESR_CLS.BIO_MESR_TYP_CD
AND BIO_MESR_CLS.GNDR_CD = B_BIO_MESR_CLS.GNDR_CD
AND BIO_MESR_CLS.AGE_RNG_MIN_YR_NO = B_BIO_MESR_CLS.AGE_RNG_MIN_YR_NO
AND BIO_MESR_CLS.AGE_RNG_MAX_YR_NO = B_BIO_MESR_CLS.AGE_RNG_MAX_YR_NO
AND BIO_MESR_CLS.BIO_MESR_RNG_LOW_NO = B_BIO_MESR_CLS.BIO_MESR_RNG_LOW_NO
AND BIO_MESR_CLS.BIO_MESR_RNG_HI_NO = B_BIO_MESR_CLS.BIO_MESR_RNG_HI_NO
AND BIO_MESR_CLS.EFF_DT_SK = B_BIO_MESR_CLS.EFF_DT_SK
AND BIO_MESR_CLS.SRC_SYS_CD_SK = B_BIO_MESR_CLS.SRC_SYS_CD_SK
WHERE BIO_MESR_CLS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtComp)
    .load()
)

w = Window.orderBy(F.lit(1))
df_SrcTrgtComp = df_SrcTrgtComp.withColumn("_rn", F.row_number().over(w))

df_ResearchFile = df_SrcTrgtComp.filter(
    F.col("SRC_AGE_RNG_MAX_YR_NO").isNull()
    | F.col("SRC_AGE_RNG_MIN_YR_NO").isNull()
    | F.col("SRC_BIO_MESR_RNG_HI_NO").isNull()
    | F.col("SRC_BIO_MESR_RNG_LOW_NO").isNull()
    | F.col("SRC_BIO_MESR_TYP_CD").isNull()
    | F.col("SRC_EFF_DT_SK").isNull()
    | F.col("SRC_GNDR_CD").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_AGE_RNG_MAX_YR_NO").isNull()
    | F.col("TRGT_AGE_RNG_MIN_YR_NO").isNull()
    | F.col("TRGT_BIO_MESR_RNG_HI_NO").isNull()
    | F.col("TRGT_BIO_MESR_RNG_LOW_NO").isNull()
    | F.col("TRGT_BIO_MESR_TYP_CD").isNull()
    | F.col("TRGT_EFF_DT_SK").isNull()
    | F.col("TRGT_GNDR_CD").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_ResearchFile = df_ResearchFile.select(
    F.col("TRGT_BIO_MESR_TYP_CD"),
    F.col("TRGT_GNDR_CD"),
    F.col("TRGT_AGE_RNG_MIN_YR_NO"),
    F.col("TRGT_AGE_RNG_MAX_YR_NO"),
    F.col("TRGT_BIO_MESR_RNG_LOW_NO"),
    F.col("TRGT_BIO_MESR_RNG_HI_NO"),
    F.rpad(F.col("TRGT_EFF_DT_SK"), 10, " ").alias("TRGT_EFF_DT_SK"),
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("SRC_BIO_MESR_TYP_CD"),
    F.col("SRC_GNDR_CD"),
    F.col("SRC_AGE_RNG_MIN_YR_NO"),
    F.col("SRC_AGE_RNG_MAX_YR_NO"),
    F.col("SRC_BIO_MESR_RNG_LOW_NO"),
    F.col("SRC_BIO_MESR_RNG_HI_NO"),
    F.rpad(F.col("SRC_EFF_DT_SK"), 10, " ").alias("SRC_EFF_DT_SK"),
    F.col("SRC_SRC_SYS_CD_SK")
)

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/IdsBioMesrClsResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify = df_SrcTrgtComp.filter(
    (F.col("_rn") == 1) & (ToleranceCd == "OUT")
).select(
    F.rpad(
        F.lit("ROW COUNT BALANCING BIO_MESR_CLS OUT OF TOLERANCE"), 70, " "
    ).alias("NOTIFICATION")
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)