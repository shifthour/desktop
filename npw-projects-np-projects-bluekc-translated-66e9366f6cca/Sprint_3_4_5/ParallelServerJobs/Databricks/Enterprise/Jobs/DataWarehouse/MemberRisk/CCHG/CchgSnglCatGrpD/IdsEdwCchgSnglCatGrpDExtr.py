# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from IDS CCHG_SNGL_CAT_GRP_D table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                  2015-08-25       5460                              Initial Programming                             EnterpriseDev2          Bhoomi Dasari           8/30/2015
# MAGIC Goutham K                11/13/2021         US-500022                   Added New Field VersionID,                     IntegrateDev2           Reddy Sanam           04/13/2022

# MAGIC Read all the Data from IDS CCHG_SNGL_CAT_GRP Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Write CCHG_SNGL_CAT_GRP_D Data into a Sequential file for Load Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_1, jdbc_props_1 = get_db_config(ids_secret_name)
extract_query_1 = f"""SELECT 
CSCG.CCHG_SNGL_CAT_GRP_SK, 
CSCG.CCHG_SNGL_CAT_GRP_CD, 
CSCG.SRC_SYS_CD_SK,
CSCG.CRT_RUN_CYC_EXCTN_SK, 
CSCG.LAST_UPDT_RUN_CYC_EXCTN_SK, 
CSCG.ICD_VRSN_CD_SK,
CSCG.CCHG_SNGL_CAT_GRP_DESC, 
CSCG.ICD_DIAG_CLM_MPPNG_DESC, 
COALESCE(CD1.TRGT_CD, 'UNK') as SRC_SYS_CD,
CSCG.VRSN_ID
FROM {IDSOwner}.CCHG_SNGL_CAT_GRP AS CSCG
LEFT OUTER JOIN
{IDSOwner}.CD_MPPNG AS CD1
ON
CSCG.SRC_SYS_CD_SK = CD1.CD_MPPNG_SK
WHERE
CSCG.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"""

df_db2_CCHG_SNGL_CAT_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_1)
    .options(**jdbc_props_1)
    .option("query", extract_query_1)
    .load()
)

jdbc_url_2, jdbc_props_2 = get_db_config(ids_secret_name)
extract_query_2 = f"""SELECT 
CD.CD_MPPNG_SK,
COALESCE(CD.TRGT_CD, 'UNK') TRGT_CD,
COALESCE(CD.TRGT_CD_NM, 'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""
df_db2_Cd_Mppng_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("query", extract_query_2)
    .load()
)

df_Copy = df_db2_Cd_Mppng_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Lookup = df_db2_CCHG_SNGL_CAT_GRP_in.alias("IdsOut").join(
    df_Copy.alias("IcdVrsnCd_lkup"),
    F.col("IdsOut.ICD_VRSN_CD_SK") == F.col("IcdVrsnCd_lkup.CD_MPPNG_SK"),
    "left"
).select(
    F.col("IdsOut.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK"),
    F.col("IdsOut.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("IdsOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("IcdVrsnCd_lkup.TRGT_CD").alias("ICD_VRSN_CD"),
    F.col("IdsOut.CCHG_SNGL_CAT_GRP_DESC").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.col("IdsOut.ICD_DIAG_CLM_MPPNG_DESC").alias("ICD_DIAG_CLM_MPPNG_DESC"),
    F.col("IdsOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IdsOut.ICD_VRSN_CD_SK").alias("ICD_VRSN_CD_SK"),
    F.col("IdsOut.VRSN_ID").alias("VRSN_ID")
)

df_lnk_xfm_Data = df_Lookup.filter(
    (F.col("CCHG_SNGL_CAT_GRP_SK") != 1) & (F.col("CCHG_SNGL_CAT_GRP_SK") != 0)
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit(CurrRunCycleDate), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit(CurrRunCycleDate), 10, " ")
).withColumn(
    "ICD_VRSN_CD", F.when(F.col("ICD_VRSN_CD").isNull(), 'UNK').otherwise(F.col("ICD_VRSN_CD"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle)
)

schema_lnk_NA_out = StructType([
    StructField("CCHG_SNGL_CAT_GRP_SK", IntegerType(), True),
    StructField("CCHG_SNGL_CAT_GRP_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("ICD_VRSN_CD", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_DESC", StringType(), True),
    StructField("ICD_DIAG_CLM_MPPNG_DESC", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ICD_VRSN_CD_SK", IntegerType(), True),
    StructField("VRSN_ID", StringType(), True)
])
data_lnk_NA_out = [(1, "NA", "NA", "1753-01-01", "1753-01-01", "NA", "NA", "NA", 100, 100, 100, 1, "NA")]
df_lnk_NA_out = spark.createDataFrame(data_lnk_NA_out, schema=schema_lnk_NA_out).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

schema_lnk_UNK_out = StructType([
    StructField("CCHG_SNGL_CAT_GRP_SK", IntegerType(), True),
    StructField("CCHG_SNGL_CAT_GRP_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("ICD_VRSN_CD", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_DESC", StringType(), True),
    StructField("ICD_DIAG_CLM_MPPNG_DESC", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ICD_VRSN_CD_SK", IntegerType(), True),
    StructField("VRSN_ID", StringType(), True)
])
data_lnk_UNK_out = [(0, "UNK", "UNK", "1753-01-01", "1753-01-01", "UNK", "UNK", "UNK", 100, 100, 100, 0, "UNK")]
df_lnk_UNK_out = spark.createDataFrame(data_lnk_UNK_out, schema=schema_lnk_UNK_out).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

df_fnl_Data = (
    df_lnk_NA_out.unionByName(df_lnk_UNK_out)
    .unionByName(df_lnk_xfm_Data)
    .select(
        "CCHG_SNGL_CAT_GRP_SK",
        "CCHG_SNGL_CAT_GRP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ICD_VRSN_CD",
        "CCHG_SNGL_CAT_GRP_DESC",
        "ICD_DIAG_CLM_MPPNG_DESC",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ICD_VRSN_CD_SK",
        "VRSN_ID"
    )
)

write_files(
    df_fnl_Data,
    f"{adls_path}/load/CCHG_SNGL_CAT_GRP_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)