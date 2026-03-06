# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_ACTVTY
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                  Steph Goddard              09/15/2007
# MAGIC 
# MAGIC 
# MAGIC Raj Mangalampally         11-04-2013          5114                             Server to Parallel Conv                      EnterpriseWrhsDevl       Jag Yelavarthi                2014-01-17

# MAGIC Read data from source table APL_CNTCT and Join on Code Mapping table to bring SRC_SYS_CD; Extract records Based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC Lookup Keys
# MAGIC 
# MAGIC 1) APL_CNTCT_CAT_CD_SK
# MAGIC Add Defaults and Null Handling
# MAGIC Job Name; IdsEdwAplCntctDExtr
# MAGIC 
# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwAplCntctDExtr
# MAGIC 
# MAGIC Table:
# MAGIC APL_CNTCT_D
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write APL_CNTCT_D Data into a Sequential file for Load Job.
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
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
APL_CNTCT.APL_CNTCT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_CNTCT.APL_ID,
APL_CNTCT.SEQ_NO,
APL_CNTCT.APL_SK,
APL_CNTCT.APL_CNTCT_CAT_CD_SK,
APL_CNTCT.APL_CNTCT_ID,
APL_CNTCT.APL_CNTCT_NM
FROM {IDSOwner}.APL_CNTCT APL_CNTCT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON APL_CNTCT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE
APL_CNTCT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_APL_CNTCT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'NA') TRGT_CD,
TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkp_CDMA_Codes_joined = df_db2_APL_CNTCT_D_in.alias("Ink_IdsEdwAplCntCtExtr_inABC").join(
    df_db2_CD_MPPNG_Extr.alias("lnk_Cd_mppng_out"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_CNTCT_CAT_CD_SK") == F.col("lnk_Cd_mppng_out.CD_MPPNG_SK"),
    "left"
)

df_lkp_CDMA_Codes = df_lkp_CDMA_Codes_joined.select(
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_CNTCT_SK").alias("APL_CNTCT_SK"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_ID").alias("APL_ID"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.SEQ_NO").alias("APL_CNTCT_SEQ_NO"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_SK").alias("APL_SK"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_CNTCT_ID").alias("APPEAL_CONTACT_IDENTIFIER"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_CNTCT_NM").alias("APPEAL_CONTACT_NAME"),
    F.col("lnk_Cd_mppng_out.TRGT_CD").alias("APL_CNTCT_CAT_CD"),
    F.col("lnk_Cd_mppng_out.TRGT_CD_NM").alias("APL_CNTCT_CAT_NM"),
    F.col("Ink_IdsEdwAplCntCtExtr_inABC.APL_CNTCT_CAT_CD_SK").alias("APL_CNTCT_CAT_CD_SK")
)

df_xfm_BusinessRules_main = df_lkp_CDMA_Codes.filter(
    (F.col("APL_CNTCT_SK") != 0) & (F.col("APL_CNTCT_SK") != 1)
).select(
    F.col("APL_CNTCT_SK").alias("APL_CNTCT_SK"),
    F.when(trim(F.col("SRC_SYS_CD")) == "", "NA").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("APL_CNTCT_SEQ_NO").alias("APL_CNTCT_SEQ_NO"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("APPEAL_CONTACT_IDENTIFIER").alias("APPEAL_CONTACT_IDENTIFIER"),
    F.col("APPEAL_CONTACT_NAME").alias("APPEAL_CONTACT_NAME"),
    F.when(
        (F.length(trim(F.col("APL_CNTCT_CAT_CD"))) == 0) | (trim(F.col("APL_CNTCT_CAT_CD")) == ""),
        "NA"
    ).otherwise(F.col("APL_CNTCT_CAT_CD")).alias("APL_CNTCT_CAT_CD"),
    F.when(
        (F.length(trim(F.col("APL_CNTCT_CAT_NM"))) == 0) | (trim(F.col("APL_CNTCT_CAT_NM")) == ""),
        "NA"
    ).otherwise(F.col("APL_CNTCT_CAT_NM")).alias("APL_CNTCT_CAT_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_CNTCT_CAT_CD_SK").alias("APL_CNTCT_CAT_CD_SK")
)

df_xfm_BusinessRules_unk_schema = StructType([
    StructField("APL_CNTCT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("APL_ID", StringType(), True),
    StructField("APL_CNTCT_SEQ_NO", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("APL_SK", IntegerType(), True),
    StructField("APPEAL_CONTACT_IDENTIFIER", StringType(), True),
    StructField("APPEAL_CONTACT_NAME", StringType(), True),
    StructField("APL_CNTCT_CAT_CD", StringType(), True),
    StructField("APL_CNTCT_CAT_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APL_CNTCT_CAT_CD_SK", IntegerType(), True),
])
unk_data = [
    (
        0, 'UNK', 'UNK', 0, '1753-01-01', '1753-01-01', 0, 'UNK', None,
        'UNK', 'UNK', 100, 100, 0
    )
]
df_xfm_BusinessRules_unk = spark.createDataFrame(unk_data, df_xfm_BusinessRules_unk_schema)

df_xfm_BusinessRules_na_schema = StructType([
    StructField("APL_CNTCT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("APL_ID", StringType(), True),
    StructField("APL_CNTCT_SEQ_NO", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("APL_SK", IntegerType(), True),
    StructField("APPEAL_CONTACT_IDENTIFIER", StringType(), True),
    StructField("APPEAL_CONTACT_NAME", StringType(), True),
    StructField("APL_CNTCT_CAT_CD", StringType(), True),
    StructField("APL_CNTCT_CAT_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APL_CNTCT_CAT_CD_SK", IntegerType(), True),
])
na_data = [
    (
        1, 'NA', 'NA', 0, '1753-01-01', '1753-01-01', 1, 'NA', None,
        'NA', 'NA', 100, 100, 1
    )
]
df_xfm_BusinessRules_na = spark.createDataFrame(na_data, df_xfm_BusinessRules_na_schema)

df_fnl_NA_UNK = df_xfm_BusinessRules_main.unionByName(df_xfm_BusinessRules_unk).unionByName(df_xfm_BusinessRules_na)

df_final = df_fnl_NA_UNK.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).select(
    "APL_CNTCT_SK",
    "SRC_SYS_CD",
    "APL_ID",
    "APL_CNTCT_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "APL_SK",
    "APPEAL_CONTACT_IDENTIFIER",
    "APPEAL_CONTACT_NAME",
    "APL_CNTCT_CAT_CD",
    "APL_CNTCT_CAT_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_CNTCT_CAT_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/APL_CNTCT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)