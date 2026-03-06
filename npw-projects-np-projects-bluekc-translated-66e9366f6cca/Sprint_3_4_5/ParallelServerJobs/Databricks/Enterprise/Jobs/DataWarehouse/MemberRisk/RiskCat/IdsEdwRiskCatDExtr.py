# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                          Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Bhoomi Dasari    11/27/2007   Mbrshp       Originally Programmed                                                                                  Steph Goddard   11/28/2007  
# MAGIC SAndrew            2009-06-26     TTR539     Added IDS run cycle to extract                                                                    Steph Goddard   06/16/2009
# MAGIC Hugh Sisson      2012-08-10     TTR435      Added FMLY_CAROVR_AMT to end of table                                             SAndrew               2012-08-20    
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela 2013-11-01 P5114            Originally Programmed(In parallel)                                                               Jag Yelavarthi       2013-12-09

# MAGIC Job name: IdsEdwRiskCatDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Adding NA and UNK rows
# MAGIC Read data from source table RISK_CAT
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MAJ_PRCTC_CAT_CD
# MAGIC Write RISK_CAT_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url_db2_IdsEdwRiskCatD_Extr, jdbc_props_db2_IdsEdwRiskCatD_Extr = get_db_config(ids_secret_name)
extract_query_db2_IdsEdwRiskCatD_Extr = f"""
SELECT 
RISK_CAT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
RISK_CAT_ID,
MAJ_PRCTC_CAT_CD_SK,
RISK_CAT_DESC,
RISK_CAT_LABEL,
RISK_CAT_LONG_DESC,
RISK_CAT_NM
FROM {IDSOwner}.RISK_CAT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_IdsEdwRiskCatD_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_IdsEdwRiskCatD_Extr)
    .options(**jdbc_props_db2_IdsEdwRiskCatD_Extr)
    .option("query", extract_query_db2_IdsEdwRiskCatD_Extr)
    .load()
)

jdbc_url_db2_CD_MPPNG_Extr, jdbc_props_db2_CD_MPPNG_Extr = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_Extr)
    .options(**jdbc_props_db2_CD_MPPNG_Extr)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lkp_Codes_temp = df_db2_IdsEdwRiskCatD_Extr.alias("Ink_IdsEdwRiskCatDExtr_InABC").join(
    df_db2_CD_MPPNG_Extr.alias("Ref_MajPrctcCatTyp"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.MAJ_PRCTC_CAT_CD_SK") == F.col("Ref_MajPrctcCatTyp.CD_MPPNG_SK"),
    "left"
)
df_lkp_Codes = df_lkp_Codes_temp.select(
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Ref_MajPrctcCatTyp.TRGT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("Ref_MajPrctcCatTyp.TRGT_CD_NM").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.RISK_CAT_DESC").alias("RISK_CAT_DESC"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.RISK_CAT_LABEL").alias("RISK_CAT_LABEL"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.RISK_CAT_LONG_DESC").alias("RISK_CAT_LONG_DESC"),
    F.col("Ink_IdsEdwRiskCatDExtr_InABC.RISK_CAT_NM").alias("RISK_CAT_NM")
)

df_main = (
    df_lkp_Codes
    .filter((F.col("RISK_CAT_SK") != 0) & (F.col("RISK_CAT_SK") != 1))
    .withColumn("SRC_SYS_CD", F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")))
    .withColumn("MAJ_PRCTC_CAT_CD", F.when(F.trim(F.col("MAJ_PRCTC_CAT_CD")) == "", F.lit("UNK")).otherwise(F.col("MAJ_PRCTC_CAT_CD")))
    .withColumn("MAJ_PRCTC_CAT_DESC", F.when(F.trim(F.col("MAJ_PRCTC_CAT_DESC")) == "", F.lit("UNK")).otherwise(F.col("MAJ_PRCTC_CAT_DESC")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

schema_lnk = StructType([
    StructField("RISK_CAT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("RISK_CAT_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("MAJ_PRCTC_CAT_CD", StringType(), True),
    StructField("MAJ_PRCTC_CAT_DESC", StringType(), True),
    StructField("RISK_CAT_DESC", StringType(), True),
    StructField("RISK_CAT_LABEL", StringType(), True),
    StructField("RISK_CAT_LONG_DESC", StringType(), True),
    StructField("RISK_CAT_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_lnk_UNK_Row = spark.createDataFrame(
    [(0, "UNK", "UNK", "1753-01-01", "1753-01-01", "UNK", "", "", "", "", "UNK", 100, 100)],
    schema_lnk
).withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
 .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))

df_lnk_NA_Row = spark.createDataFrame(
    [(1, "NA", "NA", "1753-01-01", "1753-01-01", "NA", "", "", "", "", "NA", 100, 100)],
    schema_lnk
).withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
 .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))

df_Fnl_Mbr_Cat = df_lnk_UNK_Row.unionByName(df_main).unionByName(df_lnk_NA_Row)

final_df = df_Fnl_Mbr_Cat.select(
    "RISK_CAT_SK",
    "SRC_SYS_CD",
    "RISK_CAT_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MAJ_PRCTC_CAT_CD",
    "MAJ_PRCTC_CAT_DESC",
    "RISK_CAT_DESC",
    "RISK_CAT_LABEL",
    "RISK_CAT_LONG_DESC",
    "RISK_CAT_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    final_df,
    f"{adls_path}/load/RISK_CAT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)