# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : EyeMedClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  Eye Med Claims Member Matching Error File Load
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                      Date                     Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC --------------------                 -------------------           ------------------------          -----------------------------------------------------------------------                   --------------------------------           -------------------------------   ----------------------------      
# MAGIC Madhavan B                  2017-10-03              5744                          Original Programming                                                       IntegrateDev2                    Kalyan Neelam          2018-04-04


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcFileNm = get_widget_value('SrcFileNm','')
TgtFileNm = get_widget_value('TgtFileNm','')
SrcSysCd = get_widget_value('SrcSysCd','')

schema_Seq_Err_File = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_TYP_CD", StringType(), True),
    StructField("CLM_SUBTYP_CD", StringType(), True),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), True),
    StructField("SRC_SYS_GRP_PFX", StringType(), True),
    StructField("SRC_SYS_GRP_ID", StringType(), True),
    StructField("SRC_SYS_GRP_SFX", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_GNDR_CD", StringType(), True),
    StructField("PATN_BRTH_DT_SK", StringType(), True),
    StructField("ERR_CD", StringType(), True),
    StructField("ERR_DESC", StringType(), True),
    StructField("FEP_MBR_ID", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SRC_SYS_SUB_ID", StringType(), True),
    StructField("SRC_SYS_MBR_SFX_NO", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("FILE_DT_SK", StringType(), True),
    StructField("PATN_SSN", StringType(), True)
])

df_Seq_Err_File = (
    spark.read.format("csv")
    .schema(schema_Seq_Err_File)
    .option("header", True)
    .option("quote", "\"")
    .option("escape", "\"")
    .load(f"{adls_path_publish}/external/{SrcFileNm}")
)

df_hf_eyemed_errclm_rm_dupe = df_Seq_Err_File.dropDuplicates(["CLM_ID"])

df_trn_clm_sk_gen = df_hf_eyemed_errclm_rm_dupe.select(
    GetFkeyClm(SrcSysCd, lit(1), trim(col("CLM_ID")), lit('N')).alias("CLM_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("SUB_SSN").alias("SUB_SSN"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("ERR_CD").alias("ERR_CD"),
    col("ERR_DESC").alias("ERR_DESC"),
    col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    col("GRP_ID").alias("GRP_ID"),
    col("FILE_DT_SK").alias("FILE_DT_SK"),
    col("PATN_SSN").alias("PATN_SSN")
)

df_final = df_trn_clm_sk_gen
df_final = df_final.withColumn("CLM_SVC_STRT_DT_SK", rpad(col("CLM_SVC_STRT_DT_SK"), 10, " "))
df_final = df_final.withColumn("PATN_BRTH_DT_SK", rpad(col("PATN_BRTH_DT_SK"), 10, " "))
df_final = df_final.withColumn("SRC_SYS_MBR_SFX_NO", rpad(col("SRC_SYS_MBR_SFX_NO"), 3, " "))
df_final = df_final.withColumn("FILE_DT_SK", rpad(col("FILE_DT_SK"), 10, " "))

df_final = df_final.select(
    "CLM_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "CLM_TYP_CD",
    "CLM_SUBTYP_CD",
    "CLM_SVC_STRT_DT_SK",
    "SRC_SYS_GRP_PFX",
    "SRC_SYS_GRP_ID",
    "SRC_SYS_GRP_SFX",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "ERR_CD",
    "ERR_DESC",
    "FEP_MBR_ID",
    "SUB_FIRST_NM",
    "SUB_LAST_NM",
    "SRC_SYS_SUB_ID",
    "SRC_SYS_MBR_SFX_NO",
    "GRP_ID",
    "FILE_DT_SK",
    "PATN_SSN"
)

write_files(
    df_final,
    f"{adls_path}/load/{TgtFileNm}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)