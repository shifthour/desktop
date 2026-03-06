# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALL BY: IdsEdwSpiraMemVstCntl
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Loads data from MBR_VST load file into the table EDW MBR_VST_FACT
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer		Date		Project		Description		Environment                      Code Reviewer                      Review Date
# MAGIC ----------------------------------	-------------------	-------------		------------------------------------	-----------------------------        ------------------------------------       -----------------------------------
# MAGIC Harsha Ravuri                         2019-04-19	Spira		Initial Version		EnterpriseDev2                                      Kalyan Neelam                        2019-04-22


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


RunID = get_widget_value('RunID', '')
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')

schema_MBR_VST_F_Load = StructType([
    StructField("MBR_VST_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("VST_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("PRI_POL_ID", StringType(), True),
    StructField("SEC_POL_ID", StringType(), True),
    StructField("PATN_EXTRNL_VNDR_ID", StringType(), True),
    StructField("PROV_EXTRNL_VNDR_ID", StringType(), True),
    StructField("VST_DT_SK", StringType(), True),
    StructField("VST_STTUS_CD", StringType(), True),
    StructField("VST_CLSD_DT_SK", StringType(), True),
    StructField("BILL_RVWED_DT_SK", StringType(), True),
    StructField("BILL_RVWED_BY_NM", StringType(), True),
    StructField("SVC_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("SVC_PROV_SK", IntegerType(), True),
    StructField("SVC_PROV_ID", StringType(), True),
    StructField("SVC_PROV_NM", StringType(), True),
    StructField("REL_IPA_PROV_SK", IntegerType(), True),
    StructField("REL_IPA_PROV_ID", StringType(), True),
    StructField("REL_IPA_PROV_NM", StringType(), True),
    StructField("REL_GRP_PROV_SK", IntegerType(), True),
    StructField("REL_GRP_PROV_ID", StringType(), True),
    StructField("REL_GRP_PROV_NM", StringType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("MBR_FULL_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_MBR_VST_F_Load = (
    spark.read
        .option("header", "false")
        .option("sep", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("nullValue", None)
        .schema(schema_MBR_VST_F_Load)
        .csv(f"{adls_path}/load/MBR_VST_F.dat")
)

df_Buff = df_MBR_VST_F_Load.select(
    F.col("MBR_VST_SK").alias("MBR_VST_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("VST_ID").alias("VST_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
    F.col("PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
    F.col("VST_DT_SK").alias("VST_DT_SK"),
    F.col("VST_STTUS_CD").alias("VST_STTUS_CD"),
    F.col("VST_CLSD_DT_SK").alias("VST_CLSD_DT_SK"),
    F.col("BILL_RVWED_DT_SK").alias("BILL_RVWED_DT_SK"),
    F.col("BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
    F.col("SVC_PROV_NTNL_PROV_ID").alias("SVC_PROV_NTNL_PROV_ID"),
    F.col("SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.col("REL_IPA_PROV_SK").alias("REL_IPA_PROV_SK"),
    F.col("REL_IPA_PROV_ID").alias("REL_IPA_PROV_ID"),
    F.col("REL_IPA_PROV_NM").alias("REL_IPA_PROV_NM"),
    F.col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    F.col("REL_GRP_PROV_ID").alias("REL_GRP_PROV_ID"),
    F.col("REL_GRP_PROV_NM").alias("REL_GRP_PROV_NM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_MBR_VST_F = (
    df_Buff
    .withColumn("VST_ID", F.rpad(F.col("VST_ID"), 256, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 256, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PRI_POL_ID", F.rpad(F.col("PRI_POL_ID"), 256, " "))
    .withColumn("SEC_POL_ID", F.rpad(F.col("SEC_POL_ID"), 256, " "))
    .withColumn("PATN_EXTRNL_VNDR_ID", F.rpad(F.col("PATN_EXTRNL_VNDR_ID"), 256, " "))
    .withColumn("PROV_EXTRNL_VNDR_ID", F.rpad(F.col("PROV_EXTRNL_VNDR_ID"), 256, " "))
    .withColumn("VST_DT_SK", F.rpad(F.col("VST_DT_SK"), 10, " "))
    .withColumn("VST_STTUS_CD", F.rpad(F.col("VST_STTUS_CD"), 256, " "))
    .withColumn("VST_CLSD_DT_SK", F.rpad(F.col("VST_CLSD_DT_SK"), 10, " "))
    .withColumn("BILL_RVWED_DT_SK", F.rpad(F.col("BILL_RVWED_DT_SK"), 10, " "))
    .withColumn("BILL_RVWED_BY_NM", F.rpad(F.col("BILL_RVWED_BY_NM"), 256, " "))
    .withColumn("SVC_PROV_NTNL_PROV_ID", F.rpad(F.col("SVC_PROV_NTNL_PROV_ID"), 256, " "))
    .withColumn("SVC_PROV_ID", F.rpad(F.col("SVC_PROV_ID"), 256, " "))
    .withColumn("SVC_PROV_NM", F.rpad(F.col("SVC_PROV_NM"), 256, " "))
    .withColumn("REL_IPA_PROV_ID", F.rpad(F.col("REL_IPA_PROV_ID"), 256, " "))
    .withColumn("REL_IPA_PROV_NM", F.rpad(F.col("REL_IPA_PROV_NM"), 256, " "))
    .withColumn("REL_GRP_PROV_ID", F.rpad(F.col("REL_GRP_PROV_ID"), 256, " "))
    .withColumn("REL_GRP_PROV_NM", F.rpad(F.col("REL_GRP_PROV_NM"), 256, " "))
    .withColumn("MBR_FULL_NM", F.rpad(F.col("MBR_FULL_NM"), 256, " "))
    .select(
        "MBR_VST_SK",
        "MBR_UNIQ_KEY",
        "VST_ID",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRI_POL_ID",
        "SEC_POL_ID",
        "PATN_EXTRNL_VNDR_ID",
        "PROV_EXTRNL_VNDR_ID",
        "VST_DT_SK",
        "VST_STTUS_CD",
        "VST_CLSD_DT_SK",
        "BILL_RVWED_DT_SK",
        "BILL_RVWED_BY_NM",
        "SVC_PROV_NTNL_PROV_ID",
        "SVC_PROV_SK",
        "SVC_PROV_ID",
        "SVC_PROV_NM",
        "REL_IPA_PROV_SK",
        "REL_IPA_PROV_ID",
        "REL_IPA_PROV_NM",
        "REL_GRP_PROV_SK",
        "REL_GRP_PROV_ID",
        "REL_GRP_PROV_NM",
        "MBR_SK",
        "MBR_FULL_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsEdwMbrVstFLoad_MBR_VST_F_temp", jdbc_url, jdbc_props)

(
    df_MBR_VST_F.write
        .format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", "STAGING.IdsEdwMbrVstFLoad_MBR_VST_F_temp")
        .mode("overwrite")
        .save()
)

merge_sql = f"""
MERGE {EDWOwner}.MBR_VST_F AS T
USING STAGING.IdsEdwMbrVstFLoad_MBR_VST_F_temp AS S
ON T.MBR_VST_SK = S.MBR_VST_SK
WHEN MATCHED THEN UPDATE SET
  T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
  T.VST_ID = S.VST_ID,
  T.SRC_SYS_CD = S.SRC_SYS_CD,
  T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  T.PRI_POL_ID = S.PRI_POL_ID,
  T.SEC_POL_ID = S.SEC_POL_ID,
  T.PATN_EXTRNL_VNDR_ID = S.PATN_EXTRNL_VNDR_ID,
  T.PROV_EXTRNL_VNDR_ID = S.PROV_EXTRNL_VNDR_ID,
  T.VST_DT_SK = S.VST_DT_SK,
  T.VST_STTUS_CD = S.VST_STTUS_CD,
  T.VST_CLSD_DT_SK = S.VST_CLSD_DT_SK,
  T.BILL_RVWED_DT_SK = S.BILL_RVWED_DT_SK,
  T.BILL_RVWED_BY_NM = S.BILL_RVWED_BY_NM,
  T.SVC_PROV_NTNL_PROV_ID = S.SVC_PROV_NTNL_PROV_ID,
  T.SVC_PROV_SK = S.SVC_PROV_SK,
  T.SVC_PROV_ID = S.SVC_PROV_ID,
  T.SVC_PROV_NM = S.SVC_PROV_NM,
  T.REL_IPA_PROV_SK = S.REL_IPA_PROV_SK,
  T.REL_IPA_PROV_ID = S.REL_IPA_PROV_ID,
  T.REL_IPA_PROV_NM = S.REL_IPA_PROV_NM,
  T.REL_GRP_PROV_SK = S.REL_GRP_PROV_SK,
  T.REL_GRP_PROV_ID = S.REL_GRP_PROV_ID,
  T.REL_GRP_PROV_NM = S.REL_GRP_PROV_NM,
  T.MBR_SK = S.MBR_SK,
  T.MBR_FULL_NM = S.MBR_FULL_NM,
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
  T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
INSERT (
  MBR_VST_SK,
  MBR_UNIQ_KEY,
  VST_ID,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_DT_SK,
  LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  PRI_POL_ID,
  SEC_POL_ID,
  PATN_EXTRNL_VNDR_ID,
  PROV_EXTRNL_VNDR_ID,
  VST_DT_SK,
  VST_STTUS_CD,
  VST_CLSD_DT_SK,
  BILL_RVWED_DT_SK,
  BILL_RVWED_BY_NM,
  SVC_PROV_NTNL_PROV_ID,
  SVC_PROV_SK,
  SVC_PROV_ID,
  SVC_PROV_NM,
  REL_IPA_PROV_SK,
  REL_IPA_PROV_ID,
  REL_IPA_PROV_NM,
  REL_GRP_PROV_SK,
  REL_GRP_PROV_ID,
  REL_GRP_PROV_NM,
  MBR_SK,
  MBR_FULL_NM,
  CRT_RUN_CYC_EXCTN_SK,
  LAST_UPDT_RUN_CYC_EXCTN_SK
)
VALUES (
  S.MBR_VST_SK,
  S.MBR_UNIQ_KEY,
  S.VST_ID,
  S.SRC_SYS_CD,
  S.CRT_RUN_CYC_EXCTN_DT_SK,
  S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  S.PRI_POL_ID,
  S.SEC_POL_ID,
  S.PATN_EXTRNL_VNDR_ID,
  S.PROV_EXTRNL_VNDR_ID,
  S.VST_DT_SK,
  S.VST_STTUS_CD,
  S.VST_CLSD_DT_SK,
  S.BILL_RVWED_DT_SK,
  S.BILL_RVWED_BY_NM,
  S.SVC_PROV_NTNL_PROV_ID,
  S.SVC_PROV_SK,
  S.SVC_PROV_ID,
  S.SVC_PROV_NM,
  S.REL_IPA_PROV_SK,
  S.REL_IPA_PROV_ID,
  S.REL_IPA_PROV_NM,
  S.REL_GRP_PROV_SK,
  S.REL_GRP_PROV_ID,
  S.REL_GRP_PROV_NM,
  S.MBR_SK,
  S.MBR_FULL_NM,
  S.CRT_RUN_CYC_EXCTN_SK,
  S.LAST_UPDT_RUN_CYC_EXCTN_SK
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)