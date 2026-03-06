# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Rajasekhar Mangalampally                                5114                                                         Original Programming                                   EnterpriseWrhsDevl                                Jag Yelavarthi                 2013-10-18
# MAGIC                                                                                                                                           (Server to Parallel Conversion)

# MAGIC This is Source extract data from an EDW table 
# MAGIC 
# MAGIC Job Name:
# MAGIC EdwEdwWSumAuditExtr
# MAGIC 
# MAGIC Table:
# MAGIC W_SUB_AUDIT
# MAGIC Read from source tables 
# MAGIC 1)	MBR_ELIG_AUDIT_D
# MAGIC 2)	MBR_PCP_AUDIT_D
# MAGIC 3)	SUB_ELIG_AUDIT_D
# MAGIC 4)	SUB_ADDR_AUDIT_D
# MAGIC 5)	SUB_CLS_AUDIT_D
# MAGIC 6)	DP_RATE_AUDIT_D
# MAGIC 7)	MBR_AUDIT_D
# MAGIC 8)	SUB_AUDIT_D
# MAGIC 9)	MBR_COB_AUDIT_D
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write W_SUM_AUDIT Data into a Sequential file for Load Job.
# MAGIC Add Defaults and Null Handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad, when
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# db2_EdwEdwMbrEligAuditDSum_in
extract_query = f"""
SELECT 
  MBR_ELIG_AUDIT_D.SUB_SK,
  MBR_ELIG_AUDIT_D.MBR_SFX_NO,
  MBR_ELIG_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_ELIG_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_MBR_ELIG 
FROM {EDWOwner}.MBR_ELIG_AUDIT_D MBR_ELIG_AUDIT_D
WHERE 
  MBR_ELIG_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  MBR_ELIG_AUDIT_D.SUB_SK,
  MBR_ELIG_AUDIT_D.MBR_SFX_NO,
  MBR_ELIG_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_ELIG_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwMbrEligAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_MbrElig = df_db2_EdwEdwMbrEligAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    col("CNT_MBR_ELIG").alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwSubAuditDSum_in
extract_query = f"""
SELECT 
  SUB_AUDIT_D.SUB_SK,
  SUB_AUDIT_D.MBR_SFX_NO,
  SUB_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_SUB
FROM {EDWOwner}.SUB_AUDIT_D SUB_AUDIT_D
WHERE 
  SUB_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  SUB_AUDIT_D.SUB_SK,
  SUB_AUDIT_D.MBR_SFX_NO,
  SUB_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwSubAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_Sub = df_db2_EdwEdwSubAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    when(col("MBR_SFX_NO").isNull(), lit('')).otherwise(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    col("CNT_SUB").alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwSubClsAuditDSum_in
extract_query = f"""
SELECT 
  SUB_CLS_AUDIT_D.SUB_SK,
  SUB_CLS_AUDIT_D.MBR_SFX_NO,
  SUB_CLS_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_CLS_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_SUB_CLS
FROM {EDWOwner}.SUB_CLS_AUDIT_D SUB_CLS_AUDIT_D
WHERE 
  SUB_CLS_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  SUB_CLS_AUDIT_D.SUB_SK,
  SUB_CLS_AUDIT_D.MBR_SFX_NO,
  SUB_CLS_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_CLS_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwSubClsAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_SubCls = df_db2_EdwEdwSubClsAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    col("CNT_SUB_CLS").alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwMbrAuditDSum_in
extract_query = f"""
SELECT 
  MBR_AUDIT_D.SUB_SK,
  MBR_AUDIT_D.MBR_SFX_NO,
  MBR_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_MBR
FROM {EDWOwner}.MBR_AUDIT_D MBR_AUDIT_D
WHERE 
  MBR_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY 
  MBR_AUDIT_D.SUB_SK,
  MBR_AUDIT_D.MBR_SFX_NO,
  MBR_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwMbrAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_Mbr = df_db2_EdwEdwMbrAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    col("CNT_MBR").alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwSubAddrAuditDSum_in
extract_query = f"""
SELECT 
  SUB_ADDR_AUDIT_D.SUB_SK,
  SUB_ADDR_AUDIT_D.MBR_SFX_NO,
  SUB_ADDR_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_ADDR_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_SUB_ADDR
FROM {EDWOwner}.SUB_ADDR_AUDIT_D SUB_ADDR_AUDIT_D
WHERE 
  SUB_ADDR_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  SUB_ADDR_AUDIT_D.SUB_SK,
  SUB_ADDR_AUDIT_D.MBR_SFX_NO,
  SUB_ADDR_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_ADDR_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwSubAddrAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_SubAddr = df_db2_EdwEdwSubAddrAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    col("CNT_SUB_ADDR").alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwSubEligAuditDSum_in
extract_query = f"""
SELECT 
  SUB_ELIG_AUDIT_D.SUB_SK,
  SUB_ELIG_AUDIT_D.MBR_SFX_NO,
  SUB_ELIG_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_ELIG_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_SUB_ELIG
FROM {EDWOwner}.SUB_ELIG_AUDIT_D SUB_ELIG_AUDIT_D
WHERE 
  SUB_ELIG_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  SUB_ELIG_AUDIT_D.SUB_SK,
  SUB_ELIG_AUDIT_D.MBR_SFX_NO,
  SUB_ELIG_AUDIT_D.SRC_SYS_CRT_DT_SK,
  SUB_ELIG_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwSubEligAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_SubElig = df_db2_EdwEdwSubEligAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    col("CNT_SUB_ELIG").alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwMbrCobAuditDSum_in
extract_query = f"""
SELECT 
  MBR_COB_AUDIT_D.SUB_SK,
  MBR_COB_AUDIT_D.MBR_SFX_NO,
  MBR_COB_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_COB_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_MBR_COB
FROM {EDWOwner}.MBR_COB_AUDIT_D MBR_COB_AUDIT_D
WHERE 
  MBR_COB_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  MBR_COB_AUDIT_D.SUB_SK,
  MBR_COB_AUDIT_D.MBR_SFX_NO,
  MBR_COB_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_COB_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwMbrCobAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_MbrCob = df_db2_EdwEdwMbrCobAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    col("CNT_MBR_COB").alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwMbrPcpAuditDSum_in
extract_query = f"""
SELECT 
  MBR_PCP_AUDIT_D.SUB_SK,
  MBR_PCP_AUDIT_D.MBR_SFX_NO,
  MBR_PCP_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_PCP_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_MBR_PCP
FROM {EDWOwner}.MBR_PCP_AUDIT_D MBR_PCP_AUDIT_D
WHERE 
  MBR_PCP_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  MBR_PCP_AUDIT_D.SUB_SK,
  MBR_PCP_AUDIT_D.MBR_SFX_NO,
  MBR_PCP_AUDIT_D.SRC_SYS_CRT_DT_SK,
  MBR_PCP_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwMbrPcpAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_MbrPcp = df_db2_EdwEdwMbrPcpAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    lit(0).alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    col("CNT_MBR_PCP").alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

# db2_EdwEdwDpRateAuditDSum_in
extract_query = f"""
SELECT 
  DP_RATE_AUDIT_D.SUB_SK,
  DP_RATE_AUDIT_D.MBR_SFX_NO,
  DP_RATE_AUDIT_D.SRC_SYS_CRT_DT_SK,
  DP_RATE_AUDIT_D.SRC_SYS_CRT_USER_ID,
  COUNT(*) AS CNT_DP_RATE
FROM {EDWOwner}.DP_RATE_AUDIT_D DP_RATE_AUDIT_D
WHERE 
  DP_RATE_AUDIT_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK >= '{EDWRunCycleDate}'
GROUP BY
  DP_RATE_AUDIT_D.SUB_SK,
  DP_RATE_AUDIT_D.MBR_SFX_NO,
  DP_RATE_AUDIT_D.SRC_SYS_CRT_DT_SK,
  DP_RATE_AUDIT_D.SRC_SYS_CRT_USER_ID
"""
df_db2_EdwEdwDpRateAuditDSum_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic_DpRate = df_db2_EdwEdwDpRateAuditDSum_in.select(
    col("SUB_SK").alias("SUB_SK"),
    trim(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    trim(col("SRC_SYS_CRT_USER_ID")).alias("SRC_SYS_CRT_USER_ID"),
    col("CNT_DP_RATE").alias("DP_RATE_AUDIT_CT"),
    lit(0).alias("MBR_AUDIT_CT"),
    lit(0).alias("MBR_COB_AUDIT_CT"),
    lit(0).alias("MBR_ELIG_AUDIT_CT"),
    lit(0).alias("MBR_PCP_AUDIT_CT"),
    lit(0).alias("SUB_ADDR_AUDIT_CT"),
    lit(0).alias("SUB_AUDIT_CT"),
    lit(0).alias("SUB_CLS_AUDIT_CT"),
    lit(0).alias("SUB_ELIG_AUDIT_CT")
)

df_fnl_WSumAudit = (
    df_xfm_BusinessLogic_Mbr
    .unionByName(df_xfm_BusinessLogic_MbrElig)
    .unionByName(df_xfm_BusinessLogic_MbrPcp)
    .unionByName(df_xfm_BusinessLogic_MbrCob)
    .unionByName(df_xfm_BusinessLogic_SubElig)
    .unionByName(df_xfm_BusinessLogic_Sub)
    .unionByName(df_xfm_BusinessLogic_SubAddr)
    .unionByName(df_xfm_BusinessLogic_SubCls)
    .unionByName(df_xfm_BusinessLogic_DpRate)
)

df_fnl_WSumAudit_select = df_fnl_WSumAudit.select(
    col("SUB_SK"),
    col("MBR_SFX_NO"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SRC_SYS_CRT_USER_ID"),
    col("DP_RATE_AUDIT_CT"),
    col("MBR_AUDIT_CT"),
    col("MBR_COB_AUDIT_CT"),
    col("MBR_ELIG_AUDIT_CT"),
    col("MBR_PCP_AUDIT_CT"),
    col("SUB_ADDR_AUDIT_CT"),
    col("SUB_AUDIT_CT"),
    col("SUB_CLS_AUDIT_CT"),
    col("SUB_ELIG_AUDIT_CT")
)

write_files(
    df_fnl_WSumAudit_select,
    f"{adls_path}/load/W_SUM_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)