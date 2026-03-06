# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table SUB_CLS_AUDIT into EDW table SUB_CLS_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 SUB_CLS_AUDIT
# MAGIC                 SUB
# MAGIC                 GRP
# MAGIC                 APP_USER
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Bhoomi Dasari    10/30/2006  ---    Originally Programmed
# MAGIC 
# MAGIC                                                                                                                                                                                                                  
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Rajasekhar Mangalampally   2013-10-11          5114                                                         Original Programming                                   EnterpriseWrhsDevl                                  Jag Yelavarthi                 2013-10-11
# MAGIC                                                                                                                                           (Server to Parallel Conversion)
# MAGIC Balkarn Gill                          11/20/2013        5114                             NA and UNK changes are made as per the DG standards                EnterpriseWrhsDevl

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwSubClsAuditDExtr
# MAGIC 
# MAGIC Table:
# MAGIC SUB_CLS_AUDIT_D
# MAGIC Read from source table SUB_CLS_AUDIT, GRP, CLS and APP_USER from IDS: 
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXTN_SK
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write SUB_CLS_AUDIT_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) SUB_CLS_AUDIT_ACTN_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_SUB_CLS_AUDIT_D_in = f"""
SELECT 
SUB_CLS_AUDIT.SUB_CLS_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_ROW_ID,
SUB_CLS_AUDIT.CLS_SK,
SUB_CLS_AUDIT.SRC_SYS_CRT_USER_SK,
SUB_CLS_AUDIT.SUB_SK,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_ACTN_CD_SK,
SUB_CLS_AUDIT.SRC_SYS_CRT_DT_SK,
SUB_CLS_AUDIT.SUB_UNIQ_KEY,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_EFF_DT,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_TERM_DT,
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
CLS.CLS_ID,
APP_USER.USER_ID
FROM {IDSOwner}.SUB_CLS_AUDIT SUB_CLS_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON SUB_CLS_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP,
{IDSOwner}.APP_USER APP_USER,
{IDSOwner}.CLS CLS
WHERE SUB_CLS_AUDIT.SUB_SK=SUB.SUB_SK
AND SUB_CLS_AUDIT.CLS_SK=CLS.CLS_SK
AND SUB.GRP_SK=GRP.GRP_SK
AND SUB_CLS_AUDIT.SRC_SYS_CRT_USER_SK=APP_USER.USER_SK
AND SUB_CLS_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""
df_db2_SUB_CLS_AUDIT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_CLS_AUDIT_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lkp_Codes = (
    df_db2_SUB_CLS_AUDIT_D_in.alias("src")
    .join(
        df_db2_CD_MPPNG_Extr.alias("lkp"),
        on=[F.col("src.SUB_CLS_AUDIT_ACTN_CD_SK") == F.col("lkp.CD_MPPNG_SK")],
        how="left"
    )
    .select(
        F.col("src.SUB_CLS_AUDIT_SK").alias("SUB_CLS_AUDIT_SK"),
        F.col("src.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("src.SUB_CLS_AUDIT_ROW_ID").alias("SUB_CLS_AUDIT_ROW_ID"),
        F.col("src.CLS_SK").alias("CLS_SK"),
        F.col("src.GRP_SK").alias("GRP_SK"),
        F.col("src.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("src.SUB_SK").alias("SUB_SK"),
        F.col("src.CLS_ID").alias("CLS_ID"),
        F.col("src.GRP_ID").alias("GRP_ID"),
        F.col("src.GRP_NM").alias("GRP_NM"),
        F.col("src.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("src.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("src.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.col("lkp.TRGT_CD").alias("SUB_CLS_AUDIT_ACTN_CD"),
        F.col("lkp.TRGT_CD_NM").alias("SUB_CLS_AUDIT_ACTN_NM"),
        F.col("src.SUB_CLS_AUDIT_EFF_DT").alias("SUB_CLS_AUDIT_EFF_DT"),
        F.col("src.SUB_CLS_AUDIT_TERM_DT").alias("SUB_CLS_AUDIT_TERM_DT"),
        F.col("src.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("src.SUB_CLS_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK")
    )
)

df_SubClsAuditDMain = (
    df_lkp_Codes
    .filter((F.col("SUB_CLS_AUDIT_SK") != 0) & (F.col("SUB_CLS_AUDIT_SK") != 1))
    .select(
        F.col("SUB_CLS_AUDIT_SK").alias("SUB_CLS_AUDIT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("SUB_CLS_AUDIT_ROW_ID").alias("SUB_CLS_AUDIT_ROW_ID"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CLS_SK").alias("CLS_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("CLS_ID").alias("CLS_ID"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_NM").alias("GRP_NM"),
        F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.lit("00").alias("MBR_SFX_NO"),
        F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.col("SUB_CLS_AUDIT_ACTN_CD").alias("SUB_CLS_AUDIT_ACTN_CD"),
        F.col("SUB_CLS_AUDIT_ACTN_NM").alias("SUB_CLS_AUDIT_ACTN_NM"),
        F.col("SUB_CLS_AUDIT_EFF_DT").alias("SUB_CLS_AUDIT_EFF_DT"),
        F.col("SUB_CLS_AUDIT_TERM_DT").alias("SUB_CLS_AUDIT_TERM_DT"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SUB_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK")
    )
)

df_UNK = spark.range(1).select(
    F.lit(0).alias("SUB_CLS_AUDIT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("SUB_CLS_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLS_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit("UNK").alias("CLS_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("GRP_NM"),
    F.lit(0).alias("GRP_UNIQ_KEY"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CRT_USER_ID"),
    F.lit("UNK").alias("SUB_CLS_AUDIT_ACTN_CD"),
    F.lit("UNK").alias("SUB_CLS_AUDIT_ACTN_NM"),
    F.lit(" ").alias("SUB_CLS_AUDIT_EFF_DT"),
    F.lit(" ").alias("SUB_CLS_AUDIT_TERM_DT"),
    F.lit(0).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("SUB_AUDIT_ACTN_CD_SK")
)

df_NA = spark.range(1).select(
    F.lit(1).alias("SUB_CLS_AUDIT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("SUB_CLS_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLS_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit("NA").alias("CLS_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("GRP_NM"),
    F.lit(1).alias("GRP_UNIQ_KEY"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CRT_USER_ID"),
    F.lit("NA").alias("SUB_CLS_AUDIT_ACTN_CD"),
    F.lit("NA").alias("SUB_CLS_AUDIT_ACTN_NM"),
    F.lit(" ").alias("SUB_CLS_AUDIT_EFF_DT"),
    F.lit(" ").alias("SUB_CLS_AUDIT_TERM_DT"),
    F.lit(1).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("SUB_AUDIT_ACTN_CD_SK")
)

df_fnl_NA_UNK = df_SubClsAuditDMain.unionByName(df_NA).unionByName(df_UNK)

df_final = df_fnl_NA_UNK.select(
    F.col("SUB_CLS_AUDIT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SUB_CLS_AUDIT_ROW_ID"),
    F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_SK"),
    F.col("GRP_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK"),
    F.col("CLS_ID"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_UNIQ_KEY"),
    F.col("MBR_SFX_NO"),
    F.rpad("SRC_SYS_CRT_DT_SK", 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SRC_SYS_CRT_USER_ID"),
    F.col("SUB_CLS_AUDIT_ACTN_CD"),
    F.col("SUB_CLS_AUDIT_ACTN_NM"),
    F.rpad("SUB_CLS_AUDIT_EFF_DT", 10, " ").alias("SUB_CLS_AUDIT_EFF_DT"),
    F.rpad("SUB_CLS_AUDIT_TERM_DT", 10, " ").alias("SUB_CLS_AUDIT_TERM_DT"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_AUDIT_ACTN_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_CLS_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)