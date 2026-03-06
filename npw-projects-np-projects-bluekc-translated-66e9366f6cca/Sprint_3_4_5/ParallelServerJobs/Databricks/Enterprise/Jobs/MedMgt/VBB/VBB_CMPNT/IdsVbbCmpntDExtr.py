# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS VBB_CMPNT table and creates a load file for EDW VBB_CMPNT_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewdevl        Bhoomi Dasari             5/17/2013

# MAGIC EDW VBB_CMPNT extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2007-12-12')
EDWRunCycle = get_widget_value('EDWRunCycle','0')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  VBB_CMPNT.VBB_CMPNT_SK,
  VBB_CMPNT.VBB_CMPNT_UNIQ_KEY,
  VBB_CMPNT.SRC_SYS_CD_SK,
  VBB_CMPNT.CRT_RUN_CYC_EXCTN_SK,
  VBB_CMPNT.LAST_UPDT_RUN_CYC_EXCTN_SK,
  VBB_CMPNT.VBB_PLN_SK,
  VBB_CMPNT.VBB_CMPLNC_METH_CD_SK,
  VBB_CMPNT.VBB_CMPNT_FNSH_RULE_CD_SK,
  VBB_CMPNT.VBB_CMPNT_FUNC_CAT_CD_SK,
  VBB_CMPNT.VBB_CMPNT_FUNC_CD_SK,
  VBB_CMPNT.MBR_ACHV_VALID_ALW_IN,
  VBB_CMPNT.VBB_CMPNT_REENR_IN,
  VBB_CMPNT.SRC_SYS_CRT_DTM,
  VBB_CMPNT.SRC_SYS_UPDT_DTM,
  VBB_CMPNT.VBB_CMPNT_ACHV_LVL_CT,
  VBB_CMPNT.VBB_CMPNT_FNSH_DAYS_NO,
  VBB_CMPNT.VBB_CMPNT_REINST_DAYS_NO,
  VBB_CMPNT.VBB_TMPLT_UNIQ_KEY,
  VBB_CMPNT.VBB_CMPNT_NM,
  VBB_CMPNT.VBB_CMPNT_TYP_NM
FROM {IDSOwner}.VBB_CMPNT VBB_CMPNT
WHERE VBB_CMPNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_VBB_CMPNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_bussinesslogic = (
    df_VBB_CMPNT.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("Src_sys_cd"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("Src_sys_cd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Cmplnc_meth_cd"),
        F.col("Extract.VBB_CMPLNC_METH_CD_SK") == F.col("Cmplnc_meth_cd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Func_Cat_cd"),
        F.col("Extract.VBB_CMPNT_FUNC_CAT_CD_SK") == F.col("Func_Cat_cd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Func_Cd"),
        F.col("Extract.VBB_CMPNT_FUNC_CD_SK") == F.col("Func_Cd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Cmpnt_Fnsh_Rule_Cd"),
        F.col("Extract.VBB_CMPNT_FNSH_RULE_CD_SK") == F.col("Cmpnt_Fnsh_Rule_Cd.CD_MPPNG_SK"),
        "left",
    )
)

df_LoadFile = df_bussinesslogic.select(
    F.col("Extract.VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    F.col("Extract.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.when(F.col("Src_sys_cd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Src_sys_cd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.rpad(F.lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.VBB_PLN_SK").alias("VBB_PLN_SK"),
    F.when(F.col("Cmplnc_meth_cd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Cmplnc_meth_cd.TRGT_CD")).alias("VBB_CMPLNC_METH_CD"),
    F.when(F.col("Cmplnc_meth_cd.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Cmplnc_meth_cd.TRGT_CD_NM")).alias("VBB_CMPLNC_METH_NM"),
    F.when(F.col("Cmpnt_Fnsh_Rule_Cd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Cmpnt_Fnsh_Rule_Cd.TRGT_CD")).alias("VBB_CMPNT_FNSH_RULE_CD"),
    F.when(F.col("Cmpnt_Fnsh_Rule_Cd.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Cmpnt_Fnsh_Rule_Cd.TRGT_CD_NM")).alias("VBB_CMPNT_FNSH_RULE_NM"),
    F.when(F.col("Func_Cat_cd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Func_Cat_cd.TRGT_CD")).alias("VBB_CMPNT_FUNC_CAT_CD"),
    F.when(F.col("Func_Cat_cd.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Func_Cat_cd.TRGT_CD_NM")).alias("VBB_CMPNT_FUNC_CAT_NM"),
    F.when(F.col("Func_Cd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Func_Cd.TRGT_CD")).alias("VBB_CMPNT_FUNC_CD"),
    F.when(F.col("Func_Cd.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Func_Cd.TRGT_CD_NM")).alias("VBB_CMPNT_FUNC_NM"),
    F.rpad(F.col("Extract.MBR_ACHV_VALID_ALW_IN"), 1, " ").alias("MBR_ACHV_VALID_ALW_IN"),
    F.rpad(F.col("Extract.VBB_CMPNT_REENR_IN"), 1, " ").alias("VBB_CMPNT_REENR_IN"),
    F.col("Extract.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("Extract.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("Extract.VBB_CMPNT_ACHV_LVL_CT").alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("Extract.VBB_CMPNT_FNSH_DAYS_NO").alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("Extract.VBB_CMPNT_REINST_DAYS_NO").alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("Extract.VBB_CMPNT_NM").alias("VBB_CMPNT_NM"),
    F.col("Extract.VBB_CMPNT_TYP_NM").alias("VBB_CMPNT_TYP_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.VBB_CMPLNC_METH_CD_SK").alias("VBB_CMPLNC_METH_CD_SK"),
    F.col("Extract.VBB_CMPNT_FUNC_CAT_CD_SK").alias("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.col("Extract.VBB_CMPNT_FUNC_CD_SK").alias("VBB_CMPNT_FUNC_CD_SK"),
    F.col("Extract.VBB_CMPNT_FNSH_RULE_CD_SK").alias("VBB_CMPNT_FNSH_RULE_CD_SK")
)

write_files(
    df_LoadFile,
    f"{adls_path}/load/VBB_CMPNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)