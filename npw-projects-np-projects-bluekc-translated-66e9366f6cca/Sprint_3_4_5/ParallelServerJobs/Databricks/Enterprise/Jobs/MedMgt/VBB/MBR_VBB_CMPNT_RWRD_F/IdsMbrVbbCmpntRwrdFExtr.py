# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS MBR_VBB_CMPNT_RWRD table and creates a load file for EDW MBR_VBB_CMPNT_RWRD_F
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-23    4963 VBB Phase III           Initial Programming                                    EnterpriseNewdevl        Bhoomi Dasari               7/9/2013

# MAGIC EDW MBR_VBB_CMPNT_RWRD extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2007-12-12')
EDWRunCycle = get_widget_value('EDWRunCycle','0')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

extract_query_MBR_VBB_CMPNT_RWRD = (
    "SELECT "
    "MBR_VBB_CMPNT_RWRD.MBR_VBB_CMPNT_RWRD_SK,"
    "MBR_VBB_CMPNT_RWRD.MBR_UNIQ_KEY,"
    "MBR_VBB_CMPNT_RWRD.VBB_CMPNT_UNIQ_KEY,"
    "MBR_VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SEQ_NO,"
    "MBR_VBB_CMPNT_RWRD.SRC_SYS_CD_SK,"
    "MBR_VBB_CMPNT_RWRD.CRT_RUN_CYC_EXCTN_SK,"
    "MBR_VBB_CMPNT_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK,"
    "MBR_VBB_CMPNT_RWRD.MBR_VBB_CMPNT_ENR_SK,"
    "MBR_VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SK,"
    "MBR_VBB_CMPNT_RWRD.RWRD_LMTED_RSN_CD_SK,"
    "MBR_VBB_CMPNT_RWRD.ERN_RWRD_END_DT_SK,"
    "MBR_VBB_CMPNT_RWRD.ERN_RWRD_STRT_DT_SK,"
    "MBR_VBB_CMPNT_RWRD.SRC_SYS_CRT_DTM,"
    "MBR_VBB_CMPNT_RWRD.SRC_SYS_UPDT_DTM,"
    "MBR_VBB_CMPNT_RWRD.CMPLD_ACHV_LVL_NO,"
    "MBR_VBB_CMPNT_RWRD.ERN_RWRD_AMT,"
    "MBR_VBB_CMPNT_RWRD.PD_RWRD_AMT,"
    "MBR_VBB_CMPNT_RWRD.TRZ_MBR_UNVRS_ID "
    f"FROM {IDSOwner}.MBR_VBB_CMPNT_RWRD MBR_VBB_CMPNT_RWRD "
    f"WHERE MBR_VBB_CMPNT_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
)

df_MBR_VBB_CMPNT_RWRD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MBR_VBB_CMPNT_RWRD)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

execute_dml("DROP TABLE IF EXISTS STAGING.IdsMbrVbbCmpntRwrdFExtr_MBR_D_temp", jdbc_url_edw, jdbc_props_edw)

(
    df_MBR_VBB_CMPNT_RWRD
    .write
    .format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable", "STAGING.IdsMbrVbbCmpntRwrdFExtr_MBR_D_temp")
    .mode("append")
    .save()
)

df_MBR_VBB_CMPNT_RWRD_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"SELECT t.*, d.MBR_SK AS MBR_SK_mbr "
        f"FROM STAGING.IdsMbrVbbCmpntRwrdFExtr_MBR_D_temp t "
        f"LEFT JOIN {EDWOwner}.MBR_D d ON t.MBR_UNIQ_KEY = d.MBR_UNIQ_KEY"
    )
    .load()
)

df_Extract_1 = df_MBR_VBB_CMPNT_RWRD_mbr.alias("Extract").join(
    df_hf_cdma_codes.alias("Src_sys_cd"),
    col("Extract.SRC_SYS_CD_SK") == col("Src_sys_cd.CD_MPPNG_SK"),
    how="left"
)

df_Extract_2 = df_Extract_1.join(
    df_hf_cdma_codes.alias("Rwrd_Lmt_Rsn_Cd"),
    col("Extract.RWRD_LMTED_RSN_CD_SK") == col("Rwrd_Lmt_Rsn_Cd.CD_MPPNG_SK"),
    how="left"
)

df_enriched = df_Extract_2.select(
    col("Extract.MBR_VBB_CMPNT_RWRD_SK").alias("MBR_VBB_CMPNT_RWRD_SK"),
    col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Extract.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    col("Extract.VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
    when(col("Src_sys_cd.TRGT_CD").isNull(), lit("UNK")).otherwise(col("Src_sys_cd.TRGT_CD")).alias("SRC_SYS_CD"),
    rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("MBR_SK_mbr").isNull(), lit(0)).otherwise(col("MBR_SK_mbr")).alias("MBR_SK"),
    col("Extract.MBR_VBB_CMPNT_ENR_SK").alias("MBR_VBB_CMPNT_ENR_SK"),
    col("Extract.VBB_CMPNT_RWRD_SK").alias("VBB_CMPNT_RWRD_SK"),
    when(col("Rwrd_Lmt_Rsn_Cd.TRGT_CD").isNull(), lit("UNK")).otherwise(col("Rwrd_Lmt_Rsn_Cd.TRGT_CD")).alias("RWRD_LMTED_RSN_CD"),
    when(col("Rwrd_Lmt_Rsn_Cd.TRGT_CD_NM").isNull(), lit("UNK")).otherwise(col("Rwrd_Lmt_Rsn_Cd.TRGT_CD_NM")).alias("RWRD_LMTED_RSN_NM"),
    rpad(col("Extract.ERN_RWRD_END_DT_SK"), 10, " ").alias("ERN_RWRD_END_DT_SK"),
    rpad(col("Extract.ERN_RWRD_STRT_DT_SK"), 10, " ").alias("ERN_RWRD_STRT_DT_SK"),
    col("Extract.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    col("Extract.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    col("Extract.CMPLD_ACHV_LVL_NO").alias("CMPLD_ACHEIVEMENT_LVL_NO"),
    col("Extract.ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
    col("Extract.PD_RWRD_AMT").alias("PD_RWRD_AMT"),
    col("Extract.TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.RWRD_LMTED_RSN_CD_SK").alias("RWRD_LMTED_RSN_CD_SK")
)

write_files(
    df_enriched,
    f"{adls_path}/load/MBR_VBB_CMPNT_RWRD_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)