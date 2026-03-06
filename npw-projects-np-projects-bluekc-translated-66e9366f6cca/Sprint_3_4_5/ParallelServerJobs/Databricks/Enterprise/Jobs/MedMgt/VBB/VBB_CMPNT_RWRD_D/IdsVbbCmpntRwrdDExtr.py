# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS VBB_CMPNT_RWRD table and creates a load file for EDW VBB_CMPNT_RWRD_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewdevl        Bhoomi Dasari             5/17/2013

# MAGIC EDW VBB_CMPNT_RWRD extract from IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2007-12-12')
EDWRunCycle = get_widget_value('EDWRunCycle','0')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_vbb_cmpnt_rwrd = f"""
SELECT 
VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SK,
VBB_CMPNT_RWRD.VBB_CMPNT_UNIQ_KEY,
VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SEQ_NO,
VBB_CMPNT_RWRD.SRC_SYS_CD_SK,
VBB_CMPNT_RWRD.CRT_RUN_CYC_EXCTN_SK,
VBB_CMPNT_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK,
VBB_CMPNT_RWRD.VBB_CMPNT_SK,
VBB_CMPNT_RWRD.VBB_RWRD_SK,
VBB_CMPNT_RWRD.ERN_RWRD_END_DT_TYP_CD_SK,
VBB_CMPNT_RWRD.ERN_RWRD_STRT_DT_TYP_CD_SK,
VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_BEG_DT_SK,
VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_END_DT_SK,
VBB_CMPNT_RWRD.ERN_RWRD_END_DT_SK,
VBB_CMPNT_RWRD.ERN_RWRD_STRT_DT_SK,
VBB_CMPNT_RWRD.SRC_SYS_CRT_DTM,
VBB_CMPNT_RWRD.SRC_SYS_UPDT_DTM,
VBB_CMPNT_RWRD.ERN_RWRD_AMT,
VBB_CMPNT_RWRD.RQRD_ACHV_LVL_NO,
VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_DESC
FROM {IDSOwner}.VBB_CMPNT_RWRD VBB_CMPNT_RWRD
WHERE VBB_CMPNT_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_VBB_CMPNT_RWRD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_vbb_cmpnt_rwrd)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_cmpnt_data = f"""
SELECT 
VBB_CMPNT_D.VBB_CMPNT_SK,
VBB_CMPNT_D.VBB_CMPNT_FUNC_CAT_CD,
VBB_CMPNT_D.VBB_CMPNT_FUNC_CAT_NM,
VBB_CMPNT_D.VBB_CMPNT_FUNC_CD,
VBB_CMPNT_D.VBB_CMPNT_FUNC_NM,
VBB_CMPNT_D.VBB_CMPNT_FUNC_CAT_CD_SK,
VBB_CMPNT_D.VBB_CMPNT_FUNC_CD_SK
FROM {EDWOwner}.VBB_CMPNT_D VBB_CMPNT_D
"""
df_cmpnt_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_cmpnt_data)
    .load()
)

extract_query_nonident = f"""
SELECT 
VBB_CMPNT_D.VBB_CMPNT_SK,
VBB_CMPNT_D_TWO.VBB_CMPNT_FUNC_CD,
VBB_CMPNT_D_TWO.VBB_CMPNT_FUNC_NM,
VBB_CMPNT_D_TWO.VBB_CMPNT_FUNC_CD_SK
FROM
{EDWOwner}.VBB_CMPNT_D VBB_CMPNT_D,
{EDWOwner}.VBB_PLN_CMPNT_DPNDC_D VBB_PLN_CMPNT_DPNDC_D,
{EDWOwner}.VBB_CMPNT_D VBB_CMPNT_D_TWO
WHERE 
VBB_CMPNT_D.VBB_CMPNT_FUNC_CAT_CD <> 'ID'
AND VBB_CMPNT_D.VBB_CMPNT_SK = VBB_PLN_CMPNT_DPNDC_D.VBB_CMPNT_SK
AND VBB_PLN_CMPNT_DPNDC_D.RQRD_VBB_CMPNT_SK = VBB_CMPNT_D_TWO.VBB_CMPNT_SK
AND VBB_CMPNT_D_TWO.VBB_CMPNT_FUNC_CAT_CD= 'ID'
"""
df_nonident = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_nonident)
    .load()
)

df_cmpnt = df_cmpnt_data.dropDuplicates(["VBB_CMPNT_SK"])
df_funccd_notid = df_nonident.dropDuplicates(["VBB_CMPNT_SK"])

df_biz_logic = (
    df_VBB_CMPNT_RWRD.alias("Extract")
    .join(df_hf_cdma_codes.alias("Src_sys_cd"), col("Extract.SRC_SYS_CD_SK") == col("Src_sys_cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes.alias("Rwrd_End_Dt_Typ_Cd"), col("Extract.ERN_RWRD_END_DT_TYP_CD_SK") == col("Rwrd_End_Dt_Typ_Cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes.alias("Rwrd_Strt_Dt_Typ_Cd"), col("Extract.ERN_RWRD_STRT_DT_TYP_CD_SK") == col("Rwrd_Strt_Dt_Typ_Cd.CD_MPPNG_SK"), "left")
    .join(df_cmpnt.alias("Cmpnt"), col("Extract.VBB_CMPNT_SK") == col("Cmpnt.VBB_CMPNT_SK"), "left")
    .join(df_funccd_notid.alias("funccd_notid"), col("Extract.VBB_CMPNT_SK") == col("funccd_notid.VBB_CMPNT_SK"), "left")
    .withColumn(
        "svVbbCmpntFuncCatCd",
        when(col("Cmpnt.VBB_CMPNT_FUNC_CAT_CD").isNull(), lit("UNK")).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_CAT_CD"))
    )
)

df_loadfile = df_biz_logic.select(
    col("Extract.VBB_CMPNT_RWRD_SK").alias("VBB_CMPNT_RWRD_SK"),
    col("Extract.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    col("Extract.VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
    when(col("Src_sys_cd.TRGT_CD").isNull(), lit("UNK")).otherwise(col("Src_sys_cd.TRGT_CD")).alias("SRC_SYS_CD"),
    rpad(lit(CurrRunCycleDate), 10, ' ').alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrRunCycleDate), 10, ' ').alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    col("Extract.VBB_RWRD_SK").alias("VBB_RWRD_SK"),
    when(col("Rwrd_End_Dt_Typ_Cd.TRGT_CD").isNull(), lit("UNK")).otherwise(col("Rwrd_End_Dt_Typ_Cd.TRGT_CD")).alias("ERN_RWRD_END_DT_TYP_CD"),
    when(col("Rwrd_End_Dt_Typ_Cd.TRGT_CD_NM").isNull(), lit("UNK")).otherwise(col("Rwrd_End_Dt_Typ_Cd.TRGT_CD_NM")).alias("ERN_RWRD_END_DT_TYP_NM"),
    when(col("Rwrd_Strt_Dt_Typ_Cd.TRGT_CD").isNull(), lit("UNK")).otherwise(col("Rwrd_Strt_Dt_Typ_Cd.TRGT_CD")).alias("ERN_RWRD_STRT_DT_TYP_CD"),
    when(col("Rwrd_Strt_Dt_Typ_Cd.TRGT_CD_NM").isNull(), lit("UNK")).otherwise(col("Rwrd_Strt_Dt_Typ_Cd.TRGT_CD_NM")).alias("ERN_RWRD_STRT_DT_TYP_NM"),
    when(
        col("svVbbCmpntFuncCatCd") == lit("ID"),
        when(col("Cmpnt.VBB_CMPNT_FUNC_CD").isNull(), lit("UNK")).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_CD"))
    ).otherwise(
        when(col("funccd_notid.VBB_CMPNT_FUNC_CD").isNull(), lit("UNK")).otherwise(col("funccd_notid.VBB_CMPNT_FUNC_CD"))
    ).alias("ID_VBB_CMPNT_FUNC_CD"),
    when(
        col("svVbbCmpntFuncCatCd") == lit("ID"),
        when(col("Cmpnt.VBB_CMPNT_FUNC_NM").isNull(), lit("UNK")).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_NM"))
    ).otherwise(
        when(col("funccd_notid.VBB_CMPNT_FUNC_NM").isNull(), lit("UNK")).otherwise(col("funccd_notid.VBB_CMPNT_FUNC_NM"))
    ).alias("ID_VBB_CMPNT_FUNC_NM"),
    when(col("Cmpnt.VBB_CMPNT_FUNC_CAT_CD").isNull(), lit("UNK")).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_CAT_CD")).alias("VBB_CMPNT_FUNC_CAT_CD"),
    when(col("Cmpnt.VBB_CMPNT_FUNC_CAT_NM").isNull(), lit("UNK")).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_CAT_NM")).alias("VBB_CMPNT_FUNC_CAT_NM"),
    rpad(col("Extract.VBB_CMPNT_RWRD_BEG_DT_SK"), 10, ' ').alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
    rpad(col("Extract.VBB_CMPNT_RWRD_END_DT_SK"), 10, ' ').alias("VBB_CMPNT_RWRD_END_DT_SK"),
    rpad(col("Extract.ERN_RWRD_END_DT_SK"), 10, ' ').alias("ERN_RWRD_END_DT_SK"),
    rpad(col("Extract.ERN_RWRD_STRT_DT_SK"), 10, ' ').alias("ERN_RWRD_STRT_DT_SK"),
    col("Extract.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    col("Extract.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    col("Extract.ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
    col("Extract.RQRD_ACHV_LVL_NO").alias("RQRD_ACHV_LVL_NO"),
    col("Extract.VBB_CMPNT_RWRD_DESC").alias("VBB_CMPNT_RWRD_DESC"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.ERN_RWRD_END_DT_TYP_CD_SK").alias("ERN_RWRD_END_DT_TYP_CD_SK"),
    col("Extract.ERN_RWRD_STRT_DT_TYP_CD_SK").alias("ERN_RWRD_STRT_DT_TYP_CD_SK"),
    when(
        col("svVbbCmpntFuncCatCd") == lit("ID"),
        when(col("Cmpnt.VBB_CMPNT_FUNC_CD_SK").isNull(), lit(0)).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_CD_SK"))
    ).otherwise(
        when(col("funccd_notid.VBB_CMPNT_FUNC_CD_SK").isNull(), lit(0)).otherwise(col("funccd_notid.VBB_CMPNT_FUNC_CD_SK"))
    ).alias("ID_VBB_CMPNT_FUNC_CD_SK"),
    when(col("Cmpnt.VBB_CMPNT_FUNC_CAT_CD_SK").isNull(), lit(0)).otherwise(col("Cmpnt.VBB_CMPNT_FUNC_CAT_CD_SK")).alias("VBB_CMPNT_FUNC_CAT_CD_SK")
)

write_files(
    df_loadfile,
    f"{adls_path}/load/VBB_CMPNT_RWRD_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)