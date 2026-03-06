# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: IdsEdwProvAllDelCntl
# MAGIC               
# MAGIC PROCESSING:  
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Revathi BoojiReddy 2024-03-04      US 611552             Original Development                                                      IntegrateDev1               Jeyaprasanna             2024-04-04

# MAGIC This is a delete process for edw and Ids for
# MAGIC IDS Table:
# MAGIC LAB_RSLT
# MAGIC CLM_SUPLMT_DIAG 
# MAGIC MBR_BIO_MESR_RSLT 
# MAGIC 
# MAGIC EDW Tables:
# MAGIC LAB_RSLT_F
# MAGIC CLM_SUPLMT_DIAG_D
# MAGIC MBR_BIO_MESR_RSLT_F tables .
# MAGIC IDS and EDW delete process from hit list delete file.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
TableName_edw = get_widget_value('TableName_edw','')
Environment = get_widget_value('Environment','')
EDWOwner = get_widget_value('$EDWOwner','')
SRC_SYS_CD = get_widget_value('SRC_SYS_CD','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSOwner = get_widget_value('$IDSOwner','')
SRC_SYS_CD_SK = get_widget_value('SRC_SYS_CD_SK','')
TableName_ids = get_widget_value('TableName_ids','')
LastRunCycle = get_widget_value('LastRunCycle','')
FileName = get_widget_value('FileName','')
SRC_SYS_CD_Col = get_widget_value('SRC_SYS_CD_Col','')
edw_secret_name = get_widget_value('edw_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_IDS_EWD_DelList = StructType([
    StructField("Dummy", StringType(), False)
])

df_IDS_EWD_DelList = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IDS_EWD_DelList)
    .load(f"{adls_path_raw}/landing/{FileName}")
)

df_xfm = df_IDS_EWD_DelList
df_xfm_lnk_IDS = df_xfm
df_xfm_lnk_edw = df_xfm

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
delete_sql_edw = "Delete FROM #$EDWOwner#.#TableName_edw# WHERE CRT_RUN_CYC_EXCTN_SK = #IDSRunCycle# AND LAST_UPDT_RUN_CYC_EXCTN_DT_SK < '#RunDate#' AND SRC_SYS_CD = '#SRC_SYS_CD#'"
execute_dml(delete_sql_edw, jdbc_url_edw, jdbc_props_edw)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
delete_sql_ids = "Delete FROM #$IDSOwner#.#TableName_ids# WHERE LAST_UPDT_RUN_CYC_EXCTN_SK < #LastRunCycle# AND to_char(#SRC_SYS_CD_Col#) = '#SRC_SYS_CD_SK#' AND CRT_RUN_CYC_EXCTN_SK = #IDSRunCycle#"
execute_dml(delete_sql_ids, jdbc_url_ids, jdbc_props_ids)