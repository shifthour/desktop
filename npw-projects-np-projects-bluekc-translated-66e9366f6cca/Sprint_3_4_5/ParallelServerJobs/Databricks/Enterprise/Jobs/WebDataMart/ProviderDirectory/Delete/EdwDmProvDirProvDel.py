# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                  
# MAGIC                            
# MAGIC PROCESSING:  Use the SYSDUMMY1 table to initiate the delete statements.  Each delete statement looks for source and claim ID where the run cycle is older than the current run cycle.  These rows were not update with the current process so they don't exist in the source system anymore.
# MAGIC                   
# MAGIC                    
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                 Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker                           07/30/2009      3500                                                   Originally Programmed                                                                 devlIDSnew                  
# MAGIC Steph Goddard                         10/29/08          TTR624                                             added new tables, implemented                                                   devlEDWnew                 SAndrew                       2009-10-29
# MAGIC 
# MAGIC Rajasekhar Manglampally         2013-06-11        5114                                                  Original Programming                                                                   EnterpriseWrhsDevl        Jag Yelavarthi               2013-09-01
# MAGIC                                                                                                                                      ( Server to Parallel Conversion )

# MAGIC Row Generator To Drive
# MAGIC Job Name ; 
# MAGIC EdwDmProvDirProvDelete
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
LastUpdtDt = get_widget_value('LastUpdtDt','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

schema_Row_Gen_Sys_Dummy = StructType([
    StructField("LAST_UPDT_DT", StringType(), True)
])
df_Row_Gen_Sys_Dummy = spark.createDataFrame([(LastUpdtDt,)], schema_Row_Gen_Sys_Dummy)

df_cpy_forBuffer = df_Row_Gen_Sys_Dummy

val_cpy_forBuffer = df_cpy_forBuffer.select("LAST_UPDT_DT").collect()[0][0]

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

delete_sql_1 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_BRD_CRTF WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_1, jdbc_url, jdbc_props)

delete_sql_2 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_LANG WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_2, jdbc_url, jdbc_props)

delete_sql_3 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_PROC_FEE WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_3, jdbc_url, jdbc_props)

delete_sql_4 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_NTWK WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_4, jdbc_url, jdbc_props)

delete_sql_5 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_FCLTY_TYP WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_5, jdbc_url, jdbc_props)

delete_sql_6 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_TYP WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_6, jdbc_url, jdbc_props)

delete_sql_7 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_SPEC WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_7, jdbc_url, jdbc_props)

delete_sql_8 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_8, jdbc_url, jdbc_props)

delete_sql_9 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_ADDR WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_9, jdbc_url, jdbc_props)

delete_sql_10 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_NTWK WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_10, jdbc_url, jdbc_props)

delete_sql_11 = f"DELETE FROM {WebProvDirOwner}.PROV_DIR_DM_PROV_ACRDTN WHERE LAST_UPDT_DT < '{val_cpy_forBuffer}'"
execute_dml(delete_sql_11, jdbc_url, jdbc_props)