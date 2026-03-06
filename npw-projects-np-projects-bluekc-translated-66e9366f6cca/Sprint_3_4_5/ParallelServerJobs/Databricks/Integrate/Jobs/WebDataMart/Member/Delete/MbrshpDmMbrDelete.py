# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Called by: IdsMbrshDmCntl                 
# MAGIC                            
# MAGIC PROCESSING:  Use the SYSDUMMY1 table to initiate the delete statements.  Each delete statement looks for source and claim ID where the run cycle is older than the current run cycle.  These rows were not update with the current process so they don't exist in the source system anymore.
# MAGIC                   
# MAGIC                    
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi D               07/10/2009       3500                                                  Originally Programmed                                                                   devlIDSnew                    Steph Goddard           07/15/2009
# MAGIC Ralph Tucker        08/03/2009        3500                                                  Added MBRSH_DM_MBR_PRI_CARE_PROV to Del                 devlIDSnew                    Steph Goddard           08/04/2009
# MAGIC SAndrew                 2009-10-01        4113 Mbr360                   Added tables Group Restrcit and Mcare                                                        devlIDSnew                    Steph Goddard           10/17/2009
# MAGIC 
# MAGIC Terri O'Bryan          2009-10-28         3500 Addendum                               Added table MBRSH_DM_GRP_REL_EDI                                  devlIDSnew                     Steph Godard             10/29/2009
# MAGIC Ralph Tucker         2011-12-15        TTR-1151                                         Added table MBRSH_DM_MBR_BE_KEY_XREF                        IngegrateCurDevl             Sharon Anderw           2011-12-21
# MAGIC Siva Devagiri         2013-10-01         5114                                                 Converted to parallel                                                                     IntegrateWhseDevl          Peter Marshall             10/22/2013

# MAGIC Job Name ; 
# MAGIC EdwDmProvDirProvDelete
# MAGIC Row Generator To Drive
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
RunCycle = get_widget_value('RunCycle','')

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

schema_Row_Gen_Sys_Dummy = StructType([
    StructField("LAST_UPDT_RUN_CYC_NO", StringType(), True)
])
data_Row_Gen_Sys_Dummy = [(RunCycle,)]
df_Row_Gen_Sys_Dummy = spark.createDataFrame(data_Row_Gen_Sys_Dummy, schema_Row_Gen_Sys_Dummy)

df_MbrGroupRestrict = df_Row_Gen_Sys_Dummy
df_ClmDmMbrCob = df_Row_Gen_Sys_Dummy
df_MbrElig = df_Row_Gen_Sys_Dummy
df_MbrPCP = df_Row_Gen_Sys_Dummy
df_MbrHealthScreen = df_Row_Gen_Sys_Dummy
df_MbrGroup = df_Row_Gen_Sys_Dummy
df_MbrGroupRelEDI = df_Row_Gen_Sys_Dummy
df_MbrBeKeyXref = df_Row_Gen_Sys_Dummy
df_MbrMcareEvt = df_Row_Gen_Sys_Dummy

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_GRP_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrGroup.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_GRP_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_GRP
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_GRP_Out_temp t
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_COB_out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_ClmDmMbrCob.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_COB_out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_MBR_COB
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_COB_out_temp t
)
AND EXISTS (
  SELECT *
  FROM {ClmMartOwner}.W_MBR_DEL d
  WHERE d.SRC_SYS_CD = {ClmMartOwner}.MBRSH_DM_MBR_COB.SRC_SYS_CD
    AND d.MBR_UNIQ_KEY = {ClmMartOwner}.MBRSH_DM_MBR_COB.MBR_UNIQ_KEY
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_GRP_REL_EDI_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrGroupRelEDI.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_GRP_REL_EDI_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_GRP_REL_EDI
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_GRP_REL_EDI_Out_temp t
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_ELIG_out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrElig.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_ELIG_out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_MBR_ELIG
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_ELIG_out_temp t
)
AND EXISTS (
  SELECT *
  FROM {ClmMartOwner}.W_MBR_DEL d
  WHERE d.SRC_SYS_CD = {ClmMartOwner}.MBRSH_DM_MBR_ELIG.SRC_SYS_CD
    AND d.MBR_UNIQ_KEY = {ClmMartOwner}.MBRSH_DM_MBR_ELIG.MBR_UNIQ_KEY
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_BE_KEY_XREF_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrBeKeyXref.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_BE_KEY_XREF_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_MBR_BE_KEY_XREF
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_BE_KEY_XREF_Out_temp t
)
AND EXISTS (
  SELECT *
  FROM {ClmMartOwner}.W_MBR_DEL d
  WHERE d.SRC_SYS_CD = {ClmMartOwner}.MBRSH_DM_MBR_BE_KEY_XREF.SRC_SYS_CD
    AND d.MBR_UNIQ_KEY = {ClmMartOwner}.MBRSH_DM_MBR_BE_KEY_XREF.MBR_UNIQ_KEY
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_HLTH_SCRN_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrHealthScreen.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_HLTH_SCRN_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_MBR_HLTH_SCRN
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_HLTH_SCRN_Out_temp t
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrPCP.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_MBR_PRI_CARE_PROV
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_Out_temp t
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_MCARE_EVT_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrMcareEvt.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_MCARE_EVT_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_MBR_MCARE_EVT
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_MBR_MCARE_EVT_Out_temp t
)
AND EXISTS (
  SELECT *
  FROM {ClmMartOwner}.W_MBR_DEL d
  WHERE d.SRC_SYS_CD = {ClmMartOwner}.MBRSH_DM_MBR_MCARE_EVT.SRC_SYS_CD
    AND d.MBR_UNIQ_KEY = {ClmMartOwner}.MBRSH_DM_MBR_MCARE_EVT.MBR_UNIQ_KEY
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)

execute_dml("DROP TABLE IF EXISTS STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_GRP_RSTRCT_Out_temp", jdbc_url_clmmart, jdbc_props_clmmart)
df_MbrGroupRestrict.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_GRP_RSTRCT_Out_temp") \
    .mode("overwrite") \
    .save()
delete_sql = f"""
DELETE FROM {ClmMartOwner}.MBRSH_DM_GRP_RSTRCT
WHERE LAST_UPDT_RUN_CYC_NO <> (
  SELECT t.LAST_UPDT_RUN_CYC_NO
  FROM STAGING.MbrshpDmMbrDelete_Odbc_MBRSH_DM_GRP_RSTRCT_Out_temp t
)
"""
execute_dml(delete_sql, jdbc_url_clmmart, jdbc_props_clmmart)