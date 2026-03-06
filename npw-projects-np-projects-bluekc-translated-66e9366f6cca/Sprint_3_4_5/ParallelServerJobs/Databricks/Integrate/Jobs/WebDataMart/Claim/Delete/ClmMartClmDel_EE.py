# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ODIFICATIONS:
# MAGIC               Brent Leland   03/28/2006 - Originally programmed.
# MAGIC               Brent Leland   04/28/2006 -  Added CLM_DM_CLM_LN_PROC_CD_MOD table.
# MAGIC               O. Nielsen       05/25/2006 - Changed Delete SQL to use Full Table name rather than an Alias - SQL server choked trying to delete from an Alias
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC   
# MAGIC SANdrew                2009-05-20        TTR 529               Added ClmLnRemitDisallow to table to clear out old records                     devlIDS               Steph Goddard         05/21/2009      
# MAGIC   
# MAGIC        
# MAGIC Archana Palivela     2013-10-21        5114                     Originally programmed.                                                                    IntegrateWrhsDevl

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType, DateType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
RunCycle = get_widget_value('RunCycle','')

df_row_gen_sys_dummy = spark.createDataFrame([(RunCycle, )], ["LAST_UPDT_RUN_CYC_NO"])

df_clm_dm_clm_diag = df_row_gen_sys_dummy.select(F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"))
df_clm_dm_clm_proc = df_row_gen_sys_dummy.select(F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"))
df_clm_dm_clm_ln_remit_dsalw = df_row_gen_sys_dummy.select(F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"))
df_clm_dm_clm_ln = df_row_gen_sys_dummy.select(F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"))
df_clm_dm_clm_ln_proc_cd_mod = df_row_gen_sys_dummy.select(F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"))

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_DIAG_Out_temp", jdbc_url, jdbc_props)
df_clm_dm_clm_diag.write.jdbc(jdbc_url, "STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_DIAG_Out_temp", mode="append", properties=jdbc_props)
delete_sql_diag = f"""DELETE T
FROM {ClmMartOwner}.CLM_DM_CLM_DIAG AS T
JOIN STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_DIAG_Out_temp AS S
  ON 1=1
WHERE T.LAST_UPDT_RUN_CYC_NO <> S.LAST_UPDT_RUN_CYC_NO
  AND EXISTS (
    SELECT *
    FROM {ClmMartOwner}.W_CLM_DEL d
    WHERE d.SRC_SYS_CD = T.SRC_SYS_CD
      AND d.CLM_ID = T.CLM_ID
  )"""
execute_dml(delete_sql_diag, jdbc_url, jdbc_props)

execute_dml("DROP TABLE IF EXISTS STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_PROC_Out_temp", jdbc_url, jdbc_props)
df_clm_dm_clm_proc.write.jdbc(jdbc_url, "STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_PROC_Out_temp", mode="append", properties=jdbc_props)
delete_sql_proc = f"""DELETE T
FROM {ClmMartOwner}.CLM_DM_CLM_PROC AS T
JOIN STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_PROC_Out_temp AS S
  ON 1=1
WHERE T.LAST_UPDT_RUN_CYC_NO <> S.LAST_UPDT_RUN_CYC_NO
  AND EXISTS (
    SELECT *
    FROM {ClmMartOwner}.W_CLM_DEL d
    WHERE d.SRC_SYS_CD = T.SRC_SYS_CD
      AND d.CLM_ID = T.CLM_ID
  )"""
execute_dml(delete_sql_proc, jdbc_url, jdbc_props)

execute_dml("DROP TABLE IF EXISTS STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_Out_temp", jdbc_url, jdbc_props)
df_clm_dm_clm_ln_remit_dsalw.write.jdbc(jdbc_url, "STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_Out_temp", mode="append", properties=jdbc_props)
delete_sql_ln_remit_dsalw = f"""DELETE T
FROM {ClmMartOwner}.CLM_DM_CLM_LN_REMIT_DSALW AS T
JOIN STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_Out_temp AS S
  ON 1=1
WHERE T.LAST_UPDT_RUN_CYC_NO <> S.LAST_UPDT_RUN_CYC_NO
  AND (
       T.CLM_LN_DSALW_TYP_CD = 'HST'
    OR T.CLM_LN_DSALW_TYP_CD = 'HST1'
    OR T.CLM_LN_DSALW_TYP_CD = 'HST2'
    OR T.CLM_LN_DSALW_TYP_CD = 'HST3'
    OR T.CLM_LN_DSALW_TYP_CD = 'HST4'
    OR T.CLM_LN_DSALW_TYP_CD = 'HST5'
  )
  AND EXISTS (
    SELECT *
    FROM {ClmMartOwner}.W_CLM_DEL d
    WHERE d.SRC_SYS_CD = T.SRC_SYS_CD
      AND d.CLM_ID = T.CLM_ID
  )"""
execute_dml(delete_sql_ln_remit_dsalw, jdbc_url, jdbc_props)

execute_dml("DROP TABLE IF EXISTS STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_Out_temp", jdbc_url, jdbc_props)
df_clm_dm_clm_ln.write.jdbc(jdbc_url, "STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_Out_temp", mode="append", properties=jdbc_props)
delete_sql_ln = f"""DELETE T
FROM {ClmMartOwner}.CLM_DM_CLM_LN AS T
JOIN STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_Out_temp AS S
  ON 1=1
WHERE T.LAST_UPDT_RUN_CYC_NO <> S.LAST_UPDT_RUN_CYC_NO
  AND EXISTS (
    SELECT *
    FROM {ClmMartOwner}.W_CLM_DEL d
    WHERE d.SRC_SYS_CD = T.SRC_SYS_CD
      AND d.CLM_ID = T.CLM_ID
  )"""
execute_dml(delete_sql_ln, jdbc_url, jdbc_props)

execute_dml("DROP TABLE IF EXISTS STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_Out_temp", jdbc_url, jdbc_props)
df_clm_dm_clm_ln_proc_cd_mod.write.jdbc(jdbc_url, "STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_Out_temp", mode="append", properties=jdbc_props)
delete_sql_ln_proc_cd_mod = f"""DELETE T
FROM {ClmMartOwner}.CLM_DM_CLM_LN_PROC_CD_MOD AS T
JOIN STAGING.ClmMartClmDel_EE_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_Out_temp AS S
  ON 1=1
WHERE T.LAST_UPDT_RUN_CYC_NO <> S.LAST_UPDT_RUN_CYC_NO
  AND EXISTS (
    SELECT *
    FROM {ClmMartOwner}.W_CLM_DEL d
    WHERE d.SRC_SYS_CD = T.SRC_SYS_CD
      AND d.CLM_ID = T.CLM_ID
  )"""
execute_dml(delete_sql_ln_proc_cd_mod, jdbc_url, jdbc_props)