# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 07/13/06 09:01:21 Batch  14074_32499 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_1 07/13/06 08:56:37 Batch  14074_32207 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_5 06/28/06 09:07:27 Batch  14059_32851 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_4 06/28/06 09:02:32 Batch  14059_32558 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/20/06 16:37:06 Batch  14051_59831 PROMOTE bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/20/06 16:35:44 Batch  14051_59749 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_2 06/20/06 16:34:07 Batch  14051_59651 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/19/06 11:16:37 Batch  14050_40618 PROMOTE bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/19/06 11:10:31 Batch  14050_40241 PROMOTE bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/19/06 11:02:44 Batch  14050_39770 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC 
# MAGIC ********************************************************************************************************************************************************************************************************************************************************************
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsWebDmRunCycUpd
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Med Mgt Data Mart.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	IDS - UM
# MAGIC                
# MAGIC 
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  The run cycle is passed in as a parameter.  The run cycle is the minimum of all the run cycles where the Med Mgt Dm indicator is "N".  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles greater or equal to the minimum where the indicator is "N" are updated to "Y".
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    IDS    - P_RUN_CYC
# MAGIC                   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-06-15      Tao Luo                   Original Programming.

# MAGIC Run Cycle Extract Update
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the Med Mgt Data Mart for each of the run cycles processed.  Updates all of the records that have run cycles greater or equal to the parameter run cycle.
# MAGIC Fetch one row only
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
RunCycle = get_widget_value('RunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT MAX(UM.LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.UM UM")
    .load()
)

df_Trans1 = (
    df_IDS
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("SUBJ_CD", lit("MEDMGT"))
    .withColumn("TRGT_SYS_CD", lit("IDS"))
    .withColumn("RUN_CYC_NO", lit(RunCycle))
    .withColumn("CDM_LOAD_IN", lit("Y"))
)

df_P_RUN_CYC = df_Trans1.select(
    "SRC_SYS_CD",
    "SUBJ_CD",
    "TRGT_SYS_CD",
    "RUN_CYC_NO",
    "CDM_LOAD_IN"
)

df_P_RUN_CYC = df_P_RUN_CYC.withColumn("CDM_LOAD_IN", rpad("CDM_LOAD_IN", 1, " "))

execute_dml(f"DROP TABLE IF EXISTS STAGING.MedMgtDmLoadRunCycUpd_P_RUN_CYC_temp", jdbc_url, jdbc_props)

(
    df_P_RUN_CYC
    .write
    .format("jdbc")
    .mode("overwrite")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.MedMgtDmLoadRunCycUpd_P_RUN_CYC_temp")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS target
USING STAGING.MedMgtDmLoadRunCycUpd_P_RUN_CYC_temp AS source
ON (
  target.SRC_SYS_CD = source.SRC_SYS_CD
  AND target.SUBJ_CD = source.SUBJ_CD
  AND target.TRGT_SYS_CD = source.TRGT_SYS_CD
  AND target.RUN_CYC_NO <= source.RUN_CYC_NO
  AND target.CDM_LOAD_IN = 'N'
)
WHEN MATCHED THEN
  UPDATE
  SET target.CDM_LOAD_IN = source.CDM_LOAD_IN;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)