# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/12/09 10:26:07 Batch  15261_37578 PROMOTE bckcetl:31540 edw10 dsadm bls for rt
# MAGIC ^1_2 10/12/09 10:20:46 Batch  15261_37248 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_3 09/28/09 11:45:32 Batch  15247_42360 PROMOTE bckcett:31540 testEDW u150906 TTR583-UMMedMgt_Ralph_testEDW              Maddy
# MAGIC ^1_3 09/28/09 11:36:19 Batch  15247_41826 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW              Maddy
# MAGIC ^1_2 09/02/09 09:53:22 Batch  15221_35654 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW                     Maddy
# MAGIC ^1_1 08/20/09 14:03:45 Batch  15208_50662 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW                         Maddy
# MAGIC ^1_1 02/12/08 14:23:33 Batch  14653_51818 PROMOTE bckcetl edw10 dsadm bls for sg
# MAGIC ^1_1 02/12/08 14:16:14 Batch  14653_51377 INIT bckcett testEDW dsadm bls for sg
# MAGIC ^1_1 01/31/08 13:08:41 Batch  14641_47327 PROMOTE bckcett testEDW u03651 steph for Bhoomi
# MAGIC ^1_1 01/31/08 13:06:32 Batch  14641_47195 INIT bckcett devlEDW u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwMedMgtCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restart, no other steps necessary
# MAGIC 
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Bhoomi Dasari  01-18-2008    Medmgt          Original program                                                             Steph Goddard    01/23/2008
# MAGIC 
# MAGIC Developer                       Date               Project/Altiris #               Change Description                                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Tracy Davis                    3/27/2009      3808                               Original Programming.                                                                             devlEDW                     Steph Goddard             04/03/2009
# MAGIC Ralph Tucker                 8/14/2009      15 - Prod Support            updated target code to EDW                                                                 devlEDW                     Steph Goddard             08/19/2009

# MAGIC Extract P_RUN_CYC records for IDS MedMgt with a value \"N\" in the  EDW_LOAD_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC Set EDW_LOAD_IN to 'Y'
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


BeginCycle = get_widget_value('BeginCycle','0')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD, min(RUN_CYC_NO) as RUN_CYC_NO FROM {IDSOwner}.P_RUN_CYC WHERE TRGT_SYS_CD='IDS' AND SUBJ_CD='MEDMGT' AND EDW_LOAD_IN='N' AND RUN_CYC_NO >= {BeginCycle} GROUP BY SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD"

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules = df_IDS.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUBJ_CD").alias("SUBJ_CD"),
    col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

execute_dml("DROP TABLE IF EXISTS STAGING.EdwLoadMedMgtRunCycUpd_P_RUN_CYC_temp", jdbc_url, jdbc_props)

df_BusinessRules.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwLoadMedMgtRunCycUpd_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.EdwLoadMedMgtRunCycUpd_P_RUN_CYC_temp AS S
ON
  T.SRC_SYS_CD=S.SRC_SYS_CD
  AND T.SUBJ_CD=S.SUBJ_CD
  AND T.TRGT_SYS_CD=S.TRGT_SYS_CD
  AND T.RUN_CYC_NO >= S.RUN_CYC_NO
  AND T.EDW_LOAD_IN='N'
WHEN MATCHED THEN
  UPDATE SET T.EDW_LOAD_IN='Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)