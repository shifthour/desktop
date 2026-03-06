# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/11/09 15:13:12 Batch  15046_54866 PROMOTE bckcetl ids20 dsadm rc for ralph 
# MAGIC ^1_1 03/11/09 14:40:56 Batch  15046_52880 INIT bckcett testIDScur dsadm rc for ralph 
# MAGIC ^1_4 03/09/09 11:24:23 Batch  15044_41067 PROMOTE bckcett testIDScur u03651 steph for Ralph
# MAGIC ^1_4 03/09/09 11:01:22 Batch  15044_39685 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_3 03/06/09 14:38:31 Batch  15041_52722 INIT bckcett devlIDScur u08717 Brent for Ralph
# MAGIC ^1_2 03/05/09 20:44:06 Batch  15040_74649 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_1 03/05/09 14:08:04 Batch  15040_50886 INIT bckcett devlIDScur u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     IdsAhyEligBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi D                        02/27/2009           3863                             Originally Programmed                             devIDScur                  Steph Goddard           03/04/2009

# MAGIC Update the P_RUN_CYC table with Balancing Indicators set to 'Y' to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Extract P_RUN_CYC records for IDS claims with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
BeginCycle = get_widget_value('BeginCycle','')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD, max(RUN_CYC_NO) as RUN_CYC_NO "
    f"FROM {IDSOwner}.P_RUN_CYC "
    f"WHERE TRGT_SYS_CD = '{TrgtSys}' "
    f"AND SRC_SYS_CD = '{SrcSys}' "
    f"AND SUBJ_CD = '{SubjCd}' "
    f"AND ROW_CT_BAL_IN = 'N' "
    f"AND RUN_CYC_NO >= {BeginCycle} "
    f"GROUP BY SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD"
)

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules = df_IDS.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUBJ_CD").alias("SUBJ_CD"),
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

df_BusinessRules = df_BusinessRules.withColumn("SRC_SYS_CD", F.rpad("SRC_SYS_CD", <...>, " "))
df_BusinessRules = df_BusinessRules.withColumn("SUBJ_CD", F.rpad("SUBJ_CD", <...>, " "))
df_BusinessRules = df_BusinessRules.withColumn("TRGT_SYS_CD", F.rpad("TRGT_SYS_CD", <...>, " "))

df_BusinessRules = df_BusinessRules.select("SRC_SYS_CD", "SUBJ_CD", "TRGT_SYS_CD", "RUN_CYC_NO")

drop_sql = "DROP TABLE IF EXISTS STAGING.BalAhyEligRunCycUpd_P_RUN_CYC_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_BusinessRules
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.BalAhyEligRunCycUpd_P_RUN_CYC_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING (
  SELECT SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO
  FROM STAGING.BalAhyEligRunCycUpd_P_RUN_CYC_temp
) AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO <= S.RUN_CYC_NO
  AND T.ROW_CT_BAL_IN = 'N'
  AND T.ROW_TO_ROW_BAL_IN = 'N'
  AND T.CLMN_SUM_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET
    T.ROW_CT_BAL_IN = 'Y',
    T.ROW_TO_ROW_BAL_IN = 'Y',
    T.CLMN_SUM_BAL_IN = 'Y'
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, ROW_CT_BAL_IN, ROW_TO_ROW_BAL_IN, CLMN_SUM_BAL_IN)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, 'Y', 'Y', 'Y');
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)