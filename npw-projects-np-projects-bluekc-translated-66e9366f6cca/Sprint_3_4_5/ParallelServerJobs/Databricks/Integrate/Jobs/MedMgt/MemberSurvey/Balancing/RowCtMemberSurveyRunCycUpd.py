# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrSrvyRspnBalCntl
# MAGIC 
# MAGIC PROCESSING: Updates P_RUN_CYC table and sets the BAL_IN to 'Y'
# MAGIC                       
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2011-06-01             4673                             Initial Programming                                            IntegrateWrhsDevl      Brent Leland                 06-16-2011

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Membership with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
BeginCycle = get_widget_value('BeginCycle','100')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT
SRC_SYS_CD,
TRGT_SYS_CD,
SUBJ_CD,
max(RUN_CYC_NO) as RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = '{TrgtSys}'
  AND SRC_SYS_CD = '{SrcSys}'
  AND SUBJ_CD = '{SubjCd}'
  AND ROW_CT_BAL_IN = 'N'
  AND RUN_CYC_NO >= {BeginCycle}
GROUP BY
SRC_SYS_CD,
TRGT_SYS_CD,
SUBJ_CD
"""

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

execute_dml("DROP TABLE IF EXISTS STAGING.RowCtMemberSurveyRunCycUpd_P_RUN_CYC_temp", jdbc_url, jdbc_props)

df_BusinessRules.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.RowCtMemberSurveyRunCycUpd_P_RUN_CYC_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS DST
USING STAGING.RowCtMemberSurveyRunCycUpd_P_RUN_CYC_temp AS SRC
ON
    DST.SRC_SYS_CD = SRC.SRC_SYS_CD
    AND DST.SUBJ_CD = SRC.SUBJ_CD
    AND DST.TRGT_SYS_CD = SRC.TRGT_SYS_CD
    AND DST.RUN_CYC_NO <= SRC.RUN_CYC_NO
    AND DST.ROW_CT_BAL_IN = 'N'
WHEN MATCHED THEN
    UPDATE SET
        ROW_CT_BAL_IN = 'Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)