# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 13:00:58 Batch  14605_46863 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/09/07 10:10:10 Batch  14527_36614 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/01/07 10:17:35 Batch  14519_37064 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/01/07 09:47:40 Batch  14519_35267 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_2 09/26/07 17:31:58 Batch  14514_63125 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 09/26/07 17:07:37 Batch  14514_61663 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 09/25/07 15:06:34 Batch  14513_54400 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     Both FctsProvBalCntl and DeaProvBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/21/2007          3264                              Originally Programmed                           devlIDS30                  Steph Goddard           9/6/07

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Providers with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BeginCycle = get_widget_value('BeginCycle','100')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
SRC_SYS_CD,
TRGT_SYS_CD,
SUBJ_CD,
MAX(RUN_CYC_NO) as RUN_CYC_NO
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

temp_table = "STAGING.RowCtProvRunCycUpd_P_RUN_CYC_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_BusinessRules.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO <= S.RUN_CYC_NO
  AND T.ROW_CT_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET T.ROW_CT_BAL_IN = 'Y'
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)