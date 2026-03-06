# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/01/07 15:16:03 Batch  14550_54982 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_2 11/01/07 15:05:36 Batch  14550_54348 INIT bckcett testIDS30 dsadm rc for brent 
# MAGIC ^1_2 10/31/07 10:30:33 Batch  14549_37838 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/31/07 10:24:16 Batch  14549_37464 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/24/07 15:30:15 Batch  14542_55818 INIT bckcett devlIDS30 u10157 sa - DRG project - moving to ids_current devlopment for coding changes
# MAGIC ^1_1 10/10/07 07:31:23 Batch  14528_27086 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     IdsClmBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_TO_ROW_BAL_IN and CLMN_SUM_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               09/15/2007          3264                              Originally Programmed                           devlIDS30

# MAGIC Update the P_RUN_CYC table with Balancing Indicators set to 'Y' to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Extract P_RUN_CYC records for IDS Income with a value \"N\" in the  ROW_TO_ROW_BAL_IN and CLMN_SUM_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
BeginCycle = get_widget_value('BeginCycle','')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = (
    f"SELECT SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD, MAX(RUN_CYC_NO) AS RUN_CYC_NO "
    f"FROM {IDSOwner}.P_RUN_CYC "
    f"WHERE TRGT_SYS_CD = '{TrgtSys}' "
    f"AND SRC_SYS_CD = '{SrcSys}' "
    f"AND SUBJ_CD = '{SubjCd}' "
    f"AND ROW_TO_ROW_BAL_IN = 'N' "
    f"AND CLMN_SUM_BAL_IN = 'N' "
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

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.BalClmDmClmRunCycUpd_P_RUN_CYC_temp",
    jdbc_url,
    jdbc_props
)

df_BusinessRules.write.jdbc(
    url=jdbc_url,
    table="STAGING.BalClmDmClmRunCycUpd_P_RUN_CYC_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = (
    f"MERGE {IDSOwner}.P_RUN_CYC AS T "
    f"USING STAGING.BalClmDmClmRunCycUpd_P_RUN_CYC_temp AS S "
    f"ON T.SRC_SYS_CD=S.SRC_SYS_CD "
    f"AND T.SUBJ_CD=S.SUBJ_CD "
    f"AND T.TRGT_SYS_CD=S.TRGT_SYS_CD "
    f"AND T.RUN_CYC_NO <= S.RUN_CYC_NO "
    f"AND T.ROW_TO_ROW_BAL_IN='N' "
    f"AND T.CLMN_SUM_BAL_IN='N' "
    f"WHEN MATCHED THEN "
    f"UPDATE SET T.ROW_TO_ROW_BAL_IN='Y', T.CLMN_SUM_BAL_IN='Y';"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)