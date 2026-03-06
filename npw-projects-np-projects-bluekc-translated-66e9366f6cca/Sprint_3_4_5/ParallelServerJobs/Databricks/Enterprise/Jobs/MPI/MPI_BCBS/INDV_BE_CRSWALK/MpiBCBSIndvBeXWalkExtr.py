# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: TBD
# MAGIC 
# MAGIC CALLED BY:  TBD
# MAGIC 
# MAGIC PROCESSING:  This is a daily process. These records are written to the MPI BCBS Extension Database from the MPI tool and provides any existing BE_KEY's that have received a new BE_KEY.
# MAGIC 
# MAGIC DEPENDENCIES: MPI Tool process complete and files outputted. MPI BCBS Extension ---> MPI BCBS Extension : Membership : MPI BCBS MBR_OUTP job completed.
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #      Change Description                                                                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   ------------------------------   ----------------------------------------------------------------------------------------------------------------------         ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Abhiram Dasarathy            08-29-2012      4426 - MPI           Original Programming                                                                                                 EnterpriseWrhsDevl   Bhoomi Dasari              09/18/2012
# MAGIC SAndrew                           2012-10-10        4426 MPI           Added CurDtm as extract criteria to avoid old MBR_OUTP with bekeys <> to not    EnterpriseWrhsDevl   Bhoomi Dasari              10/14/2012 
# MAGIC                                                                                              get processed over and over
# MAGIC Abhiram Dasarathy	         2012-11-30	4426 - MPI	Added SQL Query in the MBR_OUTP table to not include MPI_MBR_ID where	EnterpriseNewDevl    Bhoomi Dasari              11/30/2012
# MAGIC 						MPI_NTFCTN_IN = 'Y' AND SSN = '000000000' AND BRTH_DT = '1753-01-01'
# MAGIC 						AND OUTP.MPI_SUB_ID = 0
# MAGIC 
# MAGIC SAndrew                     2015-08-17                5318                     changed CurrDtm to CurrDtmSybTS to keep in synch with all other code changes        Enterprise Dev1  Kalyan Neelam            2015-09-22
# MAGIC 			                                          removed constaint with the transform BeKeyChgs.ASG_INDV_BE_KEY <> BeKeyChgs.PREV_INDV_BE_KEY
# MAGIC                 			                                          because the same rule is within the MBR_OUTP SQL
# MAGIC 						Made it not a Multiple Instance

# MAGIC Crosswalk transformation rules
# MAGIC MPI BCBS EXTENSION --> MPI BCBS EXTENSION
# MAGIC DataStage Extract from MPI MBR_OUTP Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


MPI_BCBSOwner = get_widget_value('MPI_BCBSOwner','')
mpibcbs_secret_name = get_widget_value('mpibcbs_secret_name','')
CurrDtmSybTS = get_widget_value('CurrDtmSybTS','')

jdbc_url, jdbc_props = get_db_config(mpibcbs_secret_name)

extract_query = f"""
SELECT
  MBR_OUTP.MPI_MBR_ID,
  MBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK,
  MBR_OUTP.PRCS_RUN_DTM,
  MBR_OUTP.PREV_INDV_BE_KEY,
  MBR_OUTP.ASG_INDV_BE_KEY
FROM {MPI_BCBSOwner}.MBR_OUTP MBR_OUTP
WHERE MBR_OUTP.PREV_INDV_BE_KEY <> MBR_OUTP.ASG_INDV_BE_KEY
  AND MBR_OUTP.PRCS_RUN_DTM = '{CurrDtmSybTS}'
  AND MBR_OUTP.MPI_MBR_ID NOT IN (
    SELECT
      MPI_MBR_ID
    FROM {MPI_BCBSOwner}.MBR_OUTP AS OUTP
    WHERE OUTP.MPI_NTFCTN_IN = 'Y'
      AND OUTP.SSN = '000000000'
      AND OUTP.BRTH_DT = '1753-01-01'
      AND OUTP.MPI_SUB_ID = 0
  )
"""

df_MBR_OUTP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BeKeyChgs = df_MBR_OUTP

df_IndvBeCrswalk = (
    df_BeKeyChgs
    .withColumn("MPI_MBR_ID", trim(col("MPI_MBR_ID")))
    .withColumn("PRCS_OWNER_SRC_SYS_ENVRN_CK", col("PRCS_OWNER_SRC_SYS_ENVRN_CK"))
    .withColumn("PRCS_RUN_DTM", col("PRCS_RUN_DTM"))
    .withColumn("PREV_INDV_BE_KEY", trim(col("PREV_INDV_BE_KEY")))
    .withColumn("NEW_INDV_BE_KEY", trim(col("ASG_INDV_BE_KEY")))
    .drop("ASG_INDV_BE_KEY")
    .select(
        "MPI_MBR_ID",
        "PRCS_OWNER_SRC_SYS_ENVRN_CK",
        "PRCS_RUN_DTM",
        "PREV_INDV_BE_KEY",
        "NEW_INDV_BE_KEY"
    )
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.MpiBCBSIndvBeXWalkExtr_INDV_BE_CRSWALK_temp",
    jdbc_url,
    jdbc_props
)

df_IndvBeCrswalk.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.MpiBCBSIndvBeXWalkExtr_INDV_BE_CRSWALK_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {MPI_BCBSOwner}.INDV_BE_CRSWALK AS T
USING
(
  SELECT 
    MPI_MBR_ID,
    PRCS_OWNER_SRC_SYS_ENVRN_CK,
    PRCS_RUN_DTM,
    PREV_INDV_BE_KEY,
    NEW_INDV_BE_KEY
  FROM STAGING.MpiBCBSIndvBeXWalkExtr_INDV_BE_CRSWALK_temp
) AS S
ON
  T.MPI_MBR_ID = S.MPI_MBR_ID
  AND T.PRCS_OWNER_SRC_SYS_ENVRN_CK = S.PRCS_OWNER_SRC_SYS_ENVRN_CK
  AND T.PRCS_RUN_DTM = S.PRCS_RUN_DTM
WHEN MATCHED THEN
  UPDATE SET
    T.PREV_INDV_BE_KEY = S.PREV_INDV_BE_KEY,
    T.NEW_INDV_BE_KEY = S.NEW_INDV_BE_KEY
WHEN NOT MATCHED THEN
  INSERT
    (MPI_MBR_ID, PRCS_OWNER_SRC_SYS_ENVRN_CK, PRCS_RUN_DTM, PREV_INDV_BE_KEY, NEW_INDV_BE_KEY)
  VALUES
    (S.MPI_MBR_ID, S.PRCS_OWNER_SRC_SYS_ENVRN_CK, S.PRCS_RUN_DTM, S.PREV_INDV_BE_KEY, S.NEW_INDV_BE_KEY)
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)