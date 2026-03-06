# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:    EdwLoadRunCycUpd_EE
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table EDW load indicator fields to show those records have been copied to the EDW.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	IDS - W_EDW_ETL_DRVR
# MAGIC                
# MAGIC 
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    IDS    - P_RUN_CYC
# MAGIC                   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description                                                                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------    -----------------------------------   -------------------------          ----------------------------
# MAGIC 2006-03-21      Brent Leland            Original Programming.
# MAGIC 2006-07-26      Steph Goddard        Changed OT@2, ADOL, EDC to PSEUDOCLAIM source
# MAGIC 2010-07-23      Kalyan Neelam        Added new sources IHM & Alineo                                                           EnterpriseNewDevl     
# MAGIC 
# MAGIC 
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               11/08/2013        5114                              Update the P_RUN_CYC table EDW load indicator fields                     EnterpriseWhseDevl
# MAGIC 
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Santosh Bokka          01/03/2014        4917                          Added Treo  Source                                                                            EnterpriseNewDevl       Bhoomi Dasari              1/17/2014
# MAGIC Santosh Bokka          01/16/2014        4917                          Added BCBSKC  Source and Updated 
# MAGIC                                                                                                RUN_CYC_NO <= ORCHESTRATE.RUN_CYC_NO                          EnterpriseNewDevl      Bhoomi Dasari              1/17/2014
# MAGIC Krishnakanth              10/13/2016        30001                       Added COBALTTALON  Source 
# MAGIC       Manivannan                                                                      RUN_CYC_NO <= ORCHESTRATE.RUN_CYC_NO                          EnterpriseDev2            Jag Yelavarthi               2016-11-16
# MAGIC 
# MAGIC Reddy Sanam     2023-03-09    US574919    In the stage "db2_COBALTTALON_MbrRisk_in", "COBALTTALON"                          EnterpriseDev2           Goutham Kalidindi         3/10/2023
# MAGIC                                                                        is updated to MEDINSIGHTS from COBALTALLON

# MAGIC Get list of source systems and the maximum run cycles for the source system from the driver table.
# MAGIC Job Name: EdwMbrRiskLoadRunCycUpd_EE
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, expr, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_IDEA_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'IDEA'
GROUP BY SRC_SYS_CD
"""
df_db2_IDEA_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_IDEA_MbrRisk_in)
    .load()
)

extract_query_db2_Alineo_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'ALINEO'
GROUP BY SRC_SYS_CD
"""
df_db2_Alineo_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Alineo_MbrRisk_in)
    .load()
)

extract_query_db2_IHM_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'IHMANALYTICS'
GROUP BY SRC_SYS_CD
"""
df_db2_IHM_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_IHM_MbrRisk_in)
    .load()
)

extract_query_db2_McSource_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'MCSOURCE'
GROUP BY SRC_SYS_CD
"""
df_db2_McSource_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_McSource_MbrRisk_in)
    .load()
)

extract_query_db2_ImpPro_MbrRisk_in = f"""
SELECT 'IMPACTPRO' AS SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'IMP'
GROUP BY SRC_SYS_CD
"""
df_db2_ImpPro_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ImpPro_MbrRisk_in)
    .load()
)

extract_query_db2_TREO_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'TREO'
GROUP BY SRC_SYS_CD
"""
df_db2_TREO_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_TREO_MbrRisk_in)
    .load()
)

extract_query_db2_BCBSKC_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'BCBSKC'
GROUP BY SRC_SYS_CD
"""
df_db2_BCBSKC_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BCBSKC_MbrRisk_in)
    .load()
)

extract_query_db2_COBALTTALON_MbrRisk_in = f"""
SELECT SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_MBR_RISK_DRVR
WHERE SRC_SYS_CD = 'MEDINSIGHTS'
GROUP BY SRC_SYS_CD
"""
df_db2_COBALTTALON_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_COBALTTALON_MbrRisk_in)
    .load()
)

df_fnl_Drvr = (
    df_db2_IDEA_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK")
    .unionByName(df_db2_Alineo_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_db2_IHM_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_db2_McSource_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_db2_ImpPro_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_db2_TREO_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_db2_BCBSKC_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_db2_COBALTTALON_MbrRisk_in.select("SRC_SYS_CD", "LAST_UPDT_RUN_CYC_EXCTN_SK"))
)

df_xfm_BusinessLogic = df_fnl_Drvr.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    expr("'MEMBER RISK'").alias("SUBJ_CD"),
    expr("'IDS'").alias("TRGT_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("RUN_CYC_NO")
).withColumn(
    "EDW_LOAD_IN", rpad(expr("'Y'"), 1, " ")
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwMbrRiskLoadRunCycUpd_EE_db2_P_RUN_CYC_in_temp",
    jdbc_url,
    jdbc_props
)

df_xfm_BusinessLogic.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwMbrRiskLoadRunCycUpd_EE_db2_P_RUN_CYC_in_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.EdwMbrRiskLoadRunCycUpd_EE_db2_P_RUN_CYC_in_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO <= S.RUN_CYC_NO
WHEN MATCHED THEN
  UPDATE SET
    T.EDW_LOAD_IN = S.EDW_LOAD_IN
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, EDW_LOAD_IN)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, S.EDW_LOAD_IN);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)