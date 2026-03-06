# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
# MAGIC 
# MAGIC CALLED BY: PreReqIdsRcvdIncmSbsdyDtlKtableCntl
# MAGIC 
# MAGIC PROCESSING: This job runs everyday - IDS MA Received Income Subsidy Detail Primary Key base table load to K table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =======================================================================================================================================================
# MAGIC Developer	Date		Project/Altiris #	Change Description				Development Project Code Reviewer	Date Reviewed       
# MAGIC =======================================================================================================================================================
# MAGIC Abhiram Dasarathy	2021-03-26	US-358377	Initial Programming				IntegrateDev2	Jaideep Mankala        03/29/2021

# MAGIC IDS Received Income Subsidy Detail Primary Key hash file load to K tables
# MAGIC This job loads K tables from hash files before Facets Provider Control runs. 
# MAGIC 
# MAGIC Since RCVD_INCM_SBSDY_DTL are loaded from multiple source systems; we will need to SYNCH data between Hash file and K table. This job will need to be run until we convert all the Server jobs that are using hash files for above 3 tables.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = """SELECT c.SRC_CD as SRC_SYS_CD,
 m.CMS_ENR_PAYMT_UNIQ_KEY,
 m.CMS_ENR_PAYMT_AMT_SEQ_NO,
 m.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD,
 m.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD,
 m.CRT_RUN_CYC_EXCTN_SK,
 m.RCVD_INCM_SBSDY_DTL_SK
FROM #$IDSOwner#.RCVD_INCM_SBSDY_DTL m,
     #$IDSOwner#.CD_MPPNG c
WHERE m.SRC_SYS_CD_SK = c.CD_MPPNG_SK
"""

df_DB2_RCVD_INCM_SBSDY_DTL_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Xfrm = (
    df_DB2_RCVD_INCM_SBSDY_DTL_In.filter(
        (F.col("RCVD_INCM_SBSDY_DTL_SK") != 0)
        & (F.col("RCVD_INCM_SBSDY_DTL_SK") != 1)
    )
    .select(
        F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
        F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK")
    )
)

df_final = df_Xfrm.select(
    F.rpad(F.col("CMS_ENR_PAYMT_UNIQ_KEY"), <...>, " ").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.rpad(F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"), <...>, " ").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.rpad(F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"), <...>, " ").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK")
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.PreReqIdsRcvdIncmSbsdyDtlKTableLoad_DB2_K_RCVD_INCM_SBSDY_DTL_Out_temp",
    jdbc_url,
    jdbc_props
)

df_final.write.jdbc(
    url=jdbc_url,
    table="STAGING.PreReqIdsRcvdIncmSbsdyDtlKTableLoad_DB2_K_RCVD_INCM_SBSDY_DTL_Out_temp",
    mode="append",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO #$IDSOwner#.K_RCVD_INCM_SBSDY_DTL AS T
USING STAGING.PreReqIdsRcvdIncmSbsdyDtlKTableLoad_DB2_K_RCVD_INCM_SBSDY_DTL_Out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.RCVD_INCM_SBSDY_DTL_SK = S.RCVD_INCM_SBSDY_DTL_SK
WHEN MATCHED THEN
  UPDATE SET
    T.CMS_ENR_PAYMT_UNIQ_KEY = S.CMS_ENR_PAYMT_UNIQ_KEY,
    T.CMS_ENR_PAYMT_AMT_SEQ_NO = S.CMS_ENR_PAYMT_AMT_SEQ_NO,
    T.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD = S.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD,
    T.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD = S.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    CMS_ENR_PAYMT_UNIQ_KEY,
    CMS_ENR_PAYMT_AMT_SEQ_NO,
    RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD,
    RCVD_INCM_SBSDY_ACCT_ACTVTY_CD,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    RCVD_INCM_SBSDY_DTL_SK
  )
  VALUES (
    S.CMS_ENR_PAYMT_UNIQ_KEY,
    S.CMS_ENR_PAYMT_AMT_SEQ_NO,
    S.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD,
    S.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.RCVD_INCM_SBSDY_DTL_SK
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)