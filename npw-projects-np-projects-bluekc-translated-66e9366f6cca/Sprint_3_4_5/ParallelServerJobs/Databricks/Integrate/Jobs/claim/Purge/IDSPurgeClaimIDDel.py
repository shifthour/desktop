# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_1 03/02/09 10:47:54 Batch  15037_38896 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 03/02/09 10:41:44 Batch  15037_38516 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 02/13/09 15:51:39 Batch  15020_57105 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 02/13/09 15:37:03 Batch  15020_56227 INIT bckcett devlIDS u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IDSPurgeClaimCntl
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING: Select claims from the W_CLM_PURGE table and delete them from the table defined in the input parameter.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2008-08-26       IAD Prod. Sup.      Original Programming.                                                                   devlIDS                          Steph Goddard          01/09/2009

# MAGIC Purge claims from any IDS Claim table
# MAGIC Select source and claim ID from driver table
# MAGIC Delete from claims from table passed in as parameter
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClaimTable = get_widget_value('ClaimTable','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_W_CLM_PURGE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD_SK as SRC_SYS_CD_SK, CLM_ID as CLM_ID FROM {IDSOwner}.W_CLM_PURGE")
    .load()
)

execute_dml("DROP TABLE IF EXISTS STAGING.IDSPurgeClaimIDDel_CLAIM_TABLE_temp", jdbc_url, jdbc_props)

(
    df_W_CLM_PURGE.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IDSPurgeClaimIDDel_CLAIM_TABLE_temp")
    .mode("overwrite")
    .save()
)

delete_stmt = f"""
DELETE T
FROM {IDSOwner}.{ClaimTable} T
JOIN STAGING.IDSPurgeClaimIDDel_CLAIM_TABLE_temp S 
  ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
  AND T.CLM_ID = S.CLM_ID
"""
execute_dml(delete_stmt, jdbc_url, jdbc_props)