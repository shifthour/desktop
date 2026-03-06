# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:   Deletes any old Bill Invoice records that were processed last month and now that Member Recast has been re-ran, will generated different Natural keys to the Income Fact tables.
# MAGIC                              This is necessary to keep the amounts in balance.
# MAGIC MODIFICATIONS:
# MAGIC Developer          Date                Project/Altiris #                                      Change Description                                               Develop Project              Code Reviewer    Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------                                     -----------------------------------------------------------------------       ---------------------------------       --------------------------   -------------------------       
# MAGIC Sandrew           2007-09-20        eproject #5137 Project Release 8.1       initail programming  
# MAGIC 
# MAGIC Bhupinder Kaur  2013-10-08     5114                                                     Rewrite in Parallel                                                    EnterpriseWrhsDevl        Peter Marshall       12/23/2013

# MAGIC Delete the data from DSCRTN_INCM_F  table.
# MAGIC JobName:
# MAGIC EdwEdwIncomeDeleteErnIncmInvc
# MAGIC Delete Old Billing INvoice data
# MAGIC Data is extracted from the DSCRTN_INCM_F Table
# MAGIC buffer
# MAGIC Deletes Billing Invoice Records processed thru the allocation process this job that may have already been loaded in last months process.
# MAGIC 
# MAGIC Sometimes new natural keys are built and therefore need to delete the old records so the billing invoice will balance back to IDS, and back to Facets.
# MAGIC Data is extracted from the FEE_DSCNT_INCM_F  Table
# MAGIC Delete the data from FEE_DSCNT_INCM_F  table.
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

EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_db2_DSCRTN_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
SRC_SYS_CD,
BILL_INVC_ID
FROM {EDWOwner}.DSCRTN_INCM_F
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK = {EDWRunCycle}"""
    )
    .load()
)

df_cpy_BusinessLogic = df_db2_DSCRTN_INCM_F_in.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID")
)

execute_dml("DROP TABLE IF EXISTS STAGING.EdwEdwIncomeDeleteInvcLoad_db2_DSCRTN_INCM_F_Out_temp", jdbc_url, jdbc_props)
df_cpy_BusinessLogic.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwIncomeDeleteInvcLoad_db2_DSCRTN_INCM_F_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {EDWOwner}.DSCRTN_INCM_F AS T
USING STAGING.EdwEdwIncomeDeleteInvcLoad_db2_DSCRTN_INCM_F_Out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.BILL_INVC_ID = S.BILL_INVC_ID
WHEN MATCHED AND T.LAST_UPDT_RUN_CYC_EXCTN_SK <> {EDWRunCycle} THEN DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_db2_FEE_DSCNT_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
SRC_SYS_CD,
BILL_INVC_ID
FROM {EDWOwner}.FEE_DSCNT_INCM_F
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK = {EDWRunCycle}"""
    )
    .load()
)

df_cpy_buffer = df_db2_FEE_DSCNT_INCM_F_in.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID")
)

execute_dml("DROP TABLE IF EXISTS STAGING.EdwEdwIncomeDeleteInvcLoad_db2_FEE_DSCNT_INCM_F_Out_temp", jdbc_url, jdbc_props)
df_cpy_buffer.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwIncomeDeleteInvcLoad_db2_FEE_DSCNT_INCM_F_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {EDWOwner}.FEE_DSCNT_INCM_F AS T
USING STAGING.EdwEdwIncomeDeleteInvcLoad_db2_FEE_DSCNT_INCM_F_Out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.BILL_INVC_ID = S.BILL_INVC_ID
WHEN MATCHED AND T.LAST_UPDT_RUN_CYC_EXCTN_SK <> {EDWRunCycle} THEN DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)