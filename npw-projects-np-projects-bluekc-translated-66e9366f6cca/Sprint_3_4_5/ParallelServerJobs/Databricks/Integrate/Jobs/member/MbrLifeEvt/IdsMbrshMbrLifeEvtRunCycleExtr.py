# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:   IdsMbrshMbrLifeEvtCtrlSeq
# MAGIC 
# MAGIC PROCESSING:   Extracts max RunCycle from the CLM table to be used for extracting data for Member Life Event
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                      Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                   --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Jagadesh Yelavarthi      2010-12-20        Alineo                       Initial ETL                                                                                    IntegrateNewDevl          Steph Goddard           01/06/2011

# MAGIC IdsMbrshMbrLifeEvtRunCycleExt - Extracts the max RunCycle from the Claim table. This runcycle numbers will be used by IdsMbrshMbrLifeEvtExtr job the next time it runs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycExtracted = get_widget_value('IDSRunCycExtracted','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_CLAIM = f"SELECT MAX(CLM.LAST_UPDT_RUN_CYC_EXCTN_SK) As RUN_CYC_NO FROM {IDSOwner}.CLM CLM, {IDSOwner}.CD_MPPNG MAP WHERE CLM.SRC_SYS_CD_SK=MAP.CD_MPPNG_SK AND MAP.TRGT_CD='FACETS' AND CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycExtracted}"

df_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CLAIM)
    .load()
)

df_Tr_Claim = df_CLAIM.select(
    lit("IdsMemberLifeEvtRunCycle").alias("JOBNAME"),
    col("RUN_CYC_NO").alias("BEGINDATE"),
    col("RUN_CYC_NO").alias("PREVBEGINDATE")
)

df_Tr_Claim_s = df_Tr_Claim.select(
    rpad(col("JOBNAME"), <...>, " ").alias("JOBNAME"),
    rpad(col("BEGINDATE"), <...>, " ").alias("BEGINDATE"),
    rpad(col("PREVBEGINDATE"), <...>, " ").alias("PREVBEGINDATE")
)

jdbc_url_cuv, jdbc_props_cuv = get_db_config(<...>)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsMbrshMbrLifeEvtRunCycleExtr_Updt_Load_Dates_CLM_temp", jdbc_url_cuv, jdbc_props_cuv)

df_Tr_Claim_s.write.format("jdbc") \
    .option("url", jdbc_url_cuv) \
    .options(**jdbc_props_cuv) \
    .option("dbtable", "STAGING.IdsMbrshMbrLifeEvtRunCycleExtr_Updt_Load_Dates_CLM_temp") \
    .mode("overwrite") \
    .save()

merge_sql_Updt_Load_Dates_CLM = (
    "MERGE LOAD_DATES as T "
    "USING STAGING.IdsMbrshMbrLifeEvtRunCycleExtr_Updt_Load_Dates_CLM_temp as S "
    "ON T.JOBNAME = S.JOBNAME "
    "WHEN MATCHED THEN UPDATE SET "
    "T.BEGINDATE = S.BEGINDATE, T.PREVBEGINDATE = S.PREVBEGINDATE "
    "WHEN NOT MATCHED THEN INSERT (JOBNAME, BEGINDATE, PREVBEGINDATE) "
    "VALUES(S.JOBNAME, S.BEGINDATE, S.PREVBEGINDATE);"
)

execute_dml(merge_sql_Updt_Load_Dates_CLM, jdbc_url_cuv, jdbc_props_cuv)