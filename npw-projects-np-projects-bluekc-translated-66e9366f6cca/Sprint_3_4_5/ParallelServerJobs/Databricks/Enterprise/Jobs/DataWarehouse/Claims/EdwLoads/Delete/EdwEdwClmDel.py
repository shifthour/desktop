# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClmDel
# MAGIC 
# MAGIC DESCRIPTION:  Delete records from Claim tables based on records extracted from IDS.
# MAGIC 
# MAGIC INPUTS:  W_CLM_DEL
# MAGIC                  SYSDUMMY1
# MAGIC   
# MAGIC HASH FILES: None
# MAGIC 
# MAGIC TRANSFORMS:   None
# MAGIC                   
# MAGIC                            
# MAGIC PROCESSING:  Use the SYSDUMMY1 table to initiate the delete statements.  Each delete statement looks for source and claim ID where the run cycle is older than the current run cycle.  These rows were not update with the current process so they don't exist in the source system anymore.
# MAGIC                   
# MAGIC  
# MAGIC OUTPUTS:  Remove source deleted rows from:
# MAGIC                                                                                      CLM_LN_PROC_CD_MOD_D
# MAGIC                                                                                      FCLTY_CLM_PROC_I
# MAGIC                                                                                      CLM_LN_COB_F
# MAGIC                                                                                      CLM_COB_F
# MAGIC                     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Brent Leland   03/21/2006 - Originally programmed.
# MAGIC               Brent Leland   08/01/2006 - Split job into two jobs.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Name               Date                 Description                                                   Project#        Environment                         Code Reviewer                  Date Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC 
# MAGIC               Pooja Sunkara   10/10/2013   Rewrite in Parallel                                              5114          EnterpriseWrhsDevl         Peter Marshall                      12/24/2013

# MAGIC EdwEdwClmDel
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunCycle = get_widget_value('RunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Stage: db2_FCLTY_CLM_PROC_I_in
df_db2_FCLTY_CLM_PROC_I_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT 
FCLTY_CLM_PROC_SK
FROM {EDWOwner}.FCLTY_CLM_PROC_I c,
     {EDWOwner}.W_CLM_DEL d
WHERE 
c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
AND d.SRC_SYS_CD = c.SRC_SYS_CD
AND d.CLM_ID = c.CLM_ID"""
    )
    .load()
)

# Stage: Cpy_FCLTY_CLM_PROC_SK
df_Cpy_FCLTY_CLM_PROC_SK = df_db2_FCLTY_CLM_PROC_I_in.select("FCLTY_CLM_PROC_SK")

# Stage: db2_FCLTY_CLM_PROC_I_Out
df_db2_FCLTY_CLM_PROC_I_Out_temp = df_Cpy_FCLTY_CLM_PROC_SK.select("FCLTY_CLM_PROC_SK")
execute_dml("DROP TABLE IF EXISTS STAGING.EdwEdwClmDel_db2_FCLTY_CLM_PROC_I_Out_temp", jdbc_url, jdbc_props)
df_db2_FCLTY_CLM_PROC_I_Out_temp.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="STAGING.EdwEdwClmDel_db2_FCLTY_CLM_PROC_I_Out_temp",
    properties=jdbc_props
)
merge_sql = f"""MERGE {EDWOwner}.FCLTY_CLM_PROC_I AS T
USING STAGING.EdwEdwClmDel_db2_FCLTY_CLM_PROC_I_Out_temp AS S
ON T.FCLTY_CLM_PROC_SK = S.FCLTY_CLM_PROC_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_COB_F_in
df_db2_CLM_LN_COB_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT 
CLM_LN_COB_SK
FROM 
{EDWOwner}.CLM_LN_COB_F c,
{EDWOwner}.W_CLM_DEL d
WHERE 
c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
AND d.SRC_SYS_CD = c.SRC_SYS_CD
AND d.CLM_ID = c.CLM_ID"""
    )
    .load()
)

# Stage: Cpy_CLM_LN_COB_SK
df_Cpy_CLM_LN_COB_SK = df_db2_CLM_LN_COB_F_in.select("CLM_LN_COB_SK")

# Stage: db2_CLM_LN_COB_F_Out
df_db2_CLM_LN_COB_F_Out_temp = df_Cpy_CLM_LN_COB_SK.select("CLM_LN_COB_SK")
execute_dml("DROP TABLE IF EXISTS STAGING.EdwEdwClmDel_db2_CLM_LN_COB_F_Out_temp", jdbc_url, jdbc_props)
df_db2_CLM_LN_COB_F_Out_temp.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="STAGING.EdwEdwClmDel_db2_CLM_LN_COB_F_Out_temp",
    properties=jdbc_props
)
merge_sql = f"""MERGE {EDWOwner}.CLM_LN_COB_F AS T
USING STAGING.EdwEdwClmDel_db2_CLM_LN_COB_F_Out_temp AS S
ON T.CLM_LN_COB_SK = S.CLM_LN_COB_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_PROC_CD_MOD_D_in
df_db2_CLM_LN_PROC_CD_MOD_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT 
CLM_LN_PROC_CD_MOD_SK
FROM {EDWOwner}.CLM_LN_PROC_CD_MOD_D c,
     {EDWOwner}.W_CLM_DEL d
WHERE 
c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
AND d.SRC_SYS_CD = c.SRC_SYS_CD
AND d.CLM_ID = c.CLM_ID"""
    )
    .load()
)

# Stage: Cpy_CLM_LN_PROC_CD_MOD_SK
df_Cpy_CLM_LN_PROC_CD_MOD_SK = df_db2_CLM_LN_PROC_CD_MOD_D_in.select("CLM_LN_PROC_CD_MOD_SK")

# Stage: db2_CLM_LN_PROC_CD_MOD_D_Out
df_db2_CLM_LN_PROC_CD_MOD_D_Out_temp = df_Cpy_CLM_LN_PROC_CD_MOD_SK.select("CLM_LN_PROC_CD_MOD_SK")
execute_dml("DROP TABLE IF EXISTS STAGING.EdwEdwClmDel_db2_CLM_LN_PROC_CD_MOD_D_Out_temp", jdbc_url, jdbc_props)
df_db2_CLM_LN_PROC_CD_MOD_D_Out_temp.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="STAGING.EdwEdwClmDel_db2_CLM_LN_PROC_CD_MOD_D_Out_temp",
    properties=jdbc_props
)
merge_sql = f"""MERGE {EDWOwner}.CLM_LN_PROC_CD_MOD_D AS T
USING STAGING.EdwEdwClmDel_db2_CLM_LN_PROC_CD_MOD_D_Out_temp AS S
ON T.CLM_LN_PROC_CD_MOD_SK = S.CLM_LN_PROC_CD_MOD_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Stage: db2_CLM_COB_F_in
df_db2_CLM_COB_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT 
CLM_COB_SK
FROM {EDWOwner}.CLM_COB_F c,
     {EDWOwner}.W_CLM_DEL d
WHERE 
c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
AND d.SRC_SYS_CD = c.SRC_SYS_CD
AND d.CLM_ID = c.CLM_ID"""
    )
    .load()
)

# Stage: Cpy_CLM_COB_SK
df_Cpy_CLM_COB_SK = df_db2_CLM_COB_F_in.select("CLM_COB_SK")

# Stage: db2_CLM_COB_F_Out
df_db2_CLM_COB_F_Out_temp = df_Cpy_CLM_COB_SK.select("CLM_COB_SK")
execute_dml("DROP TABLE IF EXISTS STAGING.EdwEdwClmDel_db2_CLM_COB_F_Out_temp", jdbc_url, jdbc_props)
df_db2_CLM_COB_F_Out_temp.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="STAGING.EdwEdwClmDel_db2_CLM_COB_F_Out_temp",
    properties=jdbc_props
)
merge_sql = f"""MERGE {EDWOwner}.CLM_COB_F AS T
USING STAGING.EdwEdwClmDel_db2_CLM_COB_F_Out_temp AS S
ON T.CLM_COB_SK = S.CLM_COB_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)