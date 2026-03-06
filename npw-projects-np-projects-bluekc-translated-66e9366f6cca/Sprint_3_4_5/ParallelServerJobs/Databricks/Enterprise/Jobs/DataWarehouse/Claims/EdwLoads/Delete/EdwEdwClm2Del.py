# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClm2Del
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
# MAGIC                                                                                      CLM_ALT_PAYE_D
# MAGIC                                                                                      CLM_LN_F
# MAGIC                                                                                      CLM_LN_DIAG_I
# MAGIC                                                                                      CLM_LN_DSALW_F
# MAGIC                                                                                      CLM_DIAG_I
# MAGIC                  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Brent Leland   03/21/2006 - Originally programmed.
# MAGIC               Brent Leland   08/01/2006 - Added delete for CLM_DIAG_I
# MAGIC                                                          - Split job into two jobs.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------     -----------------------------------    ---------------------------------------------------------                          ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               03/27/2008      3255 --  TTR281         Added Delete processes for CLM_OVRD_D,              devlEDWcur                 Steph Goddard            03/30/2008
# MAGIC                                                                                                       PCA_CLM_F,PCA_CLM_LN_F,CLM_LN_OVRD_D 
# MAGIC                                                                                                       and CLM_LN_CLNCL_EDIT_D
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara                  10-15-2013          5114                        Rewrite in Parallel                                                      EnterpriseWrhsDevl        Peter Marshall             12/24/2013

# MAGIC EdwEdwClmDel
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunCycle = get_widget_value('RunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Stage: db2_CLM_ALT_PAYE_D_in
extract_query_db2_CLM_ALT_PAYE_D_in = f"SELECT CLM_ALT_PAYE_SK FROM {EDWOwner}.CLM_ALT_PAYE_D c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_ALT_PAYE_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_ALT_PAYE_D_in)
    .load()
)

# Stage: Cpy_CLM_ALT_PAYE_SK
df_Cpy_CLM_ALT_PAYE_SK = df_db2_CLM_ALT_PAYE_D_in.select(
    F.col("CLM_ALT_PAYE_SK").alias("CLM_ALT_PAYE_SK")
)

# Stage: db2_CLM_ALT_PAYE_D_Out
drop_temp_sql_db2_CLM_ALT_PAYE_D_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_ALT_PAYE_D_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_ALT_PAYE_D_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_ALT_PAYE_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_ALT_PAYE_D_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_ALT_PAYE_D_Out = f"""
MERGE {EDWOwner}.CLM_ALT_PAYE_D as c
USING STAGING.EdwEdwClm2Del_db2_CLM_ALT_PAYE_D_Out_temp as s
ON c.CLM_ALT_PAYE_SK = s.CLM_ALT_PAYE_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_ALT_PAYE_D_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_DIAG_I_in
extract_query_db2_CLM_DIAG_I_in = f"SELECT CLM_DIAG_SK FROM {EDWOwner}.CLM_DIAG_I c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_DIAG_I_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_DIAG_I_in)
    .load()
)

# Stage: Cpy_CLM_DIAG_SK
df_Cpy_CLM_DIAG_SK = df_db2_CLM_DIAG_I_in.select(
    F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK")
)

# Stage: db2_CLM_DIAG_I_Out
drop_temp_sql_db2_CLM_DIAG_I_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_DIAG_I_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_DIAG_I_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_DIAG_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_DIAG_I_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_DIAG_I_Out = f"""
MERGE {EDWOwner}.CLM_DIAG_I as c
USING STAGING.EdwEdwClm2Del_db2_CLM_DIAG_I_Out_temp as s
ON c.CLM_DIAG_SK = s.CLM_DIAG_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_DIAG_I_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_F_in
extract_query_db2_CLM_LN_F_in = f"SELECT CLM_LN_SK FROM {EDWOwner}.CLM_LN_F c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_LN_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_F_in)
    .load()
)

# Stage: Cpy_CLM_LN_SK
df_Cpy_CLM_LN_SK = df_db2_CLM_LN_F_in.select(
    F.col("CLM_LN_SK").alias("CLM_LN_SK")
)

# Stage: db2_CLM_LN_F_Out
drop_temp_sql_db2_CLM_LN_F_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_LN_F_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_LN_F_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_LN_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_LN_F_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_LN_F_Out = f"""
MERGE {EDWOwner}.CLM_LN_F as c
USING STAGING.EdwEdwClm2Del_db2_CLM_LN_F_Out_temp as s
ON c.CLM_LN_SK = s.CLM_LN_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_LN_F_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_DIAG_I_in
extract_query_db2_CLM_LN_DIAG_I_in = f"SELECT CLM_LN_DIAG_SK FROM {EDWOwner}.CLM_LN_DIAG_I c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_LN_DIAG_I_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_DIAG_I_in)
    .load()
)

# Stage: Cpy_CLM_LN_DIAG_SK
df_Cpy_CLM_LN_DIAG_SK = df_db2_CLM_LN_DIAG_I_in.select(
    F.col("CLM_LN_DIAG_SK").alias("CLM_LN_DIAG_SK")
)

# Stage: db2_CLM_LN_DIAG_I_Out
drop_temp_sql_db2_CLM_LN_DIAG_I_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_LN_DIAG_I_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_LN_DIAG_I_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_LN_DIAG_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_LN_DIAG_I_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_LN_DIAG_I_Out = f"""
MERGE {EDWOwner}.CLM_LN_DIAG_I as c
USING STAGING.EdwEdwClm2Del_db2_CLM_LN_DIAG_I_Out_temp as s
ON c.CLM_LN_DIAG_SK = s.CLM_LN_DIAG_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_LN_DIAG_I_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_DSALW_F_in
extract_query_db2_CLM_LN_DSALW_F_in = f"SELECT CLM_LN_DSALW_SK FROM {EDWOwner}.CLM_LN_DSALW_F c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_LN_DSALW_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_DSALW_F_in)
    .load()
)

# Stage: Cpy_CLM_LN_DSALW_SK
df_Cpy_CLM_LN_DSALW_SK = df_db2_CLM_LN_DSALW_F_in.select(
    F.col("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK")
)

# Stage: db2_CLM_LN_DSALW_F_Out
drop_temp_sql_db2_CLM_LN_DSALW_F_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_LN_DSALW_F_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_LN_DSALW_F_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_LN_DSALW_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_LN_DSALW_F_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_LN_DSALW_F_Out = f"""
MERGE {EDWOwner}.CLM_LN_DSALW_F as c
USING STAGING.EdwEdwClm2Del_db2_CLM_LN_DSALW_F_Out_temp as s
ON c.CLM_LN_DSALW_SK = s.CLM_LN_DSALW_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_LN_DSALW_F_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_CLNCL_EDIT_D_in
extract_query_db2_CLM_LN_CLNCL_EDIT_D_in = f"SELECT CLM_LN_CLNCL_EDIT_SK FROM {EDWOwner}.CLM_LN_CLNCL_EDIT_D c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_LN_CLNCL_EDIT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_CLNCL_EDIT_D_in)
    .load()
)

# Stage: Cpy_CLM_LN_CLNCL_EDIT_SK
df_Cpy_CLM_LN_CLNCL_EDIT_SK = df_db2_CLM_LN_CLNCL_EDIT_D_in.select(
    F.col("CLM_LN_CLNCL_EDIT_SK").alias("CLM_LN_CLNCL_EDIT_SK")
)

# Stage: db2_CLM_LN_CLNCL_EDIT_D_Out
drop_temp_sql_db2_CLM_LN_CLNCL_EDIT_D_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_LN_CLNCL_EDIT_D_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_LN_CLNCL_EDIT_D_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_LN_CLNCL_EDIT_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_LN_CLNCL_EDIT_D_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_LN_CLNCL_EDIT_D_Out = f"""
MERGE {EDWOwner}.CLM_LN_CLNCL_EDIT_D as c
USING STAGING.EdwEdwClm2Del_db2_CLM_LN_CLNCL_EDIT_D_Out_temp as s
ON c.CLM_LN_CLNCL_EDIT_SK = s.CLM_LN_CLNCL_EDIT_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_LN_CLNCL_EDIT_D_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_LN_OVRD_D_in
extract_query_db2_CLM_LN_OVRD_D_in = f"SELECT CLM_LN_OVRD_SK FROM {EDWOwner}.CLM_LN_OVRD_D c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_LN_OVRD_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_OVRD_D_in)
    .load()
)

# Stage: Cpy_CLM_LN_OVRD_SK
df_Cpy_CLM_LN_OVRD_SK = df_db2_CLM_LN_OVRD_D_in.select(
    F.col("CLM_LN_OVRD_SK").alias("CLM_LN_OVRD_SK")
)

# Stage: db2_CLM_LN_OVRD_D_Out
drop_temp_sql_db2_CLM_LN_OVRD_D_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_LN_OVRD_D_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_LN_OVRD_D_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_LN_OVRD_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_LN_OVRD_D_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_LN_OVRD_D_Out = f"""
MERGE {EDWOwner}.CLM_LN_OVRD_D as c
USING STAGING.EdwEdwClm2Del_db2_CLM_LN_OVRD_D_Out_temp as s
ON c.CLM_LN_OVRD_SK = s.CLM_LN_OVRD_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_LN_OVRD_D_Out, jdbc_url, jdbc_props)

# Stage: db2_PCA_CLM_LN_F_in
extract_query_db2_PCA_CLM_LN_F_in = f"SELECT CLM_LN_SK FROM {EDWOwner}.PCA_CLM_LN_F c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_PCA_CLM_LN_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PCA_CLM_LN_F_in)
    .load()
)

# Stage: Cpy_PCA_CLM_LN_SK
df_Cpy_PCA_CLM_LN_SK = df_db2_PCA_CLM_LN_F_in.select(
    F.col("CLM_LN_SK").alias("CLM_LN_SK")
)

# Stage: db2_PCA_CLM_LN_F_Out
drop_temp_sql_db2_PCA_CLM_LN_F_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_PCA_CLM_LN_F_Out_temp"
execute_dml(drop_temp_sql_db2_PCA_CLM_LN_F_Out, jdbc_url, jdbc_props)

df_Cpy_PCA_CLM_LN_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_PCA_CLM_LN_F_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_PCA_CLM_LN_F_Out = f"""
MERGE {EDWOwner}.PCA_CLM_LN_F as c
USING STAGING.EdwEdwClm2Del_db2_PCA_CLM_LN_F_Out_temp as s
ON c.CLM_LN_SK = s.CLM_LN_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_PCA_CLM_LN_F_Out, jdbc_url, jdbc_props)

# Stage: db2_PCA_CLM_F_in
extract_query_db2_PCA_CLM_F_in = f"SELECT CLM_SK FROM {EDWOwner}.PCA_CLM_F c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_PCA_CLM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PCA_CLM_F_in)
    .load()
)

# Stage: Cpy_CLM_SK
df_Cpy_CLM_SK = df_db2_PCA_CLM_F_in.select(
    F.col("CLM_SK").alias("CLM_SK")
)

# Stage: db2_PCA_CLM_F_Out
drop_temp_sql_db2_PCA_CLM_F_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_PCA_CLM_F_Out_temp"
execute_dml(drop_temp_sql_db2_PCA_CLM_F_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_PCA_CLM_F_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_PCA_CLM_F_Out = f"""
MERGE {EDWOwner}.PCA_CLM_F as c
USING STAGING.EdwEdwClm2Del_db2_PCA_CLM_F_Out_temp as s
ON c.CLM_SK = s.CLM_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_PCA_CLM_F_Out, jdbc_url, jdbc_props)

# Stage: db2_CLM_OVRD_D_in
extract_query_db2_CLM_OVRD_D_in = f"SELECT CLM_OVRD_SK FROM {EDWOwner}.CLM_OVRD_D c, {EDWOwner}.W_CLM_DEL d WHERE c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle} AND d.SRC_SYS_CD = c.SRC_SYS_CD AND d.CLM_ID = c.CLM_ID"
df_db2_CLM_OVRD_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_OVRD_D_in)
    .load()
)

# Stage: Cpy_CLM_OVRD_SK
df_Cpy_CLM_OVRD_SK = df_db2_CLM_OVRD_D_in.select(
    F.col("CLM_OVRD_SK").alias("CLM_OVRD_SK")
)

# Stage: db2_CLM_OVRD_D_Out
drop_temp_sql_db2_CLM_OVRD_D_Out = "DROP TABLE IF EXISTS STAGING.EdwEdwClm2Del_db2_CLM_OVRD_D_Out_temp"
execute_dml(drop_temp_sql_db2_CLM_OVRD_D_Out, jdbc_url, jdbc_props)

df_Cpy_CLM_OVRD_SK.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwEdwClm2Del_db2_CLM_OVRD_D_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_CLM_OVRD_D_Out = f"""
MERGE {EDWOwner}.CLM_OVRD_D as c
USING STAGING.EdwEdwClm2Del_db2_CLM_OVRD_D_Out_temp as s
ON c.CLM_OVRD_SK = s.CLM_OVRD_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2_CLM_OVRD_D_Out, jdbc_url, jdbc_props)