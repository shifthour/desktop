# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 - 2023 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiMbrCobStep2Load
# MAGIC CALLED BY:  EdwBhiCOBItemSplitSteps2to7Seq
# MAGIC 
# MAGIC DESCRIPTION:  step 2 in item split - Loads records to W_BHI_MBR_COB_SPLT
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                 Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)                        Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------               -------------------         ---------------------------\(9)           -----------------------------------------------------------------------                 --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam          2013-08-28          5115 BHI                               Original programming                                                           EnterpriseNewDevl         Kalyan Neelam          2013-11-13                                   
# MAGIC 
# MAGIC Krishnakanth M         2018-04-04              TFS19405                         Changed the auto commit mode to "ON"                                EnterpriseDev2                 Jaideep Mankala      04/05/2018
# MAGIC 
# MAGIC 
# MAGIC Mrudula Kodali        2023-10-18            596836          Added parameter $APT_CONFIG_FILE  to use node_1.apt Configuration file    EnterpriseDev2\(9)Ken Bradmon\(9)2023-10-27

# MAGIC Step 2 on ItemSpit Table for COB
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# Stage: BhiMbrCobStep3 (PxDataSet)
df_BhiMbrCobStep3 = spark.read.parquet(f"{adls_path}/ds/BhiMbrCobStep3.parquet")

# Stage: Trans (CTransformerStage)
df_Load = df_BhiMbrCobStep3.select(
    col("MBR_UNIQ_KEY"),
    rpad(col("MBR_EFF_DT_SK"), 10, " ").alias("MBR_EFF_DT_SK"),
    rpad(col("MBR_TERM_DT_SK"), 10, " ").alias("MBR_TERM_DT_SK"),
    col("MBR_COB_TYP_CD"),
    rpad(col("MBR_COB_EFF_DT_SK"), 10, " ").alias("MBR_COB_EFF_DT_SK"),
    rpad(col("MBR_COB_TERM_DT_SK"), 10, " ").alias("MBR_COB_TERM_DT_SK"),
    rpad(col("BEG_EFF_DT_SK"), 10, " ").alias("BEG_EFF_DT_SK"),
    rpad(col("BEG_TERM_DT_SK"), 10, " ").alias("BEG_TERM_DT_SK"),
    rpad(col("MID_EFF_DT_SK"), 10, " ").alias("MID_EFF_DT_SK"),
    rpad(col("MID_TERM_DT_SK"), 10, " ").alias("MID_TERM_DT_SK"),
    rpad(col("END_EFF_DT_SK"), 10, " ").alias("END_EFF_DT_SK"),
    rpad(col("END_TERM_DT_SK"), 10, " ").alias("END_TERM_DT_SK"),
    rpad(col("UPDT_IN"), 1, " ").alias("UPDT_IN")
)

# Stage: W_BHI_MBR_COB_SPLT (DB2ConnectorPX) -> EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwBhiMbrCobStep3Load_W_BHI_MBR_COB_SPLT_temp", jdbc_url, jdbc_props)

df_Load.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwBhiMbrCobStep3Load_W_BHI_MBR_COB_SPLT_temp") \
    .mode("errorifexists") \
    .save()

merge_sql = f"""
MERGE INTO {EDWOwner}.W_BHI_MBR_COB_SPLT AS T
USING STAGING.EdwBhiMbrCobStep3Load_W_BHI_MBR_COB_SPLT_temp AS S
ON
    T.MBR_UNIQ_KEY=S.MBR_UNIQ_KEY AND
    T.MBR_EFF_DT_SK=S.MBR_EFF_DT_SK AND
    T.MBR_TERM_DT_SK=S.MBR_TERM_DT_SK AND
    T.MBR_COB_TYP_CD=S.MBR_COB_TYP_CD AND
    T.MBR_COB_EFF_DT_SK=S.MBR_COB_EFF_DT_SK AND
    T.MBR_COB_TERM_DT_SK=S.MBR_COB_TERM_DT_SK AND
    T.MID_EFF_DT_SK=S.MID_EFF_DT_SK AND
    T.MID_TERM_DT_SK=S.MID_TERM_DT_SK AND
    T.END_EFF_DT_SK=S.END_EFF_DT_SK AND
    T.END_TERM_DT_SK=S.END_TERM_DT_SK
WHEN MATCHED THEN UPDATE SET
    T.MBR_UNIQ_KEY=S.MBR_UNIQ_KEY,
    T.MBR_EFF_DT_SK=S.MBR_EFF_DT_SK,
    T.MBR_TERM_DT_SK=S.MBR_TERM_DT_SK,
    T.MBR_COB_TYP_CD=S.MBR_COB_TYP_CD,
    T.MBR_COB_EFF_DT_SK=S.MBR_COB_EFF_DT_SK,
    T.MBR_COB_TERM_DT_SK=S.MBR_COB_TERM_DT_SK,
    T.BEG_EFF_DT_SK=S.BEG_EFF_DT_SK,
    T.BEG_TERM_DT_SK=S.BEG_TERM_DT_SK,
    T.MID_EFF_DT_SK=S.MID_EFF_DT_SK,
    T.MID_TERM_DT_SK=S.MID_TERM_DT_SK,
    T.END_EFF_DT_SK=S.END_EFF_DT_SK,
    T.END_TERM_DT_SK=S.END_TERM_DT_SK,
    T.UPDT_IN=S.UPDT_IN
WHEN NOT MATCHED THEN INSERT
(
    MBR_UNIQ_KEY,
    MBR_EFF_DT_SK,
    MBR_TERM_DT_SK,
    MBR_COB_TYP_CD,
    MBR_COB_EFF_DT_SK,
    MBR_COB_TERM_DT_SK,
    BEG_EFF_DT_SK,
    BEG_TERM_DT_SK,
    MID_EFF_DT_SK,
    MID_TERM_DT_SK,
    END_EFF_DT_SK,
    END_TERM_DT_SK,
    UPDT_IN
)
VALUES
(
    S.MBR_UNIQ_KEY,
    S.MBR_EFF_DT_SK,
    S.MBR_TERM_DT_SK,
    S.MBR_COB_TYP_CD,
    S.MBR_COB_EFF_DT_SK,
    S.MBR_COB_TERM_DT_SK,
    S.BEG_EFF_DT_SK,
    S.BEG_TERM_DT_SK,
    S.MID_EFF_DT_SK,
    S.MID_TERM_DT_SK,
    S.END_EFF_DT_SK,
    S.END_TERM_DT_SK,
    S.UPDT_IN
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)