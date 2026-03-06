# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:  Pulls data from the table P_CAE_MBR_PRMCY in IDS to create load file for Data Mart
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Laurel Kindley\(9)10/04/2006\(9)\(9)\(9)Originally programmed
# MAGIC Laurel Kindley\(9)2008-05-23\(9)2051\(9)\(9)Added loading of P_CAE_MBR_DRVR\(9)\(9)devlIDS                                    Steph Goddard        05/28/2008
# MAGIC 
# MAGIC Michael Harmon      2015-08-26             5212 Pmt Innovations       Added filter criteria                                            Integrate/Dev1                         Kalyan Neelam        2015-09-01
# MAGIC                                                                                                         MBR.HOST_MBR_IN <> 'Y' to exclude
# MAGIC                                                                                                         hosted members

# MAGIC Extract data from P_CAE_MBR_PRMCY in IDS and load into Data Mart
# MAGIC Extract data from P_CAE_MBR_DRVR in IDS and load into Data Mart
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
RunCycle = get_widget_value('RunCycle','108')
IDSOwner = get_widget_value('IDSOwner','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ------------------------------------------------------------------------------
# STAGE: IDS (DB2Connector) extracting data from IDS
# ------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT P_CAE_MBR_PRMCY.MBR_SK as MBR_SK,
       P_CAE_MBR_PRMCY.CAE_UNIQ_KEY as CAE_UNIQ_KEY,
       P_CAE_MBR_PRMCY.INDV_BE_KEY as INDV_BE_KEY,
       P_CAE_MBR_PRMCY.PRI_IN as PRI_IN,
       P_CAE_MBR_PRMCY.GRP_SK as GRP_SK,
       P_CAE_MBR_PRMCY.MBR_BRTH_DT as MBR_BRTH_DT,
       P_CAE_MBR_PRMCY.MBR_GNDR_CD as MBR_GNDR_CD,
       P_CAE_MBR_PRMCY.LAST_UPDT_DT as LAST_UPDT_DT,
       MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY
  FROM {IDSOwner}.p_cae_mbr_prmcy P_CAE_MBR_PRMCY,
       {IDSOwner}.mbr MBR
 WHERE P_CAE_MBR_PRMCY.MBR_SK = MBR.MBR_SK
   AND MBR.HOST_MBR_IN <> 'Y'

{IDSOwner}.p_cae_mbr_prmcy P_CAE_MBR_PRMCY,
{IDSOwner}.mbr MBR
"""
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ------------------------------------------------------------------------------
# STAGE: BusinessRules (CTransformerStage)
# ------------------------------------------------------------------------------
df_BusinessRules = df_IDS.select(
    lit("FACETS").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("PRI_IN").alias("PRI_IN"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# ------------------------------------------------------------------------------
# Prepare final DataFrame for stage: P_CAE_MBR_PRMCY (CODBCStage)
# ------------------------------------------------------------------------------
df_p_cae_mbr_prmcy = df_BusinessRules.select(
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("CAE_UNIQ_KEY"),
    col("INDV_BE_KEY"),
    rpad(col("PRI_IN"), 1, " ").alias("PRI_IN"),
    col("GRP_SK"),
    col("MBR_BRTH_DT"),
    col("MBR_GNDR_CD"),
    col("LAST_UPDT_DT")
)

# Write to a temporary table and perform MERGE
temp_table_1 = "STAGING.DMPCaeMbrPrmcy_P_CAE_MBR_PRMCY_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_1}", jdbc_url, jdbc_props)

df_p_cae_mbr_prmcy.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_1) \
    .mode("append") \
    .save()

merge_sql_1 = f"""
MERGE P_CAE_MBR_PRMCY AS T
USING {temp_table_1} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.CAE_UNIQ_KEY = S.CAE_UNIQ_KEY,
    T.INDV_BE_KEY = S.INDV_BE_KEY,
    T.PRI_IN = S.PRI_IN,
    T.GRP_SK = S.GRP_SK,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.MBR_GNDR_CD = S.MBR_GNDR_CD,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, MBR_UNIQ_KEY, CAE_UNIQ_KEY, INDV_BE_KEY, PRI_IN, GRP_SK, MBR_BRTH_DT, MBR_GNDR_CD, LAST_UPDT_DT)
  VALUES (S.SRC_SYS_CD, S.MBR_UNIQ_KEY, S.CAE_UNIQ_KEY, S.INDV_BE_KEY, S.PRI_IN, S.GRP_SK, S.MBR_BRTH_DT, S.MBR_GNDR_CD, S.LAST_UPDT_DT);
"""
execute_dml(merge_sql_1, jdbc_url, jdbc_props)

# ------------------------------------------------------------------------------
# STAGE: IDS_Cae_Drvr (DB2Connector) extracting data from IDS
# ------------------------------------------------------------------------------
extract_query_2 = f"""
SELECT P_CAE_MBR_DRVR.MBR_SK as MBR_SK,
       P_CAE_MBR_DRVR.CAE_UNIQ_KEY as CAE_UNIQ_KEY,
       P_CAE_MBR_DRVR.INDV_BE_KEY as INDV_BE_KEY,
       P_CAE_MBR_DRVR.PRI_IN as PRI_IN,
       P_CAE_MBR_DRVR.GRP_SK as GRP_SK,
       P_CAE_MBR_DRVR.MBR_BRTH_DT as MBR_BRTH_DT,
       P_CAE_MBR_DRVR.MBR_GNDR_CD as MBR_GNDR_CD,
       P_CAE_MBR_DRVR.LAST_UPDT_DT as LAST_UPDT_DT,
       MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY
  FROM {IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR,
       {IDSOwner}.mbr MBR
 WHERE P_CAE_MBR_DRVR.MBR_SK = MBR.MBR_SK
   AND MBR.HOST_MBR_IN <> 'Y'

{IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR,
{IDSOwner}.mbr MBR
"""
df_IDS_Cae_Drvr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# ------------------------------------------------------------------------------
# STAGE: Drvr_BusinessRules (CTransformerStage)
# ------------------------------------------------------------------------------
df_Drvr_BusinessRules = df_IDS_Cae_Drvr.select(
    lit("FACETS").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("PRI_IN").alias("PRI_IN"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# ------------------------------------------------------------------------------
# Prepare final DataFrame for stage: P_CAE_MBR_DRVR (CODBCStage)
# ------------------------------------------------------------------------------
df_p_cae_mbr_drvr = df_Drvr_BusinessRules.select(
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("CAE_UNIQ_KEY"),
    col("INDV_BE_KEY"),
    rpad(col("PRI_IN"), 1, " ").alias("PRI_IN"),
    col("GRP_SK"),
    col("MBR_BRTH_DT"),
    col("MBR_GNDR_CD"),
    col("LAST_UPDT_DT")
)

# Write to a temporary table and perform MERGE
temp_table_2 = "STAGING.DMPCaeMbrPrmcy_P_CAE_MBR_DRVR_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_2}", jdbc_url, jdbc_props)

df_p_cae_mbr_drvr.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_2) \
    .mode("append") \
    .save()

merge_sql_2 = f"""
MERGE P_CAE_MBR_DRVR AS T
USING {temp_table_2} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.CAE_UNIQ_KEY = S.CAE_UNIQ_KEY,
    T.INDV_BE_KEY = S.INDV_BE_KEY,
    T.PRI_IN = S.PRI_IN,
    T.GRP_SK = S.GRP_SK,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.MBR_GNDR_CD = S.MBR_GNDR_CD,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, MBR_UNIQ_KEY, CAE_UNIQ_KEY, INDV_BE_KEY, PRI_IN, GRP_SK, MBR_BRTH_DT, MBR_GNDR_CD, LAST_UPDT_DT)
  VALUES (S.SRC_SYS_CD, S.MBR_UNIQ_KEY, S.CAE_UNIQ_KEY, S.INDV_BE_KEY, S.PRI_IN, S.GRP_SK, S.MBR_BRTH_DT, S.MBR_GNDR_CD, S.LAST_UPDT_DT);
"""
execute_dml(merge_sql_2, jdbc_url, jdbc_props)