# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSCommonClmErrMbrRecycCntl
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  BCBSSC Common Claims Member Matching Error Table Delete.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                          \(9) Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                 \(9)  --------------------------------         -------------------------------   ----------------------------      
# MAGIC Jaideep Mankala                   2018-03-18              5828                          Original Programming                                                     \(9)  IntegrateDev2                   Kalyan Neelam           2018-03-19


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
PClmMbrErrDeleteDate = get_widget_value('PClmMbrErrDeleteDate','')
PSrcSysCd = get_widget_value('PSrcSysCd','')

# BCBSCommonPClmMbrErr (CSeqFileStage) - Read
schema_BCBSCommonPClmMbrErr = StructType([
    StructField("CLM_SK", IntegerType(), nullable=False)
])
df_BCBSCommonPClmMbrErr = (
    spark.read
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_BCBSCommonPClmMbrErr)
    .csv(f"{adls_path}/verified/MnlMbrMatch_PClmMbrErrRecyc_Delete.dat")
)

# IDS_P_CLM_ERR_24_Month_Delete (DB2Connector - Read)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_IDS_P_CLM_ERR_24_Month_Delete = f"""
SELECT
ERR.CLM_SK
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC ERR, {IDSOwner}.CLM CLM
WHERE
ERR.CLM_SK = CLM.CLM_SK
AND CLM.PD_DT_SK < '{PClmMbrErrDeleteDate}'
AND ERR.SRC_SYS_CD in ('{PSrcSysCd}')
"""
df_IDS_P_CLM_ERR_24_Month_Delete = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_P_CLM_ERR_24_Month_Delete)
    .load()
)

# hf_bcbscommon_pclm_mbrsh_err_recyc_del_24_months (CHashedFileStage - Scenario A)
df_hf_bcbscommon_pclm_mbrsh_err_recyc_del_24_months = df_IDS_P_CLM_ERR_24_Month_Delete.dropDuplicates(["CLM_SK"])

# IDS_P_CLM_ERR (DB2Connector - Read)
extract_query_IDS_P_CLM_ERR = f"""
SELECT
CLM_SK
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE SRC_SYS_CD in ('{PSrcSysCd}')
"""
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_P_CLM_ERR)
    .load()
)

# hf_bcbscommon_ids_p_clm_err_del (CHashedFileStage - Scenario A)
df_hf_bcbscommon_ids_p_clm_err_del = df_IDS_P_CLM_ERR.dropDuplicates(["CLM_SK"])

# EDW_CLM_F (DB2Connector - Read)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_EDW_CLM_F = f"""
SELECT
CLM_SK
FROM {EDWOwner}.CLM_F
WHERE MBR_SK <> 0 
  AND SRC_SYS_CD in ('{PSrcSysCd}')
"""
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_EDW_CLM_F)
    .load()
)

# hf_bcbscommon_edw_clm_f (CHashedFileStage - Scenario A)
df_hf_bcbscommon_edw_clm_f = df_EDW_CLM_F.dropDuplicates(["CLM_SK"])

# Trn_CLM (CTransformerStage) - Join + Constraint
df_Trn_CLM = (
    df_hf_bcbscommon_ids_p_clm_err_del.alias("ERR_CLM")
    .join(
        df_hf_bcbscommon_edw_clm_f.alias("EDW_CLM"),
        F.col("ERR_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
        "left"
    )
    .where(F.col("EDW_CLM.CLM_SK").isNotNull())
    .select(F.col("ERR_CLM.CLM_SK").alias("CLM_SK"))
)

# Lnk_Col_CLM (CCollector - Round-Robin)
df_Lnk_Col_CLM = (
    df_Trn_CLM.select("CLM_SK")
    .union(df_BCBSCommonPClmMbrErr.select("CLM_SK"))
    .union(df_hf_bcbscommon_pclm_mbrsh_err_recyc_del_24_months.select("CLM_SK"))
)

# Trns1 (CTransformerStage)
df_Trns1 = df_Lnk_Col_CLM.select(F.col("CLM_SK").alias("CLM_SK"))

# P_CLM_Delete (DB2Connector - Write with delete logic)
temp_table_name = "STAGING.IdsClmMnlMbrMtchErrRecycDel_P_CLM_Delete_temp"
drop_temp_table_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_temp_table_sql, jdbc_url_ids, jdbc_props_ids)

# Write the incoming records to the staging temp table
(
    df_Trns1
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

# Merge statement to delete matching records in target
merge_sql = f"""
MERGE INTO {IDSOwner}.P_CLM_MBRSH_ERR_RECYC AS T
USING {temp_table_name} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# After_SQL
after_sql = f"""
DELETE FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE CLM_SK IN
 (
  SELECT
  CLM.CLM_SK
  FROM {IDSOwner}.CLM AS CLM
  INNER JOIN {IDSOwner}.CD_MPPNG AS CD
   ON CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  WHERE CLM.MBR_SK <> 0
   AND CD.TRGT_CD = '{PSrcSysCd}'
 )
"""
execute_dml(after_sql, jdbc_url_ids, jdbc_props_ids)