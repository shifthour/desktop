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
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
PClmMbrErrDeleteDate = get_widget_value('PClmMbrErrDeleteDate','')
pSrcSysCd = get_widget_value('PSrcSysCd','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')
SrcSysCd = get_widget_value('SrcSysCd','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

# --------------------------------------------------------------------------------
# Stage: BCBSCommonPClmMbrErr  (CSeqFileStage)
# --------------------------------------------------------------------------------
schema_BCBSCommonPClmMbrErr = StructType([
    StructField("CLM_SK", IntegerType(), nullable=False)
])
df_BCBSCommonPClmMbrErr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_BCBSCommonPClmMbrErr)
    .load(f"{adls_path}/verified/{SrcSysCd1}_PClmMbrErrRecyc_Delete.dat")
)

# --------------------------------------------------------------------------------
# Stage: IDS_P_CLM_ERR_24_Month_Delete (DB2Connector - IDS)
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids_p_clm_err_24_month = f"""
SELECT
ERR.CLM_SK
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC ERR, {IDSOwner}.CLM CLM
WHERE
ERR.CLM_SK = CLM.CLM_SK
AND CLM.PD_DT_SK < '{PClmMbrErrDeleteDate}'
AND ERR.SRC_SYS_CD = '{pSrcSysCd}'
"""
df_IDS_P_CLM_ERR_24_Month_Delete = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_p_clm_err_24_month)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_bcbscommon_pclm_mbrsh_err_recyc_del_24_months (CHashedFileStage - Scenario A)
# --------------------------------------------------------------------------------
df_delete_24_months_old_data = dedup_sort(
    df_IDS_P_CLM_ERR_24_Month_Delete,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: IDS_P_CLM_ERR (DB2Connector - IDS)
# --------------------------------------------------------------------------------
extract_query_ids_p_clm_err = f"""
SELECT
CLM_SK
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE SRC_SYS_CD = '{pSrcSysCd}'
"""
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_p_clm_err)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_bcbscommon_ids_p_clm_err_del (CHashedFileStage - Scenario A)
# --------------------------------------------------------------------------------
df_ERR_CLM = dedup_sort(
    df_IDS_P_CLM_ERR,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: EDW_CLM_F (DB2Connector - EDW)
# --------------------------------------------------------------------------------
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_edw_clm_f = f"""
SELECT
CLM_SK
FROM {EDWOwner}.CLM_F
WHERE MBR_SK <> 0
AND SRC_SYS_CD = '{SrcSysCd}'
"""
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_edw_clm_f)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_bcbscommon_edw_clm_f (CHashedFileStage - Scenario A)
# --------------------------------------------------------------------------------
df_EDW_CLM = dedup_sort(
    df_EDW_CLM_F,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: Trn_CLM (CTransformerStage)
#    Primary Link: df_ERR_CLM as ERR_CLM
#    Lookup Link:  df_EDW_CLM as EDW_CLM (left join on CLM_SK)
#    Constraint:   Pass rows where EDW_CLM.CLM_SK is not null
# --------------------------------------------------------------------------------
df_Trn_CLM_out = (
    df_ERR_CLM.alias("ERR_CLM")
    .join(df_EDW_CLM.alias("EDW_CLM"), F.col("ERR_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"), "left")
    .filter(F.col("EDW_CLM.CLM_SK").isNotNull())
    .select(F.col("ERR_CLM.CLM_SK").alias("CLM_SK"))
)

# --------------------------------------------------------------------------------
# Stage: Lnk_Col_CLM (CCollector) - Round-Robin with 3 inputs
# --------------------------------------------------------------------------------
df_Lnk_Col_CLM = (
    df_Trn_CLM_out.select("CLM_SK")
    .union(df_BCBSCommonPClmMbrErr.select("CLM_SK"))
    .union(df_delete_24_months_old_data.select("CLM_SK"))
)

# --------------------------------------------------------------------------------
# Stage: Trns1 (CTransformerStage)
# --------------------------------------------------------------------------------
df_Trns1_out = df_Lnk_Col_CLM.select(F.col("CLM_SK").alias("CLM_SK"))

# --------------------------------------------------------------------------------
# Stage: P_CLM_Delete (DB2Connector - IDS) - Delete logic + After_SQL
# --------------------------------------------------------------------------------
temp_table_name = "STAGING.BCBSCommonPClmMbrErrMtchDel_P_CLM_Delete_temp"
merge_target_table = f"{IDSOwner}.P_CLM_MBRSH_ERR_RECYC"

# Drop temp table if exists, then create it
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)
df_Trns1_out.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

# Merge logic: delete matching rows
merge_sql = f"""
MERGE INTO {merge_target_table} AS T
USING {temp_table_name} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# After_SQL delete
delete_sql = f"""
DELETE FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE CLM_SK IN
(
  SELECT
  CLM.CLM_SK
  FROM {IDSOwner}.CLM AS CLM
  INNER JOIN {IDSOwner}.CD_MPPNG AS CD
   ON CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  WHERE CLM.MBR_SK <> 0
   AND CD.TRGT_CD = '{SrcSysCd}'
)
"""
execute_dml(delete_sql, jdbc_url_ids, jdbc_props_ids)