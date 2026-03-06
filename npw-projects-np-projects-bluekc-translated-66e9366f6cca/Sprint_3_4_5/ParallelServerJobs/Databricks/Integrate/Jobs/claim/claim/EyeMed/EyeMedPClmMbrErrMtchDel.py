# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : EyeMedClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  EyeMed Claims Member Matching Error Table Delete.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                    --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Madhavan B                  2018-03-16              5744                          Original Programming                                                       IntegrateDev2                  Kalyan Neelam           2018-04-09


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FileName = get_widget_value('FileName','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

schema_EyeMedPClmMbrErr = StructType([
    StructField("CLM_SK", IntegerType(), nullable=False)
])

df_EyeMedPClmMbrErr = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_EyeMedPClmMbrErr)
    .load(f"{adls_path}/verified/{FileName}.{RunID}")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLM_SK FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC")
    .load()
)

df_hf_eyemed_ids_p_clm_err_del = dedup_sort(
    df_IDS_P_CLM_ERR,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", f"SELECT CLM_SK FROM {EDWOwner}.CLM_F WHERE (MBR_SK <> 0 AND SUB_SK <> 0 AND GRP_SK <> 0 AND PROD_SK <> 0) AND SRC_SYS_CD = 'EYEMED'")
    .load()
)

df_hf_eyemed_edw_clm_f = dedup_sort(
    df_EDW_CLM_F,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

df_join_Trn_CLM = (
    df_hf_eyemed_ids_p_clm_err_del.alias("ERR_CLM")
    .join(df_hf_eyemed_edw_clm_f.alias("EDW_CLM"), F.col("ERR_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"), "left")
)

df_Trn_CLM = (
    df_join_Trn_CLM
    .filter(F.col("EDW_CLM.CLM_SK").isNotNull())
    .select(F.col("ERR_CLM.CLM_SK").alias("CLM_SK"))
)

df_Lnk_Col_CLM = (
    df_Trn_CLM.select("CLM_SK")
    .unionByName(df_EyeMedPClmMbrErr.select("CLM_SK"))
)

df_Trns1 = df_Lnk_Col_CLM.select("CLM_SK")

df_P_CLM_Delete = df_Trns1.select("CLM_SK")

jdbc_url_ids_delete, jdbc_props_ids_delete = get_db_config(ids_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.EyeMedPClmMbrErrMtchDel_P_CLM_Delete_temp",
    jdbc_url_ids_delete,
    jdbc_props_ids_delete
)

(
    df_P_CLM_Delete.write.format("jdbc")
    .option("url", jdbc_url_ids_delete)
    .options(**jdbc_props_ids_delete)
    .option("dbtable", "STAGING.EyeMedPClmMbrErrMtchDel_P_CLM_Delete_temp")
    .mode("overwrite")
    .save()
)

delete_sql = f"""
DELETE T
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC T
INNER JOIN STAGING.EyeMedPClmMbrErrMtchDel_P_CLM_Delete_temp S
ON T.CLM_SK = S.CLM_SK
"""
execute_dml(delete_sql, jdbc_url_ids_delete, jdbc_props_ids_delete)

after_sql = f"""
DELETE FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE CLM_SK IN
(
  SELECT CLM.CLM_SK
  FROM {IDSOwner}.CLM AS CLM
  INNER JOIN {IDSOwner}.CD_MPPNG AS CD
    ON CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  WHERE (CLM.MBR_SK <> 0 AND CLM.SUB_SK <> 0 AND CLM.GRP_SK <> 0 AND CLM.PROD_SK <> 0)
    AND CD.TRGT_CD = 'EYEMED'
)
"""
execute_dml(after_sql, jdbc_url_ids_delete, jdbc_props_ids_delete)