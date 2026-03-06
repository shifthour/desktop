# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  FctsPrvcyLoadSeq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:   Delete IDS Privacy Extrnl Enty Change Data Capture rows.   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	         Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------     --------------------------------	-------------------------------	----------------------------       
# MAGIC Steph Goddard        11/24/09 	3556 Change Data Capture 	 Original Programming                       devlIDSnew

# MAGIC File created in driver job FctsPrvcyExtrnlEntyDrvrExtr
# MAGIC Apply Deletions to IDS Privacy Extrnl Enty table,
# MAGIC EDW delete not necessary - no EDW table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCdSk = get_widget_value('SrcSysCdSk','69560')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: PrvcyExtrnlEntyDelCrf (CSeqFileStage)
schema_PrvcyExtrnlEntyDelCrf = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("ENEN_CKE", IntegerType(), nullable=False),
    StructField("OP", StringType(), nullable=False)
])
df_PrvcyExtrnlEntyDelCrf = (
    spark.read
    .option("header", "false")
    .option("quote", '"')
    .option("sep", ",")
    .schema(schema_PrvcyExtrnlEntyDelCrf)
    .csv(f"{adls_path}/load/IdsPrvcyExtrnlEnty.del")
)

# Stage: PurgeTrnPrvcyExtrnlEnty (CTransformerStage)
df_PurgeTrnPrvcyExtrnlEnty_IdsDelete = df_PrvcyExtrnlEntyDelCrf.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ENEN_CKE").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY")
)

# Stage: PRVCY_EXTRNL_ENTY (CODBCStage) - DELETE logic via staging table
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
drop_sql = "DROP TABLE IF EXISTS STAGING.IdsPrvcyExtrnlEntyDel_PRVCY_EXTRNL_ENTY_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_PurgeTrnPrvcyExtrnlEnty_IdsDelete.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsPrvcyExtrnlEntyDel_PRVCY_EXTRNL_ENTY_temp") \
    .mode("overwrite") \
    .save()

delete_sql = f"""
DELETE T
FROM {IDSOwner}.PRVCY_EXTRNL_ENTY T
JOIN STAGING.IdsPrvcyExtrnlEntyDel_PRVCY_EXTRNL_ENTY_temp S
    ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
    AND T.PRVCY_EXTRNL_ENTY_UNIQ_KEY = S.PRVCY_EXTRNL_ENTY_UNIQ_KEY
"""
execute_dml(delete_sql, jdbc_url, jdbc_props)