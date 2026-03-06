# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCAFEPClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  BCA FEP Claims Member Matching Error Table Delete.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Sudhir Bomshetty                  2017-10-12              5781                          Original Programming                                                       IntegrateDev2                   Kalyan Neelam           2017-10-20


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
FileName = get_widget_value("FileName","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")

schema_BCAFEPPClmMbrErr = StructType([
    StructField("CLM_SK", IntegerType(), False)
])
df_BCAFEPPClmMbrErr = (
    spark.read.format("csv")
    .schema(schema_BCAFEPPClmMbrErr)
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .load(f"{adls_path}/verified/{FileName}.{RunID}")
)

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_ids = f"SELECT CLM_SK FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC"
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_ids)
    .load()
)

df_hf_bcafep_ids_p_clm_err_del = dedup_sort(df_IDS_P_CLM_ERR, ["CLM_SK"], [])

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_edw = f"SELECT CLM_SK FROM {EDWOwner}.CLM_F WHERE (MBR_SK <> 0 AND SUB_SK <> 0 AND GRP_SK <> 0 AND PROD_SK <> 0) AND SRC_SYS_CD = 'BCA'"
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_edw)
    .load()
)

df_hf_bcafep_edw_clm_f = dedup_sort(df_EDW_CLM_F, ["CLM_SK"], [])

df_Trn_CLM = (
    df_hf_bcafep_ids_p_clm_err_del.alias("ERR_CLM")
    .join(
        df_hf_bcafep_edw_clm_f.alias("EDW_CLM"),
        F.col("ERR_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
        "left"
    )
    .filter(~F.isnull(F.col("EDW_CLM.CLM_SK")))
    .select(F.col("ERR_CLM.CLM_SK").alias("CLM_SK"))
)

df_Lnk_Col_CLM = df_Trn_CLM.unionByName(
    df_BCAFEPPClmMbrErr.select("CLM_SK")
)

df_Trns1 = df_Lnk_Col_CLM.select(F.col("CLM_SK").alias("CLM_SK"))

jdbc_url_IDS2, jdbc_props_IDS2 = get_db_config(ids_secret_name)
delete_sql = (
    f"DELETE FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC "
    f"WHERE CLM_SK IN ( "
    f"SELECT CLM.CLM_SK "
    f"FROM {IDSOwner}.CLM CLM "
    f"INNER JOIN {IDSOwner}.CD_MPPNG CD "
    f"ON CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"WHERE (CLM.MBR_SK <> 0 AND CLM.SUB_SK <> 0 AND CLM.GRP_SK <> 0 AND CLM.PROD_SK <> 0) "
    f"AND CD.TRGT_CD = 'BCA' "
    f")"
)
execute_dml(delete_sql, jdbc_url_IDS2, jdbc_props_IDS2)