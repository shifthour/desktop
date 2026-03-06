# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Pooja Sunkara          2014-08-07              5345                             Original Programming                                                                        IntegrateWrhsDevl     Kalyan Neelam             2015-01-05

# MAGIC IdsProvLocPkey
# MAGIC Land into Seq File for the FKEY job
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_PROV_LOC Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
IDSRunCycle = get_widget_value("IDSRunCycle","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_ds_PROV_LOC_Xfrm = spark.read.parquet(f"{adls_path}/ds/PROV_LOC.{SrcSysCd}.xfrm.{RunID}.parquet")

df_Cpy_lnk_IdsProvLocPkey_All = df_ds_PROV_LOC_Xfrm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "PROV_ADDR_SK",
    "PROV_SK",
    "PRI_ADDR_IN",
    "REMIT_ADDR_IN"
)

df_Cpy_lnk_FctsIdsProvLocPkey_dedup = df_ds_PROV_LOC_Xfrm.select(
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "SRC_SYS_CD"
)

df_rdup_Natural_Keys = dedup_sort(
    df_Cpy_lnk_FctsIdsProvLocPkey_dedup,
    ["PROV_ID","PROV_ADDR_ID","PROV_ADDR_TYP_CD","PROV_ADDR_EFF_DT","SRC_SYS_CD"],
    []
)

extract_query_db2_K_PROV_LOC_in = f"SELECT PROV_ID, PROV_ADDR_ID, PROV_ADDR_TYP_CD, PROV_ADDR_EFF_DT_SK as PROV_ADDR_EFF_DT, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROV_LOC_SK FROM {IDSOwner}.K_PROV_LOC"
df_db2_K_PROV_LOC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_PROV_LOC_in)
    .load()
)

df_jn_ProvLoc = df_rdup_Natural_Keys.alias("A").join(
    df_db2_K_PROV_LOC_in.alias("B"),
    on=[
        F.col("A.PROV_ID") == F.col("B.PROV_ID"),
        F.col("A.PROV_ADDR_ID") == F.col("B.PROV_ADDR_ID"),
        F.col("A.PROV_ADDR_TYP_CD") == F.col("B.PROV_ADDR_TYP_CD"),
        F.col("A.PROV_ADDR_EFF_DT") == F.col("B.PROV_ADDR_EFF_DT"),
        F.col("A.SRC_SYS_CD") == F.col("B.SRC_SYS_CD")
    ],
    how="left"
).select(
    F.col("A.PROV_ID"),
    F.col("A.PROV_ADDR_ID"),
    F.col("A.PROV_ADDR_TYP_CD"),
    F.col("A.PROV_ADDR_EFF_DT"),
    F.col("A.SRC_SYS_CD"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK"),
    F.col("B.PROV_LOC_SK")
)

df_enriched = df_jn_ProvLoc.withColumnRenamed("PROV_LOC_SK","orig_PROV_LOC_SK")
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROV_LOC_SK",<schema>,<secret_name>)

df_lnk_Pkey_out = df_enriched.select(
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    rpad("PROV_ADDR_EFF_DT",10," ").alias("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.when(F.col("orig_PROV_LOC_SK").isNull(), F.col("IDSRunCycle")).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_LOC_SK").alias("PROV_LOC_SK")
)

df_lnk_KProvLoc_new = df_enriched.filter(
    F.col("orig_PROV_LOC_SK").isNull()
).select(
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    rpad("PROV_ADDR_EFF_DT",10," ").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_LOC_SK").alias("PROV_LOC_SK")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsProvLocPkey_db2_K_PROV_LOC_load_temp", jdbc_url, jdbc_props)

df_lnk_KProvLoc_new.write.format("jdbc")\
    .option("url", jdbc_url)\
    .options(**jdbc_props)\
    .option("dbtable","STAGING.IdsProvLocPkey_db2_K_PROV_LOC_load_temp")\
    .mode("overwrite")\
    .save()

merge_sql_db2_K_PROV_LOC_load = f"""
MERGE INTO {IDSOwner}.K_PROV_LOC AS T
USING STAGING.IdsProvLocPkey_db2_K_PROV_LOC_load_temp AS S
ON
(
  T.PROV_ID = S.PROV_ID
  AND T.PROV_ADDR_ID = S.PROV_ADDR_ID
  AND T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD
  AND T.PROV_ADDR_EFF_DT_SK = S.PROV_ADDR_EFF_DT_SK
  AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN
  UPDATE SET
    T.PROV_ID=S.PROV_ID,
    T.PROV_ADDR_ID=S.PROV_ADDR_ID,
    T.PROV_ADDR_TYP_CD=S.PROV_ADDR_TYP_CD,
    T.PROV_ADDR_EFF_DT_SK=S.PROV_ADDR_EFF_DT_SK,
    T.SRC_SYS_CD=S.SRC_SYS_CD,
    T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
    T.PROV_LOC_SK=S.PROV_LOC_SK
WHEN NOT MATCHED THEN
  INSERT
  (
    PROV_ID,
    PROV_ADDR_ID,
    PROV_ADDR_TYP_CD,
    PROV_ADDR_EFF_DT_SK,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    PROV_LOC_SK
  )
  VALUES
  (
    S.PROV_ID,
    S.PROV_ADDR_ID,
    S.PROV_ADDR_TYP_CD,
    S.PROV_ADDR_EFF_DT_SK,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.PROV_LOC_SK
  );
"""
execute_dml(merge_sql_db2_K_PROV_LOC_load, jdbc_url, jdbc_props)

df_jn_PKey = df_Cpy_lnk_IdsProvLocPkey_All.alias("A").join(
    df_lnk_Pkey_out.alias("B"),
    on=[
        F.col("A.PROV_ID") == F.col("B.PROV_ID"),
        F.col("A.PROV_ADDR_ID") == F.col("B.PROV_ADDR_ID"),
        F.col("A.PROV_ADDR_TYP_CD") == F.col("B.PROV_ADDR_TYP_CD"),
        F.col("A.PROV_ADDR_EFF_DT") == F.col("B.PROV_ADDR_EFF_DT"),
        F.col("A.SRC_SYS_CD") == F.col("B.SRC_SYS_CD")
    ],
    how="inner"
).select(
    F.col("A.PRI_NAT_KEY_STRING"),
    F.col("A.FIRST_RECYC_TS"),
    F.col("B.PROV_LOC_SK"),
    F.col("A.PROV_ID"),
    F.col("A.PROV_ADDR_ID"),
    F.col("A.PROV_ADDR_TYP_CD"),
    rpad("A.PROV_ADDR_EFF_DT",10," ").alias("PROV_ADDR_EFF_DT"),
    F.col("A.SRC_SYS_CD"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK"),
    F.col("B.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("A.SRC_SYS_CD_SK"),
    F.col("A.PROV_ADDR_SK"),
    F.col("A.PROV_SK"),
    rpad("A.PRI_ADDR_IN",1," ").alias("PRI_ADDR_IN"),
    rpad("A.REMIT_ADDR_IN",1," ").alias("REMIT_ADDR_IN")
)

write_files(
    df_jn_PKey,
    f"{adls_path}/key/PROV_LOC.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)