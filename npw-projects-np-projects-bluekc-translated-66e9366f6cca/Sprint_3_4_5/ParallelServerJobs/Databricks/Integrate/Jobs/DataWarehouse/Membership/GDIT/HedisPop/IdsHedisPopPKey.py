# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsHedisPopPKey
# MAGIC 
# MAGIC Called By: GDITIdsMbrHedisMesrYRMOExtCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC Applies Transformation rules for MBR_HEDIS_MESR_YR_MO
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Akhila M                               2018-03-22        30001                                 Orig Development                                         IntegrateDev2             Jaideep Mankala         03/30/2018

# MAGIC This Dataset created in the GDITIdsMbrHedisMesrYrMoXfrm
# MAGIC Create Primary Key for HEDIS_POP table
# MAGIC Balancing Load file
# MAGIC Final Load file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
RunDate = get_widget_value("RunDate","")
RunCycExctnSK = get_widget_value("RunCycExctnSK","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","")
IDSDB = get_widget_value("IDSDB","")
IDSAcct = get_widget_value("IDSAcct","")
IDSPW = get_widget_value("IDSPW","")

jdbc_url_DB2_K_HEDIS_POP, jdbc_props_DB2_K_HEDIS_POP = get_db_config(ids_secret_name)
extract_query_DB2_K_HEDIS_POP = f"SELECT HEDIS_POP_NM, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, HEDIS_POP_SK from {IDSOwner}.K_HEDIS_POP"
df_DB2_K_HEDIS_POP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_DB2_K_HEDIS_POP)
    .options(**jdbc_props_DB2_K_HEDIS_POP)
    .option("query", extract_query_DB2_K_HEDIS_POP)
    .load()
)

schema_Ds_Pop = StructType([
    StructField("HEDIS_POP_NM", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True)
])
df_Ds_Pop = spark.read.schema(schema_Ds_Pop).parquet(f"{adls_path}/ds/HEDIS_POP.xfrm.{RunID}.parquet")

df_Cpy_Data = df_Ds_Pop.select(
    df_Ds_Pop["HEDIS_POP_NM"].alias("HEDIS_POP_NM"),
    df_Ds_Pop["SRC_SYS_CD"].alias("SRC_SYS_CD")
)

df_Rdp_Key = dedup_sort(df_Cpy_Data, ["HEDIS_POP_NM","SRC_SYS_CD"], [])

df_Jn_Key_pre = df_Rdp_Key.alias("Lnk_K_TableJoin").join(
    df_DB2_K_HEDIS_POP.alias("Lnk_KHedisPop"),
    on=[
        df_Rdp_Key["HEDIS_POP_NM"] == df_DB2_K_HEDIS_POP["HEDIS_POP_NM"],
        df_Rdp_Key["SRC_SYS_CD"] == df_DB2_K_HEDIS_POP["SRC_SYS_CD"]
    ],
    how="left"
)
df_Jn_Key = df_Jn_Key_pre.select(
    F.col("Lnk_K_TableJoin.HEDIS_POP_NM").alias("HEDIS_POP_NM"),
    F.col("Lnk_K_TableJoin.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_KHedisPop.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_KHedisPop.HEDIS_POP_SK").alias("HEDIS_POP_SK")
)

df_enriched = df_Jn_Key.withColumn("IS_NEW", F.col("HEDIS_POP_SK").isNull())
df_enriched = df_enriched.withColumn(
    "HEDIS_POP_SK",
    F.when(F.col("HEDIS_POP_SK").isNull(), F.lit(None)).otherwise(F.col("HEDIS_POP_SK"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"HEDIS_POP_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("IS_NEW"), F.lit(RunCycExctnSK)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycExctnSK))

df_Xfm_SkCreation_all = df_enriched.select(
    F.col("HEDIS_POP_SK").alias("HEDIS_POP_SK"),
    F.col("HEDIS_POP_NM").alias("HEDIS_POP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Xfm_SkCreation_new = df_enriched.filter(F.col("IS_NEW")).select(
    F.col("HEDIS_POP_NM").alias("HEDIS_POP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(RunCycExctnSK).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("HEDIS_POP_SK").alias("HEDIS_POP_SK")
)

df_Xfm_SkCreation_balance = df_enriched.select(
    F.col("HEDIS_POP_NM").alias("HEDIS_POP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Lnk_NA = spark.createDataFrame(
    [(1, "NA", "NA", 100, 100)],
    ["HEDIS_POP_SK","HEDIS_POP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"]
)

df_Lnk_UNK = spark.createDataFrame(
    [(0, "UNK", "UNK", 100, 100)],
    ["HEDIS_POP_SK","HEDIS_POP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"]
)

jdbc_url_db2_K_HEDIS_Pop_Load, jdbc_props_db2_K_HEDIS_Pop_Load = get_db_config(ids_secret_name)
temp_table_db2_K_HEDIS_Pop_Load = "STAGING.IdsHedisPopPKey_db2_K_HEDIS_Pop_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_HEDIS_Pop_Load}", jdbc_url_db2_K_HEDIS_Pop_Load, jdbc_props_db2_K_HEDIS_Pop_Load)
df_Xfm_SkCreation_new.write.format("jdbc") \
    .option("url", jdbc_url_db2_K_HEDIS_Pop_Load) \
    .options(**jdbc_props_db2_K_HEDIS_Pop_Load) \
    .option("dbtable", temp_table_db2_K_HEDIS_Pop_Load) \
    .mode("overwrite") \
    .save()
merge_sql_db2_K_HEDIS_Pop_Load = f"""
MERGE INTO {IDSOwner}.K_HEDIS_POP AS T
USING {temp_table_db2_K_HEDIS_Pop_Load} AS S
ON (T.HEDIS_POP_NM = S.HEDIS_POP_NM AND T.SRC_SYS_CD = S.SRC_SYS_CD)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.HEDIS_POP_SK = S.HEDIS_POP_SK
WHEN NOT MATCHED THEN
  INSERT (
    HEDIS_POP_NM,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    HEDIS_POP_SK
  )
  VALUES (
    S.HEDIS_POP_NM,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.HEDIS_POP_SK
  );
"""
execute_dml(merge_sql_db2_K_HEDIS_Pop_Load, jdbc_url_db2_K_HEDIS_Pop_Load, jdbc_props_db2_K_HEDIS_Pop_Load)

write_files(
    df_Xfm_SkCreation_balance,
    f"{adls_path}/load/B_HEDIS_POP.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Fnl_All = df_Lnk_NA.select(
    "HEDIS_POP_SK","HEDIS_POP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
).unionByName(
    df_Lnk_UNK.select(
        "HEDIS_POP_SK","HEDIS_POP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
).unionByName(
    df_Xfm_SkCreation_all.select(
        "HEDIS_POP_SK","HEDIS_POP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_Fnl_All,
    f"{adls_path}/load/HEDIS_POP.{SrcSysCd}.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)