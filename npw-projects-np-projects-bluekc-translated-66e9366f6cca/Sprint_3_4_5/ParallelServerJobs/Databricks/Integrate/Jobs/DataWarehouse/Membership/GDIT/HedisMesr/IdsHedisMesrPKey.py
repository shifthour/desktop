# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsHedisMesrPKey
# MAGIC 
# MAGIC Called By: GDITIdsMbrHedisMesrYRMOExtCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC Applies Transformation rules for MBR_HEDIS_MESR_YR_MO
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Akhila M                       2018-03-22        30001                                 Orig Development                                                IntegrateDev2            Jaideep Mankala         03/30/2018

# MAGIC This Dataset created in the GDITIdsMbrHedisMesrYrMoXfrm
# MAGIC Create Primary Key for HEDIS_MESR table
# MAGIC Final Load file
# MAGIC Balancing Load file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
RunCycExctnSK = get_widget_value('RunCycExctnSK','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_DB2_K_HEDIS_MESR = f"SELECT HEDIS_MESR_NM,HEDIS_SUB_MESR_NM,HEDIS_MBR_BUCKET_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,HEDIS_MESR_SK FROM {IDSOwner}.K_HEDIS_MESR"
df_DB2_K_HEDIS_MESR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_K_HEDIS_MESR)
    .load()
)

df_Ds_Mesr = spark.read.parquet(f"{adls_path}/ds/HEDIS_MESR.xfrm.{RunID}.parquet")
df_Cpy_Data = df_Ds_Mesr.select(
    col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Rdp_Key_temp = dedup_sort(
    df_Cpy_Data,
    ["HEDIS_MESR_NM", "HEDIS_SUB_MESR_NM", "HEDIS_MBR_BUCKET_ID", "SRC_SYS_CD"],
    [
        ("HEDIS_MESR_NM","A"),
        ("HEDIS_SUB_MESR_NM","A"),
        ("HEDIS_MBR_BUCKET_ID","A"),
        ("SRC_SYS_CD","A")
    ]
)
df_Rdp_Key = df_Rdp_Key_temp.select(
    col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Jn_Key_temp = df_Rdp_Key.alias("Lnk_K_TableJoin").join(
    df_DB2_K_HEDIS_MESR.alias("Lnk_KHedisMesr"),
    (
        (col("Lnk_K_TableJoin.HEDIS_MESR_NM") == col("Lnk_KHedisMesr.HEDIS_MESR_NM")) &
        (col("Lnk_K_TableJoin.HEDIS_SUB_MESR_NM") == col("Lnk_KHedisMesr.HEDIS_SUB_MESR_NM")) &
        (col("Lnk_K_TableJoin.HEDIS_MBR_BUCKET_ID") == col("Lnk_KHedisMesr.HEDIS_MBR_BUCKET_ID")) &
        (col("Lnk_K_TableJoin.SRC_SYS_CD") == col("Lnk_KHedisMesr.SRC_SYS_CD"))
    ),
    "left"
)
df_Jn_Key = df_Jn_Key_temp.select(
    col("Lnk_K_TableJoin.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    col("Lnk_K_TableJoin.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    col("Lnk_K_TableJoin.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    col("Lnk_K_TableJoin.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_KHedisMesr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_KHedisMesr.HEDIS_MESR_SK").alias("HEDIS_MESR_SK")
)

df_temp = df_Jn_Key.withColumn("original_HEDIS_MESR_SK", col("HEDIS_MESR_SK"))
df_temp2 = df_temp.withColumn("HEDIS_MESR_SK", col("original_HEDIS_MESR_SK"))
df_enriched = df_temp2
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"HEDIS_MESR_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(col("original_HEDIS_MESR_SK").isNull(), lit(RunCycExctnSK)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    lit(RunCycExctnSK)
)
df_enriched = df_enriched.select(
    col("HEDIS_MESR_SK"),
    col("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("original_HEDIS_MESR_SK")
)

df_all = df_enriched.select(
    "HEDIS_MESR_SK",
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "HEDIS_MBR_BUCKET_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_new = df_enriched.filter(
    col("original_HEDIS_MESR_SK").isNull()
).select(
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "HEDIS_MBR_BUCKET_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "HEDIS_MESR_SK"
)

df_unk = spark.createDataFrame(
    [(0, "UNK", "UNK", "UNK", "UNK", 100, 100)],
    [
        "HEDIS_MESR_SK", 
        "HEDIS_MESR_NM", 
        "HEDIS_SUB_MESR_NM", 
        "HEDIS_MBR_BUCKET_ID", 
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_na = spark.createDataFrame(
    [(1, "NA", "NA", "NA", "NA", 100, 100)],
    [
        "HEDIS_MESR_SK", 
        "HEDIS_MESR_NM", 
        "HEDIS_SUB_MESR_NM", 
        "HEDIS_MBR_BUCKET_ID", 
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_b_table = df_enriched.select(
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "HEDIS_MBR_BUCKET_ID",
    "SRC_SYS_CD"
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsHedisMesrPKey_db2_K_HEDIS_Mesr_Load_temp", jdbc_url, jdbc_props)
df_new.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsHedisMesrPKey_db2_K_HEDIS_Mesr_Load_temp") \
    .mode("append") \
    .save()
merge_sql_db2_K_HEDIS_Mesr_Load = f"""
MERGE INTO {IDSOwner}.K_HEDIS_MESR AS T
USING STAGING.IdsHedisMesrPKey_db2_K_HEDIS_Mesr_Load_temp AS S
ON
 T.HEDIS_MESR_NM = S.HEDIS_MESR_NM
 AND T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM
 AND T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID
 AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
 T.HEDIS_MESR_NM = S.HEDIS_MESR_NM,
 T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM,
 T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID,
 T.SRC_SYS_CD = S.SRC_SYS_CD,
 T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
 T.HEDIS_MESR_SK = S.HEDIS_MESR_SK
WHEN NOT MATCHED THEN
 INSERT (HEDIS_MESR_NM,HEDIS_SUB_MESR_NM,HEDIS_MBR_BUCKET_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,HEDIS_MESR_SK)
 VALUES(S.HEDIS_MESR_NM,S.HEDIS_SUB_MESR_NM,S.HEDIS_MBR_BUCKET_ID,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.HEDIS_MESR_SK);
"""
execute_dml(merge_sql_db2_K_HEDIS_Mesr_Load, jdbc_url, jdbc_props)

write_files(
    df_b_table.select(
        "HEDIS_MESR_NM",
        "HEDIS_SUB_MESR_NM",
        "HEDIS_MBR_BUCKET_ID",
        "SRC_SYS_CD"
    ),
    f"{adls_path}/load/B_HEDIS_MESR.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_na_2 = df_na.select(
    "HEDIS_MESR_SK",
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "HEDIS_MBR_BUCKET_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)
df_unk_2 = df_unk.select(
    "HEDIS_MESR_SK",
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "HEDIS_MBR_BUCKET_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)
df_all_2 = df_all.select(
    "HEDIS_MESR_SK",
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "HEDIS_MBR_BUCKET_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)
df_Fnl_All = df_na_2.unionByName(df_unk_2).unionByName(df_all_2)

write_files(
    df_Fnl_All.select(
        "HEDIS_MESR_SK",
        "HEDIS_MESR_NM",
        "HEDIS_SUB_MESR_NM",
        "HEDIS_MBR_BUCKET_ID",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ),
    f"{adls_path}/load/HEDIS_MESR.{SrcSysCd}.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)