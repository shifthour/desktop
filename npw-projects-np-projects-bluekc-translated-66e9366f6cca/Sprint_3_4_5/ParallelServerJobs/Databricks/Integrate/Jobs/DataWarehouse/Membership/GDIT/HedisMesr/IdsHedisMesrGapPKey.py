# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsHedisMesrPKey
# MAGIC 
# MAGIC Called By: IdsMbrHedisMesrGapCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham Kalidindi             2024-09-10        US-628396                                 Orig Development                                        IntegrateDev2       Reddy Sanam               09-30-2024

# MAGIC Create Primary Key for HEDIS_MESR table
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Table K_HEDIS_MESR_GAP.Insert new SKEY rows generated as part of this run. This is a database insert operation.
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


RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
RunCycExctnSK = get_widget_value('RunCycExctnSK','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')

jdbc_url_db2_K_HEDIS_MESR_GAP_In, jdbc_props_db2_K_HEDIS_MESR_GAP_In = get_db_config(ids_secret_name)
extract_query_db2_K_HEDIS_MESR_GAP_In = (
    "SELECT HEDIS_MESR_GAP_SK,HEDIS_MESR_NM,HEDIS_SUB_MESR_NM,HEDIS_MBR_BUCKET_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK "
    f"FROM {IDSOwner}.K_HEDIS_MESR_GAP"
)
df_db2_K_HEDIS_MESR_GAP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_HEDIS_MESR_GAP_In)
    .options(**jdbc_props_db2_K_HEDIS_MESR_GAP_In)
    .option("query", extract_query_db2_K_HEDIS_MESR_GAP_In)
    .load()
)

schema_HEDIS_MESR_GAP = StructType([
    StructField("HEDIS_MESR_NM", StringType(), True),
    StructField("HEDIS_SUB_MESR_NM", StringType(), True),
    StructField("HEDIS_MBR_BUCKET_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("HEDIS_MESR_ABBR_ID", StringType(), True)
])
df_HEDIS_MESR_GAP = (
    spark.read.format("csv")
    .option("delimiter", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_HEDIS_MESR_GAP)
    .load(f"{adls_path}/verified/K_HEDIS_MESR_GAP.dat")
)

df_cpy_MultiStreams_out_lnkRemDupDataIn = df_HEDIS_MESR_GAP.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_out_lnkFullDataJnIn = df_HEDIS_MESR_GAP.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_out_lnkRemDupDataIn,
    ["HEDIS_MESR_NM","HEDIS_SUB_MESR_NM","HEDIS_MBR_BUCKET_ID","SRC_SYS_CD"],
    []
)

df_jn_K_MESR_GAP = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_HEDIS_MESR_GAP_In.alias("Extr"),
        on=[
            F.col("lnkRemDupDataOut.HEDIS_MESR_NM") == F.col("Extr.HEDIS_MESR_NM"),
            F.col("lnkRemDupDataOut.HEDIS_SUB_MESR_NM") == F.col("Extr.HEDIS_SUB_MESR_NM"),
            F.col("lnkRemDupDataOut.HEDIS_MBR_BUCKET_ID") == F.col("Extr.HEDIS_MBR_BUCKET_ID"),
            F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD")
        ],
        how="left"
    )
    .select(
        F.col("lnkRemDupDataOut.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
        F.col("lnkRemDupDataOut.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
        F.col("lnkRemDupDataOut.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Extr.HEDIS_MESR_GAP_SK").alias("HEDIS_MESR_GAP_SK"),
        F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_enriched = df_jn_K_MESR_GAP.withColumn(
    "svMesrGapSK",
    F.when(
        F.col("HEDIS_MESR_GAP_SK").isNull() | (F.col("HEDIS_MESR_GAP_SK") == 0),
        F.lit(None)
    ).otherwise(F.col("HEDIS_MESR_GAP_SK"))
).withColumn(
    "svRunCyle",
    F.when(
        F.col("HEDIS_MESR_GAP_SK").isNull() | (F.col("HEDIS_MESR_GAP_SK") == 0),
        F.lit(RunCycExctnSK)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svMesrGapSK",<schema>,<secret_name>)

df_New = (
    df_enriched
    .filter((F.col("HEDIS_MESR_GAP_SK").isNull()) | (F.col("HEDIS_MESR_GAP_SK") == 0))
    .select(
        F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
        F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
        F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svMesrGapSK").alias("HEDIS_MESR_GAP_SK")
    )
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycExctnSK).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMesrGapSK").alias("HEDIS_MESR_GAP_SK")
)

jdbc_url_db2_K_HEDIS_MESR_GAP_Load, jdbc_props_db2_K_HEDIS_MESR_GAP_Load = get_db_config(ids_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsHedisMesrGapPKey_db2_K_HEDIS_MESR_GAP_Load_temp",
    jdbc_url_db2_K_HEDIS_MESR_GAP_Load,
    jdbc_props_db2_K_HEDIS_MESR_GAP_Load
)
df_New.write \
    .format("jdbc") \
    .option("url", jdbc_url_db2_K_HEDIS_MESR_GAP_Load) \
    .options(**jdbc_props_db2_K_HEDIS_MESR_GAP_Load) \
    .option("dbtable", "STAGING.IdsHedisMesrGapPKey_db2_K_HEDIS_MESR_GAP_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_HEDIS_MESR_GAP_Load = (
    f"MERGE INTO {IDSOwner}.K_HEDIS_MESR_GAP AS T "
    "USING STAGING.IdsHedisMesrGapPKey_db2_K_HEDIS_MESR_GAP_Load_temp AS S "
    "ON T.HEDIS_MESR_GAP_SK = S.HEDIS_MESR_GAP_SK "
    "WHEN MATCHED THEN UPDATE SET "
    "T.HEDIS_MESR_NM = S.HEDIS_MESR_NM, "
    "T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM, "
    "T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID, "
    "T.SRC_SYS_CD = S.SRC_SYS_CD, "
    "T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK "
    "WHEN NOT MATCHED THEN INSERT "
    "("
    "HEDIS_MESR_GAP_SK, HEDIS_MESR_NM, HEDIS_SUB_MESR_NM, HEDIS_MBR_BUCKET_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK"
    ") VALUES ("
    "S.HEDIS_MESR_GAP_SK, S.HEDIS_MESR_NM, S.HEDIS_SUB_MESR_NM, S.HEDIS_MBR_BUCKET_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK"
    ");"
)
execute_dml(merge_sql_db2_K_HEDIS_MESR_GAP_Load, jdbc_url_db2_K_HEDIS_MESR_GAP_Load, jdbc_props_db2_K_HEDIS_MESR_GAP_Load)

df_jn_PKEYs = (
    df_cpy_MultiStreams_out_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        on=[
            F.col("lnkFullDataJnIn.HEDIS_SUB_MESR_NM") == F.col("lnkPKEYxfmOut.HEDIS_SUB_MESR_NM"),
            F.col("lnkFullDataJnIn.HEDIS_MESR_NM") == F.col("lnkPKEYxfmOut.HEDIS_MESR_NM"),
            F.col("lnkFullDataJnIn.HEDIS_MBR_BUCKET_ID") == F.col("lnkPKEYxfmOut.HEDIS_MBR_BUCKET_ID"),
            F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
        ],
        how="inner"
    )
    .select(
        F.col("lnkFullDataJnIn.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
        F.col("lnkFullDataJnIn.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
        F.col("lnkFullDataJnIn.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkFullDataJnIn.HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
        F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnkPKEYxfmOut.HEDIS_MESR_GAP_SK").alias("HEDIS_MESR_GAP_SK")
    )
)

df_seq_HEDIS_MESR_GAP_Pkey = df_jn_PKEYs.select(
    F.col("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID"),
    F.col("SRC_SYS_CD"),
    F.col("HEDIS_MESR_ABBR_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HEDIS_MESR_GAP_SK")
)
df_seq_HEDIS_MESR_GAP_Pkey = (
    df_seq_HEDIS_MESR_GAP_Pkey
    .withColumn("HEDIS_MESR_NM", F.rpad("HEDIS_MESR_NM", <...>, " "))
    .withColumn("HEDIS_SUB_MESR_NM", F.rpad("HEDIS_SUB_MESR_NM", <...>, " "))
    .withColumn("HEDIS_MBR_BUCKET_ID", F.rpad("HEDIS_MBR_BUCKET_ID", <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("HEDIS_MESR_ABBR_ID", F.rpad("HEDIS_MESR_ABBR_ID", <...>, " "))
)

write_files(
    df_seq_HEDIS_MESR_GAP_Pkey,
    f"{adls_path}/key/HEDIS_MESR_GAP_Fkey.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)