# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                           DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------             -------------------------------    ------------------------------       --------------------
# MAGIC Leandrew Moore       5/28/2013           5114                             rewrite in parallel                                                                                          Enterprisewarehouse       Pete Marshall              2013-08-08

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC Table ProdCopaySumF.This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: IdsEdwBillEntyPKey
# MAGIC Notes: 
# MAGIC 1) Configuration file inherits from the project level so there is no need to override it at a job level
# MAGIC 
# MAGIC 2) When joining data, make sure the data is explicitly patitioned and sorted on the KEY columns.
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Table K_prod__copay_sum_F.               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Read from ds_ProdOopSumFExtr (.ds -> .parquet)
df_ds_ProdOopSumFExtr = spark.read.parquet(f"{adls_path}/ds/PROD_OOP_SUM_F.parquet")

# cpy_MultiStreams
df_lnkFullDataJnIn = df_ds_ProdOopSumFExtr.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
    rpad(col("PVC_EFF_DT_SK"), 10, " ").alias("PVC_EFF_DT_SK"),
    rpad(col("PVC_TERM_DT_SK"), 10, " ").alias("PVC_TERM_DT_SK"),
    col("OOP_INDV_IN_NTWK_MAX_AMT").alias("OOP_INDV_IN_NTWK_MAX_AMT"),
    col("OOP_FMLY_IN_NTWK_MAX_AMT").alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
    col("OOP_INDV_OUT_NTWK_MAX_AMT").alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
    col("OOP_FMLY_OUT_NTWK_MAX_AMT").alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
    col("PROD_SK").alias("PROD_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_cpy_NKEY = df_ds_ProdOopSumFExtr.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
    rpad(col("PVC_EFF_DT_SK"), 10, " ").alias("PVC_EFF_DT_SK")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_out = dedup_sort(
    df_lnk_cpy_NKEY,
    partition_cols=["SRC_SYS_CD","PROD_ID","PVC_EFF_DT_SK"],
    sort_cols=[("SRC_SYS_CD","A"),("PROD_ID","A"),("PVC_EFF_DT_SK","A")]
)
df_lnkRemDupDataOut = df_rdp_NaturalKeys_out

# db2_KProdOopSumFExt (Read from EDW)
extract_query = f"SELECT PROD_ID, SRC_SYS_CD, PVC_EFF_DT_SK, PROD_OOP_SUM_SK, CRT_RUN_CYC_EXCTN_DT_SK, CRT_RUN_CYC_EXCTN_SK FROM {EDWOwner}.K_PROD_OOP_SUM_F"
df_db2_KProdOopSumFExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# jn_ProdOopSumF (leftouterjoin)
df_jn_ProdOopSumF = df_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db2_KProdOopSumFExt.alias("lnkKProdOopSumFIExt"),
    on=[
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKProdOopSumFIExt.SRC_SYS_CD"),
        col("lnkRemDupDataOut.PROD_ID") == col("lnkKProdOopSumFIExt.PROD_ID"),
        col("lnkRemDupDataOut.PVC_EFF_DT_SK") == col("lnkKProdOopSumFIExt.PVC_EFF_DT_SK")
    ],
    how="left"
).select(
    col("lnkRemDupDataOut.PROD_ID").alias("PROD_ID"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkRemDupDataOut.PVC_EFF_DT_SK").alias("PVC_EFF_DT_SK"),
    col("lnkKProdOopSumFIExt.PROD_OOP_SUM_SK").alias("PROD_OOP_SUM_SK"),
    rpad(col("lnkKProdOopSumFIExt.CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnkKProdOopSumFIExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# xfm_PKEYgen
df_xfm_in = df_jn_ProdOopSumF.withColumn("IS_NULL_SK", col("PROD_OOP_SUM_SK").isNull())
df_enriched = df_xfm_in
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROD_OOP_SUM_SK",<schema>,<secret_name>)

df_insKProdOopSumFOut = df_enriched.filter(col("IS_NULL_SK") == True).select(
    col("PROD_ID").alias("PROD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PVC_EFF_DT_SK").alias("PVC_EFF_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PROD_OOP_SUM_SK").alias("PROD_OOP_SUM_SK"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    col("PROD_ID").alias("PROD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PVC_EFF_DT_SK").alias("PVC_EFF_DT_SK"),
    col("PROD_OOP_SUM_SK").alias("PROD_OOP_SUM_SK"),
    when(col("IS_NULL_SK"), EDWRunCycleDate).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    when(col("IS_NULL_SK"), EDWRunCycle).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

# db2_ProdOopSumFLoad (merge logic)
temp_table = "STAGING.IdsEdwProdOopSumFPky_db2_ProdOopSumFLoad_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)
df_insKProdOopSumFOut.write.jdbc(url=jdbc_url, table=temp_table, mode="append", properties=jdbc_props)

merge_sql = f"""
MERGE INTO {EDWOwner}.K_PROD_OOP_SUM_F AS T
USING {temp_table} AS S
ON
    T.PROD_ID = S.PROD_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PVC_EFF_DT_SK = S.PVC_EFF_DT_SK
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.PROD_OOP_SUM_SK = S.PROD_OOP_SUM_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
    INSERT (PROD_ID, SRC_SYS_CD, PVC_EFF_DT_SK, CRT_RUN_CYC_EXCTN_DT_SK, PROD_OOP_SUM_SK, CRT_RUN_CYC_EXCTN_SK)
    VALUES (S.PROD_ID, S.SRC_SYS_CD, S.PVC_EFF_DT_SK, S.CRT_RUN_CYC_EXCTN_DT_SK, S.PROD_OOP_SUM_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKEYs (leftouterjoin)
df_jn_PKEYs = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"),
        col("lnkFullDataJnIn.PROD_ID") == col("lnkPKEYxfmOut.PROD_ID"),
        col("lnkFullDataJnIn.PVC_EFF_DT_SK") == col("lnkPKEYxfmOut.PVC_EFF_DT_SK")
    ],
    how="left"
).select(
    col("lnkPKEYxfmOut.PROD_OOP_SUM_SK").alias("PROD_OOP_SUM_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.PROD_ID").alias("PROD_ID"),
    col("lnkFullDataJnIn.PVC_EFF_DT_SK").alias("PVC_EFF_DT_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lnkFullDataJnIn.PVC_TERM_DT_SK").alias("PVC_TERM_DT_SK"),
    col("lnkFullDataJnIn.OOP_INDV_IN_NTWK_MAX_AMT").alias("OOP_INDV_IN_NTWK_MAX_AMT"),
    col("lnkFullDataJnIn.OOP_FMLY_IN_NTWK_MAX_AMT").alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
    col("lnkFullDataJnIn.OOP_INDV_OUT_NTWK_MAX_AMT").alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
    col("lnkFullDataJnIn.OOP_FMLY_OUT_NTWK_MAX_AMT").alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
    col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# seq_ProdOopSumFPKey (write to .dat)
df_final = df_jn_PKEYs.select(
    col("PROD_OOP_SUM_SK").alias("PROD_OOP_SUM_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
    rpad(col("PVC_EFF_DT_SK"), 10, " ").alias("PVC_EFF_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("PVC_TERM_DT_SK"), 10, " ").alias("PVC_TERM_DT_SK"),
    col("OOP_INDV_IN_NTWK_MAX_AMT").alias("OOP_INDV_IN_NTWK_MAX_AMT"),
    col("OOP_FMLY_IN_NTWK_MAX_AMT").alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
    col("OOP_INDV_OUT_NTWK_MAX_AMT").alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
    col("OOP_FMLY_OUT_NTWK_MAX_AMT").alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
    col("PROD_SK").alias("PROD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PROD_OOP_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)