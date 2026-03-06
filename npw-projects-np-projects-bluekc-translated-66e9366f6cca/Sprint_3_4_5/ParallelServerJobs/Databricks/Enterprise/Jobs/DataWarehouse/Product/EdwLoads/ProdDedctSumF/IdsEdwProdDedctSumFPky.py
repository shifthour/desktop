# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                    DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                         ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------                      -------------------------------    ------------------------------       --------------------
# MAGIC Leandrew Moore        6/11/2013             P5114                        rewrite in parallel                      Enterprisewarehouse    Pete Marshall              2013-08-08

# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: IdsEdwProddEDCTSumFPky
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWDB = get_widget_value('EDWDB','')
EDWAcct = get_widget_value('EDWAcct','')
EDWPW = get_widget_value('EDWPW','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# db2_KProdDedctSumFRead
df_db2_KProdDedctSumFRead = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD,PROD_ID,PROD_CMPNT_EFF_DT_SK,CRT_RUN_CYC_EXCTN_DT_SK,PROD_DEDCT_SUM_SK,CRT_RUN_CYC_EXCTN_SK FROM {EDWOwner}.K_PROD_DEDCT_SUM_F")
    .load()
)

# ds_ProdDedctSumFExtr (PxDataSet => read from parquet)
df_ds_ProdDedctSumFExtr = spark.read.parquet(f"{adls_path}/ds/PROD_DEDCT_SUM_F.parquet")

# cpy_MultiStreams
df_cpy_MultiStreams = df_ds_ProdDedctSumFExtr
df_cpy_MultiStreams_1 = df_cpy_MultiStreams.select(
    "SRC_SYS_CD",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK",
    "PROD_SK",
    "PROD_CMPNT_TERM_DT_SK",
    "DRUG_GNRC_DEDCT_AMT",
    "DRUG_NM_BRND_DEDCT_AMT",
    "DRUG_PRFRD_DEDCT_AMT",
    "DRUG_NPRFR_DEDCT_AMT",
    "FMLY_DRUG_DEDCT_AMT",
    "FMLY_IN_NTWK_DEDCT_AMT",
    "FMLY_OUT_NTWK_DEDCT_AMT",
    "INDV_DRUG_DEDCT_AMT",
    "INDV_IN_NTWK_DEDCT_AMT",
    "INDV_OUT_NTWK_DEDCT_AMT",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)
df_cpy_MultiStreams_2 = df_cpy_MultiStreams.select(
    "SRC_SYS_CD",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK"
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_2,
    partition_cols=["SRC_SYS_CD", "PROD_ID", "PROD_CMPNT_EFF_DT_SK"],
    sort_cols=[]
).select(
    "SRC_SYS_CD",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK"
)

# jn_ProdDedctSumF (left join of df_rdp_NaturalKeys and df_db2_KProdDedctSumFRead)
df_jn_ProdDedctSumF = df_rdp_NaturalKeys.alias("left").join(
    df_db2_KProdDedctSumFRead.alias("right"),
    on=[
        F.col("left.SRC_SYS_CD") == F.col("right.SRC_SYS_CD"),
        F.col("left.PROD_ID") == F.col("right.PROD_ID"),
        F.col("left.PROD_CMPNT_EFF_DT_SK") == F.col("right.PROD_CMPNT_EFF_DT_SK")
    ],
    how="left"
).select(
    F.col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("left.PROD_ID").alias("PROD_ID"),
    F.col("left.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("right.PROD_DEDCT_SUM_SK").alias("PROD_DEDCT_SUM_SK"),
    F.col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# xfm_PKEYgen
df_temp = df_jn_ProdDedctSumF.withColumn("was_null", F.col("PROD_DEDCT_SUM_SK").isNull())
df_enriched = df_temp
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROD_DEDCT_SUM_SK",<schema>,<secret_name>)

# Split output links based on constraint
df_lnklInsKProdCDedctSumFOut = df_enriched.filter(F.col("was_null") == True).select(
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_DEDCT_SUM_SK").alias("PROD_DEDCT_SUM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("PROD_DEDCT_SUM_SK").alias("PROD_DEDCT_SUM_SK"),
    F.when(F.col("was_null"), EDWRunCycleDate).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("was_null"), EDWRunCycle).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

# db2_KProdDedctSumFLoad => merge logic
temp_table_db2_KProdDedctSumFLoad = "STAGING.IdsEdwProdDedctSumFPky_db2_KProdDedctSumFLoad_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_KProdDedctSumFLoad}", jdbc_url, jdbc_props)
df_lnklInsKProdCDedctSumFOut.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_db2_KProdDedctSumFLoad) \
    .mode("overwrite") \
    .save()

merge_sql_db2_KProdDedctSumFLoad = f"""
MERGE INTO {EDWOwner}.K_PROD_DEDCT_SUM_F AS T
USING {temp_table_db2_KProdDedctSumFLoad} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PROD_ID = S.PROD_ID
    AND T.PROD_CMPNT_EFF_DT_SK = S.PROD_CMPNT_EFF_DT_SK
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
        T.PROD_DEDCT_SUM_SK = S.PROD_DEDCT_SUM_SK,
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
    INSERT
        (PROD_ID, SRC_SYS_CD, PROD_CMPNT_EFF_DT_SK, CRT_RUN_CYC_EXCTN_DT_SK, PROD_DEDCT_SUM_SK, CRT_RUN_CYC_EXCTN_SK)
    VALUES
        (S.PROD_ID, S.SRC_SYS_CD, S.PROD_CMPNT_EFF_DT_SK, S.CRT_RUN_CYC_EXCTN_DT_SK, S.PROD_DEDCT_SUM_SK, S.CRT_RUN_CYC_EXCTN_SK)
;
"""
execute_dml(merge_sql_db2_KProdDedctSumFLoad, jdbc_url, jdbc_props)

# jn_PKEYs
df_jn_PKEYs = df_cpy_MultiStreams_1.alias("left").join(
    df_lnkPKEYxfmOut.alias("right"),
    on=[
        F.col("left.SRC_SYS_CD") == F.col("right.SRC_SYS_CD"),
        F.col("left.PROD_ID") == F.col("right.PROD_ID"),
        F.col("left.PROD_CMPNT_EFF_DT_SK") == F.col("right.PROD_CMPNT_EFF_DT_SK"),
    ],
    how="left"
).select(
    F.col("right.PROD_DEDCT_SUM_SK").alias("PROD_DEDCT_SUM_SK"),
    F.col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("left.PROD_ID").alias("PROD_ID"),
    F.col("left.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("left.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("left.PROD_SK").alias("PROD_SK"),
    F.col("left.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("left.DRUG_GNRC_DEDCT_AMT").alias("DRUG_GNRC_DEDCT_AMT"),
    F.col("left.DRUG_NM_BRND_DEDCT_AMT").alias("DRUG_NM_BRND_DEDCT_AMT"),
    F.col("left.DRUG_PRFRD_DEDCT_AMT").alias("DRUG_PRFRD_DEDCT_AMT"),
    F.col("left.DRUG_NPRFR_DEDCT_AMT").alias("DRUG_NPRFR_DEDCT_AMT"),
    F.col("left.FMLY_DRUG_DEDCT_AMT").alias("FMLY_DRUG_DEDCT_AMT"),
    F.col("left.FMLY_IN_NTWK_DEDCT_AMT").alias("FMLY_IN_NTWK_DEDCT_AMT"),
    F.col("left.FMLY_OUT_NTWK_DEDCT_AMT").alias("FMLY_OUT_NTWK_DEDCT_AMT"),
    F.col("left.INDV_DRUG_DEDCT_AMT").alias("INDV_DRUG_DEDCT_AMT"),
    F.col("left.INDV_IN_NTWK_DEDCT_AMT").alias("INDV_IN_NTWK_DEDCT_AMT"),
    F.col("left.INDV_OUT_NTWK_DEDCT_AMT").alias("INDV_OUT_NTWK_DEDCT_AMT"),
    F.col("right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("left.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

# seq_ProdDedctSumFPKey => PxSequentialFile
df_seq_ProdDedctSumFPKey = (
    df_jn_PKEYs
    .withColumn("PROD_CMPNT_EFF_DT_SK", F.rpad(F.col("PROD_CMPNT_EFF_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PROD_CMPNT_TERM_DT_SK", F.rpad(F.col("PROD_CMPNT_TERM_DT_SK"), 10, " "))
    .select(
        "PROD_DEDCT_SUM_SK",
        "SRC_SYS_CD",
        "PROD_ID",
        "PROD_CMPNT_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PROD_SK",
        "PROD_CMPNT_TERM_DT_SK",
        "DRUG_GNRC_DEDCT_AMT",
        "DRUG_NM_BRND_DEDCT_AMT",
        "DRUG_PRFRD_DEDCT_AMT",
        "DRUG_NPRFR_DEDCT_AMT",
        "FMLY_DRUG_DEDCT_AMT",
        "FMLY_IN_NTWK_DEDCT_AMT",
        "FMLY_OUT_NTWK_DEDCT_AMT",
        "INDV_DRUG_DEDCT_AMT",
        "INDV_IN_NTWK_DEDCT_AMT",
        "INDV_OUT_NTWK_DEDCT_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_seq_ProdDedctSumFPKey,
    f"{adls_path}/load/PROD_DEDCT_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)