# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  OPTUMRX_IDS_EDW_DRUG_CLM_CMPND_INGR_WKLY_000
# MAGIC 
# MAGIC CALLED BY:  OptumIDSCompoundsSeq
# MAGIC 
# MAGIC JOB NAME:  OptumIDSCompoundsNDCPkey
# MAGIC 
# MAGIC Description:  Job checks the NDC_SK for an existing primary key in the hash file and if not found creates a surrogate key for next job to load the key and code into the NDC table. In other words if the NDC code is not found a new row will be added to the hash file and a record will be written for loading the NDC table in the next job. 
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer            Date            Project/User Story             Change Description                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------   -----------------   ----------------------------------------  --------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Bill Schroeder      2020-11-09  F-209426/US217375         Copied logic from Shared Container DrugClmPK.                    IntegrateDev2    Jaideep Mankala      12/19/2020

# MAGIC If primary key found, assign surragote key, otherwise get next key and update hash file.
# MAGIC If NDC primary key not found, assign surragote key and create crf file for Fkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when, lit, length, trim, regexp_replace, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve job parameters
RunCycle = get_widget_value('RunCycle','')

# Read from dummy table instead of hashed file "hf_ndc"
jdbc_url, jdbc_props = get_db_config("ids_secret_name")
df_hf_NDC_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT NDC_CD, CRT_RUN_CYC_EXCTN_SK, NDC_CD_SK FROM dummy_hf_ndc")
    .load()
)

# Read CSeqFileStage "Drug_Clm_Cmpnd_NDC_PKey"
schema_Drug_Clm_Cmpnd_NDC_PKey = StructType([
    StructField("NDC_SK", IntegerType(), False),
    StructField("NDC", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("AHFS_TCC_SK", IntegerType(), False),
    StructField("DOSE_FORM_SK", IntegerType(), False),
    StructField("TCC_SK", IntegerType(), False),
    StructField("NDC_DSM_DRUG_TYP_CD_SK", IntegerType(), False),
    StructField("NDC_DRUG_ABUSE_CTL_CD_SK", IntegerType(), False),
    StructField("NDC_DRUG_CLS_CD_SK", IntegerType(), False),
    StructField("NDC_DRUG_FORM_CD_SK", IntegerType(), False),
    StructField("NDC_FMT_CD_SK", IntegerType(), False),
    StructField("NDC_GNRC_MNFCTR_CD_SK", IntegerType(), False),
    StructField("NDC_GNRC_NMD_DRUG_CD_SK", IntegerType(), False),
    StructField("NDC_GNRC_PRICE_CD_SK", IntegerType(), False),
    StructField("NDC_GNRC_PRICE_SPREAD_CD_SK", IntegerType(), False),
    StructField("NDC_ORANGE_BOOK_CD_SK", IntegerType(), False),
    StructField("NDC_RTE_TYP_CD_SK", IntegerType(), False),
    StructField("CLM_TRANS_ADD_IN", StringType(), False),
    StructField("DESI_DRUG_IN", StringType(), False),
    StructField("DRUG_MNTN_IN", StringType(), False),
    StructField("INNVTR_IN", StringType(), False),
    StructField("INSTUT_PROD_IN", StringType(), False),
    StructField("PRIV_LBLR_IN", StringType(), False),
    StructField("SNGL_SRC_IN", StringType(), False),
    StructField("UNIT_DOSE_IN", StringType(), False),
    StructField("UNIT_OF_USE_IN", StringType(), False),
    StructField("AVG_WHLSL_PRICE_CHG_DT_SK", StringType(), False),
    StructField("GNRC_PRICE_IN_CHG_DT_SK", StringType(), False),
    StructField("OBSLT_DT_SK", StringType(), False),
    StructField("SRC_NDC_CRT_DT_SK", StringType(), False),
    StructField("SRC_NDC_UPDT_DT_SK", StringType(), False)
])

df_Drug_Clm_Cmpnd_NDC_PKey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "^")
    .option("quote", "000")
    .schema(schema_Drug_Clm_Cmpnd_NDC_PKey)
    .load(f"{adls_path}/key/DRUG_CLM_CMPND_NDC_PKey.dat")
)

# Join logic in Transformer "PkeyNDC"
df_enriched = (
    df_Drug_Clm_Cmpnd_NDC_PKey.alias("Key")
    .join(
        df_hf_NDC_lookup.alias("lnkNDC"),
        [
            col("Key.NDC") == col("lnkNDC.NDC_CD"),
            col("Key.CRT_RUN_CYC_EXCTN_SK") == col("lnkNDC.CRT_RUN_CYC_EXCTN_SK")
        ],
        "left"
    )
)

# Add a column for RunCycle so it can be referenced in expressions
df_enriched = df_enriched.withColumn("RunCycle", lit(RunCycle))

# Stage Variable: NDCCheck
df_enriched = df_enriched.withColumn(
    "NDCCheck",
    when(
        (length(trim(regexp_replace(col("Key.NDC"), "0", " "))) == 0) | (col("Key.NDC") == lit("UNK")),
        lit(1)
    ).otherwise(lit(2))
)

# Stage Variable: NDCSk (set null if we need a new surrogate, or use 0 if NDCCheck=1, else use existing)
df_enriched = df_enriched.withColumn(
    "NDCSk",
    when(col("NDCCheck") == lit(1), lit(0))
    .when(col("lnkNDC.NDC_CD_SK").isNull(), lit(None))
    .otherwise(col("lnkNDC.NDC_CD_SK"))
)

# SurrogateKeyGen for rows where NDCSk is null (KeyMgtGetNextValueConcurrent translation)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"NDCSk",<schema>,<secret_name>)

# Stage Variable: RecycleSk
df_enriched = df_enriched.withColumn(
    "RecycleSk",
    when(col("lnkNDC.NDC_CD_SK").isNull(), col("RunCycle")).otherwise(col("lnkNDC.CRT_RUN_CYC_EXCTN_SK"))
)

# Output link "NDCInsert" => filter + select
df_NDCInsert = df_enriched.filter(
    (col("NDCSk") != lit(0)) &
    (col("lnkNDC.NDC_CD_SK").isNull()) &
    (length(trim(col("Key.NDC"))) == 11)
).select(
    trim(col("Key.NDC")).alias("NDC_CD"),
    col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("NDCSk").alias("NDC_CD_SK")
)

# Write back to the same dummy table (Scenario B), we do a merge into dummy_hf_ndc
# 1) Create STAGING table
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.OptumIDSCompoundsNDCPkey_hf_NDC_insert_temp",
    jdbc_url,
    jdbc_props
)

df_NDCInsert.write \
    .mode("overwrite") \
    .jdbc(jdbc_url, "STAGING.OptumIDSCompoundsNDCPkey_hf_NDC_insert_temp", properties=jdbc_props)

# 2) Merge statement
merge_sql = """
MERGE INTO dummy_hf_ndc AS T
USING STAGING.OptumIDSCompoundsNDCPkey_hf_NDC_insert_temp AS S
ON T.NDC_CD = S.NDC_CD
WHEN MATCHED THEN
  UPDATE SET T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
             T.NDC_CD_SK = S.NDC_CD_SK
WHEN NOT MATCHED THEN
  INSERT (NDC_CD, CRT_RUN_CYC_EXCTN_SK, NDC_CD_SK)
  VALUES (S.NDC_CD, S.CRT_RUN_CYC_EXCTN_SK, S.NDC_CD_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Output link "NDC" => filter + select => final file write "Drug_Clm_Cmpnd_NDC_Load"
df_NDC_for_output = df_enriched.filter(
    (col("NDCSk") != lit(0)) &
    (col("lnkNDC.NDC_CD_SK").isNull()) &
    (length(trim(col("Key.NDC"))) == 11)
).select(
    col("NDCSk").alias("NDC_SK"),
    col("Key.NDC").alias("NDC"),
    col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Key.AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    col("Key.DOSE_FORM_SK").alias("DOSE_FORM_SK"),
    col("Key.TCC_SK").alias("TCC_SK"),
    col("Key.NDC_DSM_DRUG_TYP_CD_SK").alias("NDC_DSM_DRUG_TYP_CD_SK"),
    col("Key.NDC_DRUG_ABUSE_CTL_CD_SK").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    col("Key.NDC_DRUG_CLS_CD_SK").alias("NDC_DRUG_CLS_CD_SK"),
    col("Key.NDC_DRUG_FORM_CD_SK").alias("NDC_DRUG_FORM_CD_SK"),
    col("Key.NDC_FMT_CD_SK").alias("NDC_FMT_CD_SK"),
    col("Key.NDC_GNRC_MNFCTR_CD_SK").alias("NDC_GNRC_MNFCTR_CD_SK"),
    col("Key.NDC_GNRC_NMD_DRUG_CD_SK").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    col("Key.NDC_GNRC_PRICE_CD_SK").alias("NDC_GNRC_PRICE_CD_SK"),
    col("Key.NDC_GNRC_PRICE_SPREAD_CD_SK").alias("NDC_GNRC_PRICE_SPREAD_CD_SK"),
    col("Key.NDC_ORANGE_BOOK_CD_SK").alias("NDC_ORANGE_BOOK_CD_SK"),
    col("Key.NDC_RTE_TYP_CD_SK").alias("NDC_RTE_TYP_CD_SK"),
    col("Key.CLM_TRANS_ADD_IN").alias("CLM_TRANS_ADD_IN"),
    col("Key.DESI_DRUG_IN").alias("DESI_DRUG_IN"),
    col("Key.DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    col("Key.INNVTR_IN").alias("INNVTR_IN"),
    col("Key.INSTUT_PROD_IN").alias("INSTUT_PROD_IN"),
    col("Key.PRIV_LBLR_IN").alias("PRIV_LBLR_IN"),
    col("Key.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    col("Key.UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    col("Key.UNIT_OF_USE_IN").alias("UNIT_OF_USE_IN"),
    col("Key.AVG_WHLSL_PRICE_CHG_DT_SK").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    col("Key.GNRC_PRICE_IN_CHG_DT_SK").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    col("Key.OBSLT_DT_SK").alias("OBSLT_DT_SK"),
    col("Key.SRC_NDC_CRT_DT_SK").alias("SRC_NDC_CRT_DT_SK"),
    col("Key.SRC_NDC_UPDT_DT_SK").alias("SRC_NDC_UPDT_DT_SK")
)

# Apply rpad to char columns
df_NDC_for_output = df_NDC_for_output \
    .withColumn("CLM_TRANS_ADD_IN", rpad(col("CLM_TRANS_ADD_IN"), 1, " ")) \
    .withColumn("DESI_DRUG_IN", rpad(col("DESI_DRUG_IN"), 1, " ")) \
    .withColumn("DRUG_MNTN_IN", rpad(col("DRUG_MNTN_IN"), 1, " ")) \
    .withColumn("INNVTR_IN", rpad(col("INNVTR_IN"), 1, " ")) \
    .withColumn("INSTUT_PROD_IN", rpad(col("INSTUT_PROD_IN"), 1, " ")) \
    .withColumn("PRIV_LBLR_IN", rpad(col("PRIV_LBLR_IN"), 1, " ")) \
    .withColumn("SNGL_SRC_IN", rpad(col("SNGL_SRC_IN"), 1, " ")) \
    .withColumn("UNIT_DOSE_IN", rpad(col("UNIT_DOSE_IN"), 1, " ")) \
    .withColumn("UNIT_OF_USE_IN", rpad(col("UNIT_OF_USE_IN"), 1, " ")) \
    .withColumn("AVG_WHLSL_PRICE_CHG_DT_SK", rpad(col("AVG_WHLSL_PRICE_CHG_DT_SK"), 10, " ")) \
    .withColumn("GNRC_PRICE_IN_CHG_DT_SK", rpad(col("GNRC_PRICE_IN_CHG_DT_SK"), 10, " ")) \
    .withColumn("OBSLT_DT_SK", rpad(col("OBSLT_DT_SK"), 10, " ")) \
    .withColumn("SRC_NDC_CRT_DT_SK", rpad(col("SRC_NDC_CRT_DT_SK"), 10, " ")) \
    .withColumn("SRC_NDC_UPDT_DT_SK", rpad(col("SRC_NDC_UPDT_DT_SK"), 10, " "))

# Write final file: "Drug_Clm_Cmpnd_NDC_Load"
write_files(
    df_NDC_for_output,
    f"{adls_path}/load/DRUG_CLM_CMPND_NDC.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)