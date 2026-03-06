# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri           08/05/2013        5114                             Load DM Table MBRSH_DM_FMLY_DRUG_CLM_SUM                   EnterpriseWrhsDevl   Peter Marshall              10/21/2013

# MAGIC Job Name: IdsDmMbrshDmFmlyDrugClmSumLoad
# MAGIC Read Load File created in the IdsDmMbrsDmFmlyDrugClmSumExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate then Insert the MBRSHP_DM_FMLY_DRG_CLM_SUM Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_seq_MBRSHP_DM_FMLY_DRUG_CLM_SUM_load_csv = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CONF_FMLY_MBR_CT", IntegerType(), nullable=False),
    StructField("MNTN_TIER_1_CT", IntegerType(), nullable=False),
    StructField("MNTN_TIER_2_CT", IntegerType(), nullable=False),
    StructField("MNTN_TIER_3_CT", IntegerType(), nullable=False),
    StructField("NON_MNTN_TIER_1_CT", IntegerType(), nullable=False),
    StructField("NON_MNTN_TIER_2_CT", IntegerType(), nullable=False),
    StructField("NON_MNTN_TIER_3_CT", IntegerType(), nullable=False),
    StructField("LAST_PD_DT", TimestampType(), nullable=False),
    StructField("LAST_UPDT_DT", TimestampType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False)
])

df_seq_seq_MBRSHP_DM_FMLY_DRUG_CLM_SUM_load_csv = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("inferSchema", "false")
    .schema(schema_seq_seq_MBRSHP_DM_FMLY_DRUG_CLM_SUM_load_csv)
    .csv(f"{adls_path}/load/MBRSH_DM_FMLY_DRUG_CLM_SUM.dat")
)

df_cpy_forBuffer = df_seq_seq_MBRSHP_DM_FMLY_DRUG_CLM_SUM_load_csv.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CONF_FMLY_MBR_CT").alias("CONF_FMLY_MBR_CT"),
    F.col("MNTN_TIER_1_CT").alias("MNTN_TIER_1_CT"),
    F.col("MNTN_TIER_2_CT").alias("MNTN_TIER_2_CT"),
    F.col("MNTN_TIER_3_CT").alias("MNTN_TIER_3_CT"),
    F.col("NON_MNTN_TIER_1_CT").alias("NON_MNTN_TIER_1_CT"),
    F.col("NON_MNTN_TIER_2_CT").alias("NON_MNTN_TIER_2_CT"),
    F.col("NON_MNTN_TIER_3_CT").alias("NON_MNTN_TIER_3_CT"),
    F.col("LAST_PD_DT").alias("LAST_PD_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmFmlyDrugClmSumLoad_ODBC_MBRSHP_DM_FMLY_DRG_CLM_SUM_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmFmlyDrugClmSumLoad_ODBC_MBRSHP_DM_FMLY_DRG_CLM_SUM_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO {owner}.MBRSH_DM_FMLY_DRUG_CLM_SUM AS T
USING STAGING.IdsDmMbrshDmFmlyDrugClmSumLoad_ODBC_MBRSHP_DM_FMLY_DRG_CLM_SUM_out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.CONF_FMLY_MBR_CT = S.CONF_FMLY_MBR_CT,
    T.MNTN_TIER_1_CT = S.MNTN_TIER_1_CT,
    T.MNTN_TIER_2_CT = S.MNTN_TIER_2_CT,
    T.MNTN_TIER_3_CT = S.MNTN_TIER_3_CT,
    T.NON_MNTN_TIER_1_CT = S.NON_MNTN_TIER_1_CT,
    T.NON_MNTN_TIER_2_CT = S.NON_MNTN_TIER_2_CT,
    T.NON_MNTN_TIER_3_CT = S.NON_MNTN_TIER_3_CT,
    T.LAST_PD_DT = S.LAST_PD_DT,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    SUB_UNIQ_KEY,
    CONF_FMLY_MBR_CT,
    MNTN_TIER_1_CT,
    MNTN_TIER_2_CT,
    MNTN_TIER_3_CT,
    NON_MNTN_TIER_1_CT,
    NON_MNTN_TIER_2_CT,
    NON_MNTN_TIER_3_CT,
    LAST_PD_DT,
    LAST_UPDT_DT,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.SUB_UNIQ_KEY,
    S.CONF_FMLY_MBR_CT,
    S.MNTN_TIER_1_CT,
    S.MNTN_TIER_2_CT,
    S.MNTN_TIER_3_CT,
    S.NON_MNTN_TIER_1_CT,
    S.NON_MNTN_TIER_2_CT,
    S.NON_MNTN_TIER_3_CT,
    S.LAST_PD_DT,
    S.LAST_UPDT_DT,
    S.LAST_UPDT_RUN_CYC_NO
  );
""".format(owner=ClmMartOwner)

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=True),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=True),
    StructField("CONF_FMLY_MBR_CT", IntegerType(), nullable=True),
    StructField("MNTN_TIER_1_CT", IntegerType(), nullable=True),
    StructField("MNTN_TIER_2_CT", IntegerType(), nullable=True),
    StructField("MNTN_TIER_3_CT", IntegerType(), nullable=True),
    StructField("NON_MNTN_TIER_1_CT", IntegerType(), nullable=True),
    StructField("NON_MNTN_TIER_2_CT", IntegerType(), nullable=True),
    StructField("NON_MNTN_TIER_3_CT", IntegerType(), nullable=True),
    StructField("LAST_PD_DT", TimestampType(), nullable=True),
    StructField("LAST_UPDT_DT", TimestampType(), nullable=True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=True),
    StructField("ERRORCODE", StringType(), nullable=True),
    StructField("ERRORTEXT", StringType(), nullable=True)
])

df_seq_PROD_DM_FMLY_DRG_CLM_SUM_csv_rej = spark.createDataFrame([], schema_reject)

df_seq_PROD_DM_FMLY_DRG_CLM_SUM_csv_rej = df_seq_PROD_DM_FMLY_DRG_CLM_SUM_csv_rej.withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" "))
).withColumn(
    "ERRORCODE", F.rpad(F.col("ERRORCODE"), F.lit(<...>), F.lit(" "))
).withColumn(
    "ERRORTEXT", F.rpad(F.col("ERRORTEXT"), F.lit(<...>), F.lit(" "))
)

df_seq_PROD_DM_FMLY_DRG_CLM_SUM_csv_rej = df_seq_PROD_DM_FMLY_DRG_CLM_SUM_csv_rej.select(
    "SRC_SYS_CD",
    "SUB_UNIQ_KEY",
    "CONF_FMLY_MBR_CT",
    "MNTN_TIER_1_CT",
    "MNTN_TIER_2_CT",
    "MNTN_TIER_3_CT",
    "NON_MNTN_TIER_1_CT",
    "NON_MNTN_TIER_2_CT",
    "NON_MNTN_TIER_3_CT",
    "LAST_PD_DT",
    "LAST_UPDT_DT",
    "LAST_UPDT_RUN_CYC_NO",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_seq_PROD_DM_FMLY_DRG_CLM_SUM_csv_rej,
    f"{adls_path}/load/MBRSH_DM_FMLY_DRUG_CLM_SUM_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)