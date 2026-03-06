# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela        3/26/14              5114                             Original Programming                                                                            IntegrateWrhsDevl     Bhoomi Dasari             4/8/2014

# MAGIC Job Name: IdsDmRefAlphaPfxLoad
# MAGIC Read Load File created in the IdsDmRefAlphaPfxExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the REF_DM_ALPHA_PFX Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_REF_DM_ALPHA_PFX_csv_load = StructType([
    StructField("ALPHA_PFX_CD", StringType(), False),
    StructField("ALPHA_PFX_SK", IntegerType(), False),
    StructField("ALPHA_PFX_INSTUT_CLM_PLN_CD", StringType(), False),
    StructField("ALPHA_PFX_PROF_CLM_PLN_CD", StringType(), False),
    StructField("ALPHA_PFX_PGM_CD", StringType(), False),
    StructField("PLN_PROFL_STD_RULE_IN", StringType(), False),
    StructField("ALPHA_PFX_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_REF_DM_ALPHA_PFX_csv_load = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_REF_DM_ALPHA_PFX_csv_load)
    .load(f"{adls_path}/load/REF_DM_ALPHA_PFX.dat")
)

df_cpy_forBuffer = df_seq_REF_DM_ALPHA_PFX_csv_load.select(
    F.col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.col("ALPHA_PFX_INSTUT_CLM_PLN_CD").alias("ALPHA_PFX_INSTUT_CLM_PLN_CD"),
    F.col("ALPHA_PFX_PROF_CLM_PLN_CD").alias("ALPHA_PFX_PROF_CLM_PLN_CD"),
    F.col("ALPHA_PFX_PGM_CD").alias("ALPHA_PFX_PGM_CD"),
    F.col("PLN_PROFL_STD_RULE_IN").alias("PLN_PROFL_STD_RULE_IN"),
    F.col("ALPHA_PFX_NM").alias("ALPHA_PFX_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmRefAlphaPfxLoad_Odbc_REF_DM_ALPHA_PFX_out_temp", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmRefAlphaPfxLoad_Odbc_REF_DM_ALPHA_PFX_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.REF_DM_ALPHA_PFX AS T
USING STAGING.IdsDmRefAlphaPfxLoad_Odbc_REF_DM_ALPHA_PFX_out_temp AS S
ON T.ALPHA_PFX_CD = S.ALPHA_PFX_CD
WHEN MATCHED THEN
  UPDATE SET
    T.ALPHA_PFX_SK = S.ALPHA_PFX_SK,
    T.ALPHA_PFX_INSTUT_CLM_PLN_CD = S.ALPHA_PFX_INSTUT_CLM_PLN_CD,
    T.ALPHA_PFX_PROF_CLM_PLN_CD = S.ALPHA_PFX_PROF_CLM_PLN_CD,
    T.ALPHA_PFX_PGM_CD = S.ALPHA_PFX_PGM_CD,
    T.PLN_PROFL_STD_RULE_IN = S.PLN_PROFL_STD_RULE_IN,
    T.ALPHA_PFX_NM = S.ALPHA_PFX_NM,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    ALPHA_PFX_CD,
    ALPHA_PFX_SK,
    ALPHA_PFX_INSTUT_CLM_PLN_CD,
    ALPHA_PFX_PROF_CLM_PLN_CD,
    ALPHA_PFX_PGM_CD,
    PLN_PROFL_STD_RULE_IN,
    ALPHA_PFX_NM,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.ALPHA_PFX_CD,
    S.ALPHA_PFX_SK,
    S.ALPHA_PFX_INSTUT_CLM_PLN_CD,
    S.ALPHA_PFX_PROF_CLM_PLN_CD,
    S.ALPHA_PFX_PGM_CD,
    S.PLN_PROFL_STD_RULE_IN,
    S.ALPHA_PFX_NM,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Odbc_REF_DM_ALPHA_PFX_out_rej = (
    df_cpy_forBuffer
    .withColumn("ERRORCODE", F.lit(None).cast(StringType()))
    .withColumn("ERRORTEXT", F.lit(None).cast(StringType()))
)

df_final = df_Odbc_REF_DM_ALPHA_PFX_out_rej.select(
    F.rpad(F.col("ALPHA_PFX_CD"), <...>, " ").alias("ALPHA_PFX_CD"),
    F.col("ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.rpad(F.col("ALPHA_PFX_INSTUT_CLM_PLN_CD"), <...>, " ").alias("ALPHA_PFX_INSTUT_CLM_PLN_CD"),
    F.rpad(F.col("ALPHA_PFX_PROF_CLM_PLN_CD"), <...>, " ").alias("ALPHA_PFX_PROF_CLM_PLN_CD"),
    F.rpad(F.col("ALPHA_PFX_PGM_CD"), <...>, " ").alias("ALPHA_PFX_PGM_CD"),
    F.rpad(F.col("PLN_PROFL_STD_RULE_IN"), 1, " ").alias("PLN_PROFL_STD_RULE_IN"),
    F.rpad(F.col("ALPHA_PFX_NM"), <...>, " ").alias("ALPHA_PFX_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_final,
    f"{adls_path}/load/REF_DM_ALPHA_PFX_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)