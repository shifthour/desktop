# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi        07/25/2013          5114                             Movies Data from IDS to  MBRSH_DM_SUB_LVL_AGNT                   IntegrateWrhsDevl     Peter Marshall               10/22/2013

# MAGIC Job Name: IdsDmMbrshDmSubLvlAgntLoad
# MAGIC Read Load File created in the IdsDmMbrshDmSubLvlAgntExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate and  Insert the MBRSH_DM_SUB_LVL_AGNT Data.
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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_SUB_LVL_AGNT_csv_load = StructType([
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("SUB_LVL_AGNT_ROLE_TYP_CD", StringType(), True),
    StructField("EFF_DT", TimestampType(), True),
    StructField("TERM_DT", TimestampType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_seq_MBRSH_DM_SUB_LVL_AGNT_csv_load = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_SUB_LVL_AGNT_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_SUB_LVL_AGNT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_SUB_LVL_AGNT_csv_load.select(
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("AGNT_ID").alias("AGNT_ID"),
    col("SUB_LVL_AGNT_ROLE_TYP_CD").alias("SUB_LVL_AGNT_ROLE_TYP_CD"),
    col("EFF_DT").alias("EFF_DT"),
    col("TERM_DT").alias("TERM_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Odbc_MBRSH_DM_SUB_LVL_AGNT_out = df_cpy_forBuffer.withColumn(
    "CLS_PLN_ID", rpad(col("CLS_PLN_ID"), 255, " ")
).withColumn(
    "AGNT_ID", rpad(col("AGNT_ID"), 255, " ")
).withColumn(
    "SUB_LVL_AGNT_ROLE_TYP_CD", rpad(col("SUB_LVL_AGNT_ROLE_TYP_CD"), 255, " ")
).withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 255, " ")
).withColumn(
    "GRP_ID", rpad(col("GRP_ID"), 255, " ")
).withColumn(
    "SUB_ID", rpad(col("SUB_ID"), 255, " ")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmSubLvlAgntLoad_Odbc_MBRSH_DM_SUB_LVL_AGNT_out_temp",
    jdbc_url,
    jdbc_props
)

df_Odbc_MBRSH_DM_SUB_LVL_AGNT_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmSubLvlAgntLoad_Odbc_MBRSH_DM_SUB_LVL_AGNT_out_temp") \
    .mode("overwrite") \
    .save()

execute_dml(
    f"TRUNCATE TABLE {ClmMartOwner}.MBRSH_DM_SUB_LVL_AGNT",
    jdbc_url,
    jdbc_props
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_SUB_LVL_AGNT AS T
USING STAGING.IdsDmMbrshDmSubLvlAgntLoad_Odbc_MBRSH_DM_SUB_LVL_AGNT_out_temp AS S
ON T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
   AND T.CLS_PLN_ID = S.CLS_PLN_ID
   AND T.AGNT_ID = S.AGNT_ID
   AND T.SUB_LVL_AGNT_ROLE_TYP_CD = S.SUB_LVL_AGNT_ROLE_TYP_CD
   AND T.EFF_DT = S.EFF_DT
   AND T.TERM_DT = S.TERM_DT
   AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.CLS_PLN_ID = S.CLS_PLN_ID,
    T.AGNT_ID = S.AGNT_ID,
    T.SUB_LVL_AGNT_ROLE_TYP_CD = S.SUB_LVL_AGNT_ROLE_TYP_CD,
    T.EFF_DT = S.EFF_DT,
    T.TERM_DT = S.TERM_DT,
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.GRP_ID = S.GRP_ID,
    T.SUB_ID = S.SUB_ID,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (SUB_UNIQ_KEY,CLS_PLN_ID,AGNT_ID,SUB_LVL_AGNT_ROLE_TYP_CD,EFF_DT,TERM_DT,SRC_SYS_CD,GRP_ID,SUB_ID,LAST_UPDT_RUN_CYC_NO,IDS_LAST_UPDT_RUN_CYC_EXCTN_SK)
  VALUES (S.SUB_UNIQ_KEY,S.CLS_PLN_ID,S.AGNT_ID,S.SUB_LVL_AGNT_ROLE_TYP_CD,S.EFF_DT,S.TERM_DT,S.SRC_SYS_CD,S.GRP_ID,S.SUB_ID,S.LAST_UPDT_RUN_CYC_NO,S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

error_schema = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_SUB_LVL_AGNT_csv_rej = spark.createDataFrame([], error_schema).withColumn(
    "ERRORCODE", rpad(col("ERRORCODE"), 255, " ")
).withColumn(
    "ERRORTEXT", rpad(col("ERRORTEXT"), 255, " ")
)

write_files(
    df_seq_MBRSH_DM_SUB_LVL_AGNT_csv_rej,
    f"{adls_path}/load/MBRSH_DM_SUB_LVL_AGNT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)