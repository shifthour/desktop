# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri         07/28/2013        5114                             Load DM Table MBRSH_DM_MBR_MCARE_EVT                               IntegrateWrhsDevl       
# MAGIC 
# MAGIC Pooja Sunkara         07/30/2014        5345                            Corrected Load file name from MBRSHP_DM_MCARE_EVT.dat             IntegrateNewDevl       Jag Yelavarthi           2014-08-01
# MAGIC                                                                                                to MBRSH_DM_MBR_MCARE_EVT.dat at Sequential File stage 
# MAGIC                                                                                                and After job subroutine

# MAGIC Job Name: IdsDmMbrshDmMbrMcareLoad
# MAGIC Read Load File created in the IdsDmMbrshpDmMbrMcareExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSHP_DM_MCARE_EVT Data.
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

schema_seq_MBRSH_DM_MBR_MCARE_EVT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_MCARE_EVT_CD", StringType(), False),
    StructField("EFF_DT", TimestampType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("TERM_DT", TimestampType(), False),
    StructField("MBR_MCARE_EVT_NM", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_MBRSH_DM_MBR_MCARE_EVT_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("escape", "^")
    .schema(schema_seq_MBRSH_DM_MBR_MCARE_EVT_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_MCARE_EVT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_MCARE_EVT_csv_load.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("MBR_MCARE_EVT_CD"), <...>, " ").alias("MBR_MCARE_EVT_CD"),
    col("EFF_DT").alias("EFF_DT"),
    col("MBR_SK").alias("MBR_SK"),
    col("TERM_DT").alias("TERM_DT"),
    rpad(col("MBR_MCARE_EVT_NM"), <...>, " ").alias("MBR_MCARE_EVT_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrMcareEvtLoad_ODBC_MBRSHP_DM_MCARE_EVT_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrMcareEvtLoad_ODBC_MBRSHP_DM_MCARE_EVT_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_MBR_MCARE_EVT AS T
USING STAGING.IdsDmMbrshDmMbrMcareEvtLoad_ODBC_MBRSHP_DM_MCARE_EVT_out_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.MBR_MCARE_EVT_CD = S.MBR_MCARE_EVT_CD
    AND T.EFF_DT = S.EFF_DT)
WHEN MATCHED THEN
  UPDATE SET
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_MCARE_EVT_CD = S.MBR_MCARE_EVT_CD,
    T.EFF_DT = S.EFF_DT,
    T.MBR_SK = S.MBR_SK,
    T.TERM_DT = S.TERM_DT,
    T.MBR_MCARE_EVT_NM = S.MBR_MCARE_EVT_NM,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    MBR_MCARE_EVT_CD,
    EFF_DT,
    MBR_SK,
    TERM_DT,
    MBR_MCARE_EVT_NM,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.MBR_MCARE_EVT_CD,
    S.EFF_DT,
    S.MBR_SK,
    S.TERM_DT,
    S.MBR_MCARE_EVT_NM,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_MBR_MCARE_EVT_csv_rej = spark.createDataFrame([], schema_rej)

write_files(
    df_seq_MBRSH_DM_MBR_MCARE_EVT_csv_rej,
    f"{adls_path}/load/MBRSH_DM_MBR_MCARE_EVT_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)