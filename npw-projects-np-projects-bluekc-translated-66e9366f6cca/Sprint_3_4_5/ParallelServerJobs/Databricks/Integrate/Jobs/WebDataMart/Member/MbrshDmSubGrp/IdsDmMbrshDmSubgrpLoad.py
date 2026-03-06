# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/22/2013        5114                             Load DM Table PROD_DM_BNF_VNDR                                           EnterpriseWrhsDevl     Peter Marshall              10/18/2013     
# MAGIC 
# MAGIC Pooja Sunkara         07/30/2014         5345                            Corrected Reject file name from MBRSH_DM_SUBGRP_Rej.txt 
# MAGIC                                                                                                  to MBRSH_DM_SUBGRP_Rej.dat                                                     IntegrateNewDevl        Jag Yelavarthi               2014-08-01

# MAGIC Job Name: IdsDmMbrshDmSubgrpLoad
# MAGIC Read Load File created in the IdsDmMbrshDmSubgrpExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_SUBGRP Data.
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

schema_seq_MBRSH_DM_SUBGRP_csv_load = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SUBGRP_NM", StringType(), False),
    StructField("SUBGRP_UNIQ_KEY", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])

df_seq_MBRSH_DM_SUBGRP_csv_load = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .schema(schema_seq_MBRSH_DM_SUBGRP_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_SUBGRP.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_SUBGRP_csv_load.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUBGRP_NM").alias("SUBGRP_NM"),
    F.col("SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_cpy_forBuffer_final = df_cpy_forBuffer.select(
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("SUBGRP_ID"), 255, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBGRP_NM"), 255, " ").alias("SUBGRP_NM"),
    F.col("SUBGRP_UNIQ_KEY"),
    F.col("LAST_UPDT_RUN_CYC_NO"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmSubgrpLoad_ODBC_MBRSH_DM_SUBGRP_out_temp", jdbc_url, jdbc_props)

df_cpy_forBuffer_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmSubgrpLoad_ODBC_MBRSH_DM_SUBGRP_out_temp") \
    .mode("overwrite") \
    .save()

truncate_sql = f"TRUNCATE TABLE {ClmMartOwner}.MBRSH_DM_SUBGRP"
execute_dml(truncate_sql, jdbc_url, jdbc_props)

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_SUBGRP AS T
USING STAGING.IdsDmMbrshDmSubgrpLoad_ODBC_MBRSH_DM_SUBGRP_out_temp AS S
ON (T.GRP_ID = S.GRP_ID AND T.SUBGRP_ID = S.SUBGRP_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD)
WHEN MATCHED THEN UPDATE SET
  T.SUBGRP_NM = S.SUBGRP_NM,
  T.SUBGRP_UNIQ_KEY = S.SUBGRP_UNIQ_KEY,
  T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
  T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (GRP_ID, SUBGRP_ID, SRC_SYS_CD, SUBGRP_NM, SUBGRP_UNIQ_KEY, LAST_UPDT_RUN_CYC_NO, IDS_LAST_UPDT_RUN_CYC_EXCTN_SK)
  VALUES (S.GRP_ID, S.SUBGRP_ID, S.SRC_SYS_CD, S.SUBGRP_NM, S.SUBGRP_UNIQ_KEY, S.LAST_UPDT_RUN_CYC_NO, S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK)
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_SUBGRP_csv_rej = spark.createDataFrame([], schema_rej)

df_seq_MBRSH_DM_SUBGRP_csv_rej_final = df_seq_MBRSH_DM_SUBGRP_csv_rej.select(
    F.rpad(F.col("ERRORCODE"), 255, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), 255, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_MBRSH_DM_SUBGRP_csv_rej_final,
    f"{adls_path}/load/MBRSH_DM_SUBGRP_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)