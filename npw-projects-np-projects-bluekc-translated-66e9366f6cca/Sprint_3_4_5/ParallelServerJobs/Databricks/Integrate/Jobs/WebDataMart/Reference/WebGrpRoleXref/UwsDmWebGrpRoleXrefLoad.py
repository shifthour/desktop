# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela        3/28/14              5114                             Original Programming                                                                            IntegrateWrhsDevl      
# MAGIC 
# MAGIC Pooja Sunkara           07/30/2014         5345                           Added After job sub-routine call with the load file name.                        IntegrateNewDevl         Jag Yelavarthi           2014-08-01

# MAGIC ROLE_ID coulmn has uniqueidentifier as the datatype and the connector stage was  throwing warnings so had to use Enterprise Stage instead of a connector stage
# MAGIC Job Name: UwsDmWebGrpRoleXrefLoad
# MAGIC Read Load File created in the UwsDmWebGrpRoleXrefExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the WEB_GRP_ROLE_XREF Data.
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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_WEB_GRP_ROLE_XREF_csv_load = StructType([
    StructField("ROLE_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_UNIQ_KEY", IntegerType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DT_SK", StringType(), False)
])

df_seq_WEB_GRP_ROLE_XREF_csv_load = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_WEB_GRP_ROLE_XREF_csv_load)
    .csv(f"{adls_path}/load/WEB_GRP_ROLE_XREF.dat")
)

df_cpy_forBuffer = df_seq_WEB_GRP_ROLE_XREF_csv_load.select(
    col("ROLE_ID").alias("ROLE_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.UwsDmWebGrpRoleXrefLoad_ODBC_WEB_GRP_ROLE_XREF_temp", jdbc_url_clmmart, jdbc_props_clmmart)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.UwsDmWebGrpRoleXrefLoad_ODBC_WEB_GRP_ROLE_XREF_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.WEB_GRP_ROLE_XREF AS T
USING STAGING.UwsDmWebGrpRoleXrefLoad_ODBC_WEB_GRP_ROLE_XREF_temp AS S
ON T.ROLE_ID = S.ROLE_ID AND T.GRP_ID = S.GRP_ID
WHEN MATCHED THEN UPDATE SET
  T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
  T.USER_ID = S.USER_ID,
  T.LAST_UPDT_DT_SK = S.LAST_UPDT_DT_SK
WHEN NOT MATCHED THEN
  INSERT (ROLE_ID, GRP_ID, GRP_UNIQ_KEY, USER_ID, LAST_UPDT_DT_SK)
  VALUES (S.ROLE_ID, S.GRP_ID, S.GRP_UNIQ_KEY, S.USER_ID, S.LAST_UPDT_DT_SK);
"""
execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)

df_odbc_WEB_GRP_ROLE_XREF_rej = df_cpy_forBuffer.select(
    col("ROLE_ID"),
    col("GRP_ID"),
    col("GRP_UNIQ_KEY"),
    col("USER_ID"),
    col("LAST_UPDT_DT_SK"),
    RejectReasonCode().alias("sqlcode")
)

df_odbc_WEB_GRP_ROLE_XREF_rej_final = (
    df_odbc_WEB_GRP_ROLE_XREF_rej
    .withColumn("ROLE_ID", rpad("ROLE_ID", <...>, " "))
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("USER_ID", rpad("USER_ID", <...>, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad("LAST_UPDT_DT_SK", 10, " "))
    .select("ROLE_ID", "GRP_ID", "GRP_UNIQ_KEY", "USER_ID", "LAST_UPDT_DT_SK", "sqlcode")
)

write_files(
    df_odbc_WEB_GRP_ROLE_XREF_rej_final,
    f"{adls_path}/load/WEB_GRP_ROLE_XREF_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)