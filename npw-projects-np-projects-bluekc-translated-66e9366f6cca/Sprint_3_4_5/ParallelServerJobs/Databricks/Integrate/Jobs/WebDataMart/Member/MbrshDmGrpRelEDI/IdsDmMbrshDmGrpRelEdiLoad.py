# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri               07/17/2013        5114                             Load DM Table MBRSH_DM_GRP_REL_EDI                                IntegrateWrhsDevl    Peter Marshall               10/21/2013   
# MAGIC 
# MAGIC Pooja Sunkara              07/30/2014        5345                             Corrected naming of Load file and Reject file to 
# MAGIC                                                                                                      MBRSH_DM_GRP_REL_EDI.dat,                                                    IntegrateNewDevl         Jag Yelavarthi            2014-08-01
# MAGIC                                                                                                      MBRSH_DM_GRP_REL_EDI_Rej.dat

# MAGIC Job Name: IdsDmMbrshpRelEdiLoad
# MAGIC Read Load File created in the IdsDmMbrshpRelEdiExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_GRP_REL_EDI Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

ClmMartOwner = get_widget_value('ClmMartOwner','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

schema_seq_MBRSH_DM_GRP_REL_EDI_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_REL_ENTY_ID", StringType(), False),
    StructField("GRP_REL_ENTY_TYP_CD", StringType(), False),
    StructField("EFF_DT", TimestampType(), False),
    StructField("TERM_DT", TimestampType(), False),
    StructField("GRP_REL_ENTY_NM", StringType(), True),
    StructField("GRP_REL_ENTY_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_MBRSH_DM_GRP_REL_EDI_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_GRP_REL_EDI_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_GRP_REL_EDI.dat")
)

df_cpy_forBuffer = (
    df_seq_MBRSH_DM_GRP_REL_EDI_csv_load
    .repartition(["SRC_SYS_CD","GRP_ID","GRP_REL_ENTY_ID","GRP_REL_ENTY_TYP_CD","EFF_DT"])
    .sortWithinPartitions("SRC_SYS_CD","GRP_ID","GRP_REL_ENTY_ID","GRP_REL_ENTY_TYP_CD","EFF_DT")
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("GRP_ID").alias("GRP_ID"),
        col("GRP_REL_ENTY_ID").alias("GRP_REL_ENTY_ID"),
        col("GRP_REL_ENTY_TYP_CD").alias("GRP_REL_ENTY_TYP_CD"),
        col("EFF_DT").alias("EFF_DT"),
        col("TERM_DT").alias("TERM_DT"),
        col("GRP_REL_ENTY_NM").alias("GRP_REL_ENTY_NM"),
        col("GRP_REL_ENTY_TYP_NM").alias("GRP_REL_ENTY_TYP_NM"),
        col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
    )
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmGrpRelEdiLoad_ODBC_MBRSH_DM_GRP_REL_EDI_out_temp", jdbc_url_clmmart, jdbc_props_clmmart)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsDmMbrshDmGrpRelEdiLoad_ODBC_MBRSH_DM_GRP_REL_EDI_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_GRP_REL_EDI AS T
USING STAGING.IdsDmMbrshDmGrpRelEdiLoad_ODBC_MBRSH_DM_GRP_REL_EDI_out_temp AS S
ON (
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.GRP_ID = S.GRP_ID
    AND T.GRP_REL_ENTY_ID = S.GRP_REL_ENTY_ID
    AND T.GRP_REL_ENTY_TYP_CD = S.GRP_REL_ENTY_TYP_CD
    AND T.EFF_DT = S.EFF_DT
)
WHEN MATCHED THEN
  UPDATE SET
    T.TERM_DT = S.TERM_DT,
    T.GRP_REL_ENTY_NM = S.GRP_REL_ENTY_NM,
    T.GRP_REL_ENTY_TYP_NM = S.GRP_REL_ENTY_TYP_NM,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    GRP_ID,
    GRP_REL_ENTY_ID,
    GRP_REL_ENTY_TYP_CD,
    EFF_DT,
    TERM_DT,
    GRP_REL_ENTY_NM,
    GRP_REL_ENTY_TYP_NM,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.GRP_ID,
    S.GRP_REL_ENTY_ID,
    S.GRP_REL_ENTY_TYP_CD,
    S.EFF_DT,
    S.TERM_DT,
    S.GRP_REL_ENTY_NM,
    S.GRP_REL_ENTY_TYP_NM,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""
execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)

df_rej_schema = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_REL_ENTY_ID", StringType(), True),
    StructField("GRP_REL_ENTY_TYP_CD", StringType(), True),
    StructField("EFF_DT", TimestampType(), True),
    StructField("TERM_DT", TimestampType(), True),
    StructField("GRP_REL_ENTY_NM", StringType(), True),
    StructField("GRP_REL_ENTY_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_reject = spark.createDataFrame([], df_rej_schema)

df_reject = (
    df_reject
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("GRP_REL_ENTY_ID", rpad("GRP_REL_ENTY_ID", <...>, " "))
    .withColumn("GRP_REL_ENTY_TYP_CD", rpad("GRP_REL_ENTY_TYP_CD", <...>, " "))
    .withColumn("GRP_REL_ENTY_NM", rpad("GRP_REL_ENTY_NM", <...>, " "))
    .withColumn("GRP_REL_ENTY_TYP_NM", rpad("GRP_REL_ENTY_TYP_NM", <...>, " "))
    .withColumn("ERRORCODE", rpad("ERRORCODE", <...>, " "))
    .withColumn("ERRORTEXT", rpad("ERRORTEXT", <...>, " "))
)

df_reject_final = df_reject.select(
    "SRC_SYS_CD",
    "GRP_ID",
    "GRP_REL_ENTY_ID",
    "GRP_REL_ENTY_TYP_CD",
    "EFF_DT",
    "TERM_DT",
    "GRP_REL_ENTY_NM",
    "GRP_REL_ENTY_TYP_NM",
    "LAST_UPDT_RUN_CYC_NO",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_reject_final,
    f"{adls_path}/load/MBRSH_DM_GRP_REL_EDI_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)