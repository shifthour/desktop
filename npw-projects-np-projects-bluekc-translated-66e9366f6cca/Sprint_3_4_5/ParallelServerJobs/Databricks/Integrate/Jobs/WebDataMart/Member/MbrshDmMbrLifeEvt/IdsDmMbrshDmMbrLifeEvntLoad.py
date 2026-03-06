# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri         07/23/2013        5114                             Load DM Table MBRSHP_DM_MBRLF_EVNT                                 IntegrateWrhsDevl       Peter Marshall               10/21/2013 
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela      08/05/2014    5345                              Change reject file Name                                                                       IntegrateNewDevl         Jag Yelavarthi                2014-08-07

# MAGIC Job Name: IdsDmMbrshDmIndBeLtrLoad
# MAGIC Read Load File created in the IdsDmMbrshpDmMbrLifeEvntExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate then Insert the MBRSHP_DM_MBR_LIF_EVT Data.
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

schema_seq_MBRSHP_DM_MBR_LIFE_EVT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_LIFE_EVT_TYP_CD", StringType(), True),
    StructField("CLM_SVC_STRT_DT", TimestampType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_LIFE_EVT_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_MBRSHP_DM_MBR_LIFE_EVT_csv_load = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("inferSchema", "false")
    .option("nullValue", None)
    .schema(schema_seq_MBRSHP_DM_MBR_LIFE_EVT_csv_load)
    .csv(f"{adls_path}/load/MBRSHP_DM_MBRLF_EVNT.dat")
)

df_cpy_forBuffer = df_seq_MBRSHP_DM_MBR_LIFE_EVT_csv_load.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("MBR_LIFE_EVT_TYP_CD"), <...>, " ").alias("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    rpad(col("MBR_LIFE_EVT_TYP_NM"), <...>, " ").alias("MBR_LIFE_EVT_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrLifeEvntLoad_ODBC_MBRSHP_DM_MBR_LIF_EVT_out_temp", jdbc_url, jdbc_props)

(
    df_cpy_forBuffer.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrLifeEvntLoad_ODBC_MBRSHP_DM_MBR_LIF_EVT_out_temp")
    .mode("append")
    .save()
)

merge_sql = """
MERGE INTO {owner}.MBRSH_DM_MBR_LIFE_EVT AS T
USING STAGING.IdsDmMbrshDmMbrLifeEvntLoad_ODBC_MBRSHP_DM_MBR_LIF_EVT_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY AND
    T.MBR_LIFE_EVT_TYP_CD = S.MBR_LIFE_EVT_TYP_CD AND
    T.CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT
WHEN MATCHED THEN
    UPDATE SET
        T.CLM_ID = S.CLM_ID,
        T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
        T.MBR_LIFE_EVT_TYP_NM = S.MBR_LIFE_EVT_TYP_NM,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        MBR_UNIQ_KEY,
        MBR_LIFE_EVT_TYP_CD,
        CLM_SVC_STRT_DT,
        CLM_ID,
        SUB_UNIQ_KEY,
        MBR_LIFE_EVT_TYP_NM,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.MBR_UNIQ_KEY,
        S.MBR_LIFE_EVT_TYP_CD,
        S.CLM_SVC_STRT_DT,
        S.CLM_ID,
        S.SUB_UNIQ_KEY,
        S.MBR_LIFE_EVT_TYP_NM,
        S.LAST_UPDT_RUN_CYC_NO
    );
""".format(owner=ClmMartOwner)

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_odbc_mbrshp_dm_mbr_life_evt_out_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_LIFE_EVT_TYP_CD", StringType(), True),
    StructField("CLM_SVC_STRT_DT", TimestampType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_LIFE_EVT_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_mbrshp_dm_mbr_life_evt_out_rej = spark.createDataFrame([], schema_odbc_mbrshp_dm_mbr_life_evt_out_rej).select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("MBR_LIFE_EVT_TYP_CD"), <...>, " ").alias("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    rpad(col("MBR_LIFE_EVT_TYP_NM"), <...>, " ").alias("MBR_LIFE_EVT_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("ERRORCODE").alias("ERRORCODE"),
    col("ERRORTEXT").alias("ERRORTEXT")
)

write_files(
    df_odbc_mbrshp_dm_mbr_life_evt_out_rej,
    f"{adls_path}/load/MBRSHP_DM_MBRLF_EVNT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)