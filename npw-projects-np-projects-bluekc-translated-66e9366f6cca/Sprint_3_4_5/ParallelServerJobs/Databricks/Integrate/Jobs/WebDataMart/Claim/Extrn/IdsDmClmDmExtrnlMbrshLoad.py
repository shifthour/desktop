# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     09/16/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi            2013-11-30
# MAGIC 
# MAGIC Archana Palivela     01/21/2014          5114                             Updated the Load type to Update and Insert from Replace                   IntegrateWrhsDev       Jag Yelavarthi             2014-01-23

# MAGIC Job Name: IdsDmClmDmExtrnlMbrshLoad
# MAGIC Read Load File created in the IdsDmClmDmExtrnlMbrshExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Replace the CLM_DM_CLM_EXTRNL_MBRSH Data.
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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

schema_seq_CLM_DM_CLM_EXTRNL_MBRSH_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_SUB_ID", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_MBR_RELSHP_CD", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_MBR_FIRST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_MBR_MIDINIT", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_MBR_LAST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_MBR_BRTH_DT", TimestampType(), True),
    StructField("CLM_EXTRNL_MBRSH_MBR_GNDR_CD", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_SUB_FIRST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_SUB_MIDINIT", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_SUB_LAST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_ACTL_SUB_ID", StringType(), True),
    StructField("CLM_EXTRNL_MBRSH_SUBMT_SUB_ID", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_CLM_DM_CLM_EXTRNL_MBRSH_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_CLM_DM_CLM_EXTRNL_MBRSH_csv_load)
    .csv(f"{adls_path}/load/CLM_DM_CLM_EXTRNL_MBRSH.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_EXTRNL_MBRSH_csv_load.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_EXTRNL_MBRSH_SUB_ID",
    "CLM_EXTRNL_MBRSH_MBR_RELSHP_CD",
    "CLM_EXTRNL_MBRSH_MBR_FIRST_NM",
    "CLM_EXTRNL_MBRSH_MBR_MIDINIT",
    "CLM_EXTRNL_MBRSH_MBR_LAST_NM",
    "CLM_EXTRNL_MBRSH_MBR_BRTH_DT",
    "CLM_EXTRNL_MBRSH_MBR_GNDR_CD",
    "CLM_EXTRNL_MBRSH_SUB_FIRST_NM",
    "CLM_EXTRNL_MBRSH_SUB_MIDINIT",
    "CLM_EXTRNL_MBRSH_SUB_LAST_NM",
    "CLM_EXTRNL_MBRSH_ACTL_SUB_ID",
    "CLM_EXTRNL_MBRSH_SUBMT_SUB_ID",
    "LAST_UPDT_RUN_CYC_NO"
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmClmDmExtrnlMbrshLoad_Odbc_CLM_DM_CLM_EXTRNL_MBRSH_out_temp", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmDmExtrnlMbrshLoad_Odbc_CLM_DM_CLM_EXTRNL_MBRSH_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_EXTRNL_MBRSH AS T
USING STAGING.IdsDmClmDmExtrnlMbrshLoad_Odbc_CLM_DM_CLM_EXTRNL_MBRSH_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN
    UPDATE SET
        T.CLM_EXTRNL_MBRSH_SUB_ID = S.CLM_EXTRNL_MBRSH_SUB_ID,
        T.CLM_EXTRNL_MBRSH_MBR_RELSHP_CD = S.CLM_EXTRNL_MBRSH_MBR_RELSHP_CD,
        T.CLM_EXTRNL_MBRSH_MBR_FIRST_NM = S.CLM_EXTRNL_MBRSH_MBR_FIRST_NM,
        T.CLM_EXTRNL_MBRSH_MBR_MIDINIT = S.CLM_EXTRNL_MBRSH_MBR_MIDINIT,
        T.CLM_EXTRNL_MBRSH_MBR_LAST_NM = S.CLM_EXTRNL_MBRSH_MBR_LAST_NM,
        T.CLM_EXTRNL_MBRSH_MBR_BRTH_DT = S.CLM_EXTRNL_MBRSH_MBR_BRTH_DT,
        T.CLM_EXTRNL_MBRSH_MBR_GNDR_CD = S.CLM_EXTRNL_MBRSH_MBR_GNDR_CD,
        T.CLM_EXTRNL_MBRSH_SUB_FIRST_NM = S.CLM_EXTRNL_MBRSH_SUB_FIRST_NM,
        T.CLM_EXTRNL_MBRSH_SUB_MIDINIT = S.CLM_EXTRNL_MBRSH_SUB_MIDINIT,
        T.CLM_EXTRNL_MBRSH_SUB_LAST_NM = S.CLM_EXTRNL_MBRSH_SUB_LAST_NM,
        T.CLM_EXTRNL_MBRSH_ACTL_SUB_ID = S.CLM_EXTRNL_MBRSH_ACTL_SUB_ID,
        T.CLM_EXTRNL_MBRSH_SUBMT_SUB_ID = S.CLM_EXTRNL_MBRSH_SUBMT_SUB_ID,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        CLM_ID,
        CLM_EXTRNL_MBRSH_SUB_ID,
        CLM_EXTRNL_MBRSH_MBR_RELSHP_CD,
        CLM_EXTRNL_MBRSH_MBR_FIRST_NM,
        CLM_EXTRNL_MBRSH_MBR_MIDINIT,
        CLM_EXTRNL_MBRSH_MBR_LAST_NM,
        CLM_EXTRNL_MBRSH_MBR_BRTH_DT,
        CLM_EXTRNL_MBRSH_MBR_GNDR_CD,
        CLM_EXTRNL_MBRSH_SUB_FIRST_NM,
        CLM_EXTRNL_MBRSH_SUB_MIDINIT,
        CLM_EXTRNL_MBRSH_SUB_LAST_NM,
        CLM_EXTRNL_MBRSH_ACTL_SUB_ID,
        CLM_EXTRNL_MBRSH_SUBMT_SUB_ID,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.CLM_ID,
        S.CLM_EXTRNL_MBRSH_SUB_ID,
        S.CLM_EXTRNL_MBRSH_MBR_RELSHP_CD,
        S.CLM_EXTRNL_MBRSH_MBR_FIRST_NM,
        S.CLM_EXTRNL_MBRSH_MBR_MIDINIT,
        S.CLM_EXTRNL_MBRSH_MBR_LAST_NM,
        S.CLM_EXTRNL_MBRSH_MBR_BRTH_DT,
        S.CLM_EXTRNL_MBRSH_MBR_GNDR_CD,
        S.CLM_EXTRNL_MBRSH_SUB_FIRST_NM,
        S.CLM_EXTRNL_MBRSH_SUB_MIDINIT,
        S.CLM_EXTRNL_MBRSH_SUB_LAST_NM,
        S.CLM_EXTRNL_MBRSH_ACTL_SUB_ID,
        S.CLM_EXTRNL_MBRSH_SUBMT_SUB_ID,
        S.LAST_UPDT_RUN_CYC_NO
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Odbc_CLM_DM_CLM_EXTRNL_MBRSH_out_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

write_files(
    df_Odbc_CLM_DM_CLM_EXTRNL_MBRSH_out_rej.select("ERRORCODE", "ERRORTEXT"),
    f"{adls_path}/load/CLM_DM_CLM_EXTRNL_MBRSH_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)