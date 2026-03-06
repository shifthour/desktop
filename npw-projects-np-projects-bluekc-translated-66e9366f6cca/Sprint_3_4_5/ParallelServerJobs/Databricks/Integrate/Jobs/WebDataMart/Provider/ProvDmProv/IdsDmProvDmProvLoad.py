# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi       07/03/2013          5114                             Movies Data from IDS to  PROV_DM_PROV                                            IntegrateWrhsDevl       
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela   07/21/2014           5345                            Changed the reject file from .txt to .dat                                                   IntegrateNewDevl     
# MAGIC 
# MAGIC Jag Yelavarthi          2015-04-08        5345-Daptiv#253           Added Sorting in-addition to Partitioning in Copy stage.                         IntegrateNewDevl       Kalyan Neelam              2015-04-13
# MAGIC                                                                                                   Sort will let the job run without hanging in a multi-configuration
# MAGIC                                                                                                   environment.

# MAGIC Job Name: IdsDmProvDmProvLoad
# MAGIC Read Load File created in the IdsDmProvDmProvExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate and  Insert the PROV_DM_PROV Data.
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

schema_seq_PROV_DM_PROV_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROV_ID", StringType(), False),
    StructField("PROV_BILL_SVC_NM", StringType(), True),
    StructField("PROV_BILL_SVC_ID", StringType(), False),
    StructField("PROV_ENTY_CD", StringType(), False),
    StructField("PROV_NM", StringType(), True),
    StructField("PROV_NPI", StringType(), False),
    StructField("PROV_REL_GRP_PROV_ID", StringType(), False),
    StructField("PROV_REL_GRP_PROV_NM", StringType(), True),
    StructField("PROV_REL_IPA_PROV_ID", StringType(), False),
    StructField("PROV_REL_IPA_PROV_NM", StringType(), True),
    StructField("PROV_SPEC_CD", StringType(), False),
    StructField("PROV_SPEC_NM", StringType(), True),
    StructField("PROV_TAX_ID", StringType(), False),
    StructField("PROV_TYP_CD", StringType(), False),
    StructField("PROV_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("EXTRNL_USER_ID", StringType(), False)
])

df_seq_PROV_DM_PROV_csv_load = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .schema(schema_seq_PROV_DM_PROV_csv_load)
    .load(f"{adls_path}/load/PROV_DM_PROV.dat")
)

df_cpy_forBuffer = df_seq_PROV_DM_PROV_csv_load.select(
    col("SRC_SYS_CD"),
    col("PROV_ID"),
    col("PROV_BILL_SVC_NM"),
    col("PROV_BILL_SVC_ID"),
    col("PROV_ENTY_CD"),
    col("PROV_NM"),
    col("PROV_NPI"),
    col("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_GRP_PROV_NM"),
    col("PROV_REL_IPA_PROV_ID"),
    col("PROV_REL_IPA_PROV_NM"),
    col("PROV_SPEC_CD"),
    col("PROV_SPEC_NM"),
    col("PROV_TAX_ID"),
    col("PROV_TYP_CD"),
    col("PROV_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_NO"),
    col("EXTRNL_USER_ID")
).sort("SRC_SYS_CD", "PROV_ID")

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmProvDmProvLoad_Odbc_PROV_DM_PROV_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer_final = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PROV_ID"), <...>, " ").alias("PROV_ID"),
    rpad(col("PROV_BILL_SVC_NM"), <...>, " ").alias("PROV_BILL_SVC_NM"),
    rpad(col("PROV_BILL_SVC_ID"), <...>, " ").alias("PROV_BILL_SVC_ID"),
    rpad(col("PROV_ENTY_CD"), <...>, " ").alias("PROV_ENTY_CD"),
    rpad(col("PROV_NM"), <...>, " ").alias("PROV_NM"),
    rpad(col("PROV_NPI"), <...>, " ").alias("PROV_NPI"),
    rpad(col("PROV_REL_GRP_PROV_ID"), <...>, " ").alias("PROV_REL_GRP_PROV_ID"),
    rpad(col("PROV_REL_GRP_PROV_NM"), <...>, " ").alias("PROV_REL_GRP_PROV_NM"),
    rpad(col("PROV_REL_IPA_PROV_ID"), <...>, " ").alias("PROV_REL_IPA_PROV_ID"),
    rpad(col("PROV_REL_IPA_PROV_NM"), <...>, " ").alias("PROV_REL_IPA_PROV_NM"),
    rpad(col("PROV_SPEC_CD"), <...>, " ").alias("PROV_SPEC_CD"),
    rpad(col("PROV_SPEC_NM"), <...>, " ").alias("PROV_SPEC_NM"),
    rpad(col("PROV_TAX_ID"), <...>, " ").alias("PROV_TAX_ID"),
    rpad(col("PROV_TYP_CD"), <...>, " ").alias("PROV_TYP_CD"),
    rpad(col("PROV_TYP_NM"), <...>, " ").alias("PROV_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("EXTRNL_USER_ID"), <...>, " ").alias("EXTRNL_USER_ID")
)

df_cpy_forBuffer_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmProvDmProvLoad_Odbc_PROV_DM_PROV_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.PROV_DM_PROV AS T
USING STAGING.IdsDmProvDmProvLoad_Odbc_PROV_DM_PROV_out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.PROV_ID = S.PROV_ID
WHEN MATCHED THEN UPDATE SET
    T.PROV_BILL_SVC_NM = S.PROV_BILL_SVC_NM,
    T.PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID,
    T.PROV_ENTY_CD = S.PROV_ENTY_CD,
    T.PROV_NM = S.PROV_NM,
    T.PROV_NPI = S.PROV_NPI,
    T.PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
    T.PROV_REL_GRP_PROV_NM = S.PROV_REL_GRP_PROV_NM,
    T.PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
    T.PROV_REL_IPA_PROV_NM = S.PROV_REL_IPA_PROV_NM,
    T.PROV_SPEC_CD = S.PROV_SPEC_CD,
    T.PROV_SPEC_NM = S.PROV_SPEC_NM,
    T.PROV_TAX_ID = S.PROV_TAX_ID,
    T.PROV_TYP_CD = S.PROV_TYP_CD,
    T.PROV_TYP_NM = S.PROV_TYP_NM,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.EXTRNL_USER_ID = S.EXTRNL_USER_ID
WHEN NOT MATCHED THEN INSERT (
    SRC_SYS_CD,
    PROV_ID,
    PROV_BILL_SVC_NM,
    PROV_BILL_SVC_ID,
    PROV_ENTY_CD,
    PROV_NM,
    PROV_NPI,
    PROV_REL_GRP_PROV_ID,
    PROV_REL_GRP_PROV_NM,
    PROV_REL_IPA_PROV_ID,
    PROV_REL_IPA_PROV_NM,
    PROV_SPEC_CD,
    PROV_SPEC_NM,
    PROV_TAX_ID,
    PROV_TYP_CD,
    PROV_TYP_NM,
    LAST_UPDT_RUN_CYC_NO,
    EXTRNL_USER_ID
) VALUES (
    S.SRC_SYS_CD,
    S.PROV_ID,
    S.PROV_BILL_SVC_NM,
    S.PROV_BILL_SVC_ID,
    S.PROV_ENTY_CD,
    S.PROV_NM,
    S.PROV_NPI,
    S.PROV_REL_GRP_PROV_ID,
    S.PROV_REL_GRP_PROV_NM,
    S.PROV_REL_IPA_PROV_ID,
    S.PROV_REL_IPA_PROV_NM,
    S.PROV_SPEC_CD,
    S.PROV_SPEC_NM,
    S.PROV_TAX_ID,
    S.PROV_TYP_CD,
    S.PROV_TYP_NM,
    S.LAST_UPDT_RUN_CYC_NO,
    S.EXTRNL_USER_ID
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Odbc_PROV_DM_PROV_out_reject = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

write_files(
    df_Odbc_PROV_DM_PROV_out_reject,
    f"{adls_path}/load/PROV_DM_PROV_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)