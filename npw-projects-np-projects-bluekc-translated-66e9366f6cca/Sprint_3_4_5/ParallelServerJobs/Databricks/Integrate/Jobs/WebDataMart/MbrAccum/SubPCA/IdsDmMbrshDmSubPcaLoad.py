# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi       07/03/2013          5114                             Movies Data from IDS to  MBRSH_DM_SUB_PCA                                IntegrateWrhsDevl    Peter Marshall               10/22/2013   
# MAGIC 
# MAGIC Pooja Sunkara      08/01/2014          TFS#9537                    Checked "Only run after-job subroutine on successful job 
# MAGIC                                                                                                 completion" to send load file to processed only after job finishes.          IntegrateNewDevl         Jag Yelavarthi             2014-08-01

# MAGIC Job Name: IdsDmMbrshDmSubPcaLoad
# MAGIC Read Load File created in the IdsDmMbrshDmSubPcaExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_SUB_PCA Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_SUB_PCA_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SUB_PCA_ACCUM_PFX_ID", StringType(), nullable=False),
    StructField("PLN_YR_BEG_DT_SK", StringType(), nullable=False),
    StructField("SUB_PCA_EFF_DT_SK", StringType(), nullable=False),
    StructField("GRP_SK", IntegerType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUB_SK", IntegerType(), nullable=False),
    StructField("SUB_PCA_CAROVR_CALC_RULE_CD", StringType(), nullable=False),
    StructField("SUB_PCA_CAROVR_CALC_RULE_NM", StringType(), nullable=False),
    StructField("SUB_PCA_ALLOC_AMT", DecimalType(38,10), nullable=False),
    StructField("SUB_PCA_BAL_AMT", DecimalType(38,10), nullable=False),
    StructField("SUB_PCA_CAROVR_AMT", DecimalType(38,10), nullable=False),
    StructField("SUB_PCA_MAX_CAROVR_AMT", DecimalType(38,10), nullable=False),
    StructField("SUB_PCA_PD_AMT", DecimalType(38,10), nullable=False),
    StructField("SUB_PCA_TERM_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False)
])

df_seq_MBRSH_DM_SUB_PCA_csv_load = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("inferSchema", "false")
    .schema(schema_seq_MBRSH_DM_SUB_PCA_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_SUB_PCA.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_SUB_PCA_csv_load.select(
    "SRC_SYS_CD",
    "SUB_UNIQ_KEY",
    "SUB_PCA_ACCUM_PFX_ID",
    "PLN_YR_BEG_DT_SK",
    "SUB_PCA_EFF_DT_SK",
    "GRP_SK",
    "GRP_ID",
    "SUB_SK",
    "SUB_PCA_CAROVR_CALC_RULE_CD",
    "SUB_PCA_CAROVR_CALC_RULE_NM",
    "SUB_PCA_ALLOC_AMT",
    "SUB_PCA_BAL_AMT",
    "SUB_PCA_CAROVR_AMT",
    "SUB_PCA_MAX_CAROVR_AMT",
    "SUB_PCA_PD_AMT",
    "SUB_PCA_TERM_DT_SK",
    "LAST_UPDT_RUN_CYC_NO"
)

df_odbc_MBRSH_DM_SUB_PCA_out = df_cpy_forBuffer
df_odbc_MBRSH_DM_SUB_PCA_out = df_odbc_MBRSH_DM_SUB_PCA_out.withColumn(
    "SRC_SYS_CD", rpad("SRC_SYS_CD", 255, " ")
).withColumn(
    "SUB_PCA_ACCUM_PFX_ID", rpad("SUB_PCA_ACCUM_PFX_ID", 255, " ")
).withColumn(
    "PLN_YR_BEG_DT_SK", rpad("PLN_YR_BEG_DT_SK", 10, " ")
).withColumn(
    "SUB_PCA_EFF_DT_SK", rpad("SUB_PCA_EFF_DT_SK", 10, " ")
).withColumn(
    "GRP_ID", rpad("GRP_ID", 255, " ")
).withColumn(
    "SUB_PCA_CAROVR_CALC_RULE_CD", rpad("SUB_PCA_CAROVR_CALC_RULE_CD", 255, " ")
).withColumn(
    "SUB_PCA_CAROVR_CALC_RULE_NM", rpad("SUB_PCA_CAROVR_CALC_RULE_NM", 255, " ")
).withColumn(
    "SUB_PCA_TERM_DT_SK", rpad("SUB_PCA_TERM_DT_SK", 10, " ")
)

df_odbc_MBRSH_DM_SUB_PCA_out = df_odbc_MBRSH_DM_SUB_PCA_out.select(
    "SRC_SYS_CD",
    "SUB_UNIQ_KEY",
    "SUB_PCA_ACCUM_PFX_ID",
    "PLN_YR_BEG_DT_SK",
    "SUB_PCA_EFF_DT_SK",
    "GRP_SK",
    "GRP_ID",
    "SUB_SK",
    "SUB_PCA_CAROVR_CALC_RULE_CD",
    "SUB_PCA_CAROVR_CALC_RULE_NM",
    "SUB_PCA_ALLOC_AMT",
    "SUB_PCA_BAL_AMT",
    "SUB_PCA_CAROVR_AMT",
    "SUB_PCA_MAX_CAROVR_AMT",
    "SUB_PCA_PD_AMT",
    "SUB_PCA_TERM_DT_SK",
    "LAST_UPDT_RUN_CYC_NO"
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmSubPcaLoad_Odbc_MBRSH_DM_SUB_PCA_out_temp",
    jdbc_url,
    jdbc_props
)

df_odbc_MBRSH_DM_SUB_PCA_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmSubPcaLoad_Odbc_MBRSH_DM_SUB_PCA_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_SUB_PCA AS T
USING STAGING.IdsDmMbrshDmSubPcaLoad_Odbc_MBRSH_DM_SUB_PCA_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
    AND T.SUB_PCA_ACCUM_PFX_ID = S.SUB_PCA_ACCUM_PFX_ID
    AND T.PLN_YR_BEG_DT_SK = S.PLN_YR_BEG_DT_SK
    AND T.SUB_PCA_EFF_DT_SK = S.SUB_PCA_EFF_DT_SK
WHEN MATCHED THEN
    UPDATE SET
        T.GRP_SK = S.GRP_SK,
        T.GRP_ID = S.GRP_ID,
        T.SUB_SK = S.SUB_SK,
        T.SUB_PCA_CAROVR_CALC_RULE_CD = S.SUB_PCA_CAROVR_CALC_RULE_CD,
        T.SUB_PCA_CAROVR_CALC_RULE_NM = S.SUB_PCA_CAROVR_CALC_RULE_NM,
        T.SUB_PCA_ALLOC_AMT = S.SUB_PCA_ALLOC_AMT,
        T.SUB_PCA_BAL_AMT = S.SUB_PCA_BAL_AMT,
        T.SUB_PCA_CAROVR_AMT = S.SUB_PCA_CAROVR_AMT,
        T.SUB_PCA_MAX_CAROVR_AMT = S.SUB_PCA_MAX_CAROVR_AMT,
        T.SUB_PCA_PD_AMT = S.SUB_PCA_PD_AMT,
        T.SUB_PCA_TERM_DT_SK = S.SUB_PCA_TERM_DT_SK,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        SUB_UNIQ_KEY,
        SUB_PCA_ACCUM_PFX_ID,
        PLN_YR_BEG_DT_SK,
        SUB_PCA_EFF_DT_SK,
        GRP_SK,
        GRP_ID,
        SUB_SK,
        SUB_PCA_CAROVR_CALC_RULE_CD,
        SUB_PCA_CAROVR_CALC_RULE_NM,
        SUB_PCA_ALLOC_AMT,
        SUB_PCA_BAL_AMT,
        SUB_PCA_CAROVR_AMT,
        SUB_PCA_MAX_CAROVR_AMT,
        SUB_PCA_PD_AMT,
        SUB_PCA_TERM_DT_SK,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.SUB_UNIQ_KEY,
        S.SUB_PCA_ACCUM_PFX_ID,
        S.PLN_YR_BEG_DT_SK,
        S.SUB_PCA_EFF_DT_SK,
        S.GRP_SK,
        S.GRP_ID,
        S.SUB_SK,
        S.SUB_PCA_CAROVR_CALC_RULE_CD,
        S.SUB_PCA_CAROVR_CALC_RULE_NM,
        S.SUB_PCA_ALLOC_AMT,
        S.SUB_PCA_BAL_AMT,
        S.SUB_PCA_CAROVR_AMT,
        S.SUB_PCA_MAX_CAROVR_AMT,
        S.SUB_PCA_PD_AMT,
        S.SUB_PCA_TERM_DT_SK,
        S.LAST_UPDT_RUN_CYC_NO
    )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_MBRSH_DM_SUB_PCA_out_errors = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

write_files(
    df_odbc_MBRSH_DM_SUB_PCA_out_errors.select("ERRORCODE", "ERRORTEXT"),
    f"{adls_path}/load/MBRSH_DM_SUB_PCA_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)