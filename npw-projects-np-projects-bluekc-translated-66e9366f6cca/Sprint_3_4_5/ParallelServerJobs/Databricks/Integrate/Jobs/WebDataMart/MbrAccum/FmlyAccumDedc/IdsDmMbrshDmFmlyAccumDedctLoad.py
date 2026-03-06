# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     07/31/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       
# MAGIC  
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT                    IntegrateDev2           Kalyan Neelam             2016-11-28

# MAGIC Job Name: IdsDmMbrshDmFmlyAccumDedctLoad
# MAGIC Read Load File created in the IdsDmMbrshDmFmlyAccumDedctExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_FMLY_ACCUM_DEDCT Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

seq_MBRSH_DM_FMLY_ACCUM_DEDCT_csv_load_schema = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("YR_NO", IntegerType(), nullable=False),
    StructField("DEDCT_VRBL_CD", StringType(), nullable=False),
    StructField("DEDCT_VRBL_SEQ_NO", IntegerType(), nullable=False),
    StructField("DEDCT_LMT_NTWK_CD", StringType(), nullable=False),
    StructField("DRUG_ACCUM_IN", StringType(), nullable=False),
    StructField("ACCUM_AMT", DecimalType(38,10), nullable=False),
    StructField("FMLY_DEDCT_AMT", DecimalType(38,10), nullable=False),
    StructField("ACCUM_NO", IntegerType(), nullable=False),
    StructField("DEDCT_VRBL_DESC", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False),
    StructField("PROD_ACCUM_ID", StringType(), nullable=False),
    StructField("CAROVR_AMT", DecimalType(38,10), nullable=True),
    StructField("FMLY_DEDCT_CAROVR_AMT", DecimalType(38,10), nullable=True),
    StructField("PLN_YR_EFF_DT", TimestampType(), nullable=True),
    StructField("PLN_YR_END_DT", TimestampType(), nullable=True)
])

df_seq_MBRSH_DM_FMLY_ACCUM_DEDCT_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(seq_MBRSH_DM_FMLY_ACCUM_DEDCT_csv_load_schema)
    .load(f"{adls_path}/load/MBRSH_DM_FMLY_ACCUM_DEDCT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_FMLY_ACCUM_DEDCT_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("YR_NO").alias("YR_NO"),
    col("DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    col("DEDCT_VRBL_SEQ_NO").alias("DEDCT_VRBL_SEQ_NO"),
    col("DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    col("DRUG_ACCUM_IN").alias("DRUG_ACCUM_IN"),
    col("ACCUM_AMT").alias("ACCUM_AMT"),
    col("FMLY_DEDCT_AMT").alias("FMLY_DEDCT_AMT"),
    col("ACCUM_NO").alias("ACCUM_NO"),
    col("DEDCT_VRBL_DESC").alias("DEDCT_VRBL_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    col("CAROVR_AMT").alias("CAROVR_AMT"),
    col("FMLY_DEDCT_CAROVR_AMT").alias("FMLY_DEDCT_CAROVR_AMT"),
    col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), 254, " ").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("YR_NO").alias("YR_NO"),
    rpad(col("DEDCT_VRBL_CD"), 254, " ").alias("DEDCT_VRBL_CD"),
    col("DEDCT_VRBL_SEQ_NO").alias("DEDCT_VRBL_SEQ_NO"),
    rpad(col("DEDCT_LMT_NTWK_CD"), 254, " ").alias("DEDCT_LMT_NTWK_CD"),
    rpad(col("DRUG_ACCUM_IN"), 1, " ").alias("DRUG_ACCUM_IN"),
    col("ACCUM_AMT").alias("ACCUM_AMT"),
    col("FMLY_DEDCT_AMT").alias("FMLY_DEDCT_AMT"),
    col("ACCUM_NO").alias("ACCUM_NO"),
    rpad(col("DEDCT_VRBL_DESC"), 254, " ").alias("DEDCT_VRBL_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("PROD_ACCUM_ID"), 254, " ").alias("PROD_ACCUM_ID"),
    col("CAROVR_AMT").alias("CAROVR_AMT"),
    col("FMLY_DEDCT_CAROVR_AMT").alias("FMLY_DEDCT_CAROVR_AMT"),
    col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmFmlyAccumDedctLoad_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_temp",
    jdbc_url,
    jdbc_props
)

df_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsDmMbrshDmFmlyAccumDedctLoad_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_FMLY_ACCUM_DEDCT AS T
USING STAGING.IdsDmMbrshDmFmlyAccumDedctLoad_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY AND
    T.YR_NO = S.YR_NO AND
    T.DEDCT_VRBL_CD = S.DEDCT_VRBL_CD AND
    T.DEDCT_VRBL_SEQ_NO = S.DEDCT_VRBL_SEQ_NO
WHEN MATCHED THEN
    UPDATE SET
        T.DEDCT_LMT_NTWK_CD = S.DEDCT_LMT_NTWK_CD,
        T.DRUG_ACCUM_IN = S.DRUG_ACCUM_IN,
        T.ACCUM_AMT = S.ACCUM_AMT,
        T.FMLY_DEDCT_AMT = S.FMLY_DEDCT_AMT,
        T.ACCUM_NO = S.ACCUM_NO,
        T.DEDCT_VRBL_DESC = S.DEDCT_VRBL_DESC,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
        T.PROD_ACCUM_ID = S.PROD_ACCUM_ID,
        T.CAROVR_AMT = S.CAROVR_AMT,
        T.FMLY_DEDCT_CAROVR_AMT = S.FMLY_DEDCT_CAROVR_AMT,
        T.PLN_YR_EFF_DT = S.PLN_YR_EFF_DT,
        T.PLN_YR_END_DT = S.PLN_YR_END_DT
WHEN NOT MATCHED THEN
    INSERT
    (
        SRC_SYS_CD,
        SUB_UNIQ_KEY,
        YR_NO,
        DEDCT_VRBL_CD,
        DEDCT_VRBL_SEQ_NO,
        DEDCT_LMT_NTWK_CD,
        DRUG_ACCUM_IN,
        ACCUM_AMT,
        FMLY_DEDCT_AMT,
        ACCUM_NO,
        DEDCT_VRBL_DESC,
        LAST_UPDT_RUN_CYC_NO,
        PROD_ACCUM_ID,
        CAROVR_AMT,
        FMLY_DEDCT_CAROVR_AMT,
        PLN_YR_EFF_DT,
        PLN_YR_END_DT
    )
    VALUES
    (
        S.SRC_SYS_CD,
        S.SUB_UNIQ_KEY,
        S.YR_NO,
        S.DEDCT_VRBL_CD,
        S.DEDCT_VRBL_SEQ_NO,
        S.DEDCT_LMT_NTWK_CD,
        S.DRUG_ACCUM_IN,
        S.ACCUM_AMT,
        S.FMLY_DEDCT_AMT,
        S.ACCUM_NO,
        S.DEDCT_VRBL_DESC,
        S.LAST_UPDT_RUN_CYC_NO,
        S.PROD_ACCUM_ID,
        S.CAROVR_AMT,
        S.FMLY_DEDCT_CAROVR_AMT,
        S.PLN_YR_EFF_DT,
        S.PLN_YR_END_DT
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_rej_final = df_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_rej.select(
    rpad(col("ERRORCODE"), 254, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 254, " ").alias("ERRORTEXT")
)

write_files(
    df_Odbc_MBRSH_DM_FMLY_ACCUM_DEDCTout_rej_final,
    f"{adls_path}/load/PROD_DM_DEDCT_CMPNT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)