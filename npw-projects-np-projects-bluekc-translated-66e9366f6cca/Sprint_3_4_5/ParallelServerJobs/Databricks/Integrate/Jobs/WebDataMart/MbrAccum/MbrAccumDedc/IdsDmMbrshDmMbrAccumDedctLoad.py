# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya Raju               2013-08-20          5114                             Original Programming                                                                             IntegrateWrhsDevl   Peter Marshall               10/18/2013    
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2                            Kalyan Neelam              2016-11-28

# MAGIC Job Name: IdsDmMbrshDmMbrAccumDedctLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrAccumDedctExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_MBR_ACCUM_DEDCT Data.
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_MBR_ACCUM_DEDCT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("YR_NO", IntegerType(), False),
    StructField("DEDCT_VRBL_CD", StringType(), False),
    StructField("DEDCT_VRBL_SEQ_NO", IntegerType(), False),
    StructField("DEDCT_LMT_NTWK_CD", StringType(), False),
    StructField("DRUG_ACCUM_IN", StringType(), False),
    StructField("ACCUM_AMT", DecimalType(38,10), False),
    StructField("MBR_DEDCT_AMT", DecimalType(38,10), False),
    StructField("ACCUM_NO", IntegerType(), False),
    StructField("DEDCT_VRBL_DESC", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("PROD_ACCUM_ID", StringType(), False),
    StructField("CAROVR_AMT", DecimalType(38,10), True),
    StructField("COB_OOP_AMT", DecimalType(38,10), True),
    StructField("MBR_DEDCT_CAROVR_AMT", DecimalType(38,10), True),
    StructField("PLN_YR_EFF_DT", TimestampType(), True),
    StructField("PLN_YR_END_DT", TimestampType(), True)
])

df_seq_MBRSH_DM_MBR_ACCUM_DEDCT_csv_load = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_MBRSH_DM_MBR_ACCUM_DEDCT_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_ACCUM_DEDCT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_ACCUM_DEDCT_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    F.col("DEDCT_VRBL_SEQ_NO").alias("DEDCT_VRBL_SEQ_NO"),
    F.col("DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    F.col("DRUG_ACCUM_IN").alias("DRUG_ACCUM_IN"),
    F.col("ACCUM_AMT").alias("ACCUM_AMT"),
    F.col("MBR_DEDCT_AMT").alias("MBR_DEDCT_AMT"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("DEDCT_VRBL_DESC").alias("DEDCT_VRBL_DESC"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("CAROVR_AMT").alias("CAROVR_AMT"),
    F.col("COB_OOP_AMT").alias("COB_OOP_AMT"),
    F.col("MBR_DEDCT_CAROVR_AMT").alias("MBR_DEDCT_CAROVR_AMT"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_odbc_mbrsh_dm_mbr_accum_dedctout = (
    df_cpy_forBuffer
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), F.lit("<...>"), F.lit(" ")))
    .withColumn("DEDCT_VRBL_CD", F.rpad(F.col("DEDCT_VRBL_CD"), F.lit("<...>"), F.lit(" ")))
    .withColumn("DEDCT_LMT_NTWK_CD", F.rpad(F.col("DEDCT_LMT_NTWK_CD"), F.lit("<...>"), F.lit(" ")))
    .withColumn("DRUG_ACCUM_IN", F.rpad(F.col("DRUG_ACCUM_IN"), F.lit(1), F.lit(" ")))
    .withColumn("DEDCT_VRBL_DESC", F.rpad(F.col("DEDCT_VRBL_DESC"), F.lit("<...>"), F.lit(" ")))
    .withColumn("PROD_ACCUM_ID", F.rpad(F.col("PROD_ACCUM_ID"), F.lit("<...>"), F.lit(" ")))
    .select(
        F.col("SRC_SYS_CD"),
        F.col("MBR_UNIQ_KEY"),
        F.col("YR_NO"),
        F.col("DEDCT_VRBL_CD"),
        F.col("DEDCT_VRBL_SEQ_NO"),
        F.col("DEDCT_LMT_NTWK_CD"),
        F.col("DRUG_ACCUM_IN"),
        F.col("ACCUM_AMT"),
        F.col("MBR_DEDCT_AMT"),
        F.col("ACCUM_NO"),
        F.col("DEDCT_VRBL_DESC"),
        F.col("LAST_UPDT_RUN_CYC_NO"),
        F.col("PROD_ACCUM_ID"),
        F.col("CAROVR_AMT"),
        F.col("COB_OOP_AMT"),
        F.col("MBR_DEDCT_CAROVR_AMT"),
        F.col("PLN_YR_EFF_DT"),
        F.col("PLN_YR_END_DT")
    )
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrAccumDedctLoad_Odbc_MBRSH_DM_MBR_ACCUM_DEDCTout_temp", jdbc_url, jdbc_props)

df_odbc_mbrsh_dm_mbr_accum_dedctout.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsDmMbrshDmMbrAccumDedctLoad_Odbc_MBRSH_DM_MBR_ACCUM_DEDCTout_temp",
    mode="append",
    properties=jdbc_props
)

merge_sql = """
MERGE INTO #$ClmMartOwner#.MBRSH_DM_MBR_ACCUM_DEDCT AS T
USING STAGING.IdsDmMbrshDmMbrAccumDedctLoad_Odbc_MBRSH_DM_MBR_ACCUM_DEDCTout_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.YR_NO = S.YR_NO
    AND T.DEDCT_VRBL_CD = S.DEDCT_VRBL_CD
    AND T.DEDCT_VRBL_SEQ_NO = S.DEDCT_VRBL_SEQ_NO
WHEN MATCHED THEN UPDATE SET
    T.DEDCT_LMT_NTWK_CD = S.DEDCT_LMT_NTWK_CD,
    T.DRUG_ACCUM_IN = S.DRUG_ACCUM_IN,
    T.ACCUM_AMT = S.ACCUM_AMT,
    T.MBR_DEDCT_AMT = S.MBR_DEDCT_AMT,
    T.ACCUM_NO = S.ACCUM_NO,
    T.DEDCT_VRBL_DESC = S.DEDCT_VRBL_DESC,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.PROD_ACCUM_ID = S.PROD_ACCUM_ID,
    T.CAROVR_AMT = S.CAROVR_AMT,
    T.COB_OOP_AMT = S.COB_OOP_AMT,
    T.MBR_DEDCT_CAROVR_AMT = S.MBR_DEDCT_CAROVR_AMT,
    T.PLN_YR_EFF_DT = S.PLN_YR_EFF_DT,
    T.PLN_YR_END_DT = S.PLN_YR_END_DT
WHEN NOT MATCHED THEN INSERT
(
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    YR_NO,
    DEDCT_VRBL_CD,
    DEDCT_VRBL_SEQ_NO,
    DEDCT_LMT_NTWK_CD,
    DRUG_ACCUM_IN,
    ACCUM_AMT,
    MBR_DEDCT_AMT,
    ACCUM_NO,
    DEDCT_VRBL_DESC,
    LAST_UPDT_RUN_CYC_NO,
    PROD_ACCUM_ID,
    CAROVR_AMT,
    COB_OOP_AMT,
    MBR_DEDCT_CAROVR_AMT,
    PLN_YR_EFF_DT,
    PLN_YR_END_DT
)
VALUES
(
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.YR_NO,
    S.DEDCT_VRBL_CD,
    S.DEDCT_VRBL_SEQ_NO,
    S.DEDCT_LMT_NTWK_CD,
    S.DRUG_ACCUM_IN,
    S.ACCUM_AMT,
    S.MBR_DEDCT_AMT,
    S.ACCUM_NO,
    S.DEDCT_VRBL_DESC,
    S.LAST_UPDT_RUN_CYC_NO,
    S.PROD_ACCUM_ID,
    S.CAROVR_AMT,
    S.COB_OOP_AMT,
    S.MBR_DEDCT_CAROVR_AMT,
    S.PLN_YR_EFF_DT,
    S.PLN_YR_END_DT
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_MBRSH_DM_MBR_ACCUM_DEDCT_csv_rej = df_odbc_mbrsh_dm_mbr_accum_dedctout.select(
    F.rpad(F.lit(""), F.lit("<...>"), F.lit(" ")).alias("ERRORCODE"),
    F.rpad(F.lit(""), F.lit("<...>"), F.lit(" ")).alias("ERRORTEXT")
)

write_files(
    df_seq_MBRSH_DM_MBR_ACCUM_DEDCT_csv_rej.select("ERRORCODE","ERRORTEXT"),
    f"{adls_path}/load/PROD_DM_DEDCT_CMPNT_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)