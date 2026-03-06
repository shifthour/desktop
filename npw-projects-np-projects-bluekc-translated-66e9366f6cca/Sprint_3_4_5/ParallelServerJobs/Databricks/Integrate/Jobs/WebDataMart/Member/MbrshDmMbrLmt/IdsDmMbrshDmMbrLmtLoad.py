# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi           08/072013          5114                             Movies Data from IDS to  MBRSH_DM_MBR_LMT                            IntegrateWrhsDevl    Peter Marshall               10/23/2013   
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2                            Kalyan Neelam              2016-11-28

# MAGIC Job Name: IdsDmMbrshDmMbrLmtLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrLmtExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the 
# MAGIC 
# MAGIC MBRSH_DM_MBR_LMT  Data.
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


ClmMartOwner = get_widget_value("ClmMartOwner", "")
ClmMartArraySize = get_widget_value("ClmMartArraySize", "")
ClmMartRecordCount = get_widget_value("ClmMartRecordCount", "")
clmmart_secret_name = get_widget_value("clmmart_secret_name", "")

schema_seq_MBRSH_DM_MBR_LMT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("YR_NO", IntegerType(), False),
    StructField("PROD_ACCUM_ID", StringType(), False),
    StructField("ACCUM_NO", IntegerType(), False),
    StructField("BNF_SUM_DTL_NTWK_TYP_CD", StringType(), False),
    StructField("BNF_SUM_DTL_NTWK_TYP_NM", StringType(), False),
    StructField("BNF_SUM_DTL_TYP_CD", StringType(), False),
    StructField("BNF_SUM_DTL_TYP_NM", StringType(), False),
    StructField("LMT_AMT", DecimalType(38,10), False),
    StructField("BLUEKC_DPLY_IN", StringType(), False),
    StructField("MBR_360_DPLY_IN", StringType(), False),
    StructField("EXTRNL_DPLY_ACCUM_DESC", StringType(), False),
    StructField("INTRNL_DPLY_ACCUM_DESC", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("PLN_YR_EFF_DT", TimestampType(), True),
    StructField("PLN_YR_END_DT", TimestampType(), True)
])

df_seq_MBRSH_DM_MBR_LMT_csv_load = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_MBR_LMT_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_LMT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_LMT_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    F.col("BNF_SUM_DTL_NTWK_TYP_NM").alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("BNF_SUM_DTL_TYP_NM").alias("BNF_SUM_DTL_TYP_NM"),
    F.col("LMT_AMT").alias("LMT_AMT"),
    F.col("BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
temp_table_name = "STAGING.IdsDmMbrshDmMbrLmtLoad_Odbc_MBRSH_DM_MBR_LMT_out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.jdbc(url=jdbc_url, table=temp_table_name, mode="overwrite", properties=jdbc_props)

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_MBR_LMT AS T
USING {temp_table_name} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.YR_NO = S.YR_NO
    AND T.PROD_ACCUM_ID = S.PROD_ACCUM_ID
    AND T.ACCUM_NO = S.ACCUM_NO
    AND T.BNF_SUM_DTL_NTWK_TYP_CD = S.BNF_SUM_DTL_NTWK_TYP_CD
WHEN MATCHED THEN
    UPDATE SET
    T.BNF_SUM_DTL_NTWK_TYP_NM = S.BNF_SUM_DTL_NTWK_TYP_NM,
    T.BNF_SUM_DTL_TYP_CD = S.BNF_SUM_DTL_TYP_CD,
    T.BNF_SUM_DTL_TYP_NM = S.BNF_SUM_DTL_TYP_NM,
    T.LMT_AMT = S.LMT_AMT,
    T.BLUEKC_DPLY_IN = S.BLUEKC_DPLY_IN,
    T.MBR_360_DPLY_IN = S.MBR_360_DPLY_IN,
    T.EXTRNL_DPLY_ACCUM_DESC = S.EXTRNL_DPLY_ACCUM_DESC,
    T.INTRNL_DPLY_ACCUM_DESC = S.INTRNL_DPLY_ACCUM_DESC,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.PLN_YR_EFF_DT = S.PLN_YR_EFF_DT,
    T.PLN_YR_END_DT = S.PLN_YR_END_DT
WHEN NOT MATCHED THEN
    INSERT
    (
     SRC_SYS_CD,
     MBR_UNIQ_KEY,
     YR_NO,
     PROD_ACCUM_ID,
     ACCUM_NO,
     BNF_SUM_DTL_NTWK_TYP_CD,
     BNF_SUM_DTL_NTWK_TYP_NM,
     BNF_SUM_DTL_TYP_CD,
     BNF_SUM_DTL_TYP_NM,
     LMT_AMT,
     BLUEKC_DPLY_IN,
     MBR_360_DPLY_IN,
     EXTRNL_DPLY_ACCUM_DESC,
     INTRNL_DPLY_ACCUM_DESC,
     LAST_UPDT_RUN_CYC_NO,
     PLN_YR_EFF_DT,
     PLN_YR_END_DT
    )
    VALUES
    (
     S.SRC_SYS_CD,
     S.MBR_UNIQ_KEY,
     S.YR_NO,
     S.PROD_ACCUM_ID,
     S.ACCUM_NO,
     S.BNF_SUM_DTL_NTWK_TYP_CD,
     S.BNF_SUM_DTL_NTWK_TYP_NM,
     S.BNF_SUM_DTL_TYP_CD,
     S.BNF_SUM_DTL_TYP_NM,
     S.LMT_AMT,
     S.BLUEKC_DPLY_IN,
     S.MBR_360_DPLY_IN,
     S.EXTRNL_DPLY_ACCUM_DESC,
     S.INTRNL_DPLY_ACCUM_DESC,
     S.LAST_UPDT_RUN_CYC_NO,
     S.PLN_YR_EFF_DT,
     S.PLN_YR_END_DT
    )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_Odbc_MBRSH_DM_MBR_LMT_out_reject = spark.createDataFrame([], schema_reject).select(
    F.rpad(F.col("ERRORCODE"), 200, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), 500, " ").alias("ERRORTEXT")
)

write_files(
    df_Odbc_MBRSH_DM_MBR_LMT_out_reject,
    f"{adls_path}/load/MBRSH_DM_MBR_LMT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)