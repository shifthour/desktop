# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     08/07/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2                            Kalyan Neelam             2016-11-28

# MAGIC Job Name: IdsDmMbrshDmFmlyLmtLoad
# MAGIC Read Load File created in the IdsDmProdDmDedctCmpntExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_FMLY_LMT Data.
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

schema_seq_MBRSH_DM_FMLY_LMT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("YR_NO", IntegerType(), True),
    StructField("PROD_ACCUM_ID", StringType(), True),
    StructField("ACCUM_NO", IntegerType(), True),
    StructField("BNF_SUM_DTL_NTWK_TYP_CD", StringType(), True),
    StructField("BNF_SUM_DTL_NTWK_TYP_NM", StringType(), True),
    StructField("BNF_SUM_DTL_TYP_CD", StringType(), True),
    StructField("BNF_SUM_DTL_TYP_NM", StringType(), True),
    StructField("LMT_AMT", DecimalType(38,10), True),
    StructField("BLUEKC_DPLY_IN", StringType(), True),
    StructField("MBR_360_DPLY_IN", StringType(), True),
    StructField("EXTRNL_DPLY_ACCUM_DESC", StringType(), True),
    StructField("INTRNL_DPLY_ACCUM_DESC", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("PLN_YR_EFF_DT", TimestampType(), True),
    StructField("PLN_YR_END_DT", TimestampType(), True)
])

df_seq_MBRSH_DM_FMLY_LMT_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_FMLY_LMT_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_FMLY_LMT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_FMLY_LMT_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("YR_NO").alias("YR_NO"),
    col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    col("ACCUM_NO").alias("ACCUM_NO"),
    col("BNF_SUM_DTL_NTWK_TYP_CD").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    col("BNF_SUM_DTL_NTWK_TYP_NM").alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    col("BNF_SUM_DTL_TYP_NM").alias("BNF_SUM_DTL_TYP_NM"),
    col("LMT_AMT").alias("LMT_AMT"),
    col("BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    col("MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    col("EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    col("INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_Odbc_MBRSH_DM_FMLY_LMT_out = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("YR_NO").alias("YR_NO"),
    rpad(col("PROD_ACCUM_ID"), <...>, " ").alias("PROD_ACCUM_ID"),
    col("ACCUM_NO").alias("ACCUM_NO"),
    rpad(col("BNF_SUM_DTL_NTWK_TYP_CD"), <...>, " ").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    rpad(col("BNF_SUM_DTL_NTWK_TYP_NM"), <...>, " ").alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    rpad(col("BNF_SUM_DTL_TYP_CD"), <...>, " ").alias("BNF_SUM_DTL_TYP_CD"),
    rpad(col("BNF_SUM_DTL_TYP_NM"), <...>, " ").alias("BNF_SUM_DTL_TYP_NM"),
    col("LMT_AMT").alias("LMT_AMT"),
    rpad(col("BLUEKC_DPLY_IN"), 1, " ").alias("BLUEKC_DPLY_IN"),
    rpad(col("MBR_360_DPLY_IN"), 1, " ").alias("MBR_360_DPLY_IN"),
    rpad(col("EXTRNL_DPLY_ACCUM_DESC"), <...>, " ").alias("EXTRNL_DPLY_ACCUM_DESC"),
    rpad(col("INTRNL_DPLY_ACCUM_DESC"), <...>, " ").alias("INTRNL_DPLY_ACCUM_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmFmlyLmtLoad_Odbc_MBRSH_DM_FMLY_LMT_out_temp", jdbc_url, jdbc_props)

df_Odbc_MBRSH_DM_FMLY_LMT_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmFmlyLmtLoad_Odbc_MBRSH_DM_FMLY_LMT_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_FMLY_LMT AS T
USING STAGING.IdsDmMbrshDmFmlyLmtLoad_Odbc_MBRSH_DM_FMLY_LMT_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
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
        SUB_UNIQ_KEY,
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
        S.SUB_UNIQ_KEY,
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
    );
"""

try:
    execute_dml(merge_sql, jdbc_url, jdbc_props)
except Exception as e:
    errorcode = "1"
    errortext = str(e)
    df_error = spark.createDataFrame([(errorcode, errortext)], ["ERRORCODE", "ERRORTEXT"])
    df_error_final = df_error.select(
        rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
        rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
    )
    write_files(
        df_error_final,
        f"{adls_path}/load/MBRSH_DM_FMLY_LMT_Rej.dat",
        delimiter=",",
        mode="append",
        is_pqruet=False,
        header=False,
        quote="^",
        nullValue=None
    )