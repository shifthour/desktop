# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     08/09/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl    Peter Marshall              10/23/2013

# MAGIC Job Name: IdsDmMbrshDmMbrHlthScrnLoad
# MAGIC Read Load File created in the IdsDmProdDmDedctCmpntExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_HLTH_SCRN Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_MBR_HLTH_SCRN_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("LAST_HLTH_SCRN_DT", TimestampType(), False),
    StructField("BMI_NO", IntegerType(), False),
    StructField("CHLSTRL_RATIO_NO", DecimalType(38,10), False),
    StructField("DIASTOLIC_BP_NO", IntegerType(), False),
    StructField("GLUCOSE_NO", IntegerType(), False),
    StructField("HDL_NO", IntegerType(), False),
    StructField("HT_INCH_NO", DecimalType(38,10), False),
    StructField("LDL_NO", IntegerType(), False),
    StructField("MBR_AGE_NO", IntegerType(), False),
    StructField("PSA_NO", DecimalType(38,10), True),
    StructField("SYSTOLIC_BP_NO", IntegerType(), False),
    StructField("TOT_CHLSTRL_NO", IntegerType(), False),
    StructField("TGL_NO", IntegerType(), False),
    StructField("WAIST_CRCMFR_NO", DecimalType(38,10), False),
    StructField("WT_NO", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("BODY_FAT_PCT", DecimalType(38,10), True),
    StructField("CUR_SMOKER_RISK_IN", StringType(), False),
    StructField("DBTC_RISK_IN", StringType(), False),
    StructField("EXRCS_LACK_RISK_IN", StringType(), False),
    StructField("FRMR_SMOKER_RISK_IN", StringType(), False),
    StructField("FSTNG_IN", StringType(), False),
    StructField("HA1C_NO", DecimalType(38,10), True),
    StructField("HEART_DSS_RISK_IN", StringType(), False),
    StructField("HI_BP_RISK_IN", StringType(), False),
    StructField("HI_CHLSTRL_RISK_IN", StringType(), False),
    StructField("HLTH_SCRN_MBR_GNDR_CD", StringType(), False),
    StructField("OVERWT_RISK_IN", StringType(), False),
    StructField("PRGNCY_IN", StringType(), False),
    StructField("RFRL_TO_DM_IN", StringType(), False),
    StructField("RFRL_TO_PHYS_IN", StringType(), False),
    StructField("RESCRN_IN", StringType(), False),
    StructField("STROKE_RISK_IN", StringType(), False),
    StructField("STRS_RISK_IN", StringType(), False),
    StructField("TSH_NO", DecimalType(38,10), True)
])

df_seq_MBRSH_DM_MBR_HLTH_SCRN_csv_load = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_MBR_HLTH_SCRN_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_HLTH_SCRN.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_HLTH_SCRN_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("LAST_HLTH_SCRN_DT").alias("LAST_HLTH_SCRN_DT"),
    F.col("BMI_NO").alias("BMI_NO"),
    F.col("CHLSTRL_RATIO_NO").alias("CHLSTRL_RATIO_NO"),
    F.col("DIASTOLIC_BP_NO").alias("DIASTOLIC_BP_NO"),
    F.col("GLUCOSE_NO").alias("GLUCOSE_NO"),
    F.col("HDL_NO").alias("HDL_NO"),
    F.col("HT_INCH_NO").alias("HT_INCH_NO"),
    F.col("LDL_NO").alias("LDL_NO"),
    F.col("MBR_AGE_NO").alias("MBR_AGE_NO"),
    F.col("PSA_NO").alias("PSA_NO"),
    F.col("SYSTOLIC_BP_NO").alias("SYSTOLIC_BP_NO"),
    F.col("TOT_CHLSTRL_NO").alias("TOT_CHLSTRL_NO"),
    F.col("TGL_NO").alias("TGL_NO"),
    F.col("WAIST_CRCMFR_NO").alias("WAIST_CRCMFR_NO"),
    F.col("WT_NO").alias("WT_NO"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("BODY_FAT_PCT").alias("BODY_FAT_PCT"),
    F.col("CUR_SMOKER_RISK_IN").alias("CUR_SMOKER_RISK_IN"),
    F.col("DBTC_RISK_IN").alias("DBTC_RISK_IN"),
    F.col("EXRCS_LACK_RISK_IN").alias("EXRCS_LACK_RISK_IN"),
    F.col("FRMR_SMOKER_RISK_IN").alias("FRMR_SMOKER_RISK_IN"),
    F.col("FSTNG_IN").alias("FSTNG_IN"),
    F.col("HA1C_NO").alias("HA1C_NO"),
    F.col("HEART_DSS_RISK_IN").alias("HEART_DSS_RISK_IN"),
    F.col("HI_BP_RISK_IN").alias("HI_BP_RISK_IN"),
    F.col("HI_CHLSTRL_RISK_IN").alias("HI_CHLSTRL_RISK_IN"),
    F.col("HLTH_SCRN_MBR_GNDR_CD").alias("HLTH_SCRN_MBR_GNDR_CD"),
    F.col("OVERWT_RISK_IN").alias("OVERWT_RISK_IN"),
    F.col("PRGNCY_IN").alias("PRGNCY_IN"),
    F.col("RFRL_TO_DM_IN").alias("RFRL_TO_DM_IN"),
    F.col("RFRL_TO_PHYS_IN").alias("RFRL_TO_PHYS_IN"),
    F.col("RESCRN_IN").alias("RESCRN_IN"),
    F.col("STROKE_RISK_IN").alias("STROKE_RISK_IN"),
    F.col("STRS_RISK_IN").alias("STRS_RISK_IN"),
    F.col("TSH_NO").alias("TSH_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

temp_table_name = "STAGING.IdsDmMbrshDmMbrHlthScrnLoad_Odbc_MBRSH_DM_MBR_HLTH_SCRN_out_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_cpy_forBuffer.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_MBR_HLTH_SCRN AS T
USING {temp_table_name} AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_HLTH_SCRN_DT = S.LAST_HLTH_SCRN_DT,
    T.BMI_NO = S.BMI_NO,
    T.CHLSTRL_RATIO_NO = S.CHLSTRL_RATIO_NO,
    T.DIASTOLIC_BP_NO = S.DIASTOLIC_BP_NO,
    T.GLUCOSE_NO = S.GLUCOSE_NO,
    T.HDL_NO = S.HDL_NO,
    T.HT_INCH_NO = S.HT_INCH_NO,
    T.LDL_NO = S.LDL_NO,
    T.MBR_AGE_NO = S.MBR_AGE_NO,
    T.PSA_NO = S.PSA_NO,
    T.SYSTOLIC_BP_NO = S.SYSTOLIC_BP_NO,
    T.TOT_CHLSTRL_NO = S.TOT_CHLSTRL_NO,
    T.TGL_NO = S.TGL_NO,
    T.WAIST_CRCMFR_NO = S.WAIST_CRCMFR_NO,
    T.WT_NO = S.WT_NO,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.BODY_FAT_PCT = S.BODY_FAT_PCT,
    T.CUR_SMOKER_RISK_IN = S.CUR_SMOKER_RISK_IN,
    T.DBTC_RISK_IN = S.DBTC_RISK_IN,
    T.EXRCS_LACK_RISK_IN = S.EXRCS_LACK_RISK_IN,
    T.FRMR_SMOKER_RISK_IN = S.FRMR_SMOKER_RISK_IN,
    T.FSTNG_IN = S.FSTNG_IN,
    T.HA1C_NO = S.HA1C_NO,
    T.HEART_DSS_RISK_IN = S.HEART_DSS_RISK_IN,
    T.HI_BP_RISK_IN = S.HI_BP_RISK_IN,
    T.HI_CHLSTRL_RISK_IN = S.HI_CHLSTRL_RISK_IN,
    T.HLTH_SCRN_MBR_GNDR_CD = S.HLTH_SCRN_MBR_GNDR_CD,
    T.OVERWT_RISK_IN = S.OVERWT_RISK_IN,
    T.PRGNCY_IN = S.PRGNCY_IN,
    T.RFRL_TO_DM_IN = S.RFRL_TO_DM_IN,
    T.RFRL_TO_PHYS_IN = S.RFRL_TO_PHYS_IN,
    T.RESCRN_IN = S.RESCRN_IN,
    T.STROKE_RISK_IN = S.STROKE_RISK_IN,
    T.STRS_RISK_IN = S.STRS_RISK_IN,
    T.TSH_NO = S.TSH_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    LAST_HLTH_SCRN_DT,
    BMI_NO,
    CHLSTRL_RATIO_NO,
    DIASTOLIC_BP_NO,
    GLUCOSE_NO,
    HDL_NO,
    HT_INCH_NO,
    LDL_NO,
    MBR_AGE_NO,
    PSA_NO,
    SYSTOLIC_BP_NO,
    TOT_CHLSTRL_NO,
    TGL_NO,
    WAIST_CRCMFR_NO,
    WT_NO,
    LAST_UPDT_RUN_CYC_NO,
    BODY_FAT_PCT,
    CUR_SMOKER_RISK_IN,
    DBTC_RISK_IN,
    EXRCS_LACK_RISK_IN,
    FRMR_SMOKER_RISK_IN,
    FSTNG_IN,
    HA1C_NO,
    HEART_DSS_RISK_IN,
    HI_BP_RISK_IN,
    HI_CHLSTRL_RISK_IN,
    HLTH_SCRN_MBR_GNDR_CD,
    OVERWT_RISK_IN,
    PRGNCY_IN,
    RFRL_TO_DM_IN,
    RFRL_TO_PHYS_IN,
    RESCRN_IN,
    STROKE_RISK_IN,
    STRS_RISK_IN,
    TSH_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.LAST_HLTH_SCRN_DT,
    S.BMI_NO,
    S.CHLSTRL_RATIO_NO,
    S.DIASTOLIC_BP_NO,
    S.GLUCOSE_NO,
    S.HDL_NO,
    S.HT_INCH_NO,
    S.LDL_NO,
    S.MBR_AGE_NO,
    S.PSA_NO,
    S.SYSTOLIC_BP_NO,
    S.TOT_CHLSTRL_NO,
    S.TGL_NO,
    S.WAIST_CRCMFR_NO,
    S.WT_NO,
    S.LAST_UPDT_RUN_CYC_NO,
    S.BODY_FAT_PCT,
    S.CUR_SMOKER_RISK_IN,
    S.DBTC_RISK_IN,
    S.EXRCS_LACK_RISK_IN,
    S.FRMR_SMOKER_RISK_IN,
    S.FSTNG_IN,
    S.HA1C_NO,
    S.HEART_DSS_RISK_IN,
    S.HI_BP_RISK_IN,
    S.HI_CHLSTRL_RISK_IN,
    S.HLTH_SCRN_MBR_GNDR_CD,
    S.OVERWT_RISK_IN,
    S.PRGNCY_IN,
    S.RFRL_TO_DM_IN,
    S.RFRL_TO_PHYS_IN,
    S.RESCRN_IN,
    S.STROKE_RISK_IN,
    S.STRS_RISK_IN,
    S.TSH_NO
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_MBRSH_DM_MBR_HLTH_SCRN_out_rej = spark.createDataFrame([], StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
]))

df_seq_MBRSH_DM_MBR_HLTH_SCRN_csv_rej = df_odbc_MBRSH_DM_MBR_HLTH_SCRN_out_rej.select(
    F.col("ERRORCODE"),
    F.col("ERRORTEXT")
)

write_files(
    df_seq_MBRSH_DM_MBR_HLTH_SCRN_csv_rej,
    f"{adls_path}/load/MBRSH_DM_FMLY_LMT_Rej.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)