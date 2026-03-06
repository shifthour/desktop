# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/27/2013          5114                             Movies Data from IDS to  CLM_DM_CLM_LN                             IntegrateWrhsDevl              Jag Yelavarthi              2013-12-01

# MAGIC Job Name: IdsDmClmMartClmLnLoad
# MAGIC Read Load File created in the IdsDmClmMartClmLnLoad Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_CLM_LN  Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

# Schema for seq_CLM_DM_CLM_LN_csv_load
schema_seq_CLM_DM_CLM_LN_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_RVNU_CD", StringType(), nullable=False),
    StructField("CLM_LN_ROOM_PRICE_METH_CD", StringType(), nullable=False),
    StructField("CLM_LN_TOS_CD", StringType(), nullable=False),
    StructField("PROC_CD", StringType(), nullable=False),
    StructField("CLM_LN_CAP_LN_IN", StringType(), nullable=False),
    StructField("CLM_LN_SVC_STRT_DT", TimestampType(), nullable=True),
    StructField("CLM_LN_SVC_END_DT", TimestampType(), nullable=True),
    StructField("CLM_LN_CHRG_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_PAYBL_TO_PROV_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_UNIT_CT", IntegerType(), nullable=False),
    StructField("CLM_LN_SVC_ID", StringType(), nullable=False),
    StructField("PROC_CD_DESC", StringType(), nullable=False),
    StructField("CLM_LN_POS_CD", StringType(), nullable=True),
    StructField("CLM_LN_POS_NM", StringType(), nullable=False),
    StructField("CLM_LN_PREAUTH_CD", StringType(), nullable=True),
    StructField("CLM_LN_PREAUTH_NM", StringType(), nullable=False),
    StructField("CLM_LN_RFRL_CD", StringType(), nullable=True),
    StructField("CLM_LN_RFRL_NM", StringType(), nullable=False),
    StructField("CLM_LN_RVNU_NM", StringType(), nullable=False),
    StructField("CLM_LN_ROOM_TYP_CD", StringType(), nullable=True),
    StructField("CLM_LN_ROOM_TYP_NM", StringType(), nullable=False),
    StructField("CLM_LN_ALW_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_COINS_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_CNSD_CHRG_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_COPAY_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_DEDCT_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_DSALW_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_PAYBL_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_ALW_PRICE_UNIT_CT", IntegerType(), nullable=False),
    StructField("DSALW_EXCD_ID", StringType(), nullable=False),
    StructField("DSALW_EXCD_DESC", StringType(), nullable=False),
    StructField("EOB_EXCD_ID", StringType(), nullable=False),
    StructField("PRI_DIAG_CD", StringType(), nullable=False),
    StructField("PRI_DIAG_DESC", StringType(), nullable=False),
    StructField("SVC_PROV_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_PCA_LOB_CD", StringType(), nullable=True),
    StructField("CLM_LN_PCA_LOB_NM", StringType(), nullable=True),
    StructField("CLM_LN_PCA_PRCS_CD", StringType(), nullable=True),
    StructField("CLM_LN_PCA_PRCS_NM", StringType(), nullable=True),
    StructField("CLM_LN_PCA_CNSD_AMT", DecimalType(38,10), nullable=True),
    StructField("CLM_LN_PCA_DSALW_AMT", DecimalType(38,10), nullable=True),
    StructField("CLM_LN_PCA_NONCNSD_AMT", DecimalType(38,10), nullable=True),
    StructField("CLM_LN_PCA_PD_AMT", DecimalType(38,10), nullable=True),
    StructField("CLM_LN_PCA_PROV_PD_AMT", DecimalType(38,10), nullable=True),
    StructField("CLM_LN_PCA_SUB_PD_AMT", DecimalType(38,10), nullable=True),
    StructField("PCA_DSALW_EXCD_ID", StringType(), nullable=True),
    StructField("PCA_DSALW_EXCD_DESC", StringType(), nullable=True),
    StructField("CLM_LN_REMIT_PATN_RESP_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_REMIT_PROV_WRT_OFF_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_REMIT_NO_RESP_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_LN_FINL_DISP_CD", StringType(), nullable=True),
    StructField("CLM_LN_FINL_DISP_NM", StringType(), nullable=True)
])

df_seq_CLM_DM_CLM_LN_csv_load = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_CLM_DM_CLM_LN_csv_load)
    .csv(f"{adls_path}/load/CLM_DM_CLM_LN.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_LN_csv_load.select(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_RVNU_CD"),
    col("CLM_LN_ROOM_PRICE_METH_CD"),
    col("CLM_LN_TOS_CD"),
    col("PROC_CD"),
    col("CLM_LN_CAP_LN_IN"),
    col("CLM_LN_SVC_STRT_DT"),
    col("CLM_LN_SVC_END_DT"),
    col("CLM_LN_CHRG_AMT"),
    col("CLM_LN_PAYBL_TO_PROV_AMT"),
    col("CLM_LN_UNIT_CT"),
    col("CLM_LN_SVC_ID"),
    col("PROC_CD_DESC"),
    col("CLM_LN_POS_CD"),
    col("CLM_LN_POS_NM"),
    col("CLM_LN_PREAUTH_CD"),
    col("CLM_LN_PREAUTH_NM"),
    col("CLM_LN_RFRL_CD"),
    col("CLM_LN_RFRL_NM"),
    col("CLM_LN_RVNU_NM"),
    col("CLM_LN_ROOM_TYP_CD"),
    col("CLM_LN_ROOM_TYP_NM"),
    col("CLM_LN_ALW_AMT"),
    col("CLM_LN_COINS_AMT"),
    col("CLM_LN_CNSD_CHRG_AMT"),
    col("CLM_LN_COPAY_AMT"),
    col("CLM_LN_DEDCT_AMT"),
    col("CLM_LN_DSALW_AMT"),
    col("CLM_LN_PAYBL_AMT"),
    col("CLM_LN_ALW_PRICE_UNIT_CT"),
    col("DSALW_EXCD_ID"),
    col("DSALW_EXCD_DESC"),
    col("EOB_EXCD_ID"),
    col("PRI_DIAG_CD"),
    col("PRI_DIAG_DESC"),
    col("SVC_PROV_ID"),
    col("LAST_UPDT_RUN_CYC_NO"),
    col("CLM_LN_PCA_LOB_CD"),
    col("CLM_LN_PCA_LOB_NM"),
    col("CLM_LN_PCA_PRCS_CD"),
    col("CLM_LN_PCA_PRCS_NM"),
    col("CLM_LN_PCA_CNSD_AMT"),
    col("CLM_LN_PCA_DSALW_AMT"),
    col("CLM_LN_PCA_NONCNSD_AMT"),
    col("CLM_LN_PCA_PD_AMT"),
    col("CLM_LN_PCA_PROV_PD_AMT"),
    col("CLM_LN_PCA_SUB_PD_AMT"),
    col("PCA_DSALW_EXCD_ID"),
    col("PCA_DSALW_EXCD_DESC"),
    col("CLM_LN_REMIT_PATN_RESP_AMT"),
    col("CLM_LN_REMIT_PROV_WRT_OFF_AMT"),
    col("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT"),
    col("CLM_LN_REMIT_NO_RESP_AMT"),
    col("CLM_LN_FINL_DISP_CD"),
    col("CLM_LN_FINL_DISP_NM")
)

df_Odbc_CLM_DM_CLM_LN_out = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad(col("CLM_LN_RVNU_CD"), <...>, " ").alias("CLM_LN_RVNU_CD"),
    rpad(col("CLM_LN_ROOM_PRICE_METH_CD"), <...>, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    rpad(col("CLM_LN_TOS_CD"), <...>, " ").alias("CLM_LN_TOS_CD"),
    rpad(col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    rpad(col("CLM_LN_CAP_LN_IN"), 1, " ").alias("CLM_LN_CAP_LN_IN"),
    col("CLM_LN_SVC_STRT_DT").alias("CLM_LN_SVC_STRT_DT"),
    col("CLM_LN_SVC_END_DT").alias("CLM_LN_SVC_END_DT"),
    col("CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    col("CLM_LN_PAYBL_TO_PROV_AMT").alias("CLM_LN_PAYBL_TO_PROV_AMT"),
    col("CLM_LN_UNIT_CT").alias("CLM_LN_UNIT_CT"),
    rpad(col("CLM_LN_SVC_ID"), <...>, " ").alias("CLM_LN_SVC_ID"),
    rpad(col("PROC_CD_DESC"), <...>, " ").alias("PROC_CD_DESC"),
    rpad(col("CLM_LN_POS_CD"), <...>, " ").alias("CLM_LN_POS_CD"),
    rpad(col("CLM_LN_POS_NM"), <...>, " ").alias("CLM_LN_POS_NM"),
    rpad(col("CLM_LN_PREAUTH_CD"), <...>, " ").alias("CLM_LN_PREAUTH_CD"),
    rpad(col("CLM_LN_PREAUTH_NM"), <...>, " ").alias("CLM_LN_PREAUTH_NM"),
    rpad(col("CLM_LN_RFRL_CD"), <...>, " ").alias("CLM_LN_RFRL_CD"),
    rpad(col("CLM_LN_RFRL_NM"), <...>, " ").alias("CLM_LN_RFRL_NM"),
    rpad(col("CLM_LN_RVNU_NM"), <...>, " ").alias("CLM_LN_RVNU_NM"),
    rpad(col("CLM_LN_ROOM_TYP_CD"), <...>, " ").alias("CLM_LN_ROOM_TYP_CD"),
    rpad(col("CLM_LN_ROOM_TYP_NM"), <...>, " ").alias("CLM_LN_ROOM_TYP_NM"),
    col("CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
    col("CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
    col("CLM_LN_CNSD_CHRG_AMT").alias("CLM_LN_CNSD_CHRG_AMT"),
    col("CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
    col("CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
    col("CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
    col("CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    col("CLM_LN_ALW_PRICE_UNIT_CT").alias("CLM_LN_ALW_PRICE_UNIT_CT"),
    rpad(col("DSALW_EXCD_ID"), <...>, " ").alias("DSALW_EXCD_ID"),
    rpad(col("DSALW_EXCD_DESC"), <...>, " ").alias("DSALW_EXCD_DESC"),
    rpad(col("EOB_EXCD_ID"), <...>, " ").alias("EOB_EXCD_ID"),
    rpad(col("PRI_DIAG_CD"), <...>, " ").alias("PRI_DIAG_CD"),
    rpad(col("PRI_DIAG_DESC"), <...>, " ").alias("PRI_DIAG_DESC"),
    rpad(col("SVC_PROV_ID"), <...>, " ").alias("SVC_PROV_ID"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("CLM_LN_PCA_LOB_CD"), <...>, " ").alias("CLM_LN_PCA_LOB_CD"),
    rpad(col("CLM_LN_PCA_LOB_NM"), <...>, " ").alias("CLM_LN_PCA_LOB_NM"),
    rpad(col("CLM_LN_PCA_PRCS_CD"), <...>, " ").alias("CLM_LN_PCA_PRCS_CD"),
    rpad(col("CLM_LN_PCA_PRCS_NM"), <...>, " ").alias("CLM_LN_PCA_PRCS_NM"),
    col("CLM_LN_PCA_CNSD_AMT").alias("CLM_LN_PCA_CNSD_AMT"),
    col("CLM_LN_PCA_DSALW_AMT").alias("CLM_LN_PCA_DSALW_AMT"),
    col("CLM_LN_PCA_NONCNSD_AMT").alias("CLM_LN_PCA_NONCNSD_AMT"),
    col("CLM_LN_PCA_PD_AMT").alias("CLM_LN_PCA_PD_AMT"),
    col("CLM_LN_PCA_PROV_PD_AMT").alias("CLM_LN_PCA_PROV_PD_AMT"),
    col("CLM_LN_PCA_SUB_PD_AMT").alias("CLM_LN_PCA_SUB_PD_AMT"),
    rpad(col("PCA_DSALW_EXCD_ID"), <...>, " ").alias("PCA_DSALW_EXCD_ID"),
    rpad(col("PCA_DSALW_EXCD_DESC"), <...>, " ").alias("PCA_DSALW_EXCD_DESC"),
    col("CLM_LN_REMIT_PATN_RESP_AMT").alias("CLM_LN_REMIT_PATN_RESP_AMT"),
    col("CLM_LN_REMIT_PROV_WRT_OFF_AMT").alias("CLM_LN_REMIT_PROV_WRT_OFF_AMT"),
    col("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT").alias("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT"),
    col("CLM_LN_REMIT_NO_RESP_AMT").alias("CLM_LN_REMIT_NO_RESP_AMT"),
    rpad(col("CLM_LN_FINL_DISP_CD"), <...>, " ").alias("CLM_LN_FINL_DISP_CD"),
    rpad(col("CLM_LN_FINL_DISP_NM"), <...>, " ").alias("CLM_LN_FINL_DISP_NM")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmClmMartClmLnLoad_Odbc_CLM_DM_CLM_LN_out_temp",
    jdbc_url,
    jdbc_props
)

df_Odbc_CLM_DM_CLM_LN_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmMartClmLnLoad_Odbc_CLM_DM_CLM_LN_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_LN AS T
USING STAGING.IdsDmClmMartClmLnLoad_Odbc_CLM_DM_CLM_LN_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN UPDATE SET
    T.CLM_LN_RVNU_CD = S.CLM_LN_RVNU_CD,
    T.CLM_LN_ROOM_PRICE_METH_CD = S.CLM_LN_ROOM_PRICE_METH_CD,
    T.CLM_LN_TOS_CD = S.CLM_LN_TOS_CD,
    T.PROC_CD = S.PROC_CD,
    T.CLM_LN_CAP_LN_IN = S.CLM_LN_CAP_LN_IN,
    T.CLM_LN_SVC_STRT_DT = S.CLM_LN_SVC_STRT_DT,
    T.CLM_LN_SVC_END_DT = S.CLM_LN_SVC_END_DT,
    T.CLM_LN_CHRG_AMT = S.CLM_LN_CHRG_AMT,
    T.CLM_LN_PAYBL_TO_PROV_AMT = S.CLM_LN_PAYBL_TO_PROV_AMT,
    T.CLM_LN_UNIT_CT = S.CLM_LN_UNIT_CT,
    T.CLM_LN_SVC_ID = S.CLM_LN_SVC_ID,
    T.PROC_CD_DESC = S.PROC_CD_DESC,
    T.CLM_LN_POS_CD = S.CLM_LN_POS_CD,
    T.CLM_LN_POS_NM = S.CLM_LN_POS_NM,
    T.CLM_LN_PREAUTH_CD = S.CLM_LN_PREAUTH_CD,
    T.CLM_LN_PREAUTH_NM = S.CLM_LN_PREAUTH_NM,
    T.CLM_LN_RFRL_CD = S.CLM_LN_RFRL_CD,
    T.CLM_LN_RFRL_NM = S.CLM_LN_RFRL_NM,
    T.CLM_LN_RVNU_NM = S.CLM_LN_RVNU_NM,
    T.CLM_LN_ROOM_TYP_CD = S.CLM_LN_ROOM_TYP_CD,
    T.CLM_LN_ROOM_TYP_NM = S.CLM_LN_ROOM_TYP_NM,
    T.CLM_LN_ALW_AMT = S.CLM_LN_ALW_AMT,
    T.CLM_LN_COINS_AMT = S.CLM_LN_COINS_AMT,
    T.CLM_LN_CNSD_CHRG_AMT = S.CLM_LN_CNSD_CHRG_AMT,
    T.CLM_LN_COPAY_AMT = S.CLM_LN_COPAY_AMT,
    T.CLM_LN_DEDCT_AMT = S.CLM_LN_DEDCT_AMT,
    T.CLM_LN_DSALW_AMT = S.CLM_LN_DSALW_AMT,
    T.CLM_LN_PAYBL_AMT = S.CLM_LN_PAYBL_AMT,
    T.CLM_LN_ALW_PRICE_UNIT_CT = S.CLM_LN_ALW_PRICE_UNIT_CT,
    T.DSALW_EXCD_ID = S.DSALW_EXCD_ID,
    T.DSALW_EXCD_DESC = S.DSALW_EXCD_DESC,
    T.EOB_EXCD_ID = S.EOB_EXCD_ID,
    T.PRI_DIAG_CD = S.PRI_DIAG_CD,
    T.PRI_DIAG_DESC = S.PRI_DIAG_DESC,
    T.SVC_PROV_ID = S.SVC_PROV_ID,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.CLM_LN_PCA_LOB_CD = S.CLM_LN_PCA_LOB_CD,
    T.CLM_LN_PCA_LOB_NM = S.CLM_LN_PCA_LOB_NM,
    T.CLM_LN_PCA_PRCS_CD = S.CLM_LN_PCA_PRCS_CD,
    T.CLM_LN_PCA_PRCS_NM = S.CLM_LN_PCA_PRCS_NM,
    T.CLM_LN_PCA_CNSD_AMT = S.CLM_LN_PCA_CNSD_AMT,
    T.CLM_LN_PCA_DSALW_AMT = S.CLM_LN_PCA_DSALW_AMT,
    T.CLM_LN_PCA_NONCNSD_AMT = S.CLM_LN_PCA_NONCNSD_AMT,
    T.CLM_LN_PCA_PD_AMT = S.CLM_LN_PCA_PD_AMT,
    T.CLM_LN_PCA_PROV_PD_AMT = S.CLM_LN_PCA_PROV_PD_AMT,
    T.CLM_LN_PCA_SUB_PD_AMT = S.CLM_LN_PCA_SUB_PD_AMT,
    T.PCA_DSALW_EXCD_ID = S.PCA_DSALW_EXCD_ID,
    T.PCA_DSALW_EXCD_DESC = S.PCA_DSALW_EXCD_DESC,
    T.CLM_LN_REMIT_PATN_RESP_AMT = S.CLM_LN_REMIT_PATN_RESP_AMT,
    T.CLM_LN_REMIT_PROV_WRT_OFF_AMT = S.CLM_LN_REMIT_PROV_WRT_OFF_AMT,
    T.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT = S.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
    T.CLM_LN_REMIT_NO_RESP_AMT = S.CLM_LN_REMIT_NO_RESP_AMT,
    T.CLM_LN_FINL_DISP_CD = S.CLM_LN_FINL_DISP_CD,
    T.CLM_LN_FINL_DISP_NM = S.CLM_LN_FINL_DISP_NM
WHEN NOT MATCHED THEN INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_LN_RVNU_CD,
    CLM_LN_ROOM_PRICE_METH_CD,
    CLM_LN_TOS_CD,
    PROC_CD,
    CLM_LN_CAP_LN_IN,
    CLM_LN_SVC_STRT_DT,
    CLM_LN_SVC_END_DT,
    CLM_LN_CHRG_AMT,
    CLM_LN_PAYBL_TO_PROV_AMT,
    CLM_LN_UNIT_CT,
    CLM_LN_SVC_ID,
    PROC_CD_DESC,
    CLM_LN_POS_CD,
    CLM_LN_POS_NM,
    CLM_LN_PREAUTH_CD,
    CLM_LN_PREAUTH_NM,
    CLM_LN_RFRL_CD,
    CLM_LN_RFRL_NM,
    CLM_LN_RVNU_NM,
    CLM_LN_ROOM_TYP_CD,
    CLM_LN_ROOM_TYP_NM,
    CLM_LN_ALW_AMT,
    CLM_LN_COINS_AMT,
    CLM_LN_CNSD_CHRG_AMT,
    CLM_LN_COPAY_AMT,
    CLM_LN_DEDCT_AMT,
    CLM_LN_DSALW_AMT,
    CLM_LN_PAYBL_AMT,
    CLM_LN_ALW_PRICE_UNIT_CT,
    DSALW_EXCD_ID,
    DSALW_EXCD_DESC,
    EOB_EXCD_ID,
    PRI_DIAG_CD,
    PRI_DIAG_DESC,
    SVC_PROV_ID,
    LAST_UPDT_RUN_CYC_NO,
    CLM_LN_PCA_LOB_CD,
    CLM_LN_PCA_LOB_NM,
    CLM_LN_PCA_PRCS_CD,
    CLM_LN_PCA_PRCS_NM,
    CLM_LN_PCA_CNSD_AMT,
    CLM_LN_PCA_DSALW_AMT,
    CLM_LN_PCA_NONCNSD_AMT,
    CLM_LN_PCA_PD_AMT,
    CLM_LN_PCA_PROV_PD_AMT,
    CLM_LN_PCA_SUB_PD_AMT,
    PCA_DSALW_EXCD_ID,
    PCA_DSALW_EXCD_DESC,
    CLM_LN_REMIT_PATN_RESP_AMT,
    CLM_LN_REMIT_PROV_WRT_OFF_AMT,
    CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
    CLM_LN_REMIT_NO_RESP_AMT,
    CLM_LN_FINL_DISP_CD,
    CLM_LN_FINL_DISP_NM
)
VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_LN_RVNU_CD,
    S.CLM_LN_ROOM_PRICE_METH_CD,
    S.CLM_LN_TOS_CD,
    S.PROC_CD,
    S.CLM_LN_CAP_LN_IN,
    S.CLM_LN_SVC_STRT_DT,
    S.CLM_LN_SVC_END_DT,
    S.CLM_LN_CHRG_AMT,
    S.CLM_LN_PAYBL_TO_PROV_AMT,
    S.CLM_LN_UNIT_CT,
    S.CLM_LN_SVC_ID,
    S.PROC_CD_DESC,
    S.CLM_LN_POS_CD,
    S.CLM_LN_POS_NM,
    S.CLM_LN_PREAUTH_CD,
    S.CLM_LN_PREAUTH_NM,
    S.CLM_LN_RFRL_CD,
    S.CLM_LN_RFRL_NM,
    S.CLM_LN_RVNU_NM,
    S.CLM_LN_ROOM_TYP_CD,
    S.CLM_LN_ROOM_TYP_NM,
    S.CLM_LN_ALW_AMT,
    S.CLM_LN_COINS_AMT,
    S.CLM_LN_CNSD_CHRG_AMT,
    S.CLM_LN_COPAY_AMT,
    S.CLM_LN_DEDCT_AMT,
    S.CLM_LN_DSALW_AMT,
    S.CLM_LN_PAYBL_AMT,
    S.CLM_LN_ALW_PRICE_UNIT_CT,
    S.DSALW_EXCD_ID,
    S.DSALW_EXCD_DESC,
    S.EOB_EXCD_ID,
    S.PRI_DIAG_CD,
    S.PRI_DIAG_DESC,
    S.SVC_PROV_ID,
    S.LAST_UPDT_RUN_CYC_NO,
    S.CLM_LN_PCA_LOB_CD,
    S.CLM_LN_PCA_LOB_NM,
    S.CLM_LN_PCA_PRCS_CD,
    S.CLM_LN_PCA_PRCS_NM,
    S.CLM_LN_PCA_CNSD_AMT,
    S.CLM_LN_PCA_DSALW_AMT,
    S.CLM_LN_PCA_NONCNSD_AMT,
    S.CLM_LN_PCA_PD_AMT,
    S.CLM_LN_PCA_PROV_PD_AMT,
    S.CLM_LN_PCA_SUB_PD_AMT,
    S.PCA_DSALW_EXCD_ID,
    S.PCA_DSALW_EXCD_DESC,
    S.CLM_LN_REMIT_PATN_RESP_AMT,
    S.CLM_LN_REMIT_PROV_WRT_OFF_AMT,
    S.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
    S.CLM_LN_REMIT_NO_RESP_AMT,
    S.CLM_LN_FINL_DISP_CD,
    S.CLM_LN_FINL_DISP_NM
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_odbc_CLM_DM_CLM_LN_out_rej = spark.createDataFrame([], schema_rej)
df_odbc_CLM_DM_CLM_LN_out_rej = df_odbc_CLM_DM_CLM_LN_out_rej.select(
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)
write_files(
    df_odbc_CLM_DM_CLM_LN_out_rej,
    f"{adls_path}/load/CLM_DM_CLM_LN_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)