# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     09/18/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi               2013-12-01

# MAGIC Job Name: IdsDmWClmInitLoad
# MAGIC Read Load File created in the IdsDmClmDmClmExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Replace the W_CLM_INIT Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_W_CLM_INIT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_FINL_DISP_CD", StringType(), False),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("CLM_TRNSMSN_STTUS_CD", StringType(), False),
    StructField("CLM_TYP_CD", StringType(), False),
    StructField("CLM_REMIT_HIST_SUPRES_EOB_IN", StringType(), False),
    StructField("CLM_REMIT_HIST_SUPRES_REMIT_IN", StringType(), False),
    StructField("CMPL_MBRSH_IN", StringType(), False),
    StructField("CLM_RCVD_DT", TimestampType(), True),
    StructField("CLM_SRCH_DT", TimestampType(), True),
    StructField("CLM_SVC_STRT_DT", TimestampType(), True),
    StructField("CLM_SVC_END_DT", TimestampType(), True),
    StructField("MBR_BRTH_DT", TimestampType(), True),
    StructField("CLM_CHRG_AMT", DecimalType(38,10), False),
    StructField("CLM_PAYBL_AMT", DecimalType(38,10), False),
    StructField("CLM_REMIT_HIST_CNSD_CHRG_AMT", DecimalType(38,10), False),
    StructField("CLM_REMIT_HIST_NO_RESP_AMT", DecimalType(38,10), False),
    StructField("CLM_REMIT_HIST_PATN_RESP_AMT", DecimalType(38,10), False),
    StructField("ALPHA_PFX_SUB_ID", StringType(), False),
    StructField("CLM_SUB_ID", StringType(), False),
    StructField("CMN_PRCT_PRSCRB_CMN_PRCT_ID", StringType(), False),
    StructField("GRP_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_MIDINIT", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("PROV_BILL_SVC_ID", StringType(), False),
    StructField("PROV_PD_PROV_ID", StringType(), False),
    StructField("PROV_PCP_PROV_ID", StringType(), False),
    StructField("PROV_REL_GRP_PROV_ID", StringType(), False),
    StructField("PROV_REL_IPA_PROV_ID", StringType(), False),
    StructField("PROV_SVC_PROV_ID", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("CMT_CT", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("PCA_TYP_CD", StringType(), True)
])

df_seq_W_CLM_INIT_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_W_CLM_INIT_csv_load)
    .csv(f"{adls_path}/load/W_CLM_INIT.dat")
)

df_cpy_forBuffer = df_seq_W_CLM_INIT_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_FINL_DISP_CD").alias("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD").alias("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_REMIT_HIST_SUPRES_EOB_IN").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    col("CLM_REMIT_HIST_SUPRES_REMIT_IN").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    col("CMPL_MBRSH_IN").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT").alias("CLM_RCVD_DT"),
    col("CLM_SRCH_DT").alias("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT").alias("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT").alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT").alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT").alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID").alias("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID").alias("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID").alias("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID").alias("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID").alias("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("CMT_CT").alias("CMT_CT"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("PCA_TYP_CD").alias("PCA_TYP_CD")
)

jdbc_url_clm, jdbc_props_clm = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmWClmInitLoad_Odbc_W_CLM_INIT_out_temp",
    jdbc_url_clm,
    jdbc_props_clm
)

(
    df_cpy_forBuffer.write
    .format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("dbtable", "STAGING.IdsDmWClmInitLoad_Odbc_W_CLM_INIT_out_temp")
    .mode("overwrite")
    .save()
)

execute_dml(
    f"TRUNCATE TABLE {ClmMartOwner}.W_CLM_INIT",
    jdbc_url_clm,
    jdbc_props_clm
)

insert_sql = (
    f"INSERT INTO {ClmMartOwner}.W_CLM_INIT "
    f"(SRC_SYS_CD, CLM_ID, CLM_FINL_DISP_CD, CLM_STTUS_CD, CLM_TRNSMSN_STTUS_CD, CLM_TYP_CD, "
    f"CLM_REMIT_HIST_SUPRES_EOB_IN, CLM_REMIT_HIST_SUPRES_REMIT_IN, CMPL_MBRSH_IN, CLM_RCVD_DT, "
    f"CLM_SRCH_DT, CLM_SVC_STRT_DT, CLM_SVC_END_DT, MBR_BRTH_DT, CLM_CHRG_AMT, CLM_PAYBL_AMT, "
    f"CLM_REMIT_HIST_CNSD_CHRG_AMT, CLM_REMIT_HIST_NO_RESP_AMT, CLM_REMIT_HIST_PATN_RESP_AMT, "
    f"ALPHA_PFX_SUB_ID, CLM_SUB_ID, CMN_PRCT_PRSCRB_CMN_PRCT_ID, GRP_UNIQ_KEY, GRP_ID, GRP_NM, "
    f"MBR_UNIQ_KEY, MBR_FIRST_NM, MBR_MIDINIT, MBR_LAST_NM, PROV_BILL_SVC_ID, PROV_PD_PROV_ID, "
    f"PROV_PCP_PROV_ID, PROV_REL_GRP_PROV_ID, PROV_REL_IPA_PROV_ID, PROV_SVC_PROV_ID, "
    f"SUB_UNIQ_KEY, CMT_CT, LAST_UPDT_RUN_CYC_NO, PCA_TYP_CD) "
    f"SELECT SRC_SYS_CD, CLM_ID, CLM_FINL_DISP_CD, CLM_STTUS_CD, CLM_TRNSMSN_STTUS_CD, CLM_TYP_CD, "
    f"CLM_REMIT_HIST_SUPRES_EOB_IN, CLM_REMIT_HIST_SUPRES_REMIT_IN, CMPL_MBRSH_IN, CLM_RCVD_DT, "
    f"CLM_SRCH_DT, CLM_SVC_STRT_DT, CLM_SVC_END_DT, MBR_BRTH_DT, CLM_CHRG_AMT, CLM_PAYBL_AMT, "
    f"CLM_REMIT_HIST_CNSD_CHRG_AMT, CLM_REMIT_HIST_NO_RESP_AMT, CLM_REMIT_HIST_PATN_RESP_AMT, "
    f"ALPHA_PFX_SUB_ID, CLM_SUB_ID, CMN_PRCT_PRSCRB_CMN_PRCT_ID, GRP_UNIQ_KEY, GRP_ID, GRP_NM, "
    f"MBR_UNIQ_KEY, MBR_FIRST_NM, MBR_MIDINIT, MBR_LAST_NM, PROV_BILL_SVC_ID, PROV_PD_PROV_ID, "
    f"PROV_PCP_PROV_ID, PROV_REL_GRP_PROV_ID, PROV_REL_IPA_PROV_ID, PROV_SVC_PROV_ID, "
    f"SUB_UNIQ_KEY, CMT_CT, LAST_UPDT_RUN_CYC_NO, PCA_TYP_CD "
    f"FROM STAGING.IdsDmWClmInitLoad_Odbc_W_CLM_INIT_out_temp"
)

execute_dml(insert_sql, jdbc_url_clm, jdbc_props_clm)

df_Odbc_W_CLM_INIT_out_rej = spark.createDataFrame(
    [],
    schema=StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_Odbc_W_CLM_INIT_out_rej_final = df_Odbc_W_CLM_INIT_out_rej.select(
    rpad(col("ERRORCODE"), 255, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 255, " ").alias("ERRORTEXT")
)

write_files(
    df_Odbc_W_CLM_INIT_out_rej_final,
    f"{adls_path}/load/W_CLM_INIT_Rej.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)