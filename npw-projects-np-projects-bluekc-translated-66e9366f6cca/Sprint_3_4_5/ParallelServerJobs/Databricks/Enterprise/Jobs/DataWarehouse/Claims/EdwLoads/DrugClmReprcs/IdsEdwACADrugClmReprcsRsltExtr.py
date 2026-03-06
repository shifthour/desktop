# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  
# MAGIC 
# MAGIC CALLED BY:  IdsEdwACADrugClmReprcsRsltSeq
# MAGIC 
# MAGIC Description:  Job extracts from the DRUG_CLM_ACA_REPRCS_RSLT_F table from the IDS database for loading to EDW database.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer                          Date            Project/User Story             Change Description                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------              -----------------   ----------------------------------------  --------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Rekha Radhakrishna      2020-10-23  F-203960 / US-297051         Initial Programming.                                                                 EnterpriseDev5        Reddy Sanam              2020-12-16

# MAGIC This job extracts from the DRUG_CLM_ACA_REPRCS_RSLT table from the IDS database. Job is called from ?? Seq.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
OptumIdsRunCycle = get_widget_value('OptumIdsRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_IDS_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT CD_MPPNG.CD_MPPNG_SK as CD_MPPNG_SK,CD_MPPNG.TRGT_CD as TRGT_CD,CD_MPPNG.TRGT_CD_NM as TRGT_CD_NM FROM "
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG"
    )
    .load()
)

df_IDS_DrugClmAcaReprcsRslt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \n"
        "DRUG_CLM_ACA_REPRCS_RSLT_SK,\n"
        "CLM_ID,\n"
        "CLM_REPRCS_SEQ_NO,\n"
        "SRC_SYS_CD,\n"
        "DRUG_CLM_SK,\n"
        "QHP_CSR_VRNT_CD_SK,\n"
        "CLM_REPRCS_DT_SK,\n"
        "CLM_SUBMT_DT_SK,\n"
        "CLM_SUBMT_TM,\n"
        "ORIG_CLM_RCVD_DT_SK,\n"
        "CLM_REPRCS_PAYBL_AMT,\n"
        "CLM_REPRCS_PATN_RESP_AMT,\n"
        "SHADOW_ADJDCT_PAYBL_AMT,\n"
        "SHADOW_ADJDCT_PATN_RESP_AMT,\n"
        "SHADOW_ADJDCT_COINS_AMT,\n"
        "SHADOW_ADJDCT_COPAY_AMT,\n"
        "SHADOW_ADJDCT_DEDCT_AMT,\n"
        "PBM_CALC_CSR_SBSDY_AMT\n"
        "FROM "
        + IDSOwner
        + ".DRUG_CLM_ACA_REPRCS_RSLT\n"
        "WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= "
        + OptumIdsRunCycle
    )
    .load()
)

df_CdMppngLkp_join = df_IDS_DrugClmAcaReprcsRslt.alias("IDS_Out_Link").join(
    df_IDS_CD_MPPNG.alias("LnkCdMppng"),
    F.col("IDS_Out_Link.QHP_CSR_VRNT_CD_SK") == F.col("LnkCdMppng.CD_MPPNG_SK"),
    "left",
)

df_LnkToTrans2 = df_CdMppngLkp_join.select(
    F.col("IDS_Out_Link.DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("IDS_Out_Link.CLM_ID").alias("CLM_ID"),
    F.col("IDS_Out_Link.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("IDS_Out_Link.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("IDS_Out_Link.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("IDS_Out_Link.QHP_CSR_VRNT_CD_SK").alias("QHP_CSR_VRNT_CD_SK"),
    F.col("IDS_Out_Link.CLM_REPRCS_DT_SK").alias("CLM_REPRCS_DT_SK"),
    F.col("IDS_Out_Link.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    F.col("IDS_Out_Link.CLM_SUBMT_TM").alias("CLM_SUBMT_TM"),
    F.col("IDS_Out_Link.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("IDS_Out_Link.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("IDS_Out_Link.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("IDS_Out_Link.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("IDS_Out_Link.SHADOW_ADJDCT_PATN_RESP_AMT").alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("IDS_Out_Link.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("IDS_Out_Link.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("IDS_Out_Link.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("IDS_Out_Link.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT"),
    F.col("LnkCdMppng.TRGT_CD").alias("TRGT_CD"),
    F.col("LnkCdMppng.TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_LnkToTrans2 = df_LnkToTrans2.withColumn(
    "CLM_REPRCS_DT_SK", rpad(F.col("CLM_REPRCS_DT_SK"), 10, " ")
).withColumn(
    "CLM_SUBMT_DT_SK", rpad(F.col("CLM_SUBMT_DT_SK"), 10, " ")
).withColumn(
    "ORIG_CLM_RCVD_DT_SK", rpad(F.col("ORIG_CLM_RCVD_DT_SK"), 10, " ")
)

df_infnlEdwLoadFile_0 = df_LnkToTrans2.select(
    F.col("DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DRUG_CLM_SK").alias("CLM_SK"),
    F.col("TRGT_CD").alias("QHP_CSR_VRNT_CD"),
    F.col("TRGT_CD_NM").alias("QHP_CSR_VRNT_NM"),
    F.col("CLM_REPRCS_DT_SK").alias("CLM_REPRCS_DT_SK"),
    F.col("CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    F.col("CLM_SUBMT_TM").alias("CLM_SUBMT_TM"),
    F.col("ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("SHADOW_ADJDCT_PATN_RESP_AMT").alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("QHP_CSR_VRNT_CD_SK").alias("QHP_CSR_VRNT_CD_SK"),
)

df_infnlEdwLoadFile_1 = df_infnlEdwLoadFile_0.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "CLM_REPRCS_DT_SK", rpad(F.col("CLM_REPRCS_DT_SK"), 10, " ")
).withColumn(
    "CLM_SUBMT_DT_SK", rpad(F.col("CLM_SUBMT_DT_SK"), 10, " ")
).withColumn(
    "ORIG_CLM_RCVD_DT_SK", rpad(F.col("ORIG_CLM_RCVD_DT_SK"), 10, " ")
)
df_infnlEdwLoadFile = df_infnlEdwLoadFile_1

df_In_Na_0 = df_LnkToTrans2.limit(1)
df_In_Na_1 = (
    df_In_Na_0.withColumn("DRUG_CLM_ACA_REPRCS_RSLT_SK", F.lit(1))
    .withColumn("CLM_ID", F.lit("NA"))
    .withColumn("CLM_REPRCS_SEQ_NO", F.lit(1))
    .withColumn("SRC_SYS_CD", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("CLM_SK", F.lit(1))
    .withColumn("QHP_CSR_VRNT_CD", F.lit(1))
    .withColumn("QHP_CSR_VRNT_NM", F.lit(1))
    .withColumn("CLM_REPRCS_DT_SK", F.lit(1))
    .withColumn("CLM_SUBMT_DT_SK", F.lit(1))
    .withColumn("CLM_SUBMT_TM", F.lit("00:00:00"))
    .withColumn("ORIG_CLM_RCVD_DT_SK", F.lit(1))
    .withColumn("CLM_REPRCS_PAYBL_AMT", F.lit(0.00))
    .withColumn("CLM_REPRCS_PATN_RESP_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_PAYBL_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_PATN_RESP_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_COINS_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_COPAY_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_DEDCT_AMT", F.lit(0.00))
    .withColumn("PBM_CALC_CSR_SBSDY_AMT", F.lit(0.00))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("QHP_CSR_VRNT_CD_SK", F.lit(1))
)
df_In_Na_2 = (
    df_In_Na_1.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_REPRCS_DT_SK", rpad(F.col("CLM_REPRCS_DT_SK"), 10, " "))
    .withColumn("CLM_SUBMT_DT_SK", rpad(F.col("CLM_SUBMT_DT_SK"), 10, " "))
    .withColumn("ORIG_CLM_RCVD_DT_SK", rpad(F.col("ORIG_CLM_RCVD_DT_SK"), 10, " "))
)
df_In_Na = df_In_Na_2

df_In_Unk_0 = df_LnkToTrans2.limit(1)
df_In_Unk_1 = (
    df_In_Unk_0.withColumn("DRUG_CLM_ACA_REPRCS_RSLT_SK", F.lit(0))
    .withColumn("CLM_ID", F.lit("NA"))
    .withColumn("CLM_REPRCS_SEQ_NO", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("CLM_SK", F.lit(0))
    .withColumn("QHP_CSR_VRNT_CD", F.lit(0))
    .withColumn("QHP_CSR_VRNT_NM", F.lit(0))
    .withColumn("CLM_REPRCS_DT_SK", F.lit(0))
    .withColumn("CLM_SUBMT_DT_SK", F.lit(0))
    .withColumn("CLM_SUBMT_TM", F.lit("00:00:00"))
    .withColumn("ORIG_CLM_RCVD_DT_SK", F.lit(0))
    .withColumn("CLM_REPRCS_PAYBL_AMT", F.lit(0.00))
    .withColumn("CLM_REPRCS_PATN_RESP_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_PAYBL_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_PATN_RESP_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_COINS_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_COPAY_AMT", F.lit(0.00))
    .withColumn("SHADOW_ADJDCT_DEDCT_AMT", F.lit(0.00))
    .withColumn("PBM_CALC_CSR_SBSDY_AMT", F.lit(0.00))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("QHP_CSR_VRNT_CD_SK", F.lit(0))
)
df_In_Unk_2 = (
    df_In_Unk_1.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_REPRCS_DT_SK", rpad(F.col("CLM_REPRCS_DT_SK"), 10, " "))
    .withColumn("CLM_SUBMT_DT_SK", rpad(F.col("CLM_SUBMT_DT_SK"), 10, " "))
    .withColumn("ORIG_CLM_RCVD_DT_SK", rpad(F.col("ORIG_CLM_RCVD_DT_SK"), 10, " "))
)
df_In_Unk = df_In_Unk_2

df_Funnel_8 = df_In_Na.unionByName(df_In_Unk).unionByName(df_infnlEdwLoadFile)

df_Funnel_8_final = df_Funnel_8.select(
    F.col("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SK"),
    F.col("QHP_CSR_VRNT_CD"),
    F.col("QHP_CSR_VRNT_NM"),
    rpad(F.col("CLM_REPRCS_DT_SK"), 10, " ").alias("CLM_REPRCS_DT_SK"),
    rpad(F.col("CLM_SUBMT_DT_SK"), 10, " ").alias("CLM_SUBMT_DT_SK"),
    F.col("CLM_SUBMT_TM"),
    rpad(F.col("ORIG_CLM_RCVD_DT_SK"), 10, " ").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("CLM_REPRCS_PAYBL_AMT"),
    F.col("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("PBM_CALC_CSR_SBSDY_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("QHP_CSR_VRNT_CD_SK"),
)

write_files(
    df_Funnel_8_final,
    f"{adls_path}/load/DRUG_CLM_ACA_REPRCS_RSLT_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)