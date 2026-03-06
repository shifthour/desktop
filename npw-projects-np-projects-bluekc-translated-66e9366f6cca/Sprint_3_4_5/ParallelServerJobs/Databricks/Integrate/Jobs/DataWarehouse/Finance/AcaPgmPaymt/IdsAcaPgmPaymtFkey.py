# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #             Change Description                                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                         ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2015-10-22         5128                              Initial Programming                                                                     IntegrateDev1           Bhoomi Dasari              10/23/2015

# MAGIC Perform Foreign Key Lookups to populate ACA_PGM_PAYMT table.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC FKEY failures are written into this flat file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

DSJobName = "IdsAcaPgmPaymtFkey"

# seq_ACA_PGM_PAYMT (PxSequentialFile) - Read
schema_seq_ACA_PGM_PAYMT = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", TimestampType(), True),
    StructField("ACA_PGM_PAYMT_SK", IntegerType(), True),
    StructField("ACTVTY_YR_MO", StringType(), True),
    StructField("PAYMT_COV_YR_MO", StringType(), True),
    StructField("PAYMT_TYP_CD", StringType(), True),
    StructField("ST_CD", StringType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("ACA_PGM_PAYMT_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("COV_STRT_DT_SK", StringType(), True),
    StructField("COV_END_DT_SK", StringType(), True),
    StructField("EFT_EFF_DT_SK", StringType(), True),
    StructField("ACA_PGM_TRANS_AMT", DecimalType(38,10), True),
    StructField("ACA_PGM_PAYMT_UNIQ_KEY", IntegerType(), True),
    StructField("EFT_TRACE_ID", StringType(), True),
    StructField("EXCH_RPT_DOC_CTL_ID", StringType(), True),
    StructField("EXCH_RPT_NM", StringType(), True),
    StructField("EFF_DT_SK", StringType(), True)
])

df_seq_ACA_PGM_PAYMT = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_ACA_PGM_PAYMT)
    .csv(f"{adls_path}/key/ACA_PGM_PAYMT.{SrcSysCd}.pkey.{RunID}.dat")
)

# db2_QHP_Lkp (DB2ConnectorPX)
jdbc_url_db2_QHP_Lkp, jdbc_props_db2_QHP_Lkp = get_db_config(ids_secret_name)
extract_query_db2_QHP_Lkp = f"SELECT QHP_ID, EFF_DT_SK, QHP_SK FROM {IDSOwner}.QHP"
df_db2_QHP_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_QHP_Lkp)
    .options(**jdbc_props_db2_QHP_Lkp)
    .option("query", extract_query_db2_QHP_Lkp)
    .load()
)

# ds_CD_MPPNG_LkpData (PxDataSet) - Read from parquet
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_FilterData (PxFilter)
df_fltr_FilterData_filtered = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == 'CMS') &
    (F.col("SRC_CLCTN_CD") == 'CMS') &
    (F.col("TRGT_CLCTN_CD") == 'IDS') &
    (F.col("SRC_DOMAIN_NM") == 'ENROLLMENT PAYMENT TYPE') &
    (F.col("TRGT_DOMAIN_NM") == 'ENROLLMENT PAYMENT TYPE')
)
df_fltr_FilterData = df_fltr_FilterData_filtered.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# db2_K_Clndr_Dt_Eff_Lkp (DB2ConnectorPX)
jdbc_url_db2_K_Clndr_Dt_Eff_Lkp, jdbc_props_db2_K_Clndr_Dt_Eff_Lkp = get_db_config(ids_secret_name)
extract_query_db2_K_Clndr_Dt_Eff_Lkp = f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"
df_db2_K_Clndr_Dt_Eff_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_Clndr_Dt_Eff_Lkp)
    .options(**jdbc_props_db2_K_Clndr_Dt_Eff_Lkp)
    .option("query", extract_query_db2_K_Clndr_Dt_Eff_Lkp)
    .load()
)

# Copy (PxCopy) => split into 3 identical outputs
df_CovEndDt = df_db2_K_Clndr_Dt_Eff_Lkp.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK")
)
df_CovStrtDt = df_db2_K_Clndr_Dt_Eff_Lkp.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK")
)
df_EftEffDt = df_db2_K_Clndr_Dt_Eff_Lkp.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK")
)

# lkp_Code_SKs (PxLookup)
df_lkp_Code_SKs = (
    df_seq_ACA_PGM_PAYMT.alias("Lnk_IdsAcaPgmPaymtFkey_InAbc")
    .join(
        df_fltr_FilterData.alias("Ref_PaymtTypCd_LNk"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.PAYMT_TYP_CD") == F.col("Ref_PaymtTypCd_LNk.SRC_CD"),
        "left"
    )
    .join(
        df_db2_QHP_Lkp.alias("Ref_QHP_In"),
        (
            (F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.QHP_ID") == F.col("Ref_QHP_In.QHP_ID")) &
            (F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.EFF_DT_SK") == F.col("Ref_QHP_In.EFF_DT_SK"))
        ),
        "left"
    )
    .join(
        df_CovStrtDt.alias("CovStrtDt"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.COV_STRT_DT_SK") == F.col("CovStrtDt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_CovEndDt.alias("CovEndDt"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.COV_END_DT_SK") == F.col("CovEndDt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_EftEffDt.alias("EftEffDt"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.EFT_EFF_DT_SK") == F.col("EftEffDt.CLNDR_DT_SK"),
        "left"
    )
    .select(
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.ACA_PGM_PAYMT_SK").alias("ACA_PGM_PAYMT_SK"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.ST_CD").alias("ST_CD"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.QHP_ID").alias("QHP_ID"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ref_QHP_In.QHP_SK").alias("QHP_SK"),
        F.col("Ref_PaymtTypCd_LNk.CD_MPPNG_SK").alias("PAYMT_TYP_CD_SK"),
        F.col("CovStrtDt.CLNDR_DT_SK").alias("COV_STRT_DT_SK"),
        F.col("CovEndDt.CLNDR_DT_SK").alias("COV_END_DT_SK"),
        F.col("EftEffDt.CLNDR_DT_SK").alias("EFT_EFF_DT_SK"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.ACA_PGM_TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.EFT_TRACE_ID").alias("EFT_TRACE_ID"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.EXCH_RPT_DOC_CTL_ID").alias("EXCH_RPT_DOC_CTL_ID"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
        F.col("Lnk_IdsAcaPgmPaymtFkey_InAbc.EFF_DT_SK").alias("EFF_DT_SK")
    )
)

# xfm_CheckLkpResults (CTransformerStage)
df_xfm_CheckLkpResults = (
    df_lkp_Code_SKs
    .withColumn(
        "SvQhpFKeyLkpCheck",
        F.when(F.col("QHP_SK").isNull() & (F.col("QHP_ID") != 'NA'), F.lit('Y')).otherwise(F.lit('N'))
    )
    .withColumn(
        "SvPaymtTypCdFKeyLkpCheck",
        F.when(F.col("PAYMT_TYP_CD_SK").isNull() & (F.col("PAYMT_TYP_CD") != 'NA'), F.lit('Y')).otherwise(F.lit('N'))
    )
    .withColumn(
        "svCovStrtDtFKeyLkpCheck",
        F.when(F.col("COV_STRT_DT_SK").isNull(), F.lit('Y')).otherwise(F.lit('N'))
    )
    .withColumn(
        "svCovEndDtFKeyLkpCheck",
        F.when(F.col("COV_END_DT_SK").isNull(), F.lit('Y')).otherwise(F.lit('N'))
    )
    .withColumn(
        "svEftEffDtFKeyLkpCheck",
        F.when(F.col("EFT_EFF_DT_SK").isNull(), F.lit('Y')).otherwise(F.lit('N'))
    )
)

df_Lnk_AcaPgmPaymt_Fkey_Main = df_xfm_CheckLkpResults.select(
    F.col("ACA_PGM_PAYMT_SK"),
    F.col("ACTVTY_YR_MO"),
    F.col("PAYMT_COV_YR_MO"),
    F.col("PAYMT_TYP_CD"),
    F.col("ST_CD"),
    F.col("QHP_ID"),
    F.col("ACA_PGM_PAYMT_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("QHP_ID") == 'NA', F.lit(1))
     .when(F.col("QHP_ID") == 'UNK', F.lit(0))
     .when(F.col("QHP_SK").isNull(), F.lit(0))
     .otherwise(F.col("QHP_SK"))
     .alias("QHP_SK"),
    F.when(F.col("PAYMT_TYP_CD") == 'NA', F.lit(1))
     .when(F.col("PAYMT_TYP_CD") == 'UNK', F.lit(0))
     .when(F.col("PAYMT_TYP_CD_SK").isNull(), F.lit(0))
     .otherwise(F.col("PAYMT_TYP_CD_SK"))
     .alias("PAYMT_TYP_CD_SK"),
    F.when(F.col("COV_STRT_DT_SK").isNull(), F.lit('1753-01-01')).otherwise(F.col("COV_STRT_DT_SK")).alias("COV_STRT_DT_SK"),
    F.when(F.col("COV_END_DT_SK").isNull(), F.lit('2199-12-31')).otherwise(F.col("COV_END_DT_SK")).alias("COV_END_DT_SK"),
    F.when(F.col("EFT_EFF_DT_SK").isNull(), F.lit('1753-01-01')).otherwise(F.col("EFT_EFF_DT_SK")).alias("EFT_EFF_DT_SK"),
    F.col("ACA_PGM_TRANS_AMT"),
    F.col("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.col("EFT_TRACE_ID"),
    F.col("EXCH_RPT_DOC_CTL_ID"),
    F.col("EXCH_RPT_NM")
)

windowSpec = Window.orderBy(F.lit(1))
df_xfm_CheckLkpResults_firstRow = df_xfm_CheckLkpResults.withColumn("RN", F.row_number().over(windowSpec)).filter(F.col("RN") == 1).drop("RN")

df_Lnk_AcaPgmPaymt_UNK = df_xfm_CheckLkpResults_firstRow.select(
    F.lit(0).alias("ACA_PGM_PAYMT_SK"),
    F.lit("175301").alias("ACTVTY_YR_MO"),
    F.lit("175301").alias("PAYMT_COV_YR_MO"),
    F.lit("UNK").alias("PAYMT_TYP_CD"),
    F.lit("UNK").alias("ST_CD"),
    F.lit("UNK").alias("QHP_ID"),
    F.lit(0).alias("ACA_PGM_PAYMT_SEQ_NO"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("QHP_SK"),
    F.lit(0).alias("PAYMT_TYP_CD_SK"),
    F.lit("1753-01-01").alias("COV_STRT_DT_SK"),
    F.lit("2199-12-31").alias("COV_END_DT_SK"),
    F.lit("1753-01-01").alias("EFT_EFF_DT_SK"),
    F.lit(0).alias("ACA_PGM_TRANS_AMT"),
    F.lit(0).alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.lit("UNK").alias("EFT_TRACE_ID"),
    F.lit("UNK").alias("EXCH_RPT_DOC_CTL_ID"),
    F.lit("UNK").alias("EXCH_RPT_NM")
)

df_Lnk_AcaPgmPaymt_NA = df_xfm_CheckLkpResults_firstRow.select(
    F.lit(1).alias("ACA_PGM_PAYMT_SK"),
    F.lit("175301").alias("ACTVTY_YR_MO"),
    F.lit("175301").alias("PAYMT_COV_YR_MO"),
    F.lit("NA").alias("PAYMT_TYP_CD"),
    F.lit("NA").alias("ST_CD"),
    F.lit("NA").alias("QHP_ID"),
    F.lit(0).alias("ACA_PGM_PAYMT_SEQ_NO"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("QHP_SK"),
    F.lit(1).alias("PAYMT_TYP_CD_SK"),
    F.lit("1753-01-01").alias("COV_STRT_DT_SK"),
    F.lit("2199-12-31").alias("COV_END_DT_SK"),
    F.lit("1753-01-01").alias("EFT_EFF_DT_SK"),
    F.lit(0).alias("ACA_PGM_TRANS_AMT"),
    F.lit(1).alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.lit("NA").alias("EFT_TRACE_ID"),
    F.lit("NA").alias("EXCH_RPT_DOC_CTL_ID"),
    F.lit("NA").alias("EXCH_RPT_NM")
)

# fnl_NA_UNK_Streams (PxFunnel)
df_fnl_NA_UNK_Streams = (
    df_Lnk_AcaPgmPaymt_Fkey_Main
    .unionByName(df_Lnk_AcaPgmPaymt_UNK)
    .unionByName(df_Lnk_AcaPgmPaymt_NA)
)

df_fnl_NA_UNK_Streams = df_fnl_NA_UNK_Streams.select(
    "ACA_PGM_PAYMT_SK",
    "ACTVTY_YR_MO",
    "PAYMT_COV_YR_MO",
    "PAYMT_TYP_CD",
    "ST_CD",
    "QHP_ID",
    "ACA_PGM_PAYMT_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "QHP_SK",
    "PAYMT_TYP_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "EFT_EFF_DT_SK",
    "ACA_PGM_TRANS_AMT",
    "ACA_PGM_PAYMT_UNIQ_KEY",
    "EFT_TRACE_ID",
    "EXCH_RPT_DOC_CTL_ID",
    "EXCH_RPT_NM"
)

df_fnl_NA_UNK_Streams_out = (
    df_fnl_NA_UNK_Streams
    .withColumn("ACTVTY_YR_MO", F.rpad(F.col("ACTVTY_YR_MO"), 6, " "))
    .withColumn("PAYMT_COV_YR_MO", F.rpad(F.col("PAYMT_COV_YR_MO"), 6, " "))
    .withColumn("PAYMT_TYP_CD", F.rpad(F.col("PAYMT_TYP_CD"), 256, " "))
    .withColumn("ST_CD", F.rpad(F.col("ST_CD"), 256, " "))
    .withColumn("QHP_ID", F.rpad(F.col("QHP_ID"), 256, " "))
    .withColumn("COV_STRT_DT_SK", F.rpad(F.col("COV_STRT_DT_SK"), 10, " "))
    .withColumn("COV_END_DT_SK", F.rpad(F.col("COV_END_DT_SK"), 10, " "))
    .withColumn("EFT_EFF_DT_SK", F.rpad(F.col("EFT_EFF_DT_SK"), 10, " "))
    .withColumn("EFT_TRACE_ID", F.rpad(F.col("EFT_TRACE_ID"), 256, " "))
    .withColumn("EXCH_RPT_DOC_CTL_ID", F.rpad(F.col("EXCH_RPT_DOC_CTL_ID"), 256, " "))
    .withColumn("EXCH_RPT_NM", F.rpad(F.col("EXCH_RPT_NM"), 256, " "))
)

# seq_ACA_PGM_PAYMT_Fkey (PxSequentialFile) - Write
write_files(
    df_fnl_NA_UNK_Streams_out.select(
        "ACA_PGM_PAYMT_SK",
        "ACTVTY_YR_MO",
        "PAYMT_COV_YR_MO",
        "PAYMT_TYP_CD",
        "ST_CD",
        "QHP_ID",
        "ACA_PGM_PAYMT_SEQ_NO",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "QHP_SK",
        "PAYMT_TYP_CD_SK",
        "COV_STRT_DT_SK",
        "COV_END_DT_SK",
        "EFT_EFF_DT_SK",
        "ACA_PGM_TRANS_AMT",
        "ACA_PGM_PAYMT_UNIQ_KEY",
        "EFT_TRACE_ID",
        "EXCH_RPT_DOC_CTL_ID",
        "EXCH_RPT_NM"
    ),
    f"{adls_path}/load/ACA_PGM_PAYMT.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# FnlFkeyFailures (PxFunnel)
df_Lnk_CovEndDt_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svCovEndDtFKeyLkpCheck") == 'Y')
    .select(
        F.col("ACA_PGM_PAYMT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(';'), F.col("COV_END_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_PaymtTypCd_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("SvPaymtTypCdFKeyLkpCheck") == 'Y')
    .select(
        F.col("ACA_PGM_PAYMT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(';'), F.col("PAYMT_TYP_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_EftEffDt_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svEftEffDtFKeyLkpCheck") == 'Y')
    .select(
        F.col("ACA_PGM_PAYMT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(';'), F.col("EFT_EFF_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_CovStrtDt_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svCovStrtDtFKeyLkpCheck") == 'Y')
    .select(
        F.col("ACA_PGM_PAYMT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(';'), F.col("COV_STRT_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_Qhp_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("SvQhpFKeyLkpCheck") == 'Y')
    .select(
        F.col("ACA_PGM_PAYMT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("K_QHP").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(';'), F.col("QHP_ID"), F.lit(';'), F.col("EFF_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_FnlFkeyFailures = (
    df_Lnk_CovEndDt_Fail
    .unionByName(df_Lnk_PaymtTypCd_Fail)
    .unionByName(df_Lnk_EftEffDt_Fail)
    .unionByName(df_Lnk_CovStrtDt_Fail)
    .unionByName(df_Lnk_Qhp_Fail)
)

df_FnlFkeyFailures = df_FnlFkeyFailures.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

df_FnlFkeyFailures_out = (
    df_FnlFkeyFailures
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), 256, " "))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), 256, " "))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), 256, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), 256, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), 256, " "))
)

# seq_FkeyFailedFile (PxSequentialFile) - Write
write_files(
    df_FnlFkeyFailures_out.select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    ),
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)