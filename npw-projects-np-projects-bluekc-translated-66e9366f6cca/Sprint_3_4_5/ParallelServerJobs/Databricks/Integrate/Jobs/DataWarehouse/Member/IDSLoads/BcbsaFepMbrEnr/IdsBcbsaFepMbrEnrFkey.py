# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ========================================================================================================================================
# MAGIC 												DATASTAGE	CODE		DATE OF
# MAGIC DEVELOPER	DATE		PROJECT	DESCRIPTION					ENVIRONMENT	REVIEWER	REVIEW
# MAGIC ========================================================================================================================================
# MAGIC Kalyan Neelam	2015-11-23	5403		Initial Programming					IntegrateDev1	Bhoomi Dasari	12/2/2015
# MAGIC Abhiram Dasarathy	2016-11-02	5568 - HEDIS	Added FEP_MBR_ID column to end of the processing	IntegrateDev2	 Kalyan Neelam        2016-11-15
# MAGIC Kailash Jadhav	2017-06-27	5781 - HEDIS	Added FEP_COV_TYP_TX column to end of the processing	IntegrateDev1          Kalyan Neelam        2017-06-29

# MAGIC Perform Foreign Key Lookups to populate BCBSA_FEP_MBR_ENR table.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC FKEY failures are written into this flat file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
RunCycle = get_widget_value('RunCycle','')
LastMonthEndDate = get_widget_value('LastMonthEndDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

DSJobName = "IdsBcbsaFepMbrEnrFkey"

# Read seq_BCBSA_FEP_MBR_ENR (PxSequentialFile)
schema_seq_BCBSA_FEP_MBR_ENR = T.StructType([
    T.StructField("PRI_NAT_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_TS", T.TimestampType(), nullable=False),
    T.StructField("BCBSA_FEP_MBR_ENR_SK", T.IntegerType(), nullable=False),
    T.StructField("MBR_UNIQ_KEY", T.IntegerType(), nullable=False),
    T.StructField("PROD_SH_NM", T.StringType(), nullable=False),
    T.StructField("FEP_MBR_ENR_EFF_DT", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("BC_PLN_CD", T.StringType(), nullable=False),
    T.StructField("BS_PLN_CD", T.StringType(), nullable=False),
    T.StructField("CHMCL_DPNDC_BNF_IN", T.StringType(), nullable=False),
    T.StructField("CHMCL_DPNDC_IP_BNF_IN", T.StringType(), nullable=False),
    T.StructField("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN", T.StringType(), nullable=False),
    T.StructField("CHMCL_DPNDC_OP_BNF_IN", T.StringType(), nullable=False),
    T.StructField("CONF_COMM_IN", T.StringType(), nullable=False),
    T.StructField("DNTL_BNF_IN", T.StringType(), nullable=False),
    T.StructField("MNTL_HLTH_BNF_IN", T.StringType(), nullable=False),
    T.StructField("MNTL_HLTH_IP_BNF_IN", T.StringType(), nullable=False),
    T.StructField("MNTL_HLTH_IP_PRTL_DAY_BNF_IN", T.StringType(), nullable=False),
    T.StructField("MNTL_HLTH_OP_BNF_IN", T.StringType(), nullable=False),
    T.StructField("OP_BNF_IN", T.StringType(), nullable=False),
    T.StructField("PDX_BNF_IN", T.StringType(), nullable=False),
    T.StructField("FEP_MBR_ENR_TERM_DT", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_LAST_UPDT_DT", T.StringType(), nullable=False),
    T.StructField("FEP_PLN_PROD_ID", T.StringType(), nullable=False),
    T.StructField("FEP_PLN_RGN_ID", T.StringType(), nullable=False),
    T.StructField("MBR_ID", T.StringType(), nullable=False),
    T.StructField("MBR_EMPLMT_STTUS_NM", T.StringType(), nullable=False),
    T.StructField("PROD_DESC", T.StringType(), nullable=False),
    T.StructField("FEP_MBR_ID", T.StringType(), nullable=True),
    T.StructField("FEP_COV_TYP_TX", T.StringType(), nullable=False)
])

df_seq_BCBSA_FEP_MBR_ENR = (
    spark.read.csv(
        path=f"{adls_path}/key/BCBSA_FEP_MBR_ENR.{SrcSysCd}.pkey.{RunID}.dat",
        schema=schema_seq_BCBSA_FEP_MBR_ENR,
        sep=",",
        quote="^",
        header=False
    )
)

df_Lnk_IdsBcbsaFepMbrEnrFkey_InAbc = df_seq_BCBSA_FEP_MBR_ENR.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "BCBSA_FEP_MBR_ENR_SK",
    "MBR_UNIQ_KEY",
    "PROD_SH_NM",
    "FEP_MBR_ENR_EFF_DT",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BC_PLN_CD",
    "BS_PLN_CD",
    "CHMCL_DPNDC_BNF_IN",
    "CHMCL_DPNDC_IP_BNF_IN",
    "CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN",
    "CHMCL_DPNDC_OP_BNF_IN",
    "CONF_COMM_IN",
    "DNTL_BNF_IN",
    "MNTL_HLTH_BNF_IN",
    "MNTL_HLTH_IP_BNF_IN",
    "MNTL_HLTH_IP_PRTL_DAY_BNF_IN",
    "MNTL_HLTH_OP_BNF_IN",
    "OP_BNF_IN",
    "PDX_BNF_IN",
    "FEP_MBR_ENR_TERM_DT",
    "SRC_SYS_LAST_UPDT_DT",
    "FEP_PLN_PROD_ID",
    "FEP_PLN_RGN_ID",
    "MBR_ID",
    "MBR_EMPLMT_STTUS_NM",
    "PROD_DESC",
    "FEP_MBR_ID",
    "FEP_COV_TYP_TX"
)

# Read db2_MBR_Lkp (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_MBR_Lkp = f"SELECT MBR_UNIQ_KEY, MBR_SK FROM {IDSOwner}.MBR"
df_db2_MBR_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR_Lkp)
    .load()
)

# Read db2_PROD_SH_NM_Lkp (DB2ConnectorPX)
extract_query_db2_PROD_SH_NM_Lkp = f"SELECT PROD_SH_NM, PROD_SH_NM_SK FROM {IDSOwner}.PROD_SH_NM"
df_db2_PROD_SH_NM_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_SH_NM_Lkp)
    .load()
)

# Read ds_CD_MPPNG_LkpData (PxDataSet -> Parquet)
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_LkpData = df_ds_CD_MPPNG_LkpData.select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

df_Lnk_CdMppng_dataOut = df_ds_CD_MPPNG_LkpData

# fltr_FilterData (PxFilter) -> two output links with same condition
common_filter = (
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' "
    "AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='ACTIVATING BCBS PLAN' "
    "AND TRGT_DOMAIN_NM='SUBSCRIBER BCBS PLAN'"
)
df_Ref_BcPlnCd_LNk = (
    df_Lnk_CdMppng_dataOut
    .filter(common_filter)
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)
df_Ref_BsPlnCd_LNk = (
    df_Lnk_CdMppng_dataOut
    .filter(common_filter)
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

# db2_K_Clndr_Dt_Eff_Lkp (DB2ConnectorPX)
extract_query_db2_K_Clndr_Dt_Eff_Lkp = (
    f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT "
    "WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"
)
df_db2_K_Clndr_Dt_Eff_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_Clndr_Dt_Eff_Lkp)
    .load()
)

# Copy stage (PxCopy) -> replicate the same DF
df_TermDt = df_db2_K_Clndr_Dt_Eff_Lkp.select(F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"))
df_EffDt = df_db2_K_Clndr_Dt_Eff_Lkp.select(F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"))
df_LastUpdtDt = df_db2_K_Clndr_Dt_Eff_Lkp.select(F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"))

# lkp_Code_SKs (PxLookup) -> multiple left joins
df_lkp_joined = (
    df_Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.alias("Primary")
    .join(
        df_Ref_BcPlnCd_LNk.alias("Ref_BcPlnCd_LNk"),
        F.col("Primary.BC_PLN_CD") == F.col("Ref_BcPlnCd_LNk.SRC_CD"),
        "left"
    )
    .join(
        df_Ref_BsPlnCd_LNk.alias("Ref_BsPlnCd_LNk"),
        (
            (F.col("Primary.BS_PLN_CD") == F.col("Ref_BsPlnCd_LNk.SRC_CD"))
            & (F.lit(True))  # per the JSON, second condition references a non-existent column; must not skip
        ),
        "left"
    )
    .join(
        df_db2_MBR_Lkp.alias("Ref_MBR_In"),
        F.col("Primary.MBR_UNIQ_KEY") == F.col("Ref_MBR_In.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_EffDt.alias("EffDt"),
        F.col("Primary.FEP_MBR_ENR_EFF_DT") == F.col("EffDt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_TermDt.alias("TermDt"),
        F.col("Primary.FEP_MBR_ENR_TERM_DT") == F.col("TermDt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_LastUpdtDt.alias("LastUpdtDt"),
        F.col("Primary.SRC_SYS_LAST_UPDT_DT") == F.col("LastUpdtDt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_db2_PROD_SH_NM_Lkp.alias("Ref_ProdShNm_In"),
        F.col("Primary.PROD_SH_NM") == F.col("Ref_ProdShNm_In.PROD_SH_NM"),
        "left"
    )
)

df_Lnk_LkpDataOut = df_lkp_joined.select(
    F.col("Primary.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Primary.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("Primary.BCBSA_FEP_MBR_ENR_SK").alias("BCBSA_FEP_MBR_ENR_SK"),
    F.col("Primary.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Primary.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("EffDt.CLNDR_DT_SK").alias("FEP_MBR_ENR_EFF_DT_SK"),
    F.col("Primary.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Primary.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Primary.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Ref_MBR_In.MBR_SK").alias("MBR_SK"),
    F.col("Ref_ProdShNm_In.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("Ref_BcPlnCd_LNk.CD_MPPNG_SK").alias("BC_PLN_CD_SK"),
    F.col("Ref_BsPlnCd_LNk.CD_MPPNG_SK").alias("BS_PLN_CD_SK"),
    F.col("Primary.CHMCL_DPNDC_BNF_IN").alias("CHMCL_DPNDC_BNF_IN"),
    F.col("Primary.CHMCL_DPNDC_IP_BNF_IN").alias("CHMCL_DPNDC_IP_BNF_IN"),
    F.col("Primary.CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    F.col("Primary.CHMCL_DPNDC_OP_BNF_IN").alias("CHMCL_DPNDC_OP_BNF_IN"),
    F.col("Primary.CONF_COMM_IN").alias("CONF_COMM_IN"),
    F.col("Primary.DNTL_BNF_IN").alias("DNTL_BNF_IN"),
    F.col("Primary.MNTL_HLTH_BNF_IN").alias("MNTL_HLTH_BNF_IN"),
    F.col("Primary.MNTL_HLTH_IP_BNF_IN").alias("MNTL_HLTH_IP_BNF_IN"),
    F.col("Primary.MNTL_HLTH_IP_PRTL_DAY_BNF_IN").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    F.col("Primary.MNTL_HLTH_OP_BNF_IN").alias("MNTL_HLTH_OP_BNF_IN"),
    F.col("Primary.OP_BNF_IN").alias("OP_BNF_IN"),
    F.col("Primary.PDX_BNF_IN").alias("PDX_BNF_IN"),
    F.col("TermDt.CLNDR_DT_SK").alias("FEP_MBR_ENR_TERM_DT_SK"),
    F.col("LastUpdtDt.CLNDR_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("Primary.FEP_PLN_PROD_ID").alias("FEP_PLN_PROD_ID"),
    F.col("Primary.FEP_PLN_RGN_ID").alias("FEP_PLN_RGN_ID"),
    F.col("Primary.MBR_ID").alias("MBR_ID"),
    F.col("Primary.MBR_EMPLMT_STTUS_NM").alias("MBR_EMPLMT_STTUS_NM"),
    F.col("Primary.PROD_DESC").alias("PROD_DESC"),
    F.col("Primary.BC_PLN_CD").alias("BC_PLN_CD"),
    F.col("Primary.BS_PLN_CD").alias("BS_PLN_CD"),
    F.col("Primary.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Primary.FEP_COV_TYP_TX").alias("FEP_COV_TYP_TX")
)

# xfm_CheckLkpResults (CTransformerStage)
df_xfm = (
    df_Lnk_LkpDataOut
    .withColumn("SvMbrFKeyLkpCheck", F.when(F.col("MBR_SK").isNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("SvProdShNmFKeyLkpCheck", F.when(F.col("PROD_SH_NM_SK").isNull() & (F.col("PROD_SH_NM") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("SvBcPlnCdFKeyLkpCheck", F.when(F.col("BC_PLN_CD_SK").isNull() & (F.col("BC_PLN_CD") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("SvBsPlnCdFKeyLkpCheck", F.when(F.col("BS_PLN_CD_SK").isNull() & (F.col("BS_PLN_CD") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svFepMbrEnrEffDtFKeyLkpCheck", F.when(F.col("FEP_MBR_ENR_EFF_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svFepMbrEnrTermDtFKeyLkpCheck", F.when(F.col("FEP_MBR_ENR_TERM_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svSrcSysLastUpdtDtFKeyLkpCheck", F.when(F.col("SRC_SYS_LAST_UPDT_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N")))
)

df_Lnk_BcbsaFepMbrEnr_Fkey_Main_base = (
    df_xfm
    .withColumn("FEP_MBR_ENR_EFF_DT_SK", F.when(F.col("FEP_MBR_ENR_EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("FEP_MBR_ENR_EFF_DT_SK")))
    .withColumn("MBR_SK", F.when(F.col("MBR_SK").isNull(), F.lit(0)).otherwise(F.col("MBR_SK")))
    .withColumn("PROD_SH_NM_SK",
        F.when(F.col("PROD_SH_NM") == F.lit("NA"), F.lit(1))
         .when(F.col("PROD_SH_NM") == F.lit("UNK"), F.lit(0))
         .when(F.col("PROD_SH_NM_SK").isNull(), F.lit(0))
         .otherwise(F.col("PROD_SH_NM_SK"))
    )
    .withColumn("BC_PLN_CD_SK",
        F.when(F.col("BC_PLN_CD") == F.lit("NA"), F.lit(1))
         .when(F.col("BC_PLN_CD") == F.lit("UNK"), F.lit(0))
         .when(F.col("BC_PLN_CD_SK").isNull(), F.lit(0))
         .otherwise(F.col("BC_PLN_CD_SK"))
    )
    .withColumn("BS_PLN_CD_SK",
        F.when(F.col("BS_PLN_CD") == F.lit("NA"), F.lit(1))
         .when(F.col("BS_PLN_CD") == F.lit("UNK"), F.lit(0))
         .when(F.col("BS_PLN_CD_SK").isNull(), F.lit(0))
         .otherwise(F.col("BS_PLN_CD_SK"))
    )
    .withColumn("FEP_MBR_ENR_TERM_DT_SK", F.when(F.col("FEP_MBR_ENR_TERM_DT_SK").isNull(), F.lit("2199-12-31")).otherwise(F.col("FEP_MBR_ENR_TERM_DT_SK")))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.when(F.col("SRC_SYS_LAST_UPDT_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("SRC_SYS_LAST_UPDT_DT_SK")))
)

df_Lnk_BcbsaFepMbrEnr_Fkey_Main = df_Lnk_BcbsaFepMbrEnr_Fkey_Main_base.select(
    F.col("BCBSA_FEP_MBR_ENR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PROD_SH_NM"),
    F.col("FEP_MBR_ENR_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("BC_PLN_CD_SK"),
    F.col("BS_PLN_CD_SK"),
    F.col("CHMCL_DPNDC_BNF_IN"),
    F.col("CHMCL_DPNDC_IP_BNF_IN"),
    F.col("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    F.col("CHMCL_DPNDC_OP_BNF_IN"),
    F.col("CONF_COMM_IN"),
    F.col("DNTL_BNF_IN"),
    F.col("MNTL_HLTH_BNF_IN"),
    F.col("MNTL_HLTH_IP_BNF_IN"),
    F.col("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    F.col("MNTL_HLTH_OP_BNF_IN"),
    F.col("OP_BNF_IN"),
    F.col("PDX_BNF_IN"),
    F.col("FEP_MBR_ENR_TERM_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("FEP_PLN_PROD_ID"),
    F.col("FEP_PLN_RGN_ID"),
    F.col("MBR_ID"),
    F.col("MBR_EMPLMT_STTUS_NM"),
    F.col("PROD_DESC"),
    F.col("FEP_MBR_ID"),
    F.col("FEP_COV_TYP_TX")
)

df_Lnk_BcbsaFepMbrEnr_UNK = df_xfm.limit(1).select(
    F.lit(0).alias("BCBSA_FEP_MBR_ENR_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("UNK").alias("PROD_SH_NM"),
    F.lit("1753-01-01").alias("FEP_MBR_ENR_EFF_DT_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PROD_SH_NM_SK"),
    F.lit(0).alias("BC_PLN_CD_SK"),
    F.lit(0).alias("BS_PLN_CD_SK"),
    F.lit("N").alias("CHMCL_DPNDC_BNF_IN"),
    F.lit("N").alias("CHMCL_DPNDC_IP_BNF_IN"),
    F.lit("N").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    F.lit("N").alias("CHMCL_DPNDC_OP_BNF_IN"),
    F.lit("N").alias("CONF_COMM_IN"),
    F.lit("N").alias("DNTL_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_IP_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_OP_BNF_IN"),
    F.lit("N").alias("OP_BNF_IN"),
    F.lit("N").alias("PDX_BNF_IN"),
    F.lit("2199-12-31").alias("FEP_MBR_ENR_TERM_DT_SK"),
    F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit("UNK").alias("FEP_PLN_PROD_ID"),
    F.lit("UNK").alias("FEP_PLN_RGN_ID"),
    F.lit("UNK").alias("MBR_ID"),
    F.lit("UNK").alias("MBR_EMPLMT_STTUS_NM"),
    F.lit("UNK").alias("PROD_DESC"),
    F.lit("UNK").alias("FEP_MBR_ID"),
    F.lit("UNK").alias("FEP_COV_TYP_TX")
)

df_Lnk_BcbsaFepMbrEnr_NA = df_xfm.limit(1).select(
    F.lit(1).alias("BCBSA_FEP_MBR_ENR_SK"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit("NA").alias("PROD_SH_NM"),
    F.lit("1753-01-01").alias("FEP_MBR_ENR_EFF_DT_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PROD_SH_NM_SK"),
    F.lit(1).alias("BC_PLN_CD_SK"),
    F.lit(1).alias("BS_PLN_CD_SK"),
    F.lit("N").alias("CHMCL_DPNDC_BNF_IN"),
    F.lit("N").alias("CHMCL_DPNDC_IP_BNF_IN"),
    F.lit("N").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    F.lit("N").alias("CHMCL_DPNDC_OP_BNF_IN"),
    F.lit("N").alias("CONF_COMM_IN"),
    F.lit("N").alias("DNTL_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_IP_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    F.lit("N").alias("MNTL_HLTH_OP_BNF_IN"),
    F.lit("N").alias("OP_BNF_IN"),
    F.lit("N").alias("PDX_BNF_IN"),
    F.lit("2199-12-31").alias("FEP_MBR_ENR_TERM_DT_SK"),
    F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit("NA").alias("FEP_PLN_PROD_ID"),
    F.lit("NA").alias("FEP_PLN_RGN_ID"),
    F.lit("NA").alias("MBR_ID"),
    F.lit("NA").alias("MBR_EMPLMT_STTUS_NM"),
    F.lit("NA").alias("PROD_DESC"),
    F.lit("NA").alias("FEP_MBR_ID"),
    F.lit("NA").alias("FEP_COV_TYP_TX")
)

df_TermDt_Fail = (
    df_xfm
    .filter(F.col("svFepMbrEnrTermDtFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("FEP_MBR_ENR_TERM_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_BsPlnCd_Fail = (
    df_xfm
    .filter(F.col("SvBsPlnCdFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("BS_PLN_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_EffDt_Fail = (
    df_xfm
    .filter(F.col("svFepMbrEnrEffDtFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("FEP_MBR_ENR_EFF_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_UpdtDt_Fail = (
    df_xfm
    .filter(F.col("svSrcSysLastUpdtDtFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("SRC_SYS_LAST_UPDT_DT_SK")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Mbr_Fail = (
    df_xfm
    .filter(F.col("SvMbrFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("MBR").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("MBR_UNIQ_KEY")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_BcPlnCd_Fail = (
    df_xfm
    .filter(F.col("SvBcPlnCdFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("BC_PLN_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_ProdShNm_Fail = (
    df_xfm
    .filter(F.col("SvProdShNmFKeyLkpCheck") == "Y")
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("PROD_SH_NM").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("PROD_SH_NM")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# fnl_NA_UNK_Streams (PxFunnel) -> union
df_fnl_NA_UNK_Streams = df_Lnk_BcbsaFepMbrEnr_Fkey_Main.unionByName(df_Lnk_BcbsaFepMbrEnr_UNK).unionByName(df_Lnk_BcbsaFepMbrEnr_NA)

# seq_BCBSA_FEP_MBR_ENR_Fkey (PxSequentialFile) -> write final
# Before writing, apply rpad on char or varchar columns as per final column specs:
df_fnl_NA_UNK_Streams_out = df_fnl_NA_UNK_Streams.select(
    F.col("BCBSA_FEP_MBR_ENR_SK"),
    F.col("MBR_UNIQ_KEY"),
    rpad("PROD_SH_NM", 255, " ").alias("PROD_SH_NM"),  # no length given for many, using a large placeholder
    rpad("FEP_MBR_ENR_EFF_DT_SK", 10, " ").alias("FEP_MBR_ENR_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("BC_PLN_CD_SK"),
    F.col("BS_PLN_CD_SK"),
    rpad("CHMCL_DPNDC_BNF_IN", 1, " ").alias("CHMCL_DPNDC_BNF_IN"),
    rpad("CHMCL_DPNDC_IP_BNF_IN", 1, " ").alias("CHMCL_DPNDC_IP_BNF_IN"),
    rpad("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN", 1, " ").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    rpad("CHMCL_DPNDC_OP_BNF_IN", 1, " ").alias("CHMCL_DPNDC_OP_BNF_IN"),
    rpad("CONF_COMM_IN", 1, " ").alias("CONF_COMM_IN"),
    rpad("DNTL_BNF_IN", 1, " ").alias("DNTL_BNF_IN"),
    rpad("MNTL_HLTH_BNF_IN", 1, " ").alias("MNTL_HLTH_BNF_IN"),
    rpad("MNTL_HLTH_IP_BNF_IN", 1, " ").alias("MNTL_HLTH_IP_BNF_IN"),
    rpad("MNTL_HLTH_IP_PRTL_DAY_BNF_IN", 1, " ").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    rpad("MNTL_HLTH_OP_BNF_IN", 1, " ").alias("MNTL_HLTH_OP_BNF_IN"),
    rpad("OP_BNF_IN", 1, " ").alias("OP_BNF_IN"),
    rpad("PDX_BNF_IN", 1, " ").alias("PDX_BNF_IN"),
    rpad("FEP_MBR_ENR_TERM_DT_SK", 10, " ").alias("FEP_MBR_ENR_TERM_DT_SK"),
    rpad("SRC_SYS_LAST_UPDT_DT_SK", 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    rpad("FEP_PLN_PROD_ID", 255, " ").alias("FEP_PLN_PROD_ID"),
    rpad("FEP_PLN_RGN_ID", 255, " ").alias("FEP_PLN_RGN_ID"),
    rpad("MBR_ID", 255, " ").alias("MBR_ID"),
    rpad("MBR_EMPLMT_STTUS_NM", 255, " ").alias("MBR_EMPLMT_STTUS_NM"),
    rpad("PROD_DESC", 255, " ").alias("PROD_DESC"),
    rpad("FEP_MBR_ID", 255, " ").alias("FEP_MBR_ID"),
    rpad("FEP_COV_TYP_TX", 255, " ").alias("FEP_COV_TYP_TX")
)

write_files(
    df_fnl_NA_UNK_Streams_out,
    f"{adls_path}/load/BCBSA_FEP_MBR_ENR.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# FnlFkeyFailures (PxFunnel)
df_fnlFkeyFailures = df_TermDt_Fail.unionByName(df_BsPlnCd_Fail) \
    .unionByName(df_EffDt_Fail) \
    .unionByName(df_UpdtDt_Fail) \
    .unionByName(df_Mbr_Fail) \
    .unionByName(df_BcPlnCd_Fail) \
    .unionByName(df_ProdShNm_Fail)

df_fnlFkeyFailures_out = df_fnlFkeyFailures.select(
    F.col("PRI_SK"),
    rpad("PRI_NAT_KEY_STRING", 255, " ").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    rpad("JOB_NM", 255, " ").alias("JOB_NM"),
    rpad("ERROR_TYP", 255, " ").alias("ERROR_TYP"),
    rpad("PHYSCL_FILE_NM", 255, " ").alias("PHYSCL_FILE_NM"),
    rpad("FRGN_NAT_KEY_STRING", 255, " ").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

# seq_FkeyFailedFile (PxSequentialFile)
write_files(
    df_fnlFkeyFailures_out,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)