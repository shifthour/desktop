# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Abburi            2017-11-30              5781                             Original Programming                                                                              IntegrateDev2         Kalyan Neelam             2018-01-30

# MAGIC IdsNtnlProvTxnmyFkey_EE
# MAGIC FKEY failures are written into this flat file.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, rpad, concat
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','CMS')
IDSRunCycle = get_widget_value('IDSRunCycle','103')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','103')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','FKeysFailure.IdsNtnlProvTxnmyCntl')

# DB config for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: seq_NTNL_PROV_TXNMY_Pkey (PxSequentialFile) - Read .dat file with defined schema.
schema_seq_NTNL_PROV_TXNMY_Pkey = StructType([
    StructField("NTNL_PROV_TXNMY_SK", IntegerType(), False),
    StructField("NTNL_PROV_ID", StringType(), False),
    StructField("TXNMY_CD", StringType(), False),
    StructField("ENTY_LIC_ST_CD", StringType(), False),
    StructField("LIC_NO", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP", StringType(), True),
    StructField("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH", StringType(), True),
    StructField("CUR_RCRD_IN", StringType(), False),
    StructField("NTNL_PROV_TXNMY_ACTV_IN", StringType(), False),
    StructField("NTNL_PROV_TXNMY_PRI_IN", StringType(), False)
])

df_seq_NTNL_PROV_TXNMY_Pkey = (
    spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_NTNL_PROV_TXNMY_Pkey)
    .load(f"{adls_path}/key/NTNL_PROV_TXNMY.{SrcSysCd}.pkey.{RunID}.dat")
)

# Stage: db2_K_NTNL_PROV_Lkp (DB2ConnectorPX) - Read from IDS database
extract_query_db2_K_NTNL_PROV_Lkp = "SELECT NTNL_PROV_ID, NTNL_PROV_SK FROM " + IDSOwner + ".K_NTNL_PROV"
df_db2_K_NTNL_PROV_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_NTNL_PROV_Lkp)
    .load()
)

# Stage: db2_K_TXNMY_CD_Lkp (DB2ConnectorPX) - Read from IDS database
extract_query_db2_K_TXNMY_CD_Lkp = "SELECT TXNMY_CD_SK, TXNMY_CD FROM " + IDSOwner + ".K_TXNMY_CD"
df_db2_K_TXNMY_CD_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_TXNMY_CD_Lkp)
    .load()
)

# Stage: Copy_84 (PxCopy)
# Output 1: TxnmyCd
df_Copy_84_TxnmyCd = df_db2_K_TXNMY_CD_Lkp.select(
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("TXNMY_CD_SK").alias("TXNMY_CD_SK")
)
# Output 2: GroupTxnmycd
df_Copy_84_GroupTxnmycd = df_db2_K_TXNMY_CD_Lkp.select(
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("TXNMY_CD_SK").alias("TXNMY_CD_SK")
)

# Stage: ds_CD_MPPNG_LkpData (PxDataSet) - Translated to reading from parquet
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# Stage: fltr_FilterData (PxFilter)
df_fltr_FilterData = df_ds_CD_MPPNG_LkpData.filter(
    (col("SRC_SYS_CD") == "IDS") &
    (col("SRC_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "STATE") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("TRGT_DOMAIN_NM") == "STATE")
)

df_fltr_FilterData_ref_LicStCdSk = df_fltr_FilterData.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Stage: Lkup_Fkey (PxLookup) - chain of left joins
df_lkup_fkey = (
    df_seq_NTNL_PROV_TXNMY_Pkey.alias("Lnk_NtnlProvTxnmy_EE_In")
    .join(
        df_fltr_FilterData_ref_LicStCdSk.alias("ref_LicStCdSk"),
        col("Lnk_NtnlProvTxnmy_EE_In.ENTY_LIC_ST_CD") == col("ref_LicStCdSk.SRC_CD"),
        "left"
    )
    .join(
        df_Copy_84_GroupTxnmycd.alias("GroupTxnmycd"),
        col("Lnk_NtnlProvTxnmy_EE_In.HEALTHCARE_PROVIDER_TAXONOMY_GROUP") == col("GroupTxnmycd.TXNMY_CD"),
        "left"
    )
    .join(
        df_Copy_84_TxnmyCd.alias("TxnmyCd"),
        col("Lnk_NtnlProvTxnmy_EE_In.TXNMY_CD") == col("TxnmyCd.TXNMY_CD"),
        "left"
    )
    .join(
        df_db2_K_NTNL_PROV_Lkp.alias("Ref_NtnlProvSk"),
        col("Lnk_NtnlProvTxnmy_EE_In.NTNL_PROV_ID") == col("Ref_NtnlProvSk.NTNL_PROV_ID"),
        "left"
    )
    .select(
        col("Lnk_NtnlProvTxnmy_EE_In.NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK"),
        col("Lnk_NtnlProvTxnmy_EE_In.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("Lnk_NtnlProvTxnmy_EE_In.TXNMY_CD").alias("TXNMY_CD"),
        col("Lnk_NtnlProvTxnmy_EE_In.ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
        col("Lnk_NtnlProvTxnmy_EE_In.LIC_NO").alias("LIC_NO"),
        col("Lnk_NtnlProvTxnmy_EE_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Lnk_NtnlProvTxnmy_EE_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Lnk_NtnlProvTxnmy_EE_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Lnk_NtnlProvTxnmy_EE_In.CUR_RCRD_IN").alias("CUR_RCRD_IN"),
        col("Lnk_NtnlProvTxnmy_EE_In.NTNL_PROV_TXNMY_ACTV_IN").alias("NTNL_PROV_TXNMY_ACTV_IN"),
        col("Lnk_NtnlProvTxnmy_EE_In.NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN"),
        col("GroupTxnmycd.TXNMY_CD_SK").alias("GRP_TXNMY_CD_SK"),
        col("Ref_NtnlProvSk.NTNL_PROV_SK").alias("NTNL_PROV_SK"),
        col("TxnmyCd.TXNMY_CD_SK").alias("TXNMY_CD_SK"),
        col("ref_LicStCdSk.CD_MPPNG_SK").alias("ENTY_LIC_ST_CD_SK")
    )
)

# Stage: xfm_CheckLkpResults (CTransformerStage) - add stage variables as columns
df_xfm_CheckLkpResults = (
    df_lkup_fkey
    .withColumn(
        "svLicStCdSkLkupCheck",
        when(col("ENTY_LIC_ST_CD_SK").isNull() & (col("ENTY_LIC_ST_CD_SK") != lit("NA")), lit("Y")).otherwise(lit("N"))
    )
    .withColumn(
        "svNtnlProvSkLkupCheck",
        when(col("NTNL_PROV_SK").isNull() & (col("NTNL_PROV_SK") != lit("NA")), lit("Y")).otherwise(lit("N"))
    )
    .withColumn(
        "svTxnmyCdLkupCheck",
        when(col("TXNMY_CD_SK").isNull() & (col("TXNMY_CD_SK") != lit("NA")), lit("Y")).otherwise(lit("N"))
    )
    .withColumn(
        "svGrpTxnmyCdLkupCheck",
        when(col("GRP_TXNMY_CD_SK").isNull() & (col("GRP_TXNMY_CD_SK") != lit("NA")), lit("Y")).otherwise(lit("N"))
    )
)

# Output pin: Lnk_Main
df_lnk_main = df_xfm_CheckLkpResults.select(
    col("NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK"),
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("LIC_NO").alias("LIC_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CUR_RCRD_IN").alias("CUR_RCRD_IN"),
    col("NTNL_PROV_TXNMY_ACTV_IN").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    col("NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN"),
    when(col("GRP_TXNMY_CD_SK").isNull(), lit(1)).otherwise(col("GRP_TXNMY_CD_SK")).alias("GRP_TXNMY_CD_SK"),
    when(col("NTNL_PROV_SK").isNull(), lit(1)).otherwise(col("NTNL_PROV_SK")).alias("NTNL_PROV_SK"),
    when(col("TXNMY_CD_SK").isNull(), lit(1)).otherwise(col("TXNMY_CD_SK")).alias("TXNMY_CD_SK"),
    when(col("ENTY_LIC_ST_CD").isNull(), lit(0))
    .otherwise( when(col("ENTY_LIC_ST_CD_SK").isNull(), lit(1)).otherwise(col("ENTY_LIC_ST_CD_SK")) ).alias("ENTY_LIC_ST_CD_SK")
)

# Output pin: Lnk_UNK (constraint: first row -> produce a single row of constants)
df_lnk_unk_single = df_xfm_CheckLkpResults.limit(1)
df_lnk_unk = df_lnk_unk_single.select(
    lit(0).alias("NTNL_PROV_TXNMY_SK"),
    lit("UNK").alias("NTNL_PROV_ID"),
    lit("UNK").alias("TXNMY_CD"),
    lit("UNK").alias("ENTY_LIC_ST_CD"),
    lit("UNK").alias("LIC_NO"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("N").alias("CUR_RCRD_IN"),
    lit("N").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    lit("N").alias("NTNL_PROV_TXNMY_PRI_IN"),
    lit(0).alias("GRP_TXNMY_CD_SK"),
    lit(0).alias("NTNL_PROV_SK"),
    lit(0).alias("TXNMY_CD_SK"),
    lit(0).alias("ENTY_LIC_ST_CD_SK")
)

# Output pin: Lnk_NA (constraint: first row -> produce a single row of constants)
df_lnk_na_single = df_xfm_CheckLkpResults.limit(1)
df_lnk_na = df_lnk_na_single.select(
    lit(1).alias("NTNL_PROV_TXNMY_SK"),
    lit("NA").alias("NTNL_PROV_ID"),
    lit("NA").alias("TXNMY_CD"),
    lit("NA").alias("ENTY_LIC_ST_CD"),
    lit("NA").alias("LIC_NO"),
    lit("NA").alias("SRC_SYS_CD"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("N").alias("CUR_RCRD_IN"),
    lit("N").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    lit("N").alias("NTNL_PROV_TXNMY_PRI_IN"),
    lit(1).alias("GRP_TXNMY_CD_SK"),
    lit(1).alias("NTNL_PROV_SK"),
    lit(1).alias("TXNMY_CD_SK"),
    lit(1).alias("ENTY_LIC_ST_CD_SK")
)

# Output pin: lnk_LicStCdLkupFail
df_lnk_LicStCdLkupFail = (
    df_xfm_CheckLkpResults
    .filter(col("svLicStCdSkLkupCheck") == lit("Y"))
    .select(
        col("NTNL_PROV_TXNMY_SK").alias("PRI_SK"),
        concat(col("NTNL_PROV_ID"), lit(":"), col("TXNMY_CD"), lit(":"), col("ENTY_LIC_ST_CD"), lit(":"), col("LIC_NO")).alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        lit("IdsNtnlProvTxnmyFkey_EE").alias("JOB_NM"),
        lit("CDLOOKUP").alias("ERROR_TYP"),
        lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        concat(col("SRC_SYS_CD"), lit(";CMS;IDS;NTNL_PROV_ID;"), col("NTNL_PROV_ID")).alias("FRGN_NAT_KEY_STRING"),
        col("TXNMY_CD").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Output pin: lnk_GrpTxnmyCdLkupFail
df_lnk_GrpTxnmyCdLkupFail = (
    df_xfm_CheckLkpResults
    .filter(col("svGrpTxnmyCdLkupCheck") == lit("Y"))
    .select(
        col("NTNL_PROV_TXNMY_SK").alias("PRI_SK"),
        concat(col("NTNL_PROV_ID"), lit(";"), col("TXNMY_CD"), lit(";"), col("ENTY_LIC_ST_CD"), lit(";"), col("LIC_NO")).alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        lit("IdsNtnlProvTxnmyFkey_EE").alias("JOB_NM"),
        lit("FKLOOKUP").alias("ERROR_TYP"),
        lit("TXNMY_CD").alias("PHYSCL_FILE_NM"),
        concat(col("SRC_SYS_CD"), lit(";"), col("TXNMY_CD")).alias("FRGN_NAT_KEY_STRING"),
        col("TXNMY_CD").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Output pin: lnk_NtnlProv_LkupFail
df_lnk_NtnlProv_LkupFail = (
    df_xfm_CheckLkpResults
    .filter(col("svNtnlProvSkLkupCheck") == lit("Y"))
    .select(
        col("NTNL_PROV_TXNMY_SK").alias("PRI_SK"),
        concat(col("NTNL_PROV_ID"), lit(";"), col("TXNMY_CD"), lit(";"), col("ENTY_LIC_ST_CD"), lit(";"), col("LIC_NO")).alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        lit("IdsNtnlProvTxnmyFkey_EE").alias("JOB_NM"),
        lit("FKLOOKUP").alias("ERROR_TYP"),
        lit("NTNL_PROV").alias("PHYSCL_FILE_NM"),
        concat(col("SRC_SYS_CD"), lit(";"), col("NTNL_PROV_ID")).alias("FRGN_NAT_KEY_STRING"),
        col("NTNL_PROV_ID").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Output pin: lnk_TxnmyCd_LkupFail
df_lnk_TxnmyCd_LkupFail = (
    df_xfm_CheckLkpResults
    .filter(col("svTxnmyCdLkupCheck") == lit("Y"))
    .select(
        col("NTNL_PROV_TXNMY_SK").alias("PRI_SK"),
        concat(col("NTNL_PROV_ID"), lit(";"), col("TXNMY_CD"), lit(";"), col("ENTY_LIC_ST_CD"), lit(";"), col("LIC_NO")).alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        lit("IdsNtnlProvTxnmyFkey_EE").alias("JOB_NM"),
        lit("FKLOOKUP").alias("ERROR_TYP"),
        lit("TXNMY_CD").alias("PHYSCL_FILE_NM"),
        concat(col("SRC_SYS_CD"), lit(";"), col("TXNMY_CD")).alias("FRGN_NAT_KEY_STRING"),
        col("TXNMY_CD").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Stage: fnl_NA_UNK_Streams (PxFunnel) - union main, UNK, NA
df_fnl_NA_UNK_Streams = (
    df_lnk_main
    .unionByName(df_lnk_unk)
    .unionByName(df_lnk_na)
)

# Stage: seq_NTNL_PROV_TXNMY_FKey (PxSequentialFile)
# Final select with rpad on char/varchar columns
df_seq_NTNL_PROV_TXNMY_FKey_write = df_fnl_NA_UNK_Streams.select(
    col("NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK"),
    rpad(col("NTNL_PROV_ID"), 50, " ").alias("NTNL_PROV_ID"),
    rpad(col("TXNMY_CD"), 50, " ").alias("TXNMY_CD"),
    rpad(col("ENTY_LIC_ST_CD"), 50, " ").alias("ENTY_LIC_ST_CD"),
    rpad(col("LIC_NO"), 50, " ").alias("LIC_NO"),
    rpad(col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_TXNMY_CD_SK").alias("GRP_TXNMY_CD_SK"),
    col("NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    col("TXNMY_CD_SK").alias("TXNMY_CD_SK"),
    col("ENTY_LIC_ST_CD_SK").alias("ENTY_LIC_ST_CD_SK"),
    rpad(col("CUR_RCRD_IN"), 1, " ").alias("CUR_RCRD_IN"),
    rpad(col("NTNL_PROV_TXNMY_ACTV_IN"), 1, " ").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    rpad(col("NTNL_PROV_TXNMY_PRI_IN"), 1, " ").alias("NTNL_PROV_TXNMY_PRI_IN")
)

write_files(
    df_seq_NTNL_PROV_TXNMY_FKey_write,
    f"{adls_path}/load/NTNL_PROV_TXNMY.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Stage: Fnl_LkpFail (PxFunnel) - union of fail links
df_Fnl_LkpFail = (
    df_lnk_LicStCdLkupFail
    .unionByName(df_lnk_GrpTxnmyCdLkupFail)
    .unionByName(df_lnk_NtnlProv_LkupFail)
    .unionByName(df_lnk_TxnmyCd_LkupFail)
)

# Stage: Seq_FKeyFailedFile (PxSequentialFile)
# Final select with rpad on string columns
df_Seq_FKeyFailedFile_write = df_Fnl_LkpFail.select(
    col("PRI_SK").alias("PRI_SK"),
    rpad(col("PRI_NAT_KEY_STRING"), 50, " ").alias("PRI_NAT_KEY_STRING"),
    rpad(col("SRC_SYS_CD_SK"), 50, " ").alias("SRC_SYS_CD_SK"),
    rpad(col("JOB_NM"), 50, " ").alias("JOB_NM"),
    rpad(col("ERROR_TYP"), 50, " ").alias("ERROR_TYP"),
    rpad(col("PHYSCL_FILE_NM"), 50, " ").alias("PHYSCL_FILE_NM"),
    rpad(col("FRGN_NAT_KEY_STRING"), 50, " ").alias("FRGN_NAT_KEY_STRING"),
    rpad(col("FIRST_RECYC_TS"), 50, " ").alias("FIRST_RECYC_TS"),
    col("JOB_EXCTN_SK").alias("JOB_EXCTN_SK")
)

write_files(
    df_Seq_FKeyFailedFile_write,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsNtnlProvTxnmyFkey_EE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)