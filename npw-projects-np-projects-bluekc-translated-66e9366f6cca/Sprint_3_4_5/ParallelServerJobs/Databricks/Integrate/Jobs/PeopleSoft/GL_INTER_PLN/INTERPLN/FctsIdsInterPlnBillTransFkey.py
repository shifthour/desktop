# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FctsBcbsExtInterPlnBillLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: Extract Data from GL_INTER_PLN_BILL_DTL and Load it to IDS 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC sudheer champati        10/12/2015       5212      Originally Programmed                                                                                  Kalyan Neelam    2016-01-04

# MAGIC This job is to generate the Foreign Keys to the ON_EXCH_FED_PYMT_DTL table.
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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

DSJobName = "FctsIdsInterPlnBillTransFkey"

# Read seq_INTERPLNBILL_Pkey (PxSequentialFile)
schema_seq_INTERPLNBILL_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("INTER_PLN_BILL_TRANS_SK", IntegerType(), nullable=False),
    StructField("INTER_PLN_BILL_TRANS_CK", IntegerType(), nullable=False),
    StructField("ACCTG_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("INTER_PLN_BILL_TRANS_LOB_CD_SK", IntegerType(), nullable=False),
    StructField("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PD_FROM_DT_SK", StringType(), nullable=False),
    StructField("PD_THRU_DT_SK", StringType(), nullable=False),
    StructField("CALC_FUND_AMT", DecimalType(38,10), nullable=False),
    StructField("FUND_RATE_AMT", DecimalType(38,10), nullable=False),
    StructField("LOBD_ID", StringType(), nullable=False),
    StructField("CRME_PYMT_METH_IND", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("BILL_DTL_SRL_NO", StringType(), nullable=False),
    StructField("PDPD_ID", StringType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=True),
    StructField("PDBL_ACCT_CAT", StringType(), nullable=False)
])

df_seq_INTERPLNBILL_Pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_INTERPLNBILL_Pkey)
    .load(f"{adls_path}/key/INTER_PLN_BILL_TRANS.{SrcSysCd}.pkey.{RunID}.dat")
)

# db2_K_FNCL_LOB (DB2ConnectorPX) - IDS
ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
query_db2_K_FNCL_LOB = f"SELECT DISTINCT FL.FNCL_LOB_CD, FL.FNCL_LOB_SK, FL.SRC_SYS_CD FROM {IDSOwner}.K_FNCL_LOB FL"
df_db2_K_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", query_db2_K_FNCL_LOB)
    .load()
)

# db2_K_GRP (DB2ConnectorPX) - IDS
query_db2_K_GRP = f"SELECT GRP.GRP_SK, GRP.GRP_ID, GRP.SRC_SYS_CD FROM {IDSOwner}.K_GRP GRP"
df_db2_K_GRP = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", query_db2_K_GRP)
    .load()
)

# db2_K_PROD (DB2ConnectorPX) - IDS
query_db2_K_PROD = f"SELECT Trim(PROD_ID) AS PROD_ID, SRC_SYS_CD, PROD_SK FROM {IDSOwner}.K_PROD"
df_db2_K_PROD = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", query_db2_K_PROD)
    .load()
)

# ds_CD_MPPNG_LkpData (PxDataSet) read from parquet
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_CdMppngData (PxFilter)
df_CdMppngExtr = df_ds_CD_MPPNG_LkpData

df_lnkSrcSysCdLkpData = (
    df_CdMppngExtr
    .filter(
        "SRC_CD='BCBSA' AND SRC_CLCTN_CD='IDS' "
        "AND SRC_DOMAIN_NM='SOURCE SYSTEM' AND TRGT_CLCTN_CD='IDS' "
        "AND TRGT_DOMAIN_NM='SOURCE SYSTEM'"
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_lnkTransLobCdLkpData = (
    df_CdMppngExtr
    .filter(
        "SRC_SYS_CD='FACETS' AND SRC_DOMAIN_NM='LOB' "
        "AND TRGT_CLCTN_CD='IDS' AND TRGT_DOMAIN_NM='CLAIM LINE LOB'"
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_lnkTransPymtMethCdLkpData = (
    df_CdMppngExtr
    .filter(
        "SRC_SYS_CD='PSI' AND TRGT_CLCTN_CD='IDS' AND "
        "SRC_DOMAIN_NM='CAPITATION TRANSACTION PAYMENT METHOD' AND "
        "TRGT_DOMAIN_NM='CAPITATION TRANSACTION PAYMENT METHOD'"
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

# LkupFkey (PxLookup) - perform left joins in sequence
df_LkupFkey = (
    df_seq_INTERPLNBILL_Pkey.alias("lnk_IdsInterPlnFkey_EE_InAbc")
    .join(
        df_db2_K_FNCL_LOB.alias("FNCL_LOB"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.PDBL_ACCT_CAT") == F.col("FNCL_LOB.FNCL_LOB_CD"),
        "left"
    )
    .join(
        df_db2_K_GRP.alias("GRP"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.GRGR_ID") == F.col("GRP.GRP_ID"),
        "left"
    )
    .join(
        df_db2_K_PROD.alias("PROD"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.PDPD_ID") == F.col("PROD.PROD_ID"),
        "left"
    )
    .join(
        df_lnkSrcSysCdLkpData.alias("lnkSrcSysCdLkpData"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.SRC_SYS_CD") == F.col("lnkSrcSysCdLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_lnkTransLobCdLkpData.alias("lnkTransLobCdLkpData"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.LOBD_ID") == F.col("lnkTransLobCdLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_lnkTransPymtMethCdLkpData.alias("lnkTransPymtMethCdLkpData"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.CRME_PYMT_METH_IND") == F.col("lnkTransPymtMethCdLkpData.SRC_CD"),
        "left"
    )
    .select(
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.INTER_PLN_BILL_TRANS_SK").alias("INTER_PLN_BILL_TRANS_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.INTER_PLN_BILL_TRANS_CK").alias("INTER_PLN_BILL_TRANS_CK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
        F.col("lnkSrcSysCdLkpData.CD_MPPNG_SK").alias("SRC_SYS_CD_MPPNG_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FNCL_LOB.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("GRP.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.MBR_SK").alias("MBR_SK"),
        F.col("PROD.PROD_SK").alias("PROD_SK"),
        F.col("lnkTransLobCdLkpData.CD_MPPNG_SK").alias("INTER_PLN_BILL_TRANS_LOB_CD_SKCD_MPPNG_SK"),
        F.col("lnkTransPymtMethCdLkpData.CD_MPPNG_SK").alias("INTER_PLN_BILL_TRANS_PYMT_METH_CD_MPPNG_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.PD_FROM_DT_SK").alias("PD_FROM_DT_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.PD_THRU_DT_SK").alias("PD_THRU_DT_SK"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.CALC_FUND_AMT").alias("CALC_FUND_AMT"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.FUND_RATE_AMT").alias("FUND_RATE_AMT"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO"),
        F.col("FNCL_LOB.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("GRP.GRP_ID").alias("GRP_ID"),
        F.col("PROD.PROD_ID").alias("PROD_ID"),
        F.col("FNCL_LOB.SRC_SYS_CD").alias("FNCL_SRC_SYS_CD"),
        F.col("GRP.SRC_SYS_CD").alias("GRP_SRC_SYS_CD"),
        F.col("PROD.SRC_SYS_CD").alias("PROD_SRC_SYS_CD"),
        F.col("lnk_IdsInterPlnFkey_EE_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
    )
)

# xfm_CheckLkpResults (CTransformerStage)
df_xfm_CheckLkpResults_base = (
    df_LkupFkey
    .withColumn("SvFnclLobFKeyLkpCheck", F.when(F.col("FNCL_LOB_SK").isNull(), F.lit('Y')).otherwise(F.lit('N')))
    .withColumn("SvGrpFKeyLkpCheck", F.when(F.col("GRP_SK").isNull(), F.lit('Y')).otherwise(F.lit('N')))
    .withColumn("SvProdFKeyLkpCheck", F.when(F.col("PROD_SK").isNull(), F.lit('Y')).otherwise(F.lit('N')))
    .withColumn("SvSrcSysCdMppngLkpCheck", F.when(F.col("SRC_SYS_CD_MPPNG_SK").isNull(), F.lit('Y')).otherwise(F.lit('N')))
    .withColumn("SvTransLobCdMppngLkpCheck", F.when(F.col("INTER_PLN_BILL_TRANS_LOB_CD_SKCD_MPPNG_SK").isNull(), F.lit('Y')).otherwise(F.lit('N')))
    .withColumn("SvTransPymtCdMppngLkpCheck", F.when(F.col("INTER_PLN_BILL_TRANS_PYMT_METH_CD_MPPNG_SK").isNull(), F.lit('Y')).otherwise(F.lit('N')))
)

# Lnk_Main (no constraint => all rows) - select columns
df_xfm_Lnk_Main = df_xfm_CheckLkpResults_base.select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("INTER_PLN_BILL_TRANS_SK"),
    F.col("INTER_PLN_BILL_TRANS_CK").alias("INTER_PLN_BILL_TRANS_CK"),
    F.col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    F.col("SRC_SYS_CD_MPPNG_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("INTER_PLN_BILL_TRANS_LOB_CD_SKCD_MPPNG_SK").alias("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
    F.col("INTER_PLN_BILL_TRANS_PYMT_METH_CD_MPPNG_SK").alias("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
    F.col("PD_FROM_DT_SK").alias("PD_FROM_DT_SK"),
    F.col("PD_THRU_DT_SK").alias("PD_THRU_DT_SK"),
    F.col("CALC_FUND_AMT").alias("CALC_FUND_AMT"),
    F.col("FUND_RATE_AMT").alias("FUND_RATE_AMT"),
    F.col("BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO")
)

# lnk_FnclLobTypCdLkup_Fail
df_xfm_lnk_FnclLobTypCdLkp_Fail = df_xfm_CheckLkpResults_base.filter(
    F.col("SvFnclLobFKeyLkpCheck") == F.lit("Y")
).select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_FNCL_LOB").alias("PHYSCL_FILE_NM"),
    F.expr("FNCL_SRC_SYS_CD || FNCL_LOB_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_UNK => single row with literal columns
df_xfm_Lnk_UNK = (
    df_xfm_CheckLkpResults_base.limit(1)
    .select(
        F.lit(0).alias("INTER_PLN_BILL_TRANS_SK"),
        F.lit(0).alias("INTER_PLN_BILL_TRANS_CK"),
        F.lit("UNK").alias("ACCTG_DT_SK"),
        F.lit("UNK").alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FNCL_LOB_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("PROD_SK"),
        F.lit(0).alias("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
        F.lit(0).alias("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
        F.lit("1753-01-01").alias("PD_FROM_DT_SK"),
        F.lit("2199-12-31").alias("PD_THRU_DT_SK"),
        F.lit(0).alias("CALC_FUND_AMT"),
        F.lit(0).alias("FUND_RATE_AMT"),
        F.lit(0).alias("BILL_DTL_SRL_NO")
    )
)

# Lnk_NA => single row with literal columns
df_xfm_Lnk_NA = (
    df_xfm_CheckLkpResults_base.limit(1)
    .select(
        F.lit(1).alias("INTER_PLN_BILL_TRANS_SK"),
        F.lit(1).alias("INTER_PLN_BILL_TRANS_CK"),
        F.lit("NA").alias("ACCTG_DT_SK"),
        F.lit("NA").alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("FNCL_LOB_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("PROD_SK"),
        F.lit(1).alias("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
        F.lit(1).alias("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
        F.lit("1753-01-01").alias("PD_FROM_DT_SK"),
        F.lit("2199-12-31").alias("PD_THRU_DT_SK"),
        F.lit(0).alias("CALC_FUND_AMT"),
        F.lit(0).alias("FUND_RATE_AMT"),
        F.lit(1).alias("BILL_DTL_SRL_NO")
    )
)

# lnk_GrpTypCdLkup_Fail (assuming correct constraint is SvGrpFKeyLkpCheck = 'Y')
df_xfm_lnk_GrpTypCdLkp_Fail = df_xfm_CheckLkpResults_base.filter(
    F.col("SvGrpFKeyLkpCheck") == F.lit("Y")
).select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("K_GRP").alias("PHYSCL_FILE_NM"),
    F.expr("GRP_SRC_SYS_CD || GRP_ID").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_ProdTypCdLkup_Fail
df_xfm_lnk_ProdTypCdLkp_Fail = df_xfm_CheckLkpResults_base.filter(
    F.col("SvProdFKeyLkpCheck") == F.lit("Y")
).select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("K_PROD_TYP_CD").alias("PHYSCL_FILE_NM"),
    F.expr("PROD_SRC_SYS_CD || PROD_ID").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_SrcSysCdLKup_Fail
df_xfm_lnk_SrcSysCdLKup_Fail = df_xfm_CheckLkpResults_base.filter(
    F.col("SvSrcSysCdMppngLkpCheck") == F.lit("Y")
).select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("'BCBSA' || ';' || 'IDS' || ';' || 'IDS' || ';' || 'SOURCE SYSTEM' || ';' || 'SOURCE SYSTEM' || ';' || SRC_SYS_CD_MPPNG_SK").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_TransLobCdLkp_Fail
df_xfm_lnk_TransLobCdLkp_Fail = df_xfm_CheckLkpResults_base.filter(
    F.col("SvTransLobCdMppngLkpCheck") == F.lit("Y")
).select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("'FACETS' || ';' || 'IDS' || ';' || 'IDS' || ';' || 'LOB' || ';' || 'CLAIM LINE LOB' || INTER_PLN_BILL_TRANS_LOB_CD_SKCD_MPPNG_SK").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_TransPymtMethCdLkp_Fail
df_xfm_lnk_TransPymtMethCdLkp_Fail = df_xfm_CheckLkpResults_base.filter(
    F.col("SvTransPymtCdMppngLkpCheck") == F.lit("Y")
).select(
    F.col("INTER_PLN_BILL_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("FNCL_SRC_SYS_CD || ';' || 'IDS' || ';' || 'IDS' || ';' || 'CAPITATION TRANSACTION PAYMENT METHOD' || ';' || 'CAPITATION TRANSACTION PAYMENT METHOD' || INTER_PLN_BILL_TRANS_PYMT_METH_CD_MPPNG_SK").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# fnl_NA_UNK_Streams (PxFunnel) - union of Lnk_Main, Lnk_UNK, Lnk_NA
df_fnl_NA_UNK_Streams = df_xfm_Lnk_Main.unionByName(df_xfm_Lnk_UNK).unionByName(df_xfm_Lnk_NA)

# seq_INTER_PLAN_BILL_FKey (PxSequentialFile) - write .dat
# Apply rpad for char/varchar columns in final select
df_fnl_NA_UNK_Streams_write = df_fnl_NA_UNK_Streams.select(
    F.col("INTER_PLN_BILL_TRANS_SK"),
    F.col("INTER_PLN_BILL_TRANS_CK"),
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),  # char(10)
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SK"),
    F.col("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
    F.col("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
    F.rpad(F.col("PD_FROM_DT_SK"), 10, " ").alias("PD_FROM_DT_SK"),  # char(10)
    F.rpad(F.col("PD_THRU_DT_SK"), 10, " ").alias("PD_THRU_DT_SK"),  # char(10)
    F.col("CALC_FUND_AMT"),
    F.col("FUND_RATE_AMT"),
    F.rpad(F.col("BILL_DTL_SRL_NO"), 255, " ").alias("BILL_DTL_SRL_NO")  # varchar => assume 255
)

write_files(
    df_fnl_NA_UNK_Streams_write,
    f"{adls_path}/load/INTER_PLN_BILL_TRANS.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# FnlFkeyFailures (PxFunnel) - union of the fail DataFrames
df_FnlFkeyFailures = (
    df_xfm_lnk_GrpTypCdLkp_Fail
    .unionByName(df_xfm_lnk_ProdTypCdLkp_Fail)
    .unionByName(df_xfm_lnk_FnclLobTypCdLkp_Fail)
    .unionByName(df_xfm_lnk_SrcSysCdLKup_Fail)
    .unionByName(df_xfm_lnk_TransLobCdLkp_Fail)
    .unionByName(df_xfm_lnk_TransPymtMethCdLkp_Fail)
)

# seq_FkeyFailedFile (PxSequentialFile) - write
df_FnlFkeyFailures_write = df_FnlFkeyFailures.select(
    F.col("PRI_SK"),
    F.rpad(F.col("PRI_NAT_KEY_STRING"), 255, " ").alias("PRI_NAT_KEY_STRING"),  # varchar => 255
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("JOB_NM"), 255, " ").alias("JOB_NM"),            # assume varchar => 255
    F.rpad(F.col("ERROR_TYP"), 255, " ").alias("ERROR_TYP"),      # assume varchar => 255
    F.rpad(F.col("PHYSCL_FILE_NM"), 255, " ").alias("PHYSCL_FILE_NM"),  # assume varchar => 255
    F.rpad(F.col("FRGN_NAT_KEY_STRING"), 255, " ").alias("FRGN_NAT_KEY_STRING"),  # varchar => 255
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

write_files(
    df_FnlFkeyFailures_write,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)