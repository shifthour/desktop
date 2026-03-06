# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign key.      
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC       
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Giri Mallavaram        10/16/2019      6131 PBM Replacement               Initial Programming                       IntegrateDevl                 Kalyan Neelam          10/21/2019
# MAGIC 
# MAGIC Bhargava Rampilla   2019-10-24   6131-PBM REPLACEMENT        Modified feilds as per the              IntegrateDEV1                   Kalyan Neelam           2019-11-20         
# MAGIC                                                                                                                                 OptumRX transformation rules:
# MAGIC                                                                                                                                  EDI_DEST_QLFR_ TX
# MAGIC                                                                                                                                  NTNL_PROV_ID

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Flat files created to email lists of UNK codes to knowledge stewards
# MAGIC load file - PROV.dat
# MAGIC temporary file -  PROV.tmp
# MAGIC Do all my FK lookups here
# MAGIC Recycle records with ErrCount > 0
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
)
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value('InFile', 'IdsProvExtr.DrugProv.uniq')
Logging = get_widget_value('Logging', 'Y')

# Schema definition for IdsProvExtr (CSeqFileStage)
schema_IdsProvExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),     # char(10)
    StructField("DISCARD_IN", StringType(), nullable=False),        # char(1)
    StructField("PASS_THRU_IN", StringType(), nullable=False),      # char(1)
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False), # numeric
    StructField("SRC_SYS_CD", StringType(), nullable=False),        # varchar
    StructField("PRI_KEY_STRING", StringType(), nullable=False),    # varchar
    StructField("PROV_SK", IntegerType(), nullable=False),          # primary key
    StructField("PROV_ID", StringType(), nullable=False),           # char(12)
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CMN_PRCT", StringType(), nullable=False),          # char(12)
    StructField("PROV_BILL_SVC_SK", StringType(), nullable=False),  # char(10)
    StructField("REL_GRP_PROV", StringType(), nullable=False),      # char(12)
    StructField("REL_IPA_PROV", StringType(), nullable=False),      # char(12)
    StructField("PROV_CAP_PAYMT_EFT_METH_CD", StringType(), nullable=False),  # char(1)
    StructField("PROV_CLM_PAYMT_EFT_METH_CD", StringType(), nullable=False),  # char(1)
    StructField("PROV_CLM_PAYMT_METH_CD", StringType(), nullable=False),      # char(1)
    StructField("PROV_ENTY_CD", StringType(), nullable=False),      # char(1)
    StructField("PROV_FCLTY_TYP_CD", StringType(), nullable=False), # char(4)
    StructField("PROV_PRCTC_TYP_CD", StringType(), nullable=False), # char(4)
    StructField("PROV_SVC_CAT_CD", StringType(), nullable=False),   # char(4)
    StructField("PROV_SPEC_CD", StringType(), nullable=False),      # char(4)
    StructField("PROV_STTUS_CD", StringType(), nullable=False),     # char(2)
    StructField("PROV_TERM_RSN_CD", StringType(), nullable=False),  # char(4)
    StructField("PROV_TYP_CD", StringType(), nullable=False),       # char(4)
    StructField("TERM_DT", TimestampType(), nullable=False),
    StructField("PAYMT_HOLD_DT", TimestampType(), nullable=False),
    StructField("CLRNGHOUSE_ID", StringType(), nullable=False),     # char(30)
    StructField("EDI_DEST_ID", StringType(), nullable=False),       # char(15)
    StructField("EDI_DEST_QUAL", StringType(), nullable=False),     # char(2)
    StructField("NTNL_PROV_ID", StringType(), nullable=False),      # char(10)
    StructField("PROV_ADDR_ID", StringType(), nullable=False),      # char(12)
    StructField("PROV_NM", StringType(), nullable=True),            # varchar
    StructField("TAX_ID", StringType(), nullable=True),             # char(9)
    StructField("TXNMY_CD", StringType(), nullable=False)           # varchar
])

# Read IdsProvExtr (CSeqFileStage) from a delimited file, preserving extension
df_IdsProvExtr = (
    spark.read
    .format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsProvExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# Apply the Transformer logic (ForeignKey1) with stage variables
df_stagevars = (
    df_IdsProvExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(lit("IDS"), col("PROV_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), Logging)
    )
    .withColumn(
        "RelGrpProvSk",
        GetFkeyProv(col("SRC_SYS_CD"), col("PROV_SK"), col("REL_GRP_PROV"), Logging)
    )
    .withColumn(
        "RelIpaProvSk",
        GetFkeyProv(col("SRC_SYS_CD"), col("PROV_SK"), col("REL_IPA_PROV"), Logging)
    )
    .withColumn(
        "ProvFcltyTypCdSk",
        GetFkeyFcltyTyp(col("SRC_SYS_CD"), col("PROV_SK"), col("PROV_FCLTY_TYP_CD"), Logging)
    )
    .withColumn(
        "ProvPrctcTypCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER PRACTICE TYPE"), col("PROV_PRCTC_TYP_CD"), Logging)
    )
    .withColumn(
        "ProvSvcCatCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER SERVICE CATEGORY"), col("PROV_SVC_CAT_CD"), Logging)
    )
    .withColumn(
        "ProvEntyCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER ENTITY"), col("PROV_ENTY_CD"), Logging)
    )
    .withColumn(
        "ProvSpecCdSk",
        GetFkeyProvSpec(col("SRC_SYS_CD"), col("PROV_SK"), col("PROV_SPEC_CD"), Logging)
    )
    .withColumn(
        "ProvSttusCdSK",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER STATUS"), col("PROV_STTUS_CD"), Logging)
    )
    .withColumn(
        "ProvTermRsnCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER TERMINATION REASON"), col("PROV_TERM_RSN_CD"), Logging)
    )
    .withColumn(
        "ProvTypCdSk",
        GetFkeyProvTyp(col("SRC_SYS_CD"), col("PROV_SK"), col("PROV_TYP_CD"), Logging)
    )
    .withColumn(
        "ProvCapPaymtEftMethCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER CAPITATION PAYMENT EFT METHOD"), col("PROV_CAP_PAYMT_EFT_METH_CD"), Logging)
    )
    .withColumn(
        "ProvClmPaymtEftMethCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER CLAIM PAYMENT EFT METHOD"), col("PROV_CLM_PAYMT_EFT_METH_CD"), Logging)
    )
    .withColumn(
        "ProvClmPaymtMethCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_SK"), lit("PROVIDER CLAIM PAYMENT METHOD"), col("PROV_CLM_PAYMT_METH_CD"), Logging)
    )
    .withColumn(
        "PaymtHoldDtSk",
        GetFkeyDate(lit("IDS"), col("PROV_SK"), col("PAYMT_HOLD_DT"), Logging)
    )
    .withColumn(
        "ProvBillSvcSkVar",
        when(col("PROV_BILL_SVC_SK") == lit("NA"), lit(1)).otherwise(col("PROV_BILL_SVC_SK"))
    )
    .withColumn(
        "TermDtSk",
        GetFkeyDate(lit("IDS"), col("PROV_SK"), col("TERM_DT"), Logging)
    )
    .withColumn(
        "CmnPrtFacetsSK",
        GetFkeyCmnPrct(lit("FACETS"), col("PROV_SK"), col("CMN_PRCT"), lit("X"))
    )
    .withColumn(
        "CmnPrtVcacSK",
        GetFkeyCmnPrct(lit("VCAC"), col("PROV_SK"), col("CMN_PRCT"), lit("X"))
    )
    .withColumn(
        "CmnPrctSkVar",
        when(length(trim(col("CMN_PRCT"))) == 0, lit(1))
        .otherwise(
            when(col("CmnPrtFacetsSK") == 0, col("CmnPrtVcacSK")).otherwise(col("CmnPrtFacetsSK"))
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("PROV_SK")))
    .withColumn("svNtnlProvSK", GetFkeyNtnlProvId(lit("CMS"), col("PROV_SK"), col("NTNL_PROV_ID"), Logging))
)

# Fkey1: Constraint => (ErrCount=0) or (PassThru='Y')
df_fkey1 = (
    df_stagevars
    .filter((col("ErrCount") == 0) | (col("PassThru") == lit("Y")))
    .select(
        col("PROV_SK").alias("PROV_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("PROV_ID").alias("PROV_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("SRC_SYS_CD") == lit("OPTUMRX"), lit(1)).otherwise(col("CmnPrctSkVar")).alias("CMN_PRCT_SK"),
        when(col("SRC_SYS_CD") == lit("OPTUMRX"), lit(1)).otherwise(col("ProvBillSvcSkVar")).alias("PROV_BILL_SVC_SK"),
        col("RelGrpProvSk").alias("REL_GRP_PROV_SK"),
        col("RelIpaProvSk").alias("REL_IPA_PROV_SK"),
        col("ProvCapPaymtEftMethCdSk").alias("PROV_CAP_PAYMT_EFT_METH_CD_SK"),
        col("ProvClmPaymtEftMethCdSk").alias("PROV_CLM_PAYMT_EFT_METH_CD_SK"),
        col("ProvClmPaymtMethCdSk").alias("PROV_CLM_PAYMT_METH_CD_SK"),
        col("ProvEntyCdSk").alias("PROV_ENTY_CD_SK"),
        col("ProvFcltyTypCdSk").alias("PROV_FCLTY_TYP_CD_SK"),
        col("ProvPrctcTypCdSk").alias("PROV_PRCTC_TYP_CD_SK"),
        col("ProvSvcCatCdSk").alias("PROV_SVC_CAT_CD_SK"),
        col("ProvSpecCdSk").alias("PROV_SPEC_CD_SK"),
        col("ProvSttusCdSK").alias("PROV_STTUS_CD_SK"),
        col("ProvTermRsnCdSk").alias("PROV_TERM_RSN_CD_SK"),
        col("ProvTypCdSk").alias("PROV_TYP_CD_SK"),
        col("TermDtSk").alias("TERM_DT_SK"),
        col("PaymtHoldDtSk").alias("PAYMT_HOLD_DT_SK"),
        col("CLRNGHOUSE_ID").alias("CLRNGHSE_ID"),
        col("EDI_DEST_ID").alias("EDI_DEST_ID"),
        when(col("SRC_SYS_CD") == lit("OPTUMRX"), lit(None)).otherwise(col("EDI_DEST_QUAL")).alias("EDI_DEST_QLFR_TX"),
        when(
            col("NTNL_PROV_ID").isNull() | (trim(col("NTNL_PROV_ID")) == ""),
            lit("1")
        ).otherwise(col("NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
        col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("PROV_NM").alias("PROV_NM"),
        col("TAX_ID").alias("TAX_ID"),
        col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
        col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        col("PROV_TYP_CD").alias("PROV_TYP_CD"),
        when(col("NTNL_PROV_ID") == lit("NA"), lit(1))
        .when(col("NTNL_PROV_ID").isNull(), lit(0))
        .otherwise(col("svNtnlProvSK")).alias("NTNL_PROV_SK"),
        col("TXNMY_CD").alias("TXNMY_CD")
    )
)

# recycle1: Constraint => ErrCount > 0
df_recycle1 = (
    df_stagevars
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("PROV_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),  # PK
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("PROV_SK").alias("PROV_SK"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        col("PROV_ID").alias("PROV_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CMN_PRCT").alias("CMN_PRCT_ID"),
        col("PROV_BILL_SVC_SK").alias("PROV_BILL_SVC_SK"),
        col("REL_GRP_PROV").alias("REL_GRP_PROV_ID"),
        col("REL_IPA_PROV").alias("REL_IPA_PROV_ID"),
        col("PROV_CAP_PAYMT_EFT_METH_CD").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
        col("PROV_CLM_PAYMT_EFT_METH_CD").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
        col("PROV_CLM_PAYMT_METH_CD").alias("PROV_CLM_PAYMT_METH_CD"),
        col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
        col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYPE_CD"),
        col("PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
        col("PROV_SVC_CAT_CD").alias("PROV_SVC_CAT_CD"),
        col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        col("PROV_STTUS_CD").alias("PROV_STTUS_CD"),
        col("PROV_TERM_RSN_CD").alias("PROV_TERM_RSN_CD"),
        col("PROV_TYP_CD").alias("PROV_TYPE_CD"),
        col("TERM_DT").alias("TERM_DT"),
        col("PAYMT_HOLD_DT").alias("PAYMT_HOLD_DT_SK"),
        col("CLRNGHOUSE_ID").alias("CLRNGHSE_ID"),
        col("EDI_DEST_ID").alias("EDI_DEST_ID"),
        col("EDI_DEST_QUAL").alias("EDI_DEST_QLFR_TX"),
        col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("PROV_NM").alias("PROV_NM"),
        col("TAX_ID").alias("TAX_ID"),
        col("TXNMY_CD").alias("TXNMY_CD")
    )
)

# hf_recycle => scenario C => write to parquet
df_recycle1_for_write = df_recycle1

# RPad char/varchar columns with specified lengths before writing
df_recycle1_for_write = (
    df_recycle1_for_write
    # char(10) => INSRT_UPDT_CD
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    # char(1) => DISCARD_IN
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    # char(1) => PASS_THRU_IN
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    # char(12) => PROV_ID
    .withColumn("PROV_ID", rpad(col("PROV_ID"), 12, " "))
    # char(12) => CMN_PRCT_ID
    .withColumn("CMN_PRCT_ID", rpad(col("CMN_PRCT_ID"), 12, " "))
    # char(10) => PROV_BILL_SVC_SK
    .withColumn("PROV_BILL_SVC_SK", rpad(col("PROV_BILL_SVC_SK"), 10, " "))
    # char(12) => REL_GRP_PROV_ID
    .withColumn("REL_GRP_PROV_ID", rpad(col("REL_GRP_PROV_ID"), 12, " "))
    # char(12) => REL_IPA_PROV_ID
    .withColumn("REL_IPA_PROV_ID", rpad(col("REL_IPA_PROV_ID"), 12, " "))
    # char(1) => PROV_CAP_PAYMT_EFT_METH_CD
    .withColumn("PROV_CAP_PAYMT_EFT_METH_CD", rpad(col("PROV_CAP_PAYMT_EFT_METH_CD"), 1, " "))
    # char(1) => PROV_CLM_PAYMT_EFT_METH_CD
    .withColumn("PROV_CLM_PAYMT_EFT_METH_CD", rpad(col("PROV_CLM_PAYMT_EFT_METH_CD"), 1, " "))
    # char(1) => PROV_CLM_PAYMT_METH_CD
    .withColumn("PROV_CLM_PAYMT_METH_CD", rpad(col("PROV_CLM_PAYMT_METH_CD"), 1, " "))
    # char(1) => PROV_ENTY_CD
    .withColumn("PROV_ENTY_CD", rpad(col("PROV_ENTY_CD"), 1, " "))
    # char(4) => PROV_FCLTY_TYPE_CD
    .withColumn("PROV_FCLTY_TYPE_CD", rpad(col("PROV_FCLTY_TYPE_CD"), 4, " "))
    # char(4) => PROV_PRCTC_TYP_CD
    .withColumn("PROV_PRCTC_TYP_CD", rpad(col("PROV_PRCTC_TYP_CD"), 4, " "))
    # char(4) => PROV_SVC_CAT_CD
    .withColumn("PROV_SVC_CAT_CD", rpad(col("PROV_SVC_CAT_CD"), 4, " "))
    # char(4) => PROV_SPEC_CD
    .withColumn("PROV_SPEC_CD", rpad(col("PROV_SPEC_CD"), 4, " "))
    # char(2) => PROV_STTUS_CD
    .withColumn("PROV_STTUS_CD", rpad(col("PROV_STTUS_CD"), 2, " "))
    # char(4) => PROV_TERM_RSN_CD
    .withColumn("PROV_TERM_RSN_CD", rpad(col("PROV_TERM_RSN_CD"), 4, " "))
    # char(4) => PROV_TYPE_CD
    .withColumn("PROV_TYPE_CD", rpad(col("PROV_TYPE_CD"), 4, " "))
    # char(30) => CLRNGHSE_ID
    .withColumn("CLRNGHSE_ID", rpad(col("CLRNGHSE_ID"), 30, " "))
    # char(15) => EDI_DEST_ID
    .withColumn("EDI_DEST_ID", rpad(col("EDI_DEST_ID"), 15, " "))
    # char(2) => EDI_DEST_QLFR_TX
    .withColumn("EDI_DEST_QLFR_TX", rpad(col("EDI_DEST_QLFR_TX"), 2, " "))
    # char(10) => NTNL_PROV_ID
    .withColumn("NTNL_PROV_ID", rpad(col("NTNL_PROV_ID"), 10, " "))
    # char(12) => PROV_ADDR_ID
    .withColumn("PROV_ADDR_ID", rpad(col("PROV_ADDR_ID"), 12, " "))
    # char(9) => TAX_ID
    .withColumn("TAX_ID", rpad(col("TAX_ID"), 9, " "))
)

write_files(
    df_recycle1_for_write.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PROV_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CMN_PRCT_ID",
        "PROV_BILL_SVC_SK",
        "REL_GRP_PROV_ID",
        "REL_IPA_PROV_ID",
        "PROV_CAP_PAYMT_EFT_METH_CD",
        "PROV_CLM_PAYMT_EFT_METH_CD",
        "PROV_CLM_PAYMT_METH_CD",
        "PROV_ENTY_CD",
        "PROV_FCLTY_TYPE_CD",
        "PROV_PRCTC_TYP_CD",
        "PROV_SVC_CAT_CD",
        "PROV_SPEC_CD",
        "PROV_STTUS_CD",
        "PROV_TERM_RSN_CD",
        "PROV_TYPE_CD",
        "TERM_DT",
        "PAYMT_HOLD_DT_SK",
        "CLRNGHSE_ID",
        "EDI_DEST_ID",
        "EDI_DEST_QLFR_TX",
        "NTNL_PROV_ID",
        "PROV_ADDR_ID",
        "PROV_NM",
        "TAX_ID",
        "TXNMY_CD"
    ),
    f"hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# DefaultUNK => single-row DataFrame of constants
# Matches the output columns of "DefaultUNK" link
data_defaultUNK = [(
    0,  # PROV_SK
    0,  # SRC_SYS_CD_SK
    "UNK",  # PROV_ID (char(12))
    0,  # CRT_RUN_CYC_EXCTN_SK
    0,  # LAST_UPDT_RUN_CYC_EXCTN_SK
    0,  # CMN_PRCT_SK
    0,  # PROV_BILL_SVC_SK
    0,  # REL_GRP_PROV_SK
    0,  # REL_IPA_PROV_SK
    0,  # PROV_CAP_PAYMT_EFT_METH_CD_SK
    0,  # PROV_CLM_PAYMT_EFT_METH_CD_SK
    0,  # PROV_CLM_PAYMT_METH_CD_SK
    0,  # PROV_ENTY_CD_SK
    0,  # PROV_FCLTY_TYP_CD_SK
    0,  # PROV_PRCTC_TYP_CD_SK
    0,  # PROV_SVC_CAT_CD_SK
    0,  # PROV_SPEC_CD_SK
    0,  # PROV_STTUS_CD_SK
    0,  # PROV_TERM_RSN_CD_SK
    0,  # PROV_TYP_CD_SK
    "1753-01-01",  # TERM_DT_SK (char(10))
    "1753-01-01",  # PAYMT_HOLD_DT_SK (char(10))
    "UNK",  # CLRNGHSE_ID (char(30))
    "UNK",  # EDI_DEST_ID (char(15))
    "UNK",  # EDI_DEST_QLFR_TX (char(2))
    "UNK",  # NTNL_PROV_ID (char(10))
    "UNK",  # PROV_ADDR_ID (char(12))
    "UNK",  # PROV_NM
    "UNK",  # TAX_ID (char(9))
    "UNK",  # PROV_FCLTY_TYP_CD (char(4))
    "UNK",  # PROV_SPEC_CD (char(4))
    "UNK",  # PROV_TYP_CD (char(4))
    0,     # NTNL_PROV_SK
    "UNK"  # TXNMY_CD
)]
schema_defaultUNK = StructType([
    StructField("PROV_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CMN_PRCT_SK", IntegerType(), nullable=False),
    StructField("PROV_BILL_SVC_SK", IntegerType(), nullable=False),
    StructField("REL_GRP_PROV_SK", IntegerType(), nullable=False),
    StructField("REL_IPA_PROV_SK", IntegerType(), nullable=False),
    StructField("PROV_CAP_PAYMT_EFT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_CLM_PAYMT_EFT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_CLM_PAYMT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_ENTY_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_FCLTY_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_PRCTC_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_SVC_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_SPEC_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_TERM_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("PAYMT_HOLD_DT_SK", StringType(), nullable=False),
    StructField("CLRNGHSE_ID", StringType(), nullable=False),
    StructField("EDI_DEST_ID", StringType(), nullable=False),
    StructField("EDI_DEST_QLFR_TX", StringType(), nullable=False),
    StructField("NTNL_PROV_ID", StringType(), nullable=False),
    StructField("PROV_ADDR_ID", StringType(), nullable=False),
    StructField("PROV_NM", StringType(), nullable=False),
    StructField("TAX_ID", StringType(), nullable=False),
    StructField("PROV_FCLTY_TYP_CD", StringType(), nullable=False),
    StructField("PROV_SPEC_CD", StringType(), nullable=False),
    StructField("PROV_TYP_CD", StringType(), nullable=False),
    StructField("NTNL_PROV_SK", IntegerType(), nullable=False),
    StructField("TXNMY_CD", StringType(), nullable=False)
])
df_defaultUNK = spark.createDataFrame(data_defaultUNK, schema_defaultUNK)

# DefaultNA => single-row DataFrame of constants
data_defaultNA = [(
    1,  # PROV_SK
    1,  # SRC_SYS_CD_SK
    "NA",  # PROV_ID
    1,  # CRT_RUN_CYC_EXCTN_SK
    1,  # LAST_UPDT_RUN_CYC_EXCTN_SK
    1,  # CMN_PRCT_SK
    1,  # PROV_BILL_SVC_SK
    1,  # REL_GRP_PROV_SK
    1,  # REL_IPA_PROV_SK
    1,  # PROV_CAP_PAYMT_EFT_METH_CD_SK
    1,  # PROV_CLM_PAYMT_EFT_METH_CD_SK
    1,  # PROV_CLM_PAYMT_METH_CD_SK
    1,  # PROV_ENTY_CD_SK
    1,  # PROV_FCLTY_TYP_CD_SK
    1,  # PROV_PRCTC_TYP_CD_SK
    1,  # PROV_SVC_CAT_CD_SK
    1,  # PROV_SPEC_CD_SK
    1,  # PROV_STTUS_CD_SK
    1,  # PROV_TERM_RSN_CD_SK
    1,  # PROV_TYP_CD_SK
    "1753-01-01",  # TERM_DT_SK
    "1753-01-01",  # PAYMT_HOLD_DT_SK
    "NA",  # CLRNGHSE_ID
    "NA",  # EDI_DEST_ID
    "NA",  # EDI_DEST_QLFR_TX
    "NA",  # NTNL_PROV_ID
    "NA",  # PROV_ADDR_ID
    "NA",  # PROV_NM
    "NA",  # TAX_ID
    "NA",  # PROV_FCLTY_TYP_CD
    "NA",  # PROV_SPEC_CD
    "NA",  # PROV_TYP_CD
    1,    # NTNL_PROV_SK
    "NA"  # TXNMY_CD
)]
df_defaultNA = spark.createDataFrame(data_defaultNA, schema_defaultUNK)

# Collector => union Fkey1, DefaultUNK, DefaultNA
df_collector = df_fkey1.unionByName(df_defaultUNK).unionByName(df_defaultNA)

# RPad columns before writing to "PROV.tmp" (CSeqFileStage)
df_collector_rpad = (
    df_collector
    # char(12) => PROV_ID
    .withColumn("PROV_ID", rpad(col("PROV_ID"), 12, " "))
    # char(10) => TERM_DT_SK
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    # char(10) => PAYMT_HOLD_DT_SK
    .withColumn("PAYMT_HOLD_DT_SK", rpad(col("PAYMT_HOLD_DT_SK"), 10, " "))
    # char(30) => CLRNGHSE_ID
    .withColumn("CLRNGHSE_ID", rpad(col("CLRNGHSE_ID"), 30, " "))
    # char(15) => EDI_DEST_ID
    .withColumn("EDI_DEST_ID", rpad(col("EDI_DEST_ID"), 15, " "))
    # char(2) => EDI_DEST_QLFR_TX
    .withColumn("EDI_DEST_QLFR_TX", rpad(col("EDI_DEST_QLFR_TX"), 2, " "))
    # char(10) => NTNL_PROV_ID
    .withColumn("NTNL_PROV_ID", rpad(col("NTNL_PROV_ID"), 10, " "))
    # char(12) => PROV_ADDR_ID
    .withColumn("PROV_ADDR_ID", rpad(col("PROV_ADDR_ID"), 12, " "))
    # char(9) => TAX_ID
    .withColumn("TAX_ID", rpad(col("TAX_ID"), 9, " "))
    # char(4) => PROV_FCLTY_TYP_CD
    .withColumn("PROV_FCLTY_TYP_CD", rpad(col("PROV_FCLTY_TYP_CD"), 4, " "))
    # char(4) => PROV_SPEC_CD
    .withColumn("PROV_SPEC_CD", rpad(col("PROV_SPEC_CD"), 4, " "))
    # char(4) => PROV_TYP_CD
    .withColumn("PROV_TYP_CD", rpad(col("PROV_TYP_CD"), 4, " "))
)

# Write to "PROV.tmp"
write_files(
    df_collector_rpad.select(
        "PROV_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CMN_PRCT_SK",
        "PROV_BILL_SVC_SK",
        "REL_GRP_PROV_SK",
        "REL_IPA_PROV_SK",
        "PROV_CAP_PAYMT_EFT_METH_CD_SK",
        "PROV_CLM_PAYMT_EFT_METH_CD_SK",
        "PROV_CLM_PAYMT_METH_CD_SK",
        "PROV_ENTY_CD_SK",
        "PROV_FCLTY_TYP_CD_SK",
        "PROV_PRCTC_TYP_CD_SK",
        "PROV_SVC_CAT_CD_SK",
        "PROV_SPEC_CD_SK",
        "PROV_STTUS_CD_SK",
        "PROV_TERM_RSN_CD_SK",
        "PROV_TYP_CD_SK",
        "TERM_DT_SK",
        "PAYMT_HOLD_DT_SK",
        "CLRNGHSE_ID",
        "EDI_DEST_ID",
        "EDI_DEST_QLFR_TX",
        "NTNL_PROV_ID",
        "PROV_ADDR_ID",
        "PROV_NM",
        "TAX_ID",
        "PROV_FCLTY_TYP_CD",
        "PROV_SPEC_CD",
        "PROV_TYP_CD",
        "NTNL_PROV_SK",
        "TXNMY_CD"
    ),
    f"{adls_path}/load/PROV.tmp",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Read "PROV.tmp" again into df_ProvTmp
schema_ProvTmp = StructType([
    StructField("PROV_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CMN_PRCT_SK", IntegerType(), nullable=False),
    StructField("PROV_BILL_SVC_SK", IntegerType(), nullable=False),
    StructField("REL_GRP_PROV_SK", IntegerType(), nullable=False),
    StructField("REL_IPA_PROV_SK", IntegerType(), nullable=False),
    StructField("PROV_CAP_PAYMT_EFT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_CLM_PAYMT_EFT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_CLM_PAYMT_METH_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_ENTY_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_FCLTY_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_PRCTC_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_SVC_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_SPEC_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_TERM_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("PAYMT_HOLD_DT_SK", StringType(), nullable=False),
    StructField("CLRNGHSE_ID", StringType(), nullable=False),
    StructField("EDI_DEST_ID", StringType(), nullable=False),
    StructField("EDI_DEST_QLFR_TX", StringType(), nullable=True),
    StructField("NTNL_PROV_ID", StringType(), nullable=False),
    StructField("PROV_ADDR_ID", StringType(), nullable=False),
    StructField("PROV_NM", StringType(), nullable=True),
    StructField("TAX_ID", StringType(), nullable=False),
    StructField("PROV_FCLTY_TYP_CD", StringType(), nullable=False),
    StructField("PROV_SPEC_CD", StringType(), nullable=False),
    StructField("PROV_TYP_CD", StringType(), nullable=False),
    StructField("NTNL_PROV_SK", IntegerType(), nullable=True),
    StructField("TXNMY_CD", StringType(), nullable=False)
])
df_ProvTmp = (
    spark.read
    .format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ProvTmp)
    .load(f"{adls_path}/load/PROV.tmp")
)

# Transformer_95
df_lnkProv = df_ProvTmp.select(
    col("PROV_SK").alias("PROV_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("PROV_BILL_SVC_SK").alias("PROV_BILL_SVC_SK"),
    col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    col("REL_IPA_PROV_SK").alias("REL_IPA_PROV_SK"),
    col("PROV_CAP_PAYMT_EFT_METH_CD_SK").alias("PROV_CAP_PAYMT_EFT_METH_CD_SK"),
    col("PROV_CLM_PAYMT_EFT_METH_CD_SK").alias("PROV_CLM_PAYMT_EFT_METH_CD_SK"),
    col("PROV_CLM_PAYMT_METH_CD_SK").alias("PROV_CLM_PAYMT_METH_CD_SK"),
    col("PROV_ENTY_CD_SK").alias("PROV_ENTY_CD_SK"),
    col("PROV_FCLTY_TYP_CD_SK").alias("PROV_FCLTY_TYP_CD_SK"),
    col("PROV_PRCTC_TYP_CD_SK").alias("PROV_PRCTC_TYP_CD_SK"),
    col("PROV_SVC_CAT_CD_SK").alias("PROV_SVC_CAT_CD_SK"),
    col("PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    col("PROV_STTUS_CD_SK").alias("PROV_STTUS_CD_SK"),
    col("PROV_TERM_RSN_CD_SK").alias("PROV_TERM_RSN_CD_SK"),
    col("PROV_TYP_CD_SK").alias("PROV_TYP_CD_SK"),
    col("TERM_DT_SK").alias("TERM_DT_SK"),
    col("PAYMT_HOLD_DT_SK").alias("PAYMT_HOLD_DT_SK"),
    col("CLRNGHSE_ID").alias("CLRNGHSE_ID"),
    col("EDI_DEST_ID").alias("EDI_DEST_ID"),
    col("EDI_DEST_QLFR_TX").alias("EDI_DEST_QLFR_TX"),
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_NM").alias("PROV_NM"),
    col("TAX_ID").alias("TAX_ID"),
    col("NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    col("TXNMY_CD").alias("TXNMY_CD")
)

df_emailUnkFcltyTyp = df_ProvTmp.filter(
    (col("PROV_FCLTY_TYP_CD_SK") == 0) & (col("PROV_FCLTY_TYP_CD") != "UNK")
).select(
    col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD")
)

df_emailUnkSpecCd = df_ProvTmp.filter(
    (col("PROV_SPEC_CD_SK") == 0) & (col("PROV_SPEC_CD") != "UNK")
).select(
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD")
)

df_emailUnkProvTyp = df_ProvTmp.filter(
    (col("PROV_TYP_CD_SK") == 0) & (col("PROV_TYP_CD") != "UNK")
).select(
    col("PROV_TYP_CD").alias("PROV_TYP_CD")
)

# Prov => CSeqFileStage => write PROV.dat
df_lnkProv_for_write = (
    df_lnkProv
    # char(10) => TERM_DT_SK
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    # char(10) => PAYMT_HOLD_DT_SK
    .withColumn("PAYMT_HOLD_DT_SK", rpad(col("PAYMT_HOLD_DT_SK"), 10, " "))
    # char(15) => EDI_DEST_ID
    .withColumn("EDI_DEST_ID", rpad(col("EDI_DEST_ID"), 15, " "))
    # char(2) => EDI_DEST_QLFR_TX
    .withColumn("EDI_DEST_QLFR_TX", rpad(col("EDI_DEST_QLFR_TX"), 2, " "))
    # char(10) => NTNL_PROV_ID
    .withColumn("NTNL_PROV_ID", rpad(col("NTNL_PROV_ID"), 10, " "))
    # char(12) => PROV_ADDR_ID
    .withColumn("PROV_ADDR_ID", rpad(col("PROV_ADDR_ID"), 12, " "))
    # char(9) => TAX_ID
    .withColumn("TAX_ID", rpad(col("TAX_ID"), 9, " "))
    # char(4) => PROV_FCLTY_TYP_CD
    .withColumn("PROV_FCLTY_TYP_CD", rpad(col("PROV_FCLTY_TYP_CD"), 4, " "))
    # char(4) => PROV_SPEC_CD
    .withColumn("PROV_SPEC_CD", rpad(col("PROV_SPEC_CD"), 4, " "))
    # char(4) => PROV_TYP_CD
    .withColumn("PROV_TYP_CD", rpad(col("PROV_TYP_CD"), 4, " "))
)

write_files(
    df_lnkProv_for_write.select(
        "PROV_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CMN_PRCT_SK",
        "PROV_BILL_SVC_SK",
        "REL_GRP_PROV_SK",
        "REL_IPA_PROV_SK",
        "PROV_CAP_PAYMT_EFT_METH_CD_SK",
        "PROV_CLM_PAYMT_EFT_METH_CD_SK",
        "PROV_CLM_PAYMT_METH_CD_SK",
        "PROV_ENTY_CD_SK",
        "PROV_FCLTY_TYP_CD_SK",
        "PROV_PRCTC_TYP_CD_SK",
        "PROV_SVC_CAT_CD_SK",
        "PROV_SPEC_CD_SK",
        "PROV_STTUS_CD_SK",
        "PROV_TERM_RSN_CD_SK",
        "PROV_TYP_CD_SK",
        "TERM_DT_SK",
        "PAYMT_HOLD_DT_SK",
        "CLRNGHSE_ID",
        "EDI_DEST_ID",
        "EDI_DEST_QLFR_TX",
        "NTNL_PROV_ID",
        "PROV_ADDR_ID",
        "PROV_NM",
        "TAX_ID",
        "PROV_FCLTY_TYP_CD",
        "PROV_SPEC_CD",
        "PROV_TYP_CD",
        "NTNL_PROV_SK",
        "TXNMY_CD"
    ),
    f"{adls_path}/load/PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_unk_fclty_typ => scenario A => remove hashed file, deduplicate on key => "PROV_FCLTY_TYP_CD"
df_unkFcltyTyp_dedup = dedup_sort(df_emailUnkFcltyTyp, ["PROV_FCLTY_TYP_CD"], [])
df_unkFcltyTyp_for_write = (
    df_unkFcltyTyp_dedup
    # char(4)
    .withColumn("PROV_FCLTY_TYP_CD", rpad(col("PROV_FCLTY_TYP_CD"), 4, " "))
)
write_files(
    df_unkFcltyTyp_for_write.select("PROV_FCLTY_TYP_CD"),
    f"{adls_path}/load/BADPROVFCLTYTYP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_unk_spec_cd => scenario A => deduplicate on key => "PROV_SPEC_CD"
df_unkSpecCd_dedup = dedup_sort(df_emailUnkSpecCd, ["PROV_SPEC_CD"], [])
df_unkSpecCd_for_write = (
    df_unkSpecCd_dedup
    .withColumn("PROV_SPEC_CD", rpad(col("PROV_SPEC_CD"), 4, " "))
)
write_files(
    df_unkSpecCd_for_write.select("PROV_SPEC_CD"),
    f"{adls_path}/load/BADPROVSPECCD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_unk_prov_typ => scenario A => deduplicate on key => "PROV_TYP_CD"
df_unkProvTyp_dedup = dedup_sort(df_emailUnkProvTyp, ["PROV_TYP_CD"], [])
df_unkProvTyp_for_write = (
    df_unkProvTyp_dedup
    .withColumn("PROV_TYP_CD", rpad(col("PROV_TYP_CD"), 4, " "))
)
write_files(
    df_unkProvTyp_for_write.select("PROV_TYP_CD"),
    f"{adls_path}/load/BADPROVTYP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)