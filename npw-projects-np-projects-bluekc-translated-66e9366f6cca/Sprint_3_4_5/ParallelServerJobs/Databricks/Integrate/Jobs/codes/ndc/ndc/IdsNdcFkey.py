# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/21/07 12:23:43 Batch  14478_44630 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_4 04/25/07 15:48:06 Batch  14360_56891 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 04/25/07 15:45:35 Batch  14360_56742 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 04/25/07 15:34:30 Batch  14360_56074 INIT bckcett testIDS30 dsadm bls for s andrew
# MAGIC ^1_2 04/11/07 11:23:01 Batch  14346_40989 PROMOTE bckcett testIDS30 u03651 steph for sharon
# MAGIC ^1_2 04/11/07 11:18:19 Batch  14346_40703 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 03/30/07 12:49:49 Batch  14334_46195 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 03/30/07 12:44:38 Batch  14334_45880 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 03/21/07 15:26:34 Batch  14325_55600 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 06/05/06 15:07:57 Batch  14036_54483 PROMOTE bckcetl ids20 dsadm Gina
# MAGIC ^1_3 06/05/06 14:57:55 Batch  14036_53885 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_11 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_6 12/05/05 10:59:01 Batch  13854_39546 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_10 11/30/05 16:50:19 Batch  13849_60624 INIT bckcett devlIDS30 u05779 bj
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsNdcFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the Foreign key.  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  File from IdsNdcPkey
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - contains records that had errors on them for recycle.  Recycle is not used in this program - file is completely updated from Facets each time.
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes for diagnosis category code
# MAGIC                                  
# MAGIC                            Unknown and Not Applicable rows are created - all rows are sent through collector to combine into output file
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  This job would be run whenever there is an update to the NDC table/file.  
# MAGIC 
# MAGIC OUTPUTS:  NDC.#Source#.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  08/2004  -   Originally Programmed
# MAGIC             BJ Luce             06/15/2005 - changed for rx deductibles 
# MAGIC             BJ Luce             10/31/2005 - add new fields
# MAGIC                                                             use FDB for source system for code mapping lookups
# MAGIC                                                             use UWS for source system for code mapping lookup for DSM drug type code
# MAGIC            BJ Luce             04/2006         use environment variables, use Source in output
# MAGIC            Bhoomi              03/29/2007    Added new field to the table called NDC_RTE_TYP_CD_SK by directly mapping to GCRT2, which is brought from RROUTED3_ROUTE_DESC and looking up at NDC ROUTE TYPE domain                                                             from code mapping table in foreign key process.

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value("InFile", "IdsNewNdcExtr.TMP")
Logging = get_widget_value("Logging", "N")
Source = get_widget_value("Source", "")

# Define schema for NdcCrf (Stage: CSeqFileStage)
schema_NdcCrf = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), nullable=False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), nullable=False),
    T.StructField("DISCARD_IN", T.StringType(), nullable=False),
    T.StructField("PASS_THRU_IN", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), nullable=False),
    T.StructField("ERR_CT", T.IntegerType(), nullable=False),
    T.StructField("RECYCLE_CT", T.DecimalType(38,10), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("PRI_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("NDC_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC", T.StringType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("AHFS_TCC_CD", T.StringType(), nullable=False),
    T.StructField("DOSE_FORM", T.StringType(), nullable=False),
    T.StructField("TCC_CD", T.StringType(), nullable=False),
    T.StructField("DSM_DRUG_TYP_CD", T.StringType(), nullable=False),
    T.StructField("NDC_DRUG_ABUSE_CTL_CD", T.StringType(), nullable=False),
    T.StructField("NDC_DRUG_CLS_CD", T.StringType(), nullable=False),
    T.StructField("NDC_DRUG_FORM_CD", T.StringType(), nullable=False),
    T.StructField("NDC_FMT_CD", T.StringType(), nullable=False),
    T.StructField("NDC_GNRC_MNFCTR_CD", T.StringType(), nullable=False),
    T.StructField("NDC_GNRC_NMD_DRUG_CD", T.StringType(), nullable=False),
    T.StructField("NDC_GNRC_PRICE_CD", T.StringType(), nullable=False),
    T.StructField("NDC_GNRC_PRICE_SPREAD_CD", T.StringType(), nullable=False),
    T.StructField("NDC_ORANGE_BOOK_CD", T.StringType(), nullable=False),
    T.StructField("CLM_TRANS_ADD_IN", T.StringType(), nullable=False),
    T.StructField("DESI_DRUG_IN", T.StringType(), nullable=False),
    T.StructField("DRUG_MNTN_IN", T.StringType(), nullable=False),
    T.StructField("INNVTR_IN", T.StringType(), nullable=False),
    T.StructField("INSTUT_PROD_IN", T.StringType(), nullable=False),
    T.StructField("PRIV_LBLR_IN", T.StringType(), nullable=False),
    T.StructField("SNGL_SRC_IN", T.StringType(), nullable=False),
    T.StructField("UNIT_DOSE_IN", T.StringType(), nullable=False),
    T.StructField("UNIT_OF_USE_IN", T.StringType(), nullable=False),
    T.StructField("AVG_WHLSL_PRICE_CHG_DT_SK", T.StringType(), nullable=False),
    T.StructField("GNRC_PRICE_IN_CHG_DT_SK", T.StringType(), nullable=False),
    T.StructField("OBSLT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_NDC_CRT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_NDC_UPDT_DT_SK", T.StringType(), nullable=False),
    T.StructField("BRND_NM", T.StringType(), nullable=True),
    T.StructField("CORE_NINE_NO", T.StringType(), nullable=True),
    T.StructField("DRUG_LABEL_NM", T.StringType(), nullable=True),
    T.StructField("DRUG_STRG_DESC", T.StringType(), nullable=True),
    T.StructField("GCN_CD_TX", T.StringType(), nullable=True),
    T.StructField("GNRC_NM_SH_DESC", T.StringType(), nullable=True),
    T.StructField("LBLR_NM", T.StringType(), nullable=True),
    T.StructField("LBLR_NO", T.StringType(), nullable=True),
    T.StructField("NEEDLE_GAUGE_VAL", T.StringType(), nullable=True),
    T.StructField("NEEDLE_LGTH_VAL", T.StringType(), nullable=True),
    T.StructField("PCKG_DESC", T.StringType(), nullable=True),
    T.StructField("PCKG_SIZE_EQVLNT_NO", T.StringType(), nullable=True),
    T.StructField("PCKG_SIZE_NO", T.StringType(), nullable=True),
    T.StructField("PROD_NO", T.StringType(), nullable=True),
    T.StructField("SYRNG_CPCT_VAL", T.StringType(), nullable=True),
    T.StructField("NDC_RTE_TYP_CD", T.StringType(), nullable=False),
])

# Read input file for stage NdcCrf
df_NdcCrf = (
    spark.read
    .option("delimiter", ",")
    .option("quote", '"')
    .option("header", "false")
    .schema(schema_NdcCrf)
    .csv(f"{adls_path}/key/{InFile}")
)

# Apply Transformer logic for stage PurgeTrn (with stage variables)
df_NdcCrfTrn = (
    df_NdcCrf
    .withColumn("NdcAHFSClass", GetFkeyAhfsTcc(F.col("NDC_SK"), F.col("AHFS_TCC_CD"), Logging))
    .withColumn("NdcTherCls", GetFkeyTCC(F.col("NDC_SK"), F.col("TCC_CD"), Logging))
    .withColumn("NdcDoseForm", GetFkeyDoseForm(F.col("NDC_SK"), F.col("DOSE_FORM"), Logging))
    .withColumn("DSMDrugType", GetFkeyCodes(F.lit("UWS"), F.col("NDC_SK"), F.lit("NDC DISEASE STATE MANAGEMENT DRUG TYPE"), F.col("DSM_DRUG_TYP_CD"), Logging))
    .withColumn("DrugAbuseCtlCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC DRUG ABUSE CONTROL"), F.col("NDC_DRUG_ABUSE_CTL_CD"), Logging))
    .withColumn("DrugClsCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC DRUG CLASS"), F.col("NDC_DRUG_CLS_CD"), Logging))
    .withColumn("DrugFormCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC DRUG FORM"), F.col("NDC_DRUG_FORM_CD"), Logging))
    .withColumn("FmtCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC FORMAT"), F.col("NDC_FMT_CD"), Logging))
    .withColumn("GnrcMnfctrCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC GENERIC MANUFACTURER"), F.col("NDC_GNRC_MNFCTR_CD"), Logging))
    .withColumn("GnrcNmdDrugCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC GENERIC NAME"), F.col("NDC_GNRC_NMD_DRUG_CD"), Logging))
    .withColumn("GnrcPriceCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC GENERIC PRICE"), F.col("NDC_GNRC_PRICE_CD"), Logging))
    .withColumn("GnrcPriceSpreadCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC GENERIC PRICE SPREAD"), F.col("NDC_GNRC_PRICE_SPREAD_CD"), Logging))
    .withColumn("OrangeBookCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC ORANGE BOOK"), F.col("NDC_ORANGE_BOOK_CD"), Logging))
    .withColumn("NdcRteTypCd", GetFkeyCodes(F.lit("FDB"), F.col("NDC_SK"), F.lit("NDC ROUTE TYPE"), F.col("NDC_RTE_TYP_CD"), Logging))
    .withColumn("AvgWhlslPriceChgDt", GetFkeyDate(F.lit("IDS"), F.col("NDC_SK"), F.col("AVG_WHLSL_PRICE_CHG_DT_SK"), Logging))
    .withColumn("GnrcPricInChgDt", GetFkeyDate(F.lit("IDS"), F.col("NDC_SK"), F.col("GNRC_PRICE_IN_CHG_DT_SK"), Logging))
    .withColumn("ObsltDt", GetFkeyDate(F.lit("IDS"), F.col("NDC_SK"), F.col("OBSLT_DT_SK"), Logging))
    .withColumn("SrcNdcCrtDt", GetFkeyDate(F.lit("IDS"), F.col("NDC_SK"), F.col("SRC_NDC_CRT_DT_SK"), Logging))
    .withColumn("SrcNdcUpdtDt", GetFkeyDate(F.lit("IDS"), F.col("NDC_SK"), F.col("SRC_NDC_UPDT_DT_SK"), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("NDC_SK")))
)

# Create DataFrame for link GatherNDCData (constraint: ErrCount=0 OR PassThru='Y')
df_GatherNDCData_pre = df_NdcCrfTrn.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
df_GatherNDCData = df_GatherNDCData_pre.select(
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("NDC").alias("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NdcAHFSClass").alias("AHFS_TCC_SK"),
    F.col("NdcDoseForm").alias("DOSE_FORM_SK"),
    F.col("NdcTherCls").alias("TCC_SK"),
    F.col("DSMDrugType").alias("NDC_DSM_DRUG_TYP_CD_SK"),
    F.col("DrugAbuseCtlCd").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("DrugClsCd").alias("NDC_DRUG_CLS_CD_SK"),
    F.col("DrugFormCd").alias("NDC_DRUG_FORM_CD_SK"),
    F.col("FmtCd").alias("NDC_FMT_CD_SK"),
    F.col("GnrcMnfctrCd").alias("NDC_GNRC_MNFCTR_CD_SK"),
    F.col("GnrcNmdDrugCd").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("GnrcPriceCd").alias("NDC_GNRC_PRICE_CD_SK"),
    F.col("GnrcPriceSpreadCd").alias("NDC_GNRC_PRICE_SPREAD_CD_SK"),
    F.col("OrangeBookCd").alias("NDC_ORANGE_BOOK_CD_SK"),
    F.col("NdcRteTypCd").alias("NDC_RTE_TYP_CD_SK"),
    # The following are char(1) or char(10), apply rpad accordingly
    F.rpad(F.col("CLM_TRANS_ADD_IN"), 1, " ").alias("CLM_TRANS_ADD_IN"),
    F.rpad(F.col("DESI_DRUG_IN"), 1, " ").alias("DESI_DRUG_IN"),
    F.rpad(F.col("DRUG_MNTN_IN"), 1, " ").alias("DRUG_MNTN_IN"),
    F.rpad(F.col("INNVTR_IN"), 1, " ").alias("INNVTR_IN"),
    F.rpad(F.col("INSTUT_PROD_IN"), 1, " ").alias("INSTUT_PROD_IN"),
    F.rpad(F.col("PRIV_LBLR_IN"), 1, " ").alias("PRIV_LBLR_IN"),
    F.rpad(F.col("SNGL_SRC_IN"), 1, " ").alias("SNGL_SRC_IN"),
    F.rpad(F.col("UNIT_DOSE_IN"), 1, " ").alias("UNIT_DOSE_IN"),
    F.rpad(F.col("UNIT_OF_USE_IN"), 1, " ").alias("UNIT_OF_USE_IN"),
    F.rpad(F.col("AvgWhlslPriceChgDt"), 10, " ").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.rpad(F.col("GnrcPricInChgDt"), 10, " ").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    F.rpad(F.col("ObsltDt"), 10, " ").alias("OBSLT_DT_SK"),
    F.rpad(F.col("SrcNdcCrtDt"), 10, " ").alias("SRC_NDC_CRT_DT_SK"),
    F.rpad(F.col("SrcNdcUpdtDt"), 10, " ").alias("SRC_NDC_UPDT_DT_SK"),
    F.col("BRND_NM").alias("BRND_NM"),
    F.col("CORE_NINE_NO").alias("CORE_NINE_NO"),
    F.col("DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("DRUG_STRG_DESC").alias("DRUG_STRG_DESC"),
    F.col("GCN_CD_TX").alias("GCN_CD_TX"),
    F.col("GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("LBLR_NM").alias("LBLR_NM"),
    F.col("LBLR_NO").alias("LBLR_NO"),
    F.col("NEEDLE_GAUGE_VAL").alias("NEEDLE_GAUGE_VAL"),
    F.col("NEEDLE_LGTH_VAL").alias("NEEDLE_LGTH_VAL"),
    F.col("PCKG_DESC").alias("PCKG_DESC"),
    F.col("PCKG_SIZE_EQVLNT_NO").alias("PCKG_SIZE_EQVLNT_NO"),
    F.col("PCKG_SIZE_NO").alias("PCKG_SIZE_NO"),
    F.col("PROD_NO").alias("PROD_NO"),
    F.col("SYRNG_CPCT_VAL").alias("SYRNG_CPCT_VAL")
)

# Create DataFrame for link lnkRecycle (constraint: ErrCount > 0)
df_lnkRecycle_pre = df_NdcCrfTrn.filter(F.col("ErrCount") > 0)
df_lnkRecycle = df_lnkRecycle_pre.select(
    GetRecycleKey(F.col("NDC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("NDC").alias("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("AHFS_TCC_CD"),10," ").alias("AHFS_TCC_CD"),
    F.rpad(F.col("DOSE_FORM"),2," ").alias("DOSE_FORM"),
    F.rpad(F.col("TCC_CD"),10," ").alias("TCC_CD"),
    F.rpad(F.col("DSM_DRUG_TYP_CD"),10," ").alias("DSM_DRUG_TYP_CD"),
    F.rpad(F.col("NDC_DRUG_ABUSE_CTL_CD"),1," ").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.rpad(F.col("NDC_DRUG_CLS_CD"),1," ").alias("NDC_DRUG_CLS_CD"),
    F.rpad(F.col("NDC_DRUG_FORM_CD"),1," ").alias("NDC_DRUG_FORM_CD"),
    F.rpad(F.col("NDC_FMT_CD"),1," ").alias("NDC_FMT_CD"),
    F.rpad(F.col("NDC_GNRC_MNFCTR_CD"),1," ").alias("NDC_GNRC_MNFCTR_CD"),
    F.rpad(F.col("NDC_GNRC_NMD_DRUG_CD"),1," ").alias("NDC_GNRC_NMD_DRUG_CD"),
    F.rpad(F.col("NDC_GNRC_PRICE_CD"),1," ").alias("NDC_GNRC_PRICE_CD"),
    F.rpad(F.col("NDC_GNRC_PRICE_SPREAD_CD"),1," ").alias("NDC_GNRC_PRICE_SPREAD_CD"),
    F.rpad(F.col("NDC_ORANGE_BOOK_CD"),1," ").alias("NDC_ORANGE_BOOK_CD"),
    F.rpad(F.col("NDC_RTE_TYP_CD"),1," ").alias("NDC_RTE_TYP_CD"),
    F.rpad(F.col("CLM_TRANS_ADD_IN"),1," ").alias("CLM_TRANS_ADD_IN"),
    F.rpad(F.col("DESI_DRUG_IN"),1," ").alias("DESI_DRUG_IN"),
    F.rpad(F.col("DRUG_MNTN_IN"),1," ").alias("DRUG_MNTN_IN"),
    F.rpad(F.col("INNVTR_IN"),1," ").alias("INNVTR_IN"),
    F.rpad(F.col("INSTUT_PROD_IN"),1," ").alias("INSTUT_PROD_IN"),
    F.rpad(F.col("PRIV_LBLR_IN"),1," ").alias("PRIV_LBLR_IN"),
    F.rpad(F.col("SNGL_SRC_IN"),1," ").alias("SNGL_SRC_IN"),
    F.rpad(F.col("UNIT_DOSE_IN"),1," ").alias("UNIT_DOSE_IN"),
    F.rpad(F.col("UNIT_OF_USE_IN"),1," ").alias("UNIT_OF_USE_IN"),
    F.rpad(F.col("AVG_WHLSL_PRICE_CHG_DT_SK"),10," ").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.rpad(F.col("GNRC_PRICE_IN_CHG_DT_SK"),10," ").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    F.rpad(F.col("OBSLT_DT_SK"),10," ").alias("OBSLT_DT_SK"),
    F.rpad(F.col("SRC_NDC_CRT_DT_SK"),10," ").alias("SRC_NDC_CRT_DT_SK"),
    F.rpad(F.col("SRC_NDC_UPDT_DT_SK"),10," ").alias("SRC_NDC_UPDT_DT_SK"),
    F.col("BRND_NM").alias("BRND_NM"),
    F.col("CORE_NINE_NO").alias("CORE_NINE_NO"),
    F.col("DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("DRUG_STRG_DESC").alias("DRUG_STRG_DESC"),
    F.col("GCN_CD_TX").alias("GCN_CD_TX"),
    F.col("GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("LBLR_NM").alias("LBLR_NM"),
    F.col("LBLR_NO").alias("LBLR_NO"),
    F.col("NEEDLE_GAUGE_VAL").alias("NEEDLE_GAUGE_VAL"),
    F.col("NEEDLE_LGTH_VAL").alias("NEEDLE_LGTH_VAL"),
    F.col("PCKG_DESC").alias("PCKG_DESC"),
    F.col("PCKG_SIZE_EQVLNT_NO").alias("PCKG_SIZE_EQVLNT_NO"),
    F.col("PCKG_SIZE_NO").alias("PCKG_SIZE_NO"),
    F.col("PROD_NO").alias("PROD_NO"),
    F.col("SYRNG_CPCT_VAL").alias("SYRNG_CPCT_VAL")
)

# Write the hashed file RecycleNdc (Scenario C => to parquet)
write_files(
    df_lnkRecycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# DefaultUNK (constraint: @INROWNUM=1, a single row with specified constants)
schema_DefaultUNK = T.StructType([
    T.StructField("NDC_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC", T.StringType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("AHFS_TCC_SK", T.IntegerType(), nullable=False),
    T.StructField("DOSE_FORM_SK", T.IntegerType(), nullable=False),
    T.StructField("TCC_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DSM_DRUG_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DRUG_ABUSE_CTL_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DRUG_CLS_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DRUG_FORM_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_FMT_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_MNFCTR_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_NMD_DRUG_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_PRICE_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_PRICE_SPREAD_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_ORANGE_BOOK_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_RTE_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("CLM_TRANS_ADD_IN", T.StringType(), nullable=False),
    T.StructField("DESI_DRUG_IN", T.StringType(), nullable=False),
    T.StructField("DRUG_MNTN_IN", T.StringType(), nullable=False),
    T.StructField("INNVTR_IN", T.StringType(), nullable=False),
    T.StructField("INSTUT_PROD_IN", T.StringType(), nullable=False),
    T.StructField("PRIV_LBLR_IN", T.StringType(), nullable=False),
    T.StructField("SNGL_SRC_IN", T.StringType(), nullable=False),
    T.StructField("UNIT_DOSE_IN", T.StringType(), nullable=False),
    T.StructField("UNIT_OF_USE_IN", T.StringType(), nullable=False),
    T.StructField("AVG_WHLSL_PRICE_CHG_DT_SK", T.StringType(), nullable=False),
    T.StructField("GNRC_PRICE_IN_CHG_DT_SK", T.StringType(), nullable=False),
    T.StructField("OBSLT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_NDC_CRT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_NDC_UPDT_DT_SK", T.StringType(), nullable=False),
    T.StructField("BRND_NM", T.StringType(), nullable=False),
    T.StructField("CORE_NINE_NO", T.StringType(), nullable=False),
    T.StructField("DRUG_LABEL_NM", T.StringType(), nullable=False),
    T.StructField("DRUG_STRG_DESC", T.StringType(), nullable=False),
    T.StructField("GCN_CD_TX", T.StringType(), nullable=False),
    T.StructField("GNRC_NM_SH_DESC", T.StringType(), nullable=False),
    T.StructField("LBLR_NM", T.StringType(), nullable=False),
    T.StructField("LBLR_NO", T.StringType(), nullable=False),
    T.StructField("NEEDLE_GAUGE_VAL", T.StringType(), nullable=False),
    T.StructField("NEEDLE_LGTH_VAL", T.StringType(), nullable=False),
    T.StructField("PCKG_DESC", T.StringType(), nullable=False),
    T.StructField("PCKG_SIZE_EQVLNT_NO", T.StringType(), nullable=False),
    T.StructField("PCKG_SIZE_NO", T.StringType(), nullable=False),
    T.StructField("PROD_NO", T.StringType(), nullable=False),
    T.StructField("SYRNG_CPCT_VAL", T.StringType(), nullable=False),
])
data_DefaultUNK = [(0,"UNK",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"U","U","U","U","U","U","U","U","U","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK")]
df_DefaultUNK = spark.createDataFrame(data_DefaultUNK, schema_DefaultUNK)

# DefaultNA (constraint: @INROWNUM=1, a single row with specified constants)
schema_DefaultNA = T.StructType([
    T.StructField("NDC_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC", T.StringType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("AHFS_TCC_SK", T.IntegerType(), nullable=False),
    T.StructField("DOSE_FORM_SK", T.IntegerType(), nullable=False),
    T.StructField("TCC_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DSM_DRUG_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DRUG_ABUSE_CTL_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DRUG_CLS_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_DRUG_FORM_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_FMT_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_MNFCTR_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_NMD_DRUG_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_PRICE_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_GNRC_PRICE_SPREAD_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_ORANGE_BOOK_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("NDC_RTE_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("CLM_TRANS_ADD_IN", T.StringType(), nullable=False),
    T.StructField("DESI_DRUG_IN", T.StringType(), nullable=False),
    T.StructField("DRUG_MNTN_IN", T.StringType(), nullable=False),
    T.StructField("INNVTR_IN", T.StringType(), nullable=False),
    T.StructField("INSTUT_PROD_IN", T.StringType(), nullable=False),
    T.StructField("PRIV_LBLR_IN", T.StringType(), nullable=False),
    T.StructField("SNGL_SRC_IN", T.StringType(), nullable=False),
    T.StructField("UNIT_DOSE_IN", T.StringType(), nullable=False),
    T.StructField("UNIT_OF_USE_IN", T.StringType(), nullable=False),
    T.StructField("AVG_WHLSL_PRICE_CHG_DT_SK", T.StringType(), nullable=False),
    T.StructField("GNRC_PRICE_IN_CHG_DT_SK", T.StringType(), nullable=False),
    T.StructField("OBSLT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_NDC_CRT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_NDC_UPDT_DT_SK", T.StringType(), nullable=False),
    T.StructField("BRND_NM", T.StringType(), nullable=False),
    T.StructField("CORE_NINE_NO", T.StringType(), nullable=False),
    T.StructField("DRUG_LABEL_NM", T.StringType(), nullable=False),
    T.StructField("DRUG_STRG_DESC", T.StringType(), nullable=False),
    T.StructField("GCN_CD_TX", T.StringType(), nullable=False),
    T.StructField("GNRC_NM_SH_DESC", T.StringType(), nullable=False),
    T.StructField("LBLR_NM", T.StringType(), nullable=False),
    T.StructField("LBLR_NO", T.StringType(), nullable=False),
    T.StructField("NEEDLE_GAUGE_VAL", T.StringType(), nullable=False),
    T.StructField("NEEDLE_LGTH_VAL", T.StringType(), nullable=False),
    T.StructField("PCKG_DESC", T.StringType(), nullable=False),
    T.StructField("PCKG_SIZE_EQVLNT_NO", T.StringType(), nullable=False),
    T.StructField("PCKG_SIZE_NO", T.StringType(), nullable=False),
    T.StructField("PROD_NO", T.StringType(), nullable=False),
    T.StructField("SYRNG_CPCT_VAL", T.StringType(), nullable=False),
])
data_DefaultNA = [(1,"NA",1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,"X",