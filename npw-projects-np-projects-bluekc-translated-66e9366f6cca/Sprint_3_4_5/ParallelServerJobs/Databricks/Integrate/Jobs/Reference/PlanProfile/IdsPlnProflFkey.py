# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsPlnProflFkey
# MAGIC 
# MAGIC DESCRIPTION:    Assigns foreign keys to plan profilet table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	        File from FctsPlnProflExtr with primary key assigned
# MAGIC 
# MAGIC HASH FILES:  hf_recycle
# MAGIC                        
# MAGIC                         
# MAGIC 
# MAGIC TRANSFORMS:  foreign key lookups
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC 
# MAGIC  Developer                        Date                  Project/Altiris #                  Change Description                                           Development Project               Code Reviewer                  Date Reviewed
# MAGIC  ----------------------------------      -------------------      -----------------------------------        ---------------------------------------------------------                 ----------------------------------              ---------------------------------       --------------------
# MAGIC  Terri O'Bryan                   09/02/2010       TTR816                              Original Program                                                IntegrateNewDevl                   Brent Leland                    10-05-2011

# MAGIC Capture records generating translation errors to be uploaded to the IDS recycle table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
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
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
Logging = get_widget_value("Logging","")
InFile = get_widget_value("InFile","")

# ------------------------------------------------------------------------------
# 1) Read from CSeqFileStage: IdsPlnProflExtr
# ------------------------------------------------------------------------------
schema_IdsPlnProflExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", DecimalType(38,10), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("PLN_PROFL_SK", IntegerType(), True),
    StructField("ALPHA_PFX_CD", StringType(), True),
    StructField("ALPHA_PFX_LOCAL_PLN_CD", StringType(), True),
    StructField("CLM_SUBTYP_CD", StringType(), True),
    StructField("EFF_DT_SK", StringType(), True),
    StructField("RCPT_INCUR_STRT_DT_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ACCT_PLN_NM", StringType(), True),
    StructField("ADJ_EDIT_IN", StringType(), True),
    StructField("ADM_EXP_ALLWNC_NONSTD_AMT", DecimalType(38,10), True),
    StructField("CTL_PLN_ACKNMT_RCPT_DT_SK", StringType(), True),
    StructField("CTL_PLN_RULE_CRT_DT_SK", StringType(), True),
    StructField("CTL_PLN_RULE_DSTRB_DT_SK", StringType(), True),
    StructField("CTL_PLN_RULE_VER_DT_SK", StringType(), True),
    StructField("CTL_PLN_STATN_ID", StringType(), True),
    StructField("DF_LOCAL_PLN_STATN_ID", StringType(), True),
    StructField("DF_PRCS_SITE_STATN_ID", StringType(), True),
    StructField("END_DT_SK", StringType(), True),
    StructField("HOME_PLN_PROV_CNTCT_ALW_IN", StringType(), True),
    StructField("ITS_SFWR_VRSN_NO", IntegerType(), True),
    StructField("LOCAL_PLN_RULE_DSTRB_DT_SK", StringType(), True),
    StructField("LOCAL_PLN_RULE_RCPT_DT_SK", StringType(), True),
    StructField("LOCAL_PLN_RULE_VER_DT_SK", StringType(), True),
    StructField("LOCAL_PLN_STATN_ID", StringType(), True),
    StructField("MULT_PRCS_ID", StringType(), True),
    StructField("NEW_END_DT_SK", StringType(), True),
    StructField("ORIG_PARTCPN_DT_SK", StringType(), True),
    StructField("PLN_PROFL_ACES_FEE_CD", StringType(), True),
    StructField("PLN_PROFL_ACCT_ENR_LVL_CD", StringType(), True),
    StructField("PLN_PROFL_ADM_EXP_ALLWNC_CD", StringType(), True),
    StructField("PLN_PROFL_BNF_DLVRY_METH_CD", StringType(), True),
    StructField("PLN_PROFL_CFA_EDIT_CD", StringType(), True),
    StructField("PLN_PROFL_CTL_PLN_CD", StringType(), True),
    StructField("PLN_PROFL_DF_INFO_TYP_CD", StringType(), True),
    StructField("PLN_PROFL_DF_TRNSMS_MODE_CD", StringType(), True),
    StructField("PLN_PROFL_EDIT_STTUS_CD", StringType(), True),
    StructField("PLN_PROFL_EDITSTTUS_QLFR_CD", StringType(), True),
    StructField("PLN_PROFL_EOB_GNRTN_CD", StringType(), True),
    StructField("PLN_PROFL_MC_INFO_CD", StringType(), True),
    StructField("PLN_PROFL_NTNL_OOA_CD", StringType(), True),
    StructField("PLN_PROFL_PAYER_CD", StringType(), True),
    StructField("PLN_PROFL_PAYER_RSTRCT1_CD", StringType(), True),
    StructField("PLN_PROFL_PAYER_RSTRCT2_CD", StringType(), True),
    StructField("PLN_PROFL_PAYER_RSTRCT3_CD", StringType(), True),
    StructField("PLN_PROFL_PAYER_RSTRCT4_CD", StringType(), True),
    StructField("PLN_PROFL_PAYER_RSTRCT5_CD", StringType(), True),
    StructField("PLN_PROFL_PRCS_ARGMT_CD", StringType(), True),
    StructField("PLN_PROFL_PRCS_PLN_CD", StringType(), True),
    StructField("PLN_PROFL_PROD_CAT_CD", StringType(), True),
    StructField("PLN_PROFL_RCPT_INCUR_DT_TYP_CD", StringType(), True),
    StructField("PLN_PROFL_SF_TRNSMS_MODE_CD", StringType(), True),
    StructField("PLN_PROFL_STAT_RCRD_CD", StringType(), True),
    StructField("PLN_PROFL_TRNSMSN_MODE_CD", StringType(), True),
    StructField("PLN_PROFL_1099_GNRTN_CD", StringType(), True),
    StructField("PRICE_DATA_APPEND_IN", StringType(), True),
    StructField("PROV_DATA_APPEND_IN", StringType(), True),
    StructField("RCPT_INCUR_END_DT_SK", StringType(), True),
    StructField("RESUBMT_DF_IN", StringType(), True),
    StructField("SF_LOCAL_PLN_STATN_ID", StringType(), True),
    StructField("SF_PRCS_SITE_STATN_ID", StringType(), True),
    StructField("SRC_SYS_CLM_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_PRCS_DTM", TimestampType(), True),
    StructField("SRC_SYS_LAST_RULE_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_USER_ID", StringType(), True),
    StructField("SUBMSN_EDIT_IN", StringType(), True),
    StructField("SUBMSN_PRCS_IN", StringType(), True),
    StructField("UNFRM_PRICE_FCLTY_EDIT_IN", StringType(), True),
    StructField("CLM_SUBTYP_SRC_CD", StringType(), True),
    StructField("ALPHA_PFX_LOCAL_PLN_SRC_CD", StringType(), True)
])

df_IdsPlnProflExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsPlnProflExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# ------------------------------------------------------------------------------
# 2) Transform Stage: ForeignKey (CTransformerStage)
#    Define stage variables and produce multiple outputs
# ------------------------------------------------------------------------------
df_ForeignKey_Enriched = (
    df_IdsPlnProflExtr
    .withColumn("svSrcSysCd", GetFkeyCodes(F.lit("IDS"), F.col("PLN_PROFL_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("svAlphaPfxSk", GetFkeyAlphaPfx(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.col("ALPHA_PFX_CD"), F.lit(Logging)))
    .withColumn("svAlphaPfxLocPlnCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("ACTIVATING BCBS PLAN"), F.col("ALPHA_PFX_LOCAL_PLN_SRC_CD"), F.lit(Logging)))
    .withColumn("svClmSubtypcdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("CLAIM SUBTYPE"), F.col("CLM_SUBTYP_SRC_CD"), F.lit(Logging)))
    .withColumn("svAccsFee", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("ACCESS FEE"), F.col("PLN_PROFL_ACES_FEE_CD"), F.lit(Logging)))
    .withColumn("svAcctTyp", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("ACCOUNT TYPE"), F.col("PLN_PROFL_ACCT_ENR_LVL_CD"), F.lit(Logging)))
    .withColumn("svAdmExpAlw", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("ADMIN EXPENSE ALLOWANCE"), F.col("PLN_PROFL_ADM_EXP_ALLWNC_CD"), F.lit(Logging)))
    .withColumn("svDlvryMeth", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("DELIVERY METHOD"), F.col("PLN_PROFL_BNF_DLVRY_METH_CD"), F.lit(Logging)))
    .withColumn("svCFA", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PROFILE CFA"), F.col("PLN_PROFL_CFA_EDIT_CD"), F.lit(Logging)))
    .withColumn("svCtrlPln", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("ACTIVATING BCBS PLAN"), F.col("PLN_PROFL_CTL_PLN_CD"), F.lit(Logging)))
    .withColumn("svScdfTyp", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("SCDF TYPE"), F.col("PLN_PROFL_DF_INFO_TYP_CD"), F.lit(Logging)))
    .withColumn("svXmtMod", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("TRANSMIT MODE"), F.col("PLN_PROFL_DF_TRNSMS_MODE_CD"), F.lit(Logging)))
    .withColumn("svSttusCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PROFILE STATUS"), F.col("PLN_PROFL_EDIT_STTUS_CD"), F.lit(Logging)))
    .withColumn("svSttusQual", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("STATUS QUALIFIER"), F.col("PLN_PROFL_EDITSTTUS_QLFR_CD"), F.lit(Logging)))
    .withColumn("svEOBGen", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("EOB GENERATION"), F.col("PLN_PROFL_EOB_GNRTN_CD"), F.lit(Logging)))
    .withColumn("svMngdCare", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("MANAGED CARE"), F.col("PLN_PROFL_MC_INFO_CD"), F.lit(Logging)))
    .withColumn("svNtnlOOA", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("NATIONAL OUT OF AREA"), F.col("PLN_PROFL_NTNL_OOA_CD"), F.lit(Logging)))
    .withColumn("svPlnPyr", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PLAN PAYER"), F.col("PLN_PROFL_PAYER_CD"), F.lit(Logging)))
    .withColumn("svPlnPyrQ1", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PLAN PAYER QUALIFIER"), F.col("PLN_PROFL_PAYER_RSTRCT1_CD"), F.lit(Logging)))
    .withColumn("svPlnPyrQ2", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PLAN PAYER QUALIFIER"), F.col("PLN_PROFL_PAYER_RSTRCT2_CD"), F.lit(Logging)))
    .withColumn("svPlnPyrQ3", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PLAN PAYER QUALIFIER"), F.col("PLN_PROFL_PAYER_RSTRCT3_CD"), F.lit(Logging)))
    .withColumn("svPlnPyrQ4", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PLAN PAYER QUALIFIER"), F.col("PLN_PROFL_PAYER_RSTRCT4_CD"), F.lit(Logging)))
    .withColumn("svPlnPyrQ5", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PLAN PAYER QUALIFIER"), F.col("PLN_PROFL_PAYER_RSTRCT5_CD"), F.lit(Logging)))
    .withColumn("svPgm", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PROFILE PROGRAM"), F.col("PLN_PROFL_PRCS_ARGMT_CD"), F.lit(Logging)))
    .withColumn("svSccfPrcs", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("ACTIVATING BCBS PLAN"), F.col("PLN_PROFL_PRCS_PLN_CD"), F.lit(Logging)))
    .withColumn("svProdTyp", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PRODUCT TYPE"), F.col("PLN_PROFL_PROD_CAT_CD"), F.lit(Logging)))
    .withColumn("svRi", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("RECEIPT INCURRED"), F.col("PLN_PROFL_RCPT_INCUR_DT_TYP_CD"), F.lit(Logging)))
    .withColumn("svXmtmod", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("TRANSMIT MODE"), F.col("PLN_PROFL_SF_TRNSMS_MODE_CD"), F.lit(Logging)))
    .withColumn("svStat", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("PROFILE STATISTICS"), F.col("PLN_PROFL_STAT_RCRD_CD"), F.lit(Logging)))
    .withColumn("svx1099Gen", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("X1099 GENERATION"), F.col("PLN_PROFL_1099_GNRTN_CD"), F.lit(Logging)))
    .withColumn("svTrnsmsnModeCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PLN_PROFL_SK"), F.lit("TRANSMIT MODE"), F.col("PLN_PROFL_TRNSMSN_MODE_CD"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PLN_PROFL_SK")))
)

# ------------------------------------------------------------------------------
# 2a) Output Link Fkey: Constraint = (ErrCount = 0 OR PassThru = 'Y')
# ------------------------------------------------------------------------------
df_ForeignKey_Fkey_temp = df_ForeignKey_Enriched.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
)

df_ForeignKey_Fkey = df_ForeignKey_Fkey_temp.select(
    F.col("PLN_PROFL_SK").alias("PLN_PROFL_SK"),
    F.col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("ALPHA_PFX_LOCAL_PLN_CD").alias("ALPHA_PFX_LOCAL_PLN_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("RCPT_INCUR_STRT_DT_SK").alias("RCPT_INCUR_STRT_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svAlphaPfxSk").alias("ALPHA_PFX_SK"),
    F.col("ACCT_PLN_NM").alias("ACCT_PLN_NM"),
    F.col("ADJ_EDIT_IN").alias("ADJ_EDIT_IN"),
    F.col("ADM_EXP_ALLWNC_NONSTD_AMT").alias("ADM_EXP_ALLWNC_NONSTD_AMT"),
    F.col("svAlphaPfxLocPlnCdSk").alias("ALPHA_PFX_LOCAL_PLN_CD_SK"),
    F.col("svClmSubtypcdSk").alias("CLM_SUBTYP_CD_SK"),
    F.col("CTL_PLN_ACKNMT_RCPT_DT_SK").alias("CTL_PLN_ACKNMT_RCPT_DT_SK"),
    F.col("CTL_PLN_RULE_CRT_DT_SK").alias("CTL_PLN_RULE_CRT_DT_SK"),
    F.col("CTL_PLN_RULE_DSTRB_DT_SK").alias("CTL_PLN_RULE_DSTRB_DT_SK"),
    F.col("CTL_PLN_RULE_VER_DT_SK").alias("CTL_PLN_RULE_VER_DT_SK"),
    F.col("CTL_PLN_STATN_ID").alias("CTL_PLN_STATN_ID"),
    F.col("DF_LOCAL_PLN_STATN_ID").alias("DF_LOCAL_PLN_STATN_ID"),
    F.col("DF_PRCS_SITE_STATN_ID").alias("DF_PRCS_SITE_STATN_ID"),
    F.col("END_DT_SK").alias("END_DT_SK"),
    F.col("HOME_PLN_PROV_CNTCT_ALW_IN").alias("HOME_PLN_PROV_CNTCT_ALW_IN"),
    F.col("ITS_SFWR_VRSN_NO").alias("ITS_SFWR_VRSN_NO"),
    F.col("LOCAL_PLN_RULE_DSTRB_DT_SK").alias("LOCAL_PLN_RULE_DSTRB_DT_SK"),
    F.col("LOCAL_PLN_RULE_RCPT_DT_SK").alias("LOCAL_PLN_RULE_RCPT_DT_SK"),
    F.col("LOCAL_PLN_RULE_VER_DT_SK").alias("LOCAL_PLN_RULE_VER_DT_SK"),
    F.col("LOCAL_PLN_STATN_ID").alias("LOCAL_PLN_STATN_ID"),
    F.col("MULT_PRCS_ID").alias("MULT_PRCS_ID"),
    F.col("NEW_END_DT_SK").alias("NEW_END_DT_SK"),
    F.col("ORIG_PARTCPN_DT_SK").alias("ORIG_PARTCPN_DT_SK"),
    F.col("svAccsFee").alias("PLN_PROFL_ACES_FEE_CD_SK"),
    F.col("svAcctTyp").alias("PLN_PROFL_ACCT_ENR_LVL_CD_SK"),
    F.col("svAdmExpAlw").alias("PLN_PROFL_ADM_EXP_ALLWNC_CD_SK"),
    F.col("svDlvryMeth").alias("PLN_PROFL_BNF_DLVRY_METH_CD_SK"),
    F.col("svCFA").alias("PLN_PROFL_CFA_EDIT_CD_SK"),
    F.col("svCtrlPln").alias("PLN_PROFL_CTL_PLN_CD_SK"),
    F.col("svScdfTyp").alias("PLN_PROFL_DF_INFO_TYP_CD_SK"),
    F.col("svXmtMod").alias("PLN_PROFL_DF_TRNSMS_MODE_CD_SK"),
    F.col("svSttusCd").alias("PLN_PROFL_EDIT_STTUS_CD_SK"),
    F.col("svSttusQual").alias("PLN_PROFL_EDITSTTUS_QLFR_CD_SK"),
    F.col("svEOBGen").alias("PLN_PROFL_EOB_GNRTN_CD_SK"),
    F.col("svMngdCare").alias("PLN_PROFL_MC_INFO_CD_SK"),
    F.col("svNtnlOOA").alias("PLN_PROFL_NTNL_OOA_CD_SK"),
    F.col("svPlnPyr").alias("PLN_PROFL_PAYER_CD_SK"),
    F.col("svPlnPyrQ1").alias("PLN_PROFL_PAYER_RSTRCT1_CD_SK"),
    F.col("svPlnPyrQ2").alias("PLN_PROFL_PAYER_RSTRCT2_CD_SK"),
    F.col("svPlnPyrQ3").alias("PLN_PROFL_PAYER_RSTRCT3_CD_SK"),
    F.col("svPlnPyrQ4").alias("PLN_PROFL_PAYER_RSTRCT4_CD_SK"),
    F.col("svPlnPyrQ5").alias("PLN_PROFL_PAYER_RSTRCT5_CD_SK"),
    F.col("svPgm").alias("PLN_PROFL_PRCS_ARGMT_CD_SK"),
    F.col("svSccfPrcs").alias("PLN_PROFL_PRCS_PLN_CD_SK"),
    F.col("svProdTyp").alias("PLN_PROFL_PROD_CAT_CD_SK"),
    F.col("svRi").alias("PLN_PROFL_RCPT_INCUR_DT_CD_SK"),
    F.col("svXmtmod").alias("PLN_PROFL_SF_TRNSMS_MODE_CD_SK"),
    F.col("svStat").alias("PLN_PROFL_STAT_RCRD_CD_SK"),
    F.col("svTrnsmsnModeCd").alias("PLN_PROFL_TRNSMSN_MODE_CD_SK"),
    F.col("svx1099Gen").alias("PLN_PROFL_1099_GNRTN_CD_SK"),
    F.col("PRICE_DATA_APPEND_IN").alias("PRICE_DATA_APPEND_IN"),
    F.col("PROV_DATA_APPEND_IN").alias("PROV_DATA_APPEND_IN"),
    F.col("RCPT_INCUR_END_DT_SK").alias("RCPT_INCUR_END_DT_SK"),
    F.col("RESUBMT_DF_IN").alias("RESUBMT_DF_IN"),
    F.col("SF_LOCAL_PLN_STATN_ID").alias("SF_LOCAL_PLN_STATN_ID"),
    F.col("SF_PRCS_SITE_STATN_ID").alias("SF_PRCS_SITE_STATN_ID"),
    F.col("SRC_SYS_CLM_LAST_UPDT_DT_SK").alias("SRC_SYS_CLM_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_PRCS_DTM").cast(StringType()).alias("SRC_SYS_LAST_PRCS_DTM"),
    F.col("SRC_SYS_LAST_RULE_UPDT_DT_SK").alias("SRC_SYS_LAST_RULE_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_ID").alias("SRC_SYS_LAST_UPDT_USER_ID"),
    F.col("SUBMSN_EDIT_IN").alias("SUBMSN_EDIT_IN"),
    F.col("SUBMSN_PRCS_IN").alias("SUBMSN_PRCS_IN"),
    F.col("UNFRM_PRICE_FCLTY_EDIT_IN").alias("UNFRM_PRICE_FCLTY_EDIT_IN")
)

# ------------------------------------------------------------------------------
# 2b) Output Link DefaultNA: Constraint = @INROWNUM = 1
#     Generate exactly one row with specified constants (if at least one row exists)
# ------------------------------------------------------------------------------
# We check if we have at least one row in the enriched DF
count_enriched = df_ForeignKey_Enriched.limit(1).count()
if count_enriched > 0:
    # Construct the single row with the given "WhereExpression" overrides
    data_na = [{
        "PLN_PROFL_SK": 1,
        "ALPHA_PFX_CD": "NA",
        "ALPHA_PFX_LOCAL_PLN_CD": "NA",
        "CLM_SUBTYP_CD": "NA",
        "EFF_DT_SK": "1753-01-01",
        "RCPT_INCUR_STRT_DT_SK": "1753-01-01",
        "CRT_RUN_CYC_EXCTN_SK": 1,
        "LAST_UPDT_RUN_CYC_EXCTN_SK": 1,
        "ALPHA_PFX_SK": 1,
        "ACCT_PLN_NM": "NA",
        "ADJ_EDIT_IN": "X",
        "ADM_EXP_ALLWNC_NONSTD_AMT": 0.00,
        "ALPHA_PFX_LOCAL_PLN_CD_SK": 1,
        "CLM_SUBTYP_CD_SK": 1,
        "CTL_PLN_ACKNMT_RCPT_DT_SK": "1753-01-01",
        "CTL_PLN_RULE_CRT_DT_SK": "1753-01-01",
        "CTL_PLN_RULE_DSTRB_DT_SK": "1753-01-01",
        "CTL_PLN_RULE_VER_DT_SK": "1753-01-01",
        "CTL_PLN_STATN_ID": "NA",
        "DF_LOCAL_PLN_STATN_ID": "NA",
        "DF_PRCS_SITE_STATN_ID": "NA",
        "END_DT_SK": "1753-01-01",
        "HOME_PLN_PROV_CNTCT_ALW_IN": "X",
        "ITS_SFWR_VRSN_NO": 1,
        "LOCAL_PLN_RULE_DSTRB_DT_SK": "1753-01-01",
        "LOCAL_PLN_RULE_RCPT_DT_SK": "1753-01-01",
        "LOCAL_PLN_RULE_VER_DT_SK": "1753-01-01",
        "LOCAL_PLN_STATN_ID": "NA",
        "MULT_PRCS_ID": "NA",
        "NEW_END_DT_SK": "1753-01-01",
        "ORIG_PARTCPN_DT_SK": "1753-01-01",
        "PLN_PROFL_ACES_FEE_CD_SK": 1,
        "PLN_PROFL_ACCT_ENR_LVL_CD_SK": 1,
        "PLN_PROFL_ADM_EXP_ALLWNC_CD_SK": 1,
        "PLN_PROFL_BNF_DLVRY_METH_CD_SK": 1,
        "PLN_PROFL_CFA_EDIT_CD_SK": 1,
        "PLN_PROFL_CTL_PLN_CD_SK": 1,
        "PLN_PROFL_DF_INFO_TYP_CD_SK": 1,
        "PLN_PROFL_DF_TRNSMS_MODE_CD_SK": 1,
        "PLN_PROFL_EDIT_STTUS_CD_SK": 1,
        "PLN_PROFL_EDITSTTUS_QLFR_CD_SK": 1,
        "PLN_PROFL_EOB_GNRTN_CD_SK": 1,
        "PLN_PROFL_MC_INFO_CD_SK": 1,
        "PLN_PROFL_NTNL_OOA_CD_SK": 1,
        "PLN_PROFL_PAYER_CD_SK": 1,
        "PLN_PROFL_PAYER_RSTRCT1_CD_SK": 1,
        "PLN_PROFL_PAYER_RSTRCT2_CD_SK": 1,
        "PLN_PROFL_PAYER_RSTRCT3_CD_SK": 1,
        "PLN_PROFL_PAYER_RSTRCT4_CD_SK": 1,
        "PLN_PROFL_PAYER_RSTRCT5_CD_SK": 1,
        "PLN_PROFL_PRCS_ARGMT_CD_SK": 1,
        "PLN_PROFL_PRCS_PLN_CD_SK": 1,
        "PLN_PROFL_PROD_CAT_CD_SK": 1,
        "PLN_PROFL_RCPT_INCUR_DT_CD_SK": 1,
        "PLN_PROFL_SF_TRNSMS_MODE_CD_SK": 1,
        "PLN_PROFL_STAT_RCRD_CD_SK": 1,
        "PLN_PROFL_TRNSMSN_MODE_CD_SK": 1,
        "PLN_PROFL_1099_GNRTN_CD_SK": 1,
        "PRICE_DATA_APPEND_IN": "X",
        "PROV_DATA_APPEND_IN": "X",
        "RCPT_INCUR_END_DT_SK": "1753-01-01",
        "RESUBMT_DF_IN": "X",
        "SF_LOCAL_PLN_STATN_ID": "NA",
        "SF_PRCS_SITE_STATN_ID": "NA",
        "SRC_SYS_CLM_LAST_UPDT_DT_SK": "1753-01-01",
        "SRC_SYS_LAST_PRCS_DTM": "1753-01-01",
        "SRC_SYS_LAST_RULE_UPDT_DT_SK": "1753-01-01",
        "SRC_SYS_LAST_UPDT_DT_SK": "1753-01-01",
        "SRC_SYS_LAST_UPDT_USER_ID": "NA",
        "SUBMSN_EDIT_IN": "X",
        "SUBMSN_PRCS_IN": "X",
        "UNFRM_PRICE_FCLTY_EDIT_IN": "X"
    }]
    df_ForeignKey_DefaultNA = spark.createDataFrame(data_na)
else:
    # Empty if no row
    df_ForeignKey_DefaultNA = df_IdsPlnProflExtr.limit(0)

# ------------------------------------------------------------------------------
# 2c) Output Link DefaultUNK: Constraint = @INROWNUM = 1
#     Generate exactly one row with specified constants (if at least one row exists)
# ------------------------------------------------------------------------------
if count_enriched > 0:
    data_unk = [{
        "PLN_PROFL_SK": 0,
        "ALPHA_PFX_CD": "UNK",
        "ALPHA_PFX_LOCAL_PLN_CD": "UNK",
        "CLM_SUBTYP_CD": "UNK",
        "EFF_DT_SK": "1753-01-01",
        "RCPT_INCUR_STRT_DT_SK": "1753-01-01",
        "CRT_RUN_CYC_EXCTN_SK": 0,
        "LAST_UPDT_RUN_CYC_EXCTN_SK": 0,
        "ALPHA_PFX_SK": 0,
        "ACCT_PLN_NM": "UNK",
        "ADJ_EDIT_IN": "U",
        "ADM_EXP_ALLWNC_NONSTD_AMT": 0.00,
        "ALPHA_PFX_LOCAL_PLN_CD_SK": 0,
        "CLM_SUBTYP_CD_SK": 0,
        "CTL_PLN_ACKNMT_RCPT_DT_SK": "1753-01-01",
        "CTL_PLN_RULE_CRT_DT_SK": "1753-01-01",
        "CTL_PLN_RULE_DSTRB_DT_SK": "1753-01-01",
        "CTL_PLN_RULE_VER_DT_SK": "1753-01-01",
        "CTL_PLN_STATN_ID": "UNK",
        "DF_LOCAL_PLN_STATN_ID": "UNK",
        "DF_PRCS_SITE_STATN_ID": "UNK",
        "END_DT_SK": "1753-01-01",
        "HOME_PLN_PROV_CNTCT_ALW_IN": "U",
        "ITS_SFWR_VRSN_NO": 0,
        "LOCAL_PLN_RULE_DSTRB_DT_SK": "1753-01-01",
        "LOCAL_PLN_RULE_RCPT_DT_SK": "1753-01-01",
        "LOCAL_PLN_RULE_VER_DT_SK": "1753-01-01",
        "LOCAL_PLN_STATN_ID": "UNK",
        "MULT_PRCS_ID": "UNK",
        "NEW_END_DT_SK": "1753-01-01",
        "ORIG_PARTCPN_DT_SK": "1753-01-01",
        "PLN_PROFL_ACES_FEE_CD_SK": 0,
        "PLN_PROFL_ACCT_ENR_LVL_CD_SK": 0,
        "PLN_PROFL_ADM_EXP_ALLWNC_CD_SK": 0,
        "PLN_PROFL_BNF_DLVRY_METH_CD_SK": 0,
        "PLN_PROFL_CFA_EDIT_CD_SK": 0,
        "PLN_PROFL_CTL_PLN_CD_SK": 0,
        "PLN_PROFL_DF_INFO_TYP_CD_SK": 0,
        "PLN_PROFL_DF_TRNSMS_MODE_CD_SK": 0,
        "PLN_PROFL_EDIT_STTUS_CD_SK": 0,
        "PLN_PROFL_EDITSTTUS_QLFR_CD_SK": 0,
        "PLN_PROFL_EOB_GNRTN_CD_SK": 0,
        "PLN_PROFL_MC_INFO_CD_SK": 0,
        "PLN_PROFL_NTNL_OOA_CD_SK": 0,
        "PLN_PROFL_PAYER_CD_SK": 0,
        "PLN_PROFL_PAYER_RSTRCT1_CD_SK": 0,
        "PLN_PROFL_PAYER_RSTRCT2_CD_SK": 0,
        "PLN_PROFL_PAYER_RSTRCT3_CD_SK": 0,
        "PLN_PROFL_PAYER_RSTRCT4_CD_SK": 0,
        "PLN_PROFL_PAYER_RSTRCT5_CD_SK": 0,
        "PLN_PROFL_PRCS_ARGMT_CD_SK": 0,
        "PLN_PROFL_PRCS_PLN_CD_SK": 0,
        "PLN_PROFL_PROD_CAT_CD_SK": 0,
        "PLN_PROFL_RCPT_INCUR_DT_CD_SK": 0,
        "PLN_PROFL_SF_TRNSMS_MODE_CD_SK": 0,
        "PLN_PROFL_STAT_RCRD_CD_SK": 0,
        "PLN_PROFL_TRNSMSN_MODE_CD_SK": 0,
        "PLN_PROFL_1099_GNRTN_CD_SK": 0,
        "PRICE_DATA_APPEND_IN": "U",
        "PROV_DATA_APPEND_IN": "U",
        "RCPT_INCUR_END_DT_SK": "1753-01-01",
        "RESUBMT_DF_IN": "U",
        "SF_LOCAL_PLN_STATN_ID": "UNK",
        "SF_PRCS_SITE_STATN_ID": "UNK",
        "SRC_SYS_CLM_LAST_UPDT_DT_SK": "1753-01-01",
        "SRC_SYS_LAST_PRCS_DTM": "1753-01-01",
        "SRC_SYS_LAST_RULE_UPDT_DT_SK": "1753-01-01",
        "SRC_SYS_LAST_UPDT_DT_SK": "1753-01-01",
        "SRC_SYS_LAST_UPDT_USER_ID": "UNK",
        "SUBMSN_EDIT_IN": "U",
        "SUBMSN_PRCS_IN": "U",
        "UNFRM_PRICE_FCLTY_EDIT_IN": "U"
    }]
    df_ForeignKey_DefaultUNK = spark.createDataFrame(data_unk)
else:
    df_ForeignKey_DefaultUNK = df_IdsPlnProflExtr.limit(0)

# ------------------------------------------------------------------------------
# 2d) Output Link Recycle: Constraint = (ErrCount > 0)
#     Write to hashed file hf_recycle => Scenario C => write Parquet
# ------------------------------------------------------------------------------
df_ForeignKey_Recycle_temp = df_ForeignKey_Enriched.filter(F.col("ErrCount") > 0)

df_ForeignKey_Recycle = df_ForeignKey_Recycle_temp.select(
    F.expr("GetRecycleKey(PLN_PROFL_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PLN_PROFL_SK").alias("PLN_PROFL_SK"),
    F.col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("ALPHA_PFX_LOCAL_PLN_CD").alias("ALPHA_PFX_LOCAL_PLN_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("RCPT_INCUR_STRT_DT_SK").alias("RCPT_INCUR_STRT_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ACCT_PLN_NM").alias("ACCT_PLN_NM"),
    F.col("ADJ_EDIT_IN").alias("ADJ_EDIT_IN"),
    F.col("ADM_EXP_ALLWNC_NONSTD_AMT").alias("ADM_EXP_ALLWNC_NONSTD_AMT"),
    F.col("CTL_PLN_ACKNMT_RCPT_DT_SK").alias("CTL_PLN_ACKNMT_RCPT_DT_SK"),
    F.col("CTL_PLN_RULE_CRT_DT_SK").alias("CTL_PLN_RULE_CRT_DT_SK"),
    F.col("CTL_PLN_RULE_DSTRB_DT_SK").alias("CTL_PLN_RULE_DSTRB_DT_SK"),
    F.col("CTL_PLN_RULE_VER_DT_SK").alias("CTL_PLN_RULE_VER_DT_SK"),
    F.col("CTL_PLN_STATN_ID").alias("CTL_PLN_STATN_ID"),
    F.col("DF_LOCAL_PLN_STATN_ID").alias("DF_LOCAL_PLN_STATN_ID"),
    F.col("DF_PRCS_SITE_STATN_ID").alias("DF_PRCS_SITE_STATN_ID"),
    F.col("END_DT_SK").alias("END_DT_SK"),
    F.col("HOME_PLN_PROV_CNTCT_ALW_IN").alias("HOME_PLN_PROV_CNTCT_ALW_IN"),
    F.col("ITS_SFWR_VRSN_NO").alias("ITS_SFWR_VRSN_NO"),
    F.col("LOCAL_PLN_RULE_DSTRB_DT_SK").alias("LOCAL_PLN_RULE_DSTRB_DT_SK"),
    F.col("LOCAL_PLN_RULE_RCPT_DT_SK").alias("LOCAL_PLN_RULE_RCPT_DT_SK"),
    F.col("LOCAL_PLN_RULE_VER_DT_SK").alias("LOCAL_PLN_RULE_VER_DT_SK"),
    F.col("LOCAL_PLN_STATN_ID").alias("LOCAL_PLN_STATN_ID"),
    F.col("MULT_PRCS_ID").alias("MULT_PRCS_ID"),
    F.col("NEW_END_DT_SK").alias("NEW_END_DT_SK"),
    F.col("ORIG_PARTCPN_DT_SK").alias("ORIG_PARTCPN_DT_SK"),
    F.col("PLN_PROFL_ACES_FEE_CD").alias("PLN_PROFL_ACES_FEE_CD"),
    F.col("PLN_PROFL_ACCT_ENR_LVL_CD").alias("PLN_PROFL_ACCT_ENR_LVL_CD"),
    F.col("PLN_PROFL_ADM_EXP_ALLWNC_CD").alias("PLN_PROFL_ADM_EXP_ALLWNC_CD"),
    F.col("PLN_PROFL_BNF_DLVRY_METH_CD").alias("PLN_PROFL_BNF_DLVRY_METH_CD"),
    F.col("PLN_PROFL_CFA_EDIT_CD").alias("PLN_PROFL_CFA_EDIT_CD"),
    F.col("PLN_PROFL_CTL_PLN_CD").alias("PLN_PROFL_CTL_PLN_CD"),
    F.col("PLN_PROFL_DF_INFO_TYP_CD").alias("PLN_PROFL_DF_INFO_TYP_CD"),
    F.col("PLN_PROFL_DF_TRNSMS_MODE_CD").alias("PLN_PROFL_DF_TRNSMS_MODE_CD"),
    F.col("PLN_PROFL_EDIT_STTUS_CD").alias("PLN_PROFL_EDIT_STTUS_CD"),
    F.col("PLN_PROFL_EDITSTTUS_QLFR_CD").alias("PLN_PROFL_EDITSTTUS_QLFR_CD"),
    F.col("PLN_PROFL_EOB_GNRTN_CD").alias("PLN_PROFL_EOB_GNRTN_CD"),
    F.col("PLN_PROFL_MC_INFO_CD").alias("PLN_PROFL_MC_INFO_CD"),
    F.col("PLN_PROFL_NTNL_OOA_CD").alias("PLN_PROFL_NTNL_OOA_CD"),
    F.col("PLN_PROFL_PAYER_CD").alias("PLN_PROFL_PAYER_CD"),
    F.col("PLN_PROFL_PAYER_RSTRCT1_CD").alias("PLN_PROFL_PAYER_RSTRCT1_CD"),
    F.col("PLN_PROFL_PAYER_RSTRCT2_CD").alias("PLN_PROFL_PAYER_RSTRCT2_CD"),
    F.col("PLN_PROFL_PAYER_RSTRCT3_CD").alias("PLN_PROFL_PAYER_RSTRCT3_CD"),
    F.col("PLN_PROFL_PAYER_RSTRCT4_CD").alias("PLN_PROFL_PAYER_RSTRCT4_CD"),
    F.col("PLN_PROFL_PAYER_RSTRCT5_CD").alias("PLN_PROFL_PAYER_RSTRCT5_CD"),
    F.col("PLN_PROFL_PRCS_ARGMT_CD").alias("PLN_PROFL_PRCS_ARGMT_CD"),
    F.col("PLN_PROFL_PRCS_PLN_CD").alias("PLN_PROFL_PRCS_PLN_CD"),
    F.col("PLN_PROFL_PROD_CAT_CD").alias("PLN_PROFL_PROD_CAT_CD"),
    F.col("PLN_PROFL_RCPT_INCUR_DT_TYP_CD").alias("PLN_PROFL_RCPT_INCUR_DT_TYP_CD"),
    F.col("PLN_PROFL_SF_TRNSMS_MODE_CD").alias("PLN_PROFL_SF_TRNSMS_MODE_CD"),
    F.col("PLN_PROFL_STAT_RCRD_CD").alias("PLN_PROFL_STAT_RCRD_CD"),
    F.col("PLN_PROFL_TRNSMSN_MODE_CD").alias("PLN_PROFL_TRNSMSN_MODE_CD"),
    F.col("PLN_PROFL_1099_GNRTN_CD").alias("PLN_PROFL_1099_GNRTN_CD"),
    F.col("PRICE_DATA_APPEND_IN").alias("PRICE_DATA_APPEND_IN"),
    F.col("PROV_DATA_APPEND_IN").alias("PROV_DATA_APPEND_IN"),
    F.col("RCPT_INCUR_END_DT_SK").alias("RCPT_INCUR_END_DT_SK"),
    F.col("RESUBMT_DF_IN").alias("RESUBMT_DF_IN"),
    F.col("SF_LOCAL_PLN_STATN_ID").alias("SF_LOCAL_PLN_STATN_ID"),
    F.col("SF_PRCS_SITE_STATN_ID").alias("SF_PRCS_SITE_STATN_ID"),
    F.col("SRC_SYS_CLM_LAST_UPDT_DT_SK").alias("SRC_SYS_CLM_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_PRCS_DTM").cast(StringType()).alias("SRC_SYS_LAST_PRCS_DTM"),
    F.col("SRC_SYS_LAST_RULE_UPDT_DT_SK").alias("SRC_SYS_LAST_RULE_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_ID").alias("SRC_SYS_LAST_UPDT_USER_ID"),
    F.col("SUBMSN_EDIT_IN").alias("SUBMSN_EDIT_IN"),
    F.col("SUBMSN_PRCS_IN").alias("SUBMSN_PRCS_IN"),
    F.col("UNFRM_PRICE_FCLTY_EDIT_IN").alias("UNFRM_PRICE_FCLTY_EDIT_IN")
)

# For char/varchar columns of df_ForeignKey_Recycle, apply rpad if length is known from the original JSON
# Example:
# INSRT_UPDT_CD is char(10), so: rpad("INSRT_UPDT_CD",10," ") 
# We'll define a helper map for lengths we saw in the hashed-file columns:
char_lengths_recycle = {
    "INSRT_UPDT_CD": 10,
    "DISCARD_IN": 1,
    "PASS_THRU_IN": 1,
    "ADJ_EDIT_IN": 1,
    "CTL_PLN_ACKNMT_RCPT_DT_SK": 10,
    "CTL_PLN_RULE_CRT_DT_SK": 10,
    "CTL_PLN_RULE_DSTRB_DT_SK": 10,
    "CTL_PLN_RULE_VER_DT_SK": 10,
    "END_DT_SK": 10,
    "HOME_PLN_PROV_CNTCT_ALW_IN": 1,
    "LOCAL_PLN_RULE_DSTRB_DT_SK": 10,
    "LOCAL_PLN_RULE_RCPT_DT_SK": 10,
    "LOCAL_PLN_RULE_VER_DT_SK": 10,
    "MULT_PRCS_ID": 2,
    "NEW_END_DT_SK": 10,
    "ORIG_PARTCPN_DT_SK": 10,
    "PLN_PROFL_ACES_FEE_CD": 1,
    "PLN_PROFL_ACCT_ENR_LVL_CD": 1,
    "PLN_PROFL_ADM_EXP_ALLWNC_CD": 1,
    "PLN_PROFL_BNF_DLVRY_METH_CD": 1,
    "PLN_PROFL_CFA_EDIT_CD": 1,
    "PLN_PROFL_CTL_PLN_CD": 3,
    "PLN_PROFL_DF_INFO_TYP_CD": 1,
    "PLN_PROFL_DF_TRNSMS_MODE_CD": 1,
    "PLN_PROFL_EDIT_STTUS_CD": 1,
    "PLN_PROFL_EDITSTTUS_QLFR_CD": 2,
    "PLN_PROFL_EOB_GNRTN_CD": 1,
    "PLN_PROFL_MC_INFO_CD": 1,
    "PLN_PROFL_NTNL_OOA_CD": 1,
    "PLN_PROFL_PAYER_CD": 1,
    "PLN_PROFL_PAYER_RSTRCT1_CD": 2,
    "PLN_PROFL_PAYER_RSTRCT2_CD": 2,
    "PLN_PROFL_PAYER_RSTRCT3_CD": 2,
    "PLN_PROFL_PAYER_RSTRCT4_CD": 2,
    "PLN_PROFL_PAYER_RSTRCT5_CD": 2,
    "PLN_PROFL_PRCS_ARGMT_CD": 1,
    "PLN_PROFL_PRCS_PLN_CD": 3,
    "PLN_PROFL_PROD_CAT_CD": 1,
    "PLN_PROFL_RCPT_INCUR_DT_TYP_CD": 1,
    "PLN_PROFL_SF_TRNSMS_MODE_CD": 1,
    "PLN_PROFL_STAT_RCRD_CD": 1,
    "PLN_PROFL_1099_GNRTN_CD": 1,
    "PRICE_DATA_APPEND_IN": 1,
    "PROV_DATA_APPEND_IN": 1,
    "RCPT_INCUR_END_DT_SK": 10,
    "RESUBMT_DF_IN": 1,
    "SRC_SYS_CLM_LAST_UPDT_DT_SK": 10,
    "SRC_SYS_LAST_PRCS_DTM": 10,
    "SRC_SYS_LAST_RULE_UPDT_DT_SK": 10,
    "SRC_SYS_LAST_UPDT_DT_SK": 10,
    "SUBMSN_EDIT_IN": 1,
    "SUBMSN_PRCS_IN": 1,
    "UNFRM_PRICE_FCLTY_EDIT_IN": 1
}

df_rp_cols_recycle = []
for c in df_ForeignKey_Recycle.columns:
    if c in char_lengths_recycle:
        df_rp_cols_recycle.append(F.rpad(F.col(c), char_lengths_recycle[c], " ").alias(c))
    else:
        df_rp_cols_recycle.append(F.col(c))

df_ForeignKey_Recycle_final = df_ForeignKey_Recycle.select(*df_rp_cols_recycle)

write_files(
    df_ForeignKey_Recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------
# 3) Collector Stage: Union Fkey, DefaultNA, DefaultUNK -> single output link
# ------------------------------------------------------------------------------
df_ForeignKey_Fkey_final = df_ForeignKey_Fkey
df_ForeignKey_DefaultNA_final = df_ForeignKey_DefaultNA
df_ForeignKey_DefaultUNK_final = df_ForeignKey_DefaultUNK

# Union all
df_Collector = df_ForeignKey_Fkey_final.unionByName(df_ForeignKey_DefaultNA_final).unionByName(df_ForeignKey_DefaultUNK_final)

# ------------------------------------------------------------------------------
# 4) Write to CSeqFileStage: PLN_PROFL.dat in "load" folder
# ------------------------------------------------------------------------------
# We must apply rpad for char/varchar columns based on Collector link definition
char_lengths_pln_profl = {
    "ALPHA_PFX_CD": 0,  # no length given => skip
    "ALPHA_PFX_LOCAL_PLN_CD": 0,
    "CLM_SUBTYP_CD": 0,
    "EFF_DT_SK": 10,
    "RCPT_INCUR_STRT_DT_SK": 10,
    "ADJ_EDIT_IN": 1,
    "CTL_PLN_ACKNMT_RCPT_DT_SK": 10,
    "CTL_PLN_RULE_CRT_DT_SK": 10,
    "CTL_PLN_RULE_DSTRB_DT_SK": 10,
    "CTL_PLN_RULE_VER_DT_SK": 10,
    "END_DT_SK": 10,
    "HOME_PLN_PROV_CNTCT_ALW_IN": 1,
    "LOCAL_PLN_RULE_DSTRB_DT_SK": 10,
    "LOCAL_PLN_RULE_RCPT_DT_SK": 10,
    "LOCAL_PLN_RULE_VER_DT_SK": 10,
    "MULT_PRCS_ID": 0,   # length=2 in original, but not strictly repeated in collector snippet
    "NEW_END_DT_SK": 10,
    "ORIG_PARTCPN_DT_SK": 10,
    "PLN_PROFL_ACES_FEE_CD_SK": 0,
    "PLN_PROFL_ACCT_ENR_LVL_CD_SK": 0,
    "PLN_PROFL_ADM_EXP_ALLWNC_CD_SK": 0,
    "PLN_PROFL_BNF_DLVRY_METH_CD_SK": 0,
    "PLN_PROFL_CFA_EDIT_CD_SK": 0,
    "PLN_PROFL_CTL_PLN_CD_SK": 0,
    "PLN_PROFL_DF_INFO_TYP_CD_SK": 0,
    "PLN_PROFL_DF_TRNSMS_MODE_CD_SK": 0,
    "PLN_PROFL_EDIT_STTUS_CD_SK": 0,
    "PLN_PROFL_EDITSTTUS_QLFR_CD_SK": 0,
    "PLN_PROFL_EOB_GNRTN_CD_SK": 0,
    "PLN_PROFL_MC_INFO_CD_SK": 0,
    "PLN_PROFL_NTNL_OOA_CD_SK": 0,
    "PLN_PROFL_PAYER_CD_SK": 0,
    "PLN_PROFL_PAYER_RSTRCT1_CD_SK": 0,
    "PLN_PROFL_PAYER_RSTRCT2_CD_SK": 0,
    "PLN_PROFL_PAYER_RSTRCT3_CD_SK": 0,
    "PLN_PROFL_PAYER_RSTRCT4_CD_SK": 0,
    "PLN_PROFL_PAYER_RSTRCT5_CD_SK": 0,
    "PLN_PROFL_PRCS_ARGMT_CD_SK": 0,
    "PLN_PROFL_PRCS_PLN_CD_SK": 0,
    "PLN_PROFL_PROD_CAT_CD_SK": 0,
    "PLN_PROFL_RCPT_INCUR_DT_CD_SK": 0,
    "PLN_PROFL_SF_TRNSMS_MODE_CD_SK": 0,
    "PLN_PROFL_STAT_RCRD_CD_SK": 0,
    "PLN_PROFL_TRNSMSN_MODE_CD_SK": 0,
    "PLN_PROFL_1099_GNRTN_CD_SK": 0,
    "PRICE_DATA_APPEND_IN": 1,
    "PROV_DATA_APPEND_IN": 1,
    "RCPT_INCUR_END_DT_SK": 10,
    "RESUBMT_DF_IN": 1,
    "SRC_SYS_CLM_LAST_UPDT_DT_SK": 10,
    "SRC_SYS_LAST_PRCS_DTM": 10,
    "SRC_SYS_LAST_RULE_UPDT_DT_SK": 10,
    "SRC_SYS_LAST_UPDT_DT_SK": 10,
    "SUBMSN_EDIT_IN": 1,
    "SUBMSN_PRCS_IN": 1,
    "UNFRM_PRICE_FCLTY_EDIT_IN": 1
}

select_cols_collector = [
    "PLN_PROFL_SK",
    "ALPHA_PFX_CD",
    "ALPHA_PFX_LOCAL_PLN_CD",
    "CLM_SUBTYP_CD",
    "EFF_DT_SK",
    "RCPT_INCUR_STRT_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALPHA_PFX_SK",
    "ACCT_PLN_NM",
    "ADJ_EDIT_IN",
    "ADM_EXP_ALLWNC_NONSTD_AMT",
    "ALPHA_PFX_LOCAL_PLN_CD_SK",
    "CLM_SUBTYP_CD_SK",
    "CTL_PLN_ACKNMT_RCPT_DT_SK",
    "CTL_PLN_RULE_CRT_DT_SK",
    "CTL_PLN_RULE_DSTRB_DT_SK",
    "CTL_PLN_RULE_VER_DT_SK",
    "CTL_PLN_STATN_ID",
    "DF_LOCAL_PLN_STATN_ID",
    "DF_PRCS_SITE_STATN_ID",
    "END_DT_SK",
    "HOME_PLN_PROV_CNTCT_ALW_IN",
    "ITS_SFWR_VRSN_NO",
    "LOCAL_PLN_RULE_DSTRB_DT_SK",
    "LOCAL_PLN_RULE_RCPT_DT_SK",
    "LOCAL_PLN_RULE_VER_DT_SK",
    "LOCAL_PLN_STATN_ID",
    "MULT_PRCS_ID",
    "NEW_END_DT_SK",
    "ORIG_PARTCPN_DT_SK",
    "PLN_PROFL_ACES_FEE_CD_SK",
    "PLN_PROFL_ACCT_ENR_LVL_CD_SK",
    "PLN_PROFL_ADM_EXP_ALLWNC_CD_SK",
    "PLN_PROFL_BNF_DLVRY_METH_CD_SK",
    "PLN_PROFL_CFA_EDIT_CD_SK",
    "PLN_PROFL_CTL_PLN_CD_SK",
    "PLN_PROFL_DF_INFO_TYP_CD_SK",
    "PLN_PROFL_DF_TRNSMS_MODE_CD_SK",
    "PLN_PROFL_EDIT_STTUS_CD_SK",
    "PLN_PROFL_EDITSTTUS_QLFR_CD_SK",
    "PLN_PROFL_EOB_GNRTN_CD_SK",
    "PLN_PROFL_MC_INFO_CD_SK",
    "PLN_PROFL_NTNL_OOA_CD_SK",
    "PLN_PROFL_PAYER_CD_SK",
    "PLN_PROFL_PAYER_RSTRCT1_CD_SK",
    "PLN_PROFL_PAYER_RSTRCT2_CD_SK",
    "PLN_PROFL_PAYER_RSTRCT3_CD_SK",
    "PLN_PROFL_PAYER_RSTRCT4_CD_SK",
    "PLN_PROFL_PAYER_RSTRCT5_CD_SK",
    "PLN_PROFL_PRCS_ARGMT_CD_SK",
    "PLN_PROFL_PRCS_PLN_CD_SK",
    "PLN_PROFL_PROD_CAT_CD_SK",
    "PLN_PROFL_RCPT_INCUR_DT_CD_SK",
    "PLN_PROFL_SF_TRNSMS_MODE_CD_SK",
    "PLN_PROFL_STAT_RCRD_CD_SK",
    "PLN_PROFL_TRNSMSN_MODE_CD_SK",
    "PLN_PROFL_1099_GNRTN_CD_SK",
    "PRICE_DATA_APPEND_IN",
    "PROV_DATA_APPEND_IN",
    "RCPT_INCUR_END_DT_SK",
    "RESUBMT_DF_IN",
    "SF_LOCAL_PLN_STATN_ID",
    "SF_PRCS_SITE_STATN_ID",
    "SRC_SYS_CLM_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_PRCS_DTM",
    "SRC_SYS_LAST_RULE_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_ID",
    "SUBMSN_EDIT_IN",
    "SUBMSN_PRCS_IN",
    "UNFRM_PRICE_FCLTY_EDIT_IN"
]

df_Collector_selected = df_Collector.select(*[F.col(c) for c in select_cols_collector])

# Apply rpad for known char columns
df_rp_cols_collector = []
for c in select_cols_collector:
    if c in char_lengths_pln_profl and char_lengths_pln_profl[c] > 0:
        df_rp_cols_collector.append(F.rpad(F.col(c), char_lengths_pln_profl[c], " ").alias(c))
    else:
        df_rp_cols_collector.append(F.col(c))

df_PLN_PROFL_final = df_Collector_selected.select(*df_rp_cols_collector)

write_files(
    df_PLN_PROFL_final,
    f"{adls_path}/load/PLN_PROFL.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)