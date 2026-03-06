# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: FctsCommissionMonthlyLoadSeq
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                   10/30/2012       4873                            Originally Programmed                           IntegrateNewDevl          Kalyan Neelam             2012-11-06
# MAGIC 
# MAGIC T.Sieg                              2021-02-19                                              Adding 16 new columns for the ACA and MA     IntegrateDev2  Raja Gummadi              2021-05-03
# MAGIC                                                                                                         commission activity

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFile = get_widget_value("InFile","IdsComsnIncmExtr.tmp")
Logging = get_widget_value("Logging","X")
SrcSysCdSk = get_widget_value("SrcSysCdSk","-2020658781")

schema_IdsCmsnIncm = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), nullable=False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), nullable=False),
    T.StructField("DISCARD_IN", T.StringType(), nullable=False),
    T.StructField("PASS_THRU_IN", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), nullable=False),
    T.StructField("ERR_CT", T.IntegerType(), nullable=False),
    T.StructField("RECYCLE_CT", T.DecimalType(10,0), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("PRI_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("COMSN_INCM_SK", T.IntegerType(), nullable=False),
    T.StructField("PD_AGNT_ID", T.StringType(), nullable=False),
    T.StructField("ERN_AGNT_ID", T.StringType(), nullable=False),
    T.StructField("COMSN_PD_DT_SK", T.StringType(), nullable=False),
    T.StructField("BILL_ENTY_UNIQ_KEY", T.IntegerType(), nullable=False),
    T.StructField("BILL_INCM_RCPT_BILL_DUE_DT_SK", T.StringType(), nullable=False),
    T.StructField("COMSN_INCM_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CLS_PLN_ID", T.StringType(), nullable=False),
    T.StructField("PROD_ID", T.StringType(), nullable=False),
    T.StructField("COMSN_DTL_INCM_DISP_CD", T.StringType(), nullable=False),
    T.StructField("COMSN_TYP_CD", T.StringType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXTCN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", T.IntegerType(), nullable=False),
    T.StructField("AGNT_ID", T.StringType(), nullable=False),
    T.StructField("AGNY_AGNT_SK", T.IntegerType(), nullable=True),
    T.StructField("BILL_INCM_RCPT_SK", T.IntegerType(), nullable=True),
    T.StructField("BILL_ENTY_SK", T.IntegerType(), nullable=True),
    T.StructField("BILL_SUM_SK", T.IntegerType(), nullable=True),
    T.StructField("CLS_PLN_SK", T.IntegerType(), nullable=True),
    T.StructField("COMSN_AGMNT_SK", T.IntegerType(), nullable=True),
    T.StructField("ERN_AGNT_SK", T.IntegerType(), nullable=True),
    T.StructField("EXPRNC_CAT_SK", T.IntegerType(), nullable=True),
    T.StructField("FNCL_LOB_SK", T.IntegerType(), nullable=True),
    T.StructField("PD_AGNT_SK", T.IntegerType(), nullable=True),
    T.StructField("PROD_SK", T.IntegerType(), nullable=True),
    T.StructField("PROD_SH_NM_SK", T.IntegerType(), nullable=True),
    T.StructField("AGNT_STTUS_CD", T.StringType(), nullable=False),
    T.StructField("COMSN_CAT_CD", T.StringType(), nullable=False),
    T.StructField("FUND_TYP", T.StringType(), nullable=False),
    T.StructField("CT_TYP_CD", T.StringType(), nullable=False),
    T.StructField("COMSN_MTHDLGY_TYP", T.StringType(), nullable=False),
    T.StructField("AGNT_STTUS_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("AGNT_TIER_LVL_CD", T.StringType(), nullable=False),
    T.StructField("COMSN_BSS_SRC_CD", T.StringType(), nullable=False),
    T.StructField("AGNT_TIER_LVL_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("COMSN_BSS_SRC_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("COMSN_COV_CAT_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("COMSN_DTL_INCM_DISP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("COMSN_MTHDLGY_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("COMSN_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("CT_TYP_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("FUND_CAT_CD_SK", T.IntegerType(), nullable=False),
    T.StructField("COMSN_PD_YR_MO", T.StringType(), nullable=False),
    T.StructField("CONT_COV_STRT_DT_SK", T.StringType(), nullable=False),
    T.StructField("COV_END_DT_SK", T.StringType(), nullable=False),
    T.StructField("COV_PLN_YR", T.StringType(), nullable=False),
    T.StructField("DP_CANC_YR_MO", T.StringType(), nullable=False),
    T.StructField("PLN_YR_COV_EFF_DT_SK", T.StringType(), nullable=False),
    T.StructField("PRM_PD_TO_DT_SK", T.StringType(), nullable=False),
    T.StructField("COMSN_BSS_AMT", T.DecimalType(10,0), nullable=False),
    T.StructField("COMSN_RATE_AMT", T.DecimalType(10,0), nullable=False),
    T.StructField("PD_COMSN_AMT", T.DecimalType(10,0), nullable=False),
    T.StructField("BILL_COMSN_PRM_PCT", T.DecimalType(10,0), nullable=True),
    T.StructField("COMSN_AGMNT_SCHD_FCTR", T.DecimalType(10,0), nullable=False),
    T.StructField("COMSN_RATE_PCT", T.DecimalType(10,0), nullable=True),
    T.StructField("CNTR_CT", T.IntegerType(), nullable=True),
    T.StructField("MBR_CT", T.IntegerType(), nullable=True),
    T.StructField("SRC_TRANS_TYP_TX", T.StringType(), nullable=False),
    T.StructField("COMSN_ARGMT_ID", T.StringType(), nullable=False),
    T.StructField("EFF_STRT_DT", T.StringType(), nullable=False),
    T.StructField("ACA_BUS_SBSDY_TYP_ID", T.StringType(), nullable=True),
    T.StructField("ACA_PRM_SBSDY_AMT", T.DecimalType(10,0), nullable=True),
    T.StructField("AGNY_HIER_LVL_ID", T.StringType(), nullable=True),
    T.StructField("CMS_CUST_POL_ID", T.StringType(), nullable=True),
    T.StructField("CMS_PLN_BNF_PCKG_ID", T.StringType(), nullable=True),
    T.StructField("COMSN_BLUE_KC_BRKR_PAYMT_IN", T.StringType(), nullable=True),
    T.StructField("COMSN_PAYOUT_TYP_ID", T.StringType(), nullable=True),
    T.StructField("COMSN_RETROACTV_ADJ_IN", T.StringType(), nullable=True),
    T.StructField("CUST_BUS_TYP_ID", T.StringType(), nullable=True),
    T.StructField("CUST_BUS_SUBTYP_ID", T.StringType(), nullable=True),
    T.StructField("ICM_COMSN_PAYE_ID", T.StringType(), nullable=True),
    T.StructField("MCARE_ADVNTG_ENR_CYC_YR_ID", T.StringType(), nullable=True),
    T.StructField("MCARE_BNFCRY_ID", T.StringType(), nullable=True),
    T.StructField("MCARE_ENR_TYP_ID", T.StringType(), nullable=True),
    T.StructField("TRANS_CNTR_CT", T.IntegerType(), nullable=True),
    T.StructField("TRNSMSN_SRC_SYS_CD", T.StringType(), nullable=True)
])

df_IdsCmsnIncm = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_IdsCmsnIncm)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_base = (
    df_IdsCmsnIncm
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAgntSttusCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "AGENT STATUS", F.col("AGNT_STTUS_CD"), F.col("Logging")))
    .withColumn("svAgntBssSrcCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "COMMISSION BASIS SOURCE", F.col("COMSN_BSS_SRC_CD"), F.col("Logging")))
    .withColumn("svComsnCovCatCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "COVERAGE CATEGORY", F.col("COMSN_CAT_CD"), F.col("Logging")))
    .withColumn("svComsnTypCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "COMMISSION TYPE", F.col("COMSN_TYP_CD"), F.col("Logging")))
    .withColumn("svCtCatTypCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "COUNT TYPE", F.col("CT_TYP_CD"), F.col("Logging")))
    .withColumn("svFundCatCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "FUNDING CATEGORY", F.col("FUND_TYP"), F.col("Logging")))
    .withColumn("svComsnDtlIncmCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "COMMISSION DETAIL DISPOSITION", F.col("COMSN_DTL_INCM_DISP_CD"), F.col("Logging")))
    .withColumn("svComsnMthdglyCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "COMMISSION METHODOLOGY TYPE", F.col("COMSN_MTHDLGY_TYP"), F.col("Logging")))
    .withColumn("svAgntTierLvlCdSk", GetFkeyCodes('FACETS', F.col("COMSN_INCM_SK"), "AGENT TIER LEVEL", F.col("AGNT_TIER_LVL_CD"), F.col("Logging")))
    .withColumn("svProdSK", GetFkeyProd('FACETS', F.col("COMSN_INCM_SK"), F.col("PROD_ID"), F.col("Logging")))
    .withColumn("svPdAgntSk", GetFkeyAgnt('FACETS', F.col("COMSN_INCM_SK"), F.col("PD_AGNT_ID"), F.col("Logging")))
    .withColumn("svErnAgntSk", GetFkeyAgnt('FACETS', F.col("COMSN_INCM_SK"), F.col("ERN_AGNT_ID"), F.col("Logging")))
    .withColumn("svClsPlnSk", GetFkeyClsPln('FACETS', F.col("COMSN_INCM_SK"), F.col("CLS_PLN_ID"), F.col("Logging")))
    .withColumn("svBillEntySk", GetFkeyBillEnty('FACETS', F.col("COMSN_INCM_SK"), F.col("BILL_ENTY_UNIQ_KEY"), F.col("Logging")))
    .withColumn("svAgnyAgntSk", GetFkeyAgnt('FACETS', F.col("COMSN_INCM_SK"), F.col("AGNT_ID"), F.col("Logging")))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("COMSN_INCM_SK")))
)

df_Fkey = (
    df_ForeignKey_base
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("COMSN_INCM_SK").alias("COMSN_INCM_SK"),
        F.col("PD_AGNT_ID").alias("PD_AGNT_ID"),
        F.col("ERN_AGNT_ID").alias("ERN_AGNT_ID"),
        F.col("COMSN_PD_DT_SK").alias("COMSN_PD_DT_SK"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
        F.col("COMSN_INCM_SEQ_NO").alias("COMSN_INCM_SEQ_NO"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("COMSN_DTL_INCM_DISP_CD").alias("COMSN_DTL_INCM_DISP_CD"),
        F.col("COMSN_TYP_CD").alias("COMSN_TYP_CD"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AGNY_AGNT_SK").alias("AGNY_AGNT_SK"),
        F.col("BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
        F.col("svBillEntySk").alias("BILL_ENTY_SK"),
        F.col("BILL_SUM_SK").alias("BILL_SUM_SK"),
        F.col("svClsPlnSk").alias("CLS_PLN_SK"),
        F.col("COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
        F.col("ERN_AGNT_SK").alias("ERN_AGNT_SK"),
        F.col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("PD_AGNT_SK").alias("PD_AGNT_SK"),
        F.col("svProdSK").alias("PROD_SK"),
        F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("svAgntSttusCdSk").alias("AGNT_STTUS_CD_SK"),
        F.col("svAgntTierLvlCdSk").alias("AGNT_TIER_LVL_CD_SK"),
        F.col("svAgntBssSrcCdSk").alias("COMSN_BSS_SRC_CD_SK"),
        F.col("svComsnCovCatCdSk").alias("COMSN_COV_CAT_CD_SK"),
        F.col("svComsnDtlIncmCdSk").alias("COMSN_DTL_INCM_DISP_CD_SK"),
        F.col("svComsnMthdglyCdSk").alias("COMSN_MTHDLGY_TYP_CD_SK"),
        F.col("svComsnTypCdSk").alias("COMSN_TYP_CD_SK"),
        F.col("svCtCatTypCdSk").alias("CT_TYP_CD_SK"),
        F.col("svFundCatCdSk").alias("FUND_CAT_CD_SK"),
        F.col("COMSN_PD_YR_MO").alias("COMSN_PD_YR_MO"),
        F.col("CONT_COV_STRT_DT_SK").alias("CONT_COV_STRT_DT_SK"),
        F.col("COV_END_DT_SK").alias("COV_END_DT_SK"),
        F.col("COV_PLN_YR").alias("COV_PLN_YR"),
        F.col("DP_CANC_YR_MO").alias("DP_CANC_YR_MO"),
        F.col("PLN_YR_COV_EFF_DT_SK").alias("PLN_YR_COV_EFF_DT_SK"),
        F.col("PRM_PD_TO_DT_SK").alias("PRM_PD_TO_DT_SK"),
        F.col("COMSN_BSS_AMT").alias("COMSN_BSS_AMT"),
        F.col("COMSN_RATE_AMT").alias("COMSN_RATE_AMT"),
        F.col("PD_COMSN_AMT").alias("PD_COMSN_AMT"),
        F.col("BILL_COMSN_PRM_PCT").alias("BILL_COMSN_PRM_PCT"),
        F.col("COMSN_AGMNT_SCHD_FCTR").alias("COMSN_AGMNT_SCHD_FCTR"),
        F.col("COMSN_RATE_PCT").alias("COMSN_RATE_PCT"),
        F.col("CNTR_CT").alias("CNTR_CT"),
        F.col("MBR_CT").alias("MBR_CT"),
        F.col("SRC_TRANS_TYP_TX").alias("SRC_TRANS_TYP_TX"),
        F.col("ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
        F.col("ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
        F.col("AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
        F.col("CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
        F.col("CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
        F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
        F.col("COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
        F.col("COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
        F.col("CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
        F.col("CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
        F.col("ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
        F.col("MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
        F.col("MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
        F.col("MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
        F.col("TRANS_CNTR_CT").alias("TRANS_CNTR_CT"),
        F.col("TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD")
    )
)

df_Recycle = (
    df_ForeignKey_base
    .filter(F.col("ErrCount") > 0)
    .select(
        F.expr("GetRecycleKey(COMSN_INCM_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("COMSN_INCM_SK").alias("COMSN_INCM_SK"),
        F.col("PD_AGNT_ID").alias("PD_AGNT_ID"),
        F.col("ERN_AGNT_ID").alias("ERN_AGNT_ID"),
        F.col("COMSN_PD_DT_SK").alias("COMSN_PD_DT_SK"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
        F.col("COMSN_INCM_SEQ_NO").alias("COMSN_INCM_SEQ_NO"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("COMSN_DTL_INCM_DISP_CD").alias("COMSN_DTL_INCM_DISP_CD"),
        F.col("COMSN_TYP_CD").alias("COMSN_TYP_CD"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AGNY_AGNT_SK").alias("AGNY_AGNT_SK"),
        F.col("BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
        F.col("svBillEntySk").alias("BILL_ENTY_SK"),
        F.col("BILL_SUM_SK").alias("BILL_SUM_SK"),
        F.col("svClsPlnSk").alias("CLS_PLN_SK"),
        F.col("COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
        F.col("ERN_AGNT_SK").alias("ERN_AGNT_SK"),
        F.col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("PD_AGNT_SK").alias("PD_AGNT_SK"),
        F.col("svProdSK").alias("PROD_SK"),
        F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("svAgntSttusCdSk").alias("AGNT_STTUS_CD_SK"),
        F.col("svAgntTierLvlCdSk").alias("AGNT_TIER_LVL_CD_SK"),
        F.col("svAgntBssSrcCdSk").alias("COMSN_BSS_SRC_CD_SK"),
        F.col("svComsnCovCatCdSk").alias("COMSN_COV_CAT_CD_SK"),
        F.col("svComsnDtlIncmCdSk").alias("COMSN_DTL_INCM_DISP_CD_SK"),
        F.col("svComsnMthdglyCdSk").alias("COMSN_MTHDLGY_TYP_CD_SK"),
        F.col("svComsnTypCdSk").alias("COMSN_TYP_CD_SK"),
        F.col("svCtCatTypCdSk").alias("CT_TYP_CD_SK"),
        F.col("svFundCatCdSk").alias("FUND_CAT_CD_SK"),
        F.col("COMSN_PD_YR_MO").alias("COMSN_PD_YR_MO"),
        F.col("CONT_COV_STRT_DT_SK").alias("CONT_COV_STRT_DT_SK"),
        F.col("COV_END_DT_SK").alias("COV_END_DT_SK"),
        F.col("COV_PLN_YR").alias("COV_PLN_YR"),
        F.col("DP_CANC_YR_MO").alias("DP_CANC_YR_MO"),
        F.col("PLN_YR_COV_EFF_DT_SK").alias("PLN_YR_COV_EFF_DT_SK"),
        F.col("PRM_PD_TO_DT_SK").alias("PRM_PD_TO_DT_SK"),
        F.col("COMSN_BSS_AMT").alias("COMSN_BSS_AMT"),
        F.col("COMSN_RATE_AMT").alias("COMSN_RATE_AMT"),
        F.col("PD_COMSN_AMT").alias("PD_COMSN_AMT"),
        F.col("BILL_COMSN_PRM_PCT").alias("BILL_COMSN_PRM_PCT"),
        F.col("COMSN_AGMNT_SCHD_FCTR").alias("COMSN_AGMNT_SCHD_FCTR"),
        F.col("COMSN_RATE_PCT").alias("COMSN_RATE_PCT"),
        F.col("CNTR_CT").alias("CNTR_CT"),
        F.col("MBR_CT").alias("MBR_CT"),
        F.col("SRC_TRANS_TYP_TX").alias("SRC_TRANS_TYP_TX"),
        F.col("ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
        F.col("ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
        F.col("AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
        F.col("CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
        F.col("CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
        F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
        F.col("COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
        F.col("COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
        F.col("CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
        F.col("CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
        F.col("ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
        F.col("MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
        F.col("MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
        F.col("MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
        F.col("TRANS_CNTR_CT").alias("TRANS_CNTR_CT"),
        F.col("TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD")
    )
)

# Prepare the "Recycle" output with rpad if char/varchar and length is known
df_recycle_out = df_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("COMSN_INCM_SK"),
    F.col("PD_AGNT_ID"),
    F.col("ERN_AGNT_ID"),
    F.rpad(F.col("COMSN_PD_DT_SK"),10," ").alias("COMSN_PD_DT_SK"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"),10," ").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("COMSN_INCM_SEQ_NO"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("COMSN_DTL_INCM_DISP_CD"),
    F.col("COMSN_TYP_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNY_AGNT_SK"),
    F.col("BILL_INCM_RCPT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("BILL_SUM_SK"),
    F.col("CLS_PLN_SK"),
    F.col("COMSN_AGMNT_SK"),
    F.col("ERN_AGNT_SK"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("PD_AGNT_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("AGNT_STTUS_CD_SK"),
    F.col("AGNT_TIER_LVL_CD_SK"),
    F.col("COMSN_BSS_SRC_CD_SK"),
    F.col("COMSN_COV_CAT_CD_SK"),
    F.col("COMSN_DTL_INCM_DISP_CD_SK"),
    F.col("COMSN_MTHDLGY_TYP_CD_SK"),
    F.col("COMSN_TYP_CD_SK"),
    F.col("CT_TYP_CD_SK"),
    F.col("FUND_CAT_CD_SK"),
    F.rpad(F.col("COMSN_PD_YR_MO"),6," ").alias("COMSN_PD_YR_MO"),
    F.rpad(F.col("CONT_COV_STRT_DT_SK"),10," ").alias("CONT_COV_STRT_DT_SK"),
    F.rpad(F.col("COV_END_DT_SK"),10," ").alias("COV_END_DT_SK"),
    F.rpad(F.col("COV_PLN_YR"),4," ").alias("COV_PLN_YR"),
    F.rpad(F.col("DP_CANC_YR_MO"),6," ").alias("DP_CANC_YR_MO"),
    F.rpad(F.col("PLN_YR_COV_EFF_DT_SK"),10," ").alias("PLN_YR_COV_EFF_DT_SK"),
    F.rpad(F.col("PRM_PD_TO_DT_SK"),10," ").alias("PRM_PD_TO_DT_SK"),
    F.col("COMSN_BSS_AMT"),
    F.col("COMSN_RATE_AMT"),
    F.col("PD_COMSN_AMT"),
    F.col("BILL_COMSN_PRM_PCT"),
    F.col("COMSN_AGMNT_SCHD_FCTR"),
    F.col("COMSN_RATE_PCT"),
    F.col("CNTR_CT"),
    F.col("MBR_CT"),
    F.col("SRC_TRANS_TYP_TX"),
    F.col("ACA_BUS_SBSDY_TYP_ID"),
    F.col("ACA_PRM_SBSDY_AMT"),
    F.col("AGNY_HIER_LVL_ID"),
    F.col("CMS_CUST_POL_ID"),
    F.col("CMS_PLN_BNF_PCKG_ID"),
    F.rpad(F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN"),1," ").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("COMSN_PAYOUT_TYP_ID"),
    F.rpad(F.col("COMSN_RETROACTV_ADJ_IN"),1," ").alias("COMSN_RETROACTV_ADJ_IN"),
    F.col("CUST_BUS_TYP_ID"),
    F.col("CUST_BUS_SUBTYP_ID"),
    F.col("ICM_COMSN_PAYE_ID"),
    F.col("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("MCARE_BNFCRY_ID"),
    F.col("MCARE_ENR_TYP_ID"),
    F.col("TRANS_CNTR_CT"),
    F.col("TRNSMSN_SRC_SYS_CD")
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK_base = df_ForeignKey_base.limit(1)
df_DefaultUNK = df_DefaultUNK_base.select(
    F.lit(0).alias("COMSN_INCM_SK"),
    F.lit("UNK").alias("PD_AGNT_ID"),
    F.lit("UNK").alias("ERN_AGNT_ID"),
    F.lit("UNK").alias("COMSN_PD_DT_SK"),
    F.lit(0).alias("BILL_ENTY_UNIQ_KEY"),
    F.lit("UNK").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.lit(0).alias("COMSN_INCM_SEQ_NO"),
    F.lit("UNK").alias("CLS_PLN_ID"),
    F.lit("UNK").alias("PROD_ID"),
    F.lit("UNK").alias("COMSN_DTL_INCM_DISP_CD"),
    F.lit("UNK").alias("COMSN_TYP_CD"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("AGNY_AGNT_SK"),
    F.lit(0).alias("BILL_INCM_RCPT_SK"),
    F.lit(0).alias("BILL_ENTY_SK"),
    F.lit(0).alias("BILL_SUM_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("COMSN_AGMNT_SK"),
    F.lit(0).alias("ERN_AGNT_SK"),
    F.lit(0).alias("EXPRNC_CAT_SK"),
    F.lit(0).alias("FNCL_LOB_SK"),
    F.lit(0).alias("PD_AGNT_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("PROD_SH_NM_SK"),
    F.lit(0).alias("AGNT_STTUS_CD_SK"),
    F.lit(0).alias("AGNT_TIER_LVL_CD_SK"),
    F.lit(0).alias("COMSN_BSS_SRC_CD_SK"),
    F.lit(0).alias("COMSN_COV_CAT_CD_SK"),
    F.lit(0).alias("COMSN_DTL_INCM_DISP_CD_SK"),
    F.lit(0).alias("COMSN_MTHDLGY_TYP_CD_SK"),
    F.lit(0).alias("COMSN_TYP_CD_SK"),
    F.lit(0).alias("CT_TYP_CD_SK"),
    F.lit(0).alias("FUND_CAT_CD_SK"),
    F.lit("UNK").alias("COMSN_PD_YR_MO"),
    F.lit("UNK").alias("CONT_COV_STRT_DT_SK"),
    F.lit("UNK").alias("COV_END_DT_SK"),
    F.lit("UNK").alias("COV_PLN_YR"),
    F.lit("UNK").alias("DP_CANC_YR_MO"),
    F.lit("UNK").alias("PLN_YR_COV_EFF_DT_SK"),
    F.lit("UNK").alias("PRM_PD_TO_DT_SK"),
    F.lit(0).alias("COMSN_BSS_AMT"),
    F.lit(0).alias("COMSN_RATE_AMT"),
    F.lit(0).alias("PD_COMSN_AMT"),
    F.lit(0).alias("BILL_COMSN_PRM_PCT"),
    F.lit(0).alias("COMSN_AGMNT_SCHD_FCTR"),
    F.lit(0).alias("COMSN_RATE_PCT"),
    F.lit(0).alias("CNTR_CT"),
    F.lit(0).alias("MBR_CT"),
    F.lit("UNK").alias("SRC_TRANS_TYP_TX"),
    F.lit("UNK").alias("ACA_BUS_SBSDY_TYP_ID"),
    F.lit(0).alias("ACA_PRM_SBSDY_AMT"),
    F.lit("UNK").alias("AGNY_HIER_LVL_ID"),
    F.lit("UNK").alias("CMS_CUST_POL_ID"),
    F.lit("UNK").alias("CMS_PLN_BNF_PCKG_ID"),
    F.lit("0").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.lit("UNK").alias("COMSN_PAYOUT_TYP_ID"),
    F.lit("0").alias("COMSN_RETROACTV_ADJ_IN"),
    F.lit("UNK").alias("CUST_BUS_TYP_ID"),
    F.lit("UNK").alias("CUST_BUS_SUBTYP_ID"),
    F.lit("UNK").alias("ICM_COMSN_PAYE_ID"),
    F.lit("UNK").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.lit("UNK").alias("MCARE_BNFCRY_ID"),
    F.lit("UNK").alias("MCARE_ENR_TYP_ID"),
    F.lit(0).alias("TRANS_CNTR_CT"),
    F.lit("UNK").alias("TRNSMSN_SRC_SYS_CD")
)

df_DefaultNA_base = df_ForeignKey_base.limit(1)
df_DefaultNA = df_DefaultNA_base.select(
    F.lit(1).alias("COMSN_INCM_SK"),
    F.lit("NA").alias("PD_AGNT_ID"),
    F.lit("NA").alias("ERN_AGNT_ID"),
    F.lit("NA").alias("COMSN_PD_DT_SK"),
    F.lit(1).alias("BILL_ENTY_UNIQ_KEY"),
    F.lit("NA").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.lit(1).alias("COMSN_INCM_SEQ_NO"),
    F.lit("NA").alias("CLS_PLN_ID"),
    F.lit("NA").alias("PROD_ID"),
    F.lit("NA").alias("COMSN_DTL_INCM_DISP_CD"),
    F.lit("NA").alias("COMSN_TYP_CD"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("AGNY_AGNT_SK"),
    F.lit(1).alias("BILL_INCM_RCPT_SK"),
    F.lit(1).alias("BILL_ENTY_SK"),
    F.lit(1).alias("BILL_SUM_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("COMSN_AGMNT_SK"),
    F.lit(1).alias("ERN_AGNT_SK"),
    F.lit(1).alias("EXPRNC_CAT_SK"),
    F.lit(1).alias("FNCL_LOB_SK"),
    F.lit(1).alias("PD_AGNT_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("PROD_SH_NM_SK"),
    F.lit(1).alias("AGNT_STTUS_CD_SK"),
    F.lit(1).alias("AGNT_TIER_LVL_CD_SK"),
    F.lit(1).alias("COMSN_BSS_SRC_CD_SK"),
    F.lit(1).alias("COMSN_COV_CAT_CD_SK"),
    F.lit(1).alias("COMSN_DTL_INCM_DISP_CD_SK"),
    F.lit(1).alias("COMSN_MTHDLGY_TYP_CD_SK"),
    F.lit(1).alias("COMSN_TYP_CD_SK"),
    F.lit(1).alias("CT_TYP_CD_SK"),
    F.lit(1).alias("FUND_CAT_CD_SK"),
    F.lit("NA").alias("COMSN_PD_YR_MO"),
    F.lit("NA").alias("CONT_COV_STRT_DT_SK"),
    F.lit("NA").alias("COV_END_DT_SK"),
    F.lit("NA").alias("COV_PLN_YR"),
    F.lit("NA").alias("DP_CANC_YR_MO"),
    F.lit("NA").alias("PLN_YR_COV_EFF_DT_SK"),
    F.lit("NA").alias("PRM_PD_TO_DT_SK"),
    F.lit(1).alias("COMSN_BSS_AMT"),
    F.lit(1).alias("COMSN_RATE_AMT"),
    F.lit(0).alias("PD_COMSN_AMT"),
    F.lit(1).alias("BILL_COMSN_PRM_PCT"),
    F.lit(1).alias("COMSN_AGMNT_SCHD_FCTR"),
    F.lit(1).alias("COMSN_RATE_PCT"),
    F.lit(1).alias("CNTR_CT"),
    F.lit(1).alias("MBR_CT"),
    F.lit("NA").alias("SRC_TRANS_TYP_TX"),
    F.lit("NA").alias("ACA_BUS_SBSDY_TYP_ID"),
    F.lit(1).alias("ACA_PRM_SBSDY_AMT"),
    F.lit("NA").alias("AGNY_HIER_LVL_ID"),
    F.lit("NA").alias("CMS_CUST_POL_ID"),
    F.lit("NA").alias("CMS_PLN_BNF_PCKG_ID"),
    F.lit("1").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.lit("NA").alias("COMSN_PAYOUT_TYP_ID"),
    F.lit("1").alias("COMSN_RETROACTV_ADJ_IN"),
    F.lit("NA").alias("CUST_BUS_TYP_ID"),
    F.lit("NA").alias("CUST_BUS_SUBTYP_ID"),
    F.lit("NA").alias("ICM_COMSN_PAYE_ID"),
    F.lit("NA").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.lit("NA").alias("MCARE_BNFCRY_ID"),
    F.lit("NA").alias("MCARE_ENR_TYP_ID"),
    F.lit(1).alias("TRANS_CNTR_CT"),
    F.lit("NA").alias("TRNSMSN_SRC_SYS_CD")
)

df_Collector = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_COMSN_INCM = df_Collector.select(
    F.col("COMSN_INCM_SK"),
    F.col("PD_AGNT_ID"),
    F.col("ERN_AGNT_ID"),
    F.rpad(F.col("COMSN_PD_DT_SK"),10," ").alias("COMSN_PD_DT_SK"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"),10," ").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("COMSN_INCM_SEQ_NO"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("COMSN_DTL_INCM_DISP_CD"),
    F.col("COMSN_TYP_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNY_AGNT_SK"),
    F.col("BILL_INCM_RCPT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("BILL_SUM_SK"),
    F.col("CLS_PLN_SK"),
    F.col("COMSN_AGMNT_SK"),
    F.col("ERN_AGNT_SK"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("PD_AGNT_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("AGNT_STTUS_CD_SK"),
    F.col("AGNT_TIER_LVL_CD_SK"),
    F.col("COMSN_BSS_SRC_CD_SK"),
    F.col("COMSN_COV_CAT_CD_SK"),
    F.col("COMSN_DTL_INCM_DISP_CD_SK"),
    F.col("COMSN_MTHDLGY_TYP_CD_SK"),
    F.col("COMSN_TYP_CD_SK"),
    F.col("CT_TYP_CD_SK"),
    F.col("FUND_CAT_CD_SK"),
    F.rpad(F.col("COMSN_PD_YR_MO"),6," ").alias("COMSN_PD_YR_MO"),
    F.rpad(F.col("CONT_COV_STRT_DT_SK"),10," ").alias("CONT_COV_STRT_DT_SK"),
    F.rpad(F.col("COV_END_DT_SK"),10," ").alias("COV_END_DT_SK"),
    F.rpad(F.col("COV_PLN_YR"),4," ").alias("COV_PLN_YR"),
    F.rpad(F.col("DP_CANC_YR_MO"),6," ").alias("DP_CANC_YR_MO"),
    F.rpad(F.col("PLN_YR_COV_EFF_DT_SK"),10," ").alias("PLN_YR_COV_EFF_DT_SK"),
    F.rpad(F.col("PRM_PD_TO_DT_SK"),10," ").alias("PRM_PD_TO_DT_SK"),
    F.col("COMSN_BSS_AMT"),
    F.col("COMSN_RATE_AMT"),
    F.col("PD_COMSN_AMT"),
    F.col("BILL_COMSN_PRM_PCT"),
    F.col("COMSN_AGMNT_SCHD_FCTR"),
    F.col("COMSN_RATE_PCT"),
    F.col("CNTR_CT"),
    F.col("MBR_CT"),
    F.col("SRC_TRANS_TYP_TX"),
    F.col("ACA_BUS_SBSDY_TYP_ID"),
    F.col("ACA_PRM_SBSDY_AMT"),
    F.col("AGNY_HIER_LVL_ID"),
    F.col("CMS_CUST_POL_ID"),
    F.col("CMS_PLN_BNF_PCKG_ID"),
    F.rpad(F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN"),1," ").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("COMSN_PAYOUT_TYP_ID"),
    F.rpad(F.col("COMSN_RETROACTV_ADJ_IN"),1," ").alias("COMSN_RETROACTV_ADJ_IN"),
    F.col("CUST_BUS_TYP_ID"),
    F.col("CUST_BUS_SUBTYP_ID"),
    F.col("ICM_COMSN_PAYE_ID"),
    F.col("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("MCARE_BNFCRY_ID"),
    F.col("MCARE_ENR_TYP_ID"),
    F.col("TRANS_CNTR_CT"),
    F.col("TRNSMSN_SRC_SYS_CD")
)

write_files(
    df_COMSN_INCM,
    f"{adls_path}/load/COMSN_INCM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)