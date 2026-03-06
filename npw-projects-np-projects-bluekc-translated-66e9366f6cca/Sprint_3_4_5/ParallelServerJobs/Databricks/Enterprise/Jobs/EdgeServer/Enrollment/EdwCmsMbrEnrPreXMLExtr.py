# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2023 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:  Added below 9 new fields to target files insuredMemberRace, insuredMemberEthnicity, zipCode, federalAPTC, statePremiumSubsidy, stateCSR, ICHRA_QSEHRA, QSEHRA_Spousal, QSEHRA_Medical
# MAGIC JOB NAME: EdwCmsMbrEnrPreXMLExtr
# MAGIC CALLED BY:  EdgeServerEdwCmsEnrSeq
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Ticket #\(9)Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed||
# MAGIC =====================================================================================================================================
# MAGIC Harsha Ravuri\(9)2023-10-03\(9)US#597589\(9)Original programming\(9)\(9)EnterpriseDev2                        Jeyaprasanna          2023-10-08
# MAGIC Harsha Ravuri\(9)2024-02-21\(9)US#612091\(9)Removed unwanted zipcode logic\(9)EnterpriseDev2                        Jeyaprasanna          2024-04-18

# MAGIC Filters SUB and DEP/SPOUSE
# MAGIC Gets ATPC indicator from MBR_EXCH_D table and if a member and QHP combo have multiple indicators in the same enrollment period then code picks where QHP_APTC_IN = Y.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ProdIn = get_widget_value('ProdIn','')
CurrDate = get_widget_value('CurrDate','')
State = get_widget_value('State','')
RiskAdjYr = get_widget_value('RiskAdjYr','')
BeginDt = get_widget_value('BeginDt','')
EndDate = get_widget_value('EndDate','')
QHPID = get_widget_value('QHPID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

MBR_GNRL_LOC_HIST_D_query = f"""
SELECT Distinct mbr.MBR_UNIQ_KEY,
       mbr.GRP_ID
FROM {EDWOwner}.MBR_D mbr,
     {EDWOwner}.MBR_ENR_D mbrEnr,
     {EDWOwner}.GRP_D grp,
     {EDWOwner}.MBR_ENR_QHP_D MBR_QHP,
     {EDWOwner}.SUB_D sub,
     {EDWOwner}.w_edge_mbr_elig_extr we
WHERE mbr.MBR_SK = mbrEnr.MBR_SK
  AND mbr.GRP_SK = grp.GRP_SK
  AND MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
  AND MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
  AND MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
  AND mbrEnr.MBR_ENR_ELIG_IN = 'Y'
  AND mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
  AND MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
  AND mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndDate}'
  AND mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginDt}'
  AND mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
  AND mbrEnr.CLS_ID <> 'MHIP'
  AND MBR_QHP.QHP_ID <> 'NA'
  AND MBR_QHP.QHP_ID LIKE '{QHPID}'
  AND mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
  AND left(mbr.MBR_RELSHP_NM,3) = 'SUB'
  AND we.SUB_INDV_BE_KEY = we.mbr_indv_be_key
  AND we.sub_indv_be_key = mbr.mbr_indv_be_key
"""

df_MBR_GNRL_LOC_HIST_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", MBR_GNRL_LOC_HIST_D_query)
    .load()
)

schema_SeqFile = StructType([
    StructField("fileIdentifier", StringType(), False),
    StructField("executionZoneCode", StringType(), False),
    StructField("interfaceControlReleaseNumber", StringType(), False),
    StructField("generationDateTime", StringType(), False),
    StructField("submissionTypeCode", StringType(), False),
    StructField("insuredMemberTotalQuantity", StringType(), False),
    StructField("insuredMemberProfileTotalQuantity", StringType(), False),
    StructField("EnrollmentIssuerrecordIdentifier", StringType(), False),
    StructField("EnrollmentIssuerissuerIdentifier", StringType(), False),
    StructField("issuerInsuredMemberTotalQuantity", StringType(), False),
    StructField("issuerInsuredMemberProfileTotalQuantity", StringType(), False),
    StructField("includedinsuredMemberrecordIdentifier", StringType(), False),
    StructField("insuredMemberIdentifier", StringType(), False),
    StructField("insuredMemberBirthDate", StringType(), False),
    StructField("insuredMemberGenderCode", StringType(), False),
    StructField("includedinsuredMemberProfilerecordIdentifier", StringType(), False),
    StructField("subscriberIndicator", StringType(), False),
    StructField("subscriberIdentifier", StringType(), False),
    StructField("insurancePlanIdentifier", StringType(), False),
    StructField("coverageStartDate", StringType(), False),
    StructField("coverageEndDate", StringType(), False),
    StructField("enrollmentMaintenanceTypeCode", StringType(), False),
    StructField("insurancePlanPremiumAmount", StringType(), False),
    StructField("rateAreaIdentifier", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("RISK_ADJ_YR", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_RELSHP_CD", StringType(), False),
    StructField("SUB_ADDR_ZIP_CD_5", StringType(), True)
])

df_SeqFile = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_SeqFile)
    .load(f"{adls_path_publish}/external/Edge_RA_Enrollment_Hist_{State}.txt")
)

df_SUB = df_SeqFile

df_sub1 = df_SUB.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("executionZoneCode").alias("executionZoneCode"),
    F.col("interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("generationDateTime").alias("generationDateTime"),
    F.col("submissionTypeCode").alias("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator").alias("subscriberIndicator"),
    F.col("subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("coverageStartDate").alias("coverageStartDate"),
    F.col("coverageEndDate").alias("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_ADJ_YR").alias("RISK_ADJ_YR"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

df_DEP_SPS = df_SUB.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("executionZoneCode").alias("executionZoneCode"),
    F.col("interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("generationDateTime").alias("generationDateTime"),
    F.col("submissionTypeCode").alias("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator").alias("subscriberIndicator"),
    F.col("subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("coverageStartDate").alias("coverageStartDate"),
    F.col("coverageEndDate").alias("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_ADJ_YR").alias("RISK_ADJ_YR"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

df_DEP = df_DEP_SPS.select(
    F.col("fileIdentifier"),
    F.col("executionZoneCode"),
    F.col("interfaceControlReleaseNumber"),
    F.col("generationDateTime"),
    F.col("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier"),
    F.col("insuredMemberBirthDate"),
    F.col("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator"),
    F.col("subscriberIdentifier"),
    F.col("insurancePlanIdentifier"),
    F.col("coverageStartDate"),
    F.col("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier"),
    F.col("MBR_UNIQ_KEY"),
    F.col("RISK_ADJ_YR"),
    F.col("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD"),
    F.lit('n').alias("YEAR"),
    F.lit(None).alias("QHP_APTC_IN"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

df_ALL = df_sub1.select(
    F.col("fileIdentifier"),
    F.col("executionZoneCode"),
    F.col("interfaceControlReleaseNumber"),
    F.col("generationDateTime"),
    F.col("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier"),
    F.col("insuredMemberBirthDate"),
    F.col("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator"),
    F.col("subscriberIdentifier"),
    F.col("insurancePlanIdentifier"),
    F.col("coverageStartDate"),
    F.col("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier"),
    F.col("MBR_UNIQ_KEY"),
    F.col("RISK_ADJ_YR"),
    F.col("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

df_stpc_chk = df_sub1.select(
    F.col("includedinsuredMemberProfilerecordIdentifier"),
    F.col("MBR_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY"),
    F.col("insurancePlanIdentifier"),
    F.col("coverageStartDate"),
    F.col("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode"),
    F.col("SUB_ADDR_ZIP_CD_5"),
    F.lit(RiskAdjYr).alias("YEAR")
)

MBR_EXCH_D_query_converted = f"""
SELECT DISTINCT MEXD.MBR_UNIQ_KEY,
       MEXD.QHP_ID,
       MEXD.QHP_APTC_IN,
       MEXD.MBR_EXCH_TERM_DT_SK
FROM {EDWOwner}.MBR_EXCH_D MEXD
INNER JOIN {EDWOwner}.MBR_ENR_D MED
  ON MEXD.MBR_UNIQ_KEY = MED.MBR_UNIQ_KEY
  AND MED.PROD_SH_NM IN ('PCB','PC','BCARE','BLUE-ACCESS','BLUE-SELECT','BLUESELECT+')
  AND MED.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
  AND MED.MBR_ENR_ELIG_IN = 'Y'
  AND MED.CLS_ID <> 'MHIP'
  AND MED.MBR_ENR_EFF_DT_SK <= '{RiskAdjYr}-12-31'
  AND MED.MBR_ENR_TERM_DT_SK >= DATEADD(year,-1,CAST('{RiskAdjYr}-01-01' as date))
WHERE MEXD.QHP_ID <> 'UNK'
"""

df_MBR_EXCH_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", MBR_EXCH_D_query_converted)
    .load()
)

df_atpc_rm = df_MBR_EXCH_D.select(
    F.col("MBR_UNIQ_KEY"),
    F.expr("substring(QHP_ID,1,16)").alias("insurancePlanIdentifier"),
    F.col("QHP_APTC_IN"),
    F.when(
        F.col("MBR_EXCH_TERM_DT_SK") >= F.lit(f"{RiskAdjYr}-01-01"),
        F.lit(RiskAdjYr)
    ).otherwise(F.lit(str(int(RiskAdjYr) - 1))).alias("YEAR")
)

df_removed_duplicates = dedup_sort(
    df_atpc_rm,
    partition_cols=["MBR_UNIQ_KEY", "insurancePlanIdentifier", "YEAR"],
    sort_cols=[("MBR_UNIQ_KEY","A"),("insurancePlanIdentifier","A"),("YEAR","A"),("QHP_APTC_IN","D")]
)

df_JN_ATPC = df_stpc_chk.alias("stpc_chk").join(
    df_removed_duplicates.alias("atpc"),
    [
        F.col("stpc_chk.MBR_UNIQ_KEY") == F.col("atpc.MBR_UNIQ_KEY"),
        F.col("stpc_chk.insurancePlanIdentifier") == F.col("atpc.insurancePlanIdentifier"),
        F.col("stpc_chk.YEAR") == F.col("atpc.YEAR")
    ],
    how="left"
).select(
    F.col("stpc_chk.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("stpc_chk.insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("atpc.QHP_APTC_IN").alias("QHP_APTC_IN"),
    F.col("stpc_chk.YEAR").alias("YEAR"),
    F.col("stpc_chk.includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("stpc_chk.coverageStartDate").alias("coverageStartDate"),
    F.col("stpc_chk.coverageEndDate").alias("coverageEndDate"),
    F.col("stpc_chk.SUB_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5")
)

df_atpc_A = df_JN_ATPC.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("YEAR"),
    F.col("includedinsuredMemberProfilerecordIdentifier"),
    F.col("coverageStartDate"),
    F.col("coverageEndDate"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5"),
    F.when(F.trim(F.col("QHP_APTC_IN")) == 'Y', '1')
     .otherwise(
       F.when(F.trim(F.col("QHP_APTC_IN")) == 'N','2').otherwise('0')
     ).alias("QHP_APTC_IN")
)

df_SUB_SUB = df_atpc_A.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("YEAR"),
    F.col("includedinsuredMemberProfilerecordIdentifier"),
    F.col("coverageStartDate"),
    F.col("coverageEndDate"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5"),
    F.col("QHP_APTC_IN")
)

df_jooin_all = df_ALL.alias("ALL").join(
    df_SUB_SUB.alias("SUB_SUB"),
    [
        F.col("ALL.includedinsuredMemberProfilerecordIdentifier") == F.col("SUB_SUB.includedinsuredMemberProfilerecordIdentifier"),
        F.col("ALL.MBR_UNIQ_KEY") == F.col("SUB_SUB.MBR_UNIQ_KEY"),
        F.col("ALL.coverageStartDate") == F.col("SUB_SUB.coverageStartDate"),
        F.col("ALL.coverageEndDate") == F.col("SUB_SUB.coverageEndDate")
    ],
    how="left"
).select(
    F.col("ALL.fileIdentifier").alias("fileIdentifier"),
    F.col("ALL.executionZoneCode").alias("executionZoneCode"),
    F.col("ALL.interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("ALL.generationDateTime").alias("generationDateTime"),
    F.col("ALL.submissionTypeCode").alias("submissionTypeCode"),
    F.col("ALL.insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("ALL.insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("ALL.EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("ALL.EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("ALL.issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("ALL.issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("ALL.includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("ALL.insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("ALL.insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("ALL.insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("ALL.includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("ALL.subscriberIndicator").alias("subscriberIndicator"),
    F.col("ALL.subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("ALL.insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("ALL.coverageStartDate").alias("coverageStartDate"),
    F.col("ALL.coverageEndDate").alias("coverageEndDate"),
    F.col("ALL.enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("ALL.insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("ALL.rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("ALL.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("ALL.RISK_ADJ_YR").alias("RISK_ADJ_YR"),
    F.col("ALL.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("ALL.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("SUB_SUB.YEAR").alias("YEAR"),
    F.col("SUB_SUB.MBR_HOME_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("SUB_SUB.QHP_APTC_IN").alias("QHP_APTC_IN")
)

df_funnel = df_DEP.unionByName(df_jooin_all)

df_join_757 = df_funnel.alias("GrpID").join(
    df_MBR_GNRL_LOC_HIST_D.alias("Table1"),
    F.col("GrpID.MBR_UNIQ_KEY") == F.col("Table1.MBR_UNIQ_KEY"),
    how="left"
).select(
    F.col("GrpID.fileIdentifier").alias("fileIdentifier"),
    F.col("GrpID.executionZoneCode").alias("executionZoneCode"),
    F.col("GrpID.interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("GrpID.generationDateTime").alias("generationDateTime"),
    F.col("GrpID.submissionTypeCode").alias("submissionTypeCode"),
    F.col("GrpID.insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("GrpID.insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("GrpID.EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("GrpID.EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("GrpID.issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("GrpID.issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("GrpID.includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("GrpID.insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("GrpID.insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("GrpID.insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("GrpID.includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("GrpID.subscriberIndicator").alias("subscriberIndicator"),
    F.col("GrpID.subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("GrpID.insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("GrpID.coverageStartDate").alias("coverageStartDate"),
    F.col("GrpID.coverageEndDate").alias("coverageEndDate"),
    F.col("GrpID.enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("GrpID.insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("GrpID.rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("GrpID.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("GrpID.RISK_ADJ_YR").alias("RISK_ADJ_YR"),
    F.col("GrpID.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("GrpID.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("GrpID.YEAR").alias("YEAR"),
    F.col("GrpID.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("GrpID.QHP_APTC_IN").alias("QHP_APTC_IN"),
    F.col("Table1.GRP_ID").alias("GRP_ID")
)

df_tfm_clense = df_join_757

df_Enrollment_Prd_Hist = df_tfm_clense.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("executionZoneCode").alias("executionZoneCode"),
    F.col("interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("generationDateTime").alias("generationDateTime"),
    F.col("submissionTypeCode").alias("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator").alias("subscriberIndicator"),
    F.col("subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("coverageStartDate").alias("coverageStartDate"),
    F.col("coverageEndDate").alias("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("RISK_ADJ_YR"),4," ").alias("RISK_ADJ_YR"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.lit("00").alias("insuredMemberRace"),
    F.lit("00").alias("insuredMemberEthnicity"),
    F.when(F.col("MBR_RELSHP_CD") == "SUB", trim(F.col("SUB_ADDR_ZIP_CD_5"))).otherwise("").alias("zipCode"),
    F.when(
        (F.when(F.col("GRP_ID").isNotNull(), F.col("GRP_ID")).otherwise("")) == "10001000",
        F.when(F.col("MBR_RELSHP_CD")=="SUB", F.col("QHP_APTC_IN")).otherwise("")
    ).otherwise("").alias("federalAPTC"),
    F.when(
        (F.when(F.col("GRP_ID").isNotNull(), F.col("GRP_ID")).otherwise("")) == "10001000",
        F.when(F.col("MBR_RELSHP_CD")=="SUB", "0").otherwise("")
    ).otherwise("").alias("statePremiumSubsidy"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","0").otherwise("").alias("stateCSRtext"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","N").otherwise("").alias("ICHRA_QSEHRA"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","N").otherwise("").alias("QSEHRA_Spousal"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","N").otherwise("").alias("QSEHRA_Medical")
)

df_Enrollment = df_tfm_clense.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("executionZoneCode").alias("executionZoneCode"),
    F.col("interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("generationDateTime").alias("generationDateTime"),
    F.col("submissionTypeCode").alias("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator").alias("subscriberIndicator"),
    F.col("subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("coverageStartDate").alias("coverageStartDate"),
    F.col("coverageEndDate").alias("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("RISK_ADJ_YR"),4," ").alias("RISK_ADJ_YR"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.lit("00").alias("insuredMemberRace"),
    F.lit("00").alias("insuredMemberEthnicity"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB", trim(F.col("SUB_ADDR_ZIP_CD_5"))).otherwise("").alias("zipCode"),
    F.when(
        (F.when(F.col("GRP_ID").isNotNull(), F.col("GRP_ID")).otherwise("")) == "10001000",
        F.when(F.col("MBR_RELSHP_CD")=="SUB", F.trim(F.col("QHP_APTC_IN"))).otherwise("")
    ).otherwise("").alias("federalAPTC"),
    F.when(
        (F.when(F.col("GRP_ID").isNotNull(), F.col("GRP_ID")).otherwise("")) == "10001000",
        F.when(F.col("MBR_RELSHP_CD")=="SUB","0").otherwise("")
    ).otherwise("").alias("statePremiumSubsidy"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","0").otherwise("").alias("stateCSRtext"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","N").otherwise("").alias("ICHRA_QSEHRA"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","N").otherwise("").alias("QSEHRA_Spousal"),
    F.when(F.col("MBR_RELSHP_CD")=="SUB","N").otherwise("").alias("QSEHRA_Medical")
)

df_Enrollment_Prd_Hist = df_Enrollment_Prd_Hist.withColumn("QHP_APTC_IN", F.rpad(F.col("federalAPTC"),1," "))
df_Enrollment = df_Enrollment.withColumn("QHP_APTC_IN", F.rpad(F.col("federalAPTC"),1," "))

write_files(
    df_Enrollment_Prd_Hist,
    f"{adls_path_publish}/external/Edge_RA_Enrollment_Hist_{State}_final.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Enrollment,
    f"{adls_path_publish}/external/Edge_RA_Enrollment_{State}.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)