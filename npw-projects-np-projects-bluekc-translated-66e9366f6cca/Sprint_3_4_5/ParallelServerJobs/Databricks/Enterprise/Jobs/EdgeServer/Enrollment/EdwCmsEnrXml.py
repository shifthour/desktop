# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name: EdwCmsEnrXml
# MAGIC CALLED BY:  EdgeServerEdwCmsEnrSeq
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Nothing
# MAGIC              Previous Run Aborted:          Check for the existence of ErrCd Files on Unix and then Restart the Controller Job
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Altiris #\(9)\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed||
# MAGIC =====================================================================================================================================================================
# MAGIC Bhoomi Dasari\(9)2015-01-23\(9)5125 Risk Adjustment\(9)Original Programming\(9)\(9)\(9)\(9)\(9)EnterpriseNewDevl\(9)\(9)Kalyan Neelam\(9)2015-03-26
# MAGIC Raja Gummadi\(9)2015-07-30\(9)5125\(9)\(9)\(9)Added MBR_INDV_BE_KEY,\(9)RISK_ADJ_YR \(9)\(9)\(9)EnterpriseDev2\(9)\(9)Bhoomi Dasari\(9)07/30/2015
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)and SUB_INDV_BE_KEY columns                                                                                                            
# MAGIC Harsha Ravuri\(9)2023-10-03\(9)US#597589\(9)\(9)Added below 9 new fields to target files\(9)\(9)\(9)\(9)EnterpriseDev2                        Jeyaprasanna          2023-10-08
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)insuredMemberRace, insuredMemberEthnicity, zipCode, federalAPTC, statePremiumSubsidy, stateCSR, ICHRA_QSEHRA, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)QSEHRA_Spousal, QSEHRA_Medical

# MAGIC Job Name: EdgeServerPharmacyClaimSubmission
# MAGIC 
# MAGIC Description: This job will generate XML for EdgeServerPharmacyClaimSubmission.
# MAGIC 
# MAGIC Input: Source file with pharmacy claim data
# MAGIC Output: EdgeServerPharmacyClaimSubmission XML file with namespace declarations
# MAGIC Stage Name: XMLOutPhrmClimDtl
# MAGIC 
# MAGIC Desc: Generates Pharmacy Claim Detial  XML chunk with key on "File Identifier", "Record identifier", "Issuer identifier", "Insurance plan Record identifier" and "Insurance plan identifier" to handle the complexity of Pharmacy claim detail logic in XML
# MAGIC Stage Name: XMLOutPhrmClimInsPln
# MAGIC 
# MAGIC Desc: Generates Pharmacy Claim insurance plan XML chunk with key on "File Identifier", "Record identifier", "issurer identifier" to handle the complexity of pharmacy claim insurance plan logic in XML
# MAGIC Stage Name: SeqFile
# MAGIC 
# MAGIC Desc: Source file path need to be provided at run time as per the environment. 
# MAGIC Source file name was parameterised thru job parameter #INP_FL_NM#, which need to be provided at run time along with absolute path.
# MAGIC Stage Name: XML_PhrmClmSub
# MAGIC 
# MAGIC Desc: XML tags in description field are all defined appropriately as per XSD.
# MAGIC Target file name was parameterised thru job parameter #ITGT_XML#, which need to be provided at run time along with absolute path.
# MAGIC Data was sorted on "FileIdentifier" before landing as XML to maintain consistency with input data.
# MAGIC Stage Name: JnXML
# MAGIC 
# MAGIC Desc: Joins data from source and intermediate Pharmacy Claim Insurance Plan XML chunk on "File Identifier", "Record identifier" and "Issurer identifier" to handle the complexity of Pharmacy Claim insurance plan logic in XML
# MAGIC Stage Name: JnPhrmClmDtl
# MAGIC 
# MAGIC Desc: Joins data from source and intermediate Pharmacy Claim Detail XML chunk on "File Identifier", "Record identifier", "issurer identifier", "Insurance plan Record identifier" and "Insurance plan identifier" to handle the complexity of Pharmacy Claim detail logic in XML
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


APT_PHYSICAL_DATASET_BLOCK_SIZE = get_widget_value("$APT_PHYSICAL_DATASET_BLOCK_SIZE","")
APT_DEFAULT_TRANSPORT_BLOCK_SIZE = get_widget_value("$APT_DEFAULT_TRANSPORT_BLOCK_SIZE","")
APT_TSORT_STRESS_BLOCKSIZE = get_widget_value("$APT_TSORT_STRESS_BLOCKSIZE","")
State = get_widget_value("State","")

schema_SeqFile = StructType([
    StructField("fileIdentifier", StringType(), nullable=False),
    StructField("executionZoneCode", StringType(), nullable=False),
    StructField("interfaceControlReleaseNumber", StringType(), nullable=False),
    StructField("generationDateTime", StringType(), nullable=False),
    StructField("submissionTypeCode", StringType(), nullable=False),
    StructField("insuredMemberTotalQuantity", StringType(), nullable=False),
    StructField("insuredMemberProfileTotalQuantity", StringType(), nullable=False),
    StructField("EnrollmentIssuerrecordIdentifier", StringType(), nullable=False),
    StructField("EnrollmentIssuerissuerIdentifier", StringType(), nullable=False),
    StructField("issuerInsuredMemberTotalQuantity", StringType(), nullable=False),
    StructField("issuerInsuredMemberProfileTotalQuantity", StringType(), nullable=False),
    StructField("includedinsuredMemberrecordIdentifier", StringType(), nullable=False),
    StructField("insuredMemberIdentifier", StringType(), nullable=False),
    StructField("insuredMemberBirthDate", StringType(), nullable=False),
    StructField("insuredMemberGenderCode", StringType(), nullable=False),
    StructField("includedinsuredMemberProfilerecordIdentifier", StringType(), nullable=False),
    StructField("subscriberIndicator", StringType(), nullable=False),
    StructField("subscriberIdentifier", StringType(), nullable=False),
    StructField("insurancePlanIdentifier", StringType(), nullable=False),
    StructField("coverageStartDate", StringType(), nullable=False),
    StructField("coverageEndDate", StringType(), nullable=False),
    StructField("enrollmentMaintenanceTypeCode", StringType(), nullable=False),
    StructField("insurancePlanPremiumAmount", StringType(), nullable=False),
    StructField("rateAreaIdentifier", StringType(), nullable=True),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("RISK_ADJ_YR", StringType(), nullable=False),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("insuredMemberRace", StringType(), nullable=False),
    StructField("insuredMemberEthnicity", StringType(), nullable=False),
    StructField("zipCode", StringType(), nullable=True),
    StructField("federalAPTC", StringType(), nullable=True),
    StructField("statePremiumSubsidy", StringType(), nullable=False),
    StructField("stateCSR", StringType(), nullable=False),
    StructField("ICHRA_QSEHRA", StringType(), nullable=False),
    StructField("QSEHRA_Spousal", StringType(), nullable=False),
    StructField("QSEHRA_Medical", StringType(), nullable=False)
])

df_SeqFile = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("ignoreTrailingWhiteSpace", True)
    .schema(schema_SeqFile)
    .load(f"{adls_path_publish}/external/Edge_RA_Enrollment_{State}.txt")
)

df_Transformer_90 = df_SeqFile.select(
    trim(F.col("fileIdentifier")).alias("fileIdentifier"),
    trim(F.col("executionZoneCode")).alias("executionZoneCode"),
    trim(F.col("interfaceControlReleaseNumber")).alias("interfaceControlReleaseNumber"),
    trim(F.col("generationDateTime")).alias("generationDateTime"),
    trim(F.col("submissionTypeCode")).alias("submissionTypeCode"),
    trim(F.col("insuredMemberTotalQuantity")).alias("insuredMemberTotalQuantity"),
    trim(F.col("insuredMemberProfileTotalQuantity")).alias("insuredMemberProfileTotalQuantity"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    trim(F.col("EnrollmentIssuerissuerIdentifier")).alias("EnrollmentIssuerissuerIdentifier"),
    trim(F.col("issuerInsuredMemberTotalQuantity")).alias("issuerInsuredMemberTotalQuantity"),
    trim(F.col("issuerInsuredMemberProfileTotalQuantity")).alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    trim(F.col("insuredMemberIdentifier")).alias("insuredMemberIdentifier"),
    trim(F.col("insuredMemberBirthDate")).alias("insuredMemberBirthDate"),
    trim(F.col("insuredMemberGenderCode")).alias("insuredMemberGenderCode"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    trim(F.col("subscriberIndicator")).alias("subscriberIndicator"),
    trim(F.col("subscriberIdentifier")).alias("subscriberIdentifier"),
    trim(F.col("insurancePlanIdentifier")).alias("insurancePlanIdentifier"),
    trim(F.col("coverageStartDate")).alias("coverageStartDate"),
    trim(F.col("coverageEndDate")).alias("coverageEndDate"),
    trim(F.col("enrollmentMaintenanceTypeCode")).alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    trim(F.col("rateAreaIdentifier")).alias("rateAreaIdentifier"),
    F.col("insuredMemberRace").alias("insuredMemberRace"),
    F.col("insuredMemberEthnicity").alias("insuredMemberEthnicity"),
    F.col("zipCode").alias("zipCode"),
    F.col("federalAPTC").alias("federalAPTC"),
    F.col("statePremiumSubsidy").alias("statePremiumSubsidy"),
    F.col("stateCSR").alias("stateCSR"),
    F.col("ICHRA_QSEHRA").alias("ICHRA_QSEHRA"),
    F.col("QSEHRA_Spousal").alias("QSEHRA_Spousal"),
    F.col("QSEHRA_Medical").alias("QSEHRA_Medical")
)

df_CpSf_CpJnLnk = df_Transformer_90.select(
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
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier")
)

df_CpSf_XMLPhrmClmDtLnkIn = df_Transformer_90.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator").alias("subscriberIndicator"),
    F.col("subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("coverageStartDate").alias("coverageStartDate"),
    F.col("coverageEndDate").alias("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("zipCode").alias("zipCode"),
    F.col("federalAPTC").alias("federalAPTC"),
    F.col("statePremiumSubsidy").alias("statePremiumSubsidy"),
    F.col("stateCSR").alias("stateCSR"),
    F.col("ICHRA_QSEHRA").alias("ICHRA_QSEHRA"),
    F.col("QSEHRA_Spousal").alias("QSEHRA_Spousal"),
    F.col("QSEHRA_Medical").alias("QSEHRA_Medical")
)

df_CpSf_JnPhrmClmDtlLnkIn = df_Transformer_90.select(
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
    F.col("insuredMemberRace").alias("insuredMemberRace"),
    F.col("insuredMemberEthnicity").alias("insuredMemberEthnicity"),
    F.col("includedinsuredMemberProfilerecordIdentifier").alias("includedinsuredMemberProfilerecordIdentifier"),
    F.col("subscriberIndicator").alias("subscriberIndicator"),
    F.col("subscriberIdentifier").alias("subscriberIdentifier"),
    F.col("insurancePlanIdentifier").alias("insurancePlanIdentifier"),
    F.col("coverageStartDate").alias("coverageStartDate"),
    F.col("coverageEndDate").alias("coverageEndDate"),
    F.col("enrollmentMaintenanceTypeCode").alias("enrollmentMaintenanceTypeCode"),
    F.col("insurancePlanPremiumAmount").alias("insurancePlanPremiumAmount"),
    F.col("rateAreaIdentifier").alias("rateAreaIdentifier"),
    F.col("zipCode").alias("zipCode"),
    F.col("federalAPTC").alias("federalAPTC"),
    F.col("statePremiumSubsidy").alias("statePremiumSubsidy"),
    F.col("stateCSR").alias("stateCSR"),
    F.col("ICHRA_QSEHRA").alias("ICHRA_QSEHRA"),
    F.col("QSEHRA_Spousal").alias("QSEHRA_Spousal"),
    F.col("QSEHRA_Medical").alias("QSEHRA_Medical")
)

df_XMLOutPhrmClmDtl = df_CpSf_XMLPhrmClmDtLnkIn.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.lit(None).alias("includedMemberProfileDetail_XML_CHUNK")
)

df_JnPhrmClmDtl = df_CpSf_JnPhrmClmDtlLnkIn.alias("JnPhrmClmDtlLnkIn").join(
    df_XMLOutPhrmClmDtl.alias("PhrmClimDtlXMLOut"),
    [
        F.col("JnPhrmClmDtlLnkIn.fileIdentifier") == F.col("PhrmClimDtlXMLOut.fileIdentifier"),
        F.col("JnPhrmClmDtlLnkIn.EnrollmentIssuerrecordIdentifier") == F.col("PhrmClimDtlXMLOut.EnrollmentIssuerrecordIdentifier"),
        F.col("JnPhrmClmDtlLnkIn.EnrollmentIssuerissuerIdentifier") == F.col("PhrmClimDtlXMLOut.EnrollmentIssuerissuerIdentifier"),
        F.col("JnPhrmClmDtlLnkIn.includedinsuredMemberrecordIdentifier") == F.col("PhrmClimDtlXMLOut.includedinsuredMemberrecordIdentifier"),
        F.col("JnPhrmClmDtlLnkIn.insuredMemberIdentifier") == F.col("PhrmClimDtlXMLOut.insuredMemberIdentifier")
    ],
    how="inner"
)

df_JnPhrmClmDtl_out = df_JnPhrmClmDtl.select(
    F.col("JnPhrmClmDtlLnkIn.fileIdentifier").alias("fileIdentifier"),
    F.col("JnPhrmClmDtlLnkIn.EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("JnPhrmClmDtlLnkIn.EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("JnPhrmClmDtlLnkIn.includedinsuredMemberrecordIdentifier").alias("includedinsuredMemberrecordIdentifier"),
    F.col("JnPhrmClmDtlLnkIn.insuredMemberIdentifier").alias("insuredMemberIdentifier"),
    F.col("JnPhrmClmDtlLnkIn.insuredMemberBirthDate").alias("insuredMemberBirthDate"),
    F.col("JnPhrmClmDtlLnkIn.insuredMemberGenderCode").alias("insuredMemberGenderCode"),
    F.col("JnPhrmClmDtlLnkIn.insuredMemberRace").alias("insuredMemberRace"),
    F.col("JnPhrmClmDtlLnkIn.insuredMemberEthnicity").alias("insuredMemberEthnicity"),
    F.col("PhrmClimDtlXMLOut.includedMemberProfileDetail_XML_CHUNK").alias("includedMemberProfileDetail_XML_CHUNK")
)

df_XMLOutPhrmClmInsPln = df_JnPhrmClmDtl_out.select(
    F.lit(None).alias("fileIdentifier"),
    F.lit(None).alias("EnrollmentIssuerrecordIdentifier"),
    F.lit(None).alias("EnrollmentIssuerissuerIdentifier"),
    F.lit(None).alias("includedinsuredmember_XMLChunck")
)

df_JnXML = df_CpSf_CpJnLnk.alias("CpJnLnk").join(
    df_XMLOutPhrmClmInsPln.alias("PhrmClmInsPlnLnkOut"),
    [
        F.col("CpJnLnk.fileIdentifier") == F.col("PhrmClmInsPlnLnkOut.fileIdentifier"),
        F.col("CpJnLnk.EnrollmentIssuerrecordIdentifier") == F.col("PhrmClmInsPlnLnkOut.EnrollmentIssuerrecordIdentifier"),
        F.col("CpJnLnk.EnrollmentIssuerissuerIdentifier") == F.col("PhrmClmInsPlnLnkOut.EnrollmentIssuerissuerIdentifier")
    ],
    how="inner"
)

df_JnXML_out = df_JnXML.select(
    F.col("CpJnLnk.fileIdentifier").alias("fileIdentifier"),
    F.col("CpJnLnk.executionZoneCode").alias("executionZoneCode"),
    F.col("CpJnLnk.interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("CpJnLnk.generationDateTime").alias("generationDateTime"),
    F.col("CpJnLnk.submissionTypeCode").alias("submissionTypeCode"),
    F.col("CpJnLnk.insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("CpJnLnk.insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    F.col("CpJnLnk.EnrollmentIssuerrecordIdentifier").alias("EnrollmentIssuerrecordIdentifier"),
    F.col("CpJnLnk.EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("CpJnLnk.issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("CpJnLnk.issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("PhrmClmInsPlnLnkOut.includedinsuredmember_XMLChunck").alias("includedinsuredmember_XMLChunck")
)

df_Transformer_94 = df_JnXML_out.select(
    F.col("fileIdentifier").alias("fileIdentifier"),
    F.col("executionZoneCode").alias("executionZoneCode"),
    F.col("interfaceControlReleaseNumber").alias("interfaceControlReleaseNumber"),
    F.col("generationDateTime").alias("generationDateTime"),
    F.col("submissionTypeCode").alias("submissionTypeCode"),
    F.col("insuredMemberTotalQuantity").alias("insuredMemberTotalQuantity"),
    F.col("insuredMemberProfileTotalQuantity").alias("insuredMemberProfileTotalQuantity"),
    AsInteger(F.col("EnrollmentIssuerrecordIdentifier")).alias("EnrollmentIssuerrecordIdentifier"),
    F.col("EnrollmentIssuerissuerIdentifier").alias("EnrollmentIssuerissuerIdentifier"),
    F.col("issuerInsuredMemberTotalQuantity").alias("issuerInsuredMemberTotalQuantity"),
    F.col("issuerInsuredMemberProfileTotalQuantity").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.col("includedinsuredmember_XMLChunck").alias("includedinsuredmember_XMLChunck")
)

df_Copy_of_XML_PhrmClmSub = df_Transformer_94

df_final = df_Copy_of_XML_PhrmClmSub.select(
    F.rpad(F.col("fileIdentifier"), <...>, " ").alias("fileIdentifier"),
    F.rpad(F.col("executionZoneCode"), <...>, " ").alias("executionZoneCode"),
    F.rpad(F.col("interfaceControlReleaseNumber"), <...>, " ").alias("interfaceControlReleaseNumber"),
    F.rpad(F.col("generationDateTime"), <...>, " ").alias("generationDateTime"),
    F.rpad(F.col("submissionTypeCode"), <...>, " ").alias("submissionTypeCode"),
    F.rpad(F.col("insuredMemberTotalQuantity"), <...>, " ").alias("insuredMemberTotalQuantity"),
    F.rpad(F.col("insuredMemberProfileTotalQuantity"), <...>, " ").alias("insuredMemberProfileTotalQuantity"),
    AsInteger(F.col("EnrollmentIssuerrecordIdentifier")).alias("EnrollmentIssuerrecordIdentifier"),
    F.rpad(F.col("EnrollmentIssuerissuerIdentifier"), <...>, " ").alias("EnrollmentIssuerissuerIdentifier"),
    F.rpad(F.col("issuerInsuredMemberTotalQuantity"), <...>, " ").alias("issuerInsuredMemberTotalQuantity"),
    F.rpad(F.col("issuerInsuredMemberProfileTotalQuantity"), <...>, " ").alias("issuerInsuredMemberProfileTotalQuantity"),
    F.rpad(F.col("includedinsuredmember_XMLChunck"), <...>, " ").alias("includedinsuredmember_XMLChunck")
)

write_files(
    df_final,
    f"{adls_path_publish}/external/Edge_RA_Enrollment_{State}.xml",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)