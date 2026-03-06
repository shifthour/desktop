# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : BlueKCCommonFulFmntMetadataFileSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING : Job will insert new records from Altruista metadata file to COMM table
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                               Date                         Project                          Change Description          \(9)\(9)\(9)  Development Project          Code Reviewer                   Date Reviewed       
# MAGIC ------------------------------              ------------------        ----------------------             ---------------------------------------      \(9)\(9)\(9) ------------------------------\(9)----------------------------------         -------------------------------        
# MAGIC Sirisha Palavalli                     09/02/2020             Fullfillment                       Original Programming               \(9)                  OutboundDev3                    Jaideep Mankala      
# MAGIC Saranya A                                09/21/2021             US387189                   Lookup Change to get Comm Trmns ID            OutboundDev3                    Jaideep Mankala               09/22/2021
# MAGIC Jaideep Mankala                      01/20/2023         US563776                Added logic to capture errors before loading to table     OutboundDev3     Manasa Andru                    2023-01-23


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')
jdbc_url_commtrnsmsn, jdbc_props_commtrnsmsn = get_db_config(commtrnsmsn_secret_name)

CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
RunID = get_widget_value('RunID','')
AGInboundFile = get_widget_value('AGInboundFile','')
CurrentDateAndTime = get_widget_value('CurrentDateAndTime','')

# ------------------------------------------------------------------------------
# lkp_GetTimeZone (ODBCConnectorPX)
# ------------------------------------------------------------------------------
df_lkp_GetTimeZone = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_commtrnsmsn)
    .options(**jdbc_props_commtrnsmsn)
    .option("query", "select cast((FORMAT(SYSDATETIMEOFFSET(), 'zzz')) as varchar(6)) as LocalTimeZone, 1 as DefVal")
    .load()
)

# ------------------------------------------------------------------------------
# COMM_TMPLT_RECPNT_TRNSMSN_TYP (ODBCConnectorPX)
# ------------------------------------------------------------------------------
df_COMM_TMPLT_RECPNT_TRNSMSN_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_commtrnsmsn)
    .options(**jdbc_props_commtrnsmsn)
    .option("query", f"SELECT * FROM {CommTrnsmsnOwner}.COMM_TMPLT_RECPNT_TRNSMSN_TYP")
    .load()
)

# ------------------------------------------------------------------------------
# Seq_AG_Metadata_File (PxSequentialFile) - reading the file
# ------------------------------------------------------------------------------
schema_Seq_AG_Metadata_File = StructType([
    StructField("LetterQueueID", StringType(), True),
    StructField("FileName", StringType(), True),
    StructField("TemplateName", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("CreatedOn", StringType(), True),
    StructField("AuthorizationUniqueIdentifier", StringType(), True),
    StructField("ComplaintId", StringType(), True),
    StructField("SubscriberPrimaryIdentifier", StringType(), True),
    StructField("SubscriberSecondaryIdentifier", StringType(), True),
    StructField("SubscriberFirstName", StringType(), True),
    StructField("SubscriberMiddleName", StringType(), True),
    StructField("SubscriberDOB", StringType(), True),
    StructField("SubscriberLastName", StringType(), True),
    StructField("SubscriberGender", StringType(), True),
    StructField("SubscriberEthnicity", StringType(), True),
    StructField("SubscriberLineOfBusiness", StringType(), True),
    StructField("SubscriberBenefitPlan", StringType(), True),
    StructField("SubscriberEligibilityStartDate", StringType(), True),
    StructField("SubscriberEligibilityEndDate", StringType(), True),
    StructField("SubscriberAddress", StringType(), True),
    StructField("SubscriberCity", StringType(), True),
    StructField("SubscriberState", StringType(), True),
    StructField("SubscriberCounty", StringType(), True),
    StructField("SubscriberCountry", StringType(), True),
    StructField("SubscriberZip", StringType(), True),
    StructField("FacilityProviderPrimaryIdentifier", StringType(), True),
    StructField("FacilityProviderSecondaryIdentifier", StringType(), True),
    StructField("FacilityProviderFirstName", StringType(), True),
    StructField("FacilityProviderLastName", StringType(), True),
    StructField("FacilityProviderMiddleName", StringType(), True),
    StructField("FacilityProviderName", StringType(), True),
    StructField("FacilityProviderEthnicity", StringType(), True),
    StructField("FacilityProviderAddress", StringType(), True),
    StructField("FacilityProviderCity", StringType(), True),
    StructField("FacilityProviderState", StringType(), True),
    StructField("FacilityProviderZip", StringType(), True),
    StructField("FacilityProviderCountyDescription", StringType(), True),
    StructField("FacilityProviderOfficePhone", StringType(), True),
    StructField("FacilityProviderFax", StringType(), True),
    StructField("FacilityProviderFaxOverride", StringType(), True),
    StructField("FacilityProviderEmail", StringType(), True),
    StructField("FacilityProviderAuthorizationType", StringType(), True),
    StructField("AdmittingProviderPrimaryIdentifier", StringType(), True),
    StructField("AdmittingProviderSecondaryIdentifier", StringType(), True),
    StructField("AdmittingProviderFirstName", StringType(), True),
    StructField("AdmittingProviderLastName", StringType(), True),
    StructField("AdmittingProviderMiddleName", StringType(), True),
    StructField("AdmittingProviderName", StringType(), True),
    StructField("AdmittingProviderEthnicity", StringType(), True),
    StructField("AdmittingProviderAddress", StringType(), True),
    StructField("AdmittingProviderCity", StringType(), True),
    StructField("AdmittingProviderState", StringType(), True),
    StructField("AdmittingProviderZip", StringType(), True),
    StructField("AdmittingProviderCountyDescription", StringType(), True),
    StructField("AdmittingProviderOfficePhone", StringType(), True),
    StructField("AdmittingProviderFax", StringType(), True),
    StructField("AdmittingProviderFaxOverride", StringType(), True),
    StructField("AdmittingProviderEmail", StringType(), True),
    StructField("AdmittingProviderAuthorizationType", StringType(), True),
    StructField("ReferredToProviderPrimaryIdentifier", StringType(), True),
    StructField("ReferredToProviderSecondaryIdentifier", StringType(), True),
    StructField("ReferredToProviderFirstName", StringType(), True),
    StructField("ReferredToProviderLastName", StringType(), True),
    StructField("ReferredToProviderMiddleName", StringType(), True),
    StructField("ReferredToProviderName", StringType(), True),
    StructField("ReferredToProviderEthnicity", StringType(), True),
    StructField("ReferredToProviderAddress", StringType(), True),
    StructField("ReferredToProviderCity", StringType(), True),
    StructField("ReferredToProviderState", StringType(), True),
    StructField("ReferredToProviderZip", StringType(), True),
    StructField("ReferredToProviderCountyDescription", StringType(), True),
    StructField("ReferredToProviderOfficePhone", StringType(), True),
    StructField("ReferredToProviderFax", StringType(), True),
    StructField("ReferredToProviderFaxOverride", StringType(), True),
    StructField("ReferredToProviderEmail", StringType(), True),
    StructField("ReferredToProviderAuthorizationType", StringType(), True),
    StructField("ReferredByProviderPrimaryIdentifier", StringType(), True),
    StructField("ReferredByProviderSecondaryIdentifier", StringType(), True),
    StructField("ReferredByProviderFirstName", StringType(), True),
    StructField("ReferredByProviderLastName", StringType(), True),
    StructField("ReferredByProviderMiddleName", StringType(), True),
    StructField("ReferredByProviderName", StringType(), True),
    StructField("ReferredByProviderEthnicity", StringType(), True),
    StructField("ReferredByProviderAddress", StringType(), True),
    StructField("ReferredByProviderCity", StringType(), True),
    StructField("ReferredByProviderState", StringType(), True),
    StructField("ReferredByProviderZip", StringType(), True),
    StructField("ReferredByProviderCountyDescription", StringType(), True),
    StructField("ReferredByProviderOfficePhone", StringType(), True),
    StructField("ReferredByProviderFax", StringType(), True),
    StructField("ReferredByProviderFaxOverride", StringType(), True),
    StructField("ReferredByProviderEmail", StringType(), True),
    StructField("ReferredByProviderAuthorizationType", StringType(), True),
    StructField("Module", StringType(), True),
    StructField("Language", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("LetterType", StringType(), True),
    StructField("MemberUniqueKey", StringType(), True)
])

df_Seq_AG_Metadata_File = (
    spark.read
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\u0000")  # quoteChar is null in DS
    .schema(schema_Seq_AG_Metadata_File)
    .csv(f"{adls_path}/{AGInboundFile}")
)

# ------------------------------------------------------------------------------
# xfm_DefVal (CTransformerStage)
# ------------------------------------------------------------------------------
df_xfm_DefVal = df_Seq_AG_Metadata_File.select(
    F.col("LetterQueueID").alias("LetterQueueID"),
    F.col("FileName").alias("FileName"),
    trim(F.col("TemplateName")).alias("TemplateName"),
    F.col("CreatedBy").alias("CreatedBy"),
    F.col("CreatedOn").alias("CreatedOn"),
    F.col("AuthorizationUniqueIdentifier").alias("AuthorizationUniqueIdentifier"),
    F.col("ComplaintId").alias("ComplaintId"),
    F.col("SubscriberPrimaryIdentifier").alias("SubscriberPrimaryIdentifier"),
    F.col("SubscriberSecondaryIdentifier").alias("SubscriberSecondaryIdentifier"),
    F.col("SubscriberFirstName").alias("SubscriberFirstName"),
    F.col("SubscriberMiddleName").alias("SubscriberMiddleName"),
    F.col("SubscriberDOB").alias("SubscriberDOB"),
    F.col("SubscriberLastName").alias("SubscriberLastName"),
    F.col("SubscriberGender").alias("SubscriberGender"),
    F.col("SubscriberEthnicity").alias("SubscriberEthnicity"),
    F.col("SubscriberLineOfBusiness").alias("SubscriberLineOfBusiness"),
    F.col("SubscriberBenefitPlan").alias("SubscriberBenefitPlan"),
    F.col("SubscriberEligibilityStartDate").alias("SubscriberEligibilityStartDate"),
    F.col("SubscriberEligibilityEndDate").alias("SubscriberEligibilityEndDate"),
    F.col("SubscriberAddress").alias("SubscriberAddress"),
    F.col("SubscriberCity").alias("SubscriberCity"),
    F.col("SubscriberState").alias("SubscriberState"),
    F.col("SubscriberCounty").alias("SubscriberCounty"),
    F.col("SubscriberCountry").alias("SubscriberCountry"),
    F.col("SubscriberZip").alias("SubscriberZip"),
    F.col("FacilityProviderPrimaryIdentifier").alias("FacilityProviderPrimaryIdentifier"),
    F.col("FacilityProviderSecondaryIdentifier").alias("FacilityProviderSecondaryIdentifier"),
    F.col("FacilityProviderFirstName").alias("FacilityProviderFirstName"),
    F.col("FacilityProviderLastName").alias("FacilityProviderLastName"),
    F.col("FacilityProviderMiddleName").alias("FacilityProviderMiddleName"),
    F.col("FacilityProviderName").alias("FacilityProviderName"),
    F.col("FacilityProviderEthnicity").alias("FacilityProviderEthnicity"),
    F.col("FacilityProviderAddress").alias("FacilityProviderAddress"),
    F.col("FacilityProviderCity").alias("FacilityProviderCity"),
    F.col("FacilityProviderState").alias("FacilityProviderState"),
    F.col("FacilityProviderZip").alias("FacilityProviderZip"),
    F.col("FacilityProviderCountyDescription").alias("FacilityProviderCountyDescription"),
    F.col("FacilityProviderOfficePhone").alias("FacilityProviderOfficePhone"),
    F.col("FacilityProviderFax").alias("FacilityProviderFax"),
    F.col("FacilityProviderFaxOverride").alias("FacilityProviderFaxOverride"),
    F.col("FacilityProviderEmail").alias("FacilityProviderEmail"),
    F.col("FacilityProviderAuthorizationType").alias("FacilityProviderAuthorizationType"),
    F.col("AdmittingProviderPrimaryIdentifier").alias("AdmittingProviderPrimaryIdentifier"),
    F.col("AdmittingProviderSecondaryIdentifier").alias("AdmittingProviderSecondaryIdentifier"),
    F.col("AdmittingProviderFirstName").alias("AdmittingProviderFirstName"),
    F.col("AdmittingProviderLastName").alias("AdmittingProviderLastName"),
    F.col("AdmittingProviderMiddleName").alias("AdmittingProviderMiddleName"),
    F.col("AdmittingProviderName").alias("AdmittingProviderName"),
    F.col("AdmittingProviderEthnicity").alias("AdmittingProviderEthnicity"),
    F.col("AdmittingProviderAddress").alias("AdmittingProviderAddress"),
    F.col("AdmittingProviderCity").alias("AdmittingProviderCity"),
    F.col("AdmittingProviderState").alias("AdmittingProviderState"),
    F.col("AdmittingProviderZip").alias("AdmittingProviderZip"),
    F.col("AdmittingProviderCountyDescription").alias("AdmittingProviderCountyDescription"),
    F.col("AdmittingProviderOfficePhone").alias("AdmittingProviderOfficePhone"),
    F.col("AdmittingProviderFax").alias("AdmittingProviderFax"),
    F.col("AdmittingProviderFaxOverride").alias("AdmittingProviderFaxOverride"),
    F.col("AdmittingProviderEmail").alias("AdmittingProviderEmail"),
    F.col("AdmittingProviderAuthorizationType").alias("AdmittingProviderAuthorizationType"),
    F.col("ReferredToProviderPrimaryIdentifier").alias("ReferredToProviderPrimaryIdentifier"),
    F.col("ReferredToProviderSecondaryIdentifier").alias("ReferredToProviderSecondaryIdentifier"),
    F.col("ReferredToProviderFirstName").alias("ReferredToProviderFirstName"),
    F.col("ReferredToProviderLastName").alias("ReferredToProviderLastName"),
    F.col("ReferredToProviderMiddleName").alias("ReferredToProviderMiddleName"),
    F.col("ReferredToProviderName").alias("ReferredToProviderName"),
    F.col("ReferredToProviderEthnicity").alias("ReferredToProviderEthnicity"),
    F.col("ReferredToProviderAddress").alias("ReferredToProviderAddress"),
    F.col("ReferredToProviderCity").alias("ReferredToProviderCity"),
    F.col("ReferredToProviderState").alias("ReferredToProviderState"),
    F.col("ReferredToProviderZip").alias("ReferredToProviderZip"),
    F.col("ReferredToProviderCountyDescription").alias("ReferredToProviderCountyDescription"),
    F.col("ReferredToProviderOfficePhone").alias("ReferredToProviderOfficePhone"),
    F.col("ReferredToProviderFax").alias("ReferredToProviderFax"),
    F.col("ReferredToProviderFaxOverride").alias("ReferredToProviderFaxOverride"),
    F.col("ReferredToProviderEmail").alias("ReferredToProviderEmail"),
    F.col("ReferredToProviderAuthorizationType").alias("ReferredToProviderAuthorizationType"),
    F.col("ReferredByProviderPrimaryIdentifier").alias("ReferredByProviderPrimaryIdentifier"),
    F.col("ReferredByProviderSecondaryIdentifier").alias("ReferredByProviderSecondaryIdentifier"),
    F.col("ReferredByProviderFirstName").alias("ReferredByProviderFirstName"),
    F.col("ReferredByProviderLastName").alias("ReferredByProviderLastName"),
    F.col("ReferredByProviderMiddleName").alias("ReferredByProviderMiddleName"),
    F.col("ReferredByProviderName").alias("ReferredByProviderName"),
    F.col("ReferredByProviderEthnicity").alias("ReferredByProviderEthnicity"),
    F.col("ReferredByProviderAddress").alias("ReferredByProviderAddress"),
    F.col("ReferredByProviderCity").alias("ReferredByProviderCity"),
    F.col("ReferredByProviderState").alias("ReferredByProviderState"),
    F.col("ReferredByProviderZip").alias("ReferredByProviderZip"),
    F.col("ReferredByProviderCountyDescription").alias("ReferredByProviderCountyDescription"),
    F.col("ReferredByProviderOfficePhone").alias("ReferredByProviderOfficePhone"),
    F.col("ReferredByProviderFax").alias("ReferredByProviderFax"),
    F.col("ReferredByProviderFaxOverride").alias("ReferredByProviderFaxOverride"),
    F.col("ReferredByProviderEmail").alias("ReferredByProviderEmail"),
    F.col("ReferredByProviderAuthorizationType").alias("ReferredByProviderAuthorizationType"),
    F.col("Module").alias("Module"),
    F.col("Language").alias("Language"),
    F.col("Description").alias("Description"),
    F.col("LetterType").alias("LetterType"),
    F.col("MemberUniqueKey").alias("MemberUniqueKey"),
    F.lit(1).alias("DefVal")
)

# ------------------------------------------------------------------------------
# lkp_File_TimeZone (PxLookup)
# ------------------------------------------------------------------------------
# DataStage has no join conditions. "AllowDups" = "False". We take first row from each reference link.
tz_rows = df_lkp_GetTimeZone.collect()
ref_rows = df_COMM_TMPLT_RECPNT_TRNSMSN_TYP.collect()

val_LocalTimeZone = None
val_COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID = None

if len(tz_rows) > 0:
    val_LocalTimeZone = tz_rows[0]["LocalTimeZone"]

if len(ref_rows) > 0 and "COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID" in ref_rows[0].asDict():
    val_COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID = ref_rows[0]["COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID"]

df_lkp_File_TimeZone = (
    df_xfm_DefVal
    .withColumn("LocalTimeZone", F.lit(val_LocalTimeZone))
    .withColumn("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID", F.lit(val_COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID))
)

# ------------------------------------------------------------------------------
# Transformer_98 (CTransformerStage)
# ------------------------------------------------------------------------------
df_Transformer_98 = df_lkp_File_TimeZone.withColumn(
    "svError",
    F.when(F.length(trim(F.col("LetterQueueID"))) == 0, F.lit("MISSING COMM ID"))
     .when(F.length(trim(F.col("TemplateName"))) == 0, F.lit("MISSING TMPLT NAME"))
     .when(
         (F.length(trim(F.col("TemplateName"))) > 0) & 
         ((F.length(trim(F.col("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID"))) == 0) | (F.col("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID").isNull())),
         F.lit("NO MATCHING TMPLT FOUND")
     )
     .otherwise(F.lit("N"))
)

df_lnk_Src = df_Transformer_98.filter(F.col("svError") == "N").select(
    F.col("LetterQueueID").alias("LetterQueueID"),
    F.col("FileName").alias("FileName"),
    F.col("TemplateName").alias("TemplateName"),
    F.col("CreatedBy").alias("CreatedBy"),
    F.col("CreatedOn").alias("CreatedOn"),
    F.col("AuthorizationUniqueIdentifier").alias("AuthorizationUniqueIdentifier"),
    F.col("ComplaintId").alias("ComplaintId"),
    F.col("SubscriberPrimaryIdentifier").alias("SubscriberPrimaryIdentifier"),
    F.col("SubscriberSecondaryIdentifier").alias("SubscriberSecondaryIdentifier"),
    F.col("SubscriberFirstName").alias("SubscriberFirstName"),
    F.col("SubscriberMiddleName").alias("SubscriberMiddleName"),
    F.col("SubscriberDOB").alias("SubscriberDOB"),
    F.col("SubscriberLastName").alias("SubscriberLastName"),
    F.col("SubscriberGender").alias("SubscriberGender"),
    F.col("SubscriberEthnicity").alias("SubscriberEthnicity"),
    F.col("SubscriberLineOfBusiness").alias("SubscriberLineOfBusiness"),
    F.col("SubscriberBenefitPlan").alias("SubscriberBenefitPlan"),
    F.col("SubscriberEligibilityStartDate").alias("SubscriberEligibilityStartDate"),
    F.col("SubscriberEligibilityEndDate").alias("SubscriberEligibilityEndDate"),
    F.col("SubscriberAddress").alias("SubscriberAddress"),
    F.col("SubscriberCity").alias("SubscriberCity"),
    F.col("SubscriberState").alias("SubscriberState"),
    F.col("SubscriberCounty").alias("SubscriberCounty"),
    F.col("SubscriberCountry").alias("SubscriberCountry"),
    F.col("SubscriberZip").alias("SubscriberZip"),
    F.col("FacilityProviderPrimaryIdentifier").alias("FacilityProviderPrimaryIdentifier"),
    F.col("FacilityProviderSecondaryIdentifier").alias("FacilityProviderSecondaryIdentifier"),
    F.col("FacilityProviderFirstName").alias("FacilityProviderFirstName"),
    F.col("FacilityProviderLastName").alias("FacilityProviderLastName"),
    F.col("FacilityProviderMiddleName").alias("FacilityProviderMiddleName"),
    F.col("FacilityProviderName").alias("FacilityProviderName"),
    F.col("FacilityProviderEthnicity").alias("FacilityProviderEthnicity"),
    F.col("FacilityProviderAddress").alias("FacilityProviderAddress"),
    F.col("FacilityProviderCity").alias("FacilityProviderCity"),
    F.col("FacilityProviderState").alias("FacilityProviderState"),
    F.col("FacilityProviderZip").alias("FacilityProviderZip"),
    F.col("FacilityProviderCountyDescription").alias("FacilityProviderCountyDescription"),
    F.col("FacilityProviderOfficePhone").alias("FacilityProviderOfficePhone"),
    F.col("FacilityProviderFax").alias("FacilityProviderFax"),
    F.col("FacilityProviderFaxOverride").alias("FacilityProviderFaxOverride"),
    F.col("FacilityProviderEmail").alias("FacilityProviderEmail"),
    F.col("FacilityProviderAuthorizationType").alias("FacilityProviderAuthorizationType"),
    F.col("AdmittingProviderPrimaryIdentifier").alias("AdmittingProviderPrimaryIdentifier"),
    F.col("AdmittingProviderSecondaryIdentifier").alias("AdmittingProviderSecondaryIdentifier"),
    F.col("AdmittingProviderFirstName").alias("AdmittingProviderFirstName"),
    F.col("AdmittingProviderLastName").alias("AdmittingProviderLastName"),
    F.col("AdmittingProviderMiddleName").alias("AdmittingProviderMiddleName"),
    F.col("AdmittingProviderName").alias("AdmittingProviderName"),
    F.col("AdmittingProviderEthnicity").alias("AdmittingProviderEthnicity"),
    F.col("AdmittingProviderAddress").alias("AdmittingProviderAddress"),
    F.col("AdmittingProviderCity").alias("AdmittingProviderCity"),
    F.col("AdmittingProviderState").alias("AdmittingProviderState"),
    F.col("AdmittingProviderZip").alias("AdmittingProviderZip"),
    F.col("AdmittingProviderCountyDescription").alias("AdmittingProviderCountyDescription"),
    F.col("AdmittingProviderOfficePhone").alias("AdmittingProviderOfficePhone"),
    F.col("AdmittingProviderFax").alias("AdmittingProviderFax"),
    F.col("AdmittingProviderFaxOverride").alias("AdmittingProviderFaxOverride"),
    F.col("AdmittingProviderEmail").alias("AdmittingProviderEmail"),
    F.col("AdmittingProviderAuthorizationType").alias("AdmittingProviderAuthorizationType"),
    F.col("ReferredToProviderPrimaryIdentifier").alias("ReferredToProviderPrimaryIdentifier"),
    F.col("ReferredToProviderSecondaryIdentifier").alias("ReferredToProviderSecondaryIdentifier"),
    F.col("ReferredToProviderFirstName").alias("ReferredToProviderFirstName"),
    F.col("ReferredToProviderLastName").alias("ReferredToProviderLastName"),
    F.col("ReferredToProviderMiddleName").alias("ReferredToProviderMiddleName"),
    F.col("ReferredToProviderName").alias("ReferredToProviderName"),
    F.col("ReferredToProviderEthnicity").alias("ReferredToProviderEthnicity"),
    F.col("ReferredToProviderAddress").alias("ReferredToProviderAddress"),
    F.col("ReferredToProviderCity").alias("ReferredToProviderCity"),
    F.col("ReferredToProviderState").alias("ReferredToProviderState"),
    F.col("ReferredToProviderZip").alias("ReferredToProviderZip"),
    F.col("ReferredToProviderCountyDescription").alias("ReferredToProviderCountyDescription"),
    F.col("ReferredToProviderOfficePhone").alias("ReferredToProviderOfficePhone"),
    F.col("ReferredToProviderFax").alias("ReferredToProviderFax"),
    F.col("ReferredToProviderFaxOverride").alias("ReferredToProviderFaxOverride"),
    F.col("ReferredToProviderEmail").alias("ReferredToProviderEmail"),
    F.col("ReferredToProviderAuthorizationType").alias("ReferredToProviderAuthorizationType"),
    F.col("ReferredByProviderPrimaryIdentifier").alias("ReferredByProviderPrimaryIdentifier"),
    F.col("ReferredByProviderSecondaryIdentifier").alias("ReferredByProviderSecondaryIdentifier"),
    F.col("ReferredByProviderFirstName").alias("ReferredByProviderFirstName"),
    F.col("ReferredByProviderLastName").alias("ReferredByProviderLastName"),
    F.col("ReferredByProviderMiddleName").alias("ReferredByProviderMiddleName"),
    F.col("ReferredByProviderName").alias("ReferredByProviderName"),
    F.col("ReferredByProviderEthnicity").alias("ReferredByProviderEthnicity"),
    F.col("ReferredByProviderAddress").alias("ReferredByProviderAddress"),
    F.col("ReferredByProviderCity").alias("ReferredByProviderCity"),
    F.col("ReferredByProviderState").alias("ReferredByProviderState"),
    F.col("ReferredByProviderZip").alias("ReferredByProviderZip"),
    F.col("ReferredByProviderCountyDescription").alias("ReferredByProviderCountyDescription"),
    F.col("ReferredByProviderOfficePhone").alias("ReferredByProviderOfficePhone"),
    F.col("ReferredByProviderFax").alias("ReferredByProviderFax"),
    F.col("ReferredByProviderFaxOverride").alias("ReferredByProviderFaxOverride"),
    F.col("ReferredByProviderEmail").alias("ReferredByProviderEmail"),
    F.col("ReferredByProviderAuthorizationType").alias("ReferredByProviderAuthorizationType"),
    F.col("Module").alias("Module"),
    F.col("Language").alias("Language"),
    F.col("Description").alias("Description"),
    F.col("LetterType").alias("LetterType"),
    F.col("MemberUniqueKey").alias("MemberUniqueKey"),
    F.col("LocalTimeZone").alias("LocalTimeZone"),
    F.col("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID").alias("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID")
)

df_errors = df_Transformer_98.filter(F.col("svError") != "N").select(
    F.col("LetterQueueID").alias("LetterQueueID"),
    F.col("FileName").alias("FileName"),
    F.col("TemplateName").alias("TemplateName"),
    F.col("CreatedBy").alias("CreatedBy"),
    F.col("CreatedOn").alias("CreatedOn"),
    F.col("AuthorizationUniqueIdentifier").alias("AuthorizationUniqueIdentifier"),
    F.col("ComplaintId").alias("ComplaintId"),
    F.col("svError").alias("error")
)

# ------------------------------------------------------------------------------
# Errors (PxSequentialFile)
# ------------------------------------------------------------------------------
write_files(
    df_errors,
    f"{adls_path_publish}/external/Errors_{AGInboundFile}_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------
# xfm_Required_fields (CTransformerStage)
# ------------------------------------------------------------------------------
# Create the columns that rely on stage variables. We assume everything is a direct function call available in the namespace.
df_xfm_Required_fields_vars = df_lnk_Src.withColumn("TimeZone", Field(F.col("CreatedOn"), " ", F.lit(4))) \
    .withColumn("USCnt", Count(F.col("TemplateName"), F.lit("_"))) \
    .withColumn("TmpltSuffix", F.when(
        Right(F.col("TemplateName"), 2) == "_F",
        F.lit("_") + Field(F.col("TemplateName"), "_", F.col("USCnt"), F.col("USCnt") + 1)
    ).otherwise(
        F.lit("_") + Field(F.col("TemplateName"), "_", F.col("USCnt") + 1)
    )) \
    .withColumn("MailPrefix", Left(F.col("TemplateName"), 3)) \
    .withColumn("VarDOB", F.when(
        trim(F.col("SubscriberDOB")) == "",
        SetNull()
    ).otherwise(
        StringToDate(F.col("SubscriberDOB"), "%mm-%dd-%yyyy")
    )) \
    .withColumn("VarStartDate", F.when(
        trim(F.col("SubscriberEligibilityStartDate")) == "",
        SetNull()
    ).otherwise(
        StringToDate(F.col("SubscriberEligibilityStartDate"), "%mm-%dd-%yyyy")
    )) \
    .withColumn("VarEndDate", F.when(
        trim(F.col("SubscriberEligibilityEndDate")) == "",
        SetNull()
    ).otherwise(
        StringToDate(F.col("SubscriberEligibilityEndDate"), "%mm-%dd-%yyyy")
    )) \
    .withColumn("VarAGPAHUB", F.lit("AGPAHUB")) \
    .withColumn("VarAGPAHUBFILE", F.lit("AGPAHUBFILE")) \
    .withColumn("VarMAILRCVD", F.lit("MAILRCVD")) \
    .withColumn("VarFAXRCVD", F.lit("FAXRCVD")) \
    .withColumn("VarDOCRCVD", F.lit("DOCRCVD")) \
    .withColumn("VarNXTPG", F.lit("NXTPG")) \
    .withColumn("VarPRCPTV", F.lit("PRCPTV")) \
    .withColumn("VarERROR", F.lit("ERROR")) \
    .withColumn("VarMAILRCVDDesc", F.lit("Mail Request Received")) \
    .withColumn("VarFAXRCVDDesc", F.lit("Fax Request Received")) \
    .withColumn("VarDOCRCVDDesc", F.lit("Document Storage Request Received")) \
    .withColumn("VarERRORDesc", F.lit("Received blank data for MemberID or required Provider fields")) \
    .withColumn("VarRGHTFAX", F.lit("RGHTFAX")) \
    .withColumn("LocalTimeZone", Ereplace(F.col("LocalTimeZone"), ":", "")) \
    .withColumn("InputTimeZone", Ereplace(F.col("TimeZone"), ":", "")) \
    .withColumn("hhTZ", F.when(
        F.length(TrimLeadingTrailing(F.col("InputTimeZone"))) == 5,
        Left(F.col("InputTimeZone"), 3)
    ).otherwise(
        Left(F.col("InputTimeZone"), 2)
    )) \
    .withColumn("hhLTZ", F.when(
        F.length(TrimLeadingTrailing(F.col("LocalTimeZone"))) == 5,
        Left(F.col("LocalTimeZone"), 3)
    ).otherwise(
        Left(F.col("LocalTimeZone"), 2)
    )) \
    .withColumn("mmTZ", Right(F.col("InputTimeZone"), 2)) \
    .withColumn("mmLTZ", Right(F.col("LocalTimeZone"), 2)) \
    .withColumn("hhDiff", F.col("hhLTZ") - F.col("hhTZ")) \
    .withColumn("mmDiff", F.col("mmLTZ") - F.col("mmTZ")) \
    .withColumn("StgVarCreatedOn", TimestampOffsetByComponents(
        StringToTimestamp(F.col("CreatedOn"), "%(m,s)/%(d,s)/%yyyy %(H,s):%(n,s):%(s,s) %aa"),
        F.lit(0), F.lit(0), F.lit(0), F.col("hhDiff"), F.col("mmDiff"), F.lit(0)
    )) \
    .withColumn("ConvertedDate", TimestampToDate(F.col("StgVarCreatedOn"))) \
    .withColumn("CustomDocID", F.lit("AG") + JulianDayFromDate(F.col("ConvertedDate")) + F.col("LetterQueueID"))

# Now produce the two output links from xfm_Required_fields:
df_lnk_Mail_MBR = df_xfm_Required_fields_vars.select(
    F.lit("AG") + F.col("LetterQueueID").alias("COMM_ID"),
    F.col("VarAGPAHUB").alias("COMM_SRC_SYS_CD"),
    F.col("VarNXTPG").alias("COMM_TRGT_SYS_CD"),
    F.col("StgVarCreatedOn").alias("COMM_SENT_DTM"),
    F.when(
        trim(F.col("SubscriberPrimaryIdentifier")) == "",
        F.col("VarERROR")
    ).otherwise(F.col("VarMAILRCVD")).alias("COMM_STTUS_CD"),
    F.col("VarAGPAHUBFILE").alias("COMM_TYP_CD"),
    F.when(
        trim(F.col("SubscriberPrimaryIdentifier")) == "",
        F.col("VarERRORDesc")
    ).otherwise(F.col("VarMAILRCVDDesc")).alias("COMM_STTUS_CMNT_TX"),
    F.col("Description").alias("COMM_DESC"),
    F.col("FileName").alias("COMM_FILE_NM"),
    SetNull().alias("COMM_JOB_ID"),
    F.col("Language").alias("COMM_LANG_NM"),
    F.col("LetterType").alias("COMM_RECPNT_TYP_NM"),
    F.col("Module").alias("COMM_SRC_SYS_MDUL_CD"),
    F.col("VarNXTPG").alias("COMM_TRGT_SYS_MDUL_CD"),
    F.col("TemplateName").alias("COMM_TMPLT_NM"),
    F.col("CreatedBy").alias("COMM_RQST_BY_USER_ID"),
    F.col("StgVarCreatedOn").alias("COMM_RQST_BY_USER_DTM"),
    F.lit(0).alias("COMM_TRNSMSN_CT"),
    F.col("ComplaintId").alias("CMPLNT_ID"),
    F.col("SubscriberPrimaryIdentifier").alias("MBR_ID"),
    F.col("SubscriberSecondaryIdentifier").alias("MBR_INDV_BE_KEY"),
    F.col("VarDOB").alias("MBR_BRTH_DT"),
    F.col("VarStartDate").alias("MBR_ELIG_STRT_DT"),
    F.col("VarEndDate").alias("MBR_ELIG_END_DT"),
    F.col("SubscriberEthnicity").alias("MBR_ETHNIC_NM"),
    F.col("SubscriberGender").alias("MBR_GNDR_CD"),
    F.col("SubscriberFirstName").alias("MBR_FIRST_NM"),
    F.col("SubscriberLastName").alias("MBR_LAST_NM"),
    F.col("SubscriberMiddleName").alias("MBR_MID_NM"),
    F.col("SubscriberAddress").alias("MBR_ADDR_LN"),
    F.col("SubscriberCity").alias("MBR_ADDR_CITY_NM"),
    F.col("SubscriberState").alias("MBR_ADDR_ST_NM"),
    F.col("SubscriberZip").alias("MBR_ADDR_ZIP_CD"),
    F.col("SubscriberCounty").alias("MBR_ADDR_CNTY_NM"),
    F.col("SubscriberCountry").alias("MBR_ADDR_CTRY_NM"),
    SetNull().alias("MBR_ADDR_TYP_NM"),
    F.col("SubscriberBenefitPlan").alias("PROD_NM"),
    F.col("FacilityProviderPrimaryIdentifier").alias("PROV_ID"),
    F.col("FacilityProviderSecondaryIdentifier").alias("PROV_NTNL_PROV_ID"),
    F.col("FacilityProviderEmail").alias("PROV_EMAIL_ADDR"),
    F.col("FacilityProviderEthnicity").alias("PROV_ETHNIC_NM"),
    F.col("FacilityProviderFirstName").alias("PROV_FIRST_NM"),
    F.col("FacilityProviderLastName").alias("PROV_LAST_NM"),
    F.col("FacilityProviderMiddleName").alias("PROV_MID_NM"),
    F.col("FacilityProviderName").alias("PROV_NM"),
    F.col("FacilityProviderFax").alias("PROV_ADDR_FAX_NO"),
    F.col("FacilityProviderFaxOverride").alias("PROV_ADDR_FAX_OVRD_NO"),
    F.col("FacilityProviderAddress").alias("PROV_ADDR_LN"),
    F.col("FacilityProviderCity").alias("PROV_ADDR_CITY_NM"),
    F.col("FacilityProviderState").alias("PROV_ADDR_ST_NM"),
    F.col("FacilityProviderZip").alias("PROV_ADDR_ZIP_CD"),
    F.col("FacilityProviderCountyDescription").alias("PROV_ADDR_CNTY_NM"),
    F.col("FacilityProviderOfficePhone").alias("PROV_ADDR_PHN_NO"),
    SetNull().alias("PROV_ADDR_PHN_OVRD_NO"),
    SetNull().alias("PROV_ADDR_TYP_NM"),
    SetNull().cast(StringType()).alias("PROV_PRI_ADDR_IN"),
    F.col("FacilityProviderAuthorizationType").alias("UM_AUTH_TYP_NM"),
    F.col("AuthorizationUniqueIdentifier").alias("UM_REF_ID"),
    current_timestamp().alias("CRT_DTM"),
    F.lit("$CommTrnsmsnAcct").alias("CRT_USER_ID"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit("$CommTrnsmsnAcct").alias("LAST_UPDT_USER_ID"),
    F.col("CustomDocID").alias("IMG_ID"),
    F.col("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID").alias("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID")
)

df_lnk_Prcptv_MBR = df_xfm_Required_fields_vars.select(
    F.lit("AG") + F.col("LetterQueueID").alias("COMM_ID"),
    F.col("VarAGPAHUB").alias("COMM_SRC_SYS_CD"),
    F.col("VarPRCPTV").alias("COMM_TRGT_SYS_CD"),
    F.col("StgVarCreatedOn").alias("COMM_SENT_DTM"),
    F.when(
        trim(F.col("SubscriberPrimaryIdentifier")) == "",
        F.col("VarERROR")
    ).otherwise(F.col("VarDOCRCVD")).alias("COMM_STTUS_CD"),
    F.col("VarAGPAHUBFILE").alias("COMM_TYP_CD"),
    F.when(
        trim(F.col("SubscriberPrimaryIdentifier")) == "",
        F.col("VarERRORDesc")
    ).otherwise(F.col("VarDOCRCVDDesc")).alias("COMM_STTUS_CMNT_TX"),
    F.col("Description").alias("COMM_DESC"),
    F.col("FileName").alias("COMM_FILE_NM"),
    SetNull().alias("COMM_JOB_ID"),
    F.col("Language").alias("COMM_LANG_NM"),
    F.col("LetterType").alias("COMM_RECPNT_TYP_NM"),
    F.col("Module").alias("COMM_SRC_SYS_MDUL_CD"),
    F.col("VarPRCPTV").alias("COMM_TRGT_SYS_MDUL_CD"),
    F.col("TemplateName").alias("COMM_TMPLT_NM"),
    F.col("CreatedBy").alias("COMM_RQST_BY_USER_ID"),
    F.col("StgVarCreatedOn").alias("COMM_RQST_BY_USER_DTM"),
    F.lit(0).alias("COMM_TRNSMSN_CT"),
    F.col("ComplaintId").alias("CMPLNT_ID"),
    F.col("SubscriberPrimaryIdentifier").alias("MBR_ID"),
    F.col("SubscriberSecondaryIdentifier").alias("MBR_INDV_BE_KEY"),
    F.col("VarDOB").alias("MBR_BRTH_DT"),
    F.col("VarStartDate").alias("MBR_ELIG_STRT_DT"),
    F.col("VarEndDate").alias("MBR_ELIG_END_DT"),
    F.col("SubscriberEthnicity").alias("MBR_ETHNIC_NM"),
    F.col("SubscriberGender").alias("MBR_GNDR_CD"),
    F.col("SubscriberFirstName").alias("MBR_FIRST_NM"),
    F.col("SubscriberLastName").alias("MBR_LAST_NM"),
    F.col("SubscriberMiddleName").alias("MBR_MID_NM"),
    F.col("SubscriberAddress").alias("MBR_ADDR_LN"),
    F.col("SubscriberCity").alias("MBR_ADDR_CITY_NM"),
    F.col("SubscriberState").alias("MBR_ADDR_ST_NM"),
    F.col("SubscriberZip").alias("MBR_ADDR_ZIP_CD"),
    F.col("SubscriberCounty").alias("MBR_ADDR_CNTY_NM"),
    F.col("SubscriberCountry").alias("MBR_ADDR_CTRY_NM"),
    SetNull().alias("MBR_ADDR_TYP_NM"),
    F.col("SubscriberBenefitPlan").alias("PROD_NM"),
    F.col("FacilityProviderPrimaryIdentifier").alias("PROV_ID"),
    F.col("FacilityProviderSecondaryIdentifier").alias("PROV_NTNL_PROV_ID"),
    F.col("FacilityProviderEmail").alias("PROV_EMAIL_ADDR"),
    F.col("FacilityProviderEthnicity").alias("PROV_ETHNIC_NM"),
    F.col("FacilityProviderFirstName").alias("PROV_FIRST_NM"),
    F.col("FacilityProviderLastName").alias("PROV_LAST_NM"),
    F.col("FacilityProviderMiddleName").alias("PROV_MID_NM"),
    F.col("FacilityProviderName").alias("PROV_NM"),
    F.col("FacilityProviderFax").alias("PROV_ADDR_FAX_NO"),
    F.col("FacilityProviderFaxOverride").alias("PROV_ADDR_FAX_OVRD_NO"),
    F.col("FacilityProviderAddress").alias("PROV_ADDR_LN"),
    F.col("FacilityProviderCity").alias("PROV_ADDR_CITY_NM"),
    F.col("FacilityProviderState").alias("PROV_ADDR_ST_NM"),
    F.col("FacilityProviderZip").alias("PROV_ADDR_ZIP_CD"),
    F.col("FacilityProviderCountyDescription").alias("PROV_ADDR_CNTY_NM"),
    F.col("FacilityProviderOfficePhone").alias("PROV_ADDR_PHN_NO"),
    SetNull().alias("PROV_ADDR_PHN_OVRD_NO"),
    SetNull().alias("PROV_ADDR_TYP_NM"),
    SetNull().cast(StringType()).alias("PROV_PRI_ADDR_IN"),
    F.col("FacilityProviderAuthorizationType").alias("UM_AUTH_TYP_NM"),
    F.col("AuthorizationUniqueIdentifier").alias("UM_REF_ID"),
    current_timestamp().alias("CRT_DTM"),
    F.lit("$CommTrnsmsnAcct").alias("CRT_USER_ID"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit("$CommTrnsmsnAcct").alias("LAST_UPDT_USER_ID"),
    F.col("CustomDocID").alias("IMG_ID"),
    F.col("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID").alias("COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID")
)

# ------------------------------------------------------------------------------
# Union_Mail_Perceptive_Fax (PxFunnel)
# ------------------------------------------------------------------------------
df_Union_Mail_Perceptive_Fax = df_lnk_Mail_MBR.unionByName(df_lnk_Prcptv_MBR)

# ------------------------------------------------------------------------------
# COMM (ODBCConnectorPX) - writing to the table #$CommTrnsmsnOwner#.COMM
# ------------------------------------------------------------------------------
temp_table_COMM = "STAGING.BlueKCCommonAGFulFmntMetadataFileLoad_COMM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_COMM}", jdbc_url_commtrnsmsn, jdbc_props_commtrnsmsn)

df_Union_Mail_Perceptive_Fax.write \
    .format("jdbc") \
    .option("url", jdbc_url_commtrnsmsn) \
    .options(**jdbc_props_commtrnsmsn) \
    .option("dbtable", temp_table_COMM) \
    .mode("append") \
    .save()

insert_sql_COMM = f"INSERT INTO {CommTrnsmsnOwner}.COMM SELECT * FROM {temp_table_COMM}"
execute_dml(insert_sql_COMM, jdbc_url_commtrnsmsn, jdbc_props_commtrnsmsn)

# ------------------------------------------------------------------------------
# RejectFile (PxSequentialFile)
# ------------------------------------------------------------------------------
# In DataStage, the reject link captures DB reject rows with ERRORCODE/ERRORTEXT. 
# Spark does not capture row-level DB rejects automatically, so we produce an empty DataFrame for demonstration.

df_lnk_Rejects = spark.createDataFrame([], df_Union_Mail_Perceptive_Fax.schema)

write_files(
    df_lnk_Rejects,
    f"{adls_path_publish}/external/Reject_{AGInboundFile}_{RunID}.txt",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)