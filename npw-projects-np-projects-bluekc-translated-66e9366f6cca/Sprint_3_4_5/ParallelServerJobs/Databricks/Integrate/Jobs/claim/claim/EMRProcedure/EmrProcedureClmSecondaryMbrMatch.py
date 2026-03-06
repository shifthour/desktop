# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021, 2022 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC                    
# MAGIC Job Name: EmrProcedureClmSecondaryMbrMatch
# MAGIC Called By: EmrProcedureMbrMatchSeq
# MAGIC Purpose: This job takes the Procedure v5.0 file sent to us by the Providers, does member matching logic compared to the IDS MBR table, and generates and updated version of the Procedure v5.0 file to be read by the EmrProcedureClmExtrFormatData job.
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary  
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)PROJECT/\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)DEVELOPMENT\(9)\(9)
# MAGIC DEVELOPER\(9)\(9)DATE\(9)\(9)STORY#\(9)\(9)CHANGE DESCRIPTION\(9)\(9)\(9)\(9)\(9)PROJECT\(9)\(9)\(9)CODE REVIEWER\(9)\(9)\(9)DATE REVIEWED
# MAGIC ==============================================================================================================================================================================================================
# MAGIC Ken Bradmon\(9)\(9)2022-01-26\(9)us480737\(9)\(9)Original programming\(9)\(9)\(9)\(9)\(9)IntegrateDev2               \(9)\(9)Reddy Sanam                           \(9)03/10/2022
# MAGIC Ken Bradmon\(9)\(9)2022-09-01\(9)us542805\(9)\(9)Added new columns: PROC_CD_CPT, \(9)\(9)\(9)\(9)IntegrateDev1\(9)\(9)\(9)Harsha Ravuri\(9)\(9)\(9)06/14/2023\(9)\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPT_MOD_1, PROC_CD_CPT_2, PROC_CD_CPTII,
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPTII_MOD_1, PROC_CD_CPTII_MOD_2, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)HCPS, ICD_10_PCS_1, ICD_10_PCS_2, SNOMED, and CVX.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also changed the logic for what is considered a "duplicate" record, to be
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)routed to the duplicate rows file.
# MAGIC Ken Bradmon\(9)\(9)2023-11-14\(9)us599003\(9)\(9)Added 6 more columns to the tranformation of the stage variable \(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)\(9)01/29/2024
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"svIsDuplicateRow" in the stage called "CountRows".  
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)This will correct the issue with FALSE "too many member matches"
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)error records.  This update was missed when these columns were
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)added to this job in the fall of 2022, but this issue was just recently
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)discovered.  ALSO remove the Level6 and Level7 member match steps.
# MAGIC Ken Bradmon\(9)\(9)2024-03-05\(9)us611147\(9)\(9)Added back level 2 and 3 of the member matching logic, per new \(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)\(9)03/08/2024
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)business requirements.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)

# MAGIC Job Name: EmrProcedureClmSecondaryMbrMatch
# MAGIC Called By: EmrProcedureMbrMatchSeq
# MAGIC Purpose: This job takes the Procedure v5.0 file sent to us by the Providers, does member matching logic compared to the IDS MBR table, and generates and updated version of the Procedure v5.0 file to be read by the EmrProcedureClmExtrFormatData job.
# MAGIC In this case we don't want the ones that match in the lookup, so \"rejects\" are GOOD.  The records that match are duplicates.
# MAGIC This is the original Procedure file from the provider.
# MAGIC /ids/prod/landing
# MAGIC <PROVIDER>.PROCEDURE.CCYYMMDDhhmmss.TXT
# MAGIC The Clense stage following the sort, removes duplicates based on:
# MAGIC + POL_NO
# MAGIC + PATN_LAST_NM
# MAGIC + PATN_FIRST_NM
# MAGIC + DOB
# MAGIC + PROC_TYPE -- retired column
# MAGIC + PROC_CD_CPT
# MAGIC + PROC_CD_CPTII
# MAGIC + HCPCS 
# MAGIC + SNOMED
# MAGIC + CVX
# MAGIC + DT_OF_SVC
# MAGIC + RSLT_VAL
# MAGIC + RNDR_NTNL_PROV_ID
# MAGIC And sends those records to the DupeRows file.
# MAGIC The sort and the calculation of duplicate rows in the \"CountRows\" stage counts rows for the purpose of identifying \"too many member matches.\"  That is why it works differently than the de-dupe step in the sort stage near the beginning of the job.  And we don't just want those rows dropped, we want them to get written to the Error file, so someone can investigate those errors.
# MAGIC This is the replacement "source" Procedure file:
# MAGIC /ids/prod/landing
# MAGIC <PROVIDER>.PROCEDURE.CCYYMMDDhhmmss.TXT_MBRMATCHFINAL
# MAGIC /ids/prod/verified/
# MAGIC PROCEDURE.MEMBER_MATCH_ERRORS.CCYYMMhhmmss.TXT
# MAGIC There are 3 scenarios for Member Matching, to maximize the number of Procedure records successfully matched to a single Member record from MBR_D.
# MAGIC Procedure records that fail to match get sent to the Error file. Procedure records that match more than one record from MBR_D also get sent to the Error file.
# MAGIC Level 1 match:  Subscriber Id, Full First Name, Full Last Name, Full Date of Birth (same as v4.3 Concord match)
# MAGIC Level 2 match:  Subscriber Id, First 3 characters of Member First Name, Full Last Name, Full DOB
# MAGIC Level 3 match:  Subscriber Id, First 3 characters of Member First Name, First 3 characters of Member Last Name, Full Member Date of Birth
# MAGIC These are the source file records where all of the member matching levels 1 - 3 failed.
# MAGIC If we have more than one record "matched" from the MBR table, we want ALL of those records redirected to the error file, not just one of them.
# MAGIC So the "IdentifyDuplicates" stage sends the dupes to the "Duplicates" stage, then the "LookupDupes" only sends the REJECTED (non-matching) rows to the "ClenseOutput" stage, and all the matches go to the "TooManyMatches" stage, and then the ErrorFile.
# MAGIC These are records where the POL_NO does not match any SUB_ID in our IDS SUB table.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFile = get_widget_value('InFile','')
ProviderName = get_widget_value('ProviderName','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# SUB Stage (DB2ConnectorPX)
extract_query_SUB = f"SELECT DISTINCT upper(sub.SUB_ID) as SUB_ID FROM {IDSOwner}.SUB sub"
df_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_SUB)
    .load()
)

# Raw_Procedure_original (PxSequentialFile)
schema_raw_procedure_original = StructType([
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("PROC_TYPE", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RSLT_VAL", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_TYP", StringType(), True),
    StructField("PROC_CD_CPT", StringType(), True),
    StructField("PROC_CD_CPT_MOD_1", StringType(), True),
    StructField("PROC_CD_CPT_MOD_2", StringType(), True),
    StructField("PROC_CD_CPTII", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_1", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_2", StringType(), True),
    StructField("HCPS", StringType(), True),
    StructField("ICD_10_PCS_1", StringType(), True),
    StructField("SNOMED", StringType(), True),
    StructField("CVX", StringType(), True),
    StructField("SOURCE_ID", StringType(), True)
])
df_Raw_Procedure_original = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", None)
    .option("nullValue", None)
    .schema(schema_raw_procedure_original)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Cleanse (CTransformerStage)
df_Cleanse = df_Raw_Procedure_original
df_Cleanse = df_Cleanse.withColumn(
    "POL_NO",
    F.when((F.length(trim(F.col("POL_NO"))) == 0) | (F.col("POL_NO").isNull()), F.lit(""))
     .otherwise(F.substring(UpCase(trim(F.col("POL_NO"))), 1, 20))
).withColumn(
    "PATN_LAST_NM",
    F.when((F.length(trim(F.col("PATN_LAST_NM"))) == 0) | (F.col("PATN_LAST_NM").isNull()), F.lit(""))
     .otherwise(F.substring(UpCase(trim(F.col("PATN_LAST_NM"))), 1, 50))
).withColumn(
    "PATN_FIRST_NM",
    F.when((F.length(trim(F.col("PATN_FIRST_NM"))) == 0) | (F.col("PATN_FIRST_NM").isNull()), F.lit(""))
     .otherwise(F.substring(UpCase(trim(F.col("PATN_FIRST_NM"))), 1, 50))
).withColumn(
    "PATN_MID_NM",
    F.when((F.length(trim(F.col("PATN_MID_NM"))) == 0) | (F.col("PATN_MID_NM").isNull()), F.lit(""))
     .otherwise(F.substring(UpCase(trim(F.col("PATN_MID_NM"))), 1, 50))
).withColumn(
    "MBI",
    F.when((F.length(trim(F.col("MBI"))) == 0) | (F.col("MBI").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("MBI")), 1, 11))
).withColumn(
    "DOB",
    F.when((F.length(trim(F.col("DOB"))) == 0) | (F.col("DOB").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("DOB")), 1, 8))
).withColumn(
    "GNDR",
    F.when((F.length(trim(F.col("GNDR"))) == 0) | (F.col("GNDR").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("GNDR")), 1, 1))
).withColumn(
    "PROC_TYPE",
    F.when((F.length(trim(F.col("PROC_TYPE"))) == 0) | (F.col("PROC_TYPE").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("PROC_TYPE")), 1, 50))
).withColumn(
    "DT_OF_SVC",
    F.when((F.length(trim(F.col("DT_OF_SVC"))) == 0) | (F.col("DT_OF_SVC").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("DT_OF_SVC")), 1, 8))
).withColumn(
    "RSLT_VAL",
    F.when((F.length(trim(F.col("RSLT_VAL"))) == 0) | (F.col("RSLT_VAL").isNull()), F.lit(""))
     .otherwise(F.substring( EReplace(trim(F.col("RSLT_VAL")), '"', ''), 1, 25 ))
).withColumn(
    "RNDR_NTNL_PROV_ID",
    F.when((F.length(trim(F.col("RNDR_NTNL_PROV_ID"))) == 0) | (F.col("RNDR_NTNL_PROV_ID").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("RNDR_NTNL_PROV_ID")), 1, 10))
).withColumn(
    "RNDR_PROV_TYP",
    F.when((F.length(trim(F.col("RNDR_PROV_TYP"))) == 0) | (F.col("RNDR_PROV_TYP").isNull()), F.lit(""))
     .otherwise(F.substring(trim(F.col("RNDR_PROV_TYP")), 1, 25))
).withColumn(
    "PROC_CD_CPT",
    F.when((F.length(trim(F.col("PROC_CD_CPT"))) == 0) | (F.col("PROC_CD_CPT").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("PROC_CD_CPT"))))
).withColumn(
    "PROC_CD_CPT_MOD_1",
    F.when((F.length(trim(F.col("PROC_CD_CPT_MOD_1"))) == 0) | (F.col("PROC_CD_CPT_MOD_1").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("PROC_CD_CPT_MOD_1"))))
).withColumn(
    "PROC_CD_CPT_MOD_2",
    F.when((F.length(trim(F.col("PROC_CD_CPT_MOD_2"))) == 0) | (F.col("PROC_CD_CPT_MOD_2").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("PROC_CD_CPT_MOD_2"))))
).withColumn(
    "PROC_CD_CPTII",
    F.when((F.length(trim(F.col("PROC_CD_CPTII"))) == 0) | (F.col("PROC_CD_CPTII").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("PROC_CD_CPTII"))))
).withColumn(
    "PROC_CD_CPTII_MOD_1",
    F.when((F.length(trim(F.col("PROC_CD_CPTII_MOD_1"))) == 0) | (F.col("PROC_CD_CPTII_MOD_1").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("PROC_CD_CPTII_MOD_1"))))
).withColumn(
    "PROC_CD_CPTII_MOD_2",
    F.when((F.length(trim(F.col("PROC_CD_CPTII_MOD_2"))) == 0) | (F.col("PROC_CD_CPTII_MOD_2").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("PROC_CD_CPTII_MOD_2"))))
).withColumn(
    "HCPS",
    F.when((F.length(trim(F.col("HCPS"))) == 0) | (F.col("HCPS").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("HCPS"))))
).withColumn(
    "ICD_10_PCS_1",
    F.when((F.length(trim(F.col("ICD_10_PCS_1"))) == 0) | (F.col("ICD_10_PCS_1").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("ICD_10_PCS_1"))))
).withColumn(
    "SNOMED",
    F.when((F.length(trim(F.col("SNOMED"))) == 0) | (F.col("SNOMED").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("SNOMED"))))
).withColumn(
    "CVX",
    F.when((F.length(trim(F.col("CVX"))) == 0) | (F.col("CVX").isNull()), F.lit(""))
     .otherwise(UpCase(trim(F.col("CVX"))))
).withColumn(
    "SOURCE_ID",
    F.when((F.length(trim(F.col("SOURCE_ID"))) == 0) | (F.col("SOURCE_ID").isNull()), F.lit(""))
     .otherwise(F.substring(UpCase(trim(F.col("SOURCE_ID"))), 1, 50))
)

# LookupSub (PxLookup)
df_lookupSub_joined = df_Cleanse.alias("source").join(
    df_SUB.alias("IdsSub"),
    F.col("source.POL_NO") == F.col("IdsSub.SUB_ID"),
    "left"
)

df_sort = df_lookupSub_joined.filter(
    F.col("IdsSub.SUB_ID").isNotNull()
).select(
    F.col("source.POL_NO").alias("POL_NO"),
    F.col("source.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("source.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("source.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("source.MBI").alias("MBI"),
    F.col("source.DOB").alias("DOB"),
    F.col("source.GNDR").alias("GNDR"),
    F.col("source.PROC_TYPE").alias("PROC_TYPE"),
    F.col("source.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("source.RSLT_VAL").alias("RSLT_VAL"),
    F.col("source.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("source.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("source.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("source.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("source.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("source.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("source.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("source.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("source.HCPS").alias("HCPS"),
    F.col("source.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("source.SNOMED").alias("SNOMED"),
    F.col("source.CVX").alias("CVX"),
    F.col("source.SOURCE_ID").alias("SOURCE_ID")
)

df_NotOurMembers = df_lookupSub_joined.filter(
    F.col("IdsSub.SUB_ID").isNull()
).select(
    F.col("source.POL_NO"),
    F.col("source.PATN_LAST_NM"),
    F.col("source.PATN_FIRST_NM"),
    F.col("source.PATN_MID_NM"),
    F.col("source.MBI"),
    F.col("source.DOB"),
    F.col("source.GNDR"),
    F.col("source.PROC_TYPE"),
    F.col("source.DT_OF_SVC"),
    F.col("source.RSLT_VAL"),
    F.col("source.RNDR_NTNL_PROV_ID"),
    F.col("source.RNDR_PROV_TYP"),
    F.col("source.PROC_CD_CPT"),
    F.col("source.PROC_CD_CPT_MOD_1"),
    F.col("source.PROC_CD_CPT_MOD_2"),
    F.col("source.PROC_CD_CPTII"),
    F.col("source.PROC_CD_CPTII_MOD_1"),
    F.col("source.PROC_CD_CPTII_MOD_2"),
    F.col("source.HCPS"),
    F.col("source.ICD_10_PCS_1"),
    F.col("source.SNOMED"),
    F.col("source.CVX"),
    F.col("source.SOURCE_ID")
)

# Write NotOurMembers
df_NotOurMembers_final = df_NotOurMembers.select(
    *[F.rpad(col, 1, " ") for col in df_NotOurMembers.columns]  # Minimal default, no explicit lengths provided
)
write_files(
    df_NotOurMembers_final,
    f"{adls_path}/verified/{ProviderName}.PROCEDURE.Member_Sub_IDs_Not_Found.{RunID}.TXT",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# SortFileRecords (PxSort)
df_SortFileRecords = df_sort.orderBy(
    F.col("POL_NO").asc(),
    F.col("PATN_LAST_NM").asc(),
    F.col("PATN_FIRST_NM").asc(),
    F.col("DOB").asc(),
    F.col("PROC_CD_CPT").asc(),
    F.col("PROC_CD_CPTII").asc(),
    F.col("HCPS").asc(),
    F.col("SNOMED").asc(),
    F.col("CVX").asc(),
    F.col("DT_OF_SVC").asc(),
    F.col("RSLT_VAL").asc(),
    F.col("RNDR_NTNL_PROV_ID").asc()
)

# Clense (CTransformerStage) - remove consecutive duplicates
# We replicate the stage-variable logic using window with lag
windowSpecClense = (
    F.window("1 seconds").alias("window_none")  # dummy, won't actually use time window
)
# Instead, we use a typical partitionless window with orderBy
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

wClense = Window.orderBy("POL_NO","PATN_LAST_NM","PATN_FIRST_NM","DOB","PROC_CD_CPT","PROC_CD_CPTII","HCPS","SNOMED","CVX","DT_OF_SVC","RSLT_VAL","RNDR_NTNL_PROV_ID")

df_SortFileRecords_lag = df_SortFileRecords.withColumn("lag_POL_NO", F.lag("POL_NO",1).over(wClense)) \
    .withColumn("lag_PATN_LAST_NM", F.lag("PATN_LAST_NM",1).over(wClense)) \
    .withColumn("lag_PATN_FIRST_NM", F.lag("PATN_FIRST_NM",1).over(wClense)) \
    .withColumn("lag_DOB", F.lag("DOB",1).over(wClense)) \
    .withColumn("lag_PROC_CD_CPT", F.lag("PROC_CD_CPT",1).over(wClense)) \
    .withColumn("lag_PROC_CD_CPTII", F.lag("PROC_CD_CPTII",1).over(wClense)) \
    .withColumn("lag_HCPS", F.lag("HCPS",1).over(wClense)) \
    .withColumn("lag_SNOMED", F.lag("SNOMED",1).over(wClense)) \
    .withColumn("lag_CVX", F.lag("CVX",1).over(wClense)) \
    .withColumn("lag_DT_OF_SVC", F.lag("DT_OF_SVC",1).over(wClense)) \
    .withColumn("lag_RSLT_VAL", F.lag("RSLT_VAL",1).over(wClense)) \
    .withColumn("lag_RNDR_NTNL_PROV_ID", F.lag("RNDR_NTNL_PROV_ID",1).over(wClense))

df_Clense = df_SortFileRecords_lag.withColumn(
    "svIsDupeRow",
    F.when(
        (F.col("POL_NO") == F.col("lag_POL_NO")) &
        (F.col("PATN_LAST_NM") == F.col("lag_PATN_LAST_NM")) &
        (F.col("PATN_FIRST_NM") == F.col("lag_PATN_FIRST_NM")) &
        (F.col("DOB") == F.col("lag_DOB")) &
        (F.col("PROC_CD_CPT") == F.col("lag_PROC_CD_CPT")) &
        (F.col("PROC_CD_CPTII") == F.col("lag_PROC_CD_CPTII")) &
        (F.col("HCPS") == F.col("lag_HCPS")) &
        (F.col("SNOMED") == F.col("lag_SNOMED")) &
        (F.col("CVX") == F.col("lag_CVX")) &
        (F.col("DT_OF_SVC") == F.col("lag_DT_OF_SVC")) &
        (F.col("RSLT_VAL") == F.col("lag_RSLT_VAL")) &
        (F.col("RNDR_NTNL_PROV_ID") == F.col("lag_RNDR_NTNL_PROV_ID")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

df_Clense_cleansed = df_Clense.filter(F.col("svIsDupeRow") == "N").select(
    F.trim(F.col("POL_NO")).alias("POL_NO"),
    F.when(F.length(F.trim(F.col("PATN_LAST_NM"))) == 0, F.lit("")).otherwise(F.substring(UpCase(F.trim(F.col("PATN_LAST_NM"))),1,75)).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(F.trim(F.col("PATN_FIRST_NM"))) == 0, F.lit("")).otherwise(F.substring(UpCase(F.trim(F.col("PATN_FIRST_NM"))),1,35)).alias("LoadFile_PATN_FIRST_NM"),
    UpCase(F.col("PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    F.trim(F.col("MBI")).alias("MBI"),
    F.when(F.length(F.trim(F.col("DOB"))) == 0, F.lit("")).otherwise(F.substring(EReplace(F.trim(F.col("DOB")),"-",""),1,8)).alias("LoadFile_DOB"),
    F.trim(F.col("GNDR")).alias("LoadFile_GNDR"),
    F.trim(F.col("PROC_TYPE")).alias("PROC_TYPE"),
    F.trim(F.col("DT_OF_SVC")).alias("DT_OF_SVC"),
    F.trim(F.col("RSLT_VAL")).alias("RSLT_VAL"),
    F.trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.trim(F.col("RNDR_PROV_TYP")).alias("RNDR_PROV_TYP"),
    UpCase(F.substring(F.trim(F.col("PATN_FIRST_NM")),1,3)).alias("LoadFile_FirstNameTrunc"),
    UpCase(F.substring(F.trim(F.col("PATN_LAST_NM")),1,3)).alias("LoadFile_LastNameTrunc"),
    UpCase(F.substring(F.trim(F.col("PATN_FIRST_NM")),1,1)).alias("LoadFile_FirstInitial"),
    F.when(F.length(F.col("DOB"))==0, F.lit("")).otherwise(F.substring(F.col("DOB"),1,4)).alias("LoadFile_YearOfBirth"),
    F.col("PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("HCPS").alias("HCPS"),
    F.col("ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("SNOMED").alias("SNOMED"),
    F.col("CVX").alias("CVX"),
    F.trim(F.col("SOURCE_ID")).alias("LoadFile_SOURCE_ID")
)

df_Clense_dupeRows = df_Clense.filter(F.col("svIsDupeRow") == "Y").select(
    F.trim(F.col("POL_NO")).alias("POL_NO"),
    F.trim(F.col("PATN_LAST_NM")).alias("PATN_LAST_NM"),
    F.trim(F.col("PATN_FIRST_NM")).alias("PATN_FIRST_NM"),
    F.trim(F.col("PATN_MID_NM")).alias("PATN_MID_NM"),
    F.trim(F.col("MBI")).alias("MBI"),
    F.trim(F.col("DOB")).alias("DOB"),
    F.trim(F.col("GNDR")).alias("GNDR"),
    F.trim(F.col("PROC_TYPE")).alias("PROC_TYPE"),
    F.trim(F.col("DT_OF_SVC")).alias("DT_OF_SVC"),
    F.trim(F.col("RSLT_VAL")).alias("RSLT_VAL"),
    F.trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.trim(F.col("RNDR_PROV_TYP")).alias("RNDR_PROV_TYP"),
    F.col("PROC_CD_CPT"),
    F.col("PROC_CD_CPT_MOD_1"),
    F.col("PROC_CD_CPT_MOD_2"),
    F.col("PROC_CD_CPTII"),
    F.col("PROC_CD_CPTII_MOD_1"),
    F.col("PROC_CD_CPTII_MOD_2"),
    F.col("HCPS"),
    F.col("ICD_10_PCS_1"),
    F.col("SNOMED"),
    F.col("CVX"),
    F.trim(F.col("SOURCE_ID")).alias("SOURCE_ID")
)

# DupeRowsFile
df_Clense_dupeRows_final = df_Clense_dupeRows.select(
    *[F.rpad(col, 1, " ") for col in df_Clense_dupeRows.columns]
)
write_files(
    df_Clense_dupeRows_final,
    f"{adls_path}/verified/{ProviderName}.PROCEDURE.Duplicate_Rows_In_Source_File.{RunID}.TXT",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# MBR (DB2ConnectorPX)
extract_query_MBR = (
    f"SELECT DISTINCT upper(sub.SUB_ID) as SUB_ID, "
    f"upper(mbr.LAST_NM) as LAST_NM, upper(mbr.FIRST_NM) as FIRST_NM, "
    f"mbr.BRTH_DT_SK, mbr.MBR_UNIQ_KEY "
    f"FROM {IDSOwner}.MBR mbr, {IDSOwner}.SUB sub "
    f"WHERE mbr.SUB_SK = sub.SUB_SK"
)
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MBR)
    .load()
)

# TruncNames (CTransformerStage)
df_TruncNames = df_MBR.withColumn(
    "svLastNameTrunc",
    UpCase(F.substring(F.trim(F.col("LAST_NM")),1,3))
).withColumn(
    "svFirstNameTrunc",
    UpCase(F.substring(F.trim(F.col("FIRST_NM")),1,3))
).withColumn(
    "svFirstInitial",
    UpCase(F.substring(F.trim(F.col("FIRST_NM")),1,1))
).withColumn(
    "svYearOfBirth",
    F.substring(F.col("BRTH_DT_SK"),1,4)
).withColumn(
    "svDOB",
    F.when(
        (F.trim(F.col("BRTH_DT_SK")) == F.lit("NA")) | (F.trim(F.col("BRTH_DT_SK")) == F.lit("UNK")),
        F.lit("")
    ).otherwise(EReplace(F.trim(F.col("BRTH_DT_SK")),"-",""))
)

df_TruncNames_Level1 = df_TruncNames.select(
    F.when(F.length(F.col("SUB_ID"))==0,F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_TruncNames_Level2 = df_TruncNames.select(
    F.when(F.length(F.col("SUB_ID"))==0,F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("svFirstNameTrunc").alias("MBR_FirstNameTrunc"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_TruncNames_Level3 = df_TruncNames.select(
    F.when(F.length(F.col("SUB_ID"))==0,F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("svLastNameTrunc").alias("MBR_LastNameTrunc"),
    F.col("svFirstNameTrunc").alias("MBR_FirstNameTrunc"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# Lookup1 (PxLookup)
df_cleansed = df_Clense_cleansed.alias("cleansed")
df_Level1 = df_TruncNames_Level1.alias("Level1")

cond_lookup1 = [
    F.col("cleansed.POL_NO") == F.col("Level1.MBR_SUB_ID"),
    F.col("cleansed.LoadFile_PATN_LAST_NM") == F.col("Level1.MBR_LAST_NM"),
    F.col("cleansed.LoadFile_PATN_FIRST_NM") == F.col("Level1.MBR_FIRST_NM"),
    F.col("cleansed.LoadFile_DOB") == F.col("Level1.MBR_BRTH_DT_SK")
]

df_lookup1_joined = df_cleansed.join(df_Level1, cond_lookup1, "left")

# Separate matched vs. unmatched
df_lookup1_matched = df_lookup1_joined.filter(F.col("Level1.MBR_SUB_ID").isNotNull()).select(
    F.col("cleansed.POL_NO").alias("POL_NO"),
    F.col("Level1.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level1.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("cleansed.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("cleansed.MBI").alias("MBI"),
    F.col("Level1.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("cleansed.LoadFile_GNDR").alias("GNDR"),
    F.col("cleansed.PROC_TYPE").alias("PROC_TYPE"),
    F.col("cleansed.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("cleansed.RSLT_VAL").alias("RSLT_VAL"),
    F.col("cleansed.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("cleansed.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("cleansed.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("cleansed.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("cleansed.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("cleansed.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    F.col("cleansed.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("cleansed.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("cleansed.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("cleansed.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("cleansed.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("cleansed.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("cleansed.HCPS").alias("HCPS"),
    F.col("cleansed.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("cleansed.SNOMED").alias("SNOMED"),
    F.col("cleansed.CVX").alias("CVX"),
    F.col("cleansed.LoadFile_SOURCE_ID").alias("SOURCE_ID"),
    F.col("Level1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit("Level One").alias("MemberMatchLevel")
)

df_lookup1_unmatched = df_lookup1_joined.filter(F.col("Level1.MBR_SUB_ID").isNull())

# That unmatched goes to next lookup2 with "rej1" name
df_lookup1_unmatched_forLookup2 = df_lookup1_unmatched.select(
    F.trim(F.col("cleansed.POL_NO")).alias("POL_NO"),
    F.when(F.length(F.trim(F.col("cleansed.LoadFile_PATN_LAST_NM")))==0,F.lit("")).otherwise(F.substring(UpCase(F.trim(F.col("cleansed.LoadFile_PATN_LAST_NM"))),1,75)).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(F.trim(F.col("cleansed.LoadFile_PATN_FIRST_NM")))==0,F.lit("")).otherwise(F.substring(UpCase(F.trim(F.col("cleansed.LoadFile_PATN_FIRST_NM"))),1,35)).alias("LoadFile_PATN_FIRST_NM"),
    UpCase(F.col("cleansed.LoadFile_PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    F.trim(F.col("cleansed.MBI")).alias("MBI"),
    F.when(F.length(F.trim(F.col("cleansed.LoadFile_DOB"))) == 0, F.lit("")).otherwise(F.substring(EReplace(F.trim(F.col("cleansed.LoadFile_DOB")),"-",""),1,8)).alias("LoadFile_DOB"),
    F.trim(F.col("cleansed.LoadFile_GNDR")).alias("LoadFile_GNDR"),
    F.trim(F.col("cleansed.PROC_TYPE")).alias("PROC_TYPE"),
    F.trim(F.col("cleansed.DT_OF_SVC")).alias("DT_OF_SVC"),
    F.trim(F.col("cleansed.RSLT_VAL")).alias("RSLT_VAL"),
    F.trim(F.col("cleansed.RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.trim(F.col("cleansed.RNDR_PROV_TYP")).alias("RNDR_PROV_TYP"),
    UpCase(F.substring(F.trim(F.col("cleansed.LoadFile_PATN_FIRST_NM")),1,3)).alias("LoadFile_FirstNameTrunc"),
    UpCase(F.substring(F.trim(F.col("cleansed.LoadFile_PATN_LAST_NM")),1,3)).alias("LoadFile_LastNameTrunc"),
    UpCase(F.substring(F.trim(F.col("cleansed.LoadFile_PATN_FIRST_NM")),1,1)).alias("LoadFile_FirstInitial"),
    F.substring(F.col("cleansed.LoadFile_DOB"),1,4).alias("LoadFile_YearOfBirth"),
    F.col("cleansed.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("cleansed.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("cleansed.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("cleansed.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("cleansed.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("cleansed.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("cleansed.HCPS").alias("HCPS"),
    F.col("cleansed.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("cleansed.SNOMED").alias("SNOMED"),
    F.col("cleansed.CVX").alias("CVX"),
    F.trim(F.col("cleansed.LoadFile_SOURCE_ID")).alias("LoadFile_SOURCE_ID")
).alias("rej1")

# Lookup2 (PxLookup)
df_Level2 = df_TruncNames_Level2.alias("Level2")
cond_lookup2 = [
    F.col("rej1.POL_NO") == F.col("Level2.MBR_SUB_ID"),
    F.col("rej1.LoadFile_PATN_LAST_NM") == F.col("Level2.MBR_LAST_NM"),
    F.col("rej1.LoadFile_DOB") == F.col("Level2.MBR_BRTH_DT_SK"),
    F.col("rej1.LoadFile_FirstNameTrunc") == F.col("Level2.MBR_FirstNameTrunc")
]
df_lookup2_joined = df_lookup1_unmatched_forLookup2.join(df_Level2, cond_lookup2, "left")

df_lookup2_matched = df_lookup2_joined.filter(F.col("Level2.MBR_SUB_ID").isNotNull()).select(
    F.col("rej1.POL_NO").alias("POL_NO"),
    F.col("Level2.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level2.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej1.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("rej1.MBI").alias("MBI"),
    F.col("Level2.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("rej1.LoadFile_GNDR").alias("GNDR"),
    F.col("rej1.PROC_TYPE").alias("PROC_TYPE"),
    F.col("rej1.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("rej1.RSLT_VAL").alias("RSLT_VAL"),
    F.col("rej1.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("rej1.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("rej1.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("rej1.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("rej1.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("rej1.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    F.col("rej1.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("rej1.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("rej1.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("rej1.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("rej1.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("rej1.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("rej1.HCPS").alias("HCPS"),
    F.col("rej1.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("rej1.SNOMED").alias("SNOMED"),
    F.col("rej1.CVX").alias("CVX"),
    F.col("rej1.LoadFile_SOURCE_ID").alias("SOURCE_ID"),
    F.col("Level2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit("Level Two").alias("MemberMatchLevel")
)

df_lookup2_unmatched = df_lookup2_joined.filter(F.col("Level2.MBR_SUB_ID").isNull())

df_lookup2_unmatched_forLookup3 = df_lookup2_unmatched.select(
    F.trim(F.col("rej1.POL_NO")).alias("POL_NO"),
    F.when(F.length(F.trim(F.col("rej1.LoadFile_PATN_LAST_NM")))==0,F.lit("")).otherwise(F.substring(UpCase(F.trim(F.col("rej1.LoadFile_PATN_LAST_NM"))),1,75)).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(F.trim(F.col("rej1.LoadFile_PATN_FIRST_NM")))==0,F.lit("")).otherwise(F.substring(UpCase(F.trim(F.col("rej1.LoadFile_PATN_FIRST_NM"))),1,35)).alias("LoadFile_PATN_FIRST_NM"),
    UpCase(F.col("rej1.LoadFile_PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    F.trim(F.col("rej1.MBI")).alias("MBI"),
    F.when(F.length(F.trim(F.col("rej1.LoadFile_DOB"))) == 0, F.lit("")).otherwise(F.substring(EReplace(F.trim(F.col("rej1.LoadFile_DOB")),"-",""),1,8)).alias("LoadFile_DOB"),
    F.trim(F.col("rej1.LoadFile_GNDR")).alias("LoadFile_GNDR"),
    F.trim(F.col("rej1.PROC_TYPE")).alias("PROC_TYPE"),
    F.trim(F.col("rej1.DT_OF_SVC")).alias("DT_OF_SVC"),
    F.trim(F.col("rej1.RSLT_VAL")).alias("RSLT_VAL"),
    F.trim(F.col("rej1.RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.trim(F.col("rej1.RNDR_PROV_TYP")).alias("RNDR_PROV_TYP"),
    UpCase(F.substring(F.trim(F.col("rej1.LoadFile_PATN_FIRST_NM")),1,3)).alias("LoadFile_FirstNameTrunc"),
    UpCase(F.substring(F.trim(F.col("rej1.LoadFile_PATN_LAST_NM")),1,3)).alias("LoadFile_LastNameTrunc"),
    UpCase(F.substring(F.trim(F.col("rej1.LoadFile_PATN_FIRST_NM")),1,1)).alias("LoadFile_FirstInitial"),
    F.substring(F.col("rej1.LoadFile_DOB"),1,4).alias("LoadFile_YearOfBirth"),
    F.col("rej1.PROC_CD_CPT"),
    F.col("rej1.PROC_CD_CPT_MOD_1"),
    F.col("rej1.PROC_CD_CPT_MOD_2"),
    F.col("rej1.PROC_CD_CPTII"),
    F.col("rej1.PROC_CD_CPTII_MOD_1"),
    F.col("rej1.PROC_CD_CPTII_MOD_2"),
    F.col("rej1.HCPS"),
    F.col("rej1.ICD_10_PCS_1"),
    F.col("rej1.SNOMED"),
    F.col("rej1.CVX"),
    F.trim(F.col("rej1.LoadFile_SOURCE_ID")).alias("LoadFile_SOURCE_ID")
).alias("rej2")

# Lookup3 (PxLookup)
df_Level3 = df_TruncNames_Level3.alias("Level3")
cond_lookup3 = [
    F.col("rej2.POL_NO") == F.col("Level3.MBR_SUB_ID"),
    F.col("rej2.LoadFile_DOB") == F.col("Level3.MBR_BRTH_DT_SK"),
    F.col("rej2.LoadFile_LastNameTrunc") == F.col("Level3.MBR_LastNameTrunc"),
    F.col("rej2.LoadFile_FirstNameTrunc") == F.col("Level3.MBR_FirstNameTrunc")
]
df_lookup3_joined = df_lookup2_unmatched_forLookup3.join(df_Level3, cond_lookup3, "left")

df_lookup3_matched = df_lookup3_joined.filter(F.col("Level3.MBR_SUB_ID").isNotNull()).select(
    F.col("rej2.POL_NO").alias("POL_NO"),
    F.col("Level3.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level3.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej2.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("rej2.MBI").alias("MBI"),
    F.col("Level3.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("rej2.LoadFile_GNDR").alias("GNDR"),
    F.col("rej2.PROC_TYPE").alias("PROC_TYPE"),
    F.col("rej2.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("rej2.RSLT_VAL").alias("RSLT_VAL"),
    F.col("rej2.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("rej2.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("rej2.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("rej2.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("rej2.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("rej2.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    F.col("rej2.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("rej2.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("rej2.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("rej2.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("rej2.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("rej2.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("rej2.HCPS").alias("HCPS"),
    F.col("rej2.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("rej2.SNOMED").alias("SNOMED"),
    F.col("rej2.CVX").alias("CVX"),
    F.col("rej2.LoadFile_SOURCE_ID").alias("SOURCE_ID"),
    F.col("Level3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit("Level Three").alias("MemberMatchLevel")
)

df_lookup3_unmatched = df_lookup3_joined.filter(F.col("Level3.MBR_SUB_ID").isNull()).alias("rej3")

df_lookup3_unmatched_forRejReason3 = df_lookup3_unmatched.select(
    F.col("rej3.POL_NO").alias("POL_NO"),
    F.col("rej3.LoadFile_PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("rej3.LoadFile_PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej3.LoadFile_DOB").alias("DOB"),
    F.lit("All Member Matches Failed").alias("ErrorReason"),
    F.lit("None").alias("MemberMatchLevel")
).alias("rej3_out")

# RejReason3 (CTransformerStage) simply passes them
df_RejReason3 = df_lookup3_unmatched_forRejReason3
# Output to FunnelErrors as "RejectsFromLookup3"

# Transformer1 => pass df_lookup1_matched to FunnelMatches
df_Transformer1 = df_lookup1_matched

# Transformer2 => pass df_lookup2_matched to FunnelMatches
df_Transformer2 = df_lookup2_matched

# Transformer3 => pass df_lookup3_matched to FunnelMatches
df_Transformer3 = df_lookup3_matched

# FunnelMatches (PxFunnel) => union of the three matched
df_FunnelMatches = df_Transformer1.unionByName(df_Transformer2).unionByName(df_Transformer3)

# SortFunRecs (PxSort)
df_SortFunRecs = df_FunnelMatches.orderBy(
    F.col("POL_NO").asc(),
    F.col("MBR_UNIQ_KEY").asc(),
    F.col("PROC_TYPE").asc(),
    F.col("DT_OF_SVC").asc(),
    F.col("RSLT_VAL").asc(),
    F.col("RNDR_NTNL_PROV_ID").asc(),
    F.col("PROC_CD_CPT").asc(),
    F.col("PROC_CD_CPTII").asc(),
    F.col("HCPS").asc(),
    F.col("ICD_10_PCS_1").asc(),
    F.col("SNOMED").asc(),
    F.col("CVX").asc()
)

df_CountRows = df_SortFunRecs
# We again replicate a stage variable approach to separate duplicates
wCountRows = Window.orderBy("POL_NO","MBR_UNIQ_KEY","PROC_TYPE","DT_OF_SVC","RSLT_VAL","RNDR_NTNL_PROV_ID","PROC_CD_CPT","PROC_CD_CPTII","HCPS","ICD_10_PCS_1","SNOMED","CVX")
df_CountRows_lag = df_CountRows \
    .withColumn("lag_POL_NO", F.lag("POL_NO",1).over(wCountRows)) \
    .withColumn("lag_MBR_UNIQ_KEY", F.lag("MBR_UNIQ_KEY",1).over(wCountRows)) \
    .withColumn("lag_PROC_TYPE", F.lag("PROC_TYPE",1).over(wCountRows)) \
    .withColumn("lag_DT_OF_SVC", F.lag("DT_OF_SVC",1).over(wCountRows)) \
    .withColumn("lag_RSLT_VAL", F.lag("RSLT_VAL",1).over(wCountRows)) \
    .withColumn("lag_RNDR_NTNL_PROV_ID", F.lag("RNDR_NTNL_PROV_ID",1).over(wCountRows)) \
    .withColumn("lag_PROC_CD_CPT", F.lag("PROC_CD_CPT",1).over(wCountRows)) \
    .withColumn("lag_PROC_CD_CPTII", F.lag("PROC_CD_CPTII",1).over(wCountRows)) \
    .withColumn("lag_HCPS", F.lag("HCPS",1).over(wCountRows)) \
    .withColumn("lag_ICD_10_PCS_1", F.lag("ICD_10_PCS_1",1).over(wCountRows)) \
    .withColumn("lag_SNOMED", F.lag("SNOMED",1).over(wCountRows)) \
    .withColumn("lag_CVX", F.lag("CVX",1).over(wCountRows))

df_CountRows_out = df_CountRows_lag.withColumn("svIsDuplicateRow",
    F.when(
        (F.col("POL_NO")==F.col("lag_POL_NO")) &
        (F.col("MBR_UNIQ_KEY")==F.col("lag_MBR_UNIQ_KEY")) &
        (F.col("PROC_TYPE")==F.col("lag_PROC_TYPE")) &
        (F.col("DT_OF_SVC")==F.col("lag_DT_OF_SVC")) &
        (F.col("RSLT_VAL")==F.col("lag_RSLT_VAL")) &
        (F.col("RNDR_NTNL_PROV_ID")==F.col("lag_RNDR_NTNL_PROV_ID")) &
        (F.col("PROC_CD_CPT")==F.col("lag_PROC_CD_CPT")) &
        (F.col("PROC_CD_CPTII")==F.col("lag_PROC_CD_CPTII")) &
        (F.col("HCPS")==F.col("lag_HCPS")) &
        (F.col("ICD_10_PCS_1")==F.col("lag_ICD_10_PCS_1")) &
        (F.col("SNOMED")==F.col("lag_SNOMED")) &
        (F.col("CVX")==F.col("lag_CVX")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

df_sepDupes = df_CountRows_out.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.col("DOB"),
    F.col("GNDR"),
    F.col("PROC_TYPE"),
    F.col("DT_OF_SVC"),
    F.col("RSLT_VAL"),
    F.col("RNDR_NTNL_PROV_ID"),
    F.col("RNDR_PROV_TYP"),
    F.col("svIsDuplicateRow").alias("IS_DUPLICATE_ROW"),
    F.col("PROC_CD_CPT"),
    F.col("PROC_CD_CPT_MOD_1"),
    F.col("PROC_CD_CPT_MOD_2"),
    F.col("PROC_CD_CPTII"),
    F.col("PROC_CD_CPTII_MOD_1"),
    F.col("PROC_CD_CPTII_MOD_2"),
    F.col("HCPS"),
    F.col("ICD_10_PCS_1"),
    F.col("SNOMED"),
    F.col("CVX"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MemberMatchLevel")
).alias("sepDupes")

# IdentifyDuplicates (CTransformerStage)
df_dupes = df_sepDupes.filter(F.col("IS_DUPLICATE_ROW")=="Y").select(
    F.col("sepDupes.POL_NO").alias("POL_NO"),
    F.col("sepDupes.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("sepDupes.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("sepDupes.DOB").alias("DOB"),
    F.col("sepDupes.PROC_TYPE").alias("PROC_TYPE"),
    F.col("sepDupes.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("sepDupes.RSLT_VAL").alias("RSLT_VAL"),
    F.col("sepDupes.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID")
).alias("dupes")

df_subtractDupes = df_sepDupes.select(
    F.col("sepDupes.POL_NO"),
    F.col("sepDupes.PATN_LAST_NM"),
    F.col("sepDupes.PATN_FIRST_NM"),
    F.col("sepDupes.PATN_MID_NM"),
    F.col("sepDupes.MBI"),
    F.col("sepDupes.DOB"),
    F.col("sepDupes.GNDR"),
    F.col("sepDupes.PROC_TYPE"),
    F.col("sepDupes.DT_OF_SVC"),
    F.col("sepDupes.RSLT_VAL"),
    F.col("sepDupes.RNDR_NTNL_PROV_ID"),
    F.col("sepDupes.RNDR_PROV_TYP"),
    F.col("sepDupes.IS_DUPLICATE_ROW"),
    F.col("sepDupes.PROC_CD_CPT"),
    F.col("sepDupes.PROC_CD_CPT_MOD_1"),
    F.col("sepDupes.PROC_CD_CPT_MOD_2"),
    F.col("sepDupes.PROC_CD_CPTII"),
    F.col("sepDupes.PROC_CD_CPTII_MOD_1"),
    F.col("sepDupes.PROC_CD_CPTII_MOD_2"),
    F.col("sepDupes.HCPS"),
    F.col("sepDupes.ICD_10_PCS_1"),
    F.col("sepDupes.SNOMED"),
    F.col("sepDupes.CVX"),
    F.col("sepDupes.SOURCE_ID"),
    F.col("sepDupes.MBR_UNIQ_KEY"),
    F.col("sepDupes.MemberMatchLevel")
).alias("SubtractDupes")

# Duplicates (CTransformerStage)
df_dupes2 = df_dupes.select(
    F.col("dupes.POL_NO").alias("POL_NO"),
    F.col("dupes.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("dupes.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("dupes.DOB").alias("DOB"),
    F.col("dupes.PROC_TYPE").alias("PROC_TYPE"),
    F.col("dupes.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("dupes.RSLT_VAL").alias("RSLT_VAL"),
    F.col("dupes.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID")
).alias("dupes2")

# LookupDupes (PxLookup)
cond_lookupDupes = [
    F.col("SubtractDupes.POL_NO") == F.col("dupes2.POL_NO"),
    F.col("SubtractDupes.PROC_TYPE") == F.col("dupes2.PROC_TYPE"),
    F.col("SubtractDupes.DT_OF_SVC") == F.col("dupes2.DT_OF_SVC"),
    F.col("SubtractDupes.RSLT_VAL") == F.col("dupes2.RSLT_VAL"),
    F.col("SubtractDupes.RNDR_NTNL_PROV_ID") == F.col("dupes2.RNDR_NTNL_PROV_ID")
]
df_lookupDupes_joined = df_subtractDupes.join(df_dupes2, cond_lookupDupes, "left")

df_lookupDupes_matched = df_lookupDupes_joined.filter(F.col("dupes2.POL_NO").isNotNull()).alias("AllTheDupes")
df_lookupDupes_unmatched = df_lookupDupes_joined.filter(F.col("dupes2.POL_NO").isNull()).alias("sepDupes")

df_AllTheDupes = df_lookupDupes_matched.select(
    F.col("SubtractDupes.POL_NO").alias("POL_NO"),
    F.col("SubtractDupes.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("SubtractDupes.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("SubtractDupes.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("SubtractDupes.MBI").alias("MBI"),
    F.col("SubtractDupes.DOB").alias("DOB"),
    F.col("SubtractDupes.GNDR").alias("GNDR"),
    F.col("SubtractDupes.PROC_TYPE").alias("PROC_TYPE"),
    F.col("SubtractDupes.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("SubtractDupes.RSLT_VAL").alias("RSLT_VAL"),
    F.col("SubtractDupes.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("SubtractDupes.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("SubtractDupes.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("SubtractDupes.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("SubtractDupes.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("SubtractDupes.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("SubtractDupes.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("SubtractDupes.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("SubtractDupes.HCPS").alias("HCPS"),
    F.col("SubtractDupes.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("SubtractDupes.SNOMED").alias("SNOMED"),
    F.col("SubtractDupes.CVX").alias("CVX"),
    F.col("SubtractDupes.SOURCE_ID").alias("SOURCE_ID"),
    F.col("SubtractDupes.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SubtractDupes.MemberMatchLevel").alias("MemberMatchLevel")
)

df_ClenseOutput = df_lookupDupes_unmatched.select(
    F.col("sepDupes.POL_NO").alias("POL_NO"),
    F.col("sepDupes.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("sepDupes.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("sepDupes.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("sepDupes.MBI").alias("MBI"),
    F.col("sepDupes.DOB").alias("DOB"),
    F.col("sepDupes.GNDR").alias("GNDR"),
    F.col("sepDupes.PROC_TYPE").alias("PROC_TYPE"),
    F.col("sepDupes.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("sepDupes.RSLT_VAL").alias("RSLT_VAL"),
    F.col("sepDupes.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("sepDupes.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("sepDupes.IS_DUPLICATE_ROW").alias("IS_DUPLICATE_ROW"),
    F.col("sepDupes.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("sepDupes.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("sepDupes.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("sepDupes.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("sepDupes.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("sepDupes.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("sepDupes.HCPS").alias("HCPS"),
    F.col("sepDupes.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("sepDupes.SNOMED").alias("SNOMED"),
    F.col("sepDupes.CVX").alias("CVX"),
    F.col("sepDupes.SOURCE_ID").alias("SOURCE_ID"),
    F.col("sepDupes.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("sepDupes.MemberMatchLevel").alias("MemberMatchLevel")
).alias("output")

# ClenseOutput (CTransformerStage)
df_finaloutput = df_ClenseOutput.select(
    F.col("output.POL_NO").alias("POL_NO"),
    F.col("output.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("output.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("output.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("output.MBI").alias("MBI"),
    F.col("output.DOB").alias("DOB"),
    F.col("output.GNDR").alias("GNDR"),
    F.col("output.PROC_TYPE").alias("PROC_TYPE"),
    F.col("output.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("output.RSLT_VAL").alias("RSLT_VAL"),
    F.col("output.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("output.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("output.PROC_CD_CPT").alias("PROC_CD_CPT"),
    F.col("output.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
    F.col("output.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
    F.col("output.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
    F.col("output.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
    F.col("output.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
    F.col("output.HCPS").alias("HCPS"),
    F.col("output.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
    F.col("output.SNOMED").alias("SNOMED"),
    F.col("output.CVX").alias("CVX"),
    F.col("output.SOURCE_ID").alias("SOURCE_ID"),
    F.col("output.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("output.MemberMatchLevel").alias("MemberMatchLevel")
)

df_finaloutput_rpad = df_finaloutput.select(
    *[F.rpad(col, 1, " ") for col in df_finaloutput.columns]  # Minimal default due to unknown fixed lengths
)

# ProcedureFile (PxSequentialFile)
write_files(
    df_finaloutput_rpad,
    f"{adls_path_raw}/landing/{InFile}_MBRMATCHFINAL",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# TooManyMatches (CTransformerStage)
# This stage reads from df_AllTheDupes => merges into funnel of errors with reason 'Too Many Member Matches'
wTooManyMatches = Window.orderBy("POL_NO","PATN_LAST_NM","PATN_FIRST_NM","DOB")
df_AllTheDupes_sorted = df_AllTheDupes.orderBy(
    F.col("POL_NO").asc(),
    F.col("PATN_LAST_NM").asc(),
    F.col("PATN_FIRST_NM").asc(),
    F.col("DOB").asc()
)
df_TooManyMatches = df_AllTheDupes_sorted.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DOB").alias("DOB"),
    F.lit("Too Many Member Matches").alias("ErrorReason"),
    F.col("MemberMatchLevel").alias("MemberMatchLevel")
)

# FunnelErrors (PxFunnel) => union of "TooManyMatches" and "RejectsFromLookup3"
df_FunnelErrors = df_TooManyMatches.unionByName(df_RejReason3)

df_ErrorFile = df_FunnelErrors.select(
    F.col("POL_NO").alias("SUB_ID"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DOB").alias("DOB"),
    F.col("ErrorReason").alias("ErrorReason"),
    F.col("MemberMatchLevel").alias("MemberMatchLevel")
)
df_ErrorFile_rpad = df_ErrorFile.select(
    *[F.rpad(col, 1, " ") for col in df_ErrorFile.columns]
)

# ErrorFile (PxSequentialFile)
write_files(
    df_ErrorFile_rpad,
    f"{adls_path}/verified/{ProviderName}.PROCEDURE.MEMBER_MATCH_ERRORS.{RunID}.TXT",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)