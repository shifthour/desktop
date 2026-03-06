# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2022 - 2024 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name: ProvClmSuplDiagMbrMatch
# MAGIC Called By: ProvClmSuplDiagMbrMatchSeq
# MAGIC Purpose: This job takes the Diagnosis v5.0 file sent to us by the Providers, does member matching logic compared to the IDS MBR table, and generates and updated version of the Diagnosis v5.0 file to be read by the ProvClmSuplDiagLand job.
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary  
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)PROJECT/\(9)\(9)\(9)\(9)\(9)\(9)\(9)DEVELOPMENT\(9)\(9)
# MAGIC DEVELOPER\(9)\(9)DATE\(9)\(9)STORY#\(9)\(9)CHANGE DESCRIPTION\(9)\(9)\(9)\(9)PROJECT\(9)\(9)\(9)\(9)CODE REVIEWER\(9)\(9)DATE REVIEWED
# MAGIC ==============================================================================================================================================================================================
# MAGIC Ken Bradmon\(9)\(9)2022-03-08\(9)us480738\(9)\(9)Original programming\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2022-03-31
# MAGIC Ken Bradmon\(9)\(9)2023-05-09\(9)us542805\(9)\(9)Added MBR_UNIQ_KEY and MBR_SK to the output file.\(9)IntegrateDev2\(9)\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2023-06-16\(9)
# MAGIC Ken Bradmon\(9)\(9)2024-03-12\(9)us611149\(9)\(9)Stripped out member match levels 4, 5, 6, and 7.\(9)\(9)IntegrateDev2                                                          Reddy Sanam                         2024-04-11\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also added the column MemberMatchLevel to the output
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)file so we can see which member match level was used with
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)each record.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also added MBR_UNIQ_KEY to the CountRows stage that    
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)determines if "too many" member matches were made.

# MAGIC Job Name: ProvClmSuplDiagMbrMatch
# MAGIC Called By: ProvClmSuplDiagMbrMatchSeq
# MAGIC Purpose: This job takes the Diagnosis v4.3 file sent to us by the Providers, does member matching logic compared to the IDS MBR table, and generates and updated version of the Diagnosis v4.3 file to be read by the ProvClmSuplDiagLand job.
# MAGIC In this case we don't want the ones that match in the lookup, so \"rejects\" are good.  The records that match are duplicates.
# MAGIC This is the original Diagnosis file from the provider.
# MAGIC /ids/prod/landing
# MAGIC <PROVIDER>.DIAG.CCYYMMDDhhmmss.TXT
# MAGIC The Clense stage removes duplicates based on:
# MAGIC + POL_NO
# MAGIC + PATN_LAST_NM
# MAGIC + PATN_FIRST_NM
# MAGIC + DOB
# MAGIC + GNDR 
# MAGIC + DT_OF_SVC
# MAGIC + RNDR_NTNL_PROV_ID
# MAGIC + DIAG_CD
# MAGIC + CNTRL_NO
# MAGIC This is the replacement "source" Diagnosis file:
# MAGIC /ids/prod/landing
# MAGIC <PROVIDER>.DIAG.CCYYMMDDhhmmss.TXT_MBRMATCHFINAL
# MAGIC /ids/prod/verified/
# MAGIC DIAG.MEMBER_MATCH_ERRORS.CCYYMMhhmmss.TXT
# MAGIC There are 3 scenarios for Member Matching, to maximize the number of Diagnosis records successfully matched to a single Member record from MBR_D.
# MAGIC Diagnosis records that fail to match get sent to the Error file. Diagnosis records that match more than one record from MBR_D also get sent to the Error file.
# MAGIC These are the source file records where all of the member matching levels 1 - 3 failed.
# MAGIC If we have more than one record "matched" from the MBR table, we want ALL of those records redirected to the error file, not just one of them.
# MAGIC So the "IdentifyDuplicates" stage sends the dupes to the "Duplicates" stage, then the "LookupDupes" only sends the REJECTED (non-matching) rows to the "ClenseOutput" stage, and all the matches go to the "TooManyMatches" stage, and then the ErrorFile.
# MAGIC These are records where the POL_NO does not match any SUB_ID in our IDS SUB table.
# MAGIC This sort and the calculation of the duplicate row flag in the next stage have changed from counting dupes at the beginning of the job, because the definition of what is a duplicate now does not include the patient name and DOB.
# MAGIC 
# MAGIC WHY - because at this point in the job, we can't be sure of the patient first name, last name and the DOB all matched our MBR table.  For example some member match steps matched on only EITHER the first name or the last name.
# MAGIC But that is 100% OKAY - this works.  The point of this is only to check if we got "too many" member matches.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
InFile = get_widget_value('InFile','')
ProviderName = get_widget_value('ProviderName','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ----------------------------------------------------------------------------
# DB2ConnectorPX Stage: SUB
# ----------------------------------------------------------------------------
jdbc_url_SUB, jdbc_props_SUB = get_db_config(ids_secret_name)
extract_query_sub = f"""SELECT DISTINCT upper(sub.SUB_ID) as SUB_ID
FROM {IDSOwner}.SUB sub
"""
df_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_SUB)
    .options(**jdbc_props_SUB)
    .option("query", extract_query_sub)
    .load()
)

# ----------------------------------------------------------------------------
# PxSequentialFile Stage: Raw_Diag_original (read input file)
# ----------------------------------------------------------------------------
schema_Raw_Diag_original = StructType([
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_LAST_NM", StringType(), True),
    StructField("RNDR_PROV_FIRST_NM", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CNTRL_NO", StringType(), True),
    StructField("SOURCE_ID", StringType(), True)
])
df_Raw_Diag_original = spark.read.csv(
    path=f"{adls_path_raw}/landing/{InFile}",
    schema=schema_Raw_Diag_original,
    sep="|",
    header=True,
    quote=None,
    nullValue=None
)

# ----------------------------------------------------------------------------
# CTransformerStage: Cleanse
# ----------------------------------------------------------------------------
df_Cleanse = df_Raw_Diag_original.select(
    F.when(
        (F.length(trim(F.col("POL_NO"))) == 0) | F.col("POL_NO").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("POL_NO"))).substr(F.lit(1), F.lit(20))).alias("POL_NO"),
    F.when(
        (F.length(trim(F.col("PATN_LAST_NM"))) == 0) | F.col("PATN_LAST_NM").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("PATN_LAST_NM")))).alias("PATN_LAST_NM"),
    F.when(
        (F.length(trim(F.col("PATN_FIRST_NM"))) == 0) | F.col("PATN_FIRST_NM").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("PATN_FIRST_NM")))).alias("PATN_FIRST_NM"),
    F.when(
        (F.length(trim(F.col("PATN_MID_NM"))) == 0) | F.col("PATN_MID_NM").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("PATN_MID_NM")))).alias("PATN_MID_NM"),
    F.when(
        (F.length(trim(F.col("MBI"))) == 0) | F.col("MBI").isNull(),
        F.lit("")
    ).otherwise(trim(F.col("MBI"))).alias("MBI"),
    F.when(
        (F.length(trim(F.col("DOB"))) == 0) | F.col("DOB").isNull(),
        F.lit("")
    ).otherwise(trim(F.col("DOB")).substr(F.lit(1), F.lit(8))).alias("DOB"),
    F.when(
        (F.length(trim(F.col("GNDR"))) == 0) | F.col("GNDR").isNull(),
        F.lit("")
    ).otherwise(trim(F.col("GNDR")).substr(F.lit(1), F.lit(1))).alias("GNDR"),
    F.when(
        (F.length(trim(F.col("DT_OF_SVC"))) == 0) | F.col("DT_OF_SVC").isNull(),
        F.lit("")
    ).otherwise(trim(F.col("DT_OF_SVC")).substr(F.lit(1), F.lit(8))).alias("DT_OF_SVC"),
    F.when(
        (F.length(trim(F.col("RNDR_NTNL_PROV_ID"))) == 0) | F.col("RNDR_NTNL_PROV_ID").isNull(),
        F.lit("")
    ).otherwise(trim(F.col("RNDR_NTNL_PROV_ID")).substr(F.lit(1), F.lit(10))).alias("RNDR_NTNL_PROV_ID"),
    F.when(
        (F.length(trim(F.col("RNDR_PROV_LAST_NM"))) == 0) | F.col("RNDR_PROV_LAST_NM").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("RNDR_PROV_LAST_NM")))).alias("RNDR_PROV_LAST_NM"),
    F.when(
        (F.length(trim(F.col("RNDR_PROV_FIRST_NM"))) == 0) | F.col("RNDR_PROV_FIRST_NM").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("RNDR_PROV_FIRST_NM")))).alias("RNDR_PROV_FIRST_NM"),
    F.when(
        (F.length(trim(F.col("DIAG_CD"))) == 0) | F.col("DIAG_CD").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("DIAG_CD")))).alias("DIAG_CD"),
    F.when(
        (F.length(trim(F.col("CNTRL_NO"))) == 0) | F.col("CNTRL_NO").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("CNTRL_NO")))).alias("CNTRL_NO"),
    F.when(
        (F.length(trim(F.col("SOURCE_ID"))) == 0) | F.col("SOURCE_ID").isNull(),
        F.lit("")
    ).otherwise(F.upper(trim(F.col("SOURCE_ID")))).alias("SOURCE_ID")
)

# ----------------------------------------------------------------------------
# PxLookup Stage: LookupSub  (left join with df_SUB)
# ----------------------------------------------------------------------------
df_LookupSubJoined = df_Cleanse.alias("source").join(
    df_SUB.alias("IdsSub"),
    F.col("source.POL_NO") == F.col("IdsSub.SUB_ID"),
    "left"
)

# Matched rows -> "sort"
df_sort = df_LookupSubJoined.filter(F.col("IdsSub.SUB_ID").isNotNull()).select(
    F.col("source.POL_NO").alias("POL_NO"),
    F.col("source.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("source.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("source.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("source.MBI").alias("MBI"),
    F.col("source.DOB").alias("DOB"),
    F.col("source.GNDR").alias("GNDR"),
    F.col("source.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("source.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("source.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("source.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("source.DIAG_CD").alias("DIAG_CD"),
    F.col("source.CNTRL_NO").alias("CNTRL_NO"),
    F.col("source.SOURCE_ID").alias("SOURCE_ID")
)

# Unmatched rows -> "NotOurMembers"
df_NotOurMembers = df_LookupSubJoined.filter(F.col("IdsSub.SUB_ID").isNull()).select(
    F.col("source.POL_NO").alias("POL_NO"),
    F.col("source.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("source.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("source.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("source.MBI").alias("MBI"),
    F.col("source.DOB").alias("DOB"),
    F.col("source.GNDR").alias("GNDR"),
    F.col("source.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("source.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("source.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("source.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("source.DIAG_CD").alias("DIAG_CD"),
    F.col("source.CNTRL_NO").alias("CNTRL_NO"),
    F.col("source.SOURCE_ID").alias("SOURCE_ID")
)

# PxSequentialFile Stage: NotOurMembers
notourmembers_output_path = f"{adls_path}/verified/{ProviderName}.DIAG.Member_Sub_IDs_Not_Found.{RunID}.TXT"
write_files(
    df_NotOurMembers.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR",
        "DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM",
        "DIAG_CD","CNTRL_NO","SOURCE_ID"
    ),
    notourmembers_output_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# PxSort Stage: SortFileRecords
# ----------------------------------------------------------------------------
df_SortFileRecords = df_sort.orderBy(
    "POL_NO", "PATN_LAST_NM", "PATN_FIRST_NM", "DOB", "GNDR", "DT_OF_SVC",
    "RNDR_NTNL_PROV_ID", "DIAG_CD", "CNTRL_NO"
)

# ----------------------------------------------------------------------------
# CTransformerStage: Clense
#    Implements row-by-row logic with stage variables (detect duplicates, etc.)
# ----------------------------------------------------------------------------
# Prepare window for referencing previous row
windowSpec_Clense = Window.orderBy(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","DOB","GNDR",
    "DT_OF_SVC","RNDR_NTNL_PROV_ID","DIAG_CD","CNTRL_NO"
)

df_temp_Clense = df_SortFileRecords \
    .withColumn("prev_POL_NO", F.lag("POL_NO").over(windowSpec_Clense)) \
    .withColumn("prev_PATN_LAST_NM", F.lag("PATN_LAST_NM").over(windowSpec_Clense)) \
    .withColumn("prev_PATN_FIRST_NM", F.lag("PATN_FIRST_NM").over(windowSpec_Clense)) \
    .withColumn("prev_DOB", F.lag("DOB").over(windowSpec_Clense)) \
    .withColumn("prev_GNDR", F.lag("GNDR").over(windowSpec_Clense)) \
    .withColumn("prev_DT_OF_SVC", F.lag("DT_OF_SVC").over(windowSpec_Clense)) \
    .withColumn("prev_RNDR_NTNL_PROV_ID", F.lag("RNDR_NTNL_PROV_ID").over(windowSpec_Clense)) \
    .withColumn("prev_DIAG_CD", F.lag("DIAG_CD").over(windowSpec_Clense)) \
    .withColumn("prev_CNTRL_NO", F.lag("CNTRL_NO").over(windowSpec_Clense)) \
    .withColumn("svIsDupeRow",
        F.when(
            (F.col("POL_NO") == F.col("prev_POL_NO")) &
            (F.col("PATN_LAST_NM") == F.col("prev_PATN_LAST_NM")) &
            (F.col("PATN_FIRST_NM") == F.col("prev_PATN_FIRST_NM")) &
            (F.col("DOB") == F.col("prev_DOB")) &
            (F.col("GNDR") == F.col("prev_GNDR")) &
            (F.col("DT_OF_SVC") == F.col("prev_DT_OF_SVC")) &
            (F.col("RNDR_NTNL_PROV_ID") == F.col("prev_RNDR_NTNL_PROV_ID")) &
            (F.col("DIAG_CD") == F.col("prev_DIAG_CD")) &
            (F.col("CNTRL_NO") == F.col("prev_CNTRL_NO")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    ) \
    .withColumn("svLastNameTrunc", F.upper(trim(F.col("PATN_LAST_NM"))).substr(F.lit(1), F.lit(3))) \
    .withColumn("svFirstNameTrunc", F.upper(trim(F.col("PATN_FIRST_NM"))).substr(F.lit(1), F.lit(3))) \
    .withColumn("svFirstInitial", F.upper(trim(F.col("PATN_FIRST_NM"))).substr(F.lit(1), F.lit(1))) \
    .withColumn("svYearOfBirth",
        F.when(F.length(F.col("DOB")) == 0, F.lit("")).otherwise(F.col("DOB").substr(F.lit(1),F.lit(4)))
    )

df_cleansed = df_temp_Clense.filter(F.col("svIsDupeRow") == "N").select(
    trim(F.col("POL_NO")).alias("POL_NO"),
    F.when(F.length(trim(F.col("PATN_LAST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("PATN_LAST_NM"))).substr(F.lit(1),F.lit(75))).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(trim(F.col("PATN_FIRST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("PATN_FIRST_NM"))).substr(F.lit(1),F.lit(35))).alias("LoadFile_PATN_FIRST_NM"),
    F.upper(F.col("PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    trim(F.col("MBI")).alias("MBI"),
    F.when(F.length(trim(F.col("DOB"))) == 0,
           F.lit("")
          ).otherwise(F.expr("EReplace(trim(DOB), '-', '')").substr(F.lit(1),F.lit(8))).alias("LoadFile_DOB"),
    trim(F.col("GNDR")).alias("LoadFile_GNDR"),
    trim(F.col("DT_OF_SVC")).alias("DT_OF_SVC"),
    trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CNTRL_NO").alias("CNTRL_NO"),
    F.col("svFirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("svLastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("svFirstInitial").alias("LoadFile_FirstInitial"),
    F.col("svYearOfBirth").alias("LoadFile_YearOfBirth"),
    trim(F.col("SOURCE_ID")).alias("LoadFile_SOURCE_ID")
)

df_dupeRows = df_temp_Clense.filter(F.col("svIsDupeRow") == "Y").select(
    trim(F.col("POL_NO")).alias("POL_NO"),
    trim(F.col("PATN_LAST_NM")).alias("PATN_LAST_NM"),
    trim(F.col("PATN_FIRST_NM")).alias("PATN_FIRST_NM"),
    trim(F.col("PATN_MID_NM")).alias("PATN_MID_NM"),
    trim(F.col("MBI")).alias("MBI"),
    trim(F.col("DOB")).alias("DOB"),
    trim(F.col("GNDR")).alias("GNDR"),
    trim(F.col("DT_OF_SVC")).alias("DT_OF_SVC"),
    trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    trim(F.col("RNDR_PROV_LAST_NM")).alias("RNDR_PROV_LAST_NM"),
    trim(F.col("RNDR_PROV_FIRST_NM")).alias("RNDR_PROV_FIRST_NM"),
    trim(F.col("DIAG_CD")).alias("DIAG_CD"),
    trim(F.col("CNTRL_NO")).alias("CNTRL_NO"),
    trim(F.col("SOURCE_ID")).alias("SOURCE_ID")
)

# PxSequentialFile Stage: DupeRowsFile
dupe_rows_file_path = f"{adls_path}/verified/{ProviderName}.DIAG.Duplicate_Rows_In_Source_File.{RunID}.TXT"
write_files(
    df_dupeRows.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR",
        "DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM",
        "DIAG_CD","CNTRL_NO","SOURCE_ID"
    ),
    dupe_rows_file_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# DB2ConnectorPX Stage: MBR
# ----------------------------------------------------------------------------
jdbc_url_MBR, jdbc_props_MBR = get_db_config(ids_secret_name)
extract_query_mbr = f"""SELECT DISTINCT
   upper(sub.SUB_ID) as SUB_ID
   ,upper(mbr.LAST_NM) as LAST_NM
   ,upper(mbr.FIRST_NM) as FIRST_NM
   ,mbr.BRTH_DT_SK
   ,mbr.MBR_UNIQ_KEY
   ,mbr.MBR_SK
FROM {IDSOwner}.MBR mbr,
     {IDSOwner}.SUB sub
WHERE mbr.SUB_SK = sub.SUB_SK
-- *** this is only for testing ***
--AND sub.SUB_ID = '003367169'
"""
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MBR)
    .options(**jdbc_props_MBR)
    .option("query", extract_query_mbr)
    .load()
)

# ----------------------------------------------------------------------------
# CTransformerStage: TruncNames (from df_MBR)
# ----------------------------------------------------------------------------
df_TruncNames = df_MBR.withColumn(
    "svLastNameTrunc",
    F.upper(trim(F.col("LAST_NM"))).substr(F.lit(1),F.lit(3))
).withColumn(
    "svFirstNameTrunc",
    F.upper(trim(F.col("FIRST_NM"))).substr(F.lit(1),F.lit(3))
).withColumn(
    "svFirstInitial",
    F.upper(trim(F.col("FIRST_NM"))).substr(F.lit(1),F.lit(1))
).withColumn(
    "svYearOfBirth",
    F.col("BRTH_DT_SK").substr(F.lit(1),F.lit(4))
).withColumn(
    "svDOB",
    F.when(
        (trim(F.col("BRTH_DT_SK")) == F.lit("NA")) |
        (trim(F.col("BRTH_DT_SK")) == F.lit("UNK")),
        F.lit("")
    ).otherwise(F.expr("EReplace(trim(BRTH_DT_SK), '-', '')"))
)

df_Level1 = df_TruncNames.select(
    F.when(F.length(F.col("SUB_ID"))==0, F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SK").alias("MBR_SK")
)

df_Level2 = df_TruncNames.select(
    F.when(F.length(F.col("SUB_ID"))==0, F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("svFirstNameTrunc").alias("MBR_FirstNameTrunc"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SK").alias("MBR_SK")
)

df_Level3 = df_TruncNames.select(
    F.when(F.length(F.col("SUB_ID"))==0, F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("svLastNameTrunc").alias("MBR_LastNameTrunc"),
    F.col("svFirstNameTrunc").alias("MBR_FirstNameTrunc"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SK").alias("MBR_SK")
)

# ----------------------------------------------------------------------------
# PxLookup Stage: Lookup1  (left join: cleansed => Level1)
# ----------------------------------------------------------------------------
df_Lookup1_joined = df_cleansed.alias("cleansed").join(
    df_Level1.alias("Level1"),
    on=[
        F.col("cleansed.POL_NO") == F.col("Level1.MBR_SUB_ID"),
        F.col("cleansed.LoadFile_PATN_LAST_NM") == F.col("Level1.MBR_LAST_NM"),
        F.col("cleansed.LoadFile_PATN_FIRST_NM") == F.col("Level1.MBR_FIRST_NM"),
        F.col("cleansed.LoadFile_DOB") == F.col("Level1.MBR_BRTH_DT_SK")
    ],
    how="left"
)

# Split into matched (lookup1) vs reject (rej1). Matched => "MBR_SUB_ID" not null
df_lookup1 = df_Lookup1_joined.filter(F.col("Level1.MBR_SUB_ID").isNotNull()).select(
    F.col("cleansed.POL_NO").alias("POL_NO"),
    F.col("Level1.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level1.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("cleansed.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("cleansed.MBI").alias("MBI"),
    F.col("Level1.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("cleansed.LoadFile_GNDR").alias("GNDR"),
    F.col("cleansed.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("cleansed.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("cleansed.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("cleansed.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("cleansed.DIAG_CD").alias("DIAG_CD"),
    F.col("cleansed.CNTRL_NO").alias("CNTRL_NO"),
    F.col("cleansed.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("cleansed.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("cleansed.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("cleansed.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    F.col("Level1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Level1.MBR_SK").alias("MBR_SK"),
    F.col("cleansed.LoadFile_SOURCE_ID").alias("SOURCE_ID")
)

df_rej1 = df_Lookup1_joined.filter(F.col("Level1.MBR_SUB_ID").isNull()).select(
    F.col("cleansed.POL_NO").alias("POL_NO"),
    F.when(F.length(trim(F.col("cleansed.LoadFile_PATN_LAST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("cleansed.LoadFile_PATN_LAST_NM"))).substr(F.lit(1),F.lit(75))).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(trim(F.col("cleansed.LoadFile_PATN_FIRST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("cleansed.LoadFile_PATN_FIRST_NM"))).substr(F.lit(1),F.lit(35))).alias("LoadFile_PATN_FIRST_NM"),
    F.upper(F.col("cleansed.LoadFile_PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    trim(F.col("cleansed.MBI")).alias("MBI"),
    F.when(F.length(trim(F.col("cleansed.LoadFile_DOB"))) == 0,
           F.lit("")
          ).otherwise(F.expr("EReplace(trim(cleansed.LoadFile_DOB), '-', '')").substr(F.lit(1),F.lit(8))).alias("LoadFile_DOB"),
    trim(F.col("cleansed.LoadFile_GNDR")).alias("LoadFile_GNDR"),
    trim(F.col("cleansed.DT_OF_SVC")).alias("DT_OF_SVC"),
    trim(F.col("cleansed.RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.col("cleansed.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("cleansed.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("cleansed.DIAG_CD").alias("DIAG_CD"),
    F.col("cleansed.CNTRL_NO").alias("CNTRL_NO"),
    F.col("cleansed.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("cleansed.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("cleansed.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("cleansed.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    trim(F.col("cleansed.LoadFile_SOURCE_ID")).alias("LoadFile_SOURCE_ID")
)

# ----------------------------------------------------------------------------
# PxLookup Stage: Lookup2 (rej1 => left join => Level2)
# ----------------------------------------------------------------------------
df_Lookup2_joined = df_rej1.alias("rej1").join(
    df_Level2.alias("Level2"),
    on=[
        F.col("rej1.POL_NO") == F.col("Level2.MBR_SUB_ID"),
        F.col("rej1.LoadFile_PATN_LAST_NM") == F.col("Level2.MBR_LAST_NM"),
        F.col("rej1.LoadFile_DOB") == F.col("Level2.MBR_BRTH_DT_SK"),
        F.col("rej1.LoadFile_FirstNameTrunc") == F.col("Level2.MBR_FirstNameTrunc")
    ],
    how="left"
)

df_lookup2 = df_Lookup2_joined.filter(F.col("Level2.MBR_SUB_ID").isNotNull()).select(
    F.col("rej1.POL_NO").alias("POL_NO"),
    F.col("Level2.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level2.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej1.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("rej1.MBI").alias("MBI"),
    F.col("Level2.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("rej1.LoadFile_GNDR").alias("GNDR"),
    F.col("rej1.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("rej1.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("rej1.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("rej1.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("rej1.DIAG_CD").alias("DIAG_CD"),
    F.col("rej1.CNTRL_NO").alias("CNTRL_NO"),
    F.col("rej1.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("rej1.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("rej1.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("rej1.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    F.col("Level2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Level2.MBR_SK").alias("MBR_SK"),
    F.col("rej1.LoadFile_SOURCE_ID").alias("SOURCE_ID")
)

df_rej2 = df_Lookup2_joined.filter(F.col("Level2.MBR_SUB_ID").isNull()).select(
    F.col("rej1.POL_NO").alias("POL_NO"),
    F.when(F.length(trim(F.col("rej1.LoadFile_PATN_LAST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("rej1.LoadFile_PATN_LAST_NM"))).substr(F.lit(1),F.lit(75))).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(trim(F.col("rej1.LoadFile_PATN_FIRST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("rej1.LoadFile_PATN_FIRST_NM"))).substr(F.lit(1),F.lit(35))).alias("LoadFile_PATN_FIRST_NM"),
    F.upper(F.col("rej1.LoadFile_PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    trim(F.col("rej1.MBI")).alias("MBI"),
    F.when(F.length(trim(F.col("rej1.LoadFile_DOB"))) == 0,
           F.lit("")
          ).otherwise(F.expr("EReplace(trim(rej1.LoadFile_DOB), '-', '')").substr(F.lit(1),F.lit(8))).alias("LoadFile_DOB"),
    trim(F.col("rej1.LoadFile_GNDR")).alias("LoadFile_GNDR"),
    trim(F.col("rej1.DT_OF_SVC")).alias("DT_OF_SVC"),
    trim(F.col("rej1.RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.col("rej1.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("rej1.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("rej1.DIAG_CD").alias("DIAG_CD"),
    F.col("rej1.CNTRL_NO").alias("CNTRL_NO"),
    F.col("rej1.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("rej1.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("rej1.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("rej1.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    trim(F.col("rej1.LoadFile_SOURCE_ID")).alias("LoadFile_SOURCE_ID")
)

# ----------------------------------------------------------------------------
# PxLookup Stage: Lookup3 (rej2 => left join => Level3)
# ----------------------------------------------------------------------------
df_Lookup3_joined = df_rej2.alias("rej2").join(
    df_Level3.alias("Level3"),
    on=[
        F.col("rej2.POL_NO") == F.col("Level3.MBR_SUB_ID"),
        F.col("rej2.LoadFile_DOB") == F.col("Level3.MBR_BRTH_DT_SK"),
        F.col("rej2.LoadFile_LastNameTrunc") == F.col("Level3.MBR_LastNameTrunc"),
        F.col("rej2.LoadFile_FirstNameTrunc") == F.col("Level3.MBR_FirstNameTrunc")
    ],
    how="left"
)

df_lookup3 = df_Lookup3_joined.filter(F.col("Level3.MBR_SUB_ID").isNotNull()).select(
    F.col("rej2.POL_NO").alias("POL_NO"),
    F.col("Level3.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level3.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej2.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("rej2.MBI").alias("MBI"),
    F.col("Level3.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("rej2.LoadFile_GNDR").alias("GNDR"),
    F.col("rej2.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("rej2.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("rej2.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("rej2.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("rej2.DIAG_CD").alias("DIAG_CD"),
    F.col("rej2.CNTRL_NO").alias("CNTRL_NO"),
    F.col("rej2.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("rej2.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("rej2.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("rej2.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    F.col("Level3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Level3.MBR_SK").alias("MBR_SK"),
    F.col("rej2.LoadFile_SOURCE_ID").alias("SOURCE_ID")
)

df_rej3 = df_Lookup3_joined.filter(F.col("Level3.MBR_SUB_ID").isNull()).select(
    trim(F.col("rej2.POL_NO")).alias("POL_NO"),
    F.when(F.length(trim(F.col("rej2.LoadFile_PATN_LAST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("rej2.LoadFile_PATN_LAST_NM"))).substr(F.lit(1),F.lit(75))).alias("LoadFile_PATN_LAST_NM"),
    F.when(F.length(trim(F.col("rej2.LoadFile_PATN_FIRST_NM"))) == 0,
           F.lit("")
          ).otherwise(F.upper(trim(F.col("rej2.LoadFile_PATN_FIRST_NM"))).substr(F.lit(1),F.lit(35))).alias("LoadFile_PATN_FIRST_NM"),
    F.upper(F.col("rej2.LoadFile_PATN_MID_NM")).alias("LoadFile_PATN_MID_NM"),
    trim(F.col("rej2.MBI")).alias("MBI"),
    F.when(F.length(trim(F.col("rej2.LoadFile_DOB"))) == 0,
           F.lit("")
          ).otherwise(F.expr("EReplace(trim(rej2.LoadFile_DOB), '-', '')").substr(F.lit(1),F.lit(8))).alias("LoadFile_DOB"),
    trim(F.col("rej2.LoadFile_GNDR")).alias("LoadFile_GNDR"),
    trim(F.col("rej2.DT_OF_SVC")).alias("DT_OF_SVC"),
    trim(F.col("rej2.RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.col("rej2.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("rej2.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("rej2.DIAG_CD").alias("DIAG_CD"),
    F.col("rej2.CNTRL_NO").alias("CNTRL_NO"),
    F.col("rej2.LoadFile_FirstNameTrunc").alias("LoadFile_FirstNameTrunc"),
    F.col("rej2.LoadFile_LastNameTrunc").alias("LoadFile_LastNameTrunc"),
    F.col("rej2.LoadFile_FirstInitial").alias("LoadFile_FirstInitial"),
    F.col("rej2.LoadFile_YearOfBirth").alias("LoadFile_YearOfBirth"),
    trim(F.col("rej2.LoadFile_SOURCE_ID")).alias("LoadFile_SOURCE_ID")
)

# ----------------------------------------------------------------------------
# RejReason3
# ----------------------------------------------------------------------------
df_RejReason3 = df_rej3.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("LoadFile_PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("LoadFile_PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("LoadFile_DOB").alias("DOB"),
    F.lit("All Member Matches Failed").alias("ErrorReason"),
    F.lit("None").alias("MemberMatchLevel")
)

# ----------------------------------------------------------------------------
# CTransformerStage: Transformer1 => funnel with MemberMatchLevel="Level One"
# ----------------------------------------------------------------------------
df_Transformer1 = df_lookup1.withColumn("MemberMatchLevel", F.lit("Level One"))

# ----------------------------------------------------------------------------
# CTransformerStage: Transformer2 => funnel with MemberMatchLevel="Level Two"
# ----------------------------------------------------------------------------
df_Transformer2 = df_lookup2.withColumn("MemberMatchLevel", F.lit("Level Two"))

# ----------------------------------------------------------------------------
# CTransformerStage: Transformer3 => funnel with MemberMatchLevel="Level Three"
# ----------------------------------------------------------------------------
df_Transformer3 = df_lookup3.withColumn("MemberMatchLevel", F.lit("Level Three"))

# ----------------------------------------------------------------------------
# PxFunnel Stage: FunnelMatches => combine level1, level2, level3
# ----------------------------------------------------------------------------
df_FunnelMatches = df_Transformer1.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC",
    "RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO",
    "MBR_UNIQ_KEY","MBR_SK","SOURCE_ID","MemberMatchLevel"
).unionByName(
    df_Transformer2.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC",
        "RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO",
        "MBR_UNIQ_KEY","MBR_SK","SOURCE_ID","MemberMatchLevel"
    )
).unionByName(
    df_Transformer3.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC",
        "RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO",
        "MBR_UNIQ_KEY","MBR_SK","SOURCE_ID","MemberMatchLevel"
    )
)

# ----------------------------------------------------------------------------
# PxSort Stage: SortFunRecs
# ----------------------------------------------------------------------------
df_SortFunRecs = df_FunnelMatches.orderBy(
    "POL_NO","MBR_UNIQ_KEY","DT_OF_SVC","RNDR_NTNL_PROV_ID","DIAG_CD","CNTRL_NO"
)

# ----------------------------------------------------------------------------
# CTransformerStage: CountRows (similar row-by-row duplication check)
# ----------------------------------------------------------------------------
windowSpec_CountRows = Window.orderBy(
    "POL_NO","MBR_UNIQ_KEY","DT_OF_SVC","RNDR_NTNL_PROV_ID","DIAG_CD","CNTRL_NO"
)
df_temp_CountRows = df_SortFunRecs \
    .withColumn("prev_POL_NO2", F.lag("POL_NO").over(windowSpec_CountRows)) \
    .withColumn("prev_MBR_UNIQ_KEY", F.lag("MBR_UNIQ_KEY").over(windowSpec_CountRows)) \
    .withColumn("prev_DT_OF_SVC", F.lag("DT_OF_SVC").over(windowSpec_CountRows)) \
    .withColumn("prev_RNDR_NTNL_PROV_ID", F.lag("RNDR_NTNL_PROV_ID").over(windowSpec_CountRows)) \
    .withColumn("prev_DIAG_CD", F.lag("DIAG_CD").over(windowSpec_CountRows)) \
    .withColumn("prev_CNTRL_NO", F.lag("CNTRL_NO").over(windowSpec_CountRows)) \
    .withColumn("svIsDuplicateRow",
        F.when(
            (F.col("POL_NO") == F.col("prev_POL_NO2")) &
            (F.col("MBR_UNIQ_KEY") == F.col("prev_MBR_UNIQ_KEY")) &
            (F.col("DT_OF_SVC") == F.col("prev_DT_OF_SVC")) &
            (F.col("RNDR_NTNL_PROV_ID") == F.col("prev_RNDR_NTNL_PROV_ID")) &
            (F.col("DIAG_CD") == F.col("prev_DIAG_CD")) &
            (F.col("CNTRL_NO") == F.col("prev_CNTRL_NO")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )

df_sepDupes = df_temp_CountRows.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CNTRL_NO").alias("CNTRL_NO"),
    F.col("SOURCE_ID").alias("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MemberMatchLevel").alias("MemberMatchLevel"),
    F.col("svIsDuplicateRow").alias("IS_DUPLICATE_ROW")
)

# ----------------------------------------------------------------------------
# CTransformerStage: IdentifyDuplicates -> splits "dupes" vs "SubtractDupes"
# ----------------------------------------------------------------------------
df_dupes = df_sepDupes.filter(F.col("IS_DUPLICATE_ROW") == "Y").select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","DOB","DT_OF_SVC",
    "RNDR_NTNL_PROV_ID","DIAG_CD","CNTRL_NO"
)

df_SubtractDupes = df_sepDupes.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR",
    "DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM",
    "DIAG_CD","CNTRL_NO","SOURCE_ID","MBR_UNIQ_KEY","MBR_SK","MemberMatchLevel"
)

# ----------------------------------------------------------------------------
# CTransformerStage: Duplicates -> dupes2
# ----------------------------------------------------------------------------
df_dupes2 = df_dupes.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DOB").alias("DOB"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CNTRL_NO").alias("CNTRL_NO")
)

# ----------------------------------------------------------------------------
# PxLookup Stage: LookupDupes => AllTheDupes + output
# ----------------------------------------------------------------------------
df_LookupDupes_joined = df_SubtractDupes.alias("SubtractDupes").join(
    df_dupes2.alias("dupes2"),
    on=[
        F.col("SubtractDupes.POL_NO") == F.col("dupes2.POL_NO"),
        F.col("SubtractDupes.DT_OF_SVC") == F.col("dupes2.DT_OF_SVC"),
        F.col("SubtractDupes.RNDR_NTNL_PROV_ID") == F.col("dupes2.RNDR_NTNL_PROV_ID"),
        F.col("SubtractDupes.DIAG_CD") == F.col("dupes2.DIAG_CD"),
        F.col("SubtractDupes.CNTRL_NO") == F.col("dupes2.CNTRL_NO")
    ],
    how="left"
)

df_AllTheDupes = df_LookupDupes_joined.filter(F.col("dupes2.POL_NO").isNotNull()).select(
    F.col("SubtractDupes.POL_NO"),
    F.col("SubtractDupes.PATN_LAST_NM"),
    F.col("SubtractDupes.PATN_FIRST_NM"),
    F.col("SubtractDupes.PATN_MID_NM"),
    F.col("SubtractDupes.MBI"),
    F.col("SubtractDupes.DOB"),
    F.col("SubtractDupes.GNDR"),
    F.col("SubtractDupes.DT_OF_SVC"),
    F.col("SubtractDupes.RNDR_NTNL_PROV_ID"),
    F.col("SubtractDupes.RNDR_PROV_LAST_NM"),
    F.col("SubtractDupes.RNDR_PROV_FIRST_NM"),
    F.col("SubtractDupes.DIAG_CD"),
    F.col("SubtractDupes.CNTRL_NO"),
    F.col("SubtractDupes.SOURCE_ID"),
    F.col("SubtractDupes.MBR_UNIQ_KEY"),
    F.col("SubtractDupes.MemberMatchLevel"),
    F.col("SubtractDupes.MBR_SK")
)

df_output = df_LookupDupes_joined.filter(F.col("dupes2.POL_NO").isNull()).select(
    F.col("SubtractDupes.POL_NO").alias("POL_NO"),
    F.col("SubtractDupes.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("SubtractDupes.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("SubtractDupes.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("SubtractDupes.MBI").alias("MBI"),
    F.col("SubtractDupes.DOB").alias("DOB"),
    F.col("SubtractDupes.GNDR").alias("GNDR"),
    F.col("SubtractDupes.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("SubtractDupes.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("SubtractDupes.RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("SubtractDupes.RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("SubtractDupes.DIAG_CD").alias("DIAG_CD"),
    F.col("SubtractDupes.CNTRL_NO").alias("CNTRL_NO"),
    F.col("SubtractDupes.SOURCE_ID").alias("SOURCE_ID"),
    F.col("SubtractDupes.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SubtractDupes.MemberMatchLevel").alias("MemberMatchLevel"),
    F.col("SubtractDupes.MBR_SK").alias("MBR_SK")
)

# ----------------------------------------------------------------------------
# CTransformerStage: ClenseOutput => finaloutput
# ----------------------------------------------------------------------------
df_ClenseOutput = df_output.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    F.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CNTRL_NO").alias("CNTRL_NO"),
    F.col("SOURCE_ID").alias("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MemberMatchLevel").alias("MemberMatchLevel")
)

# ----------------------------------------------------------------------------
# PxSequentialFile Stage: ProcedureFile
# ----------------------------------------------------------------------------
final_file_path = f"{adls_path_raw}/landing/{InFile}_MBRMATCHFINAL"
write_files(
    df_ClenseOutput.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR",
        "DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM",
        "DIAG_CD","CNTRL_NO","SOURCE_ID","MBR_UNIQ_KEY","MBR_SK","MemberMatchLevel"
    ),
    final_file_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# CTransformerStage: TooManyMatches => then funnel => "AllTheDupes" with reason
# ----------------------------------------------------------------------------
df_TooManyMatches = df_AllTheDupes.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DOB").alias("DOB"),
    F.lit("Too Many Member Matches").alias("ErrorReason"),
    F.col("MemberMatchLevel").alias("MemberMatchLevel")
)

# ----------------------------------------------------------------------------
# PxFunnel Stage: FunnelErrors => combine TooManyMatches + RejReason3
# ----------------------------------------------------------------------------
df_FunnelErrors = df_TooManyMatches.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DOB").alias("DOB"),
    F.col("ErrorReason").alias("ErrorReason"),
    F.col("MemberMatchLevel").alias("MemberMatchLevel")
).unionByName(
    df_RejReason3.select(
        F.col("POL_NO").alias("POL_NO"),
        F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
        F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        F.col("DOB").alias("DOB"),
        F.col("ErrorReason").alias("ErrorReason"),
        F.col("MemberMatchLevel").alias("MemberMatchLevel")
    )
)

# ----------------------------------------------------------------------------
# PxSequentialFile Stage: ErrorFile
# ----------------------------------------------------------------------------
error_file_path = f"{adls_path}/verified/{ProviderName}.DIAG.MEMBER_MATCH_ERRORS.{RunID}.TXT"
write_files(
    df_FunnelErrors.select(
        F.col("POL_NO").alias("SUB_ID"),
        "PATN_LAST_NM","PATN_FIRST_NM","DOB","ErrorReason","MemberMatchLevel"
    ),
    error_file_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)