# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021, 2022, 2024 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsProviderMbrBioMesrRsltExtrSeq
# MAGIC                    
# MAGIC Job Name:   ProviderMbrBioMesrSecondaryMbrMatch
# MAGIC Processing: This job takes the FORMAT.*.BIOMETRIC*.TXT  file created by the IdsProviderMbrBioMesrRsltExtrFormatData job, does member matching logic compared to the IDS MBR table, and generates and updated version of the FORMAT.*.BIOMETRIC*.TXT file to be read by the IdsProviderMbrBioMesrRsltExtr job.
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary  
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)PROJECT/\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)DEVELOPMENT\(9)\(9)
# MAGIC DEVELOPER\(9)\(9)DATE\(9)\(9)STORY#\(9)\(9)CHANGE DESCRIPTION\(9)\(9)\(9)\(9)\(9)PROJECT\(9)\(9)\(9)CODE REVIEWER\(9)\(9)DATE REVIEWED
# MAGIC ==============================================================================================================================================================================================
# MAGIC Ken Bradmon\(9)\(9)2021-12-01\(9)us418692\(9)\(9)Original programming\(9)\(9)\(9)\(9)\(9)IntegrateDev3\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2022-01-05
# MAGIC Venkata Y \(9)\(9)2022-01-13\(9)us480876\(9)\(9)Added upcase function \(9)\(9)\(9)\(9)\(9)IntegrateDev3\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2022-01-13
# MAGIC                                                                                                                \(9)to POL_NO,Sub_id in db2stage
# MAGIC Ken Bradmon\(9)\(9)2022-02-01\(9)us474125\(9)\(9)Had to update how the POL_NO value from the\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Abhiram Dasarathy\(9)\(9)2022-02-08\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)file gets uppercased.
# MAGIC Ken Bradmon\(9)\(9)2022-04-18\(9)us510948\(9)\(9)Convert the RSLT_VAL to a number in the initial \(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2022-04-19\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Clense stage, sort, then convert the numbers 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)back to a Varchar again so the right 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)records get dropped by the DeDupe transformer stage.
# MAGIC Ken Bradmon\(9)\(9)2024-02-16\(9)us611146\(9)\(9)Strip out levels 4, 5, 6 and 7 of the member matching logic.\(9)\(9)IntegrateDev2                                         Goutham Kalidindi                     3/1/2024
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)also add new column "MemberMatchLevel" to the output file.
# MAGIC Ken Bradmon\(9)\(9)2024-05-03\(9)us616253\(9)\(9)Added MBR_UNIQ_KEY from the MBR table stage to the \(9)\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2024-06-05
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)svIsDuplicateRow stage variable of the CountRows stage.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)This corrects FALSE instances of "too many member matches."
# MAGIC Ken Bradmon\(9)\(9)2024-09-23\(9)us609108\(9)\(9)I made a small change to the "Cleanse" stage for how the RSLT_VAL\(9)IntegrateDev1                                         Jeyaprasanna                          2024-10-10
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)column is handled, to eliminate warnings in the log if the Provider 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)sends us a value that isn't a number.  For example if they send us 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"72 inches" for the height of that member, instead of just "72".

# MAGIC Job Name: ProviderMbrBioMesrSecondaryMbrMatch
# MAGIC Called By: IdsProviderMbrBioMesrRsltExtrSeq
# MAGIC Purpose: This job takes the FORMAT.*.BIOMETRIC*.TXT  file created by the IdsProviderMbrBioMesrRsltExtrFormatData job, does member matching logic compared to the IDS MBR table, and generates and updated version of the FORMAT.*.BIOMETRIC*.TXT file to be read by the IdsProviderMbrBioMesrRsltExtr job.
# MAGIC In this case we don't want the ones that match in the lookup, so \"rejects\" of the Lookup are good.  The records that match are duplicate records to be subtracted.
# MAGIC This is a COPY of the original \"FORMAT.*.BIOMETRIC\" file.
# MAGIC The Sort stage sorts the records based on:
# MAGIC + POL_NO
# MAGIC + Last Name
# MAGIC + First Name
# MAGIC + DOB
# MAGIC + Date of Svc
# MAGIC + Metric
# MAGIC + RSLT_VAL (Ascending)
# MAGIC Then the Clense stage removes dupes based on the first 6 columns.  So for example if we have two "Weight" records for the same patient, FOR THE SAME DAY, the lower weight is retained, and the higher weight is dropped.
# MAGIC The member's first and last names need to be converted to 50 characters each, to match the FORMAT.*.BIOMETRIC* file.
# MAGIC This is the edited "FORMAT.*.BIOMETRIC" file:
# MAGIC /ids/prod/verified/
# MAGIC FORMAT.*.BIOMETRIC.CCYYMMDDhhmmss.TXT
# MAGIC /ids/prod/verified/
# MAGIC BIOMETRIC.MEMBER_MATCH_ERRORS.CCYYMMhhmmss.TXT
# MAGIC There are 3 scenarios for Member Matching, to maximize the number of Biometric records successfully matched to a single Member record from MBR_D.
# MAGIC Biometric records that fail to match get sent to the Error file.  Biometric records that match more than one record from MBR_D also get sent to the Error file.
# MAGIC These are the source file records where all of the member matching levels 1 - 3 failed.
# MAGIC If we have more than one record "matched" from the MBR table, we want to send ALL of those records written to the error file, not just one of them.  We don't want any of those records for that member to be sent to the output file.
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
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile_F = get_widget_value('InFile_F','')
ProviderName = get_widget_value('ProviderName','')
RunID = get_widget_value('RunID','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# STAGE: SUB (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT DISTINCT sub.SUB_ID FROM {IDSOwner}.SUB sub ")
    .load()
)

# STAGE: ProvBioMesrRslt_original (PxSequentialFile) - READ
schema_ProvBioMesrRslt_original = T.StructType([
    T.StructField("POL_NO", T.StringType(), True),
    T.StructField("PATN_LAST_NM", T.StringType(), True),
    T.StructField("PATN_FIRST_NM", T.StringType(), True),
    T.StructField("PATN_MID_NM", T.StringType(), True),
    T.StructField("MBI", T.StringType(), True),
    T.StructField("DOB", T.StringType(), True),
    T.StructField("GNDR", T.StringType(), True),
    T.StructField("DT_OF_SVC", T.StringType(), True),
    T.StructField("MTRC", T.StringType(), True),
    T.StructField("RSLT_VAL", T.StringType(), True),
    T.StructField("UOM", T.StringType(), True),
    T.StructField("SOURCE_ID", T.StringType(), True)
])
df_ProvBioMesrRslt_original = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", "|")
    .option("quote", None)
    .option("inferSchema", "false")
    .schema(schema_ProvBioMesrRslt_original)
    .load(f"{adls_path}/verified/{InFile_F}_original")
)

# STAGE: Cleanse (CTransformerStage)
df_Cleanse = df_ProvBioMesrRslt_original.select(
    F.when(F.length(F.col("POL_NO")) == 0, F.lit("")).otherwise(UpCase(F.col("POL_NO"))).alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("MTRC").alias("MTRC"),
    F.when(NUM(F.col("RSLT_VAL")) == F.lit(1), StringToDecimal(F.col("RSLT_VAL"))).otherwise(F.lit(None)).alias("RSLT_VAL"),
    F.col("UOM").alias("UOM"),
    F.col("SOURCE_ID").alias("SOURCE_ID")
)

# STAGE: LookupSub (PxLookup) - LEFT JOIN with SUB
df_LookupSub_temp = (
    df_Cleanse.alias("source")
    .join(df_SUB.alias("IdsSub"), F.col("source.POL_NO") == F.col("IdsSub.SUB_ID"), "left")
)

# Output Pin: LoadFile (matched or not, but typically we pass all rows). 
# In DataStage Lookup, the reference columns are not always used in expressions for the main link. 
# We'll keep all rows in main link, but we also create a NOT-FOUND link by checking IdsSub.SUB_ID isNull.
df_LoadFile = df_LookupSub_temp.select(
    F.col("source.POL_NO").alias("LoadFile_POL_NO"),
    F.col("source.PATN_LAST_NM").alias("LoadFile_PATN_LAST_NM"),
    F.col("source.PATN_FIRST_NM").alias("LoadFile_PATN_FIRST_NM"),
    F.col("source.PATN_MID_NM").alias("LoadFile_PATN_MID_NM"),
    F.col("source.MBI").alias("LoadFile_MBI"),
    F.col("source.DOB").alias("LoadFile_DOB"),
    F.col("source.GNDR").alias("LoadFile_GNDR"),
    F.col("source.DT_OF_SVC").alias("LoadFile_DT_OF_SVC"),
    F.col("source.MTRC").alias("LoadFile_MTRC"),
    F.col("source.RSLT_VAL").alias("LoadFile_RSLT_VAL"),
    F.col("source.UOM").alias("LoadFile_UOM"),
    F.col("source.SOURCE_ID").alias("LoadFile_SOURCE_ID")
)

df_NotOurMembers = df_LookupSub_temp.filter(F.col("IdsSub.SUB_ID").isNull()).select(
    F.col("source.POL_NO").alias("POL_NO"),
    F.col("source.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("source.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("source.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("source.MBI").alias("MBI"),
    F.col("source.DOB").alias("DOB"),
    F.col("source.GNDR").alias("GNDR"),
    F.col("source.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("source.MTRC").alias("MTRC"),
    F.col("source.RSLT_VAL").alias("RSLT_VAL"),
    F.col("source.UOM").alias("UOM"),
    F.col("source.SOURCE_ID").alias("SOURCE_ID")
)
# STAGE: NotOurMembers (PxSequentialFile) - WRITE
df_NotOurMembers_select = df_NotOurMembers.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID"
)
write_files(
    df_NotOurMembers_select,
    f"{adls_path}/verified/{ProviderName}.BIOMETRIC.Member_Sub_IDs_Not_Found.{RunID}.TXT",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# STAGE: Sort1 (PxSort)
df_Sort1 = (
    df_LoadFile
    .sort(
        "LoadFile_POL_NO",
        "LoadFile_PATN_LAST_NM",
        "LoadFile_PATN_FIRST_NM",
        "LoadFile_DOB",
        "LoadFile_DT_OF_SVC",
        "LoadFile_MTRC",
        "LoadFile_RSLT_VAL"
    )
    .select(
        F.col("LoadFile_POL_NO").alias("POL_NO"),
        F.col("LoadFile_PATN_LAST_NM").alias("PATN_LAST_NM"),
        F.col("LoadFile_PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        F.col("LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
        F.col("LoadFile_MBI").alias("MBI"),
        F.col("LoadFile_DOB").alias("DOB"),
        F.col("LoadFile_GNDR").alias("GNDR"),
        F.col("LoadFile_DT_OF_SVC").alias("DT_OF_SVC"),
        F.col("LoadFile_MTRC").alias("MTRC"),
        F.col("LoadFile_RSLT_VAL").alias("RSLT_VAL"),
        F.col("LoadFile_UOM").alias("UOM"),
        F.col("LoadFile_SOURCE_ID").alias("SOURCE_ID")
    )
)

# STAGE: DeDupe (CTransformerStage) - uses stage variables to detect consecutive duplicates
# We replicate with a window-lag approach, then route 'N' -> cleansed, 'Y' -> dupeRows
windowDeDupe = Window.orderBy(
    "POL_NO",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "DOB",
    "DT_OF_SVC",
    "MTRC",
    "RSLT_VAL"
)
df_DeDupe_withLag = (
    df_Sort1
    .withColumn("lag_POL_NO", F.lag("POL_NO").over(windowDeDupe))
    .withColumn("lag_PATN_LAST_NM", F.lag("PATN_LAST_NM").over(windowDeDupe))
    .withColumn("lag_PATN_FIRST_NM", F.lag("PATN_FIRST_NM").over(windowDeDupe))
    .withColumn("lag_DOB", F.lag("DOB").over(windowDeDupe))
    .withColumn("lag_DT_OF_SVC", F.lag("DT_OF_SVC").over(windowDeDupe))
    .withColumn("lag_MTRC", F.lag("MTRC").over(windowDeDupe))
    .withColumn(
        "svIsDupeRow",
        F.when(
            (F.col("POL_NO") == F.col("lag_POL_NO")) &
            (F.col("PATN_LAST_NM") == F.col("lag_PATN_LAST_NM")) &
            (F.col("PATN_FIRST_NM") == F.col("lag_PATN_FIRST_NM")) &
            (F.col("DOB") == F.col("lag_DOB")) &
            (F.col("DT_OF_SVC") == F.col("lag_DT_OF_SVC")) &
            (F.col("MTRC") == F.col("lag_MTRC")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_DeDupe_cleansed = df_DeDupe_withLag.filter(F.col("svIsDupeRow") == "N").select(
    F.col("POL_NO").alias("LoadFile_POL_NO"),
    F.expr("If(Len(Trim(PATN_LAST_NM)) = 0, '', UpCase(Trim(PATN_LAST_NM)))").alias("LoadFile_PATN_LAST_NM"),
    F.expr("If(Len(Trim(PATN_FIRST_NM)) = 0, '', UpCase(Trim(PATN_FIRST_NM[35])))").alias("LoadFile_PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("LoadFile_PATN_MID_NM"),
    F.col("MBI").alias("LoadFile_MBI"),
    F.expr("IF(LEN(TRIM(DOB)) <> 8, '', TRIM(DOB))").alias("LoadFile_DOB"),
    F.col("GNDR").alias("LoadFile_GNDR"),
    F.col("DT_OF_SVC").alias("LoadFile_DT_OF_SVC"),
    F.expr("""
        If(MTRC = 'BPDLS',
           DecimalToString(RSLT_VAL, 'suppress_zero'),
           If(MTRC = 'BPSYS',
              DecimalToString(RSLT_VAL, 'suppress_zero'),
              DecimalToString(Field(RSLT_VAL, '.', 1), 'suppress_zero') : '.' : Field(RSLT_VAL, '.', 2)
           )
        )
    """).alias("LoadFile_RSLT_VAL"),
    F.col("UOM").alias("LoadFile_UOM"),
    F.expr("UpCase(Left(Trim(PATN_FIRST_NM), 3))").alias("LoadFile_FirstNameTrunc"),
    F.expr("UpCase(Left(Trim(PATN_LAST_NM), 3))").alias("LoadFile_LastNameTrunc"),
    F.expr("UpCase(Left(Trim(PATN_FIRST_NM), 1))").alias("LoadFile_FirstInitial"),
    F.expr("IF(LEN(TRIM(DOB)) <> 8, '', LEFT(DOB, 4))").alias("LoadFile_YearOfBirth"),
    F.col("MTRC").alias("LoadFile_MTRC"),
    F.expr("Trim(SOURCE_ID)").alias("LoadFile_SOURCE_ID")
)

df_DeDupe_dupeRows = df_DeDupe_withLag.filter(F.col("svIsDupeRow") == "Y").select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.col("DOB"),
    F.col("GNDR"),
    F.col("DT_OF_SVC"),
    F.col("MTRC"),
    F.expr("""
        If(MTRC = 'BPDLS',
           DecimalToString(RSLT_VAL, 'suppress_zero'),
           If(MTRC = 'BPSYS',
              DecimalToString(RSLT_VAL, 'suppress_zero'),
              DecimalToString(Field(RSLT_VAL, '.', 1), 'suppress_zero') : '.' : Field(RSLT_VAL, '.', 2)
           )
        )
    """).alias("RSLT_VAL"),
    F.col("UOM"),
    F.col("SOURCE_ID"),
    F.col("svIsDupeRow").alias("IsDuplicateRow")
)

# STAGE: DuplicateRowsFile (PxSequentialFile) - WRITE
df_DuplicateRowsFile = df_DeDupe_dupeRows.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","IsDuplicateRow"
)
write_files(
    df_DuplicateRowsFile,
    f"{adls_path}/verified/{ProviderName}.BIOMETRIC.Duplicate_Rows_In_Format_File.{RunID}.TXT",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Next big set of lookups with MBR table:
# STAGE: MBR (DB2ConnectorPX)
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT DISTINCT
       upper (sub.SUB_ID) as SUB_ID
      ,mbr.LAST_NM
      ,mbr.FIRST_NM
      ,mbr.BRTH_DT_SK
      ,mbr.MBR_UNIQ_KEY
FROM {IDSOwner}.MBR mbr,
     {IDSOwner}.SUB sub
WHERE mbr.SUB_SK = sub.SUB_SK
""")
    .load()
)

# STAGE: TruncNames (CTransformerStage)
df_TruncNames = df_MBR.select(
    F.when(F.length(F.col("SUB_ID"))==0, F.lit("")).otherwise(F.col("SUB_ID")).alias("MBR_SUB_ID"),
    F.expr("mbr.LAST_NM[1, 50]").alias("MBR_LAST_NM"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("UpCase(Left(Trim(LAST_NM), 3))").alias("svLastNameTrunc"),
    F.expr("UpCase(Left(Trim(FIRST_NM), 3))").alias("svFirstNameTrunc"),
    F.expr("UpCase(Left(Trim(FIRST_NM), 1))").alias("svFirstInitial"),
    F.expr("Left(BRTH_DT_SK, 4)").alias("svYearOfBirth"),
    F.expr("""
        IF(Trim(BRTH_DT_SK)='NA','',
          IF(Trim(BRTH_DT_SK)='UNK','',
            EReplace(Trim(BRTH_DT_SK),'-','')
          )
        )
    """).alias("svDOB")
)

# We'll produce separate dataframes for the 3 output pins from TruncNames
# Level1:
df_Level1 = df_TruncNames.select(
    F.col("MBR_SUB_ID").alias("MBR_SUB_ID"),
    F.expr("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)
# Level2:
df_Level2 = df_TruncNames.select(
    F.col("MBR_SUB_ID").alias("MBR_SUB_ID"),
    F.expr("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svFirstNameTrunc").alias("MBR_FirstNameTrunc")
)
# Level3:
df_Level3 = df_TruncNames.select(
    F.col("MBR_SUB_ID").alias("MBR_SUB_ID"),
    F.expr("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("svDOB").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svLastNameTrunc").alias("MBR_LastNameTrunc"),
    F.col("svFirstNameTrunc").alias("MBR_FirstNameTrunc")
)

# STAGE: Lookup1 (PxLookup) - primary link is df_DeDupe_cleansed
#   reference link is df_Level1 with left join on four columns
df_Lookup1_temp = (
    df_DeDupe_cleansed.alias("cleansed")
    .join(
        df_Level1.alias("Level1"),
        (F.col("cleansed.LoadFile_POL_NO") == F.col("Level1.MBR_SUB_ID")) &
        (F.col("cleansed.LoadFile_PATN_LAST_NM") == F.col("Level1.MBR_LAST_NM")) &
        (F.col("cleansed.LoadFile_PATN_FIRST_NM") == F.col("Level1.MBR_FIRST_NM")) &
        (F.col("cleansed.LoadFile_DOB") == F.col("Level1.MBR_BRTH_DT_SK")),
        "left"
    )
)

df_lookup1_main = df_Lookup1_temp.select(
    F.col("Level1.MBR_SUB_ID").alias("POL_NO"),
    F.col("Level1.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level1.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("cleansed.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("cleansed.LoadFile_MBI").alias("MBI"),
    F.col("Level1.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("cleansed.LoadFile_GNDR").alias("GNDR"),
    F.col("Level1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("cleansed.LoadFile_DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("cleansed.LoadFile_MTRC").alias("MTRC"),
    F.col("cleansed.LoadFile_RSLT_VAL").alias("RSLT_VAL"),
    F.col("cleansed.LoadFile_UOM").alias("UOM"),
    F.col("cleansed.LoadFile_SOURCE_ID").alias("SOURCE_ID")
)
# The "reject" link for Lookup1 is rows where Level1.MBR_SUB_ID is null => we replicate DS behavior by checking reference columns
df_lookup1_rej = df_Lookup1_temp.filter(F.col("Level1.MBR_SUB_ID").isNull()).select(
    F.col("cleansed.POL_NO"),
    F.expr("If(Len(Trim(cleansed.LoadFile_PATN_LAST_NM))=0,'',UpCase(Trim(cleansed.LoadFile_PATN_LAST_NM)))").alias("LoadFile_PATN_LAST_NM"),
    F.expr("If(Len(Trim(cleansed.LoadFile_PATN_FIRST_NM))=0,'',UpCase(Trim(cleansed.LoadFile_PATN_FIRST_NM[35])))").alias("LoadFile_PATN_FIRST_NM"),
    F.col("cleansed.LoadFile_PATN_MID_NM").alias("LoadFile_PATN_MID_NM"),
    F.col("cleansed.LoadFile_MBI").alias("LoadFile_MBI"),
    F.expr("IF(LEN(TRIM(cleansed.LoadFile_DOB))<>8,'',TRIM(cleansed.LoadFile_DOB))").alias("LoadFile_DOB"),
    F.col("cleansed.LoadFile_GNDR").alias("LoadFile_GNDR"),
    F.col("cleansed.LoadFile_DT_OF_SVC").alias("LoadFile_DT_OF_SVC"),
    F.expr("""
           If(cleansed.LoadFile_MTRC='BPDLS',
              DecimalToString(cleansed.LoadFile_RSLT_VAL,'suppress_zero'),
              If(cleansed.LoadFile_MTRC='BPSYS',
                 DecimalToString(cleansed.LoadFile_RSLT_VAL,'suppress_zero'),
                 DecimalToString(Field(cleansed.LoadFile_RSLT_VAL,'.',1),'suppress_zero')
                 :'.':Field(cleansed.LoadFile_RSLT_VAL,'.',2)
              )
           )""").alias("LoadFile_RSLT_VAL"),
    F.col("cleansed.LoadFile_UOM").alias("LoadFile_UOM"),
    F.expr("UpCase(Left(Trim(cleansed.LoadFile_PATN_FIRST_NM),3))").alias("LoadFile_FirstNameTrunc"),
    F.expr("UpCase(Left(Trim(cleansed.LoadFile_PATN_LAST_NM),3))").alias("LoadFile_LastNameTrunc"),
    F.expr("UpCase(Left(Trim(cleansed.LoadFile_PATN_FIRST_NM),1))").alias("LoadFile_FirstInitial"),
    F.expr("IF(LEN(TRIM(cleansed.LoadFile_DOB))<>8,'',LEFT(cleansed.LoadFile_DOB,4))").alias("LoadFile_YearOfBirth"),
    F.col("cleansed.LoadFile_MTRC").alias("LoadFile_MTRC"),
    F.expr("Trim(cleansed.LoadFile_SOURCE_ID)").alias("LoadFile_SOURCE_ID")
)

# STAGE: Transformer1 => sets "MemberMatchLevel" = 'Level One'
df_Transformer1 = df_lookup1_main.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.col("DOB"),
    F.col("GNDR"),
    F.col("MBR_UNIQ_KEY"),
    F.col("DT_OF_SVC"),
    F.col("MTRC"),
    F.col("RSLT_VAL"),
    F.col("UOM"),
    F.col("SOURCE_ID"),
    F.lit("Level One").alias("MemberMatchLevel")
)

# STAGE: Lookup2 (PxLookup) => primary link is df_lookup1_rej, reference is df_Level2
df_Lookup2_temp = (
    df_lookup1_rej.alias("rej1")
    .join(
        df_Level2.alias("Level2"),
        (F.col("rej1.LoadFile_POL_NO") == F.col("Level2.MBR_SUB_ID")) &
        (F.col("rej1.LoadFile_PATN_LAST_NM") == F.col("Level2.MBR_LAST_NM")) &
        (F.col("rej1.LoadFile_DOB") == F.col("Level2.MBR_BRTH_DT_SK")) &
        (F.col("rej1.LoadFile_FirstNameTrunc") == F.col("Level2.MBR_FirstNameTrunc")),
        "left"
    )
)

df_lookup2_main = df_Lookup2_temp.select(
    F.col("rej1.LoadFile_POL_NO").alias("POL_NO"),
    F.col("Level2.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level2.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej1.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("rej1.LoadFile_MBI").alias("MBI"),
    F.col("Level2.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("rej1.LoadFile_GNDR").alias("GNDR"),
    F.col("Level2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("rej1.LoadFile_DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("rej1.LoadFile_MTRC").alias("MTRC"),
    F.col("rej1.LoadFile_RSLT_VAL").alias("RSLT_VAL"),
    F.col("rej1.LoadFile_UOM").alias("UOM"),
    F.col("rej1.LoadFile_SOURCE_ID").alias("SOURCE_ID")
)
# Reject from Lookup2 => reference is null
df_lookup2_rej = df_Lookup2_temp.filter(F.col("Level2.MBR_SUB_ID").isNull()).select(
    F.col("rej1.POL_NO"),
    F.expr("If(Len(Trim(rej1.LoadFile_PATN_LAST_NM))=0,'',UpCase(Trim(rej1.LoadFile_PATN_LAST_NM)))").alias("LoadFile_PATN_LAST_NM"),
    F.expr("If(Len(Trim(rej1.LoadFile_PATN_FIRST_NM))=0,'',UpCase(Trim(rej1.LoadFile_PATN_FIRST_NM[35])))").alias("LoadFile_PATN_FIRST_NM"),
    F.col("rej1.LoadFile_PATN_MID_NM").alias("LoadFile_PATN_MID_NM"),
    F.col("rej1.LoadFile_MBI").alias("LoadFile_MBI"),
    F.expr("IF(LEN(TRIM(rej1.LoadFile_DOB))<>8,'',TRIM(rej1.LoadFile_DOB))").alias("LoadFile_DOB"),
    F.col("rej1.LoadFile_GNDR").alias("LoadFile_GNDR"),
    F.col("rej1.LoadFile_DT_OF_SVC").alias("LoadFile_DT_OF_SVC"),
    F.expr("""
           If(rej1.LoadFile_MTRC='BPDLS',
              DecimalToString(rej1.LoadFile_RSLT_VAL,'suppress_zero'),
              If(rej1.LoadFile_MTRC='BPSYS',
                 DecimalToString(rej1.LoadFile_RSLT_VAL,'suppress_zero'),
                 DecimalToString(Field(rej1.LoadFile_RSLT_VAL,'.',1),'suppress_zero')
                 :'.':Field(rej1.LoadFile_RSLT_VAL,'.',2)
              )
           )
    """).alias("LoadFile_RSLT_VAL"),
    F.col("rej1.LoadFile_UOM").alias("LoadFile_UOM"),
    F.expr("UpCase(Left(Trim(rej1.LoadFile_PATN_FIRST_NM),3))").alias("LoadFile_FirstNameTrunc"),
    F.expr("UpCase(Left(Trim(rej1.LoadFile_PATN_LAST_NM),3))").alias("LoadFile_LastNameTrunc"),
    F.expr("UpCase(Left(Trim(rej1.LoadFile_PATN_FIRST_NM),1))").alias("LoadFile_FirstInitial"),
    F.expr("IF(LEN(TRIM(rej1.LoadFile_DOB))<>8,'',LEFT(rej1.LoadFile_DOB,4))").alias("LoadFile_YearOfBirth"),
    F.col("rej1.LoadFile_MTRC").alias("LoadFile_MTRC"),
    F.expr("Trim(rej1.LoadFile_SOURCE_ID)").alias("LoadFile_SOURCE_ID")
)

# STAGE: Transformer2 => sets "MemberMatchLevel" = 'Level Two'
df_Transformer2 = df_lookup2_main.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.col("DOB"),
    F.col("GNDR"),
    F.col("MBR_UNIQ_KEY"),
    F.col("DT_OF_SVC"),
    F.col("MTRC"),
    F.col("RSLT_VAL"),
    F.col("UOM"),
    F.col("SOURCE_ID"),
    F.lit("Level Two").alias("MemberMatchLevel")
)

# STAGE: Lookup3 (PxLookup) => primary link is df_lookup2_rej, reference is df_Level3
df_Lookup3_temp = (
    df_lookup2_rej.alias("rej2")
    .join(
        df_Level3.alias("Level3"),
        (F.col("rej2.LoadFile_POL_NO") == F.col("Level3.MBR_SUB_ID")) &
        (F.col("rej2.LoadFile_DOB") == F.col("Level3.MBR_BRTH_DT_SK")) &
        (F.col("rej2.LoadFile_LastNameTrunc") == F.col("Level3.MBR_LastNameTrunc")) &
        (F.col("rej2.LoadFile_FirstNameTrunc") == F.col("Level3.MBR_FirstNameTrunc")),
        "left"
    )
)

df_lookup3_main = df_Lookup3_temp.select(
    F.col("rej2.LoadFile_POL_NO").alias("POL_NO"),
    F.col("Level3.MBR_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Level3.MBR_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("rej2.LoadFile_PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("rej2.LoadFile_MBI").alias("MBI"),
    F.col("Level3.MBR_BRTH_DT_SK").alias("DOB"),
    F.col("rej2.LoadFile_GNDR").alias("GNDR"),
    F.col("Level3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("rej2.LoadFile_DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("rej2.LoadFile_MTRC").alias("MTRC"),
    F.col("rej2.LoadFile_RSLT_VAL").alias("RSLT_VAL"),
    F.col("rej2.LoadFile_UOM").alias("UOM"),
    F.col("rej2.LoadFile_SOURCE_ID").alias("SOURCE_ID")
)
df_lookup3_rej = df_Lookup3_temp.filter(F.col("Level3.MBR_SUB_ID").isNull()).select(
    F.col("rej2.POL_NO"),
    F.expr("If(Len(Trim(rej2.LoadFile_PATN_LAST_NM))=0,'',UpCase(Trim(rej2.LoadFile_PATN_LAST_NM)))").alias("LoadFile_PATN_LAST_NM"),
    F.expr("If(Len(Trim(rej2.LoadFile_PATN_FIRST_NM))=0,'',UpCase(Trim(rej2.LoadFile_PATN_FIRST_NM[35])))").alias("LoadFile_PATN_FIRST_NM"),
    F.col("rej2.LoadFile_PATN_MID_NM").alias("LoadFile_PATN_MID_NM"),
    F.col("rej2.LoadFile_MBI").alias("LoadFile_MBI"),
    F.expr("IF(LEN(TRIM(rej2.LoadFile_DOB))<>8,'',TRIM(rej2.LoadFile_DOB))").alias("LoadFile_DOB"),
    F.col("rej2.LoadFile_GNDR").alias("LoadFile_GNDR"),
    F.col("rej2.LoadFile_DT_OF_SVC").alias("LoadFile_DT_OF_SVC"),
    F.expr("""
           If(rej2.LoadFile_MTRC='BPDLS',
              DecimalToString(rej2.LoadFile_RSLT_VAL,'suppress_zero'),
              If(rej2.LoadFile_MTRC='BPSYS',
                 DecimalToString(rej2.LoadFile_RSLT_VAL,'suppress_zero'),
                 DecimalToString(Field(rej2.LoadFile_RSLT_VAL,'.',1),'suppress_zero')
                 :'.':Field(rej2.LoadFile_RSLT_VAL,'.',2)
              )
           )
    """).alias("LoadFile_RSLT_VAL"),
    F.col("rej2.LoadFile_UOM").alias("LoadFile_UOM"),
    F.expr("UpCase(Left(Trim(rej2.LoadFile_PATN_FIRST_NM),3))").alias("LoadFile_FirstNameTrunc"),
    F.expr("UpCase(Left(Trim(rej2.LoadFile_PATN_LAST_NM),3))").alias("LoadFile_LastNameTrunc"),
    F.expr("UpCase(Left(Trim(rej2.LoadFile_PATN_FIRST_NM),1))").alias("LoadFile_FirstInitial"),
    F.expr("IF(LEN(TRIM(rej2.LoadFile_DOB))<>8,'',LEFT(rej2.LoadFile_DOB,4))").alias("LoadFile_YearOfBirth"),
    F.col("rej2.LoadFile_MTRC").alias("LoadFile_MTRC"),
    F.expr("Trim(rej2.LoadFile_SOURCE_ID)").alias("LoadFile_SOURCE_ID")
)

# STAGE: Transformer3 => sets "MemberMatchLevel"='Level Three'
df_Transformer3 = df_lookup3_main.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.col("DOB"),
    F.col("GNDR"),
    F.col("MBR_UNIQ_KEY"),
    F.col("DT_OF_SVC"),
    F.col("MTRC"),
    F.col("RSLT_VAL"),
    F.col("UOM"),
    F.col("SOURCE_ID"),
    F.lit("Level Three").alias("MemberMatchLevel")
)

# STAGE: FunnelMatches (PxFunnel) => funnels Transformer1, Transformer2, Transformer3
df_FunnelMatches = (
    df_Transformer1.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","MemberMatchLevel"
    )
    .unionByName(
        df_Transformer2.select(
            "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","MemberMatchLevel"
        )
    )
    .unionByName(
        df_Transformer3.select(
            "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","MemberMatchLevel"
        )
    )
)

# STAGE: Sort2 (PxSort) => sorts by POL_NO, DOB, MBR_UNIQ_KEY, DT_OF_SVC, MTRC
df_Sort2 = df_FunnelMatches.sort(
    "POL_NO","DOB","MBR_UNIQ_KEY","DT_OF_SVC","MTRC"
).select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","MemberMatchLevel"
)

# STAGE: CountRows (CTransformerStage) => stage variables to identify duplicates once more
windowCountRows = Window.orderBy("POL_NO","DT_OF_SVC","MTRC","MBR_UNIQ_KEY")
df_CountRows_var = (
    df_Sort2
    .withColumn("lag_POL_NO", F.lag("POL_NO").over(windowCountRows))
    .withColumn("lag_DT_OF_SVC", F.lag("DT_OF_SVC").over(windowCountRows))
    .withColumn("lag_MTRC", F.lag("MTRC").over(windowCountRows))
    .withColumn("lag_MBR_UNIQ_KEY", F.lag("MBR_UNIQ_KEY").over(windowCountRows))
    .withColumn(
        "svIsDuplicateRow",
        F.when(
            (F.col("POL_NO")==F.col("lag_POL_NO")) &
            (F.col("DT_OF_SVC")==F.col("lag_DT_OF_SVC")) &
            (F.col("MTRC")==F.col("lag_MTRC")) &
            (F.col("MBR_UNIQ_KEY")==F.col("lag_MBR_UNIQ_KEY")),
            "Y"
        ).otherwise("N")
    )
)

df_sepDupes = df_CountRows_var.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.expr("If(Len(Trim(DOB))=0,SetNull(),Trim(DOB))").alias("DOB"),
    F.col("GNDR"),
    F.col("MBR_UNIQ_KEY"),
    F.col("DT_OF_SVC"),
    F.col("MTRC"),
    F.col("RSLT_VAL"),
    F.col("UOM"),
    F.col("SOURCE_ID"),
    F.col("svIsDuplicateRow").alias("IS_DUPLICATE_ROW"),
    F.col("MemberMatchLevel")
)

# STAGE: IdentifyDuplicates (CTransformerStage)
df_IdentifyDuplicates_dupes = df_sepDupes.filter(F.col("IS_DUPLICATE_ROW")=="Y").select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("DOB")
)
df_IdentifyDuplicates_subtract = df_sepDupes.filter(F.col("IS_DUPLICATE_ROW")!="Y").select(
    "POL_NO",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_MID_NM",
    "MBI",
    "DOB",
    "GNDR",
    "DT_OF_SVC",
    "MTRC",
    "RSLT_VAL",
    "UOM",
    "SOURCE_ID",
    "MemberMatchLevel"
)

# STAGE: Duplicates (CTransformerStage)
# This transforms the 'dupes' link into 'dupes2' with columns including a primary key
df_Duplicates_dupes2 = df_IdentifyDuplicates_dupes.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DOB").alias("DOB")
)

# STAGE: LookupDupes (PxLookup)
df_LookupDupes_temp = (
    df_IdentifyDuplicates_subtract.alias("SubtractDupes")
    .join(
        df_Duplicates_dupes2.alias("dupes2"),
        F.col("SubtractDupes.POL_NO")==F.col("dupes2.POL_NO"),
        "left"
    )
)

df_AllTheDupes = df_LookupDupes_temp.select(
    F.col("SubtractDupes.POL_NO"),
    F.col("SubtractDupes.PATN_LAST_NM"),
    F.col("SubtractDupes.PATN_FIRST_NM"),
    F.col("SubtractDupes.PATN_MID_NM"),
    F.col("SubtractDupes.MBI"),
    F.col("SubtractDupes.DOB"),
    F.col("SubtractDupes.GNDR"),
    F.col("SubtractDupes.DT_OF_SVC"),
    F.col("SubtractDupes.MTRC"),
    F.col("SubtractDupes.RSLT_VAL"),
    F.col("SubtractDupes.UOM"),
    F.col("SubtractDupes.SOURCE_ID"),
    F.col("SubtractDupes.MemberMatchLevel")
)
df_ClenseOutput_pass = df_LookupDupes_temp.filter(F.col("dupes2.POL_NO").isNull()).select(
    F.col("SubtractDupes.POL_NO").alias("POL_NO"),
    F.col("SubtractDupes.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("SubtractDupes.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("SubtractDupes.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("SubtractDupes.MBI").alias("MBI"),
    F.col("SubtractDupes.DOB").alias("DOB"),
    F.col("SubtractDupes.GNDR").alias("GNDR"),
    F.col("SubtractDupes.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("SubtractDupes.MTRC").alias("MTRC"),
    F.col("SubtractDupes.RSLT_VAL").alias("RSLT_VAL"),
    F.col("SubtractDupes.UOM").alias("UOM"),
    F.col("SubtractDupes.SOURCE_ID").alias("SOURCE_ID"),
    F.col("SubtractDupes.MemberMatchLevel").alias("MemberMatchLevel")
)

# STAGE: RmDupes (PxRemDup) => retains first among duplicates with keys: POL_NO, PATN_LAST_NM, PATN_FIRST_NM, DOB
df_RmDupes = dedup_sort(
    df_AllTheDupes,
    partition_cols=["POL_NO","PATN_LAST_NM","PATN_FIRST_NM","DOB"],
    sort_cols=[("POL_NO","A"),("PATN_LAST_NM","A"),("PATN_FIRST_NM","A"),("DOB","A")]
)

# STAGE: TooManyMatches (CTransformerStage)
# This output pin has a constraint to funnel into "FunnelErrors" with "ErrorReason"='Too Many Member Matches'
df_TooManyMatches_dupes = df_RmDupes.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("DOB"),
    F.lit("Too Many Member Matches").alias("ErrorReason"),
    F.col("MemberMatchLevel")
)

# We now funnel that with the next pipeline. Meanwhile there's no explicit "if" logic in DataStage that might skip rows
# So we pass everything here. The job design shows that link goes to FunnelErrors. 

# STAGE: FunnelErrors (PxFunnel) => combined from TooManyMatches and RejReason3
# RejReason3 => "All Member Matches Failed" with MemberMatchLevel='None'
df_RejReason3 = df_lookup3_rej.select(
    F.col("rej2.POL_NO").alias("POL_NO"),
    F.col("LoadFile_PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("LoadFile_PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("LoadFile_DOB").alias("DOB"),
    F.lit("All Member Matches Failed").alias("ErrorReason"),
    F.lit("None").alias("MemberMatchLevel")
)

df_FunnelErrors = (
    df_TooManyMatches_dupes.select(
        F.col("POL_NO").alias("SUB_ID"),
        "PATN_LAST_NM","PATN_FIRST_NM","DOB","ErrorReason","MemberMatchLevel"
    )
    .unionByName(
        df_RejReason3.select(
            F.col("POL_NO").alias("SUB_ID"),
            F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
            F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
            F.col("DOB").alias("DOB"),
            F.col("ErrorReason").alias("ErrorReason"),
            F.col("MemberMatchLevel").alias("MemberMatchLevel")
        )
    )
)

# STAGE: Sort3 (PxSort) => sort by ErrorReason, SUB_ID, PATN_LAST_NM, PATN_FIRST_NM, DOB
df_Sort3 = df_FunnelErrors.sort(
    "ErrorReason","SUB_ID","PATN_LAST_NM","PATN_FIRST_NM","DOB"
).select(
    "SUB_ID","PATN_LAST_NM","PATN_FIRST_NM","DOB","ErrorReason","MemberMatchLevel"
)

# STAGE: ErrorFile (PxSequentialFile) => WRITE
df_ErrorFile = df_Sort3.select(
    "SUB_ID","PATN_LAST_NM","PATN_FIRST_NM","DOB","ErrorReason","MemberMatchLevel"
)
write_files(
    df_ErrorFile,
    f"{adls_path}/verified/{ProviderName}.BIOMETRIC.MEMBER_MATCH_ERRORS.{RunID}.TXT",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# STAGE: ClenseOutput (CTransformerStage) => uses df_ClenseOutput_pass as input
df_ClenseOutput = df_ClenseOutput_pass.select(
    F.col("POL_NO"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("MBI"),
    F.col("DOB"),
    F.col("GNDR"),
    F.col("DT_OF_SVC"),
    F.col("MTRC"),
    F.col("RSLT_VAL"),
    F.col("UOM"),
    F.col("SOURCE_ID"),
    F.col("MemberMatchLevel")
)

# STAGE: ProvBioMesrRslt (PxSequentialFile) => final output
# According to instructions, for the final file all columns with char/varchar must be rpad with unknown length <...>.
df_ProvBioMesrRslt = df_ClenseOutput.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","MemberMatchLevel"
)

# Apply rpad(...) for each column that is varchar/char. We do not know exact lengths, so per instructions use <...>.
df_ProvBioMesrRslt_final = (
    df_ProvBioMesrRslt
    .withColumn("POL_NO", F.rpad(F.col("POL_NO"), <...>, " "))
    .withColumn("PATN_LAST_NM", F.rpad(F.col("PATN_LAST_NM"), <...>, " "))
    .withColumn("PATN_FIRST_NM", F.rpad(F.col("PATN_FIRST_NM"), <...>, " "))
    .withColumn("PATN_MID_NM", F.rpad(F.col("PATN_MID_NM"), <...>, " "))
    .withColumn("MBI", F.rpad(F.col("MBI"), <...>, " "))
    .withColumn("DOB", F.rpad(F.col("DOB"), <...>, " "))
    .withColumn("GNDR", F.rpad(F.col("GNDR"), <...>, " "))
    .withColumn("DT_OF_SVC", F.rpad(F.col("DT_OF_SVC"), <...>, " "))
    .withColumn("MTRC", F.rpad(F.col("MTRC"), <...>, " "))
    .withColumn("RSLT_VAL", F.rpad(F.col("RSLT_VAL"), <...>, " "))
    .withColumn("UOM", F.rpad(F.col("UOM"), <...>, " "))
    .withColumn("SOURCE_ID", F.rpad(F.col("SOURCE_ID"), <...>, " "))
    .withColumn("MemberMatchLevel", F.rpad(F.col("MemberMatchLevel"), <...>, " "))
)

# Write final file
write_files(
    df_ProvBioMesrRslt_final.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","DT_OF_SVC","MTRC","RSLT_VAL","UOM","SOURCE_ID","MemberMatchLevel"
    ),
    f"{adls_path}/verified/{InFile_F}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)