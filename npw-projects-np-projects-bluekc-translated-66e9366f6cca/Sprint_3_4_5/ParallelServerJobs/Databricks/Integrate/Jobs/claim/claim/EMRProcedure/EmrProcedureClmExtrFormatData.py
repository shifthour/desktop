# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021, 2022 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmExtrFormatData
# MAGIC CALLED BY:  EmrProcedureClmLandSeq
# MAGIC PURPOSE:  Formats the data from the EmrProcedureMbrMatchSeq job for further processing before it is loaded to the target tables.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date\(9)\(9)Developer\(9)\(9)Project/Altiris#\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)Date Reviewed
# MAGIC ============================================================================================================================================================================================
# MAGIC 2021-02-22\(9)Vikas Abbu\(9)\(9)\(9)\(9)Initial Programming                                               \(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Jaideep Mankala   \(9)03/04/2021
# MAGIC 2021-05-04\(9)Lakshmi Devagiri                                  \(9)\(9)Updated Member query to use Upper case for\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Manasa Andru  \(9)2021-05-10    
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)First Name and Last Name                                 \(9)\(9)\(9)\(9)     
# MAGIC 2021-06-21\(9)Abhishek Pulluri\(9)\(9)\(9)\(9)Updated Upper case for POL_NO  from the Source file         \(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Manasa Andru  \(9)2021-06-23
# MAGIC 2022-09-01\(9)Ken Bradmon\(9)\(9)us542805\(9)\(9)Removed the check for NULL Gender, and PROC_TYPE.\(9)\(9)\(9)IntegrateDev1\(9)\(9)\(9)Harsha Ravuri\(9)2023-06-14\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also added the new columns: PROC_CD_CPT, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPT_MOD_1, PROC_CD_CPT_2, PROC_CD_CPTII,
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPTII_MOD2, PROC_CD_CPTII_MOD2, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)HCPS, ICD_10_PCS_1, ICD_10_PCS_2, SNOMED, and CVX.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also added logic in the "Xfm_Column_Chk" transformer to valide that DT_OF_SVC
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)is a valid date.  If not, those records get sent to the error file.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also hard-coded the RNDR_PROV_TYP to be "NA" per the new business rule.
# MAGIC 2023-12-14\(9)Ken Bradmon\(9)\(9)us603888\(9)\(9)In the database stage called "PROVIDER" I removed the tetst for "OPTICIAN"\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)2023-12-18
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)so records where the Provider specialty is that, won't count as a 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"Diabetic Retinal Exam."  This change was requested by the business folks.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Changed transformer in the stage called "Set_Proc_CD_CPT" for the stage
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)variables "svIsEyeDoc" and "svPROCCDCPT" - changed the test for NULL.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also changed the length of the PROC_CD_CPT column to 7 characters instead of five.
# MAGIC 2024-01-02\(9)Ken Bradmon\(9)\(9)us606482\(9)\(9)Strip out the decimal from the value of the ICD_10_PCS_1 column in the\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)2024-01-29\(9)\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"Xfm_Matched" stage.  Also, ignore the column called "MemberMatchLevel."
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)

# MAGIC The stage "Xfm_Column_Chck" checks for NULL values that should not be NULL, or values are too long, and routes those records to the Error file.
# MAGIC The NULL values that automatically cause a record to Error are:
# MAGIC POL_NO
# MAGIC PATN_LAST_NM
# MAGIC PATN_FIRST_NM
# MAGIC DOB
# MAGIC DT_OF_SVC
# MAGIC RNDR_NTNL_PROV_ID
# MAGIC SOURCE_ID
# MAGIC This reading the file as one big Varchar(2000)  per row looks wacky, but it is how we count the columns.  The whole file gets rejected if it doesn't have the right number of columns.
# MAGIC JOB NAME:  EmrProcedureClmExtrFormatData
# MAGIC CALLED BY:  EmrProcedureClmLandSeq
# MAGIC PURPOSE:  Formats the data from the EmrProcedureMbrMatchSeq job for further processing before it is loaded to the target tables.
# MAGIC + The stage variables called "svIsxxxNull" are for the rule that one of the following columns cannot be NULL: 13, 16, 19, 20, 21, or 22.  If they are all NULL, or more than one of them contains data, then the Xfm_Column_Chk stage will route that record to the Error file.
# MAGIC + The "svxxxFlag" variables are to check for specific values of each of those columns.  If yes, then that flag gets set to a 1.  Otherwise, that flag gets set to a zero.
# MAGIC The error message changes based on the nature of the "bad" data.
# MAGIC + Columns 13, 16, 19, 20, 21, 22 are all NULL
# MAGIC + More than one of the above columns has a value
# MAGIC + The DT_OF_SVC is an invalid date (like February 30th).
# MAGIC Those checks are also done in the Xfm_Column_Chk stage.
# MAGIC The "Calc_Claim_ID" stage is where the CLM_ID gets created.
# MAGIC Member Unique Key + DOS + an incremented number between 00 and 99.  That part happens in the Xfm_CLM_ID_Incr stage.
# MAGIC 
# MAGIC NOTE: if that incremented number gets to 100, then BAD things happen.  The CLM_ID will get truncated, we will have duplicates, and the load job will abort.
# MAGIC BUT THAT SHOULD NEVER happen.  We have been asked to leave this as is.  If this does happen we WANT the job to abort.  Notify the _IT_RA_QM_Solutions dist list, and just reset everything.  Don't try and make this process finish to load the records.
# MAGIC This stage has the "DRE logic."  It sets the PROC_CD_CPT based on whether or not the provider is an eye doctor qualified to perform Diabetic Retinal Exams, and which of these values is populated:
# MAGIC PROC_CD_CPT
# MAGIC PROC_CD_CPTII
# MAGIC HCPCS
# MAGIC ICD_10_PCS_1
# MAGIC Tack on a 2 digit sequence number to the end of the Claim ID
# MAGIC Select all the Providers where their speciality is one of the following:
# MAGIC OPHTHALMOLOGY
# MAGIC OPHTHALMOLOGY - PEDIATRIC
# MAGIC OPTOMETRIST
# MAGIC The "Xfm_Valid_Delim" stage just splits up the Varchar(2000) into the separate columns for the rest of the job.
# MAGIC NOTE:  column 25 is ignored because it isn't needed in THIS job.
# MAGIC Here is where the columns get counted to make sure there are 25 of them.
# MAGIC The \"Xfm_Un_Matched\" stage rejects the whole file if the SOURCE doesn't match a record in the CD_MPPNG table.
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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameter Retrieval
InFile = get_widget_value('InFile','')
InFile_F = get_widget_value('InFile_F','')
SourceID = get_widget_value('SourceID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ErrorFile = get_widget_value('ErrorFile','')
RejectFile = get_widget_value('RejectFile','')

# Stage 1: PROVIDER (DB2ConnectorPX - IDS database)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_provider = f"""
SELECT DISTINCT
    prov.NTNL_PROV_ID,
    'Y' as IS_EYE_DOC
FROM {IDSOwner}.PROV prov,
     {IDSOwner}.CD_MPPNG map
WHERE
    prov.PROV_SPEC_CD_SK = map.CD_MPPNG_SK
    AND map.SRC_CD_NM in ('OPHTHALMOLOGY','OPHTHALMOLOGY - PEDIATRIC','OPTOMETRIST')
"""
df_PROVIDER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_provider)
    .load()
)

# Stage 2: CD_MPPNG (DB2ConnectorPX - IDS database)
extract_query_cd_mppng = f"""
SELECT
    TRIM(SRC_DRVD_LKUP_VAL) AS SRC_DRVD_LKUP_VAL,
    1 AS FLG
FROM {IDSOwner}.CD_MPPNG
WHERE
    TRGT_DOMAIN_NM='SOURCE SYSTEM'
    AND SRC_CLCTN_CD='IDS'
    AND SRC_SYS_CD='IDS'
    AND TRGT_DOMAIN_NM='SOURCE SYSTEM'
    AND TRGT_CLCTN_CD='IDS'
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mppng)
    .load()
)

# Stage 3: Seq_Source_File (PxSequentialFile)
# Reading a single-column file named "RECORD" from #InFile#, path includes "landing"
schema_seq_source_file = StructType([
    StructField("RECORD", StringType(), True)
])
df_Seq_Source_File = (
    spark.read.format("csv")
    .option("header", True)      # Because ContainsHeader=true in DataStage
    .option("sep", "~")         # Use a delimiter unlikely to appear, so entire line is captured in RECORD
    .schema(schema_seq_source_file)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Stage 4: Xfm_Delimter_Chk (CTransformerStage)
df_xfm_delimter_chk_temp = (
    df_Seq_Source_File
    .withColumn("RECORD_noCR", F.regexp_replace(F.col("RECORD"), "\r", "")) 
    .withColumn("svDelimiter", F.size(F.split(F.col("RECORD_noCR"), "\\|")))
)

# Create outputs Lnk_All (no constraint) and Lnk_Reject_Copy (svDelimiter <> 25)
df_Lnk_All = (
    df_xfm_delimter_chk_temp
    .select(
        F.col("RECORD_noCR").alias("RECORD"),
        F.lit("1").alias("FLG"),
        F.col("svDelimiter")
    )
)

df_Lnk_Reject_Copy = (
    df_xfm_delimter_chk_temp
    .filter(F.col("svDelimiter") != 25)
    .select(
        F.lit("1").alias("FLG")
    )
)

# Stage 5: Copy_Reject (PxCopy)
df_Lnk_Reject_Delim = df_Lnk_Reject_Copy.select(F.col("FLG").alias("FLG"))

# Stage 6: Lkp_Check_Rej (PxLookup) 
#   Primary: df_Lnk_All, Lookup: df_Lnk_Reject_Delim on FLG=FLG, left join
df_temp_lkp_check_rej = (
    df_Lnk_All.alias("Lnk_All")
    .join(
        df_Lnk_Reject_Delim.alias("Lnk_Reject_Delim"),
        F.col("Lnk_All.FLG") == F.col("Lnk_Reject_Delim.FLG"),
        "left"
    )
)

# Lnk_Reject_All = rows where Lnk_Reject_Delim.FLG IS NOT NULL
df_Lnk_Reject_All = (
    df_temp_lkp_check_rej
    .filter(F.col("Lnk_Reject_Delim.FLG").isNotNull())
    .select(F.col("Lnk_All.RECORD").alias("RECORD"))
)

# Lnk_Delim_Valid = rows where Lnk_Reject_Delim.FLG IS NULL
df_Lnk_Delim_Valid = (
    df_temp_lkp_check_rej
    .filter(F.col("Lnk_Reject_Delim.FLG").isNull())
    .select(
        F.col("Lnk_All.RECORD").alias("RECORD"),
        F.lit("1").alias("FLG")
    )
)

# Stage 7: Xfm_Reject_Delim (CTransformerStage)
# We parse "Lnk_Reject_All.RECORD" into columns, and if the splitted length != 24 => "Record does not have 24 columns"
# Build a temp to compute the splitted array:
df_xfm_reject_delim_temp = (
    df_Lnk_Reject_All
    .withColumn("RECORD_noCR", F.regexp_replace(F.col("RECORD"), "\r", ""))
    .withColumn("svDelimiter", F.size(F.split(F.col("RECORD_noCR"), "\\|")))
    .withColumn("splitFields", F.split(F.col("RECORD_noCR"), "\\|"))
)

df_xfm_reject_delim = df_xfm_reject_delim_temp.select(
    F.col("splitFields").getItem(0).alias("POL_NO"),
    F.col("splitFields").getItem(1).alias("PATN_LAST_NM"),
    F.col("splitFields").getItem(2).alias("PATN_FIRST_NM"),
    F.col("splitFields").getItem(3).alias("PATN_MID_NM"),
    F.col("splitFields").getItem(4).alias("MBI"),
    F.col("splitFields").getItem(5).alias("DOB"),
    F.col("splitFields").getItem(6).alias("GNDR"),
    F.col("splitFields").getItem(7).alias("PROC_TYPE"),
    F.col("splitFields").getItem(8).alias("DT_OF_SVC"),
    F.col("splitFields").getItem(9).alias("RSLT_VAL"),
    F.col("splitFields").getItem(10).alias("RNDR_NTNL_PROV_ID"),
    F.col("splitFields").getItem(11).alias("RNDR_PROV_TYP"),
    F.col("splitFields").getItem(12).alias("PROC_CD_CPT"),
    F.col("splitFields").getItem(13).alias("PROC_CD_CPT_MOD_1"),
    F.col("splitFields").getItem(14).alias("PROC_CD_CPT_MOD_2"),
    F.col("splitFields").getItem(15).alias("PROC_CD_CPTII"),
    F.col("splitFields").getItem(16).alias("PROC_CD_CPTII_MOD_1"),
    F.col("splitFields").getItem(17).alias("PROC_CD_CPTII_MOD_2"),
    F.col("splitFields").getItem(18).alias("HCPCS"),
    F.col("splitFields").getItem(19).alias("ICD_10_PCS_1"),
    F.col("splitFields").getItem(20).alias("SNOMED"),
    F.col("splitFields").getItem(21).alias("CVX"),
    F.col("splitFields").getItem(22).alias("SOURCE_ID"),
    F.col("splitFields").getItem(23).alias("MBR_UNIQ_KEY"),
    F.when(F.col("svDelimiter") != 24, "Record does not have 24 columns").otherwise("").alias("REJECT_REASON")
)

# Stage 8: Xfm_Valid_Delim (CTransformerStage)
# Input: df_Lnk_Delim_Valid => parse the record similarly
df_xfm_valid_delim_temp = (
    df_Lnk_Delim_Valid
    .withColumn("RECORD_noCR", F.regexp_replace(F.col("RECORD"), "\r", ""))
    .withColumn("splitFields", F.split(F.col("RECORD_noCR"), "\\|"))
)
df_xfm_valid_delim = df_xfm_valid_delim_temp.select(
    F.upper(F.trim(F.col("splitFields").getItem(0))).alias("POL_NO"),
    F.trim(F.col("splitFields").getItem(1)).alias("PATN_LAST_NM"),
    F.trim(F.col("splitFields").getItem(2)).alias("PATN_FIRST_NM"),
    F.trim(F.col("splitFields").getItem(3)).alias("PATN_MID_NM"),
    F.trim(F.col("splitFields").getItem(4)).alias("MBI"),
    F.trim(F.col("splitFields").getItem(5)).alias("DOB"),
    F.trim(F.col("splitFields").getItem(6)).alias("GNDR"),
    F.trim(F.col("splitFields").getItem(7)).alias("PROC_TYPE"),
    F.trim(F.col("splitFields").getItem(8)).alias("DT_OF_SVC"),
    F.trim(F.col("splitFields").getItem(9)).alias("RSLT_VAL"),
    F.trim(F.col("splitFields").getItem(10)).alias("RNDR_NTNL_PROV_ID"),
    F.trim(F.col("splitFields").getItem(11)).alias("RNDR_PROV_TYP"),
    F.trim(F.col("splitFields").getItem(12)).alias("PROC_CD_CPT"),
    F.trim(F.col("splitFields").getItem(13)).alias("PROC_CD_CPT_MOD_1"),
    F.trim(F.col("splitFields").getItem(14)).alias("PROC_CD_CPT_MOD_2"),
    F.trim(F.col("splitFields").getItem(15)).alias("PROC_CD_CPTII"),
    F.trim(F.col("splitFields").getItem(16)).alias("PROC_CD_CPTII_MOD_1"),
    F.trim(F.col("splitFields").getItem(17)).alias("PROC_CD_CPTII_MOD_2"),
    F.trim(F.col("splitFields").getItem(18)).alias("HCPCS"),
    F.trim(F.col("splitFields").getItem(19)).alias("ICD_10_PCS_1"),
    F.trim(F.col("splitFields").getItem(20)).alias("SNOMED"),
    F.trim(F.col("splitFields").getItem(21)).alias("CVX"),
    F.trim(F.col("splitFields").getItem(22)).alias("SOURCE_ID"),
    F.trim(F.col("splitFields").getItem(23)).alias("MBR_UNIQ_KEY"),
    F.lit("1").alias("FLG")
)

# Stage 9: Lkp_Source_ID (PxLookup)
#   Primary: df_xfm_valid_delim (Rename => Lkp_Src_Sys_Cd), 
#   Lookup: df_CD_MPPNG => left join on SOURCE_ID=SRC_DRVD_LKUP_VAL
df_temp_lkp_source_id = (
    df_xfm_valid_delim.alias("Lkp_Src_Sys_Cd")
    .join(
        df_CD_MPPNG.alias("Lkp_CD_MPPNG"),
        F.col("Lkp_Src_Sys_Cd.SOURCE_ID") == F.col("Lkp_CD_MPPNG.SRC_DRVD_LKUP_VAL"),
        "left"
    )
)
# Make two outputs: Lnk_Matched => where Lkp_CD_MPPNG.FLG is not null, Lnk_UnMatched => where it's null
df_Lnk_Matched = (
    df_temp_lkp_source_id
    .filter(F.col("Lkp_CD_MPPNG.FLG").isNotNull())
    .select(
        F.col("Lkp_Src_Sys_Cd.POL_NO").alias("POL_NO"),
        F.col("Lkp_Src_Sys_Cd.PATN_LAST_NM").alias("PATN_LAST_NM"),
        F.col("Lkp_Src_Sys_Cd.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        F.col("Lkp_Src_Sys_Cd.PATN_MID_NM").alias("PATN_MID_NM"),
        F.col("Lkp_Src_Sys_Cd.MBI").alias("MBI"),
        F.col("Lkp_Src_Sys_Cd.DOB").alias("DOB"),
        F.col("Lkp_Src_Sys_Cd.GNDR").alias("GNDR"),
        F.col("Lkp_Src_Sys_Cd.PROC_TYPE").alias("PROC_TYPE"),
        F.col("Lkp_Src_Sys_Cd.DT_OF_SVC").alias("DT_OF_SVC"),
        F.col("Lkp_Src_Sys_Cd.RSLT_VAL").alias("RSLT_VAL"),
        F.col("Lkp_Src_Sys_Cd.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
        F.col("Lkp_Src_Sys_Cd.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
        F.col("Lkp_Src_Sys_Cd.PROC_CD_CPT").alias("PROC_CD_CPT"),
        F.col("Lkp_Src_Sys_Cd.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
        F.col("Lkp_Src_Sys_Cd.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
        F.col("Lkp_Src_Sys_Cd.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
        F.col("Lkp_Src_Sys_Cd.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
        F.col("Lkp_Src_Sys_Cd.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
        F.col("Lkp_Src_Sys_Cd.HCPCS").alias("HCPCS"),
        F.expr("replace(Lkp_Src_Sys_Cd.ICD_10_PCS_1,'.','')").alias("ICD_10_PCS_1"),
        F.col("Lkp_Src_Sys_Cd.SNOMED").alias("SNOMED"),
        F.col("Lkp_Src_Sys_Cd.CVX").alias("CVX"),
        F.col("Lkp_Src_Sys_Cd.SOURCE_ID").alias("SOURCE_ID"),
        F.col("Lkp_Src_Sys_Cd.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lkp_Src_Sys_Cd.FLG").alias("FLG"),
        F.lit("").alias("REJECT_REASON"),
        (
            # "ARE_Columns_13_16_19_20_21_22_ALL_NULL" => check if columns 13,16,19,20,21,22 are null
            (F.when(
                (F.col("Lkp_Src_Sys_Cd.PROC_CD_CPT").isNull()| (F.trim("Lkp_Src_Sys_Cd.PROC_CD_CPT")==""))
                ,0).otherwise(1)
             + F.when(
                (F.col("Lkp_Src_Sys_Cd.PROC_CD_CPTII").isNull()|(F.trim("Lkp_Src_Sys_Cd.PROC_CD_CPTII")==""))
                ,0).otherwise(1)
             + F.when(
                (F.col("Lkp_Src_Sys_Cd.HCPCS").isNull()|(F.trim("Lkp_Src_Sys_Cd.HCPCS")==""))
                ,0).otherwise(1)
             + F.when(
                (F.col("Lkp_Src_Sys_Cd.ICD_10_PCS_1").isNull()|(F.trim("Lkp_Src_Sys_Cd.ICD_10_PCS_1")==""))
                ,0).otherwise(1)
             + F.when(
                (F.col("Lkp_Src_Sys_Cd.SNOMED").isNull()|(F.trim("Lkp_Src_Sys_Cd.SNOMED")==""))
                ,0).otherwise(1)
             + F.when(
                (F.col("Lkp_Src_Sys_Cd.CVX").isNull()|(F.trim("Lkp_Src_Sys_Cd.CVX")==""))
                ,0).otherwise(1)
            ).alias("ARE_Columns_13_16_19_20_21_22_ALL_NULL")
        )
    )
)

df_Lnk_UnMatched = (
    df_temp_lkp_source_id
    .filter(F.col("Lkp_CD_MPPNG.FLG").isNull())
    .select(
        F.upper(F.trim(F.col("splitFields").getItem(0))).alias("POL_NO"),
        F.trim(F.col("splitFields").getItem(1)).alias("PATN_LAST_NM"),
        F.trim(F.col("splitFields").getItem(2)).alias("PATN_FIRST_NM"),
        F.trim(F.col("splitFields").getItem(3)).alias("PATN_MID_NM"),
        F.trim(F.col("splitFields").getItem(4)).alias("MBI"),
        F.trim(F.col("splitFields").getItem(5)).alias("DOB"),
        F.trim(F.col("splitFields").getItem(6)).alias("GNDR"),
        F.trim(F.col("splitFields").getItem(7)).alias("PROC_TYPE"),
        F.trim(F.col("splitFields").getItem(8)).alias("DT_OF_SVC"),
        F.trim(F.col("splitFields").getItem(9)).alias("RSLT_VAL"),
        F.trim(F.col("splitFields").getItem(10)).alias("RNDR_NTNL_PROV_ID"),
        F.trim(F.col("splitFields").getItem(11)).alias("RNDR_PROV_TYP"),
        F.trim(F.col("splitFields").getItem(12)).alias("PROC_CD_CPT"),
        F.trim(F.col("splitFields").getItem(13)).alias("PROC_CD_CPT_MOD_1"),
        F.trim(F.col("splitFields").getItem(14)).alias("PROC_CD_CPT_MOD_2"),
        F.trim(F.col("splitFields").getItem(15)).alias("PROC_CD_CPTII"),
        F.trim(F.col("splitFields").getItem(16)).alias("PROC_CD_CPTII_MOD_1"),
        F.trim(F.col("splitFields").getItem(17)).alias("PROC_CD_CPTII_MOD_2"),
        F.trim(F.col("splitFields").getItem(18)).alias("HCPCS"),
        F.trim(F.col("splitFields").getItem(19)).alias("ICD_10_PCS_1"),
        F.trim(F.col("splitFields").getItem(20)).alias("SNOMED"),
        F.trim(F.col("splitFields").getItem(21)).alias("CVX"),
        F.trim(F.col("splitFields").getItem(22)).alias("SOURCE_ID"),
        F.trim(F.col("splitFields").getItem(23)).alias("MBR_UNIQ_KEY"),
        F.lit("1").alias("FLG")
    )

# Stage 10: Xfm_Un_Matched (CTransformerStage)
# Output => Lnk_SrcID_Reject1 => (all columns plus REJECT_REASON= 'Source ID does not match CD_MPPNG table.')
# Also => Lnk_Invalid_Flg => pass FLG
df_Lnk_SrcID_Reject1 = df_Lnk_UnMatched.select(
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
    F.col("PROC_CD_CPT"),
    F.col("PROC_CD_CPT_MOD_1"),
    F.col("PROC_CD_CPT_MOD_2"),
    F.col("PROC_CD_CPTII"),
    F.col("PROC_CD_CPTII_MOD_1"),
    F.col("PROC_CD_CPTII_MOD_2"),
    F.col("HCPCS"),
    F.col("ICD_10_PCS_1"),
    F.col("SNOMED"),
    F.col("CVX"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.lit("Source ID does not match CD_MPPNG table.").alias("REJECT_REASON")
)

df_Lnk_Invalid_Flg = df_Lnk_UnMatched.select(
    F.col("FLG").alias("FLG")
)

# Stage 11: Xfm_Matched (CTransformerStage) => We compute columns. There's a bunch of stage vars about whether columns are null.
# Then we keep them all => Lkp_Valid_Data => add Ereplace(ICD_10_PCS_1, '.', '') => done above. 
# We'll recast the logic in code:
df_xfm_matched_temp = df_Lnk_Matched.withColumn("svIsCPTNull", F.when((F.col("PROC_CD_CPT").isNull())|(F.trim("PROC_CD_CPT")==""), 0).otherwise(1)) \
    .withColumn("svIsProcCdCPTiiNull", F.when((F.col("PROC_CD_CPTII").isNull())|(F.trim("PROC_CD_CPTII")==""), 0).otherwise(1)) \
    .withColumn("svIsHCPCSNull", F.when((F.col("HCPCS").isNull())|(F.trim("HCPCS")==""), 0).otherwise(1)) \
    .withColumn("svIsICD10PCS1Null", F.when((F.col("ICD_10_PCS_1").isNull())|(F.trim("ICD_10_PCS_1")==""), 0).otherwise(1)) \
    .withColumn("svIsSNOMEDNull", F.when((F.col("SNOMED").isNull())|(F.trim("SNOMED")==""), 0).otherwise(1)) \
    .withColumn("svIsCVXNull", F.when((F.col("CVX").isNull())|(F.trim("CVX")==""), 0).otherwise(1)) \
    .withColumn(
        "svTestColumns131619202122ForNULL",
        F.col("svIsCPTNull") + F.col("svIsProcCdCPTiiNull") + F.col("svIsHCPCSNull") + F.col("svIsICD10PCS1Null") + F.col("svIsSNOMEDNull") + F.col("svIsCVXNull")
    )

df_Lkp_Valid_Data = df_xfm_matched_temp.select(
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
    F.col("PROC_CD_CPT"),
    F.col("PROC_CD_CPT_MOD_1"),
    F.col("PROC_CD_CPT_MOD_2"),
    F.col("PROC_CD_CPTII"),
    F.col("PROC_CD_CPTII_MOD_1"),
    F.col("PROC_CD_CPTII_MOD_2"),
    F.col("HCPCS"),
    F.expr("replace(ICD_10_PCS_1, '.', '')").alias("ICD_10_PCS_1"),
    F.col("SNOMED"),
    F.col("CVX"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.col("FLG"),
    F.lit("").alias("REJECT_REASON"),
    F.col("svTestColumns131619202122ForNULL").alias("ARE_Columns_13_16_19_20_21_22_ALL_NULL")
)

# Stage 12: Copy_Invalid (PxCopy)
df_Lkp_Invalid_Flg = df_Lnk_Invalid_Flg.select(F.col("FLG").alias("FLG"))

# Stage 13: Lkp_Src_ID_Rej (PxLookup)
#   Primary: df_Lkp_Valid_Data, Lookup: df_Lkp_Invalid_Flg => left join => FLG=FLG
df_temp_lkp_src_id_rej = (
    df_Lkp_Valid_Data.alias("Lkp_Valid_Data")
    .join(
        df_Lkp_Invalid_Flg.alias("Lkp_Invalid_Flg"),
        F.col("Lkp_Valid_Data.FLG") == F.col("Lkp_Invalid_Flg.FLG"),
        "left"
    )
)
# Lnk_SrcID_Reject2 => rows where Lkp_Invalid_Flg.FLG is not null => but in DataStage it is actually the first output pin. 
# Actually the stage definition says: OutputPins => Lnk_SrcID_Reject2, Columns => etc. Also has Bernoulli logic. We'll interpret:
df_Lnk_SrcID_Reject2 = (
    df_temp_lkp_src_id_rej
    .filter(F.col("Lkp_Invalid_Flg.FLG").isNotNull())
    .select(
        F.col("Lkp_Valid_Data.POL_NO"),
        F.col("Lkp_Valid_Data.PATN_LAST_NM"),
        F.col("Lkp_Valid_Data.PATN_FIRST_NM"),
        F.col("Lkp_Valid_Data.PATN_MID_NM"),
        F.col("Lkp_Valid_Data.MBI"),
        F.col("Lkp_Valid_Data.DOB"),
        F.col("Lkp_Valid_Data.GNDR"),
        F.col("Lkp_Valid_Data.PROC_TYPE"),
        F.col("Lkp_Valid_Data.DT_OF_SVC"),
        F.col("Lkp_Valid_Data.RSLT_VAL"),
        F.col("Lkp_Valid_Data.RNDR_NTNL_PROV_ID"),
        F.col("Lkp_Valid_Data.RNDR_PROV_TYP"),
        F.col("Lkp_Valid_Data.PROC_CD_CPT"),
        F.col("Lkp_Valid_Data.PROC_CD_CPT_MOD_1"),
        F.col("Lkp_Valid_Data.PROC_CD_CPT_MOD_2"),
        F.col("Lkp_Valid_Data.PROC_CD_CPTII"),
        F.col("Lkp_Valid_Data.PROC_CD_CPTII_MOD_1"),
        F.col("Lkp_Valid_Data.PROC_CD_CPTII_MOD_2"),
        F.col("Lkp_Valid_Data.HCPCS"),
        F.col("Lkp_Valid_Data.ICD_10_PCS_1"),
        F.col("Lkp_Valid_Data.SNOMED"),
        F.col("Lkp_Valid_Data.CVX"),
        F.col("Lkp_Valid_Data.SOURCE_ID"),
        F.col("Lkp_Valid_Data.MBR_UNIQ_KEY"),
        F.col("Lkp_Valid_Data.REJECT_REASON").alias("REJECT_REASON")
    )
)

# Columns_Out => where Lkp_Invalid_Flg.FLG is null
df_Columns_Out = (
    df_temp_lkp_src_id_rej
    .filter(F.col("Lkp_Invalid_Flg.FLG").isNull())
    .select(
        F.col("Lkp_Valid_Data.POL_NO").alias("POL_NO"),
        F.col("Lkp_Valid_Data.PATN_LAST_NM").alias("PATN_LAST_NM"),
        F.col("Lkp_Valid_Data.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        F.col("Lkp_Valid_Data.PATN_MID_NM").alias("PATN_MID_NM"),
        F.col("Lkp_Valid_Data.MBI").alias("MBI"),
        F.col("Lkp_Valid_Data.DOB").alias("DOB"),
        F.col("Lkp_Valid_Data.GNDR").alias("GNDR"),
        F.col("Lkp_Valid_Data.PROC_TYPE").alias("PROC_TYPE"),
        F.col("Lkp_Valid_Data.DT_OF_SVC").alias("DT_OF_SVC"),
        F.col("Lkp_Valid_Data.RSLT_VAL").alias("RSLT_VAL"),
        F.col("Lkp_Valid_Data.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
        F.col("Lkp_Valid_Data.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
        F.col("Lkp_Valid_Data.PROC_CD_CPT").alias("PROC_CD_CPT"),
        F.col("Lkp_Valid_Data.PROC_CD_CPT_MOD_1").alias("PROC_CD_CPT_MOD_1"),
        F.col("Lkp_Valid_Data.PROC_CD_CPT_MOD_2").alias("PROC_CD_CPT_MOD_2"),
        F.col("Lkp_Valid_Data.PROC_CD_CPTII").alias("PROC_CD_CPTII"),
        F.col("Lkp_Valid_Data.PROC_CD_CPTII_MOD_1").alias("PROC_CD_CPTII_MOD_1"),
        F.col("Lkp_Valid_Data.PROC_CD_CPTII_MOD_2").alias("PROC_CD_CPTII_MOD_2"),
        F.col("Lkp_Valid_Data.HCPCS").alias("HCPCS"),
        F.col("Lkp_Valid_Data.ICD_10_PCS_1").alias("ICD_10_PCS_1"),
        F.col("Lkp_Valid_Data.SNOMED").alias("SNOMED"),
        F.col("Lkp_Valid_Data.CVX").alias("CVX"),
        F.col("Lkp_Valid_Data.SOURCE_ID").alias("SOURCE_ID"),
        F.col("Lkp_Valid_Data.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lkp_Valid_Data.FLG").alias("FLG"),
        F.lit("").alias("REJECT_REASON"),
        F.col("Lkp_Valid_Data.ARE_Columns_13_16_19_20_21_22_ALL_NULL").alias("ARE_Columns_13_16_19_20_21_22_ALL_NULL")
    )
)

# Stage 14: Fnl_All_Rejects (PxFunnel) => merges df_xfm_reject_delim, df_Lnk_SrcID_Reject1, df_Lnk_SrcID_Reject2
df_fnl_all_rejects = df_xfm_reject_delim.select(
    "POL_NO", "PATN_LAST_NM", "PATN_FIRST_NM", "PATN_MID_NM",
    "MBI", "DOB", "GNDR", "PROC_TYPE", "DT_OF_SVC", "RSLT_VAL",
    "RNDR_NTNL_PROV_ID", "RNDR_PROV_TYP", "PROC_CD_CPT", "PROC_CD_CPT_MOD_1",
    "PROC_CD_CPT_MOD_2", "PROC_CD_CPTII", "PROC_CD_CPTII_MOD_1", "PROC_CD_CPTII_MOD_2",
    "HCPCS", "ICD_10_PCS_1", "SNOMED", "CVX", "SOURCE_ID", "MBR_UNIQ_KEY",
    "REJECT_REASON"
).unionByName(
    df_Lnk_SrcID_Reject1.select(
        "POL_NO", "PATN_LAST_NM", "PATN_FIRST_NM", "PATN_MID_NM",
        "MBI", "DOB", "GNDR", "PROC_TYPE", "DT_OF_SVC", "RSLT_VAL",
        "RNDR_NTNL_PROV_ID", "RNDR_PROV_TYP", "PROC_CD_CPT", "PROC_CD_CPT_MOD_1",
        "PROC_CD_CPT_MOD_2", "PROC_CD_CPTII", "PROC_CD_CPTII_MOD_1", "PROC_CD_CPTII_MOD_2",
        "HCPCS", "ICD_10_PCS_1", "SNOMED", "CVX", "SOURCE_ID", "MBR_UNIQ_KEY",
        "REJECT_REASON"
    )
).unionByName(
    df_Lnk_SrcID_Reject2.select(
        "POL_NO", "PATN_LAST_NM", "PATN_FIRST_NM", "PATN_MID_NM",
        "MBI", "DOB", "GNDR", "PROC_TYPE", "DT_OF_SVC", "RSLT_VAL",
        "RNDR_NTNL_PROV_ID", "RNDR_PROV_TYP", "PROC_CD_CPT", "PROC_CD_CPT_MOD_1",
        "PROC_CD_CPT_MOD_2", "PROC_CD_CPTII", "PROC_CD_CPTII_MOD_1", "PROC_CD_CPTII_MOD_2",
        "HCPCS", "ICD_10_PCS_1", "SNOMED", "CVX", "SOURCE_ID", "MBR_UNIQ_KEY",
        "REJECT_REASON"
    )
)

# Stage 15: Seq_Reject_File (PxSequentialFile)
# Write df_fnl_all_rejects to #RejectFile#
# Use rpad for char/varchar columns if we know lengths. From the design, many are varchar. The job itself does not specify exact lengths except a few "char" = 1, but let's consistently do an rpad for them. We must keep the same column order.
df_fnl_all_rejects_rpad = df_fnl_all_rejects.select(
    rpad(F.col("POL_NO"), 255, " ").alias("POL_NO"),
    rpad(F.col("PATN_LAST_NM"), 255, " ").alias("PATN_LAST_NM"),
    rpad(F.col("PATN_FIRST_NM"), 255, " ").alias("PATN_FIRST_NM"),
    rpad(F.col("PATN_MID_NM"), 255, " ").alias("PATN_MID_NM"),
    rpad(F.col("MBI"), 255, " ").alias("MBI"),
    rpad(F.col("DOB"), 255, " ").alias("DOB"),
    rpad(F.col("GNDR"), 255, " ").alias("GNDR"),
    rpad(F.col("PROC_TYPE"), 255, " ").alias("PROC_TYPE"),
    rpad(F.col("DT_OF_SVC"), 255, " ").alias("DT_OF_SVC"),
    rpad(F.col("RSLT_VAL"), 255, " ").alias("RSLT_VAL"),
    rpad(F.col("RNDR_NTNL_PROV_ID"), 255, " ").alias("RNDR_NTNL_PROV_ID"),
    rpad(F.col("RNDR_PROV_TYP"), 255, " ").alias("RNDR_PROV_TYP"),
    rpad(F.col("PROC_CD_CPT"), 255, " ").alias("PROC_CD_CPT"),
    rpad(F.col("PROC_CD_CPT_MOD_1"), 255, " ").alias("PROC_CD_CPT_MOD_1"),
    rpad(F.col("PROC_CD_CPT_MOD_2"), 255, " ").alias("PROC_CD_CPT_MOD_2"),
    rpad(F.col("PROC_CD_CPTII"), 255, " ").alias("PROC_CD_CPTII"),
    rpad(F.col("PROC_CD_CPTII_MOD_1"), 255, " ").alias("PROC_CD_CPTII_MOD_1"),
    rpad(F.col("PROC_CD_CPTII_MOD_2"), 255, " ").alias("PROC_CD_CPTII_MOD_2"),
    rpad(F.col("HCPCS"), 255, " ").alias("HCPCS"),
    rpad(F.col("ICD_10_PCS_1"), 255, " ").alias("ICD_10_PCS_1"),
    rpad(F.col("SNOMED"), 255, " ").alias("SNOMED"),
    rpad(F.col("CVX"), 255, " ").alias("CVX"),
    rpad(F.col("SOURCE_ID"), 255, " ").alias("SOURCE_ID"),
    rpad(F.col("MBR_UNIQ_KEY"), 255, " ").alias("MBR_UNIQ_KEY"),
    rpad(F.col("REJECT_REASON"), 255, " ").alias("REJECT_REASON")
)
write_files(
    df_fnl_all_rejects_rpad,
    f"{adls_path}/verified/{RejectFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)

# Stage 16: Xfm_Column_Chk (CTransformerStage) => we apply many stage variables to check missing conditions or length conditions.
# Instead of expansions for each stage variable, replicate the logic in Python as columns. Then produce 3 outputs: Lnk_Valid, Lnk_Error, CondErrors.

df_col_chk_temp = df_Columns_Out.withColumn("svPolno", F.when((F.col("POL_NO").isNull())|(F.trim("POL_NO")==""), "Patient Insurence Policy Number,").otherwise("")) \
    .withColumn("svPatnlastNm", F.when((F.col("PATN_LAST_NM").isNull())|(F.trim("PATN_LAST_NM")==""), "Patient Last Name,").otherwise("")) \
    .withColumn("svPatnFrstNm", F.when((F.col("PATN_FIRST_NM").isNull())|(F.trim("PATN_FIRST_NM")==""), "Patient First Name,").otherwise("")) \
    .withColumn("svDOB", F.when((F.col("DOB").isNull())|(F.trim("DOB")==""), "Patient Date Of Birth,").otherwise("")) \
    .withColumn("svDateOfSrvc", F.when((F.col("DT_OF_SVC").isNull())|(F.trim("DT_OF_SVC")==""), "Service Date,").otherwise("")) \
    .withColumn("svRendNPI", F.when((F.col("RNDR_NTNL_PROV_ID").isNull())|(F.trim("RNDR_NTNL_PROV_ID")==""), "Rendering Provider NPI,").otherwise("")) \
    .withColumn("svSrcID", F.when((F.col("SOURCE_ID").isNull())|(F.trim("SOURCE_ID")==""), "Source ID,").otherwise("")) \
    .withColumn("svRejectFlg", F.expr("""
CASE WHEN (svDateOfSrvc <> '' OR svDOB <> '' OR svPatnFrstNm <> '' OR svPatnlastNm <> '' OR svPolno <> '' OR svRendNPI <> '' OR svSrcID <> '') THEN 1 ELSE 0 END
""")) \
    .withColumn("svRejectMesg", F.concat_ws("", "svDateOfSrvc","svDOB","svPatnFrstNm","svPatnlastNm","svPolno","svRendNPI","svSrcID")) \
    .withColumn("svLenPolno", F.when(F.length(F.trim(F.coalesce(F.col("POL_NO"),F.lit(""))))>20, "Patient Insurance Policy Number,").otherwise("")) \
    .withColumn("svLenPatnlastNm", F.when(F.length(F.trim(F.coalesce(F.col("PATN_LAST_NM"),F.lit(""))))>50, "Patient Last Name,").otherwise("")) \
    .withColumn("svLenPatnFirstNm", F.when(F.length(F.trim(F.coalesce(F.col("PATN_LAST_NM"),F.lit(""))))>50, "Patient Last Name,").otherwise("")) \
    .withColumn("svLenPatnMidNm", F.when(F.length(F.trim(F.coalesce(F.col("PATN_FIRST_NM"),F.lit(""))))>50, "Patient First Name,").otherwise("")) \
    .withColumn("svLenMbi", F.when(F.length(F.trim(F.coalesce(F.col("MBI"),F.lit(""))))>11, "MBI,").otherwise("")) \
    .withColumn("svLenDob", F.when(F.length(F.trim(F.coalesce(F.col("DOB"),F.lit(""))))>8, "Patient Date of Birth,").otherwise("")) \
    .withColumn("svLenProcType", F.when(F.length(F.trim(F.coalesce(F.col("PROC_TYPE"),F.lit(""))))>50, "Procedure Type,").otherwise("")) \
    .withColumn("svLenDtOfSrvc", F.when(F.length(F.trim(F.coalesce(F.col("DT_OF_SVC"),F.lit(""))))>8, "Service Date,").otherwise("")) \
    .withColumn("svLenRsltVal", F.when(F.length(F.trim(F.coalesce(F.col("RSLT_VAL"),F.lit(""))))>25, "Result,").otherwise("")) \
    .withColumn("svLenRndrNtnlProc", F.when(F.length(F.trim(F.coalesce(F.col("RNDR_NTNL_PROV_ID"),F.lit(""))))>10, "Rendering Provider NPI,").otherwise("")) \
    .withColumn("svLenRndrProvType", F.when(F.length(F.trim(F.coalesce(F.col("RNDR_PROV_TYP"),F.lit(""))))>25, "Rendering Provider Type,").otherwise("")) \
    .withColumn("svLenSrcID", F.when(F.length(F.trim(F.coalesce(F.col("SOURCE_ID"),F.lit(""))))>50, "Source ID").otherwise("")) \
    .withColumn("svLenCPT", F.when(F.length(F.trim(F.coalesce(F.col("PROC_CD_CPT"),F.lit(""))))>5, "PROC CD CPT").otherwise("")) \
    .withColumn("svLenProcCdCptMod1", F.when(F.length(F.trim(F.coalesce(F.col("PROC_CD_CPT_MOD_1"),F.lit(""))))>2, "PROC CD CPT MOD 1").otherwise("")) \
    .withColumn("svLenProcCdCptMod2", F.when(F.length(F.trim(F.coalesce(F.col("PROC_CD_CPT_MOD_2"),F.lit(""))))>2, "PROC CD CPT MOD 2").otherwise("")) \
    .withColumn("svLenProcCdCptII", F.when(F.length(F.trim(F.coalesce(F.col("PROC_CD_CPTII"),F.lit(""))))>5, "PROC CD CPT II").otherwise("")) \
    .withColumn("svLenProcCdCptIIMod1", F.when(F.length(F.trim(F.coalesce(F.col("PROC_CD_CPTII_MOD_1"),F.lit(""))))>2, "PROC CD CPT II MOD 1").otherwise("")) \
    .withColumn("svLenProcCdCptIIMod2", F.when(F.length(F.trim(F.coalesce(F.col("PROC_CD_CPTII_MOD_2"),F.lit(""))))>2, "PROC CD CPT II MOD 2").otherwise("")) \
    .withColumn("svLenHCPCS", F.when(F.length(F.trim(F.coalesce(F.col("HCPCS"),F.lit(""))))>5, "HCPCS").otherwise("")) \
    .withColumn("svLenICD10PCS1", F.when(F.length(F.trim(F.coalesce(F.col("ICD_10_PCS_1"),F.lit(""))))>7, "ICD 10 PCS 1").otherwise("")) \
    .withColumn("svLenSnoMed", F.when(F.length(F.trim(F.coalesce(F.col("SNOMED"),F.lit(""))))>20, "SNOMED").otherwise("")) \
    .withColumn("svLenRejectFlg", F.expr("""
CASE WHEN (
  svLenPolno <> '' OR svLenPatnlastNm <> '' OR svLenPatnFirstNm <> '' OR svLenPatnMidNm <> ''
  OR svLenMbi <> '' OR svLenDob <> '' OR svLenDtOfSrvc <> '' OR svLenRsltVal <> '' OR svLenRndrNtnlProc <> ''
  OR svLenRndrProvType <> '' OR svLenSrcID <> '' OR svLenCPT <> '' OR svLenProcCdCptMod1 <> ''
  OR svLenProcCdCptMod2 <> '' OR svLenProcCdCptII <> '' OR svLenProcCdCptIIMod1 <> ''
  OR svLenProcCdCptIIMod2 <> '' OR svLenHCPCS <> '' OR svLenICD10PCS1 <> '' OR svLenSnoMed <> ''
) THEN 2 ELSE 0 END
""")) \
    .withColumn("svLenRejectMesg", F.concat_ws("", "svLenPolno","svLenPatnlastNm","svLenPatnFirstNm","svLenPatnMidNm",
                                              "svLenMbi","svLenDob","svLenDtOfSrvc","svLenRsltVal","svLenRndrNtnlProc","svLenRndrProvType","svLenSrcID","svLenCPT","svLenProcCdCptMod1",
                                              "svLenProcCdCptMod2","svLenProcCdCptII","svLenProcCdCptIIMod1","svLenProcCdCptIIMod2","svLenHCPCS","svLenICD10PCS1","svLenSnoMed")) \
    .withColumn("svConditionalFlg", F.expr("""
CASE WHEN ARE_Columns_13_16_19_20_21_22_ALL_NULL=0 THEN 3
     WHEN ARE_Columns_13_16_19_20_21_22_ALL_NULL>1 THEN 4
     WHEN length(trim(DT_OF_SVC))<8 THEN 5
     WHEN length(trim(DT_OF_SVC))=8 AND IsValid("date", substring(trim(DT_OF_SVC),1,4)||"-"||substring(trim(DT_OF_SVC),5,2)||"-"||substring(trim(DT_OF_SVC),7,2))=false THEN 6
     ELSE 1 END
""")) \
    .withColumn("svConditionalRejectMesg", F.expr("""
CASE WHEN svConditionalFlg=3 THEN 'CTP, PROC_CD_CPTII, HCPCS, ICD_10_PCS_1, SNOMED and CVX are all NULL'
     WHEN svConditionalFlg=4 THEN 'More than one of these has a value: CTP, PROC_CD_CPTII, HCPCS, ICD_10_PCS_1, SNOMED, CVX'
     WHEN svConditionalFlg=5 THEN 'DT_OF_SVC is not 8 characters'
     WHEN svConditionalFlg=6 THEN 'DT_OF_SVC is not a valid date'
     ELSE '' END
"""))

# Lnk_Valid => constraint => svRejectFlg=0 AND svLenRejectFlg=0 AND svConditionalFlg=1
df_Lnk_Valid = df_col_chk_temp.filter((F.col("svRejectFlg")==0) & (F.col("svLenRejectFlg")==0) & (F.col("svConditionalFlg")==1)).select(
    F.upper(F.trim(F.col("POL_NO"))).alias("POL_NO"),
    F.upper(F.trim(F.col("PATN_LAST_NM"))).alias("PATN_LAST_NM"),
    F.upper(F.trim(F.col("PATN_FIRST_NM"))).alias("PATN_FIRST_NM"),
    F.upper(F.trim(F.col("PATN_MID_NM"))).alias("PATN_MID_NM"),
    F.trim(F.col("MBI")).alias("MBI"),
    F.trim(F.col("DOB")).alias("DOB"),
    F.trim(F.col("GNDR")).alias("GNDR"),
    F.trim(F.col("PROC_TYPE")).alias("PROC_TYPE"),
    F.trim(F.col("DT_OF_SVC")).alias("DT_OF_SVC"),
    F.trim(F.col("RSLT_VAL")).alias("RSLT_VAL"),
    F.trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
    F.trim(F.col("RNDR_PROV_TYP")).alias("RNDR_PROV_TYP"),
    F.trim(F.col("PROC_CD_CPT")).alias("PROC_CD_CPT"),
    F.trim(F.col("PROC_CD_CPT_MOD_1")).alias("PROC_CD_CPT_MOD_1"),
    F.trim(F.col("PROC_CD_CPT_MOD_2")).alias("PROC_CD_CPT_MOD_2"),
    F.trim(F.col("PROC_CD_CPTII")).alias("PROC_CD_CPTII"),
    F.trim(F.col("PROC_CD_CPTII_MOD_1")).alias("PROC_CD_CPTII_MOD_1"),
    F.trim(F.col("PROC_CD_CPTII_MOD_2")).alias("PROC_CD_CPTII_MOD_2"),
    F.trim(F.col("HCPCS")).alias("HCPCS"),
    F.trim(F.col("ICD_10_PCS_1")).alias("ICD_10_PCS_1"),
    F.trim(F.col("SNOMED")).alias("SNOMED"),
    F.trim(F.col("CVX")).alias("CVX"),
    F.trim(F.col("SOURCE_ID")).alias("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.upper(F.trim(F.substring(F.col("PATN_FIRST_NM"),1,3))).alias("PATN_FIRST_NM_THREE")
)

# Lnk_Error => constraint => svRejectFlg=1 OR svLenRejectFlg=2
df_Lnk_Error = df_col_chk_temp.filter((F.col("svRejectFlg")==1)|(F.col("svLenRejectFlg")==2)).select(
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
    F.trim(F.col("PROC_CD_CPT")).alias("PROC_CD_CPT"),
    F.trim(F.col("PROC_CD_CPT_MOD_1")).alias("PROC_CD_CPT_MOD_1"),
    F.trim(F.col("PROC_CD_CPT_MOD_2")).alias("PROC_CD_CPT_MOD_2"),
    F.trim(F.col("PROC_CD_CPTII")).alias("PROC_CD_CPTII"),
    F.trim(F.col("PROC_CD_CPTII_MOD_1")).alias("PROC_CD_CPTII_MOD_1"),
    F.trim(F.col("PROC_CD_CPTII_MOD_2")).alias("PROC_CD_CPTII_MOD_2"),
    F.col("HCPCS"),
    F.trim(F.col("ICD_10_PCS_1")).alias("ICD_10_PCS_1"),
    F.trim(F.col("SNOMED")).alias("SNOMED"),
    F.col("CVX"),
    F.trim(F.col("SOURCE_ID")).alias("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(F.col("svRejectFlg")==1, F.concat(F.trim(F.col("svRejectMesg")),F.lit(" is missing Value")))
     .when(F.col("svLenRejectFlg")==2, F.concat(F.trim(F.col("svLenRejectMesg")),F.lit(" Value Exceeding Maximun length")))
     .otherwise("").alias("ERROR_REASON")
)

# CondErrors => constraint => svConditionalFlg > 1
df_CondErrors = df_col_chk_temp.filter(F.col("svConditionalFlg")>1).select(
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
    F.trim(F.col("PROC_CD_CPT")).alias("PROC_CD_CPT"),
    F.trim(F.col("PROC_CD_CPT_MOD_1")).alias("PROC_CD_CPT_MOD_1"),
    F.trim(F.col("PROC_CD_CPT_MOD_2")).alias("PROC_CD_CPT_MOD_2"),
    F.trim(F.col("PROC_CD_CPTII")).alias("PROC_CD_CPTII"),
    F.trim(F.col("PROC_CD_CPTII_MOD_1")).alias("PROC_CD_CPTII_MOD_1"),
    F.trim(F.col("PROC_CD_CPTII_MOD_2")).alias("PROC_CD_CPTII_MOD_2"),
    F.trim(F.col("HCPCS")).alias("HCPCS"),
    F.trim(F.col("ICD_10_PCS_1")).alias("ICD_10_PCS_1"),
    F.trim(F.col("SNOMED")).alias("SNOMED"),
    F.col("CVX"),
    F.trim(F.col("SOURCE_ID")).alias("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svConditionalRejectMesg").alias("ERROR_REASON")
)

# Stage 17: ConditionalChecks => output => merges into Lnk_All_Error
df_ConditionalErrors = df_CondErrors.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","PROC_TYPE","DT_OF_SVC","RSLT_VAL",
    "RNDR_NTNL_PROV_ID","RNDR_PROV_TYP","PROC_CD_CPT","PROC_CD_CPT_MOD_1","PROC_CD_CPT_MOD_2","PROC_CD_CPTII",
    "PROC_CD_CPTII_MOD_1","PROC_CD_CPTII_MOD_2","HCPCS","ICD_10_PCS_1","SNOMED","CVX","SOURCE_ID","MBR_UNIQ_KEY",
    F.col("ERROR_REASON")
)

# Stage 18: Funnel => merges df_Lnk_Error & df_ConditionalErrors => Lnk_All_Error
df_Lnk_All_Error = df_Lnk_Error.select(
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","PROC_TYPE","DT_OF_SVC","RSLT_VAL",
    "RNDR_NTNL_PROV_ID","RNDR_PROV_TYP","PROC_CD_CPT","PROC_CD_CPT_MOD_1","PROC_CD_CPT_MOD_2","PROC_CD_CPTII",
    "PROC_CD_CPTII_MOD_1","PROC_CD_CPTII_MOD_2","HCPCS","ICD_10_PCS_1","SNOMED","CVX","SOURCE_ID","ERROR_REASON","MBR_UNIQ_KEY"
).unionByName(
    df_ConditionalErrors.select(
        "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","PROC_TYPE","DT_OF_SVC","RSLT_VAL",
        "RNDR_NTNL_PROV_ID","RNDR_PROV_TYP","PROC_CD_CPT","PROC_CD_CPT_MOD_1","PROC_CD_CPT_MOD_2","PROC_CD_CPTII",
        "PROC_CD_CPTII_MOD_1","PROC_CD_CPTII_MOD_2","HCPCS","ICD_10_PCS_1","SNOMED","CVX","SOURCE_ID","ERROR_REASON","MBR_UNIQ_KEY"
    )
)

# Stage 19: Seq_Error_File (PxSequentialFile) => writes df_Lnk_All_Error to #ErrorFile#
# Apply rpad on char/varchar columns
df_Lnk_All_Error_rpad = df_Lnk_All_Error.select(
    rpad(F.col("POL_NO"),255," ").alias("POL_NO"),
    rpad(F.col("PATN_LAST_NM"),255," ").alias("PATN_LAST_NM"),
    rpad(F.col("PATN_FIRST_NM"),255," ").alias("PATN_FIRST_NM"),
    rpad(F.col("PATN_MID_NM"),255," ").alias("PATN_MID_NM"),
    rpad(F.col("MBI"),255," ").alias("MBI"),
    rpad(F.col("DOB"),255," ").alias("DOB"),
    rpad(F.col("GNDR"),255," ").alias("GNDR"),
    rpad(F.col("PROC_TYPE"),255," ").alias("PROC_TYPE"),
    rpad(F.col("DT_OF_SVC"),255," ").alias("DT_OF_SVC"),
    rpad(F.col("RSLT_VAL"),255," ").alias("RSLT_VAL"),
    rpad(F.col("RNDR_NTNL_PROV_ID"),255," ").alias("RNDR_NTNL_PROV_ID"),
    rpad(F.col("RNDR_PROV_TYP"),255," ").alias("RNDR_PROV_TYP"),
    rpad(F.col("PROC_CD_CPT"),255," ").alias("PROC_CD_CPT"),
    rpad(F.col("PROC_CD_CPT_MOD_1"),255," ").alias("PROC_CD_CPT_MOD_1"),
    rpad(F.col("PROC_CD_CPT_MOD_2"),255," ").alias("PROC_CD_CPT_MOD_2"),
    rpad(F.col("PROC_CD_CPTII"),255," ").alias("PROC_CD_CPTII"),
    rpad(F.col("PROC_CD_CPTII_MOD_1"),255," ").alias("PROC_CD_CPTII_MOD_1"),
    rpad(F.col("PROC_CD_CPTII_MOD_2"),255," ").alias("PROC_CD_CPTII_MOD_2"),
    rpad(F.col("HCPCS"),255," ").alias("HCPCS"),
    rpad(F.col("ICD_10_PCS_1"),255," ").alias("ICD_10_PCS_1"),
    rpad(F.col("SNOMED"),255," ").alias("SNOMED"),
    rpad(F.col("CVX"),255," ").alias("CVX"),
    rpad(F.col("SOURCE_ID"),255," ").alias("SOURCE_ID"),
    rpad(F.col("ERROR_REASON"),255," ").alias("ERROR_REASON"),
    rpad(F.col("MBR_UNIQ_KEY"),255," ").alias("MBR_UNIQ_KEY")
)
write_files(
    df_Lnk_All_Error_rpad,
    f"{adls_path}/verified/{ErrorFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)

# Next: Calc_Claim_ID (Xfm_Calculation)
df_Calc_Claim_ID = df_Lnk_Valid.select(
    F.concat(F.lit("P"), F.col("MBR_UNIQ_KEY"), F.col("DT_OF_SVC")).alias("CLM_ID"),
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","PROC_TYPE","DT_OF_SVC","RSLT_VAL",
    "RNDR_NTNL_PROV_ID","RNDR_PROV_TYP","PROC_CD_CPT","PROC_CD_CPT_MOD_1","PROC_CD_CPT_MOD_2",
    "PROC_CD_CPTII","PROC_CD_CPTII_MOD_1","PROC_CD_CPTII_MOD_2","HCPCS","ICD_10_PCS_1","SNOMED","CVX",
    "SOURCE_ID","MBR_UNIQ_KEY"
)

# Sort_CLM_ID (PxSort) => sorting by CLM_ID
df_Sort_CLM_ID = df_Calc_Claim_ID.sort(F.col("CLM_ID"))

df_Lnk_CLM_ID = df_Sort_CLM_ID.withColumn("keyChange", F.when(F.lag("CLM_ID").over(
    F.Window.orderBy("CLM_ID"))==F.col("CLM_ID"),0).otherwise(1))  # approximate KeyChange

# Xfm_CLM_ID_Incr => define stage variables for line seq
# We'll do a running seq within each key. 
w = F.Window.partitionBy().orderBy("CLM_ID")
df_xfm_clm_id_incr_temp = df_Lnk_CLM_ID.withColumn("rowWithinKey", F.sum("keyChange").over(w)).withColumn("rowNumberGlobal", F.monotonically_increasing_id())

# We emulate the Stage Variable logic: if keyChange=1 => reset => else increment
# Because DataStage is row-by-row code, here we do an approach that for each group of identical CLM_ID we create a row index. Then combine it to produce the conceptual line number. For simplicity, any row within the same CLM_ID increments up to 99.
window_clm = F.Window.partitionBy("CLM_ID").orderBy("rowNumberGlobal")
df_xfm_clm_id_incr = df_xfm_clm_id_incr_temp.withColumn(
    "lineSeqWithinClm",
    F.row_number().over(window_clm)
)
df_Xfm_CLM_ID_Incr = df_xfm_clm_id_incr.select(
    F.when(F.col("keyChange")==1, 
           F.concat(F.col("CLM_ID"), F.lit("00"))
    ).otherwise(
        F.concat(F.col("CLM_ID"), F.lit("0"), F.col("lineSeqWithinClm"))
    ).alias("CLM_ID"),
    "POL_NO","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","MBI","DOB","GNDR","PROC_TYPE","DT_OF_SVC","RSLT_VAL",
    "RNDR_NTNL_PROV_ID","RNDR_PROV_TYP","PROC_CD_CPT","PROC_CD_CPT_MOD_1","PROC_CD_CPT_MOD_2",
    "PROC_CD_CPTII","PROC_CD_CPTII_MOD_1","PROC_CD_CPTII_MOD_2","HCPCS","ICD_10_PCS_1","SNOMED","CVX","SOURCE_ID","MBR_UNIQ_KEY",
    F.lit("1").alias("CLM_LN_SEQ"),  # from DS job (length=1)
    "keyChange"
).filter(F.col("lineSeqWithinClm")<100)  # The constraint "svCurrentClaimSeq < 100"

# LookUpProvSpecialty => PxLookup => Primary: df_Xfm_CLM_ID_Incr, Lookup: df_PROVIDER on RNDR_NTNL_PROV_ID=NTNL_PROV_ID
df_temp_LookUpProvSpecialty = (
    df_Xfm_CLM_ID_Incr.alias("Lookup")
    .join(
        df_PROVIDER.alias("Provider"),
        F.col("Lookup.RNDR_NTNL_PROV_ID")==F.col("Provider.NTNL_PROV_ID"),
        "left"
    )
)
df_SetProcCdCPT = df_temp_LookUpProvSpecialty.select(
    F.col("Lookup.CLM_ID"),
    F.col("Lookup.POL_NO"),
    F.col("Lookup.PATN_LAST_NM"),
    F.col("Lookup.PATN_FIRST_NM"),
    F.col("Lookup.PATN_MID_NM"),
    F.col("Lookup.MBI"),
    F.col("Lookup.DOB"),
    F.col("Lookup.GNDR"),
    F.col("Lookup.PROC_TYPE"),
    F.col("Lookup.DT_OF_SVC"),
    F.col("Lookup.RSLT_VAL"),
    F.col("Lookup.RNDR_NTNL_PROV_ID"),
    F.col("Provider.IS_EYE_DOC").alias("IS_EYE_DOC"),
    F.col("Lookup.RNDR_PROV_TYP"),
    F.col("Lookup.PROC_CD_CPT"),
    F.col("Lookup.PROC_CD_CPT_MOD_1"),
    F.col("Lookup.PROC_CD_CPT_MOD_2"),
    F.col("Lookup.PROC_CD_CPTII"),
    F.col("Lookup.PROC_CD_CPTII_MOD_1"),
    F.col("Lookup.PROC_CD_CPTII_MOD_2"),
    F.col("Lookup.HCPCS"),
    F.col("Lookup.ICD_10_PCS_1"),
    F.col("Lookup.SNOMED"),
    F.col("Lookup.CVX"),
    F.col("Lookup.SOURCE_ID"),
    F.col("Lookup.MBR_UNIQ_KEY"),
    F.col("Lookup.CLM_LN_SEQ"),
)

# Stage: Set_Proc_CD_CPT => we have stage vars to pick final PROC_CD_CPT. 
df_set_proc_cd_cpt_temp = df_SetProcCdCPT.withColumn(
    "svIsEyeDoc",
    F.when(F.col("IS_EYE_DOC")=="Y","Y").otherwise("N")
).withColumn(
    "svPROCCDCPT",
    F.when((F.col("PROC_CD_CPT").isNotNull())&(F.trim("PROC_CD_CPT")!=""), F.col("PROC_CD_CPT"))
     .when((F.col("PROC_CD_CPTII").isNotNull())&(F.trim("PROC_CD_CPTII")!=""), F.col("PROC_CD_CPTII"))
     .when((F.col("HCPCS").isNotNull())&(F.trim("HCPCS")!=""), F.col("HCPCS"))
     .when((F.col("ICD_10_PCS_1").isNotNull())&(F.trim("ICD_10_PCS_1")!=""), F.col("ICD_10_PCS_1"))
     .otherwise("NA")
).withColumn(
    "finalPROC",
    F.when(
        (F.col("svIsEyeDoc")=="Y") & (F.upper(F.trim(F.coalesce(F.col("RSLT_VAL"),F.lit(""))))=="POSITIVE"),
        F.lit("2022F")
    ).when(
        (F.col("svIsEyeDoc")=="Y") & (F.upper(F.trim(F.coalesce(F.col("RSLT_VAL"),F.lit(""))))=="NEGATIVE"),
        F.lit("2023F")
    ).otherwise(F.regexp_replace(F.col("svPROCCDCPT"), "\\.", ""))
)

df_Lnk_Out = df_set_proc_cd_cpt_temp.select(
    F.col("CLM_ID"),
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
    F.lit("NA").alias("RNDR_PROV_TYP"),  # per DS job's Expression
    F.col("finalPROC").alias("PROC_CD_CPT"),
    F.col("PROC_CD_CPT_MOD_1"),
    F.col("PROC_CD_CPT_MOD_2"),
    F.col("PROC_CD_CPTII_MOD_1"),
    F.col("PROC_CD_CPTII_MOD_2"),
    F.col("SNOMED"),
    F.col("CVX"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.col("CLM_LN_SEQ")
)

# Stage: Seq_Valid_Recds => write df_Lnk_Out to #InFile_F#
# Apply rpad
df_Lnk_Out_rpad = df_Lnk_Out.select(
    rpad(F.col("CLM_ID"),255," ").alias("CLM_ID"),
    rpad(F.col("POL_NO"),255," ").alias("POL_NO"),
    rpad(F.col("PATN_LAST_NM"),255," ").alias("PATN_LAST_NM"),
    rpad(F.col("PATN_FIRST_NM"),255," ").alias("PATN_FIRST_NM"),
    rpad(F.col("PATN_MID_NM"),255," ").alias("PATN_MID_NM"),
    rpad(F.col("MBI"),255," ").alias("MBI"),
    rpad(F.col("DOB"),255," ").alias("DOB"),
    rpad(F.col("GNDR"),255," ").alias("GNDR"),
    rpad(F.col("PROC_TYPE"),255," ").alias("PROC_TYPE"),
    rpad(F.col("DT_OF_SVC"),255," ").alias("DT_OF_SVC"),
    rpad(F.col("RSLT_VAL"),255," ").alias("RSLT_VAL"),
    rpad(F.col("RNDR_NTNL_PROV_ID"),255," ").alias("RNDR_NTNL_PROV_ID"),
    rpad(F.col("RNDR_PROV_TYP"),255," ").alias("RNDR_PROV_TYP"),
    rpad(F.col("PROC_CD_CPT"),255," ").alias("PROC_CD_CPT"),
    rpad(F.col("PROC_CD_CPT_MOD_1"),255," ").alias("PROC_CD_CPT_MOD_1"),
    rpad(F.col("PROC_CD_CPT_MOD_2"),255," ").alias("PROC_CD_CPT_MOD_2"),
    rpad(F.col("PROC_CD_CPTII_MOD_1"),255," ").alias("PROC_CD_CPTII_MOD_1"),
    rpad(F.col("PROC_CD_CPTII_MOD_2"),255," ").alias("PROC_CD_CPTII_MOD_2"),
    rpad(F.col("SNOMED"),255," ").alias("SNOMED"),
    rpad(F.col("CVX"),255," ").alias("CVX"),
    rpad(F.col("SOURCE_ID"),255," ").alias("SOURCE_ID"),
    rpad(F.col("MBR_UNIQ_KEY"),255," ").alias("MBR_UNIQ_KEY"),
    rpad(F.col("CLM_LN_SEQ"),1," ").alias("CLM_LN_SEQ")  # length=1 per job
)

write_files(
    df_Lnk_Out_rpad,
    f"{adls_path}/verified/{InFile_F}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)