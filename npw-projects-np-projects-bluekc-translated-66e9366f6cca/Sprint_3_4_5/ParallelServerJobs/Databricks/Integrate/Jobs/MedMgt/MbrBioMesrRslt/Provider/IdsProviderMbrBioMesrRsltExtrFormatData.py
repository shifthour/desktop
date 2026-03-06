# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2020 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsProviderBioMesrRsltCntl
# MAGIC                    
# MAGIC 
# MAGIC Processing:Source Input file forProvider Bio Metric Reject File Creation and formatted
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary  
# MAGIC Modifications:                        
# MAGIC                                                    \(9)\(9)Project/                                                                                                                     Code                   \(9)\(9)Date
# MAGIC Developer           \(9)\(9)Date                \(9)Altiris #        \(9)Change Description                                                                   Reviewer            \(9)\(9)Reviewed
# MAGIC -------------------------  \(9)\(9)---------------------   \(9)----------------   \(9)---------------------------------------------------------------                                 --------------------------------     \(9)-------------------------
# MAGIC Naveen Nallamothu        \(9)2021-02-24\(9)US_311874          Original Programming.                                                                  Manasa Andru                         2021-02-26  
# MAGIC Lakshmi Devagiri                     2021-03-29              US364108           Updated output file format to exclude double quotes                    Kalyan Neelam                         2021-03-31
# MAGIC                                                                                                             and Source System Code                    \(9)\(9)\(9)\(9)\(9)

# MAGIC Rejects
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value("InFile", "")
InFile_F = get_widget_value("InFile_F", "")
RejectFile = get_widget_value("RejectFile", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# --------------------------------------------------------------------------------
# Stage: ProvBioMesrRslt_InputFile (PxSequentialFile)
# Read entire row into a single column named RECORD
# --------------------------------------------------------------------------------
schema_ProvBioMesrRslt_InputFile = StructType([
    StructField("RECORD", StringType(), True)
])
df_ProvBioMesrRslt_InputFile = (
    spark.read
    .option("header", True)
    .option("delimiter", "\u0007")  # Delimiter unlikely to appear, forcing a single column
    .schema(schema_ProvBioMesrRslt_InputFile)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_Delimter_Chk (CTransformerStage)
# svDelimiter = Dcount(Ereplace(RECORD, char(14), ''), '|')
# OutputPins:
#   1) Rec_Out (where svDelimiter = 12)
#      RECORD = Ereplace(...), CMN_LKP = 1
#   2) Lnk_Reject (where svDelimiter >1 and svDelimiter <12)
#      RECORD = Ereplace(...), CMN_LKP = 1
# --------------------------------------------------------------------------------
df_xfm_delimiter_chk = (
    df_ProvBioMesrRslt_InputFile
    .withColumn("RECORD_no14", F.regexp_replace(F.col("RECORD"), "\u000E", ""))  # remove char(14)
    .withColumn("svDelimiter", F.size(F.split(F.col("RECORD_no14"), "\\|")))
)

df_Rec_Out = (
    df_xfm_delimiter_chk
    .filter(F.col("svDelimiter") == 12)
    .select(
        F.col("RECORD_no14").alias("RECORD"),
        F.lit(1).alias("CMN_LKP")
    )
)

df_Lnk_Reject = (
    df_xfm_delimiter_chk
    .filter((F.col("svDelimiter") > 1) & (F.col("svDelimiter") < 12))
    .select(
        F.col("RECORD_no14").alias("RECORD"),
        F.lit(1).alias("CMN_LKP")
    )
)

# --------------------------------------------------------------------------------
# Stage: Cp_1 (PxCopy)
# Input: Lnk_Reject
# Outputs:
#   to_RmDups1 => CMN_LKP
#   Out_Cp1 => RECORD
# --------------------------------------------------------------------------------
df_Cp_1_to_RmDups1 = df_Lnk_Reject.select(
    F.col("CMN_LKP").alias("CMN_LKP")
)
df_Cp_1_Out_Cp1 = df_Lnk_Reject.select(
    F.col("RECORD").alias("RECORD")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_1 (CTransformerStage)
# Input: Out_Cp1
# Output: out_Xfm1 => parse RECORD into columns
# --------------------------------------------------------------------------------
df_Xfm_1 = df_Cp_1_Out_Cp1.select(
    F.split(F.col("RECORD"), "\\|")[0].alias("POL_NO"),
    F.split(F.col("RECORD"), "\\|")[1].alias("PATN_LAST_NM"),
    F.split(F.col("RECORD"), "\\|")[2].alias("PATN_FIRST_NM"),
    F.split(F.col("RECORD"), "\\|")[3].alias("PATN_MID_NM"),
    F.split(F.col("RECORD"), "\\|")[4].alias("MBI"),
    F.split(F.col("RECORD"), "\\|")[5].alias("DOB"),
    F.split(F.col("RECORD"), "\\|")[6].alias("GNDR"),
    F.split(F.col("RECORD"), "\\|")[7].alias("DT_OF_SVC"),
    F.split(F.col("RECORD"), "\\|")[8].alias("MTRC"),
    F.split(F.col("RECORD"), "\\|")[9].alias("RSLT_VAL"),
    F.split(F.col("RECORD"), "\\|")[10].alias("UOM"),
    F.split(F.col("RECORD"), "\\|")[13].alias("SOURCE_ID"),
    F.lit("Record doesnt have 14 columns").alias("REJECT_REASON")
)

# --------------------------------------------------------------------------------
# Stage: Rm_Dups_1 (PxRemDup)
# Filter => RetainRecord=first, Key => CMN_LKP
# --------------------------------------------------------------------------------
df_out_RmDups1_intermediate = dedup_sort(
    df_Cp_1_to_RmDups1,
    ["CMN_LKP"],
    [("CMN_LKP", "A")]
)
df_out_RmDups1 = df_out_RmDups1_intermediate.select(
    F.col("CMN_LKP").alias("CMN_LKP")
)

# --------------------------------------------------------------------------------
# Stage: Cp_2 (PxCopy)
# Input: Rec_Out
# Output: to_Lkp1 => RECORD, CMN_LKP
# --------------------------------------------------------------------------------
df_Cp_2_to_Lkp1 = df_Rec_Out.select(
    F.col("RECORD").alias("RECORD"),
    F.col("CMN_LKP").alias("CMN_LKP")
)

# --------------------------------------------------------------------------------
# Stage: Lkp_1 (PxLookup)
# Primary: to_Lkp1, Lookup: out_RmDups1 (left join to match CMN_LKP)
# Outputs:
#   to_xfm2 => RECORD = to_Lkp1.RECORD
#   Out_lkp => RECORD, CMN_LKP = to_Lkp1.RECORD, to_Lkp1.CMN_LKP
# --------------------------------------------------------------------------------
df_Lkp_1_join = df_Cp_2_to_Lkp1.alias("to_Lkp1").join(
    df_out_RmDups1.alias("out_RmDups1"),
    F.col("to_Lkp1.CMN_LKP") == F.col("out_RmDups1.CMN_LKP"),
    "left"
)

df_to_xfm2 = df_Lkp_1_join.select(
    F.col("to_Lkp1.RECORD").alias("RECORD")
)

df_Out_lkp = df_Lkp_1_join.select(
    F.col("to_Lkp1.RECORD").alias("RECORD"),
    F.col("to_Lkp1.CMN_LKP").alias("CMN_LKP")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_2 (CTransformerStage)
# Input: to_xfm2
# Output: out_ColGen1 => parse RECORD into columns with different indexes
# --------------------------------------------------------------------------------
df_Xfm_2 = df_to_xfm2.select(
    F.split(F.col("RECORD"), "\\|")[0].alias("POL_NO"),
    F.split(F.col("RECORD"), "\\|")[1].alias("PATN_LAST_NM"),
    F.split(F.col("RECORD"), "\\|")[2].alias("PATN_FIRST_NM"),
    F.split(F.col("RECORD"), "\\|")[3].alias("PATN_MID_NM"),
    F.split(F.col("RECORD"), "\\|")[4].alias("MBI"),
    F.split(F.col("RECORD"), "\\|")[5].alias("DOB"),
    F.split(F.col("RECORD"), "\\|")[6].alias("GNDR"),
    F.split(F.col("RECORD"), "\\|")[7].alias("DT_OF_SVC"),
    F.split(F.col("RECORD"), "\\|")[8].alias("MTRC"),
    F.split(F.col("RECORD"), "\\|")[10].alias("RSLT_VAL"),
    F.split(F.col("RECORD"), "\\|")[12].alias("UOM"),
    F.split(F.col("RECORD"), "\\|")[13].alias("SOURCE_ID"),
    F.lit(" ").alias("REJECT_REASON")
)

# --------------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2ConnectorPX) (Database = IDS)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_CD_MPPNG = (
    f"SELECT TRIM(SRC_DRVD_LKUP_VAL) AS SRC_DRVD_LKUP_VAL, 1 AS FLG "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM='SOURCE SYSTEM' "
    f"AND SRC_CLCTN_CD='IDS' "
    f"AND SRC_SYS_CD='IDS' "
    f"AND TRGT_DOMAIN_NM='SOURCE SYSTEM' "
    f"AND TRGT_CLCTN_CD='IDS'"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: xfrm1 (CTransformerStage)
# Input: Out_lkp
# Output: BioMesr => remove \r, \n, trim each field part
# --------------------------------------------------------------------------------
df_xfrm1 = df_Out_lkp.select(
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[0]), "\n", ""), "\r", "").alias("POL_NO"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[1]), "\n", ""), "\r", "").alias("PATN_LAST_NM"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[2]), "\n", ""), "\r", "").alias("PATN_FIRST_NM"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[3]), "\n", ""), "\r", "").alias("PATN_MID_NM"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[4]), "\n", ""), "\r", "").alias("MBI"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[5]), "\n", ""), "\r", "").alias("DOB"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[6]), "\n", ""), "\r", "").alias("GNDR"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[7]), "\n", ""), "\r", "").alias("DT_OF_SVC"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[8]), "\n", ""), "\r", "").alias("MTRC"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[9]), "\n", ""), "\r", "").alias("RSLT_VAL"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[10]), "\n", ""), "\r", "").alias("UOM"),
    F.regexp_replace(F.regexp_replace(trim(F.split(F.col("RECORD"), "\\|")[11]), "\n", ""), "\r", "").alias("SOURCE_ID")
)

# --------------------------------------------------------------------------------
# Stage: Cp2 (PxCopy)
# Input: BioMesr
# Output: Columns_Ou => pass columns as-is
# --------------------------------------------------------------------------------
df_Cp2_Columns_Ou = df_xfrm1.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("MTRC").alias("MTRC"),
    F.col("RSLT_VAL").alias("RSLT_VAL"),
    F.col("UOM").alias("UOM"),
    F.col("SOURCE_ID").alias("SOURCE_ID")
)

# --------------------------------------------------------------------------------
# Stage: Lkp_Source_Id (PxLookup)
# Primary: Columns_Ou, Lookup: CD_MPPNG (left join on SOURCE_ID = SRC_DRVD_LKUP_VAL)
# Output: DSLink137 => all columns + FLG
# --------------------------------------------------------------------------------
df_Lkp_Source_Id = df_Cp2_Columns_Ou.alias("Columns_Ou").join(
    df_CD_MPPNG.alias("Lkp_CD_MPPNG"),
    F.col("Columns_Ou.SOURCE_ID") == F.col("Lkp_CD_MPPNG.SRC_DRVD_LKUP_VAL"),
    "left"
)
df_DSLink137 = df_Lkp_Source_Id.select(
    F.col("Columns_Ou.POL_NO").alias("POL_NO"),
    F.col("Columns_Ou.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Columns_Ou.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Columns_Ou.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("Columns_Ou.MBI").alias("MBI"),
    F.col("Columns_Ou.DOB").alias("DOB"),
    F.col("Columns_Ou.GNDR").alias("GNDR"),
    F.col("Columns_Ou.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("Columns_Ou.MTRC").alias("MTRC"),
    F.col("Columns_Ou.RSLT_VAL").alias("RSLT_VAL"),
    F.col("Columns_Ou.UOM").alias("UOM"),
    F.col("Columns_Ou.SOURCE_ID").alias("SOURCE_ID"),
    F.col("Lkp_CD_MPPNG.FLG").alias("FLG")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_3 (CTransformerStage)
# Input: DSLink137
# Outputs:
#   1) NoRejects => FLG=1 => pass columns + LKP=1
#   2) Rejects => FLG=0 => columns => LKP=1
# --------------------------------------------------------------------------------
df_NoRejects = df_DSLink137.filter(F.col("FLG") == 1).select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("MTRC").alias("MTRC"),
    F.col("RSLT_VAL").alias("RSLT_VAL"),
    F.col("UOM").alias("UOM"),
    F.col("SOURCE_ID").alias("SOURCE_ID"),
    F.lit(1).alias("LKP")
)

df_Rejects = df_DSLink137.filter(F.col("FLG") == 0).select(
    F.lit(1).alias("LKP")
)

# --------------------------------------------------------------------------------
# Stage: Rm_Dups_2 (PxRemDup)
# Filter => RetainRecord=first, Key => LKP
# --------------------------------------------------------------------------------
df_Rm_Dups_2_intermediate = dedup_sort(
    df_Rejects,
    ["LKP"],
    [("LKP", "A")]
)
df_to_Cp3 = df_Rm_Dups_2_intermediate.select(
    F.col("LKP").alias("LKP")
)

# --------------------------------------------------------------------------------
# Stage: Cp_3 (PxCopy)
# Input: to_Cp3
# Output: out_Cp3 => LKP
# --------------------------------------------------------------------------------
df_Cp_3_out_Cp3 = df_to_Cp3.select(
    F.col("LKP").alias("LKP")
)

# --------------------------------------------------------------------------------
# Stage: Lkp_2 (PxLookup)
# Primary: NoRejects, Lookup: out_Cp3 (left join => NoRejects.LKP=out_Cp3.LKP)
# Outputs:
#   out_Lkp2 => pass NoRejects columns
#   to_cp5 => pass NoRejects columns (DataStage doesn't specify further, so replicate same columns)
# --------------------------------------------------------------------------------
df_Lkp_2_join = df_NoRejects.alias("NoRejects").join(
    df_Cp_3_out_Cp3.alias("out_Cp3"),
    F.col("NoRejects.LKP") == F.col("out_Cp3.LKP"),
    "left"
)

df_out_Lkp2 = df_Lkp_2_join.select(
    F.col("NoRejects.POL_NO").alias("POL_NO"),
    F.col("NoRejects.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("NoRejects.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("NoRejects.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("NoRejects.MBI").alias("MBI"),
    F.col("NoRejects.DOB").alias("DOB"),
    F.col("NoRejects.GNDR").alias("GNDR"),
    F.col("NoRejects.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("NoRejects.MTRC").alias("MTRC"),
    F.col("NoRejects.RSLT_VAL").alias("RSLT_VAL"),
    F.col("NoRejects.UOM").alias("UOM"),
    F.col("NoRejects.SOURCE_ID").alias("SOURCE_ID")
)

df_to_cp5 = df_Lkp_2_join.select(
    F.col("NoRejects.POL_NO").alias("POL_NO"),
    F.col("NoRejects.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("NoRejects.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("NoRejects.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("NoRejects.MBI").alias("MBI"),
    F.col("NoRejects.DOB").alias("DOB"),
    F.col("NoRejects.GNDR").alias("GNDR"),
    F.col("NoRejects.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("NoRejects.MTRC").alias("MTRC"),
    F.col("NoRejects.RSLT_VAL").alias("RSLT_VAL"),
    F.col("NoRejects.UOM").alias("UOM"),
    F.col("NoRejects.SOURCE_ID").alias("SOURCE_ID")
)

# --------------------------------------------------------------------------------
# Stage: Cp_5 (PxCopy)
# Input: to_cp5
# Output: to_outputfile => columns => same as to_cp5
# --------------------------------------------------------------------------------
df_Cp_5_to_outputfile = df_to_cp5.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("MTRC").alias("MTRC"),
    F.col("RSLT_VAL").alias("RSLT_VAL"),
    F.col("UOM").alias("UOM"),
    F.col("SOURCE_ID").alias("SOURCE_ID")
)

# --------------------------------------------------------------------------------
# Stage: ProvBioMesrRslt (PxSequentialFile)
# Write to #InFile_F# => f"{adls_path}/verified/{InFile_F}"
# no header, delimiter='|'
# --------------------------------------------------------------------------------
# Apply rpad for each column (assuming char/varchar) with an arbitrary length, e.g. 50
df_ProvBioMesrRslt_final = df_Cp_5_to_outputfile.select(
    F.rpad(F.col("POL_NO"), 50, " ").alias("POL_NO"),
    F.rpad(F.col("PATN_LAST_NM"), 50, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), 50, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_MID_NM"), 50, " ").alias("PATN_MID_NM"),
    F.rpad(F.col("MBI"), 50, " ").alias("MBI"),
    F.rpad(F.col("DOB"), 50, " ").alias("DOB"),
    F.rpad(F.col("GNDR"), 50, " ").alias("GNDR"),
    F.rpad(F.col("DT_OF_SVC"), 50, " ").alias("DT_OF_SVC"),
    F.rpad(F.col("MTRC"), 50, " ").alias("MTRC"),
    F.rpad(F.col("RSLT_VAL"), 50, " ").alias("RSLT_VAL"),
    F.rpad(F.col("UOM"), 50, " ").alias("UOM"),
    F.rpad(F.col("SOURCE_ID"), 50, " ").alias("SOURCE_ID")
)
write_files(
    df_ProvBioMesrRslt_final,
    f"{adls_path}/verified/{InFile_F}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Cp_4 (PxCopy)
# Input: out_Lkp2
# Output: Out_Cp4 => pass columns
# --------------------------------------------------------------------------------
df_Cp_4_Out_Cp4 = df_out_Lkp2.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("MTRC").alias("MTRC"),
    F.col("RSLT_VAL").alias("RSLT_VAL"),
    F.col("UOM").alias("UOM"),
    F.col("SOURCE_ID").alias("SOURCE_ID")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_4 (CTransformerStage)
# Input: Out_Cp4
# Output: out_Xfm4 => adds REJECT_REASON='Source Id Match not found in BlueKC'
# --------------------------------------------------------------------------------
df_Xfm_4 = df_Cp_4_Out_Cp4.select(
    F.col("POL_NO").alias("POL_NO"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("MBI").alias("MBI"),
    F.col("DOB").alias("DOB"),
    F.col("GNDR").alias("GNDR"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("MTRC").alias("MTRC"),
    F.col("RSLT_VAL").alias("RSLT_VAL"),
    F.col("UOM").alias("UOM"),
    F.col("SOURCE_ID").alias("SOURCE_ID"),
    F.lit("Source Id Match not found in BlueKC").alias("REJECT_REASON")
)

# --------------------------------------------------------------------------------
# Stage: Fn_1 (PxFunnel)
# Inputs: out_ColGen1 (df_Xfm_2), out_Xfm1 (df_Xfm_1), out_Xfm4 (df_Xfm_4)
# Output: out_Fn1 => union of columns POL_NO..REJECT_REASON
# --------------------------------------------------------------------------------
df_Xfm_2_for_funnel = df_Xfm_2.select(
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
    F.col("REJECT_REASON")
)

df_Xfm_1_for_funnel = df_Xfm_1.select(
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
    F.col("REJECT_REASON")
)

df_Xfm_4_for_funnel = df_Xfm_4.select(
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
    F.col("REJECT_REASON")
)

df_Fn_1_partial = df_Xfm_2_for_funnel.unionByName(df_Xfm_1_for_funnel)
df_Fn_1 = df_Fn_1_partial.unionByName(df_Xfm_4_for_funnel)

# --------------------------------------------------------------------------------
# Stage: ProvBioMesrRslt_RejectFile (PxSequentialFile)
# Write out_Fn1 => #RejectFile# => f"{adls_path}/verified/{RejectFile}"
# Overwrite, header=true, delimiter='|'
# --------------------------------------------------------------------------------
# Apply rpad for each column (assuming char/varchar) with an arbitrary length
df_ProvBioMesrRslt_RejectFile_final = df_Fn_1.select(
    F.rpad(F.col("POL_NO"), 50, " ").alias("POL_NO"),
    F.rpad(F.col("PATN_LAST_NM"), 50, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), 50, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_MID_NM"), 50, " ").alias("PATN_MID_NM"),
    F.rpad(F.col("MBI"), 50, " ").alias("MBI"),
    F.rpad(F.col("DOB"), 50, " ").alias("DOB"),
    F.rpad(F.col("GNDR"), 50, " ").alias("GNDR"),
    F.rpad(F.col("DT_OF_SVC"), 50, " ").alias("DT_OF_SVC"),
    F.rpad(F.col("MTRC"), 50, " ").alias("MTRC"),
    F.rpad(F.col("RSLT_VAL"), 50, " ").alias("RSLT_VAL"),
    F.rpad(F.col("UOM"), 50, " ").alias("UOM"),
    F.rpad(F.col("SOURCE_ID"), 50, " ").alias("SOURCE_ID"),
    F.rpad(F.col("REJECT_REASON"), 50, " ").alias("REJECT_REASON")
)
write_files(
    df_ProvBioMesrRslt_RejectFile_final,
    f"{adls_path}/verified/{RejectFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)