# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2021, 2022, 2023, 2024 -  Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC CommonIdsLabRsltExtrFormatData
# MAGIC Called by: CommonIdsLabRsltSeq
# MAGIC                    
# MAGIC 
# MAGIC Processing: All Lab Pre processing Job. 
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary     
# MAGIC 
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)Project/                                                                                                                             \(9)\(9)\(9)\(9)Develop\(9)\(9)\(9)Code                   \(9)\(9)Date
# MAGIC Developer  \(9)\(9)Date  \(9)\(9)Altiris #\(9)\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Environment\(9)\(9)Reviewer            \(9)\(9)Reviewed
# MAGIC =================================================================================================================================================================================================================
# MAGIC Lakshmi Devagiri    \(9)\(9)2021-02-21\(9)US278143              \(9)Original Programming.\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Kalyan Neelam   \(9)\(9)2021-02-23
# MAGIC Lakshmi Devagiri    \(9)\(9)2021-04-26\(9)US373622              \(9)Updated Member query to Ignore Case on First Name and Last Name   \(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Jaideep Mankala  \(9)\(9)04/27/2021         
# MAGIC Abhishek Pulluri             \(9)2021-06-20                                             \(9)Updated  Upcase to  POL_NO                                                               \(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Manasa Andru     \(9)\(9)2021-06-23        
# MAGIC Ken Bradmon\(9)\(9)2022-08-17\(9)us542423     \(9)Removed the logic to Error a specific record if the LOCAL_RSLT_CD\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Harsha Ravuri\(9)\(9)2022-09-29\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)is NULL.  Removed the logic to Error a specific record if the ACESION_NO
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)value is NULL.  ADDED logic to Error a record if the LOINC_CD is NULL.
# MAGIC Ken Bradmon\(9)\(9)2023-04-26\(9)u542805\(9)\(9)Removed the member matching logic, and the IDS_MBR_UNIQ_KEY stage.  \(9)\(9)\(9)IntegrateDev1\(9)\(9)Harsha Ravuri\(9)\(9)2023-06-14
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)It isn't needed in this job, because we have more sophisticated member matching 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)logic in the CommonIdsLabSecondaryMbrMatch job.  And having that here might cause
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)records to get dropped.
# MAGIC Ken Bradmon\(9)\(9)2024-02-13\(9)us610327\(9)\(9)Added logic to tolerate one new column called "MemberMatchLevel" that was  added to the\(9)IntegrateDev2\(9)\(9)Harsha Ravuri\(9)\(9)2024-02-24
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"member match final" file.  That column isn't used anywhere at all in this job,
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9) but we wanted it in the incoming file, and this job counts the columns
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)in the file.  So that enhancement was necessary to prevent the whole file from being rejected.
# MAGIC Ken Bradmon\(9)\(9)2024-08-06\(9)us625513\(9)\(9)Removed the check of the ORDER_NTNL_PROV_ID column in the "Xfm_Column_Chk" stage\(9)IntegrateDev1\(9)\(9)Harsha Ravuri\(9)\(9)2024-09-05
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9) that caused the record to error out if this value is NULL.  This column now needs to be TRULY
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)nullable, and not one of the reasons that a record gets directed to the ERROR file.  Also removed
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)the reject path of the "Lkp3" stage.  If the lookup fails, we still want that record to get included
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)in the "Format" file.  ALSO, if the lookup fails, set the value of the ORDER_NTNL_PROV_ID 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)to "NA" rather than the "bad" value in the source file.

# MAGIC Rejects
# MAGIC ALL ERROR RECORDS
# MAGIC This part just counts the columns, and if there are not 31 columns, the whole file gets rejected.
# MAGIC These records all get rejected because the file does not have 31 columns.
# MAGIC This stage is where the values of every column are checked for being too long.  If too long, that record gets routed to the Error file.
# MAGIC Set ORDER_NTNL_PROV_ID = \"NA\" if it is NULL because the lookup in the NTNL_PROV table didn't find a record.
# MAGIC The records that go to the ERROR file are sent here because 
# MAGIC 1) the record contains a NULL for a not-nullable column OR
# MAGIC 2) the value for a particular column is too long.
# MAGIC The \"Xfm_Column_chk\" stage is where the values of the columns that should not be NULL are checked.  If those values are NULL, those records get sent to the Error file.
# MAGIC The 31st column in the input file is ignored, because it isn't used anywhere in THIS job.
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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# PARAMETERS
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile = get_widget_value('InFile','')
InFile_F = get_widget_value('InFile_F','')
ErrorFile = get_widget_value('ErrorFile','')
RejectFile = get_widget_value('RejectFile','')

# READ DATABASE TABLES FOR LOOKUPS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# LOINC_CD (Stage: DB2ConnectorPX)
extract_query = f"select DISTINCT LOINC_CD\nFROM {IDSOwner}.LOINC_CD"
df_LOINC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# IDS_NTNL_PROV (Stage: DB2ConnectorPX)
extract_query = f"select DISTINCT NTNL_PROV_ID\nFROM {IDSOwner}.NTNL_PROV"
df_IDS_NTNL_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CD_MPPNG (Stage: DB2ConnectorPX)
extract_query = f"""SELECT TRIM(SRC_DRVD_LKUP_VAL) AS SRC_DRVD_LKUP_VAL,
       1 AS FLG
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM='SOURCE SYSTEM'
AND SRC_CLCTN_CD='IDS'
AND SRC_SYS_CD='IDS'
AND TRGT_DOMAIN_NM='SOURCE SYSTEM'
AND TRGT_CLCTN_CD='IDS'
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# IDS_PROV (Stage: DB2ConnectorPX)
extract_query = f"""
SELECT DISTINCT 
       MIN (PROV.PROV_ID) OVER(PARTITION BY PROV.NTNL_PROV_ID ORDER BY PROV.TERM_DT_SK DESC) PROV_ID,
       FIRST_VALUE(PROV.TERM_DT_SK) OVER(PARTITION BY PROV.NTNL_PROV_ID ORDER BY PROV.TERM_DT_SK DESC) PROV_TERM_DT_SK,
       PROV.NTNL_PROV_ID RNDR_NTNL_PROV_ID
FROM
  {IDSOwner}.PROV PROV
INNER JOIN {IDSOwner}.NTNL_PROV NTNL_PROV ON PROV.NTNL_PROV_SK = NTNL_PROV.NTNL_PROV_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG ON PROV.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
WHERE
  CD_MPPNG.TRGT_CD='FACETS'
  AND CD_MPPNG.TRGT_CD = 'FACETS'
  AND PROV.NTNL_PROV_ID<>'NA'
"""
df_IDS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# READ Lab_InputFile (PxSequentialFile) with single column RECORD.
# ContainsHeader=True; define a schema with one column "RECORD".
schema_Lab_InputFile = T.StructType([
    T.StructField("RECORD", T.StringType(), True)
])
df_Lab_InputFile = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "\u0001")  # Use a delimiter unlikely to appear, ensuring entire line is placed in RECORD
    .schema(schema_Lab_InputFile)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# -------------- Xfm_Delimter_Chk (Transformer) --------------
# Stage variable: svDelimiter = Dcount(Ereplace(RECORD, char(30), ''), '|') => number of fields after removing ASCII 30
# Constraint 1: svDelimiter = 31 => Rec_Out
# Constraint 2: svDelimiter <> 31 => Lnk_Reject
df_Xfm_Delimter_Chk_stagevar = df_Lab_InputFile.withColumn(
    "svDelimiter",
    F.size(F.split(F.regexp_replace(F.col("RECORD"), u"\u001E", ""), "\|"))
)

# Rec_Out
df_Rec_Out = (
    df_Xfm_Delimter_Chk_stagevar
    .filter(F.col("svDelimiter") == 31)
    .select(
        F.regexp_replace(F.col("RECORD"), "\r", "").alias("RECORD"),
        F.lit("1").alias("CMN_LKP")
    )
)

# Lnk_Reject
df_Lnk_Reject = (
    df_Xfm_Delimter_Chk_stagevar
    .filter(F.col("svDelimiter") != 31)
    .select(
        F.regexp_replace(F.col("RECORD"), "\r", "").alias("RECORD"),
        F.lit("1").alias("CMN_LKP")
    )
)

# -------------- Cp_1 (PxCopy) --------------
# Input: df_Lnk_Reject
# Outputs:
#   to_RmDups1 => CMN_LKP
#   Out_Cp1 => RECORD
df_Cp_1_to_RmDups1 = df_Lnk_Reject.select(F.col("CMN_LKP"))
df_Cp_1_Out_Cp1 = df_Lnk_Reject.select(F.col("RECORD"))

# -------------- Xfm_1 (Transformer) --------------
# Input: Out_Cp1 => df_Cp_1_Out_Cp1
# Output => out_Xfm1 => parse RECORD into fields and extra REJECT_REASON
df_Xfm_1 = (
    df_Cp_1_Out_Cp1.select(
        F.expr("split(RECORD, '\\|')[0]").alias("RSLT_DT"),
        F.expr("split(RECORD, '\\|')[1]").alias("ACESION_NO"),
        F.expr("split(RECORD, '\\|')[2]").alias("LAB_CD"),
        F.expr("split(RECORD, '\\|')[3]").alias("PATN_LAST_NM"),
        F.expr("split(RECORD, '\\|')[4]").alias("PATN_FIRST_NM"),
        F.expr("split(RECORD, '\\|')[5]").alias("PATN_MID_NM"),
        F.expr("split(RECORD, '\\|')[6]").alias("DOB"),
        F.expr("split(RECORD, '\\|')[7]").alias("PATN_AGE"),
        F.expr("split(RECORD, '\\|')[8]").alias("GNDR"),
        F.expr("split(RECORD, '\\|')[9]").alias("VNDR_BILL_ID"),
        F.expr("split(RECORD, '\\|')[10]").alias("POL_NO"),
        F.expr("split(RECORD, '\\|')[11]").alias("ORDER_NTNL_PROV_ID"),
        F.expr("split(RECORD, '\\|')[12]").alias("ORDER_ACCT_NM"),
        F.expr("split(RECORD, '\\|')[13]").alias("RNDR_NTNL_PROV_ID"),
        F.expr("split(RECORD, '\\|')[14]").alias("ICD_VRSN"),
        F.expr("split(RECORD, '\\|')[15]").alias("DIAG_CD"),
        F.expr("split(RECORD, '\\|')[16]").alias("DIAG_CD_2"),
        F.expr("split(RECORD, '\\|')[17]").alias("DIAG_CD_3"),
        F.expr("split(RECORD, '\\|')[18]").alias("ORDER_NM"),
        F.expr("split(RECORD, '\\|')[19]").alias("LOCAL_ORDER_CD"),
        F.expr("split(RECORD, '\\|')[20]").alias("LOINC_CD"),
        F.expr("split(RECORD, '\\|')[21]").alias("RSLT_NM"),
        F.expr("split(RECORD, '\\|')[22]").alias("LOCAL_RSLT_CD"),
        F.expr("split(RECORD, '\\|')[23]").alias("RSLT_VAL_NUM"),
        F.expr("split(RECORD, '\\|')[24]").alias("RSLT_VAL_TEXT"),
        F.expr("split(RECORD, '\\|')[25]").alias("RSLT_UNIT"),
        F.expr("split(RECORD, '\\|')[26]").alias("ABNORM_FLAG"),
        F.expr("split(RECORD, '\\|')[27]").alias("MRN_ID"),
        F.expr("split(RECORD, '\\|')[28]").alias("SOURCE_ID"),
        F.expr("split(RECORD, '\\|')[29]").alias("MBR_UNIQ_KEY"),
        F.lit("Record doesnt have 31 columns").alias("REJECT_REASON")
    )
)

# -------------- Cp_2 (PxCopy) --------------
# Input: df_Rec_Out
# Output => to_Lkp1 => (RECORD, CMN_LKP)
df_Cp_2_to_Lkp1 = df_Rec_Out.select(
    F.col("RECORD"),
    F.col("CMN_LKP")
)

# -------------- Rm_Dups_1 (PxRemDup) --------------
# Input: df_Cp_1_to_RmDups1 (CMN_LKP)
# Filter => Retain first, key=CMN_LKP
df_Rm_Dups_1 = dedup_sort(
    df_Cp_1_to_RmDups1,
    ["CMN_LKP"],
    []
)

# -------------- Lkp_1 (PxLookup) --------------
# Primary link = df_Cp_2_to_Lkp1 (alias p)
# Lookup link = df_Rm_Dups_1 (alias l), joinType=left, p.CMN_LKP = l.CMN_LKP
# Output pins:
#   to_Xfm2 => RECORD
#   Out_Lkp1 => RECORD, CMN_LKP
df_Lkp_1_join = p = df_Cp_2_to_Lkp1.alias("p").join(
    df_Rm_Dups_1.alias("l"),
    F.col("p.CMN_LKP") == F.col("l.CMN_LKP"),
    "left"
)

df_Lkp_1_to_Xfm2 = df_Lkp_1_join.select(
    F.col("p.RECORD").alias("RECORD")
)
df_Lkp_1_Out_Lkp1 = df_Lkp_1_join.select(
    F.col("p.RECORD").alias("RECORD"),
    F.col("p.CMN_LKP").alias("CMN_LKP")
)

# -------------- Xfm_2 (Transformer) --------------
# Input => df_Lkp_1_to_Xfm2
# parse RECORD similarly, produce out_ColGen1
df_Xfm_2 = (
    df_Lkp_1_to_Xfm2.select(
        F.expr("split(RECORD, '\\|')[0]").alias("RSLT_DT"),
        F.expr("split(RECORD, '\\|')[1]").alias("ACESION_NO"),
        F.expr("split(RECORD, '\\|')[2]").alias("LAB_CD"),
        F.expr("split(RECORD, '\\|')[3]").alias("PATN_LAST_NM"),
        F.expr("split(RECORD, '\\|')[4]").alias("PATN_FIRST_NM"),
        F.expr("split(RECORD, '\\|')[5]").alias("PATN_MID_NM"),
        F.expr("split(RECORD, '\\|')[6]").alias("DOB"),
        F.expr("split(RECORD, '\\|')[7]").alias("PATN_AGE"),
        F.expr("split(RECORD, '\\|')[8]").alias("GNDR"),
        F.expr("split(RECORD, '\\|')[9]").alias("VNDR_BILL_ID"),
        F.expr("split(RECORD, '\\|')[10]").alias("POL_NO"),
        F.expr("split(RECORD, '\\|')[11]").alias("ORDER_NTNL_PROV_ID"),
        F.expr("split(RECORD, '\\|')[12]").alias("ORDER_ACCT_NM"),
        F.expr("split(RECORD, '\\|')[13]").alias("RNDR_NTNL_PROV_ID"),
        F.expr("split(RECORD, '\\|')[14]").alias("ICD_VRSN"),
        F.expr("split(RECORD, '\\|')[15]").alias("DIAG_CD"),
        F.expr("split(RECORD, '\\|')[16]").alias("DIAG_CD_2"),
        F.expr("split(RECORD, '\\|')[17]").alias("DIAG_CD_3"),
        F.expr("split(RECORD, '\\|')[18]").alias("ORDER_NM"),
        F.expr("split(RECORD, '\\|')[19]").alias("LOCAL_ORDER_CD"),
        F.expr("split(RECORD, '\\|')[20]").alias("LOINC_CD"),
        F.expr("split(RECORD, '\\|')[21]").alias("RSLT_NM"),
        F.expr("split(RECORD, '\\|')[22]").alias("LOCAL_RSLT_CD"),
        F.expr("split(RECORD, '\\|')[23]").alias("RSLT_VAL_NUM"),
        F.expr("split(RECORD, '\\|')[24]").alias("RSLT_VAL_TEXT"),
        F.expr("split(RECORD, '\\|')[25]").alias("RSLT_UNIT"),
        F.expr("split(RECORD, '\\|')[26]").alias("ABNORM_FLAG"),
        F.expr("split(RECORD, '\\|')[27]").alias("MRN_ID"),
        F.expr("split(RECORD, '\\|')[28]").alias("SOURCE_ID"),
        F.expr("split(RECORD, '\\|')[29]").alias("MBR_UNIQ_KEY"),
        F.lit("").alias("REJECT_REASON")
    )
)

# -------------- Xfm_5 (Transformer) --------------
# Input => df_Lkp_1_Out_Lkp1
# This does many stage vars that check lengths of fields against a max length to see if 'Column XYZ doesnt match...' 
# Then a single constraint: if all checks are '' => T else F => "svRejectFlg"
# T => Columns_Ou
# F => Out_Xfm5
# We'll replicate the logic in a single pass.

df_Xfm_5_stagevar = df_Lkp_1_Out_Lkp1.withColumn("svRsltDtLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[0]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[0]")) > 8),
        F.lit("Column RSLT_DT doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svAcesLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[1]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[1]")) > 16),
        F.lit("Column ACESION_NO doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svLabCdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[2]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[2]")) > 4),
        F.lit("Column LAB_CD doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svLastNmLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[3]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[3]")) > 50),
        F.lit("Column PATN_LAST_NM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svFirstNmLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[4]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[4]")) > 50),
        F.lit("Column PATN_FIRST_NM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svMidNmLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[5]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[5]")) > 50),
        F.lit("Column PATN_MID_NM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svDobLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[6]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[6]")) > 8),
        F.lit("Column DOB doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svAgeLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[7]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[7]")) > 3),
        F.lit("Column PATN_AGE doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svGndrLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[8]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[8]")) > 1),
        F.lit("Column GNDR doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svBillIndLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[9]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[9]")) > 10),
        F.lit("Column VNDR_BILL_ID doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svPolnumLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[10]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[10]")) > 20),
        F.lit("Column POL_NO doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svOrdrNtnlProvIdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[11]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[11]")) > 20),
        F.lit("Column ORDER_NTNL_PROV_ID doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svOrdrAcctNmLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[12]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[12]")) > 30),
        F.lit("Column ORDER_ACCT_NM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svRndrNtnlProvIdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[13]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[13]")) > 10),
        F.lit("Column RNDR_NTNL_PROV_ID doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svIcdVrsnLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[14]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[14]")) > 2),
        F.lit("Column ICD_VRSN doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svDiagCdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[15]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[15]")) > 18),
        F.lit("Column DIAG_CD doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svDiagCd2Len",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[16]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[16]")) > 18),
        F.lit("Column DIAG_CD_2 doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svDiagCd3Len",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[17]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[17]")) > 18),
        F.lit("Column DIAG_CD_3 doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svOrdrNmLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[18]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[18]")) > 75),
        F.lit("Column ORDER_NM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svLocalOrdrCdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[19]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[19]")) > 10),
        F.lit("Column LOCAL_ORDER_CD doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svLoincCdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[20]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[20]")) > 10),
        F.lit("Column LOINC_CD doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svRsltNmLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[21]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[21]")) > 30),
        F.lit("Column RSLT_NM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svLocalRsltCdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[22]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[22]")) > 10),
        F.lit("Column LOCAL_RSLT_CD doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svRsltValNumLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[23]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[23]")) > 13),
        F.lit("Column RSLT_VAL_NUM doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svRsltValTextLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[24]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[24]")) > 18),
        F.lit("Column RSLT_VAL_TEXT doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svRsltUnitLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[25]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[25]")) > 70),
        F.lit("Column RSLT_UNIT doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svAbnormFlgLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[26]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[26]")) > 2),
        F.lit("Column ABNORM_FLAG doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svMrnIdLen",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[27]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[27]")) > 15),
        F.lit("Column MRN_ID doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
).withColumn("svSrcId",
    F.when(
        (F.trim(F.expr("split(RECORD,'\\|')[28]")) != "") &
        (F.length(F.expr("split(RECORD,'\\|')[28]")) > 50),
        F.lit("Column SOURCE_ID doesnt match with BLUEKC LAB Format;")
    ).otherwise(F.lit(""))
)

# gluing them all to see if all blank => 'T' else 'F'
cols_check = [
    "svRsltDtLen","svAcesLen","svLabCdLen","svLastNmLen","svFirstNmLen","svMidNmLen","svDobLen","svAgeLen","svGndrLen",
    "svBillIndLen","svPolnumLen","svOrdrNtnlProvIdLen","svOrdrAcctNmLen","svRndrNtnlProvIdLen","svIcdVrsnLen",
    "svDiagCdLen","svDiagCd2Len","svDiagCd3Len","svOrdrNmLen","svLocalOrdrCdLen","svLoincCdLen","svRsltNmLen",
    "svLocalRsltCdLen","svRsltValNumLen","svRsltValTextLen","svRsltUnitLen","svAbnormFlgLen","svMrnIdLen","svSrcId"
]
all_blank = F.lit("T")
for c in cols_check:
    all_blank = all_blank.when(F.col(c) != "", "F").otherwise("T")

df_Xfm_5_stagevar = df_Xfm_5_stagevar.withColumn("svRejectFlg", all_blank)

# build the reject message
df_Xfm_5_stagevar = df_Xfm_5_stagevar.withColumn(
    "svRejectMesg",
    F.concat_ws("", *cols_check)
)

# Constraint: svRejectFlg = 'T' => Columns_Ou
df_Xfm_5_Columns_Ou = df_Xfm_5_stagevar.filter(F.col("svRejectFlg") == "T").select(
    F.expr("split(RECORD,'\\|')[0]").alias("RSLT_DT"),
    F.expr("split(RECORD,'\\|')[1]").alias("ACESION_NO"),
    F.expr("split(RECORD,'\\|')[2]").alias("LAB_CD"),
    F.expr("split(RECORD,'\\|')[3]").alias("PATN_LAST_NM"),
    F.expr("split(RECORD,'\\|')[4]").alias("PATN_FIRST_NM"),
    F.expr("split(RECORD,'\\|')[5]").alias("PATN_MID_NM"),
    F.expr("split(RECORD,'\\|')[6]").alias("DOB"),
    F.expr("split(RECORD,'\\|')[7]").alias("PATN_AGE"),
    F.expr("split(RECORD,'\\|')[8]").alias("GNDR"),
    F.expr("split(RECORD,'\\|')[9]").alias("VNDR_BILL_ID"),
    F.expr("split(RECORD,'\\|')[10]").alias("POL_NO"),
    F.expr("split(RECORD,'\\|')[11]").alias("ORDER_NTNL_PROV_ID"),
    F.expr("split(RECORD,'\\|')[12]").alias("ORDER_ACCT_NM"),
    F.expr("split(RECORD,'\\|')[13]").alias("RNDR_NTNL_PROV_ID"),
    F.expr("split(RECORD,'\\|')[14]").alias("ICD_VRSN"),
    F.expr("split(RECORD,'\\|')[15]").alias("DIAG_CD"),
    F.expr("split(RECORD,'\\|')[16]").alias("DIAG_CD_2"),
    F.expr("split(RECORD,'\\|')[17]").alias("DIAG_CD_3"),
    F.expr("split(RECORD,'\\|')[18]").alias("ORDER_NM"),
    F.expr("split(RECORD,'\\|')[19]").alias("LOCAL_ORDER_CD"),
    F.expr("split(RECORD,'\\|')[20]").alias("LOINC_CD"),
    F.expr("split(RECORD,'\\|')[21]").alias("RSLT_NM"),
    F.expr("split(RECORD,'\\|')[22]").alias("LOCAL_RSLT_CD"),
    F.expr("split(RECORD,'\\|')[23]").alias("RSLT_VAL_NUM"),
    F.expr("split(RECORD,'\\|')[24]").alias("RSLT_VAL_TEXT"),
    F.expr("split(RECORD,'\\|')[25]").alias("RSLT_UNIT"),
    F.expr("split(RECORD,'\\|')[26]").alias("ABNORM_FLAG"),
    F.expr("split(RECORD,'\\|')[27]").alias("MRN_ID"),
    F.expr("split(RECORD,'\\|')[28]").alias("SOURCE_ID"),
    F.expr("split(RECORD,'\\|')[29]").alias("MBR_UNIQ_KEY")
)

# Constraint: svRejectFlg = 'F' => Out_Xfm5
df_Xfm_5_Out_Xfm5 = df_Xfm_5_stagevar.filter(F.col("svRejectFlg") == "F").select(
    F.expr("split(RECORD,'\\|')[0]").alias("RSLT_DT"),
    F.expr("split(RECORD,'\\|')[1]").alias("ACESION_NO"),
    F.expr("split(RECORD,'\\|')[2]").alias("LAB_CD"),
    F.expr("split(RECORD,'\\|')[3]").alias("PATN_LAST_NM"),
    F.expr("split(RECORD,'\\|')[4]").alias("PATN_FIRST_NM"),
    F.expr("split(RECORD,'\\|')[5]").alias("PATN_MID_NM"),
    F.expr("split(RECORD,'\\|')[6]").alias("DOB"),
    F.expr("split(RECORD,'\\|')[7]").alias("PATN_AGE"),
    F.expr("split(RECORD,'\\|')[8]").alias("GNDR"),
    F.expr("split(RECORD,'\\|')[9]").alias("VNDR_BILL_ID"),
    F.expr("split(RECORD,'\\|')[10]").alias("POL_NO"),
    F.expr("split(RECORD,'\\|')[11]").alias("ORDER_NTNL_PROV_ID"),
    F.expr("split(RECORD,'\\|')[12]").alias("ORDER_ACCT_NM"),
    F.expr("split(RECORD,'\\|')[13]").alias("RNDR_NTNL_PROV_ID"),
    F.expr("split(RECORD,'\\|')[14]").alias("ICD_VRSN"),
    F.expr("split(RECORD,'\\|')[15]").alias("DIAG_CD"),
    F.expr("split(RECORD,'\\|')[16]").alias("DIAG_CD_2"),
    F.expr("split(RECORD,'\\|')[17]").alias("DIAG_CD_3"),
    F.expr("split(RECORD,'\\|')[18]").alias("ORDER_NM"),
    F.expr("split(RECORD,'\\|')[19]").alias("LOCAL_ORDER_CD"),
    F.expr("split(RECORD,'\\|')[20]").alias("LOINC_CD"),
    F.expr("split(RECORD,'\\|')[21]").alias("RSLT_NM"),
    F.expr("split(RECORD,'\\|')[22]").alias("LOCAL_RSLT_CD"),
    F.expr("split(RECORD,'\\|')[23]").alias("RSLT_VAL_NUM"),
    F.expr("split(RECORD,'\\|')[24]").alias("RSLT_VAL_TEXT"),
    F.expr("split(RECORD,'\\|')[25]").alias("RSLT_UNIT"),
    F.expr("split(RECORD,'\\|')[26]").alias("ABNORM_FLAG"),
    F.expr("split(RECORD,'\\|')[27]").alias("MRN_ID"),
    F.expr("split(RECORD,'\\|')[28]").alias("SOURCE_ID"),
    F.expr("split(RECORD,'\\|')[29]").alias("MBR_UNIQ_KEY"),
    F.trim(F.col("svRejectMesg"),).alias("ERROR_REASON")
)

# -------------- Lkp_Source_Id (PxLookup) --------------
# Primary link => df_Xfm_5_Columns_Ou => rename => "Columns_Ou"
# Lookup link => df_CD_MPPNG => left join => Columns_Ou.SOURCE_ID = CD_MPPNG.SRC_DRVD_LKUP_VAL
df_Lkp_Source_Id_join = df_Xfm_5_Columns_Ou.alias("Columns_Ou").join(
    df_CD_MPPNG.alias("Lkp_CD_MPPNG"),
    F.col("Columns_Ou.SOURCE_ID") == F.col("Lkp_CD_MPPNG.SRC_DRVD_LKUP_VAL"),
    "left"
)
df_Lkp_Source_Id_to_Xfm3 = df_Lkp_Source_Id_join.select(
    F.col("Columns_Ou.RSLT_DT"),
    F.col("Columns_Ou.ACESION_NO"),
    F.col("Columns_Ou.LAB_CD"),
    F.col("Columns_Ou.PATN_LAST_NM"),
    F.col("Columns_Ou.PATN_FIRST_NM"),
    F.col("Columns_Ou.PATN_MID_NM"),
    F.col("Columns_Ou.DOB"),
    F.col("Columns_Ou.PATN_AGE"),
    F.col("Columns_Ou.GNDR"),
    F.col("Columns_Ou.VNDR_BILL_ID"),
    F.col("Columns_Ou.POL_NO"),
    F.col("Columns_Ou.ORDER_NTNL_PROV_ID"),
    F.col("Columns_Ou.ORDER_ACCT_NM"),
    F.col("Columns_Ou.RNDR_NTNL_PROV_ID"),
    F.col("Columns_Ou.ICD_VRSN"),
    F.col("Columns_Ou.DIAG_CD"),
    F.col("Columns_Ou.DIAG_CD_2"),
    F.col("Columns_Ou.DIAG_CD_3"),
    F.col("Columns_Ou.ORDER_NM"),
    F.col("Columns_Ou.LOCAL_ORDER_CD"),
    F.col("Columns_Ou.LOINC_CD"),
    F.col("Columns_Ou.RSLT_NM"),
    F.col("Columns_Ou.LOCAL_RSLT_CD"),
    F.col("Columns_Ou.RSLT_VAL_NUM"),
    F.col("Columns_Ou.RSLT_VAL_TEXT"),
    F.col("Columns_Ou.RSLT_UNIT"),
    F.col("Columns_Ou.ABNORM_FLAG"),
    F.col("Columns_Ou.MRN_ID"),
    F.col("Columns_Ou.SOURCE_ID"),
    F.col("Columns_Ou.MBR_UNIQ_KEY"),
    F.col("Lkp_CD_MPPNG.FLG").alias("FLG")
)

# -------------- Xfm_3 (Transformer) --------------
# If FLG=0 => Reject => LKP=1 => to Rm_Dups3
# If FLG=1 => NoRejects => pass to Lkp with LKP=1
df_Xfm_3_Reject = (
    df_Lkp_Source_Id_to_Xfm3.filter(F.col("FLG") == 0)
    .select(F.lit("1").alias("LKP"))
)
df_Xfm_3_NoRejects = (
    df_Lkp_Source_Id_to_Xfm3.filter(F.col("FLG") == 1)
    .select(
        F.col("RSLT_DT"),
        F.col("ACESION_NO"),
        F.col("LAB_CD"),
        F.col("PATN_LAST_NM"),
        F.col("PATN_FIRST_NM"),
        F.col("PATN_MID_NM"),
        F.col("DOB"),
        F.col("PATN_AGE"),
        F.col("GNDR"),
        F.col("VNDR_BILL_ID"),
        F.col("POL_NO"),
        F.col("ORDER_NTNL_PROV_ID"),
        F.col("ORDER_ACCT_NM"),
        F.col("RNDR_NTNL_PROV_ID"),
        F.col("ICD_VRSN"),
        F.col("DIAG_CD"),
        F.col("DIAG_CD_2"),
        F.col("DIAG_CD_3"),
        F.col("ORDER_NM"),
        F.col("LOCAL_ORDER_CD"),
        F.col("LOINC_CD"),
        F.col("RSLT_NM"),
        F.col("LOCAL_RSLT_CD"),
        F.col("RSLT_VAL_NUM"),
        F.col("RSLT_VAL_TEXT"),
        F.col("RSLT_UNIT"),
        F.col("ABNORM_FLAG"),
        F.col("MRN_ID"),
        F.col("SOURCE_ID"),
        F.col("MBR_UNIQ_KEY"),
        F.lit("1").alias("LKP")
    )
)

# -------------- Rm_Dups3 (PxRemDup) --------------
# Input => Xfm_3_Reject => dedup by LKP
df_Rm_Dups3 = dedup_sort(df_Xfm_3_Reject, ["LKP"], [])

# -------------- Lkp (PxLookup) --------------
# Primary link => df_Xfm_3_NoRejects => alias NoRejects
# Lookup link => df_Rm_Dups3 => alias to_Lkp => left join => NoRejects.LKP=to_Lkp.LKP
df_Lkp_join = df_Xfm_3_NoRejects.alias("NoRejects").join(
    df_Rm_Dups3.alias("to_Lkp"),
    (F.col("NoRejects.LKP") == F.col("to_Lkp.LKP")),
    "left"
)

df_Lkp_to_Xfm4 = df_Lkp_join.select(
    F.col("NoRejects.RSLT_DT"),
    F.col("NoRejects.ACESION_NO"),
    F.col("NoRejects.LAB_CD"),
    F.col("NoRejects.PATN_LAST_NM"),
    F.col("NoRejects.PATN_FIRST_NM"),
    F.col("NoRejects.PATN_MID_NM"),
    F.col("NoRejects.DOB"),
    F.col("NoRejects.PATN_AGE"),
    F.col("NoRejects.GNDR"),
    F.col("NoRejects.VNDR_BILL_ID"),
    F.col("NoRejects.POL_NO"),
    F.col("NoRejects.ORDER_NTNL_PROV_ID"),
    F.col("NoRejects.ORDER_ACCT_NM"),
    F.col("NoRejects.RNDR_NTNL_PROV_ID"),
    F.col("NoRejects.ICD_VRSN"),
    F.col("NoRejects.DIAG_CD"),
    F.col("NoRejects.DIAG_CD_2"),
    F.col("NoRejects.DIAG_CD_3"),
    F.col("NoRejects.ORDER_NM"),
    F.col("NoRejects.LOCAL_ORDER_CD"),
    F.col("NoRejects.LOINC_CD"),
    F.col("NoRejects.RSLT_NM"),
    F.col("NoRejects.LOCAL_RSLT_CD"),
    F.col("NoRejects.RSLT_VAL_NUM"),
    F.col("NoRejects.RSLT_VAL_TEXT"),
    F.col("NoRejects.RSLT_UNIT"),
    F.col("NoRejects.ABNORM_FLAG"),
    F.col("NoRejects.MRN_ID"),
    F.col("NoRejects.SOURCE_ID"),
    F.col("NoRejects.MBR_UNIQ_KEY")
)

df_Lkp_main = df_Lkp_join.select(
    F.col("NoRejects.RSLT_DT"),
    F.col("NoRejects.ACESION_NO"),
    F.col("NoRejects.LAB_CD"),
    F.col("NoRejects.PATN_LAST_NM"),
    F.col("NoRejects.PATN_FIRST_NM"),
    F.col("NoRejects.PATN_MID_NM"),
    F.col("NoRejects.DOB"),
    F.col("NoRejects.PATN_AGE"),
    F.col("NoRejects.GNDR"),
    F.col("NoRejects.VNDR_BILL_ID"),
    F.col("NoRejects.POL_NO"),
    F.col("NoRejects.ORDER_NTNL_PROV_ID"),
    F.col("NoRejects.ORDER_ACCT_NM"),
    F.col("NoRejects.RNDR_NTNL_PROV_ID"),
    F.col("NoRejects.ICD_VRSN"),
    F.col("NoRejects.DIAG_CD"),
    F.col("NoRejects.DIAG_CD_2"),
    F.col("NoRejects.DIAG_CD_3"),
    F.col("NoRejects.ORDER_NM"),
    F.col("NoRejects.LOCAL_ORDER_CD"),
    F.col("NoRejects.LOINC_CD"),
    F.col("NoRejects.RSLT_NM"),
    F.col("NoRejects.LOCAL_RSLT_CD"),
    F.col("NoRejects.RSLT_VAL_NUM"),
    F.col("NoRejects.RSLT_VAL_TEXT"),
    F.col("NoRejects.RSLT_UNIT"),
    F.col("NoRejects.ABNORM_FLAG"),
    F.col("NoRejects.MRN_ID"),
    F.col("NoRejects.SOURCE_ID"),
    F.col("NoRejects.MBR_UNIQ_KEY"),
    F.lit(1).alias("LKP")
)

# -------------- Xfm_Column_Chk (Transformer) --------------
# Input => df_Lkp_main
# We do stage variables checking if certain columns are null => if any are missing => reject=0, else pass=1
df_Xfm_Column_Chk_stagevar = df_Lkp_main.withColumn("svPolno",
    F.when((F.col("POL_NO").isNull()) | (F.trim(F.col("POL_NO")) == ""), "POL_NO Not Found;").otherwise("")
).withColumn("svPatnlastNm",
    F.when((F.col("PATN_LAST_NM").isNull()) | (F.trim(F.col("PATN_LAST_NM")) == ""), "PATN_LAST_NM Not Found;").otherwise("")
).withColumn("svPatnFrstNm",
    F.when((F.col("PATN_FIRST_NM").isNull()) | (F.trim(F.col("PATN_FIRST_NM")) == ""), "PATN_FIRST_NM Not Found;").otherwise("")
).withColumn("svDOB",
    F.when((F.col("DOB").isNull()) | (F.trim(F.col("DOB")) == ""), "DOB Not Found;").otherwise("")
).withColumn("svRsltDt",
    F.when((F.col("RSLT_DT").isNull()) | (F.trim(F.col("RSLT_DT")) == ""), "RESULT DATE(RSLT_DT) Not Found;").otherwise("")
).withColumn("svLoincCD",
    F.when((F.col("LOINC_CD").isNull()) | (F.trim(F.col("LOINC_CD")) == ""), "LOINC_CD Not Found;").otherwise("")
).withColumn("svRsltVal",
    F.when(
        ((F.col("RSLT_VAL_NUM").isNull()) | (F.trim(F.col("RSLT_VAL_NUM")) == "")) &
        ((F.col("RSLT_VAL_TEXT").isNull()) | (F.trim(F.col("RSLT_VAL_TEXT")) == "")),
        "RSLT_VAL_NUM/RSLT_VAL_TEXT Not Found;"
    ).otherwise("")
).withColumn("svOrdrNm",
    F.when((F.col("ORDER_NM").isNull()) | (F.trim(F.col("ORDER_NM")) == ""), "ORDER_NM Not Found;").otherwise("")
).withColumn("svRsltNm",
    F.when((F.col("RSLT_NM").isNull()) | (F.trim(F.col("RSLT_NM")) == ""), "RSLT_NM Not Found;").otherwise("")
)

cols_valchk = ["svPolno","svPatnlastNm","svPatnFrstNm","svDOB","svRsltDt","svLoincCD","svRsltVal","svOrdrNm","svRsltNm"]
all_ok = F.lit(1)
for cv in cols_valchk:
    all_ok = all_ok.when(F.col(cv) != "", 0).otherwise(1)

df_Xfm_Column_Chk_stagevar = df_Xfm_Column_Chk_stagevar.withColumn("svRejectFlg", all_ok)

df_Xfm_Column_Chk_stagevar = df_Xfm_Column_Chk_stagevar.withColumn(
    "svRejectMesg",
    F.concat_ws("", *cols_valchk)
)

# Constraint => svRejectFlg=1 => Lnk_Valid
df_Xfm_Column_Chk_Lnk_Valid = (
    df_Xfm_Column_Chk_stagevar
    .filter(F.col("svRejectFlg") == 1)
    .select(
        F.col("RSLT_DT"),
        F.trim(F.col("ACESION_NO")).alias("ACESION_NO"),
        F.trim(F.col("LAB_CD")).alias("LAB_CD"),
        F.trim(F.col("PATN_LAST_NM")).alias("PATN_LAST_NM"),
        F.trim(F.col("PATN_FIRST_NM")).alias("PATN_FIRST_NM"),
        F.trim(F.col("PATN_MID_NM")).alias("PATN_MID_NM"),
        F.when(F.col("svDOB") != "", F.lit("1753-01-01"))
         .otherwise(F.expr("to_date(DOB, 'yyyyMMdd')")).alias("DOB"),
        F.trim(F.col("PATN_AGE")).alias("PATN_AGE"),
        F.trim(F.col("GNDR")).alias("GNDR"),
        F.trim(F.col("VNDR_BILL_ID")).alias("VNDR_BILL_ID"),
        F.upper(F.trim(F.col("POL_NO"))).alias("POL_NO"),
        F.trim(F.col("ORDER_NTNL_PROV_ID")).alias("ORDER_NTNL_PROV_ID"),
        F.trim(F.col("ORDER_ACCT_NM")).alias("ORDER_ACCT_NM"),
        F.trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
        F.trim(F.col("ICD_VRSN")).alias("ICD_VRSN"),
        F.trim(F.col("DIAG_CD")).alias("DIAG_CD"),
        F.trim(F.col("DIAG_CD_2")).alias("DIAG_CD_2"),
        F.trim(F.col("DIAG_CD_3")).alias("DIAG_CD_3"),
        F.trim(F.col("ORDER_NM")).alias("ORDER_NM"),
        F.trim(F.col("LOCAL_ORDER_CD")).alias("LOCAL_ORDER_CD"),
        F.trim(F.col("LOINC_CD")).alias("LOINC_CD"),
        F.trim(F.col("RSLT_NM")).alias("RSLT_NM"),
        F.trim(F.col("LOCAL_RSLT_CD")).alias("LOCAL_RSLT_CD"),
        F.trim(F.col("RSLT_VAL_NUM")).alias("RSLT_VAL_NUM"),
        F.trim(F.col("RSLT_VAL_TEXT")).alias("RSLT_VAL_TEXT"),
        F.trim(F.col("RSLT_UNIT")).alias("RSLT_UNIT"),
        F.trim(F.col("ABNORM_FLAG")).alias("ABNORM_FLAG"),
        F.trim(F.col("MRN_ID")).alias("MRN_ID"),
        F.trim(F.col("SOURCE_ID")).alias("SOURCE_ID"),
        F.col("MBR_UNIQ_KEY"),
        F.upper(F.trim(F.col("PATN_LAST_NM"))).alias("UPPER_PATN_LAST_NM"),
        F.upper(F.trim(F.col("PATN_FIRST_NM"))).alias("UPPER_PATN_FIRST_NM"),
    )
)

# Constraint => svRejectFlg=0 => Lnk_Error
df_Xfm_Column_Chk_Lnk_Error = (
    df_Xfm_Column_Chk_stagevar
    .filter(F.col("svRejectFlg") == 0)
    .select(
        F.trim(F.col("RSLT_DT")).alias("RSLT_DT"),
        F.trim(F.col("ACESION_NO")).alias("ACESION_NO"),
        F.trim(F.col("LAB_CD")).alias("LAB_CD"),
        F.trim(F.col("PATN_LAST_NM")).alias("PATN_LAST_NM"),
        F.trim(F.col("PATN_FIRST_NM")).alias("PATN_FIRST_NM"),
        F.trim(F.col("PATN_MID_NM")).alias("PATN_MID_NM"),
        F.trim(F.col("DOB")).alias("DOB"),
        F.trim(F.col("PATN_AGE")).alias("PATN_AGE"),
        F.trim(F.col("GNDR")).alias("GNDR"),
        F.trim(F.col("VNDR_BILL_ID")).alias("VNDR_BILL_ID"),
        F.trim(F.col("POL_NO")).alias("POL_NO"),
        F.trim(F.col("ORDER_NTNL_PROV_ID")).alias("ORDER_NTNL_PROV_ID"),
        F.trim(F.col("ORDER_ACCT_NM")).alias("ORDER_ACCT_NM"),
        F.trim(F.col("RNDR_NTNL_PROV_ID")).alias("RNDR_NTNL_PROV_ID"),
        F.trim(F.col("ICD_VRSN")).alias("ICD_VRSN"),
        F.trim(F.col("DIAG_CD")).alias("DIAG_CD"),
        F.trim(F.col("DIAG_CD_2")).alias("DIAG_CD_2"),
        F.trim(F.col("DIAG_CD_3")).alias("DIAG_CD_3"),
        F.trim(F.col("ORDER_NM")).alias("ORDER_NM"),
        F.trim(F.col("LOCAL_ORDER_CD")).alias("LOCAL_ORDER_CD"),
        F.trim(F.col("LOINC_CD")).alias("LOINC_CD"),
        F.trim(F.col("RSLT_NM")).alias("RSLT_NM"),
        F.trim(F.col("LOCAL_RSLT_CD")).alias("LOCAL_RSLT_CD"),
        F.trim(F.col("RSLT_VAL_NUM")).alias("RSLT_VAL_NUM"),
        F.trim(F.col("RSLT_VAL_TEXT")).alias("RSLT_VAL_TEXT"),
        F.trim(F.col("RSLT_UNIT")).alias("RSLT_UNIT"),
        F.trim(F.col("ABNORM_FLAG")).alias("ABNORM_FLAG"),
        F.trim(F.col("MRN_ID")).alias("MRN_ID"),
        F.trim(F.col("SOURCE_ID")).alias("SOURCE_ID"),
        F.col("MBR_UNIQ_KEY"),
        F.trim(F.col("svRejectMesg")).alias("ERROR_REASON")
    )
)

# -------------- Xfm_4 (Transformer) --------------
# Input => df_Lkp_to_Xfm4
# Output => out_Xfm4 => "SOURCE ID Do not match" in REJECT_REASON
df_Xfm_4 = df_Lkp_to_Xfm4.select(
    F.col("RSLT_DT"),
    F.col("ACESION_NO"),
    F.col("LAB_CD"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("DOB"),
    F.col("PATN_AGE"),
    F.col("GNDR"),
    F.col("VNDR_BILL_ID"),
    F.col("POL_NO"),
    F.col("ORDER_NTNL_PROV_ID"),
    F.col("ORDER_ACCT_NM"),
    F.col("RNDR_NTNL_PROV_ID"),
    F.col("ICD_VRSN"),
    F.col("DIAG_CD"),
    F.col("DIAG_CD_2"),
    F.col("DIAG_CD_3"),
    F.col("ORDER_NM"),
    F.col("LOCAL_ORDER_CD"),
    F.col("LOINC_CD"),
    F.col("RSLT_NM"),
    F.col("LOCAL_RSLT_CD"),
    F.col("RSLT_VAL_NUM"),
    F.col("RSLT_VAL_TEXT"),
    F.col("RSLT_UNIT"),
    F.col("ABNORM_FLAG"),
    F.col("MRN_ID"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.lit("SOURCE ID Do not match").alias("REJECT_REASON")
)

# -------------- Fn_1 (PxFunnel) --------------
# Inputs => out_ColGen1 (df_Xfm_2), out_Xfm1 (df_Xfm_1), out_Xfm4 (df_Xfm_4)
# funnel => union all
df_Fn_1 = df_Xfm_2.unionByName(df_Xfm_1, allowMissingColumns=True).unionByName(df_Xfm_4, allowMissingColumns=True)

# -------------- Lab_RejectFile (PxSequentialFile) --------------
# Input => df_Fn_1 => write to #RejectFile# under /verified => pipeline says "verified" => use adls_path => preserve extension
path_Lab_RejectFile = f"{adls_path}/verified/{RejectFile}"
write_files(
    df_Fn_1,
    path_Lab_RejectFile,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)

# -------------- IDS_PROV => Lnk_PROV_ID (PxLookup) --------------
# Input => Lnk_Valid => df_Xfm_Column_Chk_Lnk_Valid
# Lookup => df_IDS_PROV => left join => match multiple columns
df_Lnk_PROV_ID_join = df_Xfm_Column_Chk_Lnk_Valid.alias("Lnk_Valid").join(
    df_IDS_PROV.alias("Lkp_PROV_ID"),
    (
        (F.col("Lnk_Valid.ORDER_NTNL_PROV_ID") == F.col("Lkp_PROV_ID.NTNL_PROV_ID")) &
        (F.col("Lnk_Valid.SOURCE_ID") == F.col("Lkp_PROV_ID.PROV_ID")) &
        (F.col("Lnk_Valid.RNDR_NTNL_PROV_ID") == F.col("Lkp_PROV_ID.RNDR_NTNL_PROV_ID"))
    ),
    "left"
)
df_Lnk_PROV_ID_Out_XfmValid = df_Lnk_PROV_ID_join.select(
    F.col("Lnk_Valid.RSLT_DT"),
    F.col("Lnk_Valid.ACESION_NO"),
    F.col("Lnk_Valid.LAB_CD"),
    F.col("Lnk_Valid.PATN_LAST_NM"),
    F.col("Lnk_Valid.PATN_FIRST_NM"),
    F.col("Lnk_Valid.PATN_MID_NM"),
    F.col("Lnk_Valid.DOB"),
    F.col("Lnk_Valid.PATN_AGE"),
    F.col("Lnk_Valid.GNDR"),
    F.col("Lnk_Valid.VNDR_BILL_ID"),
    F.col("Lnk_Valid.POL_NO"),
    F.col("Lnk_Valid.ORDER_NTNL_PROV_ID"),
    F.col("Lnk_Valid.ORDER_ACCT_NM"),
    F.col("Lnk_Valid.RNDR_NTNL_PROV_ID"),
    F.col("Lnk_Valid.ICD_VRSN"),
    F.col("Lnk_Valid.DIAG_CD"),
    F.col("Lnk_Valid.DIAG_CD_2"),
    F.col("Lnk_Valid.DIAG_CD_3"),
    F.col("Lnk_Valid.ORDER_NM"),
    F.col("Lnk_Valid.LOCAL_ORDER_CD"),
    F.col("Lnk_Valid.LOINC_CD"),
    F.col("Lnk_Valid.RSLT_NM"),
    F.col("Lnk_Valid.LOCAL_RSLT_CD"),
    F.col("Lnk_Valid.RSLT_VAL_NUM"),
    F.col("Lnk_Valid.RSLT_VAL_TEXT"),
    F.col("Lnk_Valid.RSLT_UNIT"),
    F.col("Lnk_Valid.ABNORM_FLAG"),
    F.col("Lnk_Valid.MRN_ID"),
    F.col("Lnk_Valid.SOURCE_ID"),
    F.col("Lnk_Valid.MBR_UNIQ_KEY"),
    F.col("Lkp_PROV_ID.PROV_ID").alias("PROV_ID"),
    F.col("Lkp_PROV_ID.PROV_TERM_DT_SK").alias("PROV_TERM_DT_SK")
)

# -------------- Rm_Dups_5 (PxRemDup) --------------
# Input => df_Lnk_PROV_ID_Out_XfmValid => dedup on big list
dedup_keys_5 = [
    "RSLT_DT","ACESION_NO","LAB_CD","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","DOB","PATN_AGE","GNDR","VNDR_BILL_ID",
    "POL_NO","ORDER_NTNL_PROV_ID","ORDER_ACCT_NM","RNDR_NTNL_PROV_ID","ICD_VRSN","DIAG_CD","DIAG_CD_2","DIAG_CD_3",
    "ORDER_NM","LOCAL_ORDER_CD","LOINC_CD","RSLT_NM","LOCAL_RSLT_CD","RSLT_VAL_NUM","RSLT_VAL_TEXT","RSLT_UNIT",
    "ABNORM_FLAG","MRN_ID","SOURCE_ID"
]
df_Rm_Dups_5 = dedup_sort(df_Lnk_PROV_ID_Out_XfmValid, dedup_keys_5, [])

# -------------- Lkp3 (PxLookup) --------------
# Primary => df_Rm_Dups_5 => alias Lnk_Valid
# Lookup => df_IDS_NTNL_PROV => alias Lkp_PROV_ID => left join => same multi-col
df_Lkp3_join = df_Rm_Dups_5.alias("Lnk_Valid").join(
    df_IDS_NTNL_PROV.alias("Lkp_PROV_ID"),
    (
        (F.col("Lnk_Valid.ORDER_NTNL_PROV_ID") == F.col("Lkp_PROV_ID.NTNL_PROV_ID")) &
        (F.col("Lnk_Valid.SOURCE_ID") == F.col("Lkp_PROV_ID.NTNL_PROV_ID")) &  # job says? Actually it had: SourceKey= Lnk_Valid.SOURCE_ID => Lkp_PROV_ID.PROV_ID? The DS job is ambiguous. We'll follow the exact "JoinConditions" you listed:
        (F.col("Lnk_Valid.RNDR_NTNL_PROV_ID") == F.col("Lkp_PROV_ID.NTNL_PROV_ID"))
    ),
    "left"
)
df_Lkp3_to_Lkp4 = df_Lkp3_join.select(
    F.col("Lnk_Valid.RSLT_DT"),
    F.col("Lnk_Valid.ACESION_NO"),
    F.col("Lnk_Valid.LAB_CD"),
    F.col("Lnk_Valid.PATN_LAST_NM"),
    F.col("Lnk_Valid.PATN_FIRST_NM"),
    F.col("Lnk_Valid.PATN_MID_NM"),
    F.col("Lnk_Valid.DOB"),
    F.col("Lnk_Valid.PATN_AGE"),
    F.col("Lnk_Valid.GNDR"),
    F.col("Lnk_Valid.VNDR_BILL_ID"),
    F.col("Lnk_Valid.POL_NO"),
    F.col("Lkp_PROV_ID.NTNL_PROV_ID").alias("ORDER_NTNL_PROV_ID"),  # per DS
    F.col("Lnk_Valid.ORDER_ACCT_NM"),
    F.col("Lnk_Valid.RNDR_NTNL_PROV_ID"),
    F.col("Lnk_Valid.ICD_VRSN"),
    F.col("Lnk_Valid.DIAG_CD"),
    F.col("Lnk_Valid.DIAG_CD_2"),
    F.col("Lnk_Valid.DIAG_CD_3"),
    F.col("Lnk_Valid.ORDER_NM"),
    F.col("Lnk_Valid.LOCAL_ORDER_CD"),
    F.col("Lnk_Valid.LOINC_CD"),
    F.col("Lnk_Valid.RSLT_NM"),
    F.col("Lnk_Valid.LOCAL_RSLT_CD"),
    F.col("Lnk_Valid.RSLT_VAL_NUM"),
    F.col("Lnk_Valid.RSLT_VAL_TEXT"),
    F.col("Lnk_Valid.RSLT_UNIT"),
    F.col("Lnk_Valid.ABNORM_FLAG"),
    F.col("Lnk_Valid.MRN_ID"),
    F.col("Lnk_Valid.SOURCE_ID"),
    F.col("Lnk_Valid.MBR_UNIQ_KEY"),
    F.col("Lnk_Valid.PROV_ID")
)

# -------------- Lkp_4 (PxLookup) --------------
# Primary => df_Lkp3_to_Lkp4 => alias to_Lkp4
# Lookup => df_LOINC_CD => alias Lkp_LOINC_CD => left join => LOINC_CD
df_Lkp4_join = df_Lkp3_to_Lkp4.alias("to_Lkp4").join(
    df_LOINC_CD.alias("Lkp_LOINC_CD"),
    F.col("to_Lkp4.LOINC_CD") == F.col("Lkp_LOINC_CD.LOINC_CD"),
    "left"
)
df_Lkp4_main = df_Lkp4_join.select(
    F.col("to_Lkp4.RSLT_DT"),
    F.col("to_Lkp4.ACESION_NO"),
    F.col("to_Lkp4.LAB_CD"),
    F.col("to_Lkp4.PATN_LAST_NM"),
    F.col("to_Lkp4.PATN_FIRST_NM"),
    F.col("to_Lkp4.PATN_MID_NM"),
    F.col("to_Lkp4.DOB"),
    F.col("to_Lkp4.PATN_AGE"),
    F.col("to_Lkp4.GNDR"),
    F.col("to_Lkp4.VNDR_BILL_ID"),
    F.col("to_Lkp4.POL_NO"),
    F.col("to_Lkp4.ORDER_NTNL_PROV_ID"),
    F.col("to_Lkp4.ORDER_ACCT_NM"),
    F.col("to_Lkp4.RNDR_NTNL_PROV_ID"),
    F.col("to_Lkp4.ICD_VRSN"),
    F.col("to_Lkp4.DIAG_CD"),
    F.col("to_Lkp4.DIAG_CD_2"),
    F.col("to_Lkp4.DIAG_CD_3"),
    F.col("to_Lkp4.ORDER_NM"),
    F.col("to_Lkp4.LOCAL_ORDER_CD"),
    F.col("to_Lkp4.LOINC_CD"),
    F.col("to_Lkp4.RSLT_NM"),
    F.col("to_Lkp4.LOCAL_RSLT_CD"),
    F.col("to_Lkp4.RSLT_VAL_NUM"),
    F.col("to_Lkp4.RSLT_VAL_TEXT"),
    F.col("to_Lkp4.RSLT_UNIT"),
    F.col("to_Lkp4.ABNORM_FLAG"),
    F.col("to_Lkp4.MRN_ID"),
    F.col("to_Lkp4.SOURCE_ID"),
    F.col("to_Lkp4.MBR_UNIQ_KEY"),
    F.col("to_Lkp4.PROV_ID")
)

df_Lkp4_out_Lkp4 = df_Lkp4_join.select(
    F.col("to_Lkp4.RSLT_DT"),
    F.col("to_Lkp4.ACESION_NO"),
    F.col("to_Lkp4.LAB_CD"),
    F.col("to_Lkp4.PATN_LAST_NM"),
    F.col("to_Lkp4.PATN_FIRST_NM"),
    F.col("to_Lkp4.PATN_MID_NM"),
    F.col("to_Lkp4.DOB"),
    F.col("to_Lkp4.PATN_AGE"),
    F.col("to_Lkp4.GNDR"),
    F.col("to_Lkp4.VNDR_BILL_ID"),
    F.col("to_Lkp4.POL_NO"),
    F.col("to_Lkp4.ORDER_NTNL_PROV_ID"),
    F.col("to_Lkp4.ORDER_ACCT_NM"),
    F.col("to_Lkp4.RNDR_NTNL_PROV_ID"),
    F.col("to_Lkp4.ICD_VRSN"),
    F.col("to_Lkp4.DIAG_CD"),
    F.col("to_Lkp4.DIAG_CD_2"),
    F.col("to_Lkp4.DIAG_CD_3"),
    F.col("to_Lkp4.ORDER_NM"),
    F.col("to_Lkp4.LOCAL_ORDER_CD"),
    F.col("to_Lkp4.LOINC_CD"),
    F.col("to_Lkp4.RSLT_NM"),
    F.col("to_Lkp4.LOCAL_RSLT_CD"),
    F.col("to_Lkp4.RSLT_VAL_NUM"),
    F.col("to_Lkp4.RSLT_VAL_TEXT"),
    F.col("to_Lkp4.RSLT_UNIT"),
    F.col("to_Lkp4.ABNORM_FLAG"),
    F.col("to_Lkp4.MRN_ID"),
    F.col("to_Lkp4.SOURCE_ID"),
    F.col("to_Lkp4.MBR_UNIQ_KEY"),
    F.col("to_Lkp4.PROV_ID")
)

# -------------- Xfm8 (Transformer) --------------
# Input => df_Lkp4_main
df_Xfm8 = df_Lkp4_main.select(
    F.col("RSLT_DT"),
    F.col("ACESION_NO"),
    F.col("LAB_CD"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("DOB"),
    F.col("PATN_AGE"),
    F.col("GNDR"),
    F.col("VNDR_BILL_ID"),
    F.col("POL_NO"),
    F.when(
        (F.length(F.trim(F.col("ORDER_NTNL_PROV_ID"))) == 0) | (F.trim(F.col("ORDER_NTNL_PROV_ID")) == ""),
        "NA"
    ).otherwise(F.trim(F.col("ORDER_NTNL_PROV_ID"))).alias("ORDER_NTNL_PROV_ID"),
    F.col("ORDER_ACCT_NM"),
    F.col("RNDR_NTNL_PROV_ID"),
    F.when(
        F.col("ICD_VRSN").isNotNull(),
        F.when(
            F.trim(F.concat(F.lit("ICD"), F.regexp_replace(F.regexp_replace(F.col("ICD_VRSN"), "-", ""), "ICD", ""))) == "ICD",
            ""
        ).otherwise(F.trim(F.concat(F.lit("ICD"), F.regexp_replace(F.regexp_replace(F.col("ICD_VRSN"), "-", ""), "ICD", ""))))
    ).otherwise(F.col("ICD_VRSN")).alias("ICD_VRSN"),
    F.when(F.col("DIAG_CD").isNotNull(), F.regexp_replace(F.col("DIAG_CD"), "\\.", "")).otherwise(F.col("DIAG_CD")).alias("DIAG_CD"),
    F.when(F.col("DIAG_CD_2").isNotNull(), F.regexp_replace(F.col("DIAG_CD_2"), "\\.", "")).otherwise(F.col("DIAG_CD_2")).alias("DIAG_CD_2"),
    F.when(F.col("DIAG_CD_3").isNotNull(), F.regexp_replace(F.col("DIAG_CD_3"), "\\.", "")).otherwise(F.col("DIAG_CD_3")).alias("DIAG_CD_3"),
    F.col("ORDER_NM"),
    F.col("LOCAL_ORDER_CD"),
    F.col("LOINC_CD"),
    F.col("RSLT_NM"),
    F.col("LOCAL_RSLT_CD"),
    F.col("RSLT_VAL_NUM"),
    F.col("RSLT_VAL_TEXT"),
    F.col("RSLT_UNIT"),
    F.col("ABNORM_FLAG"),
    F.col("MRN_ID"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PROV_ID")
)

# -------------- Seq_Valid_Recds (PxSequentialFile) --------------
# Input => df_Xfm8 => write => #InFile_F# => /verified => preserve extension
path_Seq_Valid_Recds = f"{adls_path}/verified/{InFile_F}"
write_files(
    df_Xfm8,
    path_Seq_Valid_Recds,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)

# -------------- Xfm_9 (Transformer) --------------
# Input => df_Lkp4_out_Lkp4 => "ERROR_REASON" => 'LOINC_CD_SK Not Found in BLUEKC'
df_Xfm_9 = df_Lkp4_out_Lkp4.select(
    F.col("RSLT_DT"),
    F.col("ACESION_NO"),
    F.col("LAB_CD"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_MID_NM"),
    F.col("DOB"),
    F.col("PATN_AGE"),
    F.col("GNDR"),
    F.col("VNDR_BILL_ID"),
    F.col("POL_NO"),
    F.col("ORDER_NTNL_PROV_ID"),
    F.col("ORDER_ACCT_NM"),
    F.col("RNDR_NTNL_PROV_ID"),
    F.col("ICD_VRSN"),
    F.col("DIAG_CD"),
    F.col("DIAG_CD_2"),
    F.col("DIAG_CD_3"),
    F.col("ORDER_NM"),
    F.col("LOCAL_ORDER_CD"),
    F.col("LOINC_CD"),
    F.col("RSLT_NM"),
    F.col("LOCAL_RSLT_CD"),
    F.col("RSLT_VAL_NUM"),
    F.col("RSLT_VAL_TEXT"),
    F.col("RSLT_UNIT"),
    F.col("ABNORM_FLAG"),
    F.col("MRN_ID"),
    F.col("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.lit("LOINC_CD_SK Not Found in BLUEKC").alias("ERROR_REASON")
)

# -------------- Rm_Dups_3 (PxRemDup) --------------
# Input => df_Xfm_9 => dedup on big list
dedup_keys_9 = [
    "RSLT_DT","ACESION_NO","LAB_CD","PATN_LAST_NM","PATN_FIRST_NM","PATN_MID_NM","DOB","PATN_AGE","GNDR","VNDR_BILL_ID",
    "POL_NO","ORDER_NTNL_PROV_ID","ORDER_ACCT_NM","RNDR_NTNL_PROV_ID","ICD_VRSN","DIAG_CD","DIAG_CD_2","DIAG_CD_3","ORDER_NM",
    "LOCAL_ORDER_CD","LOINC_CD","RSLT_NM","LOCAL_RSLT_CD","RSLT_VAL_NUM","RSLT_VAL_TEXT","RSLT_UNIT","ABNORM_FLAG","MRN_ID","SOURCE_ID"
]
df_Rm_Dups_3_final = dedup_sort(df_Xfm_9, dedup_keys_9, [])

# -------------- Fn (PxFunnel) --------------
# Inputs => Lnk_Error (df_Xfm_Column_Chk_Lnk_Error), Out_Xfm5 (df_Xfm_5_Out_Xfm5), to_Fn (df_Rm_Dups_3_final)
df_Fn = df_Xfm_Column_Chk_Lnk_Error.unionByName(df_Xfm_5_Out_Xfm5, allowMissingColumns=True).unionByName(df_Rm_Dups_3_final, allowMissingColumns=True)

# -------------- Seq_Error_File (PxSequentialFile) --------------
# Input => df_Fn => #ErrorFile# => /verified => preserve extension
path_Seq_Error_File = f"{adls_path}/verified/{ErrorFile}"
write_files(
    df_Fn,
    path_Seq_Error_File,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)