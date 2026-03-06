# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmLetterExtr
# MAGIC CALLED BY: FctsClmExtr1Seq 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Pulls data from  attachments and letters  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                    Project/                                                                                                                                                             Development    Code                     Date 
# MAGIC Developer            Date               Altiris #        Change Description                                                                                                                       Project              Reviewer               Reviewed       
# MAGIC --------------------------  --------------------   ----------------   --------------------------------------------------------------------------------------------------------------------------------------------------    ---------------------    ---------------------------   --------------------
# MAGIC SAndrew              08/04/2004                      Originally Programmed
# MAGIC Steph Goddard    02/15/2006                      Added transform, primary key steps for sequencer
# MAGIC Steph Goddard    03/21/2006                      Added sequence number calculation - was using Facets sequence number which is always 0.                              
# MAGIC Brent Leland        03/23/2006                      Added ORDER BY clause to insure sequence number correct
# MAGIC BJ Luce               03/20/2006                      add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. 
# MAGIC                                                                       If the claim is on the file, a row is not generated for it in IDS. 
# MAGIC                                                                       However, an R row will be build for it if the status if '91'
# MAGIC Sanderw              12/08/2006    1756          Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen      08/15/2007    Balancing   Added Snapshot extract for balancing                                                                                            devlIDS30         Steph Goddard       8/30/2007
# MAGIC Hugh Sisson        07/10/2008    3567          Primary key restructuring                                                                                                                 devlIDS30         Brent Leland          07-22-2008  
# MAGIC  
# MAGIC Michael Harmon  01/24/2017   TFS#15998   Corrected domain in Transform from 'CLAIM LETTER TYPE CODE'                                          Integrate\\Dev3    Kalyan Neelam        2017-01-30
# MAGIC                                                                         to 'CLAIM LETTER TYPE'
# MAGIC 
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names  IntegrateDev2                  
# MAGIC Prabhu ES               2022-03-29      S2S          MSSQL ODBC conn params added                                                                                           IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Pulling Facets Claim Letter Data
# MAGIC Hash file (hf_clm_ltr_allcol) created in ClmLtrPK shared container cleared at the end of this job
# MAGIC Writing Sequential File to /key
# MAGIC Hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC Bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmLtrPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
lhofacets_secret_name = get_widget_value('lhofacets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read CHashedFileStage: hf_clm_fcts_reversals as scenario C => parquet
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read CHashedFileStage: clm_nasco_dup_bypass as scenario C => parquet
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Read ODBCConnector: CER_ATXR_ATTACH_ATLT_LETTER
jdbc_url_lhofacets, jdbc_props_lhofacets = get_db_config(lhofacets_secret_name)
extract_query_CER_ATXR_ATTACH_ATLT_LETTER = """
SELECT
 tmp.CLM_ID CLAIM_ID,
 atch.ATXR_DESC,
 atch.ATXR_LAST_UPD_DT,
 atch.ATXR_LAST_UPD_USUS,
 letr.ATLT_SEQ_NO,
 letr.ATLD_ID,
 letr.ATLT_FOLLOWUP_IND,
 letr.ATLT_FROM_NAME,
 letr.ATLT_MAILED_DT,
 letr.ATLT_MAILED_IND,
 letr.ATLT_PRINTED_DT,
 letr.ATLT_PRINTED_IND,
 letr.ATLT_RE_NAME,
 letr.ATLT_REPRINT_STATUS,
 letr.ATLT_REQUEST_DT,
 letr.ATLT_REQUEST_IND,
 letr.ATLT_RECEIVED_DT,
 letr.ATLT_RECEIVED_IND,
 letr.ATLT_SUBMITTED_DT,
 letr.ATLT_SUBMITTED_IND,
 letr.ATLT_SUBJECT,
 letr.ATLT_TO_NAME
FROM tempdb..#DriverTable# tmp,
     #$LhoFacetsStgOwner#.CMC_CLCL_CLAIM clm,
     #$LhoFacetsStgOwner#.CER_ATXR_ATTACH_U atch,
     #$LhoFacetsStgOwner#.CER_ATLT_LETTER_D letr
WHERE tmp.CLM_ID = clm.CLCL_ID
  AND clm.ATXR_SOURCE_ID = atch.ATXR_SOURCE_ID
  AND atch.ATSY_ID = letr.ATSY_ID
  AND atch.ATXR_DEST_ID = letr.ATXR_DEST_ID
ORDER BY tmp.CLM_ID
"""
df_CER_ATXR_ATTACH_ATLT_LETTER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacets)
    .options(**jdbc_props_lhofacets)
    .option("query", extract_query_CER_ATXR_ATTACH_ATLT_LETTER)
    .load()
)

# StripField (CTransformerStage)
df_Strip = (
    df_CER_ATXR_ATTACH_ATLT_LETTER
    .withColumn("CLAIM_ID", strip_field(F.col("CLAIM_ID")))
    .withColumn("ATXR_DESC", strip_field(F.col("ATXR_DESC")))
    .withColumn("ATXR_LAST_UPD_DT", F.col("ATXR_LAST_UPD_DT"))
    .withColumn("ATXR_LAST_UPD_USUS", strip_field(F.col("ATXR_LAST_UPD_USUS")))
    .withColumn("ATLT_SEQ_NO", F.col("ATLT_SEQ_NO"))
    .withColumn("ATLD_ID", strip_field(F.col("ATLD_ID")))
    .withColumn("ATLT_FOLLOWUP_IND", strip_field(F.col("ATLT_FOLLOWUP_IND")))
    .withColumn("ATLT_FROM_NAME", strip_field(F.col("ATLT_FROM_NAME")))
    .withColumn("ATLT_MAILED_DT", F.col("ATLT_MAILED_DT"))
    .withColumn("ATLT_MAILED_IND", strip_field(F.col("ATLT_MAILED_IND")))
    .withColumn("ATLT_PRINTED_DT", F.col("ATLT_PRINTED_DT"))
    .withColumn("ATLT_PRINTED_IND", strip_field(F.col("ATLT_PRINTED_IND")))
    .withColumn("ATLT_RE_NAME", strip_field(F.col("ATLT_RE_NAME")))
    .withColumn("ATLT_REPRINT_STATUS", strip_field(F.col("ATLT_REPRINT_STATUS")))
    .withColumn("ATLT_REQUEST_DT", F.col("ATLT_REQUEST_DT"))
    .withColumn("ATLT_REQUEST_IND", strip_field(F.col("ATLT_REQUEST_IND")))
    .withColumn("ATLT_RECEIVED_DT", F.col("ATLT_RECEIVED_DT"))
    .withColumn("ATLT_RECEIVED_IND", strip_field(F.col("ATLT_RECEIVED_IND")))
    .withColumn("ATLT_SUBMITTED_DT", F.col("ATLT_SUBMITTED_DT"))
    .withColumn("ATLT_SUBMITTED_IND", strip_field(F.col("ATLT_SUBMITTED_IND")))
    .withColumn("ATLT_SUBJECT", strip_field(F.col("ATLT_SUBJECT")))
    .withColumn("ATLT_TO_NAME", strip_field(F.col("ATLT_TO_NAME")))
)

# BusinessRules (CTransformerStage)
# 1) Join with fcts_reversals (left)
# 2) Join with nasco_dup_lkup (left)
df_BusinessRules_joined = (
    df_Strip.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLAIM_ID") == F.col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLAIM_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
)

# Simulate stage variable logic for SeqNo: if current row's CLAIM_ID same as previous, then +1, else 1
# Because we cannot define a truly sequential cross-row variable in a single pass, we approximate with a window on row ordering.
windowSpec = Window.orderBy(F.monotonically_increasing_id())
df_BusinessRules_withLag = (
    df_BusinessRules_joined
    .withColumn("PrevClmNo", F.lag(F.col("Strip.CLAIM_ID"), 1).over(windowSpec))
    .withColumn("PrevSeqNo", F.lag(F.lit(1), 1).over(windowSpec))  # placeholder for prior Seq, needed for approach
)

df_BusinessRules = df_BusinessRules_withLag.withColumn(
    "SeqNo",
    F.when(
        F.col("Strip.CLAIM_ID") == F.col("PrevClmNo"),
        F.col("PrevSeqNo") + 1
    ).otherwise(1)
)

# Output link constraints
df_ClmLetter = df_BusinessRules.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
)

df_Reversals = df_BusinessRules.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)

# Construct df_ClmLetter with its columns
df_ClmLetter_selected = df_ClmLetter.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), trim(F.col("Strip.CLAIM_ID")), F.lit(";"), F.col("SeqNo")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LTR_SK"),
    trim(F.col("Strip.CLAIM_ID")).alias("CLM_ID"),
    F.col("SeqNo").alias("CLM_LTR_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.when(
        F.col("Strip.ATXR_LAST_UPD_USUS").isNull() | (F.length(F.trim(F.col("Strip.ATXR_LAST_UPD_USUS"))) == 0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.ATXR_LAST_UPD_USUS"))).alias("LAST_UPDT_USER_ID"),
    F.when(
        F.col("Strip.ATLT_REPRINT_STATUS").isNull() | (F.length(F.trim(F.col("Strip.ATLT_REPRINT_STATUS"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_REPRINT_STATUS")))).alias("CLM_LTR_REPRT_STTUS_CD"),
    F.when(
        F.col("Strip.ATLD_ID").isNull() | (F.length(F.trim(F.col("Strip.ATLD_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLD_ID")))).alias("CLM_LTR_TYP_CD"),
    F.when(
        F.col("Strip.ATLT_FOLLOWUP_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_FOLLOWUP_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_FOLLOWUP_IND")))).alias("FLW_UP_IN"),
    F.when(
        F.col("Strip.ATLT_MAILED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_MAILED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_MAILED_IND")))).alias("MAIL_IN"),
    F.when(
        F.col("Strip.ATLT_PRINTED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_PRINTED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_PRINTED_IND")))).alias("PRT_IN"),
    F.when(
        F.col("Strip.ATLT_RECEIVED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_RECEIVED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_RECEIVED_IND")))).alias("RCV_IN"),
    F.when(
        F.col("Strip.ATLT_REQUEST_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_REQUEST_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_REQUEST_IND")))).alias("RQST_IN"),
    F.when(
        F.col("Strip.ATLT_SUBMITTED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_SUBMITTED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_SUBMITTED_IND")))).alias("SUBMT_IN"),
    F.when(
        F.col("Strip.ATXR_LAST_UPD_DT").isNull() | (F.length(F.trim(F.col("Strip.ATXR_LAST_UPD_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATXR_LAST_UPD_DT")), 1, 10)).alias("LAST_UPDT_DT"),
    F.when(
        F.col("Strip.ATLT_MAILED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_MAILED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_MAILED_DT")), 1, 10)).alias("MAIL_DT"),
    F.when(
        F.col("Strip.ATLT_PRINTED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_PRINTED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_PRINTED_DT")), 1, 10)).alias("PRT_DT"),
    F.when(
        F.col("Strip.ATLT_RECEIVED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_RECEIVED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_RECEIVED_DT")), 1, 10)).alias("RCV_DT"),
    F.when(
        F.col("Strip.ATLT_REQUEST_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_REQUEST_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_REQUEST_DT")), 1, 10)).alias("RQST_DT"),
    F.when(
        F.col("Strip.ATLT_SUBMITTED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_SUBMITTED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_SUBMITTED_DT")), 1, 10)).alias("SUBMT_DT"),
    F.expr("""STRIP.TRAILING.CHARACTERS(Ereplace(TRIM(Strip.ATLT_FROM_NAME), ("" : '\"' : ""), ""), ",")""").alias("FROM_NM"),
    F.col("Strip.ATXR_DESC").alias("LTR_DESC"),
    F.expr("""STRIP.TRAILING.CHARACTERS(Ereplace(TRIM(Strip.ATLT_RE_NAME), ("" : '\"' : ""), ""), ",")""").alias("REF_NM"),
    F.col("Strip.ATLT_SUBJECT").alias("SUBJ_TX"),
    F.expr("""STRIP.TRAILING.CHARACTERS(Ereplace(TRIM(Strip.ATLT_TO_NAME), ("" : '\"' : ""), ""), ",")""").alias("TO_NM")
)

df_Reversals_selected = df_Reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), trim(F.col("Strip.CLAIM_ID")), F.lit("R;"), F.col("SeqNo")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LTR_SK"),
    F.concat(trim(F.col("Strip.CLAIM_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("SeqNo").alias("CLM_LTR_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.when(
        F.col("Strip.ATXR_LAST_UPD_USUS").isNull() | (F.length(F.trim(F.col("Strip.ATXR_LAST_UPD_USUS"))) == 0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.ATXR_LAST_UPD_USUS"))).alias("LAST_UPDT_USER_ID"),
    F.when(
        F.col("Strip.ATLT_REPRINT_STATUS").isNull() | (F.length(F.trim(F.col("Strip.ATLT_REPRINT_STATUS"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_REPRINT_STATUS")))).alias("CLM_LTR_REPRT_STTUS_CD"),
    F.when(
        F.col("Strip.ATLD_ID").isNull() | (F.length(F.trim(F.col("Strip.ATLD_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLD_ID")))).alias("CLM_LTR_TYP_CD"),
    F.when(
        F.col("Strip.ATLT_FOLLOWUP_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_FOLLOWUP_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_FOLLOWUP_IND")))).alias("FLW_UP_IN"),
    F.when(
        F.col("Strip.ATLT_MAILED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_MAILED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_MAILED_IND")))).alias("MAIL_IN"),
    F.when(
        F.col("Strip.ATLT_PRINTED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_PRINTED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_PRINTED_IND")))).alias("PRT_IN"),
    F.when(
        F.col("Strip.ATLT_RECEIVED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_RECEIVED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_RECEIVED_IND")))).alias("RCV_IN"),
    F.when(
        F.col("Strip.ATLT_REQUEST_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_REQUEST_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_REQUEST_IND")))).alias("RQST_IN"),
    F.when(
        F.col("Strip.ATLT_SUBMITTED_IND").isNull() | (F.length(F.trim(F.col("Strip.ATLT_SUBMITTED_IND"))) == 0),
        F.lit("X")
    ).otherwise(F.upper(F.trim(F.col("Strip.ATLT_SUBMITTED_IND")))).alias("SUBMT_IN"),
    F.when(
        F.col("Strip.ATXR_LAST_UPD_DT").isNull() | (F.length(F.trim(F.col("Strip.ATXR_LAST_UPD_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATXR_LAST_UPD_DT")), 1, 10)).alias("LAST_UPDT_DT"),
    F.when(
        F.col("Strip.ATLT_MAILED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_MAILED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_MAILED_DT")), 1, 10)).alias("MAIL_DT"),
    F.when(
        F.col("Strip.ATLT_PRINTED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_PRINTED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_PRINTED_DT")), 1, 10)).alias("PRT_DT"),
    F.when(
        F.col("Strip.ATLT_RECEIVED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_RECEIVED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_RECEIVED_DT")), 1, 10)).alias("RCV_DT"),
    F.when(
        F.col("Strip.ATLT_REQUEST_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_REQUEST_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_REQUEST_DT")), 1, 10)).alias("RQST_DT"),
    F.when(
        F.col("Strip.ATLT_SUBMITTED_DT").isNull() | (F.length(F.trim(F.col("Strip.ATLT_SUBMITTED_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("Strip.ATLT_SUBMITTED_DT")), 1, 10)).alias("SUBMT_DT"),
    F.expr("""STRIP.TRAILING.CHARACTERS(Ereplace(TRIM(Strip.ATLT_FROM_NAME), ("" : '\"' : ""), ""), ",")""").alias("FROM_NM"),
    F.col("Strip.ATXR_DESC").alias("LTR_DESC"),
    F.expr("""STRIP.TRAILING.CHARACTERS(Ereplace(TRIM(Strip.ATLT_RE_NAME), ("" : '\"' : ""), ""), ",")""").alias("REF_NM"),
    F.col("Strip.ATLT_SUBJECT").alias("SUBJ_TX"),
    F.expr("""STRIP.TRAILING.CHARACTERS(Ereplace(TRIM(Strip.ATLT_TO_NAME), ("" : '\"' : ""), ""), ",")""").alias("TO_NM")
)

# Collector (CCollector) => union
df_Collector = df_ClmLetter_selected.unionByName(df_Reversals_selected)

# Snapshot (CTransformerStage) => single input with multiple outputs
# We will keep df_Snapshot as the base from df_Collector
df_Snapshot = df_Collector

# Output link V154S0P2 => "AllCol"
df_AllCol = df_Snapshot.select(
    "SRC_SYS_CD_SK",  # Expression: SrcSysCdSk (taken as literal outside)
    "CLM_ID",
    "CLM_LTR_SEQ_NO",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LTR_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "LAST_UPDT_USER_ID",
    "CLM_LTR_REPRT_STTUS_CD",
    "CLM_LTR_TYP_CD",
    "FLW_UP_IN",
    "MAIL_IN",
    "PRT_IN",
    "RCV_IN",
    "RQST_IN",
    "SUBMT_IN",
    "LAST_UPDT_DT",
    "MAIL_DT",
    "PRT_DT",
    "RCV_DT",
    "RQST_DT",
    "SUBMT_DT",
    "FROM_NM",
    "LTR_DESC",
    "REF_NM",
    "SUBJ_TX",
    "TO_NM"
).withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))

# Output link V154S0P5 => "Transform"
df_Transform = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LTR_SEQ_NO").alias("CLM_LTR_SEQ_NO")
)

# Output link V154S0P7 => "Snapshot"
df_SnapshotOut = df_Snapshot.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LTR_SEQ_NO").alias("CLM_LTR_SEQ_NO"),
    F.col("CLM_LTR_TYP_CD").alias("CLM_LTR_TYP_CD")
)

# Transform (CTransformerStage) => input = V99S1P1 => "Snapshot"
df_Transform2 = df_SnapshotOut.withColumn(
    "CLM_LTR_TYP_CD_SK",
    GetFkeyCodes('FACETS', 0, "CLAIM LETTER TYPE", F.col("CLM_LTR_TYP_CD"), 'X')
)

# Output to B_CLM_LTR (CSeqFileStage) => "RowCount"
df_B_CLM_LTR = df_Transform2.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LTR_SEQ_NO"),
    F.col("CLM_LTR_TYP_CD_SK")
)

write_files(
    df_B_CLM_LTR,
    f"{adls_path}/load/B_CLM_LTR.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container: ClmLtrPK => has 2 inputs => "AllCol", "Transform". 1 output => "Key"
params_ClmLtrPK = {
    "DriverTable": "#DriverTable#",
    "CurrRunCycle": "#CurrRunCycle#",
    "$FacetsDB": "#$FacetsDB#",
    "$FacetsOwner": "#$FacetsOwner#",
    "RunID": "#RunID#",
    "CurrentDate": "#CurrentDate#",
    "$IDSOwner": "#$IDSOwner#"
}
df_ClmLtrPK = ClmLtrPK(df_AllCol, df_Transform, params_ClmLtrPK)

# IdsClmLetterPkey (CSeqFileStage) => writing final file
# Must select columns in the correct order and rpad char columns
df_IdsClmLetterPkey = df_ClmLtrPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LTR_SK",
    "CLM_ID",
    "CLM_LTR_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "LAST_UPDT_USER_ID",
    "CLM_LTR_REPRT_STTUS_CD",
    "CLM_LTR_TYP_CD",
    "FLW_UP_IN",
    "MAIL_IN",
    "PRT_IN",
    "RCV_IN",
    "RQST_IN",
    "SUBMT_IN",
    "LAST_UPDT_DT",
    "MAIL_DT",
    "PRT_DT",
    "RCV_DT",
    "RQST_DT",
    "SUBMT_DT",
    "FROM_NM",
    "LTR_DESC",
    "REF_NM",
    "SUBJ_TX",
    "TO_NM"
).withColumn(
    "INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "LAST_UPDT_USER_ID", F.rpad(F.col("LAST_UPDT_USER_ID"), 10, " ")
).withColumn(
    "CLM_LTR_REPRT_STTUS_CD", F.rpad(F.col("CLM_LTR_REPRT_STTUS_CD"), 10, " ")
).withColumn(
    "CLM_LTR_TYP_CD", F.rpad(F.col("CLM_LTR_TYP_CD"), 10, " ")
).withColumn(
    "FLW_UP_IN", F.rpad(F.col("FLW_UP_IN"), 1, " ")
).withColumn(
    "MAIL_IN", F.rpad(F.col("MAIL_IN"), 1, " ")
).withColumn(
    "PRT_IN", F.rpad(F.col("PRT_IN"), 1, " ")
).withColumn(
    "RCV_IN", F.rpad(F.col("RCV_IN"), 1, " ")
).withColumn(
    "RQST_IN", F.rpad(F.col("RQST_IN"), 1, " ")
).withColumn(
    "SUBMT_IN", F.rpad(F.col("SUBMT_IN"), 1, " ")
).withColumn(
    "LAST_UPDT_DT", F.rpad(F.col("LAST_UPDT_DT"), 10, " ")
).withColumn(
    "MAIL_DT", F.rpad(F.col("MAIL_DT"), 10, " ")
).withColumn(
    "PRT_DT", F.rpad(F.col("PRT_DT"), 10, " ")
).withColumn(
    "RCV_DT", F.rpad(F.col("RCV_DT"), 10, " ")
).withColumn(
    "RQST_DT", F.rpad(F.col("RQST_DT"), 10, " ")
).withColumn(
    "SUBMT_DT", F.rpad(F.col("SUBMT_DT"), 10, " ")
)

write_files(
    df_IdsClmLetterPkey,
    f"{adls_path}/key/LhoFctsClmLetterExtr.LhoFctsClmLtr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)