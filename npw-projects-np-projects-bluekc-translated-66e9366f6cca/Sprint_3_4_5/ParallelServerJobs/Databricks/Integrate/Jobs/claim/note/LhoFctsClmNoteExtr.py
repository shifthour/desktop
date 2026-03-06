# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmNoteExtr
# MAGIC CALLED BY:  LhoFctsClmOnDmdExtr1Seq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from  attachments and letters  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:    CER_ATXR_ATTACH_U
# MAGIC                   CER_ATLT_NOTE_D
# MAGIC                   CMC_CLCL_CLAIM
# MAGIC 
# MAGIC HASH FILES:  6;hf_clm_note_seqno0;hf_clm_note_seqno1;hf_clm_note_seqno2;hf_clm_note_seqno3;hf_clm_note_seqno4;hf_clm_note_allcol
# MAGIC                         Hash file (hf_clm_note_allcol) cleared is in the shared container; ClmNotePK.
# MAGIC                        hf_clm_nasco_dup_bypass - do not clear
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file who's name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               SAndrew   08/04/2004-   Originally Programmed
# MAGIC               Steph Goddard  02/16/06   Combined extract, transform, primary key for sequencer
# MAGIC               Steph Goddard  03/21/06   Changed to create sequence number for multiple records instead of copying sequence number from Facets.
# MAGIC                                                            Facets sequence number is always zero
# MAGIC                                                           Also corrected hash file names in hash.clear
# MAGIC               Brent Leland      03/23/2006   Added ORDER BY clause to insure sequence number correct
# MAGIC               BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC              Steph Goddard  04/04/2006   removed hash files - this table needs to be remapped.  Defaulted fields to "NOT VALID" or "NOT VALID AT THIS TIME"
# MAGIC                Sanderw  12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007       Balancing              Added Snapshot extract for balancing                                        devlIDS30                      Steph Goddard          8/30/07
# MAGIC Ralph Tucker          06/06/2008      3657 Primary Key   Changed primary key from hash file to DB2 table                        devlIDS                          Steph Goddard          07/03/2008
# MAGIC                                                                                         Remove uppercase of claim ID
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names  IntegrateDev2
# MAGIC 
# MAGIC Harikanth Reddy    10/12/2020                                      Brought up to standards                                                                IntegrateDev2
# MAGIC Kotha Venkat
# MAGIC Prabhu ES               2022-03-29        S2S                         MSSQL ODBC conn params added                                             IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Pulls data from  attachments and letters
# MAGIC Writing Sequential File to ../key
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC This table needs new requirements for getting the claim note description.  In the meantime, only the note_d is read - we are ignoring the note_c records.
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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmNotePK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('IDSOwner','')
lhoFacetsStg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read from hashed file "hf_clm_fcts_reversals" (Scenario C -> Parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read from hashed file "hf_clm_nasco_dup_bypass" (Scenario C -> Parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Connect to database for ODBCConnector stage "CER_ATXR_ATTACH_ATNT_NOTE"
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    "SELECT \n"
    " tmp.CLM_ID CLAIM_ID,\n"
    " atch.ATSY_ID,\n"
    " atch.ATXR_DEST_ID,\n"
    " note.ATNT_SEQ_NO,\n"
    " atch.ATTB_ID,\n"
    " atch.ATXR_DESC,\n"
    " atch.ATXR_LAST_UPD_DT,\n"
    " atch.ATXR_LAST_UPD_USUS\n"
    "FROM tempdb..#DriverTable# tmp,\n"
    "     #$LhoFacetsStgOwner#.CMC_CLCL_CLAIM clm,\n"
    "     #$LhoFacetsStgOwner#.CER_ATXR_ATTACH_U atch,\n"
    "     #$LhoFacetsStgOwner#.CER_ATNT_NOTE_D note\n"
    "WHERE tmp.CLM_ID = clm.CLCL_ID\n"
    "  AND clm.ATXR_SOURCE_ID = atch.ATXR_SOURCE_ID\n"
    "  AND atch.ATSY_ID = note.ATSY_ID\n"
    "  AND atch.ATXR_DEST_ID = note.ATXR_DEST_ID\n"
    "ORDER BY tmp.CLM_ID"
)
df_CER_ATXR_ATTACH_ATNT_NOTE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer "StripField" -> remove CHAR(10), CHAR(13), CHAR(9)
df_strip = (
    df_CER_ATXR_ATTACH_ATNT_NOTE
    .withColumn("CLAIM_ID", strip_field(F.col("CLAIM_ID")))
    .withColumn("ATSY_ID", strip_field(F.col("ATSY_ID")))
    .withColumn("ATTB_ID", strip_field(F.col("ATTB_ID")))
    .withColumn("ATXR_DESC", strip_field(F.col("ATXR_DESC")))
    .withColumn("ATXR_LAST_UPD_USUS", strip_field(F.col("ATXR_LAST_UPD_USUS")))
    .select(
        F.col("CLAIM_ID"),
        F.col("ATSY_ID"),
        F.col("ATXR_DEST_ID"),
        F.col("ATNT_SEQ_NO"),
        F.col("ATTB_ID"),
        F.col("ATXR_DESC"),
        F.col("ATXR_LAST_UPD_DT"),
        F.col("ATXR_LAST_UPD_USUS")
    )
)

# BusinessRules: join with fcts_reversals (left) and nasco_dup_lkup (left), define SeqNo, define SrcSysCd
df_br_joined = (
    df_strip.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"),
          F.col("Strip.CLAIM_ID") == F.col("fcts_reversals.CLCL_ID"),
          "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
          F.col("Strip.CLAIM_ID") == F.col("nasco_dup_lkup.CLM_ID"),
          "left")
)

# Emulate DS stage variable for SeqNo: row_number() per CLAIM_ID
windowSpec = Window.partitionBy(F.col("Strip.CLAIM_ID")).orderBy(F.col("Strip.CLAIM_ID"))
df_br_seq = df_br_joined.withColumn("SeqNo", F.row_number().over(windowSpec))

# Add SrcSysCd from a user-defined function
df_br_seq = df_br_seq.withColumn(
    "SrcSysCd",
    GetFkeyCodes(F.lit("IDS"), F.lit(1), F.lit("SOURCE SYSTEM"), F.lit("FACETS"), F.lit("X"))
)

# Split into two outputs based on constraints
df_clmnote_out = df_br_seq.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
)
df_reversals_out = df_br_seq.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89"))
        | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
        | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("99"))
    )
)

# Derive columns for "ClmNote"
df_clmnote_out = df_clmnote_out.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(
        F.col("SrcSysCd"),
        trim(F.col("Strip.CLAIM_ID")),
        F.lit(";"),
        F.col("SeqNo")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_NOTE_SK"),
    trim(F.col("Strip.CLAIM_ID")).alias("CLM_ID"),
    F.col("SeqNo").alias("CLM_NOTE_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.lit("NA"), 10, " ").alias("LAST_UPDT_USER_ID"),
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_NOTE_TYP_CD"),
    F.when(
        F.col("Strip.ATXR_LAST_UPD_DT").isNull() |
        (F.length(trim(F.col("Strip.ATXR_LAST_UPD_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.substring(trim(F.col("Strip.ATXR_LAST_UPD_DT")), 1, 10)
    ).alias("LAST_UPDT_DT"),
    F.lit("NOT VALID AT THIS TIME").alias("NOTE_DESC"),
    F.lit("NOT VALID AT THIS TIME").alias("NOTE_TX")
)

# Derive columns for "reversals"
df_reversals_out = df_reversals_out.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(
        F.col("SrcSysCd"),
        F.lit(";"),
        trim(F.col("Strip.CLAIM_ID")),
        F.lit("R;"),
        F.col("SeqNo")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_NOTE_SK"),
    F.concat(trim(F.col("Strip.CLAIM_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("SeqNo").alias("CLM_NOTE_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.lit("NA"), 10, " ").alias("LAST_UPDT_USER_ID"),
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_NOTE_TYP_CD"),
    F.when(
        F.col("Strip.ATXR_LAST_UPD_DT").isNull() |
        (F.length(trim(F.col("Strip.ATXR_LAST_UPD_DT"))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.substring(trim(F.col("Strip.ATXR_LAST_UPD_DT")), 1, 10)
    ).alias("LAST_UPDT_DT"),
    F.lit("NOT VALID AT THIS TIME").alias("NOTE_DESC"),
    F.lit("NOT VALID AT THIS TIME").alias("NOTE_TX")
)

# Collector: union by name
collector_cols = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_NOTE_SK",
    "CLM_ID",
    "CLM_NOTE_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_USER_ID",
    "CLM_NOTE_TYP_CD",
    "LAST_UPDT_DT",
    "NOTE_DESC",
    "NOTE_TX"
]
df_collector = df_clmnote_out.select(collector_cols).unionByName(df_reversals_out.select(collector_cols))

# SnapShot stage outputs:
# 1) "SnapShot" pin
df_SnapShot_output = df_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_NOTE_SEQ_NO")
)

# 2) "AllCol" pin
df_AllCol_output = df_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_NOTE_SEQ_NO").alias("CLM_NOTE_SEQ"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    # stage variable RowPassThru="Y"
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(
        F.col("SRC_SYS_CD"),
        trim(F.col("CLM_ID")),
        F.lit(";"),
        F.col("CLM_NOTE_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("CLM_NOTE_TYP_CD").alias("CLM_NOTE_TYP_CD"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("NOTE_DESC").alias("NOTE_DESC"),
    F.col("NOTE_TX").alias("NOTE_TX")
)

# 3) "Transform" pin
df_Transform_output = df_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_NOTE_SEQ_NO").alias("CLM_NOTE_SEQ_NO")
)

# Transformer2: input = df_SnapShot_output
df_Transformer2 = df_SnapShot_output.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_NOTE_SEQ_NO")
)

# Write "B_CLM_NOTE" file
b_clm_note_file_path = f"{adls_path}/load/B_CLM_NOTE.{SrcSysCd}.dat.{RunID}"
write_files(
    df_Transformer2,
    b_clm_note_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container "ClmNotePK"
params = {
    "DriverTable": DriverTable,
    "CurrRunCycle": CurrRunCycle,
    "$FacetsDB": "<...>",
    "$FacetsOwner": "<...>",
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "$IDSOwner": IDSOwner,
    "SrcSysCd": SrcSysCd
}
df_key = ClmNotePK(df_AllCol_output, df_Transform_output, params)

# Final output "FctsClmNoteExtr"
# RPad columns of char/varchar in final select
df_fctsClmNoteExtr = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_NOTE_SK"),
    F.col("CLM_ID"),
    F.col("CLM_NOTE_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("LAST_UPDT_USER_ID"), 10, " ").alias("LAST_UPDT_USER_ID"),
    F.rpad(F.col("CLM_NOTE_TYP_CD"), 10, " ").alias("CLM_NOTE_TYP_CD"),
    F.rpad(F.col("LAST_UPDT_DT"), 10, " ").alias("LAST_UPDT_DT"),
    F.col("NOTE_DESC"),
    F.col("NOTE_TX")
)

fcts_clm_note_extr_file_path = f"{adls_path}/key/LhoFctsClmNoteExtr.LhoFctsClmNote.dat.{RunID}"
write_files(
    df_fctsClmNoteExtr,
    fcts_clm_note_extr_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)