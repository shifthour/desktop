# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
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
# MAGIC Prabhu ES            2022-02-28   S2S               MSSQL connection parameters added                                                                                       IntegrateDev5                    Manasa Andru          2022-06-10

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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, lit, regexp_replace, trim as spark_trim, when, length, substring, upper, row_number, rpad, concat, ltrim, rtrim
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value("DriverTable","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# Read from hashed file hf_clm_fcts_reversals (Scenario C -> read parquet)
df_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read from hashed file hf_clm_nasco_dup_bypass (Scenario C -> read parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ODBCConnector: CER_ATXR_ATTACH_ATLT_LETTER
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT 
Trim(tmp.CLM_ID) as CLAIM_ID, 
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
FROM tempdb..{DriverTable} tmp,
     {FacetsOwner}.CMC_CLCL_CLAIM clm,
     {FacetsOwner}.CER_ATXR_ATTACH_U atch,
     {FacetsOwner}.CER_ATLT_LETTER_D letr
WHERE tmp.CLM_ID = clm.CLCL_ID
AND clm.ATXR_SOURCE_ID = atch.ATXR_SOURCE_ID
AND atch.ATSY_ID = letr.ATSY_ID
AND atch.ATXR_DEST_ID = letr.ATXR_DEST_ID
ORDER BY tmp.CLM_ID
"""
df_extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# StripField Transformer
df_strip = df_extract.select(
    regexp_replace(col("CLAIM_ID"), "[\r\n\t]", "").alias("CLAIM_ID"),
    regexp_replace(col("ATXR_DESC"), "[\r\n\t]", "").alias("ATXR_DESC"),
    col("ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT"),
    regexp_replace(col("ATXR_LAST_UPD_USUS"), "[\r\n\t]", "").alias("ATXR_LAST_UPD_USUS"),
    col("ATLT_SEQ_NO").alias("ATLT_SEQ_NO"),
    regexp_replace(col("ATLD_ID"), "[\r\n\t]", "").alias("ATLD_ID"),
    regexp_replace(col("ATLT_FOLLOWUP_IND"), "[\r\n\t]", "").alias("ATLT_FOLLOWUP_IND"),
    regexp_replace(col("ATLT_FROM_NAME"), "[\r\n\t]", "").alias("ATLT_FROM_NAME"),
    col("ATLT_MAILED_DT").alias("ATLT_MAILED_DT"),
    regexp_replace(col("ATLT_MAILED_IND"), "[\r\n\t]", "").alias("ATLT_MAILED_IND"),
    col("ATLT_PRINTED_DT").alias("ATLT_PRINTED_DT"),
    regexp_replace(col("ATLT_PRINTED_IND"), "[\r\n\t]", "").alias("ATLT_PRINTED_IND"),
    regexp_replace(col("ATLT_RE_NAME"), "[\r\n\t]", "").alias("ATLT_RE_NAME"),
    regexp_replace(col("ATLT_REPRINT_STATUS"), "[\r\n\t]", "").alias("ATLT_REPRINT_STATUS"),
    col("ATLT_REQUEST_DT").alias("ATLT_REQUEST_DT"),
    regexp_replace(col("ATLT_REQUEST_IND"), "[\r\n\t]", "").alias("ATLT_REQUEST_IND"),
    col("ATLT_RECEIVED_DT").alias("ATLT_RECEIVED_DT"),
    regexp_replace(col("ATLT_RECEIVED_IND"), "[\r\n\t]", "").alias("ATLT_RECEIVED_IND"),
    col("ATLT_SUBMITTED_DT").alias("ATLT_SUBMITTED_DT"),
    regexp_replace(col("ATLT_SUBMITTED_IND"), "[\r\n\t]", "").alias("ATLT_SUBMITTED_IND"),
    regexp_replace(col("ATLT_SUBJECT"), "[\r\n\t]", "").alias("ATLT_SUBJECT"),
    regexp_replace(col("ATLT_TO_NAME"), "[\r\n\t]", "").alias("ATLT_TO_NAME")
)

# BusinessRules Transformer (two lookup links)
df_businessRules = (
    df_strip
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), col("CLAIM_ID") == col("nasco_dup_lkup.CLM_ID"), "left")
    .join(df_fcts_reversals.alias("fcts_reversals"), col("CLAIM_ID") == col("fcts_reversals.CLCL_ID"), "left")
)

windowSpec = Window.partitionBy().orderBy(col("CLAIM_ID"))
df_businessRules = df_businessRules.withColumn("SeqNo", row_number().over(windowSpec))

# ClmLetter output link
df_ClmLetter = df_businessRules.filter(col("nasco_dup_lkup.CLM_ID").isNull())
df_ClmLetter_out = df_ClmLetter.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SrcSysCd"),
    concat(lit(SrcSysCd), lit(";"), spark_trim(col("CLAIM_ID")), lit(";"), col("SeqNo")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_LTR_SK"),
    spark_trim(col("CLAIM_ID")).alias("CLM_ID"),
    col("SeqNo").alias("CLM_LTR_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    when(col("ATXR_LAST_UPD_USUS").isNull() | (spark_trim(col("ATXR_LAST_UPD_USUS")) == ""), lit("NA"))
      .otherwise(spark_trim(col("ATXR_LAST_UPD_USUS"))).alias("LAST_UPDT_USER_ID"),
    when(col("ATLT_REPRINT_STATUS").isNull() | (spark_trim(col("ATLT_REPRINT_STATUS")) == ""), lit("NA"))
      .otherwise(upper(spark_trim(col("ATLT_REPRINT_STATUS")))).alias("CLM_LTR_REPRT_STTUS_CD"),
    when(col("ATLD_ID").isNull() | (spark_trim(col("ATLD_ID")) == ""), lit("NA"))
      .otherwise(upper(spark_trim(col("ATLD_ID")))).alias("CLM_LTR_TYP_CD"),
    when(col("ATLT_FOLLOWUP_IND").isNull() | (spark_trim(col("ATLT_FOLLOWUP_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_FOLLOWUP_IND")))).alias("FLW_UP_IN"),
    when(col("ATLT_MAILED_IND").isNull() | (spark_trim(col("ATLT_MAILED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_MAILED_IND")))).alias("MAIL_IN"),
    when(col("ATLT_PRINTED_IND").isNull() | (spark_trim(col("ATLT_PRINTED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_PRINTED_IND")))).alias("PRT_IN"),
    when(col("ATLT_RECEIVED_IND").isNull() | (spark_trim(col("ATLT_RECEIVED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_RECEIVED_IND")))).alias("RCV_IN"),
    when(col("ATLT_REQUEST_IND").isNull() | (spark_trim(col("ATLT_REQUEST_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_REQUEST_IND")))).alias("RQST_IN"),
    when(col("ATLT_SUBMITTED_IND").isNull() | (spark_trim(col("ATLT_SUBMITTED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_SUBMITTED_IND")))).alias("SUBMT_IN"),
    when(col("ATXR_LAST_UPD_DT").isNull() | (spark_trim(col("ATXR_LAST_UPD_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATXR_LAST_UPD_DT")), 1, 10)).alias("LAST_UPDT_DT"),
    when(col("ATLT_MAILED_DT").isNull() | (spark_trim(col("ATLT_MAILED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_MAILED_DT")), 1, 10)).alias("MAIL_DT"),
    when(col("ATLT_PRINTED_DT").isNull() | (spark_trim(col("ATLT_PRINTED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_PRINTED_DT")), 1, 10)).alias("PRT_DT"),
    when(col("ATLT_RECEIVED_DT").isNull() | (spark_trim(col("ATLT_RECEIVED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_RECEIVED_DT")), 1, 10)).alias("RCV_DT"),
    when(col("ATLT_REQUEST_DT").isNull() | (spark_trim(col("ATLT_REQUEST_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_REQUEST_DT")), 1, 10)).alias("RQST_DT"),
    when(col("ATLT_SUBMITTED_DT").isNull() | (spark_trim(col("ATLT_SUBMITTED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_SUBMITTED_DT")), 1, 10)).alias("SUBMT_DT"),
    regexp_replace(rtrim(regexp_replace(spark_trim(col("ATLT_FROM_NAME")), '"', '')), ',$', '').alias("FROM_NM"),
    col("ATXR_DESC").alias("LTR_DESC"),
    regexp_replace(rtrim(regexp_replace(spark_trim(col("ATLT_RE_NAME")), '"', '')), ',$', '').alias("REF_NM"),
    regexp_replace(col("ATLT_SUBJECT"), '[\r\n\t]', '').alias("SUBJ_TX"),
    regexp_replace(rtrim(regexp_replace(spark_trim(col("ATLT_TO_NAME")), '"', '')), ',$', '').alias("TO_NM")
)

# reversals output link
df_Reversals = df_businessRules.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89", "91", "99"))
)
df_Reversals_out = df_Reversals.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat(lit("FACETS"), lit(";"), spark_trim(col("CLAIM_ID")), lit("R;"), col("SeqNo")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_LTR_SK"),
    concat(spark_trim(col("CLAIM_ID")), lit("R")).alias("CLM_ID"),
    col("SeqNo").alias("CLM_LTR_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    when(col("ATXR_LAST_UPD_USUS").isNull() | (spark_trim(col("ATXR_LAST_UPD_USUS")) == ""), lit("NA"))
      .otherwise(spark_trim(col("ATXR_LAST_UPD_USUS"))).alias("LAST_UPDT_USER_ID"),
    when(col("ATLT_REPRINT_STATUS").isNull() | (spark_trim(col("ATLT_REPRINT_STATUS")) == ""), lit("NA"))
      .otherwise(upper(spark_trim(col("ATLT_REPRINT_STATUS")))).alias("CLM_LTR_REPRT_STTUS_CD"),
    when(col("ATLD_ID").isNull() | (spark_trim(col("ATLD_ID")) == ""), lit("NA"))
      .otherwise(upper(spark_trim(col("ATLD_ID")))).alias("CLM_LTR_TYP_CD"),
    when(col("ATLT_FOLLOWUP_IND").isNull() | (spark_trim(col("ATLT_FOLLOWUP_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_FOLLOWUP_IND")))).alias("FLW_UP_IN"),
    when(col("ATLT_MAILED_IND").isNull() | (spark_trim(col("ATLT_MAILED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_MAILED_IND")))).alias("MAIL_IN"),
    when(col("ATLT_PRINTED_IND").isNull() | (spark_trim(col("ATLT_PRINTED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_PRINTED_IND")))).alias("PRT_IN"),
    when(col("ATLT_RECEIVED_IND").isNull() | (spark_trim(col("ATLT_RECEIVED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_RECEIVED_IND")))).alias("RCV_IN"),
    when(col("ATLT_REQUEST_IND").isNull() | (spark_trim(col("ATLT_REQUEST_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_REQUEST_IND")))).alias("RQST_IN"),
    when(col("ATLT_SUBMITTED_IND").isNull() | (spark_trim(col("ATLT_SUBMITTED_IND")) == ""), lit("X"))
      .otherwise(upper(spark_trim(col("ATLT_SUBMITTED_IND")))).alias("SUBMT_IN"),
    when(col("ATXR_LAST_UPD_DT").isNull() | (spark_trim(col("ATXR_LAST_UPD_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATXR_LAST_UPD_DT")), 1, 10)).alias("LAST_UPDT_DT"),
    when(col("ATLT_MAILED_DT").isNull() | (spark_trim(col("ATLT_MAILED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_MAILED_DT")), 1, 10)).alias("MAIL_DT"),
    when(col("ATLT_PRINTED_DT").isNull() | (spark_trim(col("ATLT_PRINTED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_PRINTED_DT")), 1, 10)).alias("PRT_DT"),
    when(col("ATLT_RECEIVED_DT").isNull() | (spark_trim(col("ATLT_RECEIVED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_RECEIVED_DT")), 1, 10)).alias("RCV_DT"),
    when(col("ATLT_REQUEST_DT").isNull() | (spark_trim(col("ATLT_REQUEST_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_REQUEST_DT")), 1, 10)).alias("RQST_DT"),
    when(col("ATLT_SUBMITTED_DT").isNull() | (spark_trim(col("ATLT_SUBMITTED_DT")) == ""), lit("UNK"))
      .otherwise(substring(spark_trim(col("ATLT_SUBMITTED_DT")), 1, 10)).alias("SUBMT_DT"),
    regexp_replace(rtrim(regexp_replace(spark_trim(col("ATLT_FROM_NAME")), '"', '')), ',$', '').alias("FROM_NM"),
    col("ATXR_DESC").alias("LTR_DESC"),
    regexp_replace(rtrim(regexp_replace(spark_trim(col("ATLT_RE_NAME")), '"', '')), ',$', '').alias("REF_NM"),
    regexp_replace(col("ATLT_SUBJECT"), '[\r\n\t]', '').alias("SUBJ_TX"),
    regexp_replace(rtrim(regexp_replace(spark_trim(col("ATLT_TO_NAME")), '"', '')), ',$', '').alias("TO_NM")
)

# Collector
commonCols = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT",
    "ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","CLM_LTR_SK","CLM_ID","CLM_LTR_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_SK","LAST_UPDT_USER_ID",
    "CLM_LTR_REPRT_STTUS_CD","CLM_LTR_TYP_CD","FLW_UP_IN","MAIL_IN","PRT_IN","RCV_IN","RQST_IN",
    "SUBMT_IN","LAST_UPDT_DT","MAIL_DT","PRT_DT","RCV_DT","RQST_DT","SUBMT_DT","FROM_NM","LTR_DESC",
    "REF_NM","SUBJ_TX","TO_NM"
]
df_reversals_sel = df_Reversals_out.select([col(c) for c in commonCols])
df_clmletter_sel = df_ClmLetter_out.select([col(c) for c in commonCols])
df_collector = df_reversals_sel.unionByName(df_clmletter_sel)

# Snapshot outputs: AllCol, Transform, Snapshot
df_allCol = df_collector.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LTR_SEQ_NO"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LTR_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("LAST_UPDT_USER_ID"),
    col("CLM_LTR_REPRT_STTUS_CD"),
    col("CLM_LTR_TYP_CD"),
    col("FLW_UP_IN"),
    col("MAIL_IN"),
    col("PRT_IN"),
    col("RCV_IN"),
    col("RQST_IN"),
    col("SUBMT_IN"),
    col("LAST_UPDT_DT"),
    col("MAIL_DT"),
    col("PRT_DT"),
    col("RCV_DT"),
    col("RQST_DT"),
    col("SUBMT_DT"),
    col("FROM_NM"),
    col("LTR_DESC"),
    col("REF_NM"),
    col("SUBJ_TX"),
    col("TO_NM")
)

df_transform = df_collector.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LTR_SEQ_NO")
)

df_snapshot = df_collector.select(
    col("CLM_ID"),
    col("CLM_LTR_SEQ_NO"),
    col("CLM_LTR_TYP_CD")
)

# Transform stage after Snapshot
df_transform_2 = df_snapshot.withColumn(
    "CLM_LTR_TYP_CD_SK",
    GetFkeyCodes('FACETS', 0, "CLAIM LETTER TYPE", col("CLM_LTR_TYP_CD"), "X")
)

df_RowCount = df_transform_2.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LTR_SEQ_NO"),
    col("CLM_LTR_TYP_CD_SK")
)

# Final write to B_CLM_LTR (SeqFile)
df_B_CLM_LTR = df_RowCount.withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " "))
write_files(
    df_B_CLM_LTR.select("SRC_SYS_CD_SK","CLM_ID","CLM_LTR_SEQ_NO","CLM_LTR_TYP_CD_SK"),
    f"{adls_path}/load/B_CLM_LTR.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmLtrPK
# COMMAND ----------

params_clmltrpk = {
    "DriverTable": "#DriverTable#",
    "CurrRunCycle": "#CurrRunCycle#",
    "$FacetsDB": "#$FacetsDB#",
    "$FacetsOwner": "#$FacetsOwner#",
    "RunID": "#RunID#",
    "CurrentDate": "#CurrentDate#",
    "$IDSOwner": "#$IDSOwner#"
}

df_key = ClmLtrPK(df_allCol, df_transform, params_clmltrpk)

# Final write to IdsClmLetterPkey (SeqFile)
df_key_final = (
    df_key
    .withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " "))
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("LAST_UPDT_USER_ID", rpad(col("LAST_UPDT_USER_ID"), 10, " "))
    .withColumn("CLM_LTR_REPRT_STTUS_CD", rpad(col("CLM_LTR_REPRT_STTUS_CD"), 10, " "))
    .withColumn("CLM_LTR_TYP_CD", rpad(col("CLM_LTR_TYP_CD"), 10, " "))
    .withColumn("FLW_UP_IN", rpad(col("FLW_UP_IN"), 1, " "))
    .withColumn("MAIL_IN", rpad(col("MAIL_IN"), 1, " "))
    .withColumn("PRT_IN", rpad(col("PRT_IN"), 1, " "))
    .withColumn("RCV_IN", rpad(col("RCV_IN"), 1, " "))
    .withColumn("RQST_IN", rpad(col("RQST_IN"), 1, " "))
    .withColumn("SUBMT_IN", rpad(col("SUBMT_IN"), 1, " "))
    .withColumn("LAST_UPDT_DT", rpad(col("LAST_UPDT_DT"), 10, " "))
    .withColumn("MAIL_DT", rpad(col("MAIL_DT"), 10, " "))
    .withColumn("PRT_DT", rpad(col("PRT_DT"), 10, " "))
    .withColumn("RCV_DT", rpad(col("RCV_DT"), 10, " "))
    .withColumn("RQST_DT", rpad(col("RQST_DT"), 10, " "))
    .withColumn("SUBMT_DT", rpad(col("SUBMT_DT"), 10, " "))
)

write_files(
    df_key_final.select(df_key.columns),
    f"{adls_path}/key/FctsClmLetterExtr.FctsClmLtr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)