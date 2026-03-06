# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2007, 2008, 2014 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsCustSvcExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract Customer Service data from Facets into CRF file
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                    Project/                                                                                                                            Code                   Date
# MAGIC Developer             Date              Altiris #          Change Description                                                                                     Reviewer            Reviewed
# MAGIC ---------------------------  -------------------   ------------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Parikshith Chada  2/2/2007       3028             Originally Programmed                                                                                 Steph Goddard   02/21/2007
# MAGIC Parik                     06/22/2007   3264             Added balancing process to the overall job                                                Steph Goddard   09/14/2007                             
# MAGIC Ralph Tucker       12/28/2007   15                 Added Hit List Processing                                                                           Steph Goddard   01/09/2008
# MAGIC Ralph Tucker       1/15/2008     15                 Changed driver table name                                                                         Steph Goddard   01/17/2008
# MAGIC Brent Leland         02/27/2008   3567 PKey   Changed to use new primary key process                                                   Steph Goddard   05/06/2008
# MAGIC Steph Goddard     06/20/2010   4022            changed field to VarBinary and converted                           
# MAGIC Manasa Andru      2013-11-25    TFS1432      Changed the datatype from SmallInt to Integer                                            Kalyan Neelam   2013-11-26
# MAGIC                                                                         for fields NOTE_SEQ_NO and TASK_SEQ_NO
# MAGIC Hugh Sisson         2014-06-16    TFS4126      Changed key file column from LAST_UPDT_USER_SK Integer to             Kalyan Neelam   2014-06-23
# MAGIC                                                                         LAST_UPDT_USER VarChar(255)
# MAGIC Prabhu ES            2022-03-01    S2S              MSSQL connection parameters added    
# MAGIC 
# MAGIC Ravi Ranjan         2023-02-09    #576629      Applied logic to convert the Non- Breaking Space to SPACE                     Goutham Kalidindi    20230-02-10
# MAGIC                                                                        and convert en dash and em dash to regular dash in db source stages                                          
# MAGIC 
# MAGIC Deepika C           2025-03-13    US 644529   Updated stage variable svLastDTM in Trans1 stage                                     Jeyaprasanna       2025-03-17

# MAGIC Balancing snapshot of source table
# MAGIC Assign Primary Key
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Customer Service Data
# MAGIC Apply business logic
# MAGIC Writing Sequential File to /pkey
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, NumericType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_CMC_CSCF_CONF = f"""
SELECT
     task.CSSC_ID,
     task.CSTK_SEQ_NO,
     REPLACE(REPLACE(REPLACE(Trim(task.CSTK_SUMMARY), NCHAR(8212), '-') , NCHAR(8211), '-'),CHAR(160),' ') as CSTK_SUMMARY,
     conf.CSCF_SEQ_NO,
     convert(varchar(255), conf.CSCF_TEXT) as CSCF_TEXT
FROM
     {FacetsOwner}.CMC_CSTK_TASK task,
     {FacetsOwner}.CMC_CSCF_CONF conf,
     tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE task.CSSC_ID = conf.CSSC_ID
  AND task.CSTK_SEQ_NO = conf.CSTK_SEQ_NO
  AND task.CSSC_ID = DRVR.CSSC_ID
  AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
  AND conf.CSCF_TYPE='1'
"""

df_CMC_CSCF_CONF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CSCF_CONF)
    .load()
)

df_StripFields2 = df_CMC_CSCF_CONF.select(
    F.regexp_replace(F.upper(F.col("CSSC_ID")), "[\\r\\n\\t]", "").alias("CSSC_ID"),
    F.col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("CSCF_SEQ_NO").alias("CSCF_SEQ_NO"),
    F.regexp_replace(F.col("CSCF_TEXT"), "[\\r\\n\\t]", "").alias("CSCF_TEXT"),
    F.regexp_replace(F.upper(F.col("CSTK_SUMMARY")), "[\\r\\n\\t]", "").alias("CSTK_SUMMARY")
)

df_BusinessRules2_intermediate = df_StripFields2.withColumn("RowPassThru", F.lit("Y"))\
.withColumn("svSrcSysCd", F.lit("FACETS"))\
.withColumn("svCsscId", trim(F.col("CSSC_ID")))

df_BusinessRules2_intermediate = df_BusinessRules2_intermediate.withColumn(
    "svLastUpdt",
    F.expr(
        "substring(CSCF_TEXT, instr(CSCF_TEXT, '<CSTN_LAST_UPD_DTM>') + length('<CSTN_LAST_UPD_DTM>'), instr(CSCF_TEXT, '</CSTN_LAST_UPD_DTM>') - instr(CSCF_TEXT, '<CSTN_LAST_UPD_DTM>') - length('<CSTN_LAST_UPD_DTM>'))"
    )
)

df_BusinessRules2 = df_BusinessRules2_intermediate.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD_placeholder_for_FIELDDROP"),
    F.col("svCsscId").alias("CUST_SVC_ID_placeholder_for_FIELDDROP"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO_placeholder_for_FIELDDROP"),
    F.col("CSCF_SEQ_NO").alias("NOTE_SEQ_NO_placeholder_for_FIELDDROP"),
    F.trim(F.col("svLastUpdt")).alias("temp_svLastUpdt"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    (F.col("svSrcSysCd") + F.lit(";") + F.col("svCsscId") + F.lit(";")
     + F.col("CSTK_SEQ_NO").cast("string") + F.lit(";") + F.lit("CSCF") + F.lit(";")
     + F.col("CSCF_SEQ_NO").cast("string")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_NOTE_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    F.expr(
        "substring(CSCF_TEXT, instr(CSCF_TEXT, '<CSTN_LAST_UPD_USID>') + length('<CSTN_LAST_UPD_USID>'), instr(CSCF_TEXT, '</CSTN_LAST_UPD_USID>') - instr(CSCF_TEXT, '<CSTN_LAST_UPD_USID>') - length('<CSTN_LAST_UPD_USID>'))"
    ).alias("LAST_UPDT_USER_placeholder_for_FIELDDROP"),
    F.col("CSTK_SUMMARY").alias("NOTE_SUM_TX_placeholder_for_FIELDDROP")
)

df_BusinessRules2 = df_BusinessRules2.withColumn(
    "FIRST_RECYC_DT",
    current_date()
).withColumn(
    "SRC_SYS_CD",
    F.col("SRC_SYS_CD_placeholder_for_FIELDDROP")
).withColumn(
    "CUST_SVC_ID",
    F.col("CUST_SVC_ID_placeholder_for_FIELDDROP")
).withColumn(
    "TASK_SEQ_NO",
    F.col("TASK_SEQ_NO_placeholder_for_FIELDDROP").cast("int")
).withColumn(
    "CUST_SVC_TASK_NOTE_LOC_CD",
    F.lit("CSCF")
).withColumn(
    "NOTE_SEQ_NO",
    F.col("NOTE_SEQ_NO_placeholder_for_FIELDDROP").cast("int")
).withColumn(
    "LAST_UPDT_DTM",
    F.when(
        F.substring(F.col("temp_svLastUpdt"), 1, 1).isin(["J","F","M","A","S","O","N","D"]),
        FORMAT.DATE(F.trim(F.col("temp_svLastUpdt")), "ACCESS", "DATESTRING", "DB2TIMESTAMP")
    ).otherwise(
        FORMAT.DATE(F.trim(F.col("temp_svLastUpdt")), "ACCESS", "TIMESTAMP", "DB2TIMESTAMP")
    )
).withColumn(
    "LAST_UPDT_USER",
    F.col("LAST_UPDT_USER_placeholder_for_FIELDDROP")
).withColumn(
    "NOTE_SUM_TX",
    F.col("NOTE_SUM_TX_placeholder_for_FIELDDROP")
).select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_NOTE_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER",
    "NOTE_SUM_TX"
)

df_rule2out = df_BusinessRules2.dropDuplicates(["PRI_KEY_STRING"])

extract_query_CER_ATNT_NOTE_D = f"""
SELECT
  task.CSSC_ID,
  task.CSTK_SEQ_NO,
  noted.ATNT_SEQ_NO,
  REPLACE(REPLACE(REPLACE(Trim(attach.ATXR_DESC), NCHAR(8212), '-') , NCHAR(8211), '-'),CHAR(160),' ') as ATXR_DESC,
  attach.ATXR_LAST_UPD_DT,
  attach.ATXR_LAST_UPD_USUS
FROM
  {FacetsOwner}.CMC_CSTK_TASK task,
  {FacetsOwner}.CER_ATNT_NOTE_D noted,
  {FacetsOwner}.CER_ATXR_ATTACH_U attach,
  tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE task.ATXR_SOURCE_ID = attach.ATXR_SOURCE_ID
  AND attach.ATSY_ID = noted.ATSY_ID
  AND attach.ATXR_DEST_ID=noted.ATXR_DEST_ID
  AND attach.ATSY_ID='ATN0'
  AND task.CSSC_ID = DRVR.CSSC_ID
  AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_CER_ATNT_NOTE_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CER_ATNT_NOTE_D)
    .load()
)

df_StripFields1 = df_CER_ATNT_NOTE_D.select(
    F.regexp_replace(F.upper(F.col("CSSC_ID")), "[\\r\\n\\t]", "").alias("CSSC_ID"),
    F.col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("ATNT_SEQ_NO").alias("ATNT_SEQ_NO"),
    F.regexp_replace(F.upper(F.col("ATXR_DESC")), "[\\r\\n\\t]", "").alias("ATXR_DESC"),
    F.col("ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT"),
    F.regexp_replace(F.col("ATXR_LAST_UPD_USUS"), "[\\r\\n\\t]", "").alias("ATXR_LAST_UPD_USUS")
)

df_BusinessRules1_intermediate = df_StripFields1.withColumn("RowPassThru", F.lit("Y"))\
.withColumn("svSrcSysCd", F.lit("FACETS"))\
.withColumn("svCsscId", trim(F.col("CSSC_ID")))

df_BusinessRules1 = df_BusinessRules1_intermediate.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD_placeholder_for_FIELDDROP"),
    (F.col("svSrcSysCd") + F.lit(";") + F.col("svCsscId") + F.lit(";")
     + F.col("CSTK_SEQ_NO").cast("string") + F.lit(";") + F.lit("ATNT") + F.lit(";")
     + F.col("ATNT_SEQ_NO").cast("string") + F.lit(";")
     + F.col("ATXR_LAST_UPD_DT").cast("string")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_NOTE_SK"),
    F.col("svCsscId").alias("CUST_SVC_ID_placeholder_for_FIELDDROP"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO_placeholder_for_FIELDDROP"),
    F.lit("ATNT").alias("CUST_SVC_TASK_NOTE_LOC_CD_placeholder_for_FIELDDROP"),
    F.col("ATNT_SEQ_NO").alias("NOTE_SEQ_NO_placeholder_for_FIELDDROP"),
    FORMAT.DATE(F.col("ATXR_LAST_UPD_DT"), "SYBASE", "TIMESTAMP", "DB2Timestamp").alias("temp_LAST_UPDT_DTM"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    trim(F.col("ATXR_LAST_UPD_USUS")).alias("temp_LAST_UPDT_USER"),
    trim(F.col("ATXR_DESC")).alias("temp_NOTE_SUM_TX")
)

df_BusinessRules1 = df_BusinessRules1.withColumn(
    "SRC_SYS_CD",
    F.col("SRC_SYS_CD_placeholder_for_FIELDDROP")
).withColumn(
    "CUST_SVC_ID",
    F.col("CUST_SVC_ID_placeholder_for_FIELDDROP")
).withColumn(
    "TASK_SEQ_NO",
    F.col("TASK_SEQ_NO_placeholder_for_FIELDDROP").cast("int")
).withColumn(
    "CUST_SVC_TASK_NOTE_LOC_CD",
    F.col("CUST_SVC_TASK_NOTE_LOC_CD_placeholder_for_FIELDDROP")
).withColumn(
    "NOTE_SEQ_NO",
    F.col("NOTE_SEQ_NO_placeholder_for_FIELDDROP").cast("int")
).withColumn(
    "LAST_UPDT_DTM",
    F.col("temp_LAST_UPDT_DTM")
).withColumn(
    "LAST_UPDT_USER",
    F.col("temp_LAST_UPDT_USER")
).withColumn(
    "NOTE_SUM_TX",
    F.col("temp_NOTE_SUM_TX")
).select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_NOTE_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER",
    "NOTE_SUM_TX"
)

df_rule1out = df_BusinessRules1.dropDuplicates(["PRI_KEY_STRING"])

commonColsCollector = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_NOTE_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER",
    "NOTE_SUM_TX"
]

df_Collector = df_rule1out.select(commonColsCollector).union(df_rule2out.select(commonColsCollector))

df_noteDedup = df_Collector.dropDuplicates(["PRI_KEY_STRING"])

df_Trans1_intermediate = df_noteDedup.withColumn("RowPassThru", F.lit("Y")).withColumn(
    "svLastDTM",
    F.concat(
        F.col("LAST_UPDT_DTM").substr(F.lit(1), F.lit(10)),
        F.lit(" "),
        F.col("LAST_UPDT_DTM").substr(F.lit(12), F.lit(2)),
        F.lit(":"),
        F.col("LAST_UPDT_DTM").substr(F.lit(15), F.lit(2)),
        F.lit(":"),
        F.col("LAST_UPDT_DTM").substr(F.lit(18), F.lit(2)),
        F.lit("."),
        F.substring(F.concat(F.col("LAST_UPDT_DTM").substr(F.lit(21), F.lit(6)), F.lit("000000")), 1, 6)
    )
)

df_trans1_allcol = df_Trans1_intermediate.select(
    F.lit(SrcSysCdSk).alias("SYS_SYS_CD_SK"),
    F.col("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("NOTE_SEQ_NO"),
    F.col("svLastDTM").alias("LAST_UPDT_DTM"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CUST_SVC_TASK_NOTE_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_TASK_SK"),
    F.col("LAST_UPDT_USER"),
    F.col("NOTE_SUM_TX")
)

df_trans1_transform = df_Trans1_intermediate.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("NOTE_SEQ_NO"),
    F.col("svLastDTM").alias("LAST_UPDT_DTM")
)

df_trans1_snapshot = df_Trans1_intermediate.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    GetFkeyCodes("FACETS", F.lit(100), "CUSTOMER SERVICE TASK NOTE SOURCE", F.col("CUST_SVC_TASK_NOTE_LOC_CD"), "X").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
    F.col("NOTE_SEQ_NO"),
    F.col("svLastDTM").alias("LAST_UPDT_DTM")
)

write_files(
    df_trans1_snapshot,
    f"{adls_path}/load/B_CUST_SVC_TASK_NOTE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskNotePK
# COMMAND ----------

params_CustSvcTaskNotePK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "$IDSOwner": IDSOwner
}

df_IdsCustSvcTaskNoteExtr = CustSvcTaskNotePK(df_trans1_allcol, df_trans1_transform, params_CustSvcTaskNotePK)

final_cols_IdsCustSvcTaskNoteExtr = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_NOTE_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER",
    "NOTE_SUM_TX"
]

df_IdsCustSvcTaskNoteExtr_ordered = df_IdsCustSvcTaskNoteExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CUST_SVC_TASK_NOTE_SK"),
    F.col("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("NOTE_SEQ_NO"),
    F.col("LAST_UPDT_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_TASK_SK"),
    F.col("LAST_UPDT_USER"),
    F.col("NOTE_SUM_TX")
)

write_files(
    df_IdsCustSvcTaskNoteExtr_ordered,
    f"{adls_path}/key/IdsCustSvcTaskNoteExtr.CSTaskNote.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)