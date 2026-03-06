# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: FctsCustSvcExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  The Extraction Process for Source 2 involves decoding a XML text document which is done in the business logic.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #              Change Description                                                                              Development Project       Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------     ---------------------------------------------------------                                                     ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               2/8/2007      3028                                Originally Programmed                                                                          devlIDS30                      Steph Goddard             02/21/2007
# MAGIC Parik                                06/22/2007    3264                               Added balancing process to the overall job                                           devlIDS30                      Steph Goddard             09/14/2007
# MAGIC Ralph Tucker                 12/28/2007     15                                   Added Hit List Processing                                                                     devlIDS30                      Steph Goddard             01/09/2008
# MAGIC Ralph Tucker                 1/15/2008       15                                   Changed driver table name                                                                    devlIDS                          Steph Goddard             01/17/2008
# MAGIC Brent Leland                   02/27/2008     3567 Primary Key           Changed to use new primary key process                                              devlIDScur                     Steph Goddard             05/06/2008       
# MAGIC                                                                                                       Removed timestamp on end of driver table.
# MAGIC                                                                                                       Removed 3 hash files from after job clearing
# MAGIC Steph Goddard              03/18/2009     Prod Supp                       Changed hash clear to task_note_ln_allcol                                           devlIDS                           SAndrew                      03/18/2009
# MAGIC Steph Goddard              06/15/2010     4022 - Facets 4.7.1        changed metadata for Facets input                                                       IntegrateNewDevl            SAndrew                     2010-06-16
# MAGIC Sandrew                        06/16/2010                                            renamed hf_cscf_type_lkup to hf_cust_svc_tln_cscf_type_lkup
# MAGIC 
# MAGIC Bhoomi Dasari               11/11/2011      TTR-1043                      Renamed hash files                                                                                IntegrateWrhsDevl             SAndrew                     2011-11-16
# MAGIC                                                                                                      1) hf_transf1_note_ln --> hf_cust_svc_tnl_transf1_note_ln
# MAGIC                                                                                                      2) hf_transf2_note_ln -->hf_cust_svc_tnl_transf2_note_ln
# MAGIC Prabhu ES                     2022-03-01      S2S Remediation           MSSQL connection parameters added                                                    IntegrateDev5                 Kalyan Neelam                2022-06-09
# MAGIC 
# MAGIC 
# MAGIC Deepika C                     2025-03-13      US 644529                    Updated stage variable svLastDTM in Trans1 stage                                IntegrateDev2                 Jeyaprasanna              2025-03-17

# MAGIC Writing Sequential File to ../key
# MAGIC Assign Primary Key
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Customer Service Task Note Line Data
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskNoteLnPK
# COMMAND ----------

# FACETS ODBCConnector
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

df_FACETS_Extract2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
SELECT
 task.CSSC_ID,
 task.CSTK_SEQ_NO,
 conf.CSCF_SEQ_NO as CSCF_LN_SEQ,
 noteln.CSCF_SEQ_NO,
 noteln.STRT_SEQ_NO,
 convert(varchar(255), conf.CSCF_TEXT) as CSCF_TEXT
FROM
 {FacetsOwner}.CMC_CSTK_TASK task,
 {FacetsOwner}.CMC_CSCF_CONF conf,
 tempdb..TMP_IDS_CUST_SVC_DRVR DRVR,
 tempdb..TMP_CUST_SVC_TASK_NOTE_LN noteln
WHERE
 task.CSSC_ID=conf.CSSC_ID
 AND task.CSTK_SEQ_NO=conf.CSTK_SEQ_NO
 AND conf.CSSC_ID=noteln.CSSC_ID
 AND conf.CSTK_SEQ_NO=noteln.CSTK_SEQ_NO
 AND conf.CSCF_SEQ_NO>=noteln.STRT_SEQ_NO
 AND conf.CSCF_SEQ_NO<=noteln.END_SEQ_NO
 AND task.CSSC_ID = DRVR.CSSC_ID
 AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
 AND conf.CSCF_TYPE='T'
"""
    )
    .load()
)

df_FACETS_Type1Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
SELECT
 noteln.CSSC_ID,
 noteln.CSTK_SEQ_NO,
 noteln.CSCF_SEQ_NO,
 convert(varchar(255), noteln.CSCF_TEXT) as CSCF_TEXT
FROM
 tempdb..TMP_CUST_SVC_TASK_NOTE_LN noteln,
 {FacetsOwner}.CMC_CSTK_TASK task,
 tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
 noteln.CSSC_ID=task.CSSC_ID
 AND noteln.CSTK_SEQ_NO=task.CSTK_SEQ_NO
 AND task.CSSC_ID = DRVR.CSSC_ID
 AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""
    )
    .load()
)

# hf_cust_svc_tln_cscf_type_lkup (Scenario A: deduplicate on keys CSSC_ID, CSTK_SEQ_NO, CSCF_SEQ_NO)
df_hf_cust_svc_tln_cscf_type_lkup = df_FACETS_Type1Extr.dropDuplicates(["CSSC_ID","CSTK_SEQ_NO","CSCF_SEQ_NO"])

# StripFields2 Transformer (primary link: Extract2, lookup link: Type1Lkup)
df_StripFields2_pre = (
    df_FACETS_Extract2.alias("Extract2")
    .join(
        df_hf_cust_svc_tln_cscf_type_lkup.alias("Type1Lkup"),
        (
            (F.col("Extract2.CSSC_ID") == F.col("Type1Lkup.CSSC_ID"))
            & (F.col("Extract2.CSTK_SEQ_NO") == F.col("Type1Lkup.CSTK_SEQ_NO"))
            & (F.col("Extract2.CSCF_SEQ_NO") == F.col("Type1Lkup.CSCF_SEQ_NO"))
        ),
        "left"
    )
)

df_StripFields2_pre = df_StripFields2_pre.withColumn(
    "svText",
    F.when(F.col("Type1Lkup.CSCF_TEXT").isNull(), F.lit(" ")).otherwise(F.col("Type1Lkup.CSCF_TEXT"))
)

df_StripFields2 = df_StripFields2_pre.select(
    F.upper(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("Extract2.CSSC_ID"), "\n", ""), "\r", ""
            ),
            "\t", ""
        )
    ).alias("CSSC_ID"),
    F.col("Extract2.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("Extract2.CSCF_LN_SEQ").alias("CSCF_LN_SEQ"),
    F.col("Extract2.CSCF_SEQ_NO").alias("CSCF_SEQ_NO"),
    F.col("Extract2.STRT_SEQ_NO").alias("STRT_SEQ_NO"),
    F.upper(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("svText"), "\n", ""), "\r", ""
            ),
            "\t", ""
        )
    ).alias("CSCF_TEXT")
)

# BusinessRules2 Transformer
df_BusinessRules2_pre = df_StripFields2.withColumn("RowPassThru", F.lit("Y")).withColumn("svSrcSysCd", F.lit("FACETS"))
df_BusinessRules2_pre = df_BusinessRules2_pre.withColumn("svCsscId", trim(F.col("CSSC_ID")))

df_BusinessRules2_pre = df_BusinessRules2_pre.withColumn(
    "svLastUpdt",
    F.expr(
        """
substring(
  CSCF_TEXT,
  locate('<CSTN_LAST_UPD_DTM>', CSCF_TEXT) + length('<CSTN_LAST_UPD_DTM>'),
  locate('</CSTN_LAST_UPD_DTM>', CSCF_TEXT) 
    - locate('<CSTN_LAST_UPD_DTM>', CSCF_TEXT)
    - length('<CSTN_LAST_UPD_DTM>')
)
"""
    )
)

df_BusinessRules2 = df_BusinessRules2_pre.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(
        F.col("svSrcSysCd"),
        F.lit(";"),
        F.col("svCsscId"),
        F.lit(";"),
        F.col("CSTK_SEQ_NO"),
        F.lit(";"),
        F.lit("CSCF"),
        F.lit(";"),
        F.col("CSCF_SEQ_NO"),
        F.lit(";"),
        F.col("svLastUpdt"),
        F.lit(";"),
        F.col("CSCF_LN_SEQ")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_NOTE_LN_SK"),
    F.col("svCsscId").alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.lit("CSCF").alias("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("CSCF_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.when(
        F.trim(F.col("svLastUpdt")).substr(F.lit(1),F.lit(1)).isin("J","F","M","A","S","O","N","D"),
        FORMAT_DATE(F.trim(F.col("svLastUpdt")),"ACCESS","DATESTRING","DB2TIMESTAMP")
    ).otherwise(
        FORMAT_DATE(F.trim(F.col("svLastUpdt")),"ACCESS","TIMESTAMP","DB2TIMESTAMP")
    ).alias("LAST_UPDT_DTM"),
    (F.col("CSCF_LN_SEQ") - F.col("STRT_SEQ_NO")).alias("LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CUST_SVC_TASK_NOTE_SK"),
    trim(F.col("CSCF_TEXT")).alias("LN_TX")
)

# hf_cust_svc_tnl_transf2_note_ln (Scenario A deduplicate on PRI_KEY_STRING)
df_hf_cust_svc_tnl_transf2_note_ln = df_BusinessRules2.dropDuplicates(["PRI_KEY_STRING"])

# CER_ATXR_ATTACH_U ODBCConnector
df_CER_ATXR_ATTACH_U = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
SELECT
 task.CSSC_ID,
 task.CSTK_SEQ_NO,
 noted.ATNT_SEQ_NO,
 notec.ATND_SEQ_NO,
 convert(varchar(100), notec.ATND_TEXT) as ATND_TEXT,
 attach.ATXR_LAST_UPD_DT
FROM
 {FacetsOwner}.CMC_CSTK_TASK task,
 {FacetsOwner}.CER_ATXR_ATTACH_U attach,
 {FacetsOwner}.CER_ATNT_NOTE_D noted,
 {FacetsOwner}.CER_ATND_NOTE_C notec,
 tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
 task.ATXR_SOURCE_ID=attach.ATXR_SOURCE_ID
 AND attach.ATSY_ID=noted.ATSY_ID
 AND attach.ATXR_DEST_ID=noted.ATXR_DEST_ID
 AND noted.ATSY_ID=notec.ATSY_ID
 AND noted.ATXR_DEST_ID=notec.ATXR_DEST_ID
 AND noted.ATNT_SEQ_NO=notec.ATNT_SEQ_NO
 AND task.CSSC_ID = DRVR.CSSC_ID
 AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
 AND attach.ATSY_ID='ATN0'
"""
    )
    .load()
)

# StripFields1 Transformer
df_StripFields1 = df_CER_ATXR_ATTACH_U.select(
    F.upper(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("CSSC_ID"), "\n", ""), "\r", ""
            ),
            "\t", ""
        )
    ).alias("CSSC_ID"),
    F.col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("ATNT_SEQ_NO").alias("ATNT_SEQ_NO"),
    F.col("ATND_SEQ_NO").alias("ATND_SEQ_NO"),
    F.upper(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("ATND_TEXT"), "\n", ""), "\r", ""
            ),
            "\t", ""
        )
    ).alias("ATND_TEXT"),
    F.col("ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT")
)

# BusinessRules1 Transformer
df_BusinessRules1_pre = df_StripFields1.withColumn("RowPassThru", F.lit("Y")).withColumn("svSrcSysCd", F.lit("FACETS"))
df_BusinessRules1_pre = df_BusinessRules1_pre.withColumn("svCsscId", trim(F.col("CSSC_ID")))

df_BusinessRules1 = df_BusinessRules1_pre.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(
        F.col("svSrcSysCd"),
        F.lit(";"),
        F.col("svCsscId"),
        F.lit(";"),
        F.col("CSTK_SEQ_NO"),
        F.lit(";"),
        F.lit("ATNT"),
        F.lit(";"),
        F.col("ATNT_SEQ_NO"),
        F.lit(";"),
        F.col("ATXR_LAST_UPD_DT"),
        F.lit(";"),
        F.col("ATND_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_NOTE_LN_SK"),
    F.col("svCsscId").alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.lit("ATNT").alias("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("ATNT_SEQ_NO").alias("NOTE_SEQ_NO"),
    FORMAT_DATE(F.col("ATXR_LAST_UPD_DT"),"SYBASE","TIMESTAMP","DB2TIMESTAMP").alias("LAST_UPDT_DTM"),
    F.col("ATND_SEQ_NO").alias("LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CUST_SVC_TASK_NOTE_SK"),
    trim(F.col("ATND_TEXT")).alias("LN_TX")
)

# hf_cust_svc_tnl_transf1_note_ln (Scenario A deduplicate on PRI_KEY_STRING)
df_hf_cust_svc_tnl_transf1_note_ln = df_BusinessRules1.dropDuplicates(["PRI_KEY_STRING"])

# Link_Collector (Round-Robin collector): union of the two hashed file outputs
df_link_collector = df_hf_cust_svc_tnl_transf1_note_ln.unionByName(df_hf_cust_svc_tnl_transf2_note_ln)

# Trans1 Transformer
df_Trans1_pre = df_link_collector.alias("Combine").withColumn("RowPassThru", F.lit("Y"))
df_Trans1_pre = df_Trans1_pre.withColumn(
    "svLastDTM",
    F.concat_ws(
        "",
        F.col("Combine.LAST_UPDT_DTM").substr(F.lit(1),F.lit(10)),
        F.lit(" "),
        F.col("Combine.LAST_UPDT_DTM").substr(F.lit(12),F.lit(2)),
        F.lit(":"),
        F.col("Combine.LAST_UPDT_DTM").substr(F.lit(15),F.lit(2)),
        F.lit(":"),
        F.col("Combine.LAST_UPDT_DTM").substr(F.lit(18),F.lit(2)),
        F.lit("."),
        F.substring(
            F.concat(
                F.col("Combine.LAST_UPDT_DTM").substr(F.lit(21),F.lit(6)),
                F.lit("000000")
            ),1,6
        )
    )
)

# Output pin "AllCol"
df_Trans1_AllCol = df_Trans1_pre.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Combine.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Combine.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("Combine.CUST_SVC_TASK_NOTE_LOC_CD").alias("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("Combine.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.col("svLastDTM").alias("LAST_UPDT_DTM"),
    F.col("Combine.LN_SEQ_NO").alias("LN_SEQ_NO"),
    F.col("Combine.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Combine.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Combine.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Combine.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Combine.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Combine.ERR_CT").alias("ERR_CT"),
    F.col("Combine.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Combine.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Combine.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Combine.CUST_SVC_TASK_NOTE_LN_SK").alias("CUST_SVC_TASK_NOTE_LN_SK"),
    F.col("Combine.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Combine.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Combine.CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
    F.col("Combine.LN_TX").alias("LN_TX")
)

# Output pin "Snapshot"
df_Trans1_Snapshot = df_Trans1_pre.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Combine.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Combine.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    GetFkeyCodes("FACETS",F.lit(1),"CUSTOMER SERVICE TASK NOTE SOURCE",F.col("Combine.CUST_SVC_TASK_NOTE_LOC_CD"),"X").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
    F.col("Combine.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.col("svLastDTM").alias("LAST_UPDT_DTM"),
    F.col("Combine.LN_SEQ_NO").alias("LN_SEQ_NO")
)

# Output pin "Transform"
df_Trans1_Transform = df_Trans1_pre.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Combine.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Combine.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("Combine.CUST_SVC_TASK_NOTE_LOC_CD").alias("CUST_SVC_TASK_NOTE_LOC_CD"),
    F.col("Combine.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.col("svLastDTM").alias("LAST_UPDT_DTM"),
    F.col("Combine.LN_SEQ_NO").alias("LN_SEQ_NO")
)

# B_CUST_SVC_TASK_NOTE_LN (CSeqFileStage) writing df_Trans1_Snapshot
# Final select with rpad if char or varchar. The design does not specify lengths for all columns here.
# For demonstration, only rpad the columns that had explicit lengths in the job approach (none shown for the snapshot).
df_Trans1_Snapshot_out = df_Trans1_Snapshot.select(
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD_SK",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "LN_SEQ_NO"
)

write_files(
    df_Trans1_Snapshot_out,
    f"{adls_path}/load/B_CUST_SVC_TASK_NOTE_LN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CustSvcTaskNoteLnPK (Shared Container)
params_CustSvcTaskNoteLnPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "IDSOwner": IDSOwner
}
df_CustSvcTaskNoteLnPK_Key = CustSvcTaskNoteLnPK(df_Trans1_AllCol, df_Trans1_Transform, params_CustSvcTaskNoteLnPK)

# IdsCustSvcTaskNoteLnExtr (CSeqFileStage)
# The link "Key" columns (final file). Do rpad for char/varchar columns that have declared lengths:
# INSRT_UPDT_CD char(10), DISCARD_IN char(1), PASS_THRU_IN char(1).
df_final = (
    df_CustSvcTaskNoteLnPK_Key
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CUST_SVC_TASK_NOTE_LN_SK"),
        F.col("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO"),
        F.col("CUST_SVC_TASK_NOTE_LOC_CD"),
        F.col("NOTE_SEQ_NO"),
        F.col("LAST_UPDT_DTM"),
        F.col("LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_TASK_NOTE_SK"),
        F.col("LN_TX")
    )
)

write_files(
    df_final,
    f"{adls_path}/key/IdsCustSvcTaskNoteLnExtr.CSTaskNoteLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)