# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_CSTK_TASK,CER_ATXR_ATTACH_U and CER_ATUF_USERFLD_D for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC PROCESSING: Should be Run after CUST_SVC_TASK.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                             Project #                 Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                    ----------------                ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty    11/2006                    Initial program                                                                                                     devlIDS30                          Steph Goddard          02/12/2007
# MAGIC 
# MAGIC Parik                      06/22/2007              Added balancing process to the overall job                         3264                      devlIDS30                           Steph Goddard          09/14/2007
# MAGIC Ralph Tucker        12/28/2007              Added Hit List processing                                                     15                          devlIDS30                          Steph Goddard          01/09/2008
# MAGIC Ralph Tucker        1/15/2008                Changed driver table name                                                   15                          devlIDS                              Steph Goddard          01/17/2008
# MAGIC Brent Leland          03/03/2008             Changed to use new primary key process                           3567 Primary Key     devlIDScur                         Steph Goddard          05/06/2008
# MAGIC Prabhu ES              2022-03-01             MSSQL connection parameters added                                 S2S Remediation    IntegrateDev5                     Kalyan Neelam           2022-06-09

# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets from Customer Service Data
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source table
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
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
CurrDate = get_widget_value('CurrDate','2008-03-17')
RunID = get_widget_value('RunID','073380484000')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
SrcSysCdSk = get_widget_value('SrcSysCdSk','72686')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskCstmDtlPK
# COMMAND ----------

# FctsCustSvcTaskCstmDtl (ODBCConnector)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_FctsCustSvcTaskCstmDtl = f"""
SELECT 
          CSTK.CSSC_ID,
          CSTK.CSTK_SEQ_NO,          
          ATXR.ATSY_ID,          
          ATXR.ATXR_DEST_ID,
          ATXR.ATXR_DESC,
          ATXR.ATXR_CREATE_DT,
          ATXR.ATXR_CREATE_USUS,
          ATXR.ATXR_LAST_UPD_DT,
          ATXR.ATXR_LAST_UPD_USUS,
          ATUF.ATUF_SEQ_NO,
          ATUF.ATUF_TEXT1,
          ATUF.ATUF_TEXT2,
          ATUF.ATUF_TEXT3,
          ATUF.ATUF_NUM1,
          ATUF.ATUF_MONEY1,
          ATUF.ATUF_DATE1,
          ATUF.ATUF_DATE2,
          CSTK.CSTK_INPUT_DTM,
          CSTK.CSTK_MCTR_SUBJ
FROM 
          {FacetsOwner}.CMC_CSTK_TASK CSTK, 
          {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,  
          {FacetsOwner}.CER_ATUF_USERFLD_D ATUF,
          tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
         CSTK.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID AND
         ATXR.ATSY_ID = ATUF.ATSY_ID AND 
         ATXR.ATXR_DEST_ID =  ATUF.ATXR_DEST_ID AND
         (ATXR.ATSY_ID='CSBD' 
         OR ATXR.ATSY_ID='CSDI'                    
         OR (ATXR.ATSY_ID='CSMR' 
                AND (CSTK.CSTK_MCTR_SUBJ='RA03' 
                     OR CSTK.CSTK_MCTR_SUBJ='RA05' 
                     OR CSTK.CSTK_MCTR_SUBJ='RA06' 
                     OR CSTK.CSTK_MCTR_SUBJ='RA07' 
                     OR CSTK.CSTK_MCTR_SUBJ='RA08' ))) 
    AND CSTK.CSSC_ID = DRVR.CSSC_ID 
    AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""
df_FctsCustSvcTaskCstmDtl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_FctsCustSvcTaskCstmDtl)
    .load()
)

# StripFields (CTransformerStage)
df_StripFields = (
    df_FctsCustSvcTaskCstmDtl.select(
        F.col("CSSC_ID").alias("CSSC_ID"),
        F.col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
        F.col("ATSY_ID").alias("ATSY_ID"),
        F.col("ATXR_DEST_ID").alias("ATXR_DEST_ID"),
        F.col("ATUF_SEQ_NO").alias("ATUF_SEQ_NO"),
        F.col("ATXR_DESC").alias("ATXR_DESC"),
        F.col("ATXR_CREATE_DT").alias("ATXR_CREATE_DT"),
        F.col("ATXR_CREATE_USUS").alias("ATXR_CREATE_USUS"),
        F.col("ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT"),
        F.col("ATXR_LAST_UPD_USUS").alias("ATXR_LAST_UPD_USUS"),
        F.col("ATUF_TEXT1").alias("ATUF_TEXT1"),
        F.col("ATUF_TEXT2").alias("ATUF_TEXT2"),
        F.col("ATUF_TEXT3").alias("ATUF_TEXT3"),
        F.col("ATUF_NUM1").alias("ATUF_NUM1"),
        F.col("ATUF_MONEY1").alias("ATUF_MONEY1"),
        F.col("ATUF_DATE1").alias("ATUF_DATE1"),
        F.col("ATUF_DATE2").alias("ATUF_DATE2")
    )
    .withColumn("CSSC_ID", strip_field(F.col("CSSC_ID")))
    .withColumn("ATSY_ID", strip_field(F.col("ATSY_ID")))
    .withColumn("ATXR_DESC", strip_field(F.col("ATXR_DESC")))
    .withColumn("ATXR_CREATE_USUS", strip_field(F.col("ATXR_CREATE_USUS")))
    .withColumn("ATXR_LAST_UPD_USUS", strip_field(F.col("ATXR_LAST_UPD_USUS")))
    .withColumn("ATUF_TEXT1", strip_field(F.col("ATUF_TEXT1")))
    .withColumn("ATUF_TEXT2", strip_field(F.col("ATUF_TEXT2")))
    .withColumn("ATUF_TEXT3", strip_field(F.col("ATUF_TEXT3")))
)

# hf_cust_svc_task_cstm_dtl_dedup (CHashedFileStage) - Scenario A: drop duplicates by key
df_hf_cust_svc_task_cstm_dtl_dedup = df_StripFields.dropDuplicates(
    ["CSSC_ID", "CSTK_SEQ_NO", "ATSY_ID", "ATXR_DEST_ID", "ATUF_SEQ_NO"]
)

# BusinessRules (CTransformerStage)
# Two output links: "AllCol" and "Transform"

# For substring indexing in PySpark substring(col, pos, length), pos is 1-based
df_BusinessRules_input = df_hf_cust_svc_task_cstm_dtl_dedup
df_enriched = (
    df_BusinessRules_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", trim(F.col("CSSC_ID")))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("CUST_SVC_TASK_CSTM_DTL_CD", F.col("ATSY_ID"))
    .withColumn(
        "CSTM_DTL_UNIQ_ID",
        F.concat(
            F.substring("ATXR_DEST_ID", 1, 10),
            F.lit(" "),
            F.substring("ATXR_DEST_ID", 12, 2),
            F.lit(":"),
            F.substring("ATXR_DEST_ID", 15, 2),
            F.lit(":"),
            F.substring("ATXR_DEST_ID", 18, 2),
            F.lit("."),
            F.substring("ATXR_DEST_ID", 21, 6)
        )
    )
    .withColumn("CSTM_DTL_SEQ_NO", F.col("ATUF_SEQ_NO"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS"), F.lit(";"),
            trim(F.col("CSSC_ID")), F.lit(";"),
            F.col("CSTK_SEQ_NO").cast("string"), F.lit(";"),
            F.col("ATSY_ID"), F.lit(";"),
            F.col("ATXR_DEST_ID"), F.lit(";"),
            F.col("ATUF_SEQ_NO").cast("string")
        )
    )
    .withColumn("CUST_SVC_TASK_CSTM_DTL_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CRT_BY_USER", F.col("ATXR_CREATE_USUS"))
    .withColumn("CUST_SVC_TASK", F.col("CSSC_ID"))
    .withColumn("LAST_UPDT_USER", F.col("ATXR_LAST_UPD_USUS"))
    .withColumn("CRT_DTM", F.col("ATXR_CREATE_DT"))
    .withColumn("CSTM_DTL_DT_1", F.col("ATUF_DATE1"))
    .withColumn("CSTM_DTL_DT_2", F.col("ATUF_DATE2"))
    .withColumn("LAST_UPDT_DTM", F.col("ATXR_LAST_UPD_DT"))
    .withColumn(
        "CSTM_DTL_MNY_1",
        F.when(
            (F.length(trim(F.col("ATUF_MONEY1"))) == 0) | (F.col("ATUF_MONEY1").isNull()), 
            F.lit("0.00")
        ).otherwise(F.col("ATUF_MONEY1"))
    )
    .withColumn(
        "CSTM_DTL_NO_1",
        F.when(
            (F.length(trim(F.col("ATUF_NUM1"))) == 0) | (F.col("ATUF_NUM1").isNull()),
            F.lit("0")
        ).otherwise(F.upper(F.col("ATUF_NUM1")))
    )
    .withColumn(
        "CSTM_DTL_DESC",
        F.when(
            (F.length(trim(F.col("ATXR_DESC"))) == 0) | (F.col("ATXR_DESC").isNull()),
            F.lit("")
        ).otherwise(F.upper(F.col("ATXR_DESC")))
    )
    .withColumn(
        "CSTM_DTL_TX_1",
        F.when(
            (F.length(trim(F.col("ATUF_TEXT1"))) == 0) | (F.col("ATUF_TEXT1").isNull()),
            F.lit("")
        ).otherwise(F.upper(F.col("ATUF_TEXT1")))
    )
    .withColumn(
        "CSTM_DTL_TX_2",
        F.when(
            (F.length(trim(F.col("ATUF_TEXT2"))) == 0) | (F.col("ATUF_TEXT2").isNull()),
            F.lit("")
        ).otherwise(F.upper(F.col("ATUF_TEXT2")))
    )
    .withColumn(
        "CSTM_DTL_TX_3",
        F.when(
            (F.length(trim(F.col("ATUF_TEXT3"))) == 0) | (F.col("ATUF_TEXT3").isNull()),
            F.lit("")
        ).otherwise(F.upper(F.col("ATUF_TEXT3")))
    )
)

df_transform = (
    df_BusinessRules_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", trim(F.col("CSSC_ID")))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("CUST_SVC_TASK_CSTM_DTL_CD", F.col("ATSY_ID"))
    .withColumn(
        "CSTM_DTL_UNIQ_ID",
        F.concat(
            F.substring("ATXR_DEST_ID", 1, 10),
            F.lit(" "),
            F.substring("ATXR_DEST_ID", 12, 2),
            F.lit(":"),
            F.substring("ATXR_DEST_ID", 15, 2),
            F.lit(":"),
            F.substring("ATXR_DEST_ID", 18, 2),
            F.lit("."),
            F.substring("ATXR_DEST_ID", 21, 6)
        )
    )
    .withColumn("CSTM_DTL_SEQ_NO", F.col("ATUF_SEQ_NO"))
)

# CustSvcTaskCstmDtlPK (CContainerStage)
params = {
    "CurrDate": CurrDate,
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "IDSOwner": IDSOwner
}
df_CustSvcTaskCstmDtlPK_out = CustSvcTaskCstmDtlPK(df_enriched, df_transform, params)

# IdsCustSvcTaskCstmDtlExtr (CSeqFileStage)
# Final select in correct order, applying rpad to char columns
df_final_IdsCustSvcTaskCstmDtl = df_CustSvcTaskCstmDtlPK_out.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_CSTM_DTL_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_CSTM_DTL_CD",
    "CSTM_DTL_UNIQ_ID",
    "CSTM_DTL_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_BY_USER",
    "CUST_SVC_TASK",
    "LAST_UPDT_USER",
    "CRT_DTM",
    F.rpad(F.col("CSTM_DTL_DT_1"), 10, " ").alias("CSTM_DTL_DT_1"),
    F.rpad(F.col("CSTM_DTL_DT_2"), 10, " ").alias("CSTM_DTL_DT_2"),
    "LAST_UPDT_DTM",
    "CSTM_DTL_MNY_1",
    "CSTM_DTL_NO_1",
    "CSTM_DTL_DESC",
    "CSTM_DTL_TX_1",
    "CSTM_DTL_TX_2",
    "CSTM_DTL_TX_3"
)

out_path_ids = f"{adls_path}/key/IdsCustSvcTaskCstmDtlExtr.CSTaskCstmDtl.dat.{RunID}"
write_files(
    df_final_IdsCustSvcTaskCstmDtl,
    out_path_ids,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Facets_Source (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_Facets_Source = f"""
SELECT 
          task.CSSC_ID,
          task.CSTK_SEQ_NO,          
          attach.ATSY_ID,          
          attach.ATXR_DEST_ID,
          attach.ATXR_DESC,
          userfld.ATUF_SEQ_NO,
          userfld.ATUF_MONEY1,
          userfld.ATUF_DATE1
FROM 
          {FacetsOwner}.CMC_CSTK_TASK task, 
          {FacetsOwner}.CER_ATXR_ATTACH_U attach, 
          {FacetsOwner}.CER_ATUF_USERFLD_D userfld,
          tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
         task.ATXR_SOURCE_ID = attach.ATXR_SOURCE_ID AND
         attach.ATSY_ID = userfld.ATSY_ID AND 
         attach.ATXR_DEST_ID =  userfld.ATXR_DEST_ID AND 
         (attach.ATSY_ID='CSBD' OR attach.ATSY_ID='CSDI' OR 
         (attach.ATSY_ID='CSMR' AND 
         (task.CSTK_MCTR_SUBJ='RA03' OR 
         task.CSTK_MCTR_SUBJ='RA05' OR 
         task.CSTK_MCTR_SUBJ='RA06' OR 
         task.CSTK_MCTR_SUBJ='RA07' OR 
         task.CSTK_MCTR_SUBJ='RA08' ))) 
    AND task.CSSC_ID = DRVR.CSSC_ID 
    AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_Source)
    .load()
)

# Transform (CTransformerStage) - producing "Snapshot_File"
df_transformSnapshot = (
    df_Facets_Source
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", strip_field(trim(F.col("CSSC_ID"))))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn(
        "CUST_SVC_TASK_CSTM_DTL_CD_SK",
        F.expr("GetFkeyCodes('FACETS', 100, 'ATTACHMENT TYPE', trim(ATSY_ID), 'X')")
    )
    .withColumn(
        "CSTM_DTL_UNIQ_ID",
        F.expr("FORMAT.DATE(ATXR_DEST_ID, 'SYBASE', 'TIMESTAMP', 'DB2TIMESTAMP')")
    )
    .withColumn("CSTM_DTL_SEQ_NO", F.col("ATUF_SEQ_NO"))
    .withColumn(
        "CSTM_DTL_DT_1_SK",
        F.expr("GetFkeyDate('IDS', 102, FORMAT.DATE(ATUF_DATE1, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD'), 'X')")
    )
    .withColumn(
        "CSTM_DTL_MNY_1",
        F.when(
            (F.length(trim(F.col("ATUF_MONEY1"))) == 0) | (F.col("ATUF_MONEY1").isNull()),
            F.lit("0.00")
        ).otherwise(F.col("ATUF_MONEY1"))
    )
    .withColumn(
        "CSTM_DTL_DESC",
        F.when(
            (F.length(trim(F.col("ATXR_DESC"))) == 0) | (F.col("ATXR_DESC").isNull()),
            F.lit("")
        ).otherwise(F.upper(strip_field(trim(F.col("ATXR_DESC")))))
    )
)

# Snapshot_File (CSeqFileStage)
df_final_snapshot = df_transformSnapshot.select(
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_CSTM_DTL_CD_SK",
    "CSTM_DTL_UNIQ_ID",
    "CSTM_DTL_SEQ_NO",
    F.rpad(F.col("CSTM_DTL_DT_1_SK"), 10, " ").alias("CSTM_DTL_DT_1_SK"),
    "CSTM_DTL_MNY_1",
    "CSTM_DTL_DESC"
)
out_path_snapshot = f"{adls_path}/load/B_CUST_SVC_TASK_CSTM_DTL.dat"
write_files(
    df_final_snapshot,
    out_path_snapshot,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)