# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC 
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_CSTK_TASK , CER-ATLT_LETTER_D and CER_ATXR_ATTACH_U for loading into IDS.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #               Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------             ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     12/02/2007                Initial program                                                                                    CustSvc/3028      devlIDS30                          Steph Goddard          02/13/2007
# MAGIC 
# MAGIC Parik                      06/22/2007            Added balancing process to the overall job                                        3264                     devlIDS30                          Steph Goddard          09/14/2007
# MAGIC Ralph Tucker        12/28/2007            Added Hit List Processing                                                                   15                         devlIDS30                          Steph Goddard          01/09/2008
# MAGIC Ralph Tucker        1/15/2008              Changed driver table name                                                                 15                         devlIDS                              Steph Goddard          01/17/2008
# MAGIC Brent Leland          03/04/2008           Added new primary key process                                                          3567 Primary Key  devlIDScur                         Steph Goddard          05/06/2008
# MAGIC 
# MAGIC Manasa Andru       2013-09-28            Cleared temp hash file and corrected the job for standards                   TTR-1056             IntegrateNewDevl            Kalyan Neelam           2013-10-03
# MAGIC Prabhu ES             2022-03-01            MSSQL connection parameters added                                                  S2S Remediation  IntegrateDev5                 Kalyan Neelam            2022-06-09

# MAGIC Writing Sequential File to ../key
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC Strip Fields
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','2013-09-30')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1581')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskLtrPK
# COMMAND ----------

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_FctsCustSrvTaskLtrEtxr = f"""
SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
ATXR.ATSY_ID,
ATLT.ATLT_SEQ_NO,
ATLT.ATXR_DEST_ID,
ATXR.ATXR_DESC,
ATXR.ATXR_CREATE_DT,
ATXR.ATXR_CREATE_USUS,
ATXR.ATXR_LAST_UPD_DT,
ATXR.ATXR_LAST_UPD_USUS,
ATLT.ATLD_ID, 
ATLT.ATLT_FROM_NAME,
ATLT.ATLT_FROM_ADDR1,
ATLT.ATLT_FROM_ADDR2,
ATLT.ATLT_FROM_ADDR3,
ATLT.ATLT_FROM_CITY,
ATLT.ATLT_FROM_STATE,
ATLT.ATLT_FROM_ZIP,
ATLT.ATLT_FROM_COUNTY,
ATLT.ATLT_FROM_PHONE,
ATLT.ATLT_FROM_PHONE_EXT,
ATLT.ATLT_FROM_FAX,
ATLT.ATLT_TO_NAME,
ATLT.ATLT_TO_ADDR1,
ATLT.ATLT_TO_ADDR2,
ATLT.ATLT_TO_ADDR3,
ATLT.ATLT_TO_CITY,
ATLT.ATLT_TO_STATE,
ATLT.ATLT_TO_ZIP,
ATLT.ATLT_TO_COUNTY,
ATLT.ATLT_TO_PHONE,
ATLT.ATLT_TO_PHONE_EXT,
ATLT.ATLT_TO_FAX,
ATLT.ATLT_RE_NAME,
ATLT.ATLT_RE_ADDR1,
ATLT.ATLT_RE_ADDR2,
ATLT.ATLT_RE_ADDR3,
ATLT.ATLT_RE_CITY,
ATLT.ATLT_RE_STATE,
ATLT.ATLT_RE_ZIP,
ATLT.ATLT_RE_COUNTY,
ATLT.ATLT_RE_PHONE,
ATLT.ATLT_RE_PHONE_EXT,
ATLT.ATLT_RE_FAX,
ATLT.ATLT_SUBJECT,
ATLT.ATLT_REQUEST_IND,
ATLT.ATLT_SUBMITTED_IND,
ATLT.ATLT_PRINTED_IND,
ATLT.ATLT_MAILED_IND,
ATLT.ATLT_REPRINT_STATUS,
ATLT.ATLT_REQUEST_DT,
ATLT.ATLT_SUBMITTED_DT,
ATLT.ATLT_PRINTED_DT,
ATLT.ATLT_MAILED_DT,
ATLT.ATLT_DATA1,
ATLT.ATLT_DATA2,
ATLT.ATLT_DATA3,
ATLT.ATLT_DATA4,
CSTK.CSTK_INPUT_DTM
FROM 
{FacetsOwner}.CMC_CSTK_TASK CSTK, 
{FacetsOwner}.CER_ATXR_ATTACH_U ATXR,  
{FacetsOwner}.CER_ATLT_LETTER_D ATLT,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
CSTK.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID 
AND ATXR.ATSY_ID = ATLT.ATSY_ID 
AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
AND ATXR.ATSY_ID IN ('LCF1', 'LCF2', 'LCF4', 'XC01', 'LC02', 'LC03', 'LC05', 'LC07', 'LC42')
AND CSTK.CSSC_ID = DRVR.CSSC_ID
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_FctsCustSrvTaskLtrEtxr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FctsCustSrvTaskLtrEtxr)
    .load()
)

df_hf_fcts_cust_svc_tsk_ltr_main_extr = dedup_sort(
    df_FctsCustSrvTaskLtrEtxr,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO","ATSY_ID","ATLT_SEQ_NO","ATXR_DEST_ID"],
    sort_cols=[]
)

df_StripField = (
    df_hf_fcts_cust_svc_tsk_ltr_main_extr
    .withColumn("CSSC_ID", strip_field(F.col("CSSC_ID")))
    .withColumn("CSTK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("ATSY_ID", strip_field(F.col("ATSY_ID")))
    .withColumn("ATLT_SEQ_NO", F.col("ATLT_SEQ_NO"))
    .withColumn("ATXR_DEST_ID", F.col("ATXR_DEST_ID"))
    .withColumn("ATXR_DESC", strip_field(UpCase(F.col("ATXR_DESC"))))
    .withColumn("ATXR_CREATE_DT", F.col("ATXR_CREATE_DT"))
    .withColumn("ATXR_CREATE_USUS", strip_field(F.col("ATXR_CREATE_USUS")))
    .withColumn("ATXR_LAST_UPD_DT", F.col("ATXR_LAST_UPD_DT"))
    .withColumn("ATXR_LAST_UPD_USUS", strip_field(F.col("ATXR_LAST_UPD_USUS")))
    .withColumn("ATLD_ID", strip_field(F.col("ATLD_ID")))
    .withColumn("ATLT_FROM_NAME", strip_field(UpCase(F.col("ATLT_FROM_NAME"))))
    .withColumn("ATLT_FROM_ADDR1", strip_field(UpCase(F.col("ATLT_FROM_ADDR1"))))
    .withColumn("ATLT_FROM_ADDR2", strip_field(UpCase(F.col("ATLT_FROM_ADDR2"))))
    .withColumn("ATLT_FROM_ADDR3", strip_field(UpCase(F.col("ATLT_FROM_ADDR3"))))
    .withColumn("ATLT_FROM_CITY", strip_field(UpCase(F.col("ATLT_FROM_CITY"))))
    .withColumn("ATLT_FROM_STATE", strip_field(UpCase(F.col("ATLT_FROM_STATE"))))
    .withColumn("ATLT_FROM_ZIP", strip_field(F.col("ATLT_FROM_ZIP")))
    .withColumn("ATLT_FROM_COUNTY", strip_field(UpCase(F.col("ATLT_FROM_COUNTY"))))
    .withColumn("ATLT_FROM_PHONE", strip_field(F.col("ATLT_FROM_PHONE")))
    .withColumn("ATLT_FROM_PHONE_EXT", strip_field(F.col("ATLT_FROM_PHONE_EXT")))
    .withColumn("ATLT_FROM_FAX", strip_field(F.col("ATLT_FROM_FAX")))
    .withColumn("ATLT_TO_NAME", strip_field(UpCase(F.col("ATLT_TO_NAME"))))
    .withColumn("ATLT_TO_ADDR1", strip_field(UpCase(F.col("ATLT_TO_ADDR1"))))
    .withColumn("ATLT_TO_ADDR2", strip_field(UpCase(F.col("ATLT_TO_ADDR2"))))
    .withColumn("ATLT_TO_ADDR3", strip_field(UpCase(F.col("ATLT_TO_ADDR3"))))
    .withColumn("ATLT_TO_CITY", strip_field(UpCase(F.col("ATLT_TO_CITY"))))
    .withColumn("ATLT_TO_STATE", strip_field(UpCase(F.col("ATLT_TO_STATE"))))
    .withColumn("ATLT_TO_ZIP", strip_field(F.col("ATLT_TO_ZIP")))
    .withColumn("ATLT_TO_COUNTY", strip_field(UpCase(F.col("ATLT_TO_COUNTY"))))
    .withColumn("ATLT_TO_PHONE", strip_field(F.col("ATLT_TO_PHONE")))
    .withColumn("ATLT_TO_PHONE_EXT", strip_field(F.col("ATLT_TO_PHONE_EXT")))
    .withColumn("ATLT_TO_FAX", strip_field(F.col("ATLT_TO_FAX")))
    .withColumn("ATLT_RE_NAME", strip_field(UpCase(F.col("ATLT_RE_NAME"))))
    .withColumn("ATLT_RE_ADDR1", strip_field(UpCase(F.col("ATLT_RE_ADDR1"))))
    .withColumn("ATLT_RE_ADDR2", strip_field(UpCase(F.col("ATLT_RE_ADDR2"))))
    .withColumn("ATLT_RE_ADDR3", strip_field(UpCase(F.col("ATLT_RE_ADDR3"))))
    .withColumn("ATLT_RE_CITY", strip_field(UpCase(F.col("ATLT_RE_CITY"))))
    .withColumn("ATLT_RE_STATE", strip_field(UpCase(F.col("ATLT_RE_STATE"))))
    .withColumn("ATLT_RE_ZIP", strip_field(F.col("ATLT_RE_ZIP")))
    .withColumn("ATLT_RE_COUNTY", strip_field(UpCase(F.col("ATLT_RE_COUNTY"))))
    .withColumn("ATLT_RE_PHONE", strip_field(F.col("ATLT_RE_PHONE")))
    .withColumn("ATLT_RE_PHONE_EXT", strip_field(F.col("ATLT_RE_PHONE_EXT")))
    .withColumn("ATLT_RE_FAX", strip_field(F.col("ATLT_RE_FAX")))
    .withColumn("ATLT_SUBJECT", strip_field(UpCase(F.col("ATLT_SUBJECT"))))
    .withColumn("ATLT_REQUEST_IND", strip_field(UpCase(F.col("ATLT_REQUEST_IND"))))
    .withColumn("ATLT_SUBMITTED_IND", strip_field(UpCase(F.col("ATLT_SUBMITTED_IND"))))
    .withColumn("ATLT_PRINTED_IND", strip_field(UpCase(F.col("ATLT_PRINTED_IND"))))
    .withColumn("ATLT_MAILED_IND", strip_field(UpCase(F.col("ATLT_MAILED_IND"))))
    .withColumn("ATLT_REPRINT_STATUS", strip_field(F.col("ATLT_REPRINT_STATUS")))
    .withColumn("ATLT_REQUEST_DT", F.col("ATLT_REQUEST_DT"))
    .withColumn("ATLT_SUBMITTED_DT", F.col("ATLT_SUBMITTED_DT"))
    .withColumn("ATLT_PRINTED_DT", F.col("ATLT_PRINTED_DT"))
    .withColumn("ATLT_MAILED_DT", F.col("ATLT_MAILED_DT"))
    .withColumn("ATLT_DATA1", strip_field(UpCase(F.col("ATLT_DATA1"))))
    .withColumn("ATLT_DATA2", strip_field(UpCase(F.col("ATLT_DATA2"))))
    .withColumn("ATLT_DATA3", strip_field(UpCase(F.col("ATLT_DATA3"))))
    .withColumn("ATLT_DATA4", strip_field(UpCase(F.col("ATLT_DATA4"))))
)

df_BusinessLogic = (
    df_StripField
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svCustSvcId", trim(UpCase(F.col("CSSC_ID"))))
)

df_AllCol = df_BusinessLogic.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("svCustSvcId").alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("ATSY_ID").alias("CUST_SVC_TASK_LTR_STYLE_CD"),
    F.col("ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("ATXR_DEST_ID").alias("LTR_DEST_ID"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat_ws(";", F.lit("FACETS"), F.col("CSSC_ID"), F.col("CSTK_SEQ_NO"), F.col("ATSY_ID"), F.col("ATLT_SEQ_NO"), F.col("ATXR_DEST_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_LTR_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ATXR_CREATE_USUS").alias("CRT_BY_USER_SK"),
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    F.col("ATXR_LAST_UPD_USUS").alias("LAST_UPDT_USER_SK"),
    F.when((F.length(trim(F.col("ATLT_REPRINT_STATUS"))) == 0) | F.col("ATLT_REPRINT_STATUS").isNull(), F.lit("NA"))
      .otherwise(trim(F.col("ATLT_REPRINT_STATUS"))).alias("CUST_SVC_TASK_LTR_REPRT_STTUS_"),
    F.when((F.length(trim(F.col("ATLD_ID"))) == 0) | F.col("ATLD_ID").isNull(), F.lit("NA"))
      .otherwise(trim(F.col("ATLD_ID"))).alias("CUST_SVC_TASK_LTR_TYP_CD_SK"),
    F.date_format(F.col("ATXR_CREATE_DT"), "yyyy-MM-dd").alias("CRT_DT_SK"),
    F.date_format(F.col("ATXR_LAST_UPD_DT"), "yyyy-MM-dd").alias("LAST_UPDT_DT_SK"),
    F.date_format(F.col("ATLT_MAILED_DT"), "yyyy-MM-dd").alias("MAILED_DT_SK"),
    F.date_format(F.col("ATLT_PRINTED_DT"), "yyyy-MM-dd").alias("PRTED_DT_SK"),
    F.date_format(F.col("ATLT_REQUEST_DT"), "yyyy-MM-dd").alias("RQST_DT_SK"),
    F.date_format(F.col("ATLT_SUBMITTED_DT"), "yyyy-MM-dd").alias("SUBMT_DT_SK"),
    F.when((F.length(trim(F.col("ATLT_MAILED_IND"))) == 0) | F.col("ATLT_MAILED_IND").isNull(), F.lit("U"))
      .otherwise(trim(F.col("ATLT_MAILED_IND"))).alias("MAILED_IN"),
    F.when((F.length(trim(F.col("ATLT_PRINTED_IND"))) == 0) | F.col("ATLT_PRINTED_IND").isNull(), F.lit("U"))
      .otherwise(trim(F.col("ATLT_PRINTED_IND"))).alias("PRTED_IN"),
    F.when((F.length(trim(F.col("ATLT_REQUEST_IND"))) == 0) | F.col("ATLT_REQUEST_IND").isNull(), F.lit("U"))
      .otherwise(trim(F.col("ATLT_REQUEST_IND"))).alias("RQST_IN"),
    F.when((F.length(trim(F.col("ATLT_SUBMITTED_IND"))) == 0) | F.col("ATLT_SUBMITTED_IND").isNull(), F.lit("U"))
      .otherwise(trim(F.col("ATLT_SUBMITTED_IND"))).alias("SUBMT_IN"),
    F.when((F.length(trim(F.col("ATLT_TO_NAME"))) == 0) | F.col("ATLT_TO_NAME").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_NAME"))).alias("RECPNT_NM"),
    F.when((F.length(trim(F.col("ATLT_TO_ADDR1"))) == 0) | F.col("ATLT_TO_ADDR1").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_ADDR1"))).alias("RECPNT_ADDR_LN_1"),
    F.when((F.length(trim(F.col("ATLT_TO_ADDR2"))) == 0) | F.col("ATLT_TO_ADDR2").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_ADDR2"))).alias("RECPNT_ADDR_LN_2"),
    F.when((F.length(trim(F.col("ATLT_TO_ADDR3"))) == 0) | F.col("ATLT_TO_ADDR3").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_ADDR3"))).alias("RECPNT_ADDR_LN_3"),
    F.when((F.length(trim(F.col("ATLT_TO_CITY"))) == 0) | F.col("ATLT_TO_CITY").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_CITY"))).alias("RECPNT_CITY_NM"),
    F.when((F.length(trim(F.col("ATLT_TO_STATE"))) == 0) | F.col("ATLT_TO_STATE").isNull(), F.lit("NA"))
      .otherwise(trim(F.col("ATLT_TO_STATE"))).alias("CS_TASK_LTR_RECPNT_ST_CD_SK"),
    F.when((F.length(trim(UpCase(F.col("ATLT_TO_ZIP")))) == 0) | F.col("ATLT_TO_ZIP").isNull(), F.lit(None))
      .otherwise(trim(UpCase(F.col("ATLT_TO_ZIP")))).alias("RECPNT_POSTAL_CD"),
    F.when((F.length(trim(F.col("ATLT_TO_COUNTY"))) == 0) | F.col("ATLT_TO_COUNTY").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_COUNTY"))).alias("RECPNT_CNTY_NM"),
    F.when((F.length(trim(F.col("ATLT_TO_PHONE"))) == 0) | F.col("ATLT_TO_PHONE").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_PHONE"))).alias("RECPNT_PHN_NO"),
    F.when((F.length(trim(F.col("ATLT_TO_PHONE_EXT"))) == 0) | F.col("ATLT_TO_PHONE_EXT").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_PHONE_EXT"))).alias("RECPNT_PHN_NO_EXT"),
    F.when((F.length(trim(F.col("ATLT_TO_FAX"))) == 0) | F.col("ATLT_TO_FAX").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_TO_FAX"))).alias("RECPNT_FAX_NO"),
    F.when((F.length(trim(F.col("ATLT_RE_NAME"))) == 0) | F.col("ATLT_RE_NAME").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_NAME"))).alias("REF_NM"),
    F.when((F.length(trim(F.col("ATLT_RE_ADDR1"))) == 0) | F.col("ATLT_RE_ADDR1").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_ADDR1"))).alias("REF_ADDR_LN_1"),
    F.when((F.length(trim(F.col("ATLT_RE_ADDR2"))) == 0) | F.col("ATLT_RE_ADDR2").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_ADDR2"))).alias("REF_ADDR_LN_2"),
    F.when((F.length(trim(F.col("ATLT_RE_ADDR3"))) == 0) | F.col("ATLT_RE_ADDR3").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_ADDR3"))).alias("REF_ADDR_LN_3"),
    F.when((F.length(trim(F.col("ATLT_RE_CITY"))) == 0) | F.col("ATLT_RE_CITY").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_CITY"))).alias("REF_CITY_NM"),
    F.when((F.length(trim(F.col("ATLT_RE_STATE"))) == 0) | F.col("ATLT_RE_STATE").isNull(), F.lit("NA"))
      .otherwise(trim(F.col("ATLT_RE_STATE"))).alias("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
    F.when((F.length(trim(F.col("ATLT_RE_ZIP"))) == 0) | F.col("ATLT_RE_ZIP").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_ZIP"))).alias("REF_POSTAL_CD"),
    F.when((F.length(trim(F.col("ATLT_RE_COUNTY"))) == 0) | F.col("ATLT_RE_COUNTY").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_COUNTY"))).alias("REF_CNTY_NM"),
    F.when((F.length(trim(F.col("ATLT_RE_PHONE"))) == 0) | F.col("ATLT_RE_PHONE").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_PHONE"))).alias("REF_PHN_NO"),
    F.when((F.length(trim(F.col("ATLT_RE_PHONE_EXT"))) == 0) | F.col("ATLT_RE_PHONE_EXT").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_PHONE_EXT"))).alias("REF_PHN_NO_EXT"),
    F.when((F.length(trim(F.col("ATLT_RE_FAX"))) == 0) | F.col("ATLT_RE_FAX").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_RE_FAX"))).alias("REF_FAX_NO"),
    F.when((F.length(trim(F.col("ATLT_FROM_NAME"))) == 0) | F.col("ATLT_FROM_NAME").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_NAME"))).alias("SEND_NM"),
    F.when((F.length(trim(F.col("ATLT_FROM_ADDR1"))) == 0) | F.col("ATLT_FROM_ADDR1").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_ADDR1"))).alias("SEND_ADDR_LN_1"),
    F.when((F.length(trim(F.col("ATLT_FROM_ADDR2"))) == 0) | F.col("ATLT_FROM_ADDR2").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_ADDR2"))).alias("SEND_ADDR_LN_2"),
    F.when((F.length(trim(F.col("ATLT_FROM_ADDR3"))) == 0) | F.col("ATLT_FROM_ADDR3").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_ADDR3"))).alias("SEND_ADDR_LN_3"),
    F.when((F.length(trim(F.col("ATLT_FROM_CITY"))) == 0) | F.col("ATLT_FROM_CITY").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_CITY"))).alias("SEND_CITY_NM"),
    F.when((F.length(trim(F.col("ATLT_FROM_STATE"))) == 0) | F.col("ATLT_FROM_STATE").isNull(), F.lit("NA"))
      .otherwise(trim(F.col("ATLT_FROM_STATE"))).alias("CUST_SVC_TASK_LTR_SEND_ST_CD"),
    F.when((F.length(trim(F.col("ATLT_FROM_ZIP"))) == 0) | F.col("ATLT_FROM_ZIP").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_ZIP"))).alias("SEND_POSTAL_CD"),
    F.when((F.length(trim(F.col("ATLT_FROM_COUNTY"))) == 0) | F.col("ATLT_FROM_COUNTY").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_COUNTY"))).alias("SEND_CNTY_NM"),
    F.when((F.length(trim(F.col("ATLT_FROM_PHONE"))) == 0) | F.col("ATLT_FROM_PHONE").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_PHONE"))).alias("SEND_PHN_NO"),
    F.when((F.length(trim(F.col("ATLT_FROM_PHONE_EXT"))) == 0) | F.col("ATLT_FROM_PHONE_EXT").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_PHONE_EXT"))).alias("SEND_PHN_NO_EXT"),
    F.when((F.length(trim(F.col("ATLT_FROM_FAX"))) == 0) | F.col("ATLT_FROM_FAX").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_FROM_FAX"))).alias("SEND_FAX_NO"),
    F.when((F.length(trim(F.col("ATXR_DESC"))) == 0) | F.col("ATXR_DESC").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATXR_DESC"))).alias("EXPL_TX"),
    F.when((F.length(trim(F.col("ATLT_DATA1"))) == 0) | F.col("ATLT_DATA1").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_DATA1"))).alias("LTR_TX_1"),
    F.when((F.length(trim(F.col("ATLT_DATA2"))) == 0) | F.col("ATLT_DATA2").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_DATA2"))).alias("LTR_TX_2"),
    F.when((F.length(trim(F.col("ATLT_DATA3"))) == 0) | F.col("ATLT_DATA3").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_DATA3"))).alias("LTR_TX_3"),
    F.when((F.length(trim(F.col("ATLT_DATA4"))) == 0) | F.col("ATLT_DATA4").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_DATA4"))).alias("LTR_TX_4"),
    F.when((F.length(trim(F.col("ATLT_SUBJECT"))) == 0) | F.col("ATLT_SUBJECT").isNull(), F.lit(None))
      .otherwise(trim(F.col("ATLT_SUBJECT"))).alias("MSG_TX")
)

df_Transform = df_BusinessLogic.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(UpCase(strip_field(F.col("CSSC_ID")))).alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("ATSY_ID").alias("CUST_SVC_TASK_LTR_STYLE_CD"),
    F.col("ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("ATXR_DEST_ID").alias("LTR_DEST_ID")
)

params_CustSvcTaskLtrPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "$IDSOwner": IDSOwner
}

df_Key = CustSvcTaskLtrPK(df_AllCol, df_Transform, params_CustSvcTaskLtrPK)

cols_IdsCustSvcTaskLtr = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_LTR_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_LTR_STYLE_CD",
    "LTR_SEQ_NO",
    "LTR_DEST_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_BY_USER_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER_SK",
    "CUST_SVC_TASK_LTR_REPRT_STTUS",
    "CUST_SVC_TASK_LTR_TYP_CD_SK",
    "CRT_DT_SK",
    "LAST_UPDT_DT_SK",
    "MAILED_DT_SK",
    "PRTED_DT_SK",
    "RQST_DT_SK",
    "SUBMT_DT_SK",
    "MAILED_IN",
    "PRTED_IN",
    "RQST_IN",
    "SUBMT_IN",
    "RECPNT_NM",
    "RECPNT_ADDR_LN_1",
    "RECPNT_ADDR_LN_2",
    "RECPNT_ADDR_LN_3",
    "RECPNT_CITY_NM",
    "CS_TASK_LTR_RECPNT_ST_CD_SK",
    "RECPNT_POSTAL_CD",
    "RECPNT_CNTY_NM",
    "RECPNT_PHN_NO",
    "RECPNT_PHN_NO_EXT",
    "RECPNT_FAX_NO",
    "REF_NM",
    "REF_ADDR_LN_1",
    "REF_ADDR_LN_2",
    "REF_ADDR_LN_3",
    "REF_CITY_NM",
    "CUST_SVC_TASK_LTR_REF_ST_CD_SK",
    "REF_POSTAL_CD",
    "REF_CNTY_NM",
    "REF_PHN_NO",
    "REF_PHN_NO_EXT",
    "REF_FAX_NO",
    "SEND_NM",
    "SEND_ADDR_LN_1",
    "SEND_ADDR_LN_2",
    "SEND_ADDR_LN_3",
    "SEND_CITY_NM",
    "CUST_SVC_TASK_LTR_SEND_ST_CD_S",
    "SEND_POSTAL_CD",
    "SEND_CNTY_NM",
    "SEND_PHN_NO",
    "SEND_PHN_NO_EXT",
    "SEND_FAX_NO",
    "EXPL_TX",
    "LTR_TX_1",
    "LTR_TX_2",
    "LTR_TX_3",
    "LTR_TX_4",
    "MSG_TX"
]

df_for_write_IdsCustSvcTaskLtr = df_Key.select(*[F.col(c) for c in cols_IdsCustSvcTaskLtr])

char_cols_IdsCustSvcTaskLtr = {
    "INSRT_UPDT_CD": 10,
    "DISCARD_IN": 1,
    "PASS_THRU_IN": 1,
    "RECPNT_PHN_NO_EXT": 5,
    "REF_PHN_NO_EXT": 5,
    "SEND_PHN_NO_EXT": 5
}

df_rpad_IdsCustSvcTaskLtr = df_for_write_IdsCustSvcTaskLtr
for col_name, length_val in char_cols_IdsCustSvcTaskLtr.items():
    df_rpad_IdsCustSvcTaskLtr = df_rpad_IdsCustSvcTaskLtr.withColumn(
        col_name, F.rpad(F.col(col_name), length_val, " ")
    )

write_files(
    df_rpad_IdsCustSvcTaskLtr,
    f"{adls_path}/key/IdsCustSvcTaskLtrExtr.CSTaskLtr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_Facets_Source = f"""
SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
ATXR.ATSY_ID,
ATLT.ATXR_DEST_ID,
ATLT.ATLT_SEQ_NO
FROM 
{FacetsOwner}.CMC_CSTK_TASK CSTK, 
{FacetsOwner}.CER_ATXR_ATTACH_U ATXR,  
{FacetsOwner}.CER_ATLT_LETTER_D ATLT,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
CSTK.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID 
AND ATXR.ATSY_ID = ATLT.ATSY_ID 
AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
AND ATXR.ATSY_ID IN ('LCF1', 'LCF2', 'LCF4', 'XC01', 'LC02', 'LC03', 'LC05', 'LC07', 'LC42')
AND CSTK.CSSC_ID = DRVR.CSSC_ID
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_Source)
    .load()
)

df_Transform_stagevar = (
    df_Facets_Source
    .withColumn(
        "svCustSvcTaskLtrStyleCdSk",
        GetFkeyCodes("FACETS", 100, "ATTACHMENT TYPE", trim(strip_field(F.col("ATSY_ID"))), "X")
    )
)

df_Transform_Output = df_Transform_stagevar.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(UpCase(strip_field(F.col("CSSC_ID")))).alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("svCustSvcTaskLtrStyleCdSk").alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
    F.col("ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("ATXR_DEST_ID").alias("LTR_DEST_ID")
)

cols_Snapshot_File = [
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_LTR_STYLE_CD_SK",
    "LTR_SEQ_NO",
    "LTR_DEST_ID"
]

df_for_write_Snapshot_File = df_Transform_Output.select(*[F.col(c) for c in cols_Snapshot_File])

write_files(
    df_for_write_Snapshot_File,
    f"{adls_path}/load/B_CUST_SVC_TASK_LTR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)