# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_CSTK_TASK , CMC_GRGR_GROUP , CMC_SBSB_SUB_GROUP, CER_USGU_USERGRP_U and CER_USGU_USERGRP_U for loading into IDS.
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the CUST_SVC_TASK_LTR, CUST_SVC_TASK_STTUS, CUST_SVC_TASK_CSTM_DTL , CUST_SVC_TASK_NOTE and CUST_SVC_TASK_NOTE_LN tables.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #                            Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------                           ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     12/02/2007                Initial program                                                                                    CustSvc/3028                    devlIDS30                          Steph Goddard          02/13/2007
# MAGIC Steph Goddard    06/08/2007               changed extract from input_date to last_update date                        custsvc/3028                     devlIDS30                          Brent Leland              06/08/2007
# MAGIC                                                               also set up hash.clear
# MAGIC Parik                      06/22/2007              Added balancing process to the overall job                                      3264                                    devlIDS30                           Steph Goddard         09/14/2007
# MAGIC Ralph Tucker        12/29/2007              Added Hit List processing                                                                  15                                       devlIDS30                          Steph Goddard         01/09/2008
# MAGIC Ralph Tucker        1/15/2008                Changed driver table name                                                                15                                       devlIDS                              Steph Goddard         01/17/2008
# MAGIC Brent Leland          02/26/2008             Added new primary key process                                                        3567  Primary Key                devlIDScur                         Steph Goddard         05/06/2008    
# MAGIC Dan Long              10/16/2013             Change Facets_Source stage to include a left outer join.                  TFS-1173                            IntegrateNewDevl            Kalyan Neelam              2013-10-18
# MAGIC                                                               Changed the Derivation of the Stage Variable svGrpSk
# MAGIC                                                               in the Transform stage.
# MAGIC    
# MAGIC Manasa Andru       2014-09-14              Corrected the hashed file names to standards and added these        TFS - 1267                           IntegrateNewDevl           Kalyan Neelam             2014-09-15
# MAGIC                                                                      to HASH.CLEAR After-job subroutine
# MAGIC 
# MAGIC Dan Long             10/23/2014             Change the derivation for the CUST_ID in the tranformer  stage to     TFS-8433                            IntegrateNewDevl           Kalyan Neelam             2014-10-24
# MAGIC                                                              Trim(STRIP.FIELD(If Snapshot.CSTK_CUSTOMER = ' ' 
# MAGIC                                                              then  'NA' else UpCase(Snapshot.CSTK_CUSTOMER)))
# MAGIC Prabhu ES            2022-03-01              MSSQL connection parameters added                                                S2S Remediation                 IntegrateDev5               Kalyan Neelam           2022-06-09

# MAGIC Writing Sequential File to ../key
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Apply business logic.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20070112929')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskPK
# COMMAND ----------

# Obtain JDBC connection info for Facets
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# ---------------------------------------------------------
# Stages: CMC_CSTK_TASK (ODBCConnector) - multiple output pins
# ---------------------------------------------------------

query_Extract1 = f"""SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
CSTK.GRGR_CK,
CSTK.CSTK_INPUT_USID,
CSTK.CSTK_LAST_UPD_USID,
CSTK.MEME_CK,
CSTK.PRPR_ID,
CSTK.SGSG_CK,
CSTK.SBSB_CK,
CSTK.CSTK_MCTR_CATG,
CSTK.CSPD_CAT,
CSTK.CSTK_MCTR_POC,
CSTK.CSTK_CUST_IND,
CSTK.CSTK_MCTR_REAS,
CSTK.CSTK_ABOUT_TYPE,
CSTK.CSTK_PAGE_TYPE,
CSTK.CSTK_MCTR_PRTY,
CSTK.CSTK_STS,
CSTK.CSTK_MCTR_SUBJ,
CSTK.CSTK_COMP_IND,
CSTK.CSTK_INPUT_DTM,
CSTK.CSTK_LAST_UPD_DTM,
CSTK.CSTK_NEXT_REV_DT,
CSTK.CSTK_RECD_DT,
CSTK.CSTK_NEXT_REV_INT,
CSTK.CSTK_CUSTOMER,
CSTK.USUS_SITE_CODE,
CSTK.CSTK_INPUT_UDEPT,
CSTK.CSTK_SUMMARY,
CSTK.CSTK_ASSIGN_USID,
CSTK.ATXR_SOURCE_ID

FROM 
{FacetsOwner}.CMC_CSTK_TASK CSTK,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR

WHERE 
CSTK.CSSC_ID = DRVR.CSSC_ID 
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_CMC_CSTK_TASK_Extract1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_Extract1)
    .load()
)

query_cmc_mepe_elig = f"""SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
MEPE.MEME_CK,
MEPE.CSPD_CAT,
CSTK.CSTK_RECD_DT,
MEPE.MEPE_TERM_DT,
MEPE.PDPD_ID,
CSTK.CSTK_LAST_UPD_DTM as CSTK_INPUT_DTM

FROM 
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR,
{FacetsOwner}.CMC_CSTK_TASK CSTK,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE

WHERE  CSTK.CSSC_ID = DRVR.CSSC_ID 
       AND  CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO 
       AND  CSTK.MEME_CK = MEPE.MEME_CK
       AND  CSTK.CSPD_CAT = MEPE.CSPD_CAT
       AND  CSTK.CSTK_RECD_DT >= MEPE.MEPE_EFF_DT
       AND  CSTK.CSTK_RECD_DT <= MEPE.MEPE_TERM_DT
"""

df_CMC_CSTK_TASK_cmc_mepe_elig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cmc_mepe_elig)
    .load()
)

query_cer_usgu_usergrp = f"""SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
USUS.USUS_ID,
CSTK.CSTK_INPUT_USID,
CSTK.CSTK_MCTR_SUBJ,
USUS.USUS_GROUP_IND,
CSTK.CSTK_LAST_UPD_DTM as CSTK_INPUT_DTM

FROM 
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR,
{FacetsOwner}.CMC_CSTK_TASK CSTK,
{FacetsOwner}.CER_USGU_USERGRP_U USGU,
{FacetsOwner}.CER_USUS_USER_D USUS

WHERE  CSTK.CSSC_ID = DRVR.CSSC_ID 
       AND  CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO 
       AND  CSTK.CSTK_INPUT_USID = USGU.USUS_ID
       AND  USGU.USGR_USUS_ID = USUS.USUS_ID
       AND  USUS.USUS_GROUP_IND = 'R'
"""

df_CMC_CSTK_TASK_cer_usgu_usergrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cer_usgu_usergrp)
    .load()
)

query_cmc_csts_sttus = f"""SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
STATUS.CSTS_STS,
CSTK.CSTK_LAST_UPD_DTM as CSTK_INPUT_DTM,
max(STATUS.CSTS_STS_DTM) as CSTS_STS_DTM

FROM 
{FacetsOwner}.CMC_CSTK_TASK CSTK,
{FacetsOwner}.CMC_CSTS_STATUS STATUS,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR

WHERE 
CSTK.CSSC_ID = STATUS.CSSC_ID
AND CSTK.CSTK_SEQ_NO = STATUS.CSTK_SEQ_NO
AND CSTK.CSTK_STS = 'CL'
AND CSTS_STS = 'CL'
AND CSTK.CSSC_ID = DRVR.CSSC_ID 
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO 

GROUP BY
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
STATUS.CSTS_STS,
CSTK.CSTK_LAST_UPD_DTM
"""

df_CMC_CSTK_TASK_cmc_csts_sttus = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cmc_csts_sttus)
    .load()
)

query_cmc_sgsg_sub_group = f"""SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
SGSG.SGSG_CK,
GRGR.GRGR_CK,
SGSG.SGSG_ID,
GRGR.GRGR_ID

FROM 
{FacetsOwner}.CMC_CSTK_TASK CSTK,
{FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG,
{FacetsOwner}.CMC_GRGR_GROUP GRGR,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR

WHERE
CSTK.SGSG_CK = SGSG.SGSG_CK
AND CSTK.GRGR_CK = GRGR.GRGR_CK
AND CSTK.CSSC_ID = DRVR.CSSC_ID 
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_CMC_CSTK_TASK_cmc_sgsg_sub_group = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cmc_sgsg_sub_group)
    .load()
)

query_cer_atxrattach_extr = f"""SELECT
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
ATXR.ATSY_ID,
ATXR.ATXR_SOURCE_ID,
max(ATXR.ATXR_LAST_UPD_DT) as ATXR_LAST_UPD_DT

FROM 
{FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
{FacetsOwner}.CMC_CSTK_TASK CSTK,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR

WHERE
CSTK.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID
AND ATXR.ATSY_ID = 'CSBD'
AND CSTK.CSTK_STS = 'CL'
AND CSTK.CSSC_ID = DRVR.CSSC_ID 
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO 

GROUP BY
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
ATXR.ATSY_ID,
ATXR.ATXR_SOURCE_ID
"""

df_CMC_CSTK_TASK_cer_atxrattach_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cer_atxrattach_extr)
    .load()
)

query_cer_atuf_usefld = f"""SELECT
ATUF.ATSY_ID,
ATUF.ATXR_DEST_ID,
ATUF.ATUF_DATE1

FROM 
{FacetsOwner}.CER_ATUF_USERFLD_D ATUF

WHERE
ATUF.ATSY_ID = 'CSBD'
"""

df_CMC_CSTK_TASK_cer_atuf_usefld = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cer_atuf_usefld)
    .load()
)

query_cer_atxrattach_extr1 = f"""SELECT
ATXR.ATSY_ID,
ATXR.ATXR_SOURCE_ID,
ATXR.ATXR_DEST_ID,
ATXR.ATXR_LAST_UPD_DT
FROM 
{FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
{FacetsOwner}.CMC_CSTK_TASK CSTK,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
CSTK.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID
AND CSTK.CSSC_ID = DRVR.CSSC_ID 
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_CMC_CSTK_TASK_cer_atxrattach_extr1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cer_atxrattach_extr1)
    .load()
)

# ---------------------------------------------------------
# StripField4 (CTransformerStage) => outputs to "hf_cust_svc_task_csts_sttus"
# ---------------------------------------------------------

df_StripField4 = df_CMC_CSTK_TASK_cmc_csts_sttus.alias("cmc_csts_sttus").select(
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_csts_sttus.CSSC_ID"), "\r", ""), "\n", ""), "\t", "").alias("CSSC_ID"),
    F.col("cmc_csts_sttus.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_csts_sttus.CSTS_STS"), "\r", ""), "\n", ""), "\t", "").alias("CSTS_STS"),
    F.col("cmc_csts_sttus.CSTS_STS_DTM").alias("CSTS_STS_DTM")
)

# Deduplicate for "hf_cust_svc_task_csts_sttus" (Scenario A)
df_hf_cust_svc_task_csts_sttus = dedup_sort(
    df_StripField4,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO"],
    sort_cols=[]
)

# ---------------------------------------------------------
# StripField1 (CTransformerStage) => outputs to "hf_cust_svc_task_usus_id"
# ---------------------------------------------------------

df_StripField1 = df_CMC_CSTK_TASK_cer_usgu_usergrp.alias("cer_usgu_usergrp").select(
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cer_usgu_usergrp.CSSC_ID"), "\r", ""), "\n", ""), "\t", "").alias("CSSC_ID"),
    F.col("cer_usgu_usergrp.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cer_usgu_usergrp.CSTK_MCTR_SUBJ"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_MCTR_SUBJ"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cer_usgu_usergrp.USUS_ID"), "\r", ""), "\n", ""), "\t", "").alias("USUS_ID"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cer_usgu_usergrp.CSTK_INPUT_USID"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_INPUT_USID"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cer_usgu_usergrp.USUS_GROUP_IND"), "\r", ""), "\n", ""), "\t", "").alias("USUS_GROUP_IND")
)

# Deduplicate for "hf_cust_svc_task_usus_id" (Scenario A)
df_hf_cust_svc_task_usus_id = dedup_sort(
    df_StripField1,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO","CSTK_MCTR_SUBJ"],
    sort_cols=[]
)

# ---------------------------------------------------------
# StripField2 (CTransformerStage) => outputs to "hf_cust_svc_task_pdpd_id"
# ---------------------------------------------------------

df_StripField2 = df_CMC_CSTK_TASK_cmc_mepe_elig.alias("cmc_mepe_elig").select(
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_mepe_elig.CSSC_ID"), "\r", ""), "\n", ""), "\t", "").alias("CSSC_ID"),
    F.col("cmc_mepe_elig.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("cmc_mepe_elig.MEME_CK").alias("MEME_CK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_mepe_elig.CSPD_CAT"), "\r", ""), "\n", ""), "\t", "").alias("CSPD_CAT"),
    F.col("cmc_mepe_elig.CSTK_RECD_DT").alias("CSTK_RECD_DT"),
    F.col("cmc_mepe_elig.MEPE_TERM_DT").alias("MEPE_TERM_DT"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_mepe_elig.PDPD_ID"), "\r", ""), "\n", ""), "\t", "").alias("PDPD_ID")
)

# Deduplicate for "hf_cust_svc_task_pdpd_id" (Scenario A)
df_hf_cust_svc_task_pdpd_id = dedup_sort(
    df_StripField2,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO","MEME_CK","CSPD_CAT"],
    sort_cols=[]
)

# ---------------------------------------------------------
# StripField5 (CTransformerStage) => outputs to "hf_cust_svc_task_sgsg_grgr_id"
# ---------------------------------------------------------

df_StripField5 = df_CMC_CSTK_TASK_cmc_sgsg_sub_group.alias("cmc_sgsg_sub_group").select(
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_sgsg_sub_group.CSSC_ID"), "\r", ""), "\n", ""), "\t", "").alias("CSSC_ID"),
    F.col("cmc_sgsg_sub_group.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("cmc_sgsg_sub_group.SGSG_CK").alias("SGSG_CK"),
    F.col("cmc_sgsg_sub_group.GRGR_CK").alias("GRGR_CK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_sgsg_sub_group.SGSG_ID"), "\r", ""), "\n", ""), "\t", "").alias("SGSG_ID"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cmc_sgsg_sub_group.GRGR_ID"), "\r", ""), "\n", ""), "\t", "").alias("GRGR_ID")
)

# Deduplicate for "hf_cust_svc_task_sgsg_grgr_id" (Scenario A)
df_hf_cust_svc_task_sgsg_grgr_id = dedup_sort(
    df_StripField5,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO","SGSG_CK","GRGR_CK"],
    sort_cols=[]
)

# ---------------------------------------------------------
# StripField7 (CTransformerStage) => outputs to "hf_cust_svc_task_clsd_dt"
# ---------------------------------------------------------

df_StripField7 = df_CMC_CSTK_TASK_cer_atuf_usefld.alias("cer_atuf_usefld").select(
    F.col("cer_atuf_usefld.ATSY_ID").alias("ATSY_ID"),
    F.col("cer_atuf_usefld.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    F.col("cer_atuf_usefld.ATUF_DATE1").alias("ATUF_DATE1")
)

# Deduplicate for "hf_cust_svc_task_clsd_dt" (Scenario A)
df_hf_cust_svc_task_clsd_dt = dedup_sort(
    df_StripField7,
    partition_cols=["ATSY_ID","ATXR_DEST_ID"],
    sort_cols=[]
)

# ---------------------------------------------------------
# StripField3 (CTransformerStage) => outputs to "hf_cust_svc_task_atxrdest_id"
# ---------------------------------------------------------

df_StripField3 = df_CMC_CSTK_TASK_cer_atxrattach_extr1.alias("cer_atxrattach_extr1").select(
    F.col("cer_atxrattach_extr1.ATSY_ID").alias("ATSY_ID"),
    F.col("cer_atxrattach_extr1.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.col("cer_atxrattach_extr1.ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT"),
    F.col("cer_atxrattach_extr1.ATXR_DEST_ID").alias("ATXR_DEST_ID")
)

# Deduplicate for "hf_cust_svc_task_atxrdest_id" (Scenario A)
df_hf_cust_svc_task_atxrdest_id = dedup_sort(
    df_StripField3,
    partition_cols=["ATSY_ID","ATXR_SOURCE_ID","ATXR_LAST_UPD_DT"],
    sort_cols=[]
)

# ---------------------------------------------------------
# hf_cust_svc_task_atxrdest_id => outputs to "atufuser" => input for "StripField6" as lookup
# StripField6 has:
#   Primary link => df_CMC_CSTK_TASK_cer_atxrattach_extr
#   Lookup link => "atufuser" from "hf_cust_svc_task_atxrdest_id"
# ---------------------------------------------------------

df_cer_atxrattach_extr_prime = df_CMC_CSTK_TASK_cer_atxrattach_extr.alias("cer_atxrattach_extr")

df_hf_cust_svc_task_atxrdest_id_dedup = df_hf_cust_svc_task_atxrdest_id.alias("atufuser")

df_StripField6 = df_cer_atxrattach_extr_prime.join(
    df_hf_cust_svc_task_atxrdest_id_dedup,
    on=[
        df_cer_atxrattach_extr_prime["ATSY_ID"] == df_hf_cust_svc_task_atxrdest_id_dedup["ATSY_ID"],
        df_cer_atxrattach_extr_prime["ATXR_SOURCE_ID"] == df_hf_cust_svc_task_atxrdest_id_dedup["ATXR_SOURCE_ID"],
        df_cer_atxrattach_extr_prime["ATXR_LAST_UPD_DT"] == df_hf_cust_svc_task_atxrdest_id_dedup["ATXR_LAST_UPD_DT"]
    ],
    how="left"
).select(
    F.col("cer_atxrattach_extr.CSSC_ID").alias("CSSC_ID"),
    F.col("cer_atxrattach_extr.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("cer_atxrattach_extr.ATSY_ID"), "\r", ""), "\n", ""), "\t", "").alias("ATSY_ID"),
    F.col("atufuser.ATXR_DEST_ID").alias("ATXR_DEST_ID")
)

# Deduplicate for "hf_cust_svc_task_atsy_id" (Scenario A) 
# (StripField6 => hf_cust_svc_task_atsy_id => next pinned to "StripField")
df_hf_cust_svc_task_atsy_id = dedup_sort(
    df_StripField6,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO","ATSY_ID","ATXR_DEST_ID"],
    sort_cols=[]
)

# ---------------------------------------------------------
# StripField (CTransformerStage):
#   Primary link => df_CMC_CSTK_TASK_Extract1
#   Lookup link => df_hf_cust_svc_task_atsy_id
# ---------------------------------------------------------

df_Extract1_prime = df_CMC_CSTK_TASK_Extract1.alias("Extract1")
df_hf_cust_svc_task_atsy_id_dedup = df_hf_cust_svc_task_atsy_id.alias("atuf")

df_StripField = df_Extract1_prime.join(
    df_hf_cust_svc_task_atsy_id_dedup,
    on=[
        df_Extract1_prime["CSSC_ID"] == df_hf_cust_svc_task_atsy_id_dedup["CSSC_ID"],
        df_Extract1_prime["CSTK_SEQ_NO"] == df_hf_cust_svc_task_atsy_id_dedup["CSTK_SEQ_NO"]
    ],
    how="left"
).select(
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSSC_ID"), "\r", ""), "\n", ""), "\t", "").alias("CSSC_ID"),
    F.col("Extract1.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("Extract1.GRGR_CK").alias("GRGR_CK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_INPUT_USID"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_INPUT_USID"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_LAST_UPD_USID"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_LAST_UPD_USID"),
    F.col("Extract1.MEME_CK").alias("MEME_CK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.PRPR_ID"), "\r", ""), "\n", ""), "\t", "").alias("PRPR_ID"),
    F.col("Extract1.SGSG_CK").alias("SGSG_CK"),
    F.col("Extract1.SBSB_CK").alias("SBSB_CK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_MCTR_CATG"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_MCTR_CATG"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSPD_CAT"), "\r", ""), "\n", ""), "\t", "").alias("CSPD_CAT"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_MCTR_POC"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_MCTR_POC"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_CUST_IND"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_CUST_IND"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_MCTR_REAS"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_MCTR_REAS"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_ABOUT_TYPE"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_ABOUT_TYPE"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_PAGE_TYPE"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_PAGE_TYPE"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_MCTR_PRTY"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_MCTR_PRTY"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_STS"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_STS"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_MCTR_SUBJ"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_MCTR_SUBJ"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_COMP_IND"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_COMP_IND"),
    F.col("Extract1.CSTK_INPUT_DTM").alias("CSTK_INPUT_DTM"),
    F.col("Extract1.CSTK_LAST_UPD_DTM").alias("CSTK_LAST_UPD_DTM"),
    F.col("Extract1.CSTK_NEXT_REV_DT").alias("CSTK_NEXT_REV_DT"),
    F.col("Extract1.CSTK_RECD_DT").alias("CSTK_RECD_DT"),
    F.col("Extract1.CSTK_NEXT_REV_INT").alias("CSTK_NEXT_REV_INT"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_CUSTOMER"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_CUSTOMER"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.USUS_SITE_CODE"), "\r", ""), "\n", ""), "\t", "").alias("USUS_SITE_CODE"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_INPUT_UDEPT"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_INPUT_UDEPT"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_SUMMARY"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_SUMMARY"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Extract1.CSTK_ASSIGN_USID"), "\r", ""), "\n", ""), "\t", "").alias("CSTK_ASSIGN_USID"),
    F.col("Extract1.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("atuf.ATSY_ID"), "\r", ""), "\n", ""), "\t", "").alias("ATSY_ID"),
    F.col("atuf.ATXR_DEST_ID").alias("ATXR_DEST_ID")
)

# ---------------------------------------------------------
# Joining_souces (CTransformerStage)
#   Primary link => df_StripField
#   Lookup links => 
#       df_hf_cust_svc_task_csts_sttus (CSTS_STS) 
#       df_hf_cust_svc_task_usus_id
#       df_hf_cust_svc_task_pdpd_id
#       df_hf_cust_svc_task_sgsg_grgr_id
#       df_hf_cust_svc_task_clsd_dt
# ---------------------------------------------------------

df_hf_cust_svc_task_csts_sttus_lk = df_hf_cust_svc_task_csts_sttus.alias("cmc_csts_sttus1")
df_hf_cust_svc_task_usus_id_lk = df_hf_cust_svc_task_usus_id.alias("cer_usgu_usergrp1")
df_hf_cust_svc_task_pdpd_id_lk = df_hf_cust_svc_task_pdpd_id.alias("cmc_mepe_elig1")
df_hf_cust_svc_task_sgsg_grgr_id_lk = df_hf_cust_svc_task_sgsg_grgr_id.alias("cmc_sgsg_sub_group1")
df_hf_cust_svc_task_clsd_dt_lk = df_hf_cust_svc_task_clsd_dt.alias("cer_atuf_usefld1")

df_Joining_souces_1 = df_StripField.alias("Strip1").join(
    df_hf_cust_svc_task_csts_sttus_lk,
    on=[
        F.col("Strip1.CSSC_ID") == F.col("cmc_csts_sttus1.CSSC_ID"),
        F.col("Strip1.CSTK_SEQ_NO") == F.col("cmc_csts_sttus1.CSTK_SEQ_NO")
    ],
    how="left"
).join(
    df_hf_cust_svc_task_usus_id_lk,
    on=[
        F.col("Strip1.CSSC_ID") == F.col("cer_usgu_usergrp1.CSSC_ID"),
        F.col("Strip1.CSTK_SEQ_NO") == F.col("cer_usgu_usergrp1.CSTK_SEQ_NO"),
        F.col("Strip1.CSTK_MCTR_SUBJ") == F.col("cer_usgu_usergrp1.CSTK_MCTR_SUBJ"),
        F.col("Strip1.CSTK_INPUT_USID") == F.col("cer_usgu_usergrp1.USUS_ID")
    ],
    how="left"
).join(
    df_hf_cust_svc_task_pdpd_id_lk,
    on=[
        F.col("Strip1.CSSC_ID") == F.col("cmc_mepe_elig1.CSSC_ID"),
        F.col("Strip1.CSTK_SEQ_NO") == F.col("cmc_mepe_elig1.CSTK_SEQ_NO"),
        F.col("Strip1.MEME_CK") == F.col("cmc_mepe_elig1.MEME_CK"),
        F.col("Strip1.CSPD_CAT") == F.col("cmc_mepe_elig1.CSPD_CAT")
    ],
    how="left"
).join(
    df_hf_cust_svc_task_sgsg_grgr_id_lk,
    on=[
        F.col("Strip1.CSSC_ID") == F.col("cmc_sgsg_sub_group1.CSSC_ID"),
        F.col("Strip1.CSTK_SEQ_NO") == F.col("cmc_sgsg_sub_group1.CSTK_SEQ_NO"),
        F.col("Strip1.SGSG_CK") == F.col("cmc_sgsg_sub_group1.SGSG_CK"),
        F.col("Strip1.GRGR_CK") == F.col("cmc_sgsg_sub_group1.GRGR_CK")
    ],
    how="left"
).join(
    df_hf_cust_svc_task_clsd_dt_lk,
    on=[
        F.col("Strip1.ATSY_ID") == F.col("cer_atuf_usefld1.ATSY_ID"),
        F.col("Strip1.ATXR_DEST_ID") == F.col("cer_atuf_usefld1.ATXR_DEST_ID")
    ],
    how="left"
)

df_Joining_souces = df_Joining_souces_1.select(
    F.trim(F.col("Strip1.CSSC_ID")).alias("CSSC_ID"),
    F.col("Strip1.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.trim(F.col("Strip1.GRGR_CK")).alias("GRGR_CK"),
    F.trim(F.col("Strip1.CSTK_INPUT_USID")).alias("CSTK_INPUT_USID"),
    F.trim(F.col("Strip1.CSTK_LAST_UPD_USID")).alias("CSTK_LAST_UPD_USID"),
    F.trim(F.col("Strip1.MEME_CK")).alias("MEME_CK"),
    F.trim(F.col("Strip1.PRPR_ID")).alias("PRPR_ID"),
    F.trim(F.col("Strip1.SGSG_CK")).alias("SGSG_CK"),
    F.trim(F.col("Strip1.SBSB_CK")).alias("SBSB_CK"),
    F.trim(F.col("Strip1.CSTK_MCTR_CATG")).alias("CSTK_MCTR_CATG"),
    F.trim(F.col("Strip1.CSPD_CAT")).alias("CSPD_CAT"),
    F.trim(F.col("Strip1.CSTK_MCTR_POC")).alias("CSTK_MCTR_POC"),
    F.trim(F.col("Strip1.CSTK_CUST_IND")).alias("CSTK_CUST_IND"),
    F.trim(F.col("Strip1.CSTK_MCTR_REAS")).alias("CSTK_MCTR_REAS"),
    F.trim(F.col("Strip1.CSTK_ABOUT_TYPE")).alias("CSTK_ABOUT_TYPE"),
    F.trim(F.col("Strip1.CSTK_PAGE_TYPE")).alias("CSTK_PAGE_TYPE"),
    F.trim(F.col("Strip1.CSTK_MCTR_PRTY")).alias("CSTK_MCTR_PRTY"),
    F.trim(F.col("Strip1.CSTK_STS")).alias("CSTK_STS"),
    F.trim(F.col("Strip1.CSTK_MCTR_SUBJ")).alias("CSTK_MCTR_SUBJ"),
    F.trim(F.col("Strip1.CSTK_COMP_IND")).alias("CSTK_COMP_IND"),
    F.expr("FORMAT.DATE(Strip1.CSTK_INPUT_DTM, 'SYBASE', 'Timestamp', 'DB2TIMESTAMP')").alias("CSTK_INPUT_DTM"),
    F.expr("FORMAT.DATE(Strip1.CSTK_LAST_UPD_DTM, 'SYBASE', 'Timestamp', 'DB2TIMESTAMP')").alias("CSTK_LAST_UPD_DTM"),
    F.expr("FORMAT.DATE(Strip1.CSTK_NEXT_REV_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("CSTK_NEXT_REV_DT"),
    F.expr("FORMAT.DATE(Strip1.CSTK_RECD_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("CSTK_RECD_DT"),
    F.trim(F.col("Strip1.CSTK_NEXT_REV_INT")).alias("CSTK_NEXT_REV_INT"),
    F.trim(F.col("Strip1.CSTK_CUSTOMER")).alias("CSTK_CUSTOMER"),
    F.trim(F.col("Strip1.USUS_SITE_CODE")).alias("USUS_SITE_CODE"),
    F.trim(F.col("Strip1.CSTK_INPUT_UDEPT")).alias("CSTK_INPUT_UDEPT"),
    F.trim(F.col("Strip1.CSTK_SUMMARY")).alias("CSTK_SUMMARY"),
    F.trim(F.col("Strip1.CSTK_ASSIGN_USID")).alias("CSTK_ASSIGN_USID"),
    F.expr("If(cmc_mepe_elig1.CSPD_CAT = 0 and cmc_mepe_elig1.MEME_CK = 0 or IsNull(cmc_mepe_elig1.CSPD_CAT) = @TRUE and IsNull(cmc_mepe_elig1.MEME_CK) = @TRUE then 'NA' else cmc_mepe_elig1.PDPD_ID)").alias("PDPD_ID"),
    F.trim(F.col("cmc_sgsg_sub_group1.SGSG_ID")).alias("SGSG_ID"),
    F.trim(F.col("cmc_sgsg_sub_group1.GRGR_ID")).alias("GRGR_ID"),
    F.col("cer_usgu_usergrp1.USUS_ID").alias("USUS_ID"),
    F.expr("FORMAT.DATE(cer_atuf_usefld1.ATUF_DATE1, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("ATUF_DATE1"),
    F.expr("FORMAT.DATE(cmc_csts_sttus1.CSTS_STS_DTM, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("CSTS_STS_DTM"),
    F.col("cer_atuf_usefld1.ATSY_ID").alias("ATSY_ID"),
    F.expr("""If(Strip1.CSTK_STS <> 'CL' 
             then '1753-01-01' 
             else  
             if(Strip1.ATSY_ID = 'CSBD' and FORMAT.DATE(cer_atuf_usefld1.ATUF_DATE1, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD') <> '1753-01-01' 
               and (FORMAT.DATE(cer_atuf_usefld1.ATUF_DATE1, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD') >= FORMAT.DATE(Strip1.CSTK_INPUT_DTM, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD') 
                 or FORMAT.DATE(cer_atuf_usefld1.ATUF_DATE1, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD') >= FORMAT.DATE(Strip1.CSTK_RECD_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD'))) 
               then FORMAT.DATE(cer_atuf_usefld1.ATUF_DATE1, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD') 
             else FORMAT.DATE(cmc_csts_sttus1.CSTS_STS_DTM, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD'))""").alias("CLSD_DT_SK"),
    F.col("cmc_csts_sttus1.CSTS_STS").alias("CSTS_STS"),
    F.trim(F.col("cer_usgu_usergrp1.CSTK_INPUT_USID")).alias("CSTK_INPUT_USID_1"),
    F.col("cer_usgu_usergrp1.CSTK_MCTR_SUBJ").alias("CSTK_MCTR_SUBJ_1")
)

# Deduplicate for "hf_cust_svc_task_dedup" (Scenario A)
df_hf_cust_svc_task_dedup = dedup_sort(
    df_Joining_souces,
    partition_cols=["CSSC_ID","CSTK_SEQ_NO"],
    sort_cols=[]
)

# ---------------------------------------------------------
# Next: BusinessLogic (CTransformerStage)
#   Primary link => df_hf_cust_svc_task_dedup
# ---------------------------------------------------------

df_BusinessLogic = df_hf_cust_svc_task_dedup.alias("FnlExtract").select(
    F.expr("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("FnlExtract.CSSC_ID").alias("CUST_SVC_ID"),
    F.col("FnlExtract.CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.expr("CurrDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.expr("concat('FACETS',';',FnlExtract.CSSC_ID,';',FnlExtract.CSTK_SEQ_NO)").alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FnlExtract.CSSC_ID").alias("CUST_SVC_SK"),
    F.expr("If ((length(FnlExtract.GRGR_CK)=0) OR IsNull(FnlExtract.GRGR_CK)=@TRUE) then 'NA' else (FnlExtract.GRGR_CK)").alias("GRP_SK"),
    F.expr("If ((length(FnlExtract.CSTK_INPUT_USID)=0) OR IsNull(FnlExtract.CSTK_INPUT_USID)=@TRUE) then 'NA' else (FnlExtract.CSTK_INPUT_USID)").alias("INPT_USER_SK"),
    F.expr("If ((length(FnlExtract.CSTK_LAST_UPD_USID)=0) OR IsNull(FnlExtract.CSTK_LAST_UPD_USID)=@TRUE) then 'NA' else (FnlExtract.CSTK_LAST_UPD_USID)").alias("LAST_UPDT_USER_SK"),
    F.expr("If ((length(FnlExtract.MEME_CK)=0) OR IsNull(FnlExtract.MEME_CK)=@TRUE) then 'NA' else UpCase(FnlExtract.MEME_CK)").alias("MBR_SK"),
    F.expr("If ((length(FnlExtract.PDPD_ID)=0) or IsNull(FnlExtract.PDPD_ID)=@TRUE) then 'NA' else FnlExtract.PDPD_ID").alias("PROD_SK"),
    F.expr("If ((length(FnlExtract.PRPR_ID)=0) OR IsNull(FnlExtract.PRPR_ID)=@TRUE) then 'NA' else UpCase(FnlExtract.PRPR_ID)").alias("PROV_SK"),
    F.expr("If ((length(FnlExtract.SGSG_CK)=0) OR IsNull(FnlExtract.SGSG_CK)=@TRUE) then 'NA' else UpCase(FnlExtract.SGSG_CK)").alias("SUBGRP_SK"),
    F.expr("If ((length(FnlExtract.SBSB_CK)=0) OR IsNull(FnlExtract.SBSB_CK)=@TRUE) then 'NA' else UpCase(FnlExtract.SBSB_CK)").alias("SUB_SK"),
    F.expr("If ((length(FnlExtract.CSTK_MCTR_CATG)=0) OR IsNull(FnlExtract.CSTK_MCTR_CATG)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_MCTR_CATG)").alias("CUST_SVC_TASK_CAT_CD_SK"),
    F.expr("If ((length(FnlExtract.CSPD_CAT)=0) OR IsNull(FnlExtract.CSPD_CAT)=@TRUE) then 'NA' else UpCase(FnlExtract.CSPD_CAT)").alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_MCTR_POC)=0) OR IsNull(FnlExtract.CSTK_MCTR_POC)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_MCTR_POC)").alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_CUST_IND)=0) OR IsNull(FnlExtract.CSTK_CUST_IND)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_CUST_IND)").alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
    F.expr("""If(FnlExtract.CSTK_STS='CL' 
                 then 
                   if((length(FnlExtract.CSTK_MCTR_REAS)=0) or IsNull(FnlExtract.CSTK_MCTR_REAS)=@TRUE) 
                     then 'NA' else FnlExtract.CSTK_MCTR_REAS 
                 else 'NA')""").alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_INPUT_USID_1)=0) or IsNull(FnlExtract.CSTK_INPUT_USID_1)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_INPUT_USID_1)").alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_ABOUT_TYPE)=0) OR IsNull(FnlExtract.CSTK_ABOUT_TYPE)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_ABOUT_TYPE)").alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_PAGE_TYPE)=0) OR IsNull(FnlExtract.CSTK_PAGE_TYPE)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_PAGE_TYPE)").alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_MCTR_PRTY)=0) OR IsNull(FnlExtract.CSTK_MCTR_PRTY)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_MCTR_PRTY)").alias("CUST_SVC_TASK_PRTY_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_STS)=0) OR IsNull(trim(length(FnlExtract.CSTK_STS)))=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_STS)").alias("CUST_SVC_TASK_STTUS_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_MCTR_REAS)=0) OR IsNull(FnlExtract.CSTK_MCTR_REAS)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_MCTR_REAS)").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_MCTR_SUBJ)=0) OR IsNull(FnlExtract.CSTK_MCTR_SUBJ)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_MCTR_SUBJ)").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
    F.expr("If ((length(FnlExtract.CSTK_COMP_IND)=0) OR IsNull(FnlExtract.CSTK_COMP_IND)=@TRUE) then 'X' else UpCase(FnlExtract.CSTK_COMP_IND)").alias("CMPLNT_IN"),
    F.expr("""If trim(FnlExtract.CSTK_STS='CL') 
               then  
                 if((length(FnlExtract.CLSD_DT_SK)=0) or IsNull(FnlExtract.CLSD_DT_SK)=@TRUE) 
                   then '1753-01-01' 
                 else FnlExtract.CLSD_DT_SK 
               else '1753-01-01'""").alias("CLSD_DT_SK"),
    F.col("FnlExtract.CSTK_INPUT_DTM").alias("INPT_DTM"),
    F.col("FnlExtract.CSTK_LAST_UPD_DTM").alias("LAST_UPDT_DTM"),
    F.col("FnlExtract.CSTK_NEXT_REV_DT").alias("NEXT_RVW_DT_SK"),
    F.col("FnlExtract.CSTK_RECD_DT").alias("RCVD_DT_SK"),
    F.expr("If ((length(FnlExtract.CSTK_NEXT_REV_INT)=0) OR IsNull(FnlExtract.CSTK_NEXT_REV_INT)=@TRUE) then 0 else UpCase(FnlExtract.CSTK_NEXT_REV_INT)").alias("NEXT_RVW_INTRVL_DAYS"),
    F.expr("If ((length(FnlExtract.CSTK_CUSTOMER)=0) OR IsNull(FnlExtract.CSTK_CUSTOMER)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_CUSTOMER)").alias("CUST_ID"),
    F.expr("If ((length(FnlExtract.USUS_SITE_CODE)=0) OR IsNull(FnlExtract.USUS_SITE_CODE)=@TRUE) then 'NA' else UpCase(FnlExtract.USUS_SITE_CODE)").alias("CUST_SVC_TASK_USER_SITE_ID"),
    F.expr("If ((length(FnlExtract.CSTK_INPUT_UDEPT)=0) OR IsNull(FnlExtract.CSTK_INPUT_UDEPT)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_INPUT_UDEPT)").alias("INPT_USER_CC_ID"),
    F.expr("If ((length(FnlExtract.CSTK_SUMMARY)=0) OR IsNull(FnlExtract.CSTK_SUMMARY)=@TRUE) then @NULL else UpCase(FnlExtract.CSTK_SUMMARY)").alias("SUM_DESC"),
    F.expr("If ((length(FnlExtract.SGSG_ID)=0) OR IsNull(FnlExtract.SGSG_ID)=@TRUE) then 'NA' else UpCase(FnlExtract.SGSG_ID)").alias("SGSG_ID"),
    F.expr("If ((length(FnlExtract.GRGR_ID)=0) OR IsNull(FnlExtract.GRGR_ID)=@TRUE) then 'NA' else UpCase(FnlExtract.GRGR_ID)").alias("GRGR_ID"),
    F.col("FnlExtract.USUS_ID").alias("USUS_ID"),
    F.expr("If ((length(FnlExtract.CSTK_ASSIGN_USID)=0) OR IsNull(FnlExtract.CSTK_ASSIGN_USID)=@TRUE) then 'NA' else UpCase(FnlExtract.CSTK_ASSIGN_USID)").alias("CSTK_ASSIGN_USID"),
    F.col("FnlExtract.ATUF_DATE1").alias("ATUF_DATE1"),
    F.col("FnlExtract.CSTS_STS_DTM").alias("CSTS_STS_DTM"),
    F.col("FnlExtract.CSTS_STS").alias("CSTS_STS"),
    F.col("FnlExtract.CSTK_MCTR_SUBJ_1").alias("CSTK_MCTR_SUBJ_1"),
    F.col("FnlExtract.CSTK_STS").alias("CSTK_STS")
)

df_BusinessLogic_min = df_hf_cust_svc_task_dedup.alias("FnlExtract").select(
    F.expr("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("FnlExtract.CSSC_ID").alias("CUST_SVC_ID"),
    F.col("FnlExtract.CSTK_SEQ_NO").alias("TASK_SEQ_NO")
)

# ---------------------------------------------------------
# CustSvcTaskPK (Shared Container) - 2 inputs, 1 output
# Inputs: 
#   Link "Transform" => df_BusinessLogic_min
#   Link "AllCol" => df_BusinessLogic
# Output => "Key"
# ---------------------------------------------------------

params_CustSvcTaskPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk
}
df_Key = CustSvcTaskPK(df_BusinessLogic_min, df_BusinessLogic, params_CustSvcTaskPK)

# ---------------------------------------------------------
# IdsCustSvcTask (CSeqFileStage) => writes to "IdsCustSvcTaskExtr.CSTask.dat.#RunID#" 
# ---------------------------------------------------------

# Translate file path:
# "key/IdsCustSvcTaskExtr.CSTask.dat.#RunID#" does not have "landing" or "external",
# so it belongs to f"{adls_path}/key/IdsCustSvcTaskExtr.CSTask.dat.{RunID}"
out_path_IdsCustSvcTask = f"{adls_path}/key/IdsCustSvcTaskExtr.CSTask.dat.{RunID}"

# Write the final "df_Key" DataFrame to the .dat file
# (CSeqFileStage => preserve extension .dat, use write_files with the correct delimiter, no header)
write_files(
    df_Key,
    out_path_IdsCustSvcTask,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ---------------------------------------------------------
# Facets_Source (ODBCConnector) => single query => "Snapshot"
# ---------------------------------------------------------
query_Facets_Source = f"""SELECT 
CSTK.CSSC_ID,
CSTK.CSTK_SEQ_NO,
CSTK.CSTK_CUSTOMER,
GRGR.GRGR_ID

FROM 
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR,
{FacetsOwner}.CMC_CSTK_TASK CSTK 
left outer join {FacetsOwner}.CMC_GRGR_GROUP GRGR
on CSTK.GRGR_CK = GRGR.GRGR_CK

WHERE 
CSTK.CSSC_ID = DRVR.CSSC_ID 
AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_Facets_Source)
    .load()
)

# ---------------------------------------------------------
# Transform (CTransformerStage) after Facets_Source => outputs to "Snapshot_File"
# ---------------------------------------------------------
df_Transform_facets = df_Facets_Source.alias("Snapshot").select(
    F.expr("GetFkeyCodes('IDS', 1, 'SOURCE SYSTEM', 'FACETS', 'X')").alias("SRC_SYS_CD_SK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("Snapshot.CSSC_ID"), "\r", ""), "\n", ""), "\t", "").alias("CUST_SVC_ID"),
    F.col("Snapshot.CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.expr("""If ((length(Snapshot.GRGR_ID)=0) OR IsNull(Snapshot.GRGR_ID)) 
                then 1 
              else GetFkeyGrp('FACETS', 100, trim(Snapshot.GRGR_ID), 'X')""").alias("GRP_SK"),
    F.regexp_replace(F.regexp_replace(F.regexp_replace(F.expr("""If (Snapshot.CSTK_CUSTOMER=' ') 
                then 'NA' 
                else UpCase(Snapshot.CSTK_CUSTOMER)"""), "\r", ""), "\n", ""), "\t", "").alias("CUST_ID")
)

# ---------------------------------------------------------
# Snapshot_File (CSeqFileStage) => writes "B_CUST_SVC_TASK.FACETS.dat" in "load" folder
# => f"{adls_path}/load/B_CUST_SVC_TASK.FACETS.dat"
# ---------------------------------------------------------

out_path_Snapshot_File = f"{adls_path}/load/B_CUST_SVC_TASK.FACETS.dat"

write_files(
    df_Transform_facets,
    out_path_Snapshot_File,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)