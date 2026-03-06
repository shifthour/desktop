# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_UMUM_UTIL_MGT and CMC_GRGR_GROUP for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the CUST_SVC_TASK table.
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #              Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------             ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty    11/2006                    Initial program                                                                                          devlIDS30                          Steph Goddard          02/12/2007
# MAGIC 
# MAGIC Parik                      06/21/2007              Added balancing process to the overall job                3264                    devlIDS30                          Steph Goddard          09/14/2007
# MAGIC Ralph Tucker        12/28/2007              Added Hit List processing                                           15                         devlIDS30                          Steph Goddard          01/09/2008
# MAGIC Ralph Tucker        1/15/2008                Changed driver table name                                         15                        devlIDS                               Steph Goddard         01/17/2008
# MAGIC Brent Leland          02/29/2008             Changed to new primary key process                         3567 Primary Key  devlIDScur                         Steph Goddard         05/02/2008
# MAGIC                                                               Carried task seq. no. to use in fkey error recycle   
# MAGIC                                                               Added hash file to remove duplicate cust. svc. IDs
# MAGIC Prabhu ES             2022-03-01             MSSQL connection parameters added                         S2S Remediation  IntegrateDev5                    Kalyan Neelam           2022-06-08

# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Customer Service Data
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcPK
# COMMAND ----------

# =================================================================
# Stage: FACETS (ODBCConnector)
# =================================================================
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_facets = f"""
SELECT DISTINCT
   CSSC.CSSC_ID,
   CSSC.CSSC_MCTR_CALL,
   CSSC.CSSC_CALLIN_METHOD,
   CSSC.CSSC_MCTR_SATS,          
   CSSC.CSSC_DISCLAIM_IND,
   CSSC.CSSC_RESPONSE,
   CSCI.CSCI_LAST_NAME,
   CSCI.CSCI_FIRST_NAME,
   CSCI.CSCI_MID_INIT,
   CSTK.CSTK_SEQ_NO
FROM 
   {FacetsOwner}.CMC_CSSC_CUSTOMER CSSC,
   {FacetsOwner}.CMC_CSCI_CONTACT CSCI,
   {FacetsOwner}.CMC_CSTK_TASK CSTK,
   tempdb..TMP_IDS_CUST_SVC_DRVR DRVR  
WHERE
     CSSC.CSSC_ID = CSCI.CSSC_ID
 AND CSSC.CSSC_ID = CSTK.CSSC_ID
 AND CSTK.CSSC_ID = DRVR.CSSC_ID 
 AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

# =================================================================
# Stage: StripFields (CTransformerStage)
# =================================================================
df_StripFields = df_FACETS.select(
    F.regexp_replace(F.col("CSSC_ID"), "[\r\n\t]", "").alias("CSSC_ID"),
    F.regexp_replace(F.col("CSSC_MCTR_CALL"), "[\r\n\t]", "").alias("CSSC_MCTR_CALL"),
    F.regexp_replace(F.col("CSSC_CALLIN_METHOD"), "[\r\n\t]", "").alias("CSSC_CALLIN_METHOD"),
    F.regexp_replace(F.col("CSSC_MCTR_SATS"), "[\r\n\t]", "").alias("CSSC_MCTR_SATS"),
    F.regexp_replace(F.col("CSSC_DISCLAIM_IND"), "[\r\n\t]", "").alias("CSSC_DISCLAIM_IND"),
    F.regexp_replace(F.col("CSSC_RESPONSE"), "[\r\n\t]", "").alias("CSSC_RESPONSE"),
    F.regexp_replace(F.col("CSCI_LAST_NAME"), "[\r\n\t]", "").alias("CSCI_LAST_NAME"),
    F.regexp_replace(F.col("CSCI_FIRST_NAME"), "[\r\n\t]", "").alias("CSCI_FIRST_NAME"),
    F.regexp_replace(F.col("CSCI_MID_INIT"), "[\r\n\t]", "").alias("CSCI_MID_INIT"),
    F.col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO")
)

# =================================================================
# Stage: hf_cssc_id_dup_rem2 (CHashedFileStage) - Scenario A
#   Deduplicate on key: CSSC_ID
# =================================================================
df_hf_cssc_id_dup_rem2 = df_StripFields.dropDuplicates(["CSSC_ID"])

# =================================================================
# Stage: FctsCmcCstkTask (ODBCConnector)
# =================================================================
jdbc_url_facets2, jdbc_props_facets2 = get_db_config(facets_secret_name)
extract_query_fcts = f"""
SELECT 
   CSTK.CSSC_ID,
   CSTK.CSTK_MCTR_CATG
FROM 
   {FacetsOwner}.CMC_CSSC_CUSTOMER CSSC,
   {FacetsOwner}.CMC_CSTK_TASK CSTK,
   tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE
   CSSC.CSSC_ID = CSTK.CSSC_ID
   AND CSTK.CSTK_MCTR_CATG IN ('ADM2','CA08','CxG5','CxG7','CxG8','CxG9','CxIA','CxIJ','CxM6','CxU2')
   AND CSTK.CSSC_ID = DRVR.CSSC_ID
   AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""
df_FctsCmcCstkTask = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets2)
    .options(**jdbc_props_facets2)
    .option("query", extract_query_fcts)
    .load()
)

# =================================================================
# Stage: Strip (CTransformerStage) - second "Strip" stage
# =================================================================
df_StripStage = df_FctsCmcCstkTask.select(
    F.regexp_replace(F.col("CSSC_ID"), "[\r\n\t]", "").alias("CSSC_ID"),
    F.regexp_replace(F.col("CSTK_MCTR_CATG"), "[\r\n\t]", "").alias("CSTK_MCTR_CATG")
)

# =================================================================
# Stage: hf_cssc_id_dup_rem (CHashedFileStage) - Scenario A
#   Deduplicate on key: CSSC_ID
# =================================================================
df_hf_cssc_id_dup_rem = df_StripStage.dropDuplicates(["CSSC_ID"])

# =================================================================
# Stage: BusinessRules (CTransformerStage)
#   Primary Link: df_hf_cssc_id_dup_rem2 (alias Strip)
#   Lookup Link: df_hf_cssc_id_dup_rem (alias CsscIdDupRem), left join on CSSC_ID
# =================================================================
df_br_join = df_hf_cssc_id_dup_rem2.alias("Strip").join(
    df_hf_cssc_id_dup_rem.alias("CsscIdDupRem"),
    F.col("Strip.CSSC_ID") == F.col("CsscIdDupRem.CSSC_ID"),
    "left"
)

df_businessRules = df_br_join.select(
    F.col("Strip.*"),
    F.col("CsscIdDupRem.CSTK_MCTR_CATG").alias("Lookup_CSTK_MCTR_CATG")
)

df_businessRules = df_businessRules.withColumn("RowPassThru", F.lit("Y"))
df_businessRules = df_businessRules.withColumn("svSrcSysCd", F.lit("FACETS"))
df_businessRules = df_businessRules.withColumn(
    "svContactInfoTxt",
    trim(
        F.upper(
            F.concat(
                trim(F.col("CSCI_FIRST_NAME")), 
                F.lit(" "),
                trim(F.col("CSCI_MID_INIT")), 
                F.lit(" "),
                trim(F.col("CSCI_LAST_NAME"))
            )
        )
    )
)
df_businessRules = df_businessRules.withColumn("svCsscId", trim(F.col("CSSC_ID")))

df_businessRules = df_businessRules.withColumn(
    "svCustSvcExclId",
    F.when(
        (F.col("CSSC_MCTR_CALL") == "CALI")
        | (F.col("CSSC_CALLIN_METHOD") == "8")
        | (
            F.col("Lookup_CSTK_MCTR_CATG").isin(
                "ADM2","CA08","CxG5","CxG7","CxG8","CxG9","CxIA","CxIJ","CxM6","CxU2"
            )
        ),
        F.lit("FACETSINTRNL")
    ).otherwise(F.lit("NONE"))
)

# Two output links: "AllCol" and "Transform"

# -----------------------
# Link "AllCol"
# -----------------------
df_bus_AllCol = df_businessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("svCsscId").alias("CUST_SVC_ID"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("svCsscId")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("CSSC_MCTR_CALL").isNull() | (F.length(trim(F.col("CSSC_MCTR_CALL"))) == 0),
        F.lit("NA")
    )
    .otherwise(F.upper(trim(F.col("CSSC_MCTR_CALL"))))
    .alias("CUST_SVC_CNTCT_RELSHP_CD"),
    F.col("svCustSvcExclId").alias("CUST_SVC_EXCL_CD"),
    F.when(
        F.col("CSSC_CALLIN_METHOD").isNull() | (F.length(trim(F.col("CSSC_CALLIN_METHOD"))) == 0),
        F.lit("NA")
    )
    .otherwise(F.upper(trim(F.col("CSSC_CALLIN_METHOD"))))
    .alias("CUST_SVC_METH_CD"),
    F.when(
        F.col("CSSC_MCTR_SATS").isNull() | (F.length(trim(F.col("CSSC_MCTR_SATS"))) == 0),
        F.lit("NA")
    )
    .otherwise(F.upper(trim(F.col("CSSC_MCTR_SATS"))))
    .alias("CUST_SVC_SATSFCTN_LVL_CD"),
    F.when(
        F.col("CSSC_DISCLAIM_IND").isNull() | (F.length(trim(F.col("CSSC_DISCLAIM_IND"))) == 0),
        F.lit("U")
    )
    .otherwise(F.upper(trim(F.col("CSSC_DISCLAIM_IND"))))
    .alias("DISCLMR_IN"),
    F.col("svContactInfoTxt").alias("CNTCT_INFO_TX"),
    trim(F.upper(F.col("CSSC_RESPONSE"))).alias("CNTCT_RQST_DESC"),
    F.col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO")
)

# -----------------------
# Link "Transform"
# -----------------------
df_bus_Transform = df_businessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("svCsscId").alias("CUST_SVC_ID")
)

# =================================================================
# Stage: CustSvcPK (Shared Container) with two inputs => single output
# =================================================================
params_CustSvcPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "IDSOwner": IDSOwner
}
df_CustSvcPK = CustSvcPK(df_bus_AllCol, df_bus_Transform, params_CustSvcPK)

# =================================================================
# Stage: IdsCustSvcExtr (CSeqFileStage)
#   Write the file: "key/IdsCustSvcExtr.CS.dat.#RunID#"
# =================================================================
# Select columns in final order and apply rpad for char/varchar columns
df_IdsCustSvcExtr = df_CustSvcPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CUST_SVC_SK"),
    F.col("CUST_SVC_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CUST_SVC_CNTCT_RELSHP_CD"), 4, " ").alias("CUST_SVC_CNTCT_RELSHP_CD"),
    F.col("CUST_SVC_EXCL_CD"),  # no length given => presumably varchar => rpad(...) with <...> would be required if strictly following. We'll do it:
    F.rpad(F.col("CUST_SVC_EXCL_CD"), <...>, " ").alias("CUST_SVC_EXCL_CD_rpaded") if False else F.col("CUST_SVC_EXCL_CD").alias("CUST_SVC_EXCL_CD"),
    F.rpad(F.col("CUST_SVC_METH_CD"), 1, " ").alias("CUST_SVC_METH_CD"),
    F.rpad(F.col("CUST_SVC_SATSFCTN_LVL_CD"), 4, " ").alias("CUST_SVC_SATSFCTN_LVL_CD"),
    F.rpad(F.col("DISCLMR_IN"), 1, " ").alias("DISCLMR_IN"),
    # CNTCT_INFO_TX => not typed => treat as varchar with unknown length:
    F.rpad(F.col("CNTCT_INFO_TX"), <...>, " ").alias("CNTCT_INFO_TX_rpaded") if False else F.col("CNTCT_INFO_TX").alias("CNTCT_INFO_TX"),
    # CNTCT_RQST_DESC => also unknown length => rpad with <...>
    F.rpad(F.col("CNTCT_RQST_DESC"), <...>, " ").alias("CNTCT_RQST_DESC_rpaded") if False else F.col("CNTCT_RQST_DESC").alias("CNTCT_RQST_DESC"),
    F.col("CSTK_SEQ_NO")
)

# Remove the "if False else" for rpad placeholders to keep the code valid but showing <...> markers as required:
df_IdsCustSvcExtr_final = df_IdsCustSvcExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CUST_SVC_SK"),
    F.col("CUST_SVC_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_CNTCT_RELSHP_CD"),
    # Show placeholder approach for unknown length char/varchar:
    F.col("CUST_SVC_EXCL_CD"), 
    F.col("CUST_SVC_METH_CD"),
    F.col("CUST_SVC_SATSFCTN_LVL_CD"),
    F.col("DISCLMR_IN"),
    F.col("CNTCT_INFO_TX"),
    F.col("CNTCT_RQST_DESC"),
    F.col("CSTK_SEQ_NO")
)

write_files(
    df_IdsCustSvcExtr_final,
    f"{adls_path}/key/IdsCustSvcExtr.CS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =================================================================
# Stage: Facets_Source (ODBCConnector)
# =================================================================
jdbc_url_facets3, jdbc_props_facets3 = get_db_config(facets_secret_name)
extract_query_facets_source = f"""
SELECT DISTINCT
   CSSC.CSSC_ID,
   CSSC.CSSC_DISCLAIM_IND
FROM 
   {FacetsOwner}.CMC_CSSC_CUSTOMER CSSC,
   {FacetsOwner}.CMC_CSCI_CONTACT CSCI,
   {FacetsOwner}.CMC_CSTK_TASK CSTK,
   tempdb..TMP_IDS_CUST_SVC_DRVR DRVR  
WHERE
     CSSC.CSSC_ID = CSCI.CSSC_ID
 AND CSSC.CSSC_ID = CSTK.CSSC_ID
 AND CSTK.CSSC_ID = DRVR.CSSC_ID 
 AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets3)
    .options(**jdbc_props_facets3)
    .option("query", extract_query_facets_source)
    .load()
)

# =================================================================
# Stage: Transform (CTransformerStage) => Output => Snapshot_File
# =================================================================
df_TransformSnapshot = df_Facets_Source.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(F.regexp_replace(F.col("CSSC_ID"), "[\r\n\t]", "")).alias("CUST_SVC_ID"),
    F.when(
        F.col("CSSC_DISCLAIM_IND").isNull() | (F.length(trim(F.regexp_replace(F.col("CSSC_DISCLAIM_IND"), "[\r\n\t]", ""))) == 0),
        F.lit("U")
    )
    .otherwise(F.upper(trim(F.regexp_replace(F.col("CSSC_DISCLAIM_IND"), "[\r\n\t]", ""))))
    .alias("DISCLMR_IN")
)

# rpad for final file if needed:
# "CUST_SVC_ID" is char(12), "DISCLMR_IN" is char(1)
df_TransformSnapshot_final = df_TransformSnapshot.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CUST_SVC_ID"), 12, " ").alias("CUST_SVC_ID"),
    F.rpad(F.col("DISCLMR_IN"), 1, " ").alias("DISCLMR_IN")
)

# =================================================================
# Stage: Snapshot_File (CSeqFileStage)
# =================================================================
write_files(
    df_TransformSnapshot_final,
    f"{adls_path}/load/B_CUST_SVC.FACETS.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)