# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmChkExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                 Pulls data from FACETS to a landing file for the IDS
# MAGIC                 Creates an output file in the key directory for input to the IDS transform job.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC                 tempdb..TMP_IDS_CLAIM
# MAGIC                 CMC_CLCL_CLAIM 
# MAGIC                 CMC_CLCK_CLM_CHECK 
# MAGIC                 CMC_CKCK_CHECK
# MAGIC                 CMC_CDML_CL_LINE
# MAGIC                 CMC_CLED_EDI_DATA
# MAGIC                 CER_ATXR_ATTACH_U
# MAGIC                 CER_ATMM_MEMO_D  
# MAGIC 
# MAGIC HASH FILES:
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                 STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                 Output file is created with a temp. name.  Job Control renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                 Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Hugh Sisson            10/05/2006                                    Originally Programmed                                                                 
# MAGIC Brent Leland            05/01/2007     IAD Prod. Supp.     Added current date parameter to job to use in transform stages   devlIDS30
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-16      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                           Steph Goddard          07/22/2008
# MAGIC 
# MAGIC Bhoomi Dasari        2009-06-21       4202                        Changed logic for the source extraction (RunOutIn)                    devlIDSnew                      Steph Goddard         06/22/2009
# MAGIC                                                                                         and added (RunIn)
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                   Kalyan Neelam          2022-06-10
# MAGIC Hugh Sisson           2022-07-11       S2S Remediation fix  Replace  four queries to properly handle left outer join               IntegrateDev5                   Reddy Sanam            2022-07-11

# MAGIC Hash file (hf_clm_chk_allcol) cleared from the container - ClmChkPK
# MAGIC Writing Sequential File to ../key
# MAGIC Extract Facets Data
# MAGIC This container is used in:
# MAGIC FctsClmChkExtr
# MAGIC NascoClmChkTrns
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    DateType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmChkPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDateParam = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Read the hashed file hf_clm_fcts_reversals (Scenario C => read from parquet)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
    .select(
        "CLCL_ID",
        "CLCL_CUR_STS",
        "CLCL_PAID_DT",
        "CLCL_ID_ADJ_TO",
        "CLCL_ID_ADJ_FROM"
    )
)

# ODBCConnector => FACETS => multiple output pins => read from the same connection with different queries

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# MedIn
extract_query_MedIn = f"""
SELECT DISTINCT 
    CLMCHK.CLCL_ID, 
    CLMCHK.CLCK_PAYEE_IND, 
    CLMCHK.LOBD_ID AS LOBD_ID, 
    CLMCHK.CKPY_REF_ID, 
    CLMCHK.CKPY_PAY_DT, 
    CLMCHK.CLCK_NET_AMT, 
    CLMCHK.CLCK_ORIG_AMT, 
    CLMCHK.CLCK_PYMT_OVRD_IND, 
    CHK.CKCK_SEQ_NO, 
    CHK.CKCK_CK_NO, 
    CHK.CKCK_TYPE, 
    CHK.CKCK_PAYEE_NAME, 
    CHK.CKCK_REISS_DT, 
    CHK.CKCK_PRINTED_DT, 
    HSA.LOBD_ID AS HSA_LOBD_ID
FROM tempdb..{DriverTable} DT,
     {FacetsOwner}.CMC_CDML_CL_LINE CLMLN,
     {FacetsOwner}.CMC_CLCK_CLM_CHECK CLMCHK
         LEFT OUTER JOIN {FacetsOwner}.CMC_CDHL_HSA_LINE HSA
            ON CLMCHK.CLCL_ID= HSA.CLCL_ID 
           AND CLMCHK.LOBD_ID = HSA.LOBD_ID
         LEFT OUTER JOIN (
             SELECT *
             FROM {FacetsOwner}.CMC_CKCK_CHECK CHK1
             WHERE CHK1.CKCK_SEQ_NO = (
                 SELECT MAX(CHK2.CKCK_SEQ_NO) 
                 FROM {FacetsOwner}.CMC_CKCK_CHECK CHK2 
                 WHERE CHK2.CKPY_REF_ID = CHK1.CKPY_REF_ID
             )
         ) CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
WHERE CLMCHK.CLCL_ID = DT.CLM_ID
  AND CLMCHK.CLCL_ID=CLMLN.CLCL_ID
  AND CLMLN.SESE_ID <> 'HRAI'
  AND CLMLN.SESE_ID <> 'HRAF'
"""

df_MedIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_MedIn)
    .load()
)

# ConvIn
extract_query_ConvIn = f"""
SELECT DISTINCT
       CLM.CLCL_ID,
       CLM.CLCL_PAID_DT,
       CLMLN.CDML_PAID_AMT,
       MEMO.ATMM_TEXT
FROM
       tempdb..{DriverTable}             DT,
       {FacetsOwner}.CMC_CLCL_CLAIM      CLM,
       {FacetsOwner}.CMC_CDML_CL_LINE    CLMLN,
       {FacetsOwner}.CMC_CLED_EDI_DATA   EDI,
       {FacetsOwner}.CER_ATXR_ATTACH_U   ATT,
       {FacetsOwner}.CER_ATMM_MEMO_D     MEMO
WHERE
       DT.CLM_ID = CLM.CLCL_ID
   AND CLM.CLCL_ID = EDI.CLCL_ID
   AND CLM.CLCL_ID = CLMLN.CLCL_ID
   AND CLM.ATXR_SOURCE_ID = ATT.ATXR_SOURCE_ID
   AND ATT.ATSY_ID = MEMO.ATSY_ID
   AND ATT.ATXR_DEST_ID = MEMO.ATXR_DEST_ID
   AND EDI.CLED_TRAD_PARTNER = 'EMPOWERED BNFTS'
   AND MEMO.ATMM_TEXT LIKE '%KHS CHECK NBR=[0-9]%'
"""

df_ConvIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_ConvIn)
    .load()
)

# RunOutIn
extract_query_RunOutIn = f"""
SELECT DISTINCT 
    CLMCHK.CLCL_ID,
    CLMCHK.CLCK_PAYEE_IND,
    CLMCHK.LOBD_ID AS LOBD_ID,
    CLMCHK.CKPY_REF_ID,
    CLMCHK.CKPY_PAY_DT,
    CLMCHK.CLCK_NET_AMT,
    CLMCHK.CLCK_ORIG_AMT,
    CLMCHK.CLCK_PYMT_OVRD_IND,
    CHK.CKCK_SEQ_NO,
    CHK.CKCK_CK_NO,
    CHK.CKCK_TYPE,
    CHK.CKCK_PAYEE_NAME,
    CHK.CKCK_REISS_DT,
    CHK.CKCK_PRINTED_DT,
    CLMLN.SESE_ID
FROM tempdb..{DriverTable} DT,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM,
     {FacetsOwner}.CMC_CDML_CL_LINE CLMLN,
     {FacetsOwner}.CMC_CLCK_CLM_CHECK CLMCHK
         LEFT OUTER JOIN (
             SELECT *
             FROM {FacetsOwner}.CMC_CKCK_CHECK CHK1
             WHERE CHK1.CKCK_SEQ_NO = (
                 SELECT MAX(CHK2.CKCK_SEQ_NO)
                 FROM {FacetsOwner}.CMC_CKCK_CHECK CHK2
                 WHERE CHK2.CKPY_REF_ID = CHK1.CKPY_REF_ID
             )
         ) CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
WHERE CLM.CLCL_ID = DT.CLM_ID
  AND CLM.CLCL_ID = CLMCHK.CLCL_ID
  AND CLM.CLCL_ID= CLMLN.CLCL_ID
  AND CLMLN.SESE_RULE = 'CIF'
  AND CLMLN.CDML_SEQ_NO = 1
  AND ( CLMLN.SESE_ID = 'HRAI' OR CLMLN.SESE_ID = 'HRAF' )
"""

df_RunOutIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_RunOutIn)
    .load()
)

# DentIn
extract_query_DentIn = f"""
SELECT DISTINCT
    CLMCHK.CLCL_ID,
    CLMCHK.CLCK_PAYEE_IND,
    CLMCHK.LOBD_ID AS LOBD_ID,
    CLMCHK.CKPY_REF_ID,
    CLMCHK.CKPY_PAY_DT,
    CLMCHK.CLCK_NET_AMT,
    CLMCHK.CLCK_ORIG_AMT,
    CLMCHK.CLCK_PYMT_OVRD_IND,
    CHK.CKCK_SEQ_NO,
    CHK.CKCK_CK_NO,
    CHK.CKCK_TYPE,
    CHK.CKCK_PAYEE_NAME,
    CHK.CKCK_REISS_DT,
    CHK.CKCK_PRINTED_DT,
    HSA.LOBD_ID AS HSA_LOBD_ID
FROM tempdb..{DriverTable} DT,
     {FacetsOwner}.CMC_CLCK_CLM_CHECK CLMCHK
         LEFT OUTER JOIN {FacetsOwner}.CMC_CDHL_HSA_LINE HSA
            ON CLMCHK.CLCL_ID = HSA.CLCL_ID 
           AND CLMCHK.LOBD_ID = HSA.LOBD_ID
           AND HSA.CDML_SEQ_NO = 1
         LEFT OUTER JOIN (
             SELECT *
             FROM {FacetsOwner}.CMC_CKCK_CHECK CHK1
             WHERE CHK1.CKCK_SEQ_NO = (
                 SELECT MAX(CHK2.CKCK_SEQ_NO)
                 FROM {FacetsOwner}.CMC_CKCK_CHECK CHK2
                 WHERE CHK2.CKPY_REF_ID = CHK1.CKPY_REF_ID
             )
         ) CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
WHERE CLMCHK.CLCL_ID = DT.CLM_ID
  AND CLMCHK.CLCL_CL_TYPE = 'D'
"""

df_DentIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_DentIn)
    .load()
)

# RunIn
extract_query_RunIn = f"""
SELECT DISTINCT
    CLMCHK.CLCL_ID,
    CLMCHK.CLCK_PAYEE_IND,
    CLMCHK.LOBD_ID AS LOBD_ID,
    CLMCHK.CKPY_REF_ID,
    CLMCHK.CKPY_PAY_DT,
    CLMCHK.CLCK_NET_AMT,
    CLMCHK.CLCK_ORIG_AMT,
    CLMCHK.CLCK_PYMT_OVRD_IND,
    CHK.CKCK_SEQ_NO,
    CHK.CKCK_CK_NO,
    CHK.CKCK_TYPE,
    CHK.CKCK_PAYEE_NAME,
    CHK.CKCK_REISS_DT,
    CHK.CKCK_PRINTED_DT,
    HSA.LOBD_ID AS HSA_LOBD_ID
FROM tempdb..{DriverTable} DT,
     {FacetsOwner}.CMC_CDML_CL_LINE CLMLN,
     {FacetsOwner}.CMC_CLCK_CLM_CHECK CLMCHK
         LEFT OUTER JOIN (
             SELECT *
             FROM {FacetsOwner}.CMC_CKCK_CHECK CHK1
             WHERE CHK1.CKCK_SEQ_NO = (
                 SELECT MAX(CHK2.CKCK_SEQ_NO)
                 FROM {FacetsOwner}.CMC_CKCK_CHECK CHK2
                 WHERE CHK2.CKPY_REF_ID = CHK1.CKPY_REF_ID
             )
         ) CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
         LEFT OUTER JOIN {FacetsOwner}.CMC_CDHL_HSA_LINE HSA
            ON CLMCHK.CLCL_ID = HSA.CLCL_ID
           AND CLMCHK.LOBD_ID = HSA.LOBD_ID
WHERE CLMCHK.CLCL_ID = DT.CLM_ID
  AND CLMCHK.CLCL_ID=CLMLN.CLCL_ID
  AND ( CLMLN.SESE_ID = 'HRAI' OR CLMLN.SESE_ID = 'HRAF')
  AND CLMLN.SESE_RULE = 'HRA'
  AND CLMLN.CDML_SEQ_NO = 1
"""
df_RunIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_RunIn)
    .load()
)

# --------------------------------------------------------------
# ConvBusinessLogic => df_ConvIn => df_ConvOut
# --------------------------------------------------------------

df_ConvIn_transformed = (
    df_ConvIn
    .withColumn("svChkNo", F.substring(F.col("ATMM_TEXT"), 26, 6))
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) | (F.length(trim(F.col("CLCL_ID"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CLCL_ID")))
    )
)

# Filter constraint: Len(Trim(svChkNo)) > 0
df_ConvOut = df_ConvIn_transformed.filter(F.length(trim(F.col("svChkNo"))) > 0)

# Build the output columns
df_ConvOut = (
    df_ConvOut
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FACETS;"), F.col("svClclId"), F.lit(";S;PCA")))
    .withColumn("CLM_CHK_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("svClclId"))
    .withColumn("CLM_CHK_PAYE_TYP_CD", F.lit("S"))
    .withColumn("CLM_CHK_LOB_CD", F.lit("PCA"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CLM_ID_1", F.col("svClclId"))
    .withColumn("CLM_CHK_PAYMT_METH_CD", F.lit("X"))
    .withColumn("CLM_REMIT_HIST_PAYMTOVRD_CD", F.lit("NA"))
    .withColumn("EXTRNL_CHK_IN", F.lit("Y"))
    .withColumn("PCA_CHK_IN", F.lit("Y"))
    .withColumn("CHK_PD_DT", F.substring(F.col("CLCL_PAID_DT"), 1, 10))
    .withColumn("CHK_NET_PAYMT_AMT", F.substring(F.col("ATMM_TEXT"), 48, 8))
    .withColumn("CHK_ORIG_AMT", F.lit(0))
    .withColumn("CHK_NO", F.col("svChkNo"))
    .withColumn("CHK_SEQ_NO", F.lit(0))
    .withColumn("CHK_PAYE_NM", F.lit("NA"))
    .withColumn("CHK_PAYMT_REF_ID", F.lit("NA"))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
        "SRC_SYS_CD","PRI_KEY_STRING","CLM_CHK_SK","CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_ID_1","CLM_CHK_PAYMT_METH_CD","CLM_REMIT_HIST_PAYMTOVRD_CD",
        "EXTRNL_CHK_IN","PCA_CHK_IN","CHK_PD_DT","CHK_NET_PAYMT_AMT","CHK_ORIG_AMT","CHK_NO","CHK_SEQ_NO",
        "CHK_PAYE_NM","CHK_PAYMT_REF_ID"
    )
)

# Deduplicate for hf_clm_chk_conv (Scenario A) on primary key: (CLM_ID, CLM_CHK_PAYE_TYP_CD, CLM_CHK_LOB_CD)
df_ConvFileOut = dedup_sort(
    df_ConvOut,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]  # Arbitrary sort columns
)

# --------------------------------------------------------------
# CurrMedLogic => df_MedIn => df_MedOut
# --------------------------------------------------------------

df_MedIn_transformed = (
    df_MedIn
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) | (F.length(trim(F.col("CLCL_ID"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CLCL_ID")))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) | (F.length(trim(F.col("CLCK_PAYEE_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PAYEE_IND"))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) | (F.length(trim(F.col("LOBD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("LOBD_ID"))))
    )
)

df_MedOut = (
    df_MedIn_transformed
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS;"),
            F.col("svClclId"),
            F.lit(";"),
            F.col("svClckPayeeInd"),
            F.lit(";"),
            F.col("svLobdId")
        )
    )
    .withColumn("CLM_CHK_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("svClclId"))
    .withColumn("CLM_CHK_PAYE_TYP_CD", F.col("svClckPayeeInd"))
    .withColumn("CLM_CHK_LOB_CD", F.col("svLobdId"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CLM_ID_1", F.col("svClclId"))
    .withColumn(
        "CLM_CHK_PAYMT_METH_CD",
        F.when(
            (F.col("CKCK_TYPE").isNull()) | (F.length(trim(F.col("CKCK_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKCK_TYPE"))))
    )
    .withColumn(
        "CLM_REMIT_HIST_PAYMTOVRD_CD",
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull()) | (F.length(trim(F.col("CLCK_PYMT_OVRD_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PYMT_OVRD_IND"))))
    )
    .withColumn("EXTRNL_CHK_IN", F.lit("N"))
    .withColumn(
        "PCA_CHK_IN",
        F.when(F.col("HSA_LOBD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y"))
    )
    .withColumn(
        "CHK_PD_DT",
        F.when(
            (F.substring(F.col("CKCK_REISS_DT"),1,10) != "1753-01-01"),
            F.substring(F.col("CKCK_PRINTED_DT"),1,10)
        ).otherwise(F.substring(F.col("CKPY_PAY_DT"),1,10))
    )
    .withColumn(
        "CHK_NET_PAYMT_AMT",
        F.when(
            (F.col("CLCK_NET_AMT").isNull()) | (F.length(F.col("CLCK_NET_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_ORIG_AMT",
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull()) | (F.length(F.col("CLCK_ORIG_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_NO",
        F.when(F.col("CKCK_CK_NO") >= 0, F.col("CKCK_CK_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_SEQ_NO",
        F.when(F.col("CKCK_SEQ_NO") >= 0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_PAYE_NM",
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull()) | (F.length(trim(F.col("CKCK_PAYEE_NAME"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("CKCK_PAYEE_NAME"))))
    )
    .withColumn(
        "CHK_PAYMT_REF_ID",
        F.when(
            (F.col("CKPY_REF_ID").isNull()) | (F.length(trim(F.col("CKPY_REF_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKPY_REF_ID"))))
    )
    .select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
        "SRC_SYS_CD","PRI_KEY_STRING","CLM_CHK_SK","CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_ID_1","CLM_CHK_PAYMT_METH_CD","CLM_REMIT_HIST_PAYMTOVRD_CD",
        "EXTRNL_CHK_IN","PCA_CHK_IN","CHK_PD_DT","CHK_NET_PAYMT_AMT","CHK_ORIG_AMT","CHK_NO","CHK_SEQ_NO",
        "CHK_PAYE_NM","CHK_PAYMT_REF_ID"
    )
)

# Deduplicate for hf_clm_chk_med
df_MedFileIn = dedup_sort(
    df_MedOut,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]
)

# --------------------------------------------------------------
# RunOutBusinessLogic => df_RunOutIn => df_RunOutOut
# --------------------------------------------------------------

df_RunOutIn_transformed = (
    df_RunOutIn
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) | (F.length(trim(F.col("CLCL_ID"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CLCL_ID")))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) | (F.length(trim(F.col("CLCK_PAYEE_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PAYEE_IND"))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) | (F.length(trim(F.col("LOBD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("LOBD_ID"))))
    )
)

df_RunOutOut = (
    df_RunOutIn_transformed
    .filter(F.col("SESE_ID").isNotNull())   # Constraint => IsNull(RunOutIn.SESE_ID) = @FALSE
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS;"),
            F.col("svClclId"),
            F.lit(";"),
            F.col("svClckPayeeInd"),
            F.lit(";"),
            F.col("svLobdId")
        )
    )
    .withColumn("CLM_CHK_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("svClclId"))
    .withColumn("CLM_CHK_PAYE_TYP_CD", F.col("svClckPayeeInd"))
    .withColumn("CLM_CHK_LOB_CD", F.col("svLobdId"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CLM_ID_1", F.col("svClclId"))
    .withColumn(
        "CLM_CHK_PAYMT_METH_CD",
        F.when(
            (F.col("CKCK_TYPE").isNull()) | (F.length(trim(F.col("CKCK_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKCK_TYPE"))))
    )
    .withColumn(
        "CLM_REMIT_HIST_PAYMTOVRD_CD",
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull()) | (F.length(trim(F.col("CLCK_PYMT_OVRD_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PYMT_OVRD_IND"))))
    )
    .withColumn("EXTRNL_CHK_IN", F.lit("N"))
    .withColumn("PCA_CHK_IN", F.lit("Y"))
    .withColumn(
        "CHK_PD_DT",
        F.when(
            (F.substring(F.col("CKCK_REISS_DT"),1,10) != "1753-01-01"),
            F.substring(F.col("CKCK_PRINTED_DT"),1,10)
        ).otherwise(F.substring(F.col("CKPY_PAY_DT"),1,10))
    )
    .withColumn(
        "CHK_NET_PAYMT_AMT",
        F.when(
            (F.col("CLCK_NET_AMT").isNull()) | (F.length(F.col("CLCK_NET_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_ORIG_AMT",
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull()) | (F.length(F.col("CLCK_ORIG_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_NO",
        F.when(F.col("CKCK_CK_NO") >= 0, F.col("CKCK_CK_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_SEQ_NO",
        F.when(F.col("CKCK_SEQ_NO") >= 0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_PAYE_NM",
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull()) | (F.length(trim(F.col("CKCK_PAYEE_NAME"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("CKCK_PAYEE_NAME"))))
    )
    .withColumn(
        "CHK_PAYMT_REF_ID",
        F.when(
            (F.col("CKPY_REF_ID").isNull()) | (F.length(trim(F.col("CKPY_REF_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKPY_REF_ID"))))
    )
    .select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
        "SRC_SYS_CD","PRI_KEY_STRING","CLM_CHK_SK","CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_ID_1","CLM_CHK_PAYMT_METH_CD","CLM_REMIT_HIST_PAYMTOVRD_CD",
        "EXTRNL_CHK_IN","PCA_CHK_IN","CHK_PD_DT","CHK_NET_PAYMT_AMT","CHK_ORIG_AMT","CHK_NO","CHK_SEQ_NO",
        "CHK_PAYE_NM","CHK_PAYMT_REF_ID"
    )
)

# Deduplicate for hf_clm_chk_runout
df_RunOutFileIn = dedup_sort(
    df_RunOutOut,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]
)

# --------------------------------------------------------------
# CurrDentLogic => df_DentIn => df_DentOut
# --------------------------------------------------------------

df_DentIn_transformed = (
    df_DentIn
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) | (F.length(trim(F.col("CLCL_ID"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CLCL_ID")))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) | (F.length(trim(F.col("CLCK_PAYEE_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PAYEE_IND"))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) | (F.length(trim(F.col("LOBD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("LOBD_ID"))))
    )
)

df_DentOut = (
    df_DentIn_transformed
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS;"),
            F.col("svClclId"),
            F.lit(";"),
            F.col("svClckPayeeInd"),
            F.lit(";"),
            F.col("svLobdId")
        )
    )
    .withColumn("CLM_CHK_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("svClclId"))
    .withColumn("CLM_CHK_PAYE_TYP_CD", F.col("svClckPayeeInd"))
    .withColumn("CLM_CHK_LOB_CD", F.col("svLobdId"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CLM_ID_1", F.col("svClclId"))
    .withColumn(
        "CLM_CHK_PAYMT_METH_CD",
        F.when(
            (F.col("CKCK_TYPE").isNull()) | (F.length(trim(F.col("CKCK_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKCK_TYPE"))))
    )
    .withColumn(
        "CLM_REMIT_HIST_PAYMTOVRD_CD",
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull()) | (F.length(trim(F.col("CLCK_PYMT_OVRD_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PYMT_OVRD_IND"))))
    )
    .withColumn("EXTRNL_CHK_IN", F.lit("N"))
    .withColumn(
        "PCA_CHK_IN",
        F.when(F.col("HSA_LOBD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y"))
    )
    .withColumn(
        "CHK_PD_DT",
        F.when(
            (F.substring(F.col("CKCK_REISS_DT"),1,10) != "1753-01-01"),
            F.substring(F.col("CKCK_PRINTED_DT"),1,10)
        ).otherwise(F.substring(F.col("CKPY_PAY_DT"),1,10))
    )
    .withColumn(
        "CHK_NET_PAYMT_AMT",
        F.when(
            (F.col("CLCK_NET_AMT").isNull()) | (F.length(F.col("CLCK_NET_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_ORIG_AMT",
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull()) | (F.length(F.col("CLCK_ORIG_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_NO",
        F.when(F.col("CKCK_CK_NO") >= 0, F.col("CKCK_CK_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_SEQ_NO",
        F.when(F.col("CKCK_SEQ_NO") >= 0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_PAYE_NM",
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull()) | (F.length(trim(F.col("CKCK_PAYEE_NAME"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("CKCK_PAYEE_NAME"))))
    )
    .withColumn(
        "CHK_PAYMT_REF_ID",
        F.when(
            (F.col("CKPY_REF_ID").isNull()) | (F.length(trim(F.col("CKPY_REF_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKPY_REF_ID"))))
    )
    .select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
        "SRC_SYS_CD","PRI_KEY_STRING","CLM_CHK_SK","CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_ID_1","CLM_CHK_PAYMT_METH_CD","CLM_REMIT_HIST_PAYMTOVRD_CD",
        "EXTRNL_CHK_IN","PCA_CHK_IN","CHK_PD_DT","CHK_NET_PAYMT_AMT","CHK_ORIG_AMT","CHK_NO","CHK_SEQ_NO",
        "CHK_PAYE_NM","CHK_PAYMT_REF_ID"
    )
)

# Deduplicate for hf_clm_chk_dent
df_DentFileIn = dedup_sort(
    df_DentOut,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]
)

# --------------------------------------------------------------
# CurrRunInLogic => df_RunIn => df_RunOut
# --------------------------------------------------------------

df_RunIn_transformed = (
    df_RunIn
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) | (F.length(trim(F.col("CLCL_ID"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CLCL_ID")))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) | (F.length(trim(F.col("CLCK_PAYEE_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PAYEE_IND"))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) | (F.length(trim(F.col("LOBD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("LOBD_ID"))))
    )
)

df_RunOut = (
    df_RunIn_transformed
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS;"),
            F.col("svClclId"),
            F.lit(";"),
            F.col("svClckPayeeInd"),
            F.lit(";"),
            F.col("svLobdId")
        )
    )
    .withColumn("CLM_CHK_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("svClclId"))
    .withColumn("CLM_CHK_PAYE_TYP_CD", F.col("svClckPayeeInd"))
    .withColumn("CLM_CHK_LOB_CD", F.col("svLobdId"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CLM_ID_1", F.col("svClclId"))
    .withColumn(
        "CLM_CHK_PAYMT_METH_CD",
        F.when(
            (F.col("CKCK_TYPE").isNull()) | (F.length(trim(F.col("CKCK_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKCK_TYPE"))))
    )
    .withColumn(
        "CLM_REMIT_HIST_PAYMTOVRD_CD",
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull()) | (F.length(trim(F.col("CLCK_PYMT_OVRD_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CLCK_PYMT_OVRD_IND"))))
    )
    .withColumn("EXTRNL_CHK_IN", F.lit("N"))
    .withColumn(
        "PCA_CHK_IN",
        F.when(F.col("HSA_LOBD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y"))
    )
    .withColumn(
        "CHK_PD_DT",
        F.when(
            (F.substring(F.col("CKCK_REISS_DT"),1,10) != "1753-01-01"),
            F.substring(F.col("CKCK_PRINTED_DT"),1,10)
        ).otherwise(F.substring(F.col("CKPY_PAY_DT"),1,10))
    )
    .withColumn(
        "CHK_NET_PAYMT_AMT",
        F.when(
            (F.col("CLCK_NET_AMT").isNull()) | (F.length(F.col("CLCK_NET_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_ORIG_AMT",
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull()) | (F.length(F.col("CLCK_ORIG_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT").cast(DecimalType(18,2)))
    )
    .withColumn(
        "CHK_NO",
        F.when(F.col("CKCK_CK_NO") >= 0, F.col("CKCK_CK_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_SEQ_NO",
        F.when(F.col("CKCK_SEQ_NO") >= 0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0))
    )
    .withColumn(
        "CHK_PAYE_NM",
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull()) | (F.length(trim(F.col("CKCK_PAYEE_NAME"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("CKCK_PAYEE_NAME"))))
    )
    .withColumn(
        "CHK_PAYMT_REF_ID",
        F.when(
            (F.col("CKPY_REF_ID").isNull()) | (F.length(trim(F.col("CKPY_REF_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("CKPY_REF_ID"))))
    )
    .select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
        "SRC_SYS_CD","PRI_KEY_STRING","CLM_CHK_SK","CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_ID_1","CLM_CHK_PAYMT_METH_CD","CLM_REMIT_HIST_PAYMTOVRD_CD",
        "EXTRNL_CHK_IN","PCA_CHK_IN","CHK_PD_DT","CHK_NET_PAYMT_AMT","CHK_ORIG_AMT","CHK_NO","CHK_SEQ_NO",
        "CHK_PAYE_NM","CHK_PAYMT_REF_ID"
    )
)

# Deduplicate for hf_clm_chk_runin
df_RunFileIn = dedup_sort(
    df_RunOut,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]
)

# --------------------------------------------------------------
# Collect1 => union of 5 inputs
# --------------------------------------------------------------

df_Collect1 = df_ConvFileOut.unionByName(df_MedFileIn)\
    .unionByName(df_RunOutFileIn)\
    .unionByName(df_DentFileIn)\
    .unionByName(df_RunFileIn)

# Next => hf_clm_chk_collected => scenario A => deduplicate => feed Reversals
df_CollectOut = dedup_sort(
    df_Collect1,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]
)

# --------------------------------------------------------------
# Reversals => primary link df_CollectOut, lookup link df_hf_clm_fcts_reversals
# Left Join condition: CLM_ID = CLCL_ID
# Output: Straight (no constraint) and Reversals (constraint)
# --------------------------------------------------------------

df_merged = df_CollectOut.alias("CollectOut").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversals"),
    on=(F.col("CollectOut.CLM_ID") == F.col("fcts_reversals.CLCL_ID")),
    how="left"
)

# Straight => just pass columns from CollectOut
df_Straight = df_merged.select(
    F.col("CollectOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CollectOut.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("CollectOut.DISCARD_IN").alias("DISCARD_IN"),
    F.col("CollectOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("CollectOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("CollectOut.ERR_CT").alias("ERR_CT"),
    F.col("CollectOut.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("CollectOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CollectOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CollectOut.CLM_CHK_SK").alias("CLM_CHK_SK"),
    F.col("CollectOut.CLM_ID").alias("CLM_ID"),
    F.col("CollectOut.CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
    F.col("CollectOut.CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD"),
    F.col("CollectOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CollectOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CollectOut.CLM_ID_1").alias("CLM_ID_1"),
    F.col("CollectOut.CLM_CHK_PAYMT_METH_CD").alias("CLM_CHK_PAYMT_METH_CD"),
    F.col("CollectOut.CLM_REMIT_HIST_PAYMTOVRD_CD").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
    F.col("CollectOut.EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
    F.col("CollectOut.PCA_CHK_IN").alias("PCA_CHK_IN"),
    F.col("CollectOut.CHK_PD_DT").alias("CHK_PD_DT"),
    F.col("CollectOut.CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
    F.col("CollectOut.CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
    F.col("CollectOut.CHK_NO").alias("CHK_NO"),
    F.col("CollectOut.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("CollectOut.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("CollectOut.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
)

# Reversals => constraint => fcts_reversals.CLCL_ID not null and (CLCL_CUR_STS='91' or '89')
df_Reversals = df_merged.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
    )
).select(
    F.col("CollectOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CollectOut.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("CollectOut.DISCARD_IN").alias("DISCARD_IN"),
    F.col("CollectOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("CollectOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("CollectOut.ERR_CT").alias("ERR_CT"),
    F.col("CollectOut.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("CollectOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"),F.col("CollectOut.CLM_ID"),F.lit("R;"),F.col("CollectOut.CLM_CHK_PAYE_TYP_CD"),F.lit(";"),F.col("CollectOut.CLM_CHK_LOB_CD")).alias("PRI_KEY_STRING"),
    F.col("CollectOut.CLM_CHK_SK").alias("CLM_CHK_SK"),
    F.concat(F.col("CollectOut.CLM_ID"),F.lit("R")).alias("CLM_ID"),
    F.col("CollectOut.CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
    F.col("CollectOut.CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD"),
    F.col("CollectOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CollectOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.concat(F.col("CollectOut.CLM_ID_1"),F.lit("R")).alias("CLM_ID_1"),
    F.col("CollectOut.CLM_CHK_PAYMT_METH_CD").alias("CLM_CHK_PAYMT_METH_CD"),
    F.col("CollectOut.CLM_REMIT_HIST_PAYMTOVRD_CD").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
    F.col("CollectOut.EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
    F.col("CollectOut.PCA_CHK_IN").alias("PCA_CHK_IN"),
    F.col("CollectOut.CHK_PD_DT").alias("CHK_PD_DT"),
    (-1 * F.col("CollectOut.CHK_NET_PAYMT_AMT")).alias("CHK_NET_PAYMT_AMT"),
    (-1 * F.col("CollectOut.CHK_ORIG_AMT")).alias("CHK_ORIG_AMT"),
    F.col("CollectOut.CHK_NO").alias("CHK_NO"),
    F.col("CollectOut.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("CollectOut.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("CollectOut.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
)

# Collect2 => union of df_Straight and df_Reversals
df_Collect2 = df_Straight.unionByName(df_Reversals)

# Next => hf_clm_chk_transformed => scenario A => deduplicate => feed Logic
df_TransformOut = dedup_sort(
    df_Collect2,
    partition_cols=["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"],
    sort_cols=[("CLM_ID","A")]
)

# --------------------------------------------------------------
# Logic => two output links: AllCol and Transform
# --------------------------------------------------------------

# We'll build df_AllCol with all columns
df_AllCol = (
    df_TransformOut
    .withColumn("SRC_SYS_CD_SK", F.expr("SrcSysCdSk"))  # According to the job, Expression is "SrcSysCdSk"
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("CLM_CHK_PAYE_TYP_CD"),
        F.col("CLM_CHK_LOB_CD"),
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_CHK_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_ID_1"),
        F.col("CLM_CHK_PAYMT_METH_CD"),
        F.col("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        F.col("EXTRNL_CHK_IN"),
        F.col("PCA_CHK_IN"),
        F.col("CHK_PD_DT"),
        F.col("CHK_NET_PAYMT_AMT"),
        F.col("CHK_ORIG_AMT"),
        F.col("CHK_NO"),
        F.col("CHK_SEQ_NO"),
        F.col("CHK_PAYE_NM"),
        F.col("CHK_PAYMT_REF_ID")
    )
)

# We'll build df_Transform with only a few columns
df_Transform = (
    df_TransformOut
    .withColumn("SRC_SYS_CD_SK", F.expr("SrcSysCdSk"))
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("CLM_CHK_PAYE_TYP_CD"),
        F.col("CLM_CHK_LOB_CD")
    )
)

# --------------------------------------------------------------
# Shared Container => ClmChkPK => 2 inputs, 1 output
# --------------------------------------------------------------

params = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner,
    "CurrentDate": CurrentDateParam
}
df_ClmChkPKOut = ClmChkPK(df_AllCol, df_Transform, params)

# --------------------------------------------------------------
# FctsClmChkExtr => final output => .dat file
# --------------------------------------------------------------

# Select columns in final order
df_final = df_ClmChkPKOut.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_CHK_SK",
    "CLM_ID",
    "CLM_CHK_PAYE_TYP_CD",
    "CLM_CHK_LOB_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_ID_1",
    "CLM_CHK_PAYMT_METH_CD",
    "CLM_REMIT_HIST_PAYMTOVRD_CD",
    "EXTRNL_CHK_IN",
    "PCA_CHK_IN",
    "CHK_PD_DT",
    "CHK_NET_PAYMT_AMT",
    "CHK_ORIG_AMT",
    "CHK_NO",
    "CHK_SEQ_NO",
    "CHK_PAYE_NM",
    "CHK_PAYMT_REF_ID"
)

# rpad for columns that have known char lengths
df_final = (
    df_final
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_ID_1", F.rpad(F.col("CLM_ID_1"), 18, " "))
    .withColumn("EXTRNL_CHK_IN", F.rpad(F.col("EXTRNL_CHK_IN"), 1, " "))
    .withColumn("PCA_CHK_IN", F.rpad(F.col("PCA_CHK_IN"), 1, " "))
)

write_files(
    df_final,
    f"{adls_path}/key/FctsClmChkExtr.FctsClmChk.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)