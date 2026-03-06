# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmChkExtr
# MAGIC JOB NAME:     LhoFctsClmExtr1Seq
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
# MAGIC 
# MAGIC Reddy Sanam      2020-10-12                                         brought up to standards                                                              IntegrateDev2
# MAGIC 
# MAGIC Prabhu ES              2022-03-29      S2S                           MSSQL ODBC conn params added                                          IntegrateDev5

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
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
    DateType,
    NumericType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmChkPK
# COMMAND ----------

# Retrieve job parameters
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrentDate = get_widget_value('CurrentDate','2009-06-23')
RunID = get_widget_value('RunID','20090623')
SrcSysCdSk = get_widget_value('SrcSysCdSk','69560')
IDSOwner = get_widget_value('IDSOwner','')
SrcSysCd = get_widget_value('SrcSysCd','LUMERIS')

# For database owners that match pattern, also get the secret names:
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read from hf_clm_fcts_reversals (Scenario C -> read from Parquet)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
)

# Set up the DB config for LhoFACETS (ODBCConnector stage).
jdbc_url_lhofacetsstg, jdbc_props_lhofacetsstg = get_db_config(lhofacetsstg_secret_name)

# LhoFACETS has multiple output links, each with a distinct query.

# 1) MedIn
query_medIn = f"""
SELECT DISTINCT
       CLMCHK.CLCL_ID, 
       CLMCHK.CLCK_PAYEE_IND, 
       CLMCHK.LOBD_ID as LOBD_ID, 
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
       HSA.LOBD_ID as HSA_LOBD_ID
FROM  
       tempdb..#{DriverTable} DT,
       {LhoFacetsStgOwner}.CMC_CDML_CL_LINE   CLMLN,
       {LhoFacetsStgOwner}.CMC_CLCK_CLM_CHECK CLMCHK
       LEFT OUTER JOIN
       {LhoFacetsStgOwner}.CMC_CDHL_HSA_LINE   HSA
         ON CLMCHK.CLCL_ID= HSA.CLCL_ID AND CLMCHK.LOBD_ID = HSA.LOBD_ID
       LEFT OUTER JOIN 
       {LhoFacetsStgOwner}.CMC_CKCK_CHECK      CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
WHERE 
       CLMCHK.CLCL_ID = DT.CLM_ID
   AND CLMCHK.CLCL_ID=CLMLN.CLCL_ID
   AND CLMLN.SESE_ID <> 'HRAI'
   AND CLMLN.SESE_ID <> 'HRAF'
   AND CHK.CKCK_SEQ_NO = ( SELECT MAX(CHK2.CKCK_SEQ_NO)
                           FROM {LhoFacetsStgOwner}.CMC_CKCK_CHECK CHK2
                           WHERE CHK2.CKPY_REF_ID = CHK.CKPY_REF_ID )
"""
df_LhoFACETS_MedIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", query_medIn)
    .load()
)

# 2) ConvIn
query_convIn = f"""
SELECT DISTINCT
       CLM.CLCL_ID,
       CLM.CLCL_PAID_DT,
       CLMLN.CDML_PAID_AMT,
       MEMO.ATMM_TEXT
FROM
       tempdb..#{DriverTable} DT,
       {LhoFacetsStgOwner}.CMC_CLCL_CLAIM     CLM,
       {LhoFacetsStgOwner}.CMC_CDML_CL_LINE   CLMLN,
       {LhoFacetsStgOwner}.CMC_CLED_EDI_DATA  EDI,
       {LhoFacetsStgOwner}.CER_ATXR_ATTACH_U  ATT,
       {LhoFacetsStgOwner}.CER_ATMM_MEMO_D    MEMO
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
df_LhoFACETS_ConvIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", query_convIn)
    .load()
)

# 3) RunOutIn
query_runOutIn = f"""
SELECT DISTINCT
       CLMCHK.CLCL_ID, 
       CLMCHK.CLCK_PAYEE_IND, 
       CLMCHK.LOBD_ID as LOBD_ID, 
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
FROM  
       tempdb..#{DriverTable} DT,
       {LhoFacetsStgOwner}.CMC_CLCL_CLAIM       CLM,
       {LhoFacetsStgOwner}.CMC_CDML_CL_LINE     CLMLN,
       {LhoFacetsStgOwner}.CMC_CLCK_CLM_CHECK   CLMCHK
       LEFT OUTER JOIN
       {LhoFacetsStgOwner}.CMC_CKCK_CHECK       CHK 
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
WHERE 
       CLM.CLCL_ID = DT.CLM_ID
   AND CLM.CLCL_ID = CLMCHK.CLCL_ID
   AND CLM.CLCL_ID= CLMLN.CLCL_ID
   AND CLMLN.SESE_RULE = 'CIF'
   AND CLMLN.CDML_SEQ_NO = 1
   AND ( CLMLN.SESE_ID = 'HRAI' OR CLMLN.SESE_ID = 'HRAF' )
   AND CHK.CKCK_SEQ_NO = ( SELECT MAX(CHK2.CKCK_SEQ_NO)
                           FROM {LhoFacetsStgOwner}.CMC_CKCK_CHECK CHK2
                           WHERE CHK2.CKPY_REF_ID = CHK.CKPY_REF_ID )
"""
df_LhoFACETS_RunOutIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", query_runOutIn)
    .load()
)

# 4) DentIn
query_dentIn = f"""
SELECT DISTINCT
       CLMCHK.CLCL_ID, 
       CLMCHK.CLCK_PAYEE_IND, 
       CLMCHK.LOBD_ID as LOBD_ID, 
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
       HSA.LOBD_ID as HSA_LOBD_ID
FROM  
       tempdb..#{DriverTable} DT,
       {LhoFacetsStgOwner}.CMC_CLCK_CLM_CHECK     CLMCHK
       LEFT OUTER JOIN
       {LhoFacetsStgOwner}.CMC_CDHL_HSA_LINE        HSA
         ON CLMCHK.CLCL_ID = HSA.CLCL_ID
            AND CLMCHK.LOBD_ID = HSA.LOBD_ID
       LEFT OUTER JOIN 
       {LhoFacetsStgOwner}.CMC_CKCK_CHECK           CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
WHERE 
       CLMCHK.CLCL_ID = DT.CLM_ID
   AND CLMCHK.CLCL_CL_TYPE  = 'D'
   AND HSA.CDML_SEQ_NO = 1
   AND CHK.CKCK_SEQ_NO = ( SELECT MAX(CHK2.CKCK_SEQ_NO)
                           FROM {LhoFacetsStgOwner}.CMC_CKCK_CHECK CHK2
                           WHERE CHK2.CKPY_REF_ID = CHK.CKPY_REF_ID )
"""
df_LhoFACETS_DentIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", query_dentIn)
    .load()
)

# 5) RunIn
query_runIn = f"""
SELECT DISTINCT
       CLMCHK.CLCL_ID, 
       CLMCHK.CLCK_PAYEE_IND, 
       CLMCHK.LOBD_ID as LOBD_ID, 
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
       HSA.LOBD_ID as HSA_LOBD_ID
FROM  
       tempdb..#{DriverTable} DT,
       {LhoFacetsStgOwner}.CMC_CDML_CL_LINE CLMLN,
       {LhoFacetsStgOwner}.CMC_CLCK_CLM_CHECK CLMCHK
       LEFT OUTER JOIN 
       {LhoFacetsStgOwner}.CMC_CKCK_CHECK CHK
         ON CLMCHK.CKPY_REF_ID = CHK.CKPY_REF_ID
       LEFT OUTER JOIN
       {LhoFacetsStgOwner}.CMC_CDHL_HSA_LINE HSA
         ON CLMCHK.CLCL_ID = HSA.CLCL_ID
            AND CLMCHK.LOBD_ID = HSA.LOBD_ID
WHERE 
       CLMCHK.CLCL_ID = DT.CLM_ID
   AND CLMCHK.CLCL_ID=CLMLN.CLCL_ID
   AND ( CLMLN.SESE_ID = 'HRAI' OR CLMLN.SESE_ID = 'HRAF' )
   AND CLMLN.SESE_RULE = 'HRA'
   AND CLMLN.CDML_SEQ_NO = 1
   AND CHK.CKCK_SEQ_NO = ( SELECT MAX(CHK2.CKCK_SEQ_NO)
                           FROM {LhoFacetsStgOwner}.CMC_CKCK_CHECK CHK2
                           WHERE CHK2.CKPY_REF_ID = CHK.CKPY_REF_ID )
"""
df_LhoFACETS_RunIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", query_runIn)
    .load()
)

###############################################################################
# Helper to do the "scenario A" dedup by key columns
def scenarioA_dedup(df_in, pk_cols):
    # We have dedup_sort available
    return dedup_sort(df_in, pk_cols, [])

###############################################################################
# ConvBusinessLogic Transformation
df_ConvBusinessLogic = (
    df_LhoFACETS_ConvIn
    .withColumn(
        "svSrcSysCd",
        F.lit("LUMERIS")
    )
    .withColumn(
        "svChkNo",
        F.expr("substring(ATMM_TEXT, 26, 6)")
    )
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) | 
            (F.length(trim(strip_field(F.col("CLCL_ID")))) == 0),
            F.lit("")
        ).otherwise(trim(strip_field(F.col("CLCL_ID"))))
    )
)

# Output columns (ConvOut), applying constraint "Len(Trim(svChkNo)) > 0"
cond_ConvOut = (
    F.length(trim(F.col("svChkNo"))) > 0
)
df_ConvOut = (
    df_ConvBusinessLogic
    .filter(cond_ConvOut)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("svClclId"), F.lit(";S;PCA")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_CHK_SK"),
        F.col("svClclId").alias("CLM_ID"),
        F.lit("S").alias("CLM_CHK_PAYE_TYP_CD"),
        F.lit("PCA").alias("CLM_CHK_LOB_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svClclId").alias("CLM_ID_1"),
        F.lit("X").alias("CLM_CHK_PAYMT_METH_CD"),
        F.lit("NA").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        rpad(F.lit("Y"), 1, " ").alias("EXTRNL_CHK_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PCA_CHK_IN"),
        F.expr("substring(CLCL_PAID_DT,1,10)").alias("CHK_PD_DT"),
        F.expr("substring(ATMM_TEXT,48,8)").alias("CHK_NET_PAYMT_AMT"),
        F.lit(0).alias("CHK_ORIG_AMT"),
        F.col("svChkNo").alias("CHK_NO"),
        F.lit(0).alias("CHK_SEQ_NO"),
        F.lit("NA").alias("CHK_PAYE_NM"),
        F.lit("NA").alias("CHK_PAYMT_REF_ID")
    )
)
# Deduplicate => Scenario A for "hf_clm_chk_conv"
df_hf_clm_chk_conv = scenarioA_dedup(
    df_ConvOut,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# CurrMedLogic Transformation
df_CurrMedLogic = (
    df_LhoFACETS_MedIn
    .withColumn("svSrcSysCd", F.lit("LUMERIS"))
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) |
            (F.length(trim(strip_field(F.col("CLCL_ID")))) == 0),
            F.lit("")
        ).otherwise(trim(strip_field(F.col("CLCL_ID"))))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) |
            (F.length(trim(strip_field(F.col("CLCK_PAYEE_IND")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PAYEE_IND")))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) |
            (F.length(trim(strip_field(F.col("LOBD_ID")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("LOBD_ID")))))
    )
)

df_MedOut = (
    df_CurrMedLogic
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("svSrcSysCd"),F.lit(";"),F.col("svClclId"),F.lit(";"),F.col("svClckPayeeInd"),F.lit(";"),F.col("svLobdId")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_CHK_SK"),
        F.col("svClclId").alias("CLM_ID"),
        F.col("svClckPayeeInd").alias("CLM_CHK_PAYE_TYP_CD"),
        F.col("svLobdId").alias("CLM_CHK_LOB_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svClclId").alias("CLM_ID_1"),
        F.when(
            (F.col("CKCK_TYPE").isNull()) |
            (F.length(trim(strip_field(F.col("CKCK_TYPE")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_TYPE"))))).alias("CLM_CHK_PAYMT_METH_CD"),
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull()) |
            (F.length(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))).alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        rpad(F.lit("N"),1," ").alias("EXTRNL_CHK_IN"),
        F.when(F.col("HSA_LOBD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y")).alias("PCA_CHK_IN"),
        F.when(
            (F.substring("CKCK_REISS_DT",1,10) != "1753-01-01"),
            F.substring("CKCK_PRINTED_DT",1,10)
        ).otherwise(F.substring("CKPY_PAY_DT",1,10)).alias("CHK_PD_DT"),
        F.when(
            (F.col("CLCK_NET_AMT").isNull()) | (F.length(F.col("CLCK_NET_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT")).alias("CHK_NET_PAYMT_AMT"),
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull()) | (F.length(F.col("CLCK_ORIG_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT")).alias("CHK_ORIG_AMT"),
        F.when(F.col("CKCK_CK_NO")>=0, F.col("CKCK_CK_NO")).otherwise(F.lit(0)).alias("CHK_NO"),
        F.when(F.col("CKCK_SEQ_NO")>=0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0)).alias("CHK_SEQ_NO"),
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull()) |
            (F.length(trim(strip_field(F.col("CKCK_PAYEE_NAME")))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))).alias("CHK_PAYE_NM"),
        F.when(
            (F.col("CKPY_REF_ID").isNull()) |
            (F.length(trim(strip_field(F.col("CKPY_REF_ID")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKPY_REF_ID"))))).alias("CHK_PAYMT_REF_ID")
    )
)
df_hf_clm_chk_med = scenarioA_dedup(
    df_MedOut,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# RunOutBusinessLogic
df_RunOutBusinessLogic = (
    df_LhoFACETS_RunOutIn
    .withColumn("svSrcSysCd", F.lit("LUMERIS"))
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) |
            (F.length(trim(strip_field(F.col("CLCL_ID")))) == 0),
            F.lit("")
        ).otherwise(trim(strip_field(F.col("CLCL_ID"))))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) |
            (F.length(trim(strip_field(F.col("CLCK_PAYEE_IND")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PAYEE_IND")))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) |
            (F.length(trim(strip_field(F.col("LOBD_ID")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("LOBD_ID")))))
    )
)

cond_RunOutOut = ~F.col("SESE_ID").isNull()  # "Constraint": "IsNull(RunOutIn.SESE_ID) = @FALSE"
df_RunOutOut = (
    df_RunOutBusinessLogic
    .filter(cond_RunOutOut)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("svSrcSysCd"),F.lit(";"),F.col("svClclId"),F.lit(";"),F.col("svClckPayeeInd"),F.lit(";"),F.col("svLobdId")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_CHK_SK"),
        F.col("svClclId").alias("CLM_ID"),
        F.col("svClckPayeeInd").alias("CLM_CHK_PAYE_TYP_CD"),
        F.col("svLobdId").alias("CLM_CHK_LOB_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svClclId").alias("CLM_ID_1"),
        F.when(
            (F.col("CKCK_TYPE").isNull()) | (F.length(trim(strip_field(F.col("CKCK_TYPE"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_TYPE"))))).alias("CLM_CHK_PAYMT_METH_CD"),
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull())|(F.length(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))).alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        rpad(F.lit("N"),1," ").alias("EXTRNL_CHK_IN"),
        rpad(F.lit("Y"),1," ").alias("PCA_CHK_IN"),
        F.when(
            (F.substring("CKCK_REISS_DT",1,10) != "1753-01-01"),
            F.substring("CKCK_PRINTED_DT",1,10)
        ).otherwise(F.substring("CKPY_PAY_DT",1,10)).alias("CHK_PD_DT"),
        F.when(
            (F.col("CLCK_NET_AMT").isNull())|(F.length(F.col("CLCK_NET_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT")).alias("CHK_NET_PAYMT_AMT"),
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull())|(F.length(F.col("CLCK_ORIG_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT")).alias("CHK_ORIG_AMT"),
        F.when(F.col("CKCK_CK_NO")>=0, F.col("CKCK_CK_NO")).otherwise(F.lit(0)).alias("CHK_NO"),
        F.when(F.col("CKCK_SEQ_NO")>=0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0)).alias("CHK_SEQ_NO"),
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull())|(F.length(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))==0),
            F.lit("")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))).alias("CHK_PAYE_NM"),
        F.when(
            (F.col("CKPY_REF_ID").isNull())|(F.length(trim(strip_field(F.col("CKPY_REF_ID"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKPY_REF_ID"))))).alias("CHK_PAYMT_REF_ID")
    )
)
df_hf_clm_chk_runout = scenarioA_dedup(
    df_RunOutOut,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# CurrDentLogic
df_CurrDentLogic = (
    df_LhoFACETS_DentIn
    .withColumn("svSrcSysCd", F.lit("LUMERIS"))
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull()) |
            (F.length(trim(strip_field(F.col("CLCL_ID")))) == 0),
            F.lit("")
        ).otherwise(trim(strip_field(F.col("CLCL_ID"))))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull()) |
            (F.length(trim(strip_field(F.col("CLCK_PAYEE_IND")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PAYEE_IND")))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull()) |
            (F.length(trim(strip_field(F.col("LOBD_ID")))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("LOBD_ID")))))
    )
)

df_DentOut = (
    df_CurrDentLogic
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("svSrcSysCd"),F.lit(";"),F.col("svClclId"),F.lit(";"),F.col("svClckPayeeInd"),F.lit(";"),F.col("svLobdId")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_CHK_SK"),
        F.col("svClclId").alias("CLM_ID"),
        F.col("svClckPayeeInd").alias("CLM_CHK_PAYE_TYP_CD"),
        F.col("svLobdId").alias("CLM_CHK_LOB_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svClclId").alias("CLM_ID_1"),
        F.when(
            (F.col("CKCK_TYPE").isNull())|(F.length(trim(strip_field(F.col("CKCK_TYPE"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_TYPE"))))).alias("CLM_CHK_PAYMT_METH_CD"),
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull())|(F.length(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))).alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        rpad(F.lit("N"),1," ").alias("EXTRNL_CHK_IN"),
        F.when(F.col("HSA_LOBD_ID").isNull(),F.lit("N")).otherwise(F.lit("Y")).alias("PCA_CHK_IN"),
        F.when(
            (F.substring("CKCK_REISS_DT",1,10) != "1753-01-01"),
            F.substring("CKCK_PRINTED_DT",1,10)
        ).otherwise(F.substring("CKPY_PAY_DT",1,10)).alias("CHK_PD_DT"),
        F.when(
            (F.col("CLCK_NET_AMT").isNull())|(F.length(F.col("CLCK_NET_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT")).alias("CHK_NET_PAYMT_AMT"),
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull())|(F.length(F.col("CLCK_ORIG_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT")).alias("CHK_ORIG_AMT"),
        F.when(F.col("CKCK_CK_NO")>=0, F.col("CKCK_CK_NO")).otherwise(F.lit(0)).alias("CHK_NO"),
        F.when(F.col("CKCK_SEQ_NO")>=0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0)).alias("CHK_SEQ_NO"),
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull())|(F.length(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))==0),
            F.lit("")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))).alias("CHK_PAYE_NM"),
        F.when(
            (F.col("CKPY_REF_ID").isNull())|(F.length(trim(strip_field(F.col("CKPY_REF_ID"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKPY_REF_ID"))))).alias("CHK_PAYMT_REF_ID")
    )
)
df_hf_clm_chk_dent = scenarioA_dedup(
    df_DentOut,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# CurrRunInLogic
df_CurrRunInLogic = (
    df_LhoFACETS_RunIn
    .withColumn("svSrcSysCd", F.lit("LUMERIS"))
    .withColumn(
        "svClclId",
        F.when(
            (F.col("CLCL_ID").isNull())|
            (F.length(trim(strip_field(F.col("CLCL_ID"))))==0),
            F.lit("")
        ).otherwise(trim(strip_field(F.col("CLCL_ID"))))
    )
    .withColumn(
        "svClckPayeeInd",
        F.when(
            (F.col("CLCK_PAYEE_IND").isNull())|
            (F.length(trim(strip_field(F.col("CLCK_PAYEE_IND"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PAYEE_IND")))))
    )
    .withColumn(
        "svLobdId",
        F.when(
            (F.col("LOBD_ID").isNull())|
            (F.length(trim(strip_field(F.col("LOBD_ID"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("LOBD_ID")))))
    )
)
df_RunOut = (
    df_CurrRunInLogic
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("svSrcSysCd"),F.lit(";"),F.col("svClclId"),F.lit(";"),F.col("svClckPayeeInd"),F.lit(";"),F.col("svLobdId")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_CHK_SK"),
        F.col("svClclId").alias("CLM_ID"),
        F.col("svClckPayeeInd").alias("CLM_CHK_PAYE_TYP_CD"),
        F.col("svLobdId").alias("CLM_CHK_LOB_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svClclId").alias("CLM_ID_1"),
        F.when(
            (F.col("CKCK_TYPE").isNull())|(F.length(trim(strip_field(F.col("CKCK_TYPE"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_TYPE"))))).alias("CLM_CHK_PAYMT_METH_CD"),
        F.when(
            (F.col("CLCK_PYMT_OVRD_IND").isNull())|(F.length(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CLCK_PYMT_OVRD_IND"))))).alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        rpad(F.lit("N"),1," ").alias("EXTRNL_CHK_IN"),
        F.when(F.col("HSA_LOBD_ID").isNull(),F.lit("N")).otherwise(F.lit("Y")).alias("PCA_CHK_IN"),
        F.when(
            (F.substring("CKCK_REISS_DT",1,10) != "1753-01-01"),
            F.substring("CKCK_PRINTED_DT",1,10)
        ).otherwise(F.substring("CKPY_PAY_DT",1,10)).alias("CHK_PD_DT"),
        F.when(
            (F.col("CLCK_NET_AMT").isNull())|(F.length(F.col("CLCK_NET_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_NET_AMT")).alias("CHK_NET_PAYMT_AMT"),
        F.when(
            (F.col("CLCK_ORIG_AMT").isNull())|(F.length(F.col("CLCK_ORIG_AMT"))==0),
            F.lit(0.00)
        ).otherwise(F.col("CLCK_ORIG_AMT")).alias("CHK_ORIG_AMT"),
        F.when(F.col("CKCK_CK_NO")>=0, F.col("CKCK_CK_NO")).otherwise(F.lit(0)).alias("CHK_NO"),
        F.when(F.col("CKCK_SEQ_NO")>=0, F.col("CKCK_SEQ_NO")).otherwise(F.lit(0)).alias("CHK_SEQ_NO"),
        F.when(
            (F.col("CKCK_PAYEE_NAME").isNull())|(F.length(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))==0),
            F.lit("")
        ).otherwise(F.upper(trim(strip_field(F.col("CKCK_PAYEE_NAME"))))).alias("CHK_PAYE_NM"),
        F.when(
            (F.col("CKPY_REF_ID").isNull())|(F.length(trim(strip_field(F.col("CKPY_REF_ID"))))==0),
            F.lit("NA")
        ).otherwise(F.upper(trim(strip_field(F.col("CKPY_REF_ID"))))).alias("CHK_PAYMT_REF_ID")
    )
)
df_hf_clm_chk_runin = scenarioA_dedup(
    df_RunOut,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# Collect1 merges 5 inputs (ConvFileOut, MedFileIn, RunOutFileIn, DentFileIn, RunFileIn).
df_Collect1 = df_hf_clm_chk_conv.unionByName(df_hf_clm_chk_med) \
    .unionByName(df_hf_clm_chk_runout) \
    .unionByName(df_hf_clm_chk_dent) \
    .unionByName(df_hf_clm_chk_runin)

# Next stage: hf_clm_chk_collected (scenario A => deduplicate) => Reversals
df_hf_clm_chk_collected = scenarioA_dedup(
    df_Collect1,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# Reversals Transformer with a left join to df_hf_clm_fcts_reversals on (CollectOut.CLM_ID = fcts_reversals.CLCL_ID)
# We produce 2 outputs: "Straight" (no additional constraint) and "Reversals" with constraint 
#    "(IsNull(fcts_reversals.CLCL_ID) = @FALSE) AND ((fcts_reversals.CLCL_CUR_STS = '91') OR (fcts_reversals.CLCL_CUR_STS = '89'))"

df_reversals_joined = df_hf_clm_chk_collected.alias("CollectOut").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversals"),
    on = (F.col("CollectOut.CLM_ID") == F.col("fcts_reversals.CLCL_ID")),
    how = "left"
)

# Straight link: just pass everything
df_Straight = (
    df_reversals_joined.select(
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
        F.col("CollectOut.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID"),
    )
)

# Reversals link: constraint => fcts_reversals.CLCL_ID not null and fcts_reversals.CLCL_CUR_STS in ('91','89')
df_Reversals = (
    df_reversals_joined
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
        (
          (F.col("fcts_reversals.CLCL_CUR_STS") == "91") | 
          (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
        )
    )
    .select(
        F.col("CollectOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("CollectOut.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("CollectOut.DISCARD_IN").alias("DISCARD_IN"),
        F.col("CollectOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("CollectOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("CollectOut.ERR_CT").alias("ERR_CT"),
        F.col("CollectOut.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("CollectOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.concat(F.lit("LUMERIS;"),F.col("CollectOut.CLM_ID"),F.lit("R;"),F.col("CollectOut.CLM_CHK_PAYE_TYP_CD"),F.lit(";"),F.col("CollectOut.CLM_CHK_LOB_CD")).alias("PRI_KEY_STRING"),
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
        -F.col("CollectOut.CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
        -F.col("CollectOut.CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
        F.col("CollectOut.CHK_NO").alias("CHK_NO"),
        F.col("CollectOut.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
        F.col("CollectOut.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
        F.col("CollectOut.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
    )
)

###############################################################################
# Collect2 merges the Straight and Reversals
df_Collect2 = df_Straight.unionByName(df_Reversals)

# Next: hf_clm_chk_transformed => scenario A => deduplicate => then to "Logic"
df_hf_clm_chk_transformed = scenarioA_dedup(
    df_Collect2,
    ["CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD"]
)

###############################################################################
# Logic transformer has one primary output with all columns (AllCol) and one with fewer columns (Transform)

df_Logic = df_hf_clm_chk_transformed.alias("Transform1")

# We create two outputs from df_Logic
df_Logic_AllCol = (
    df_Logic
    .select(
        # The stage shows "SRC_SYS_CD_SK" as a primary key expression = "SrcSysCdSk",
        # That routine or expression is presumably a UDF or param. We'll pick it from the param: "SrcSysCdSk"? 
        # But the JSON says it is in the next stage. For now, we carry all columns plus "SRC_SYS_CD_SK"? 
        F.expr("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  
        F.col("Transform1.CLM_ID").alias("CLM_ID"),
        F.col("Transform1.CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
        F.col("Transform1.CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD"),
        F.col("Transform1.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Transform1.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Transform1.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Transform1.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Transform1.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Transform1.ERR_CT").alias("ERR_CT"),
        F.col("Transform1.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Transform1.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform1.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Transform1.CLM_CHK_SK").alias("CLM_CHK_SK"),
        F.col("Transform1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Transform1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Transform1.CLM_ID_1").alias("CLM_ID_1"),
        F.col("Transform1.CLM_CHK_PAYMT_METH_CD").alias("CLM_CHK_PAYMT_METH_CD"),
        F.col("Transform1.CLM_REMIT_HIST_PAYMTOVRD_CD").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        F.col("Transform1.EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
        F.col("Transform1.PCA_CHK_IN").alias("PCA_CHK_IN"),
        F.col("Transform1.CHK_PD_DT").alias("CHK_PD_DT"),
        F.col("Transform1.CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
        F.col("Transform1.CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
        F.col("Transform1.CHK_NO").alias("CHK_NO"),
        F.col("Transform1.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
        F.col("Transform1.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
        F.col("Transform1.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
    )
)

df_Logic_Transform = (
    df_Logic
    .select(
        F.expr("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("Transform1.CLM_ID").alias("CLM_ID"),
        F.col("Transform1.CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
        F.col("Transform1.CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD")
    )
)

###############################################################################
# ClmChkPK shared container stage has 2 input pins: "AllCol" and "Transform" => 1 output pin => "Key"

params_ClmChkPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": "<...>",  # Not directly used inside container by name here, but we keep placeholders if needed
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner,
    "CurrentDate": CurrentDate
}
df_ClmChkPK = ClmChkPK(df_Logic_AllCol, df_Logic_Transform, params_ClmChkPK)

###############################################################################
# Finally, "LhoFctsClmChkExtr" is a CSeqFileStage that writes to file:
# "key/LhoFctsClmChkExtr.LhoFctsClmChk.dat.#RunID#"
# => preserve .dat extension
final_file_path = f"{adls_path}/key/LhoFctsClmChkExtr.LhoFctsClmChk.dat.{RunID}"

# The columns to be written match the final output columns in the JSON for that stage:
#  JOB_EXCTN_RCRD_ERR_SK,int
#  INSRT_UPDT_CD,char(10)
#  DISCARD_IN,char(1)
#  PASS_THRU_IN,char(1)
#  FIRST_RECYC_DT,timestamp
#  ERR_CT,int
#  RECYCLE_CT,numeric
#  SRC_SYS_CD,varchar
#  PRI_KEY_STRING,varchar
#  CLM_CHK_SK,int
#  CLM_ID,char(18)
#  CLM_CHK_PAYE_TYP_CD,char or varchar
#  CLM_CHK_LOB_CD,char or varchar
#  CRT_RUN_CYC_EXCTN_SK,int
#  LAST_UPDT_RUN_CYC_EXCTN_SK,int
#  CLM_ID_1,char(18)
#  CLM_CHK_PAYMT_METH_CD,varchar
#  CLM_REMIT_HIST_PAYMTOVRD_CD,varchar
#  EXTRNL_CHK_IN,char(1)
#  PCA_CHK_IN,char(1)
#  CHK_PD_DT,date
#  CHK_NET_PAYMT_AMT,decimal
#  CHK_ORIG_AMT,decimal
#  CHK_NO,int
#  CHK_SEQ_NO,int
#  CHK_PAYE_NM,varchar
#  CHK_PAYMT_REF_ID,varchar

df_final = df_ClmChkPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").cast(IntegerType()).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").cast(TimestampType()).alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").cast(IntegerType()).alias("ERR_CT"),
    F.col("RECYCLE_CT").cast(DecimalType(15,2)).alias("RECYCLE_CT"),
    rpad(F.col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
    rpad(F.col("PRI_KEY_STRING"), 50, " ").alias("PRI_KEY_STRING"),
    F.col("CLM_CHK_SK").cast(IntegerType()).alias("CLM_CHK_SK"),
    rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    rpad(F.col("CLM_CHK_PAYE_TYP_CD"), 50, " ").alias("CLM_CHK_PAYE_TYP_CD"),
    rpad(F.col("CLM_CHK_LOB_CD"), 50, " ").alias("CLM_CHK_LOB_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("CLM_ID_1"), 18, " ").alias("CLM_ID_1"),
    rpad(F.col("CLM_CHK_PAYMT_METH_CD"), 50, " ").alias("CLM_CHK_PAYMT_METH_CD"),
    rpad(F.col("CLM_REMIT_HIST_PAYMTOVRD_CD"), 50, " ").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
    rpad(F.col("EXTRNL_CHK_IN"), 1, " ").alias("EXTRNL_CHK_IN"),
    rpad(F.col("PCA_CHK_IN"), 1, " ").alias("PCA_CHK_IN"),
    F.col("CHK_PD_DT").cast(DateType()).alias("CHK_PD_DT"),
    F.col("CHK_NET_PAYMT_AMT").cast(DecimalType(15,2)).alias("CHK_NET_PAYMT_AMT"),
    F.col("CHK_ORIG_AMT").cast(DecimalType(15,2)).alias("CHK_ORIG_AMT"),
    F.col("CHK_NO").cast(IntegerType()).alias("CHK_NO"),
    F.col("CHK_SEQ_NO").cast(IntegerType()).alias("CHK_SEQ_NO"),
    rpad(F.col("CHK_PAYE_NM"), 50, " ").alias("CHK_PAYE_NM"),
    rpad(F.col("CHK_PAYMT_REF_ID"), 50, " ").alias("CHK_PAYMT_REF_ID")
)

write_files(
    df_final,
    final_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)