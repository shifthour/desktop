# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsClmLnMedExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC \(9)Extract Claim Line Data extract from Facets, constructs reference file 
# MAGIC \(9)containing final disposition code for claims
# MAGIC                construct hash files to use in FctsClmLnTrsn
# MAGIC 
# MAGIC PRIMARY SOURCE:
# MAGIC \(9)CMC_CDML_CL_LINE\(9)- Facets claim line records
# MAGIC 
# MAGIC 
# MAGIC SECONDARY SOURCES:
# MAGIC          used to create hash file used in claim
# MAGIC                 CMC_CDMD_LI_DISALL - create hf_clm_ln_disall 
# MAGIC                 CMC_CDOR_LI_OVR        create hf_med_ovr
# MAGIC \(9)
# MAGIC        used to create hash files used in FctsClmLnTrns
# MAGIC     
# MAGIC                CMC_CDOR_LI_OVR
# MAGIC                CMC_CLCL_CLAIM
# MAGIC                 CMC_CLHP_HOSP
# MAGIC                 CMC_PSCD_POS_DESC
# MAGIC                 CMC_CLMI_MISC
# MAGIC                  CDDO_DNLI_OVR
# MAGIC                  CDDL_CL_LINE
# MAGIC \(9)TMP_IDS_CLAIM\(9)- Temporary table containing list of claim id's to 
# MAGIC \(9)\(9)\(9)  process (populated in ClmDriverBuild)
# MAGIC 
# MAGIC          IDS    CD_MPPNG
# MAGIC                    DSALW_EXCD
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC \(9)TmpOutFile param\(9)- Sequential file containing extracted claim line data to be transformed (../verified)
# MAGIC \(9)\(9)  
# MAGIC \(9)hf_clm_sts\(9)- Contains claim status based on claim and final 
# MAGIC \(9)\(9)\(9)  disposition code.  Used by claim extract/transform 
# MAGIC \(9)\(9)\(9)  process.
# MAGIC                hf_clm_ln_cdmd_disall - used in FctsClmLnExtr
# MAGIC                hf_clm_ln_cdor_ovr - used in FctsClmLnExtr
# MAGIC                hf_clm_ln_cdsm_msupp - used in FctsClmLnExtr
# MAGIC                hf_clm_ln_cdcb_cob - used in FctsClmLnExtr
# MAGIC               hf_clm_ln_cdor_ovr_pt - used in FctsClmLnExtr
# MAGIC               hf_clm_ln_cdmd_disall_pt - used in FctsClmLnExtr
# MAGIC               hf_clm_ln_cdmd_provcntrct - used in FctsClmLnTrns
# MAGIC               hf_clm_ln_clor - used in FctsClmLnTrns
# MAGIC               hf_clm_ln_chlp - used in FctsClmLntrns
# MAGIC               hf_clm_ln_pscd
# MAGIC                hf_clm_ln_subtype - used in FctsClmLnTrns
# MAGIC                hf_clm_ln_cdmd_type - created and cleared in this job
# MAGIC               hf_clm_ln_ovr_y08
# MAGIC               hf_clm_ln_cdor_u
# MAGIC                W_BAL_IDS_CLM_LN.dat for balancing
# MAGIC 
# MAGIC JOB PARAMETERS:
# MAGIC \(9)FacetsServer\(9)- Server where Facets database resides
# MAGIC \(9)FacetsSRC\(9)- Name of database containing Facets data
# MAGIC \(9)FacetsDB\(9)\(9)- Database\\owner of Facets tables
# MAGIC \(9)FacetsAcct\(9)- Authenticated user for connecting to Facets database
# MAGIC \(9)FacetsPW\(9)- Authenticated password for connecting to Facets database
# MAGIC \(9)FilePath\(9)\(9)- Directory path for sequential data files
# MAGIC \(9)TmpOutFile\(9)- Name of sequential output file
# MAGIC \(9)jpClmSts\(9)\(9)- List of available status codes for active claims
# MAGIC \(9)jpClmStsClsd\(9)- Status codes for closed or deleted claims
# MAGIC                 IDS parameters
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC \(9)Developer\(9)Date\(9)\(9)Comment
# MAGIC \(9)-----------------------------\(9)---------------------------\(9)-----------------------------------------------------------
# MAGIC \(9)Kevin Soderlund\(9)03/09/2004\(9)Originally Programmed
# MAGIC \(9)Kevin Soderlund\(9)05/01/2004\(9)Coding complete for release 1.0
# MAGIC \(9)Kevin Soderlund\(9)06/28/2004\(9)Updated with changes for release 1.1
# MAGIC \(9)Kevin Soderlund\(9)08/25/2004\(9)Updated final disposition code logic
# MAGIC \(9)Kevin Soderlund\(9)09/30/2004\(9)Updated final disposition code logic for "NONPRICE"
# MAGIC                 Brent Leland            10/07/2004             Changed job name from FctsClmLnExtrDispCd to FctsClmLnDispCdExtr
# MAGIC                 Brent Leland            12/14/2004             Added load of hashfiles that were done in FctsClmTrns job.
# MAGIC                 Sharon Andrew       1/20/2005               Added ClmLnSequence Nbr to ClmLnStatus hash file.     
# MAGIC                 Sharon Andrew       7/18/2005               Took out CMC_CDCB_LI_COB from main extract.   COB is not pulled / populated on the Claim LIne table anymore.   It was the following:
# MAGIC \(9)\(9)\(9)\(9)\(9) INNER JOIN tempdb..#DriverTable# TMP ON TMP.CLM_ID = CL_LINE.CLCL_ID
# MAGIC \(9)\(9)\(9)\(9)\(9) LEFT OUTER JOIN #FacetsDB#.CMC_CDCB_LI_COB LI_COB ON
# MAGIC     \(9)\(9)\(9)\(9)\(9)         LI_COB.CLCL_ID = CL_LINE.CLCL_ID AND 
# MAGIC     \(9)\(9)\(9)\(9)\(9)         LI_COB.CDML_SEQ_NO = CL_LINE.CDML_SEQ_NO
# MAGIC      \(9)\(9)\(9)\(9)\(9) LEFT OUTER JOIN #FacetsDB#.CMC_EXCD_EXPL_CD EXCD ON CL_LINE.CDML_DISALL_EXCD = EXCD.EXCD_ID
# MAGIC                Sharon Andrew      9/1/2005                   In determining claim line status and for suspended - added following criteria
# MAGIC                                                                                Then If (Len(Trim(CMC_CDML_CL_LINE.CDML_DISALL_EXCD)) = 0 and (CMC_CDML_CL_LINE.CDML_CONSIDER_CHG=0))  Then "NONPRICE"  Else "DENIEDREJ" Else "ACPTD"
# MAGIC 
# MAGIC BJ 1/13/2006 - I have redone the job description, it was for FctsClmLnDispCdExtr......so we don't know what modifications listed above belong to this job.
# MAGIC                 Brent Leland           12/27/2005             Added extract for balancing Claim Line table to Facets.
# MAGIC                 BJ Luce                1/13/2006 - added logic to use excd codes on IDS DSALW_EXCD
# MAGIC                                                                  change name of hash files to hf_clm_ln_xxxxx instead of hf_cdml_xxxxxx
# MAGIC                                              2/28/2006    check for CDML_DISALL_AMT <> 0 for disallow logic
# MAGIC                Steph Goddard     03/31/2006   Changed CDML_NON_COV_CHG to CDML_OOP_CALC_BASE and pulled from Facets to populate MBR_LIAB_BSS_AMT in transform
# MAGIC                                                                   added dental to Provider Contract extract for disallow non par savings amount
# MAGIC                Steph Goddard   04/19/2006   added hash file for clm_nasco_dup_bypass so these claims aren't written to balancing table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer\(9)Date\(9)\(9)Project                Comment                                                                                                                                              Development Project      Code Reviewer          Date Reviewed
# MAGIC -----------------------------\(9)---------------------------\(9)-----------                -----------------------------------------------------------                                                                                                 -----------------------------------     ------------------------          -------------------------
# MAGIC Rick Henry              2012-05-02               4896                   Pull IPCD_TYPE from Facets for Proc_Cd_Sk lookup                                                                         NewDevl                              SAndrew                 2012-05-18
# MAGIC SAndrew                   2012-07-12            ProdSupprt           Moved the derivation of the IPCD_TYPE from the main extract sql to a lookup                                  
# MAGIC                                                                                           hf_clm_ln_med_ipcd_cd_type
# MAGIC Kalyan Neelam          2013-01-28    4963 VBB Phase 3  Added 3 new columns on end - VBB_RULE_ID,                                                                                     IntegrateNewDevl          Bhoomi Dasari            3/14/2013
# MAGIC                                                                                          VBB_EXCD_ID, CLM_LN_VBB_IN. Added CMC_CDSD_SUPP_DATA and IDS SVC table lookups
# MAGIC 
# MAGIC Manasa Andru          2014-10-17          TFS - 9580          Added hf_clm_ln_its_home_dscn_amt hahsed file, 2 new fields(ITS_SUPLMT_DSCNT_AMT and       IntegrateCurDevl             Kalyan Neelam            2014-10-22
# MAGIC                                                                                                           ITS_SRCHRG_AMT) and updated CDML_ITS_DISC_AMT and  
# MAGIC                                                                                                                  CDML_PR_PYMT_AMT as per the new mapping rules.
# MAGIC K Chintalapani           2015-07-02       TFS - 9987          Used the Trim function in the Facets source stage for Claim line extract to remove the spaces in
# MAGIC                                                                                           CLCL_ID to  account for missing claimlines.                                                                                          IntegrateDev1                  Kalyan Neelam            2015-07-02
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07          5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,                                                                 IntegrateDev1                Kalyan Neelam            2017-08-29
# MAGIC                                                                                        NDC_UNIT_CT) at the end
# MAGIC                                                                                        
# MAGIC Jaideep Mankala   2017-11-20        5828                      Added new field to identify MED / PDX claim when passing to Fkey job  \(9)\(9)\(9)    IntegrateDev2
# MAGIC 
# MAGIC Madhavan B          2018-02-06        5792 \(9)     Changed the datatype of the column                                                                                                        IntegrateDev1                   Kalyan Neelam            2018-02-08
# MAGIC                                                       \(9)\(9)     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Mrudula Kodali      2020-11-23        US310395           Added new fields APC_ID and APC_STTUS_ID                                                                                      IntegrateDev2\(9)Abhiram Dasarathy\(9)2020-12-14
# MAGIC 
# MAGIC Rojarani K              2021-09-24        US212909          Modify transformation rules for CLM_LN.NDC_UNIT_CT to include:                                                       IntegrateDev2                     Raja Gummadi             2021-09-28
# MAGIC                                                                                        If CDSD_NDC_UNITS is non-numeric then zero-length string.
# MAGIC Prabhu ES            2022-02-26       S2S Remediation      MSSQL connection parameters added                                                                                                 IntegrateDev5\(9)Ken Bradmon\(9)2022-06-10

# MAGIC Added APCD_ID & APSI_STS_IND columns to CMC_CDML_CL_LINE table as part of 310395
# MAGIC Hashed file hf_clm_sts contains claim status based on claim and final disposition code.  Used by claim extract/transform process.
# MAGIC 
# MAGIC Created in this process but updated in FctsDntlClmLineExtr
# MAGIC Validate extracted data, remove trailing spaces, and determine final disposition code
# MAGIC Sequential file (parameterized name) containing extracted claim line data to be transformed (../verified)
# MAGIC hashed file hf_clm_ln_dsalw_ln_amts created in FctsClmLnHashFileExtr3
# MAGIC 
# MAGIC also used in:
# MAGIC FctsClmLnMedTrns
# MAGIC FctsDntlClmLineExtr
# MAGIC FctsClmLnDntlTrns
# MAGIC 
# MAGIC Do not clear in this process
# MAGIC Extracts medical claim line data from CMC_CDML_CL_LINE  based on claim id's in temporary table TMP_IDS_CLAIM (populated in ClmDriverBuild)
# MAGIC Extract list of claim lines to be used in balancing
# MAGIC These hash files go to FctsClmLnMedTrns and FctsClmLnDntlTrns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

TmpOutFile = get_widget_value('TmpOutFile','')
jpClmStsALL = get_widget_value('jpClmStsALL','')
jpClmStsClsd = get_widget_value('jpClmStsClsd','')
DriverTable = get_widget_value('DriverTable','')
ipClmStsActive = get_widget_value('ipClmStsActive','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
RunID = get_widget_value('RunID','')

# --------------------------------------------------------------------------------
# 1) Stages: DB2Connector for Ids (Database=IDS)
#    Two output pins: (a) "DsalwExcdCdml" – originally had placeholders
#                     (b) "SVC"
#    We remove the parameter placeholders and load entire relevant sets for lookups.
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# (a) DsalwExcdCdml: Remove ? placeholders and ORDER BY, returning entire relevant subset
extract_query_Ids_DsalwExcdCdml = f"""
SELECT 
  de.EXCD_ID,
  de.EFF_DT_SK,
  de.TERM_DT_SK,
  cm1.SRC_CD
FROM {IDSOwner}.DSALW_EXCD de,
     {IDSOwner}.CD_MPPNG cm1,
     {IDSOwner}.CD_MPPNG cm2
WHERE de.SRC_SYS_CD_SK = cm2.CD_MPPNG_SK
  AND cm2.SRC_CD = 'FACETS'
  AND de.EXCD_RESP_CD_SK = cm1.CD_MPPNG_SK
"""

df_Ids_DsalwExcdCdml = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Ids_DsalwExcdCdml)
    .load()
)

# (b) SVC
extract_query_Ids_SVC = f"""
SELECT SVC.SVC_ID as SVC_ID 
FROM {IDSOwner}.SVC SVC 
WHERE SVC.VBB_IN = 'Y'
"""
df_Ids_SVC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Ids_SVC)
    .load()
)

# --------------------------------------------------------------------------------
# 2) Read hashed files that appear as CHashedFileStage with no corresponding writes
#    All are scenario C => read from "<filename>.parquet"
# --------------------------------------------------------------------------------

df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet("hf_clm_ln_dsalw_ln_amts.parquet")
df_hf_clm_ln_med_svc_vbbin_lkup = spark.read.parquet("hf_clm_ln_med_svc_vbbin_lkup.parquet")
df_hf_clm_ln_pscd = spark.read.parquet("hf_clm_ln_pscd.parquet")  # "hfclmln" stage
df_hf_clm_ln_ovr_y08 = spark.read.parquet("hf_clm_ln_ovr_y08.parquet")
df_hf_clm_ln_med_ipcd_cd_type = spark.read.parquet("hf_clm_ln_med_ipcd_cd_type.parquet")
df_hf_clm_ln_med_cdsd_supp_data = spark.read.parquet("hf_clm_ln_med_cdsd_supp_data.parquet")
df_hf_clm_ln_its_home_dscn_amt = spark.read.parquet("hf_clm_ln_its_home_dscn_amt.parquet")

# We will also later read/write other hashed files similarly.

# --------------------------------------------------------------------------------
# 3) ODBCConnector (FacetsOwner) – multiple output pins each with distinct query
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# For "CMC_PSCD_POS_DESC" → pinned to "hfclmln"
# The JSON did not provide explicit SQL, we infer that columns match "hfclmln" columns: (PSCD_ID, TPCT_MCTR_SETG)
extract_query_Facets_CMC_PSCD_POS_DESC = f"""
SELECT
 PSCD_ID,
 TPCT_MCTR_SETG
FROM {FacetsOwner}.CMC_PSCD_POS_DESC
"""
df_Facets_CMC_PSCD_POS_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_CMC_PSCD_POS_DESC)
    .load()
)

# For "CMC_IPCD_PROC_CD" → pinned to "hf_clm_ln_med_ipcd_cd_type"
extract_query_Facets_CMC_IPCD_PROC_CD = f"""
SELECT 
  -- Example columns not fully specified in JSON, but we glean partial structure
  IPCD_ID,
  IPCD_TYPE
FROM {FacetsOwner}.CMC_IPCD_PROC_CD
-- No explicit filter
"""
df_Facets_CMC_IPCD_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_CMC_IPCD_PROC_CD)
    .load()
)

# For "CMC_CDSD_SUPP_DATA" → pinned to "hf_clm_ln_med_cdsd_supp_data"
extract_query_Facets_CMC_CDSD_SUPP_DATA = f"""
SELECT 
 CAST(Trim(SUPP.CLCL_ID)  as  CHAR(12)) as CLCL_ID,
 SUPP.CDML_SEQ_NO,
 SUPP.VBBD_RULE,
 SUPP.CDSD_VBB_EXCD_ID
FROM {FacetsOwner}.CMC_CDSD_SUPP_DATA SUPP,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = SUPP.CLCL_ID
"""
df_Facets_CMC_CDSD_SUPP_DATA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_CMC_CDSD_SUPP_DATA)
    .load()
)

# For "CMC_CDMI_LI_ITS" → pinned to "hf_clm_ln_its_home_dscn_amt"
extract_query_Facets_CMC_CDMI_LI_ITS = f"""
SELECT 
 CAST(Trim(DSCN.CLCL_ID) as CHAR(12)) as CLCL_ID,
 DSCN.CDML_SEQ_NO,
 DSCN.CDMI_SUPP_DISC_AMT_NVL,
 DSCN.CDMI_SURCHG_AMT
FROM {FacetsOwner}.CMC_CDMI_LI_ITS DSCN,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = DSCN.CLCL_ID
"""
df_Facets_CMC_CDMI_LI_ITS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_CMC_CDMI_LI_ITS)
    .load()
)

# For "CMC_CDML_CL_LINE" → pinned to "tTrimFields"
extract_query_Facets_CMC_CDML_CL_LINE = f"""
SELECT
 CAST(Trim(CL_LINE.CLCL_ID) as CHAR(12)) as CLCL_ID,
 CL_LINE.CDML_SEQ_NO,
 CL_LINE.CDML_CONSIDER_CHG,
 CL_LINE.CDML_AG_PRICE,
 CL_LINE.CDML_ALLOW,
 CL_LINE.CDML_AUTHSV_SEQ_NO,
 CL_LINE.CDML_CAP_IND,
 CL_LINE.CDML_CHG_AMT,
 CL_LINE.CDML_CL_NTWK_IND,
 CL_LINE.CDML_COINS_AMT,
 CL_LINE.CDML_COPAY_AMT,
 CL_LINE.CDML_CUR_STS,
 CL_LINE.CDML_DED_ACC_NO,
 CL_LINE.CDML_DED_AMT,
 CL_LINE.CDML_DISALL_AMT,
 CL_LINE.CDML_DISALL_EXCD,
 CL_LINE.CDML_EOB_EXCD,
 CL_LINE.CDML_FROM_DT,
 CL_LINE.CDML_IP_PRICE,
 CL_LINE.CDML_ITS_DISC_AMT,
 CL_LINE.CDML_PAID_AMT,
 CL_LINE.CDML_PC_IND,
 CL_LINE.CDML_PF_PRICE,
 CL_LINE.CDML_PR_PYMT_AMT,
 CL_LINE.CDML_PRE_AUTH_IND,
 CL_LINE.CDML_PRICE_IND,
 CL_LINE.CDML_REF_IND,
 CL_LINE.CDML_REFSV_SEQ_NO,
 CL_LINE.CDML_RISK_WH_AMT,
 CL_LINE.CDML_ROOM_TYPE,
 CL_LINE.CDML_SB_PYMT_AMT,
 CL_LINE.CDML_SE_PRICE,
 CL_LINE.CDML_SUP_DISC_AMT,
 CL_LINE.CDML_TO_DT,
 CL_LINE.CDML_UMAUTH_ID,
 CL_LINE.CDML_UMREF_ID,
 CL_LINE.CDML_UNITS,
 CL_LINE.CDML_UNITS_ALLOW,
 TMP.CLM_STS,
 CL_LINE.DEDE_PFX,
 CAST(Trim(CL_LINE.IPCD_ID) as CHAR(7)) AS IPCD_ID,
 CL_LINE.LOBD_ID,
 CL_LINE.LTLT_PFX,
 CL_LINE.PDVC_LOBD_PTR,
 CL_LINE.PRPR_ID,
 CL_LINE.PSCD_ID,
 CL_LINE.RCRC_ID,
 CL_LINE.SEPC_PRICE_ID,
 CL_LINE.SEPY_PFX,
 CL_LINE.SESE_ID,
 CL_LINE.SESE_RULE,
 CLM.CLCL_CL_SUB_TYPE,
 CLM.CLCL_NTWK_IND,
 CLM.CLCL_PAID_DT,
 CL_LINE.CDML_OOP_CALC_BASE,
 CAST(Trim(CL_LINE.IPCD_ID) as CHAR(7)) AS CDML_IPCD_ID,
 SUPP_DATA.CDSD_NDC_CODE,
 SUPP_DATA.CLCL_ID AS CLCL_ID_SUPP,
 SUPP_DATA.CDSD_NDC_UNITS,
 SUPP_DATA.CDSD_NDC_MCTR_TYPE,
 CL_LINE.APCD_ID,
 CL_LINE.APSI_STS_IND
FROM
     tempdb..{DriverTable} TMP
     JOIN {FacetsOwner}.CMC_CLCL_CLAIM CLM ON CLM.CLCL_ID=TMP.CLM_ID
     JOIN {FacetsOwner}.CMC_CDML_CL_LINE CL_LINE ON CL_LINE.CLCL_ID=TMP.CLM_ID
     LEFT OUTER JOIN {FacetsOwner}.CMC_CDSD_SUPP_DATA SUPP_DATA
       ON CL_LINE.CLCL_ID = SUPP_DATA.CLCL_ID
      AND CL_LINE.CDML_SEQ_NO = SUPP_DATA.CDML_SEQ_NO
"""
df_Facets_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_CMC_CDML_CL_LINE)
    .load()
)

# For "CMC_CDOR_LI_OVR" → pinned to "hf_clm_ln_ovr_y08"
extract_query_Facets_CMC_CDOR_LI_OVR = f"""
SELECT 
 CAST(Trim(OVR.CLCL_ID)  as  CHAR(12))  as CLCL_ID,
 OVR.CDML_SEQ_NO,
 OVR.CDOR_OR_AMT
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CDOR_LI_OVR OVR
WHERE TMP.CLM_ID = OVR.CLCL_ID
  AND CDOR_OR_ID = 'AX'
  AND EXCD_ID = 'Y08'
"""
df_Facets_CMC_CDOR_LI_OVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_CMC_CDOR_LI_OVR)
    .load()
)

# --------------------------------------------------------------------------------
# Now read them into their hashed-file replacements for the "Facets" stage outputs
# in scenario C (some are already read above if same name).
# --------------------------------------------------------------------------------
# We'll assume we have them as above, so that the tTrimFields stage can merge them.
# (Already assigned to df_* variables.)

# --------------------------------------------------------------------------------
# 4) The main Transformer (tTrimFields) merges everything from "CMC_CDML_CL_LINE" as primary,
#    plus multiple left lookups from df_Ids_DsalwExcdCdml, df_Ids_SVC, hashed-file DFs, etc.
#    Then creates stage variables, final columns for two output pins:
#    "FctsClmLn" => goes to a SeqFile "FctsClmLnMedExtr"
#    "hf_clm_sts" => goes to hashed file "hf_clm_sts"
# --------------------------------------------------------------------------------

# PRIMARY LINK: "CMC_CDML_CL_LINE" (df_Facets_CMC_CDML_CL_LINE)
df_main = df_Facets_CMC_CDML_CL_LINE.alias("CMC_CDML_CL_LINE")

# Join "lkupSvcLocTypCd" => hfclmln (df_hf_clm_ln_pscd) on CMC_CDML_CL_LINE.PSCD_ID = lkupSvcLocTypCd.PSCD_ID
df_main = df_main.join(
    df_hf_clm_ln_pscd.alias("lkupSvcLocTypCd"),
    on=[df_main["PSCD_ID"] == F.col("lkupSvcLocTypCd.PSCD_ID")],
    how="left"
)

# Join "DsalwExcdCdml" => df_Ids_DsalwExcdCdml with conditions:
#   df_main["CDML_DISALL_EXCD"] = EXCD_ID
#   df_main["CLCL_PAID_DT"].substr(0,10) = EFF_DT_SK
#   df_main["CLCL_PAID_DT"].substr(0,10) = TERM_DT_SK
# We replicate these as left join on all three conditions.  Note that we must compare string to string.
df_main = df_main.join(
    df_Ids_DsalwExcdCdml.alias("DsalwExcdCdml"),
    (df_main["CDML_DISALL_EXCD"] == F.col("DsalwExcdCdml.EXCD_ID"))
    & (F.substring(df_main["CLCL_PAID_DT"], 1, 10) == F.col("DsalwExcdCdml.EFF_DT_SK"))
    & (F.substring(df_main["CLCL_PAID_DT"], 1, 10) == F.col("DsalwExcdCdml.TERM_DT_SK")),
    how="left"
)

# Next 5 references to hf_clm_ln_dsalw_ln_amts with different link names => "RefMY","RefNY","RefNN","RefOY","RefPY"
# Each is a left join on:
#   CLCL_ID = CLCL_ID
#   CDML_SEQ_NO = CDML_SEQ_NO
#   and EXCD_RESP_CD, BYPS_IN are constants or 'N'/'Y' as given
# We do them one by one, then keep columns with distinct aliases to check if row is found.

# RefMY: EXCD_RESP_CD='M', BYPS_IN='Y'
df_main = df_main.join(
    df_hf_clm_ln_dsalw_ln_amts.alias("RefMY"),
    (df_main["CLCL_ID"] == F.col("RefMY.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("RefMY.CDML_SEQ_NO"))
    & (F.lit("M") == F.col("RefMY.EXCD_RESP_CD"))
    & (F.lit("Y") == F.col("RefMY.BYPS_IN")),
    how="left"
)

# RefNY: EXCD_RESP_CD='N', BYPS_IN='Y'
df_main = df_main.join(
    df_hf_clm_ln_dsalw_ln_amts.alias("RefNY"),
    (df_main["CLCL_ID"] == F.col("RefNY.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("RefNY.CDML_SEQ_NO"))
    & (F.lit("N") == F.col("RefNY.EXCD_RESP_CD"))
    & (F.lit("Y") == F.col("RefNY.BYPS_IN")),
    how="left"
)

# RefNN: EXCD_RESP_CD='N', BYPS_IN='N'
df_main = df_main.join(
    df_hf_clm_ln_dsalw_ln_amts.alias("RefNN"),
    (df_main["CLCL_ID"] == F.col("RefNN.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("RefNN.CDML_SEQ_NO"))
    & (F.lit("N") == F.col("RefNN.EXCD_RESP_CD"))
    & (F.lit("N") == F.col("RefNN.BYPS_IN")),
    how="left"
)

# RefOY: EXCD_RESP_CD='O', BYPS_IN='Y'
df_main = df_main.join(
    df_hf_clm_ln_dsalw_ln_amts.alias("RefOY"),
    (df_main["CLCL_ID"] == F.col("RefOY.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("RefOY.CDML_SEQ_NO"))
    & (F.lit("O") == F.col("RefOY.EXCD_RESP_CD"))
    & (F.lit("Y") == F.col("RefOY.BYPS_IN")),
    how="left"
)

# RefPY: EXCD_RESP_CD='P', BYPS_IN='Y'
df_main = df_main.join(
    df_hf_clm_ln_dsalw_ln_amts.alias("RefPY"),
    (df_main["CLCL_ID"] == F.col("RefPY.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("RefPY.CDML_SEQ_NO"))
    & (F.lit("P") == F.col("RefPY.EXCD_RESP_CD"))
    & (F.lit("Y") == F.col("RefPY.BYPS_IN")),
    how="left"
)

# Join "Y08" => df_hf_clm_ln_ovr_y08
df_main = df_main.join(
    df_hf_clm_ln_ovr_y08.alias("Y08"),
    (df_main["CLCL_ID"] == F.col("Y08.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("Y08.CDML_SEQ_NO")),
    how="left"
)

# Join "ipcd_proc_cd" => df_hf_clm_ln_med_ipcd_cd_type
df_main = df_main.join(
    df_hf_clm_ln_med_ipcd_cd_type.alias("ipcd_proc_cd"),
    df_main["CDML_IPCD_ID"] == F.col("ipcd_proc_cd.IPCD_ID"),
    how="left"
)

# Join "cdsd_supp_data" => df_hf_clm_ln_med_cdsd_supp_data
df_main = df_main.join(
    df_hf_clm_ln_med_cdsd_supp_data.alias("cdsd_supp_data"),
    (df_main["CLCL_ID"] == F.col("cdsd_supp_data.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("cdsd_supp_data.CDML_SEQ_NO")),
    how="left"
)

# Join "svc_lkup" => df_hf_clm_ln_med_svc_vbbin_lkup, matching "Trim(CMC_CDML_CL_LINE.SESE_ID)" with "SVC_ID"
# We'll do a left join on F.trim(df_main["SESE_ID"]) == df_Ids_SVC["SVC_ID"]? Actually the stage says it uses df_hf_clm_ln_med_svc_vbbin_lkup. 
df_main = df_main.join(
    df_hf_clm_ln_med_svc_vbbin_lkup.alias("svc_lkup"),
    F.trim(df_main["SESE_ID"]) == F.col("svc_lkup.SVC_ID"),
    how="left"
)

# Join "its_home_dscn_amt" => df_hf_clm_ln_its_home_dscn_amt
df_main = df_main.join(
    df_hf_clm_ln_its_home_dscn_amt.alias("its_home_dscn_amt"),
    (df_main["CLCL_ID"] == F.col("its_home_dscn_amt.CLCL_ID"))
    & (df_main["CDML_SEQ_NO"] == F.col("its_home_dscn_amt.CDML_SEQ_NO")),
    how="left"
)

# --------------------------------------------------------------------------------
# Create the stage variables as columns in a chain of withColumn calls
# --------------------------------------------------------------------------------

# We will reference them in order, so we'll store intermediate columns with those names:

# svNA
df_main = df_main.withColumn(
    "svNA",
    F.when(
        (F.col("CDML_CUR_STS").isNotNull()) & (F.instr(jpClmStsALL, F.col("CDML_CUR_STS")) < 1),
        F.lit("NA")
    ).otherwise(F.lit(""))
)

# svClsdDel
df_main = df_main.withColumn(
    "svClsdDel",
    F.when(
        (F.trim(F.col("svNA")) == F.lit("NA")),
        F.lit("")
    ).otherwise(
        F.when(
            F.instr(jpClmStsClsd, F.col("CDML_CUR_STS")) > 0,
            F.lit("CLSDDEL")
        ).otherwise(F.lit(""))
    )
)

# svAcptd
df_main = df_main.withColumn(
    "svAcptd",
    F.when(
        (F.trim(F.col("svNA")) == F.lit("NA")) |
        (F.trim(F.col("svClsdDel")) == F.lit("CLSDDEL")) |
        (F.instr(ipClmStsActive, F.col("CDML_CUR_STS")) < 1),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("CDML_PR_PYMT_AMT").isNotNull() & (F.col("CDML_PR_PYMT_AMT") != 0))
            | (F.col("CDML_SB_PYMT_AMT").isNotNull() & (F.col("CDML_SB_PYMT_AMT") != 0)),
            F.lit("ACPTD")
        ).otherwise(F.lit(""))
    )
)

# svSuspend
df_main = df_main.withColumn(
    "svSuspend",
    F.when(
        (F.trim(F.col("svNA")) == F.lit("NA")) |
        (F.trim(F.col("svClsdDel")) == F.lit("CLSDDEL")) |
        (F.trim(F.col("svAcptd")) == F.lit("ACPTD")),
        F.lit("")
    ).otherwise(
        F.when(
            F.col("RefNY.CLCL_ID").isNotNull(),
            F.lit("SUSP")
        ).otherwise(
            F.when(
                F.col("RefOY.CLCL_ID").isNotNull() | F.col("RefPY.CLCL_ID").isNotNull() | F.col("RefMY.CLCL_ID").isNotNull(),
                F.lit("")
            ).otherwise(
                F.when(
                    F.col("RefNN.CLCL_ID").isNotNull(),
                    F.lit("SUSP")
                ).otherwise(F.lit(""))
            )
        )
    )
)

# svDenied
df_main = df_main.withColumn(
    "svDenied",
    F.when(
        (F.trim(F.col("svNA")) == F.lit("NA"))
        | (F.trim(F.col("svClsdDel")) == F.lit("CLSDDEL"))
        | (F.trim(F.col("svAcptd")) == F.lit("ACPTD"))
        | (F.trim(F.col("svSuspend")) == F.lit("SUSP")),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("CDML_CONSIDER_CHG") == 0) & (F.col("CDML_CHG_AMT") == 0),
            F.lit("DENIEDREJ")
        ).otherwise(
            F.when(
                (F.col("CDML_CONSIDER_CHG") <= (F.col("CDML_DISALL_AMT") - F.when(F.col("Y08.CLCL_ID").isNotNull(), F.col("Y08.CDOR_OR_AMT")).otherwise(F.lit(0)))),
                F.when(
                    (F.length(F.trim(F.col("CDML_DISALL_EXCD"))) == 0) & (F.col("CDML_CONSIDER_CHG") == 0),
                    F.lit("NONPRICE")
                ).otherwise(F.lit("DENIEDREJ"))
            ).otherwise(F.lit("ACPTD"))
        )
    )
)

# svFinalDispCd
df_main = df_main.withColumn(
    "svFinalDispCd",
    F.when(
        (F.trim(F.col("svNA")) == F.lit("NA")),
        F.col("svNA")
    ).otherwise(
        F.when(
            (F.trim(F.col("svClsdDel")) == F.lit("CLSDDEL")),
            F.col("svClsdDel")
        ).otherwise(
            F.when(
                (F.trim(F.col("svAcptd")) == F.lit("ACPTD")),
                F.col("svAcptd")
            ).otherwise(
                F.when(
                    (F.trim(F.col("svSuspend")) == F.lit("SUSP")),
                    F.col("svSuspend")
                ).otherwise(
                    F.when(
                        (F.trim(F.col("svDenied")) != F.lit("")),
                        F.col("svDenied")
                    ).otherwise(F.lit("ACPTD"))
                )
            )
        )
    )
)

# svVbbRuleId
df_main = df_main.withColumn(
    "svVbbRuleId",
    F.when(
        (F.col("cdsd_supp_data.VBBD_RULE").isNull()) | (F.length(F.trim(F.col("cdsd_supp_data.VBBD_RULE"))) == 0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("cdsd_supp_data.VBBD_RULE")))
)

# svNDC
df_main = df_main.withColumn(
    "svNDC",
    F.when(
        (F.col("CDSD_NDC_CODE").isNull())
        | (F.length(F.col("CDSD_NDC_CODE")) < 10)
        | (F.trim(F.col("CDSD_NDC_CODE")) == F.lit(""))
        | (F.col("CDSD_NDC_CODE").cast("double").isNull()),
        F.lit(1)
    ).otherwise(F.lit(0))
)

# --------------------------------------------------------------------------------
# Now produce the two outputs:
#   1) FctsClmLn -> CSeqFileStage "FctsClmLnMedExtr"
#   2) hf_clm_sts -> CHashedFileStage "hf_clm_sts"
# We carefully select columns in the given order, applying the final expression logic.
# --------------------------------------------------------------------------------

# 1) FctsClmLn
df_FctsClmLn = df_main.select(
    F.when(F.col("CLCL_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("CLCL_ID"))).alias("CLCL_ID"),
    F.when(F.col("CDML_SEQ_NO").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_SEQ_NO"))).alias("CDML_SEQ_NO"),
    F.when(F.col("CLM_STS").isNull(), F.lit("")).otherwise(F.trim(F.col("CLM_STS"))).alias("CLM_STS"),
    F.when(F.col("CDML_AG_PRICE").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_AG_PRICE"))).alias("CDML_AG_PRICE"),
    F.when(F.col("CDML_ALLOW").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_ALLOW"))).alias("CDML_ALLOW"),
    F.when(F.col("CDML_AUTHSV_SEQ_NO").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_AUTHSV_SEQ_NO"))).alias("CDML_AUTHSV_SEQ_NO"),
    F.when(F.col("CDML_CAP_IND").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_CAP_IND"))).alias("CDML_CAP_IND"),
    F.when(F.col("CDML_CHG_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_CHG_AMT"))).alias("CDML_CHG_AMT"),
    F.when(F.col("CDML_CL_NTWK_IND").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_CL_NTWK_IND"))).alias("CDML_CL_NTWK_IND"),
    F.when(F.col("CDML_COINS_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_COINS_AMT"))).alias("CDML_COINS_AMT"),
    F.when(F.col("CDML_CONSIDER_CHG").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_CONSIDER_CHG"))).alias("CDML_CONSIDER_CHG"),
    F.when(F.col("CDML_COPAY_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_COPAY_AMT"))).alias("CDML_COPAY_AMT"),
    F.when(F.col("CDML_CUR_STS").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_CUR_STS"))).alias("CDML_CUR_STS"),
    F.when(F.col("CDML_DED_ACC_NO").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_DED_ACC_NO"))).alias("CDML_DED_ACC_NO"),
    F.when(F.col("CDML_DED_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_DED_AMT"))).alias("CDML_DED_AMT"),
    F.when(F.col("CDML_DISALL_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_DISALL_AMT"))).alias("CDML_DISALL_AMT"),
    F.when(F.col("CDML_DISALL_EXCD").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_DISALL_EXCD"))).alias("CDML_DISALL_EXCD"),
    F.when(F.col("CDML_EOB_EXCD").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_EOB_EXCD"))).alias("CDML_EOB_EXCD"),
    F.when(F.col("CDML_FROM_DT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_FROM_DT"))).alias("CDML_FROM_DT"),
    F.when(F.col("CDML_IP_PRICE").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_IP_PRICE"))).alias("CDML_IP_PRICE"),
    # CDML_ITS_DISC_AMT logic
    F.when(
        F.col("CDML_ITS_DISC_AMT").isNull(),
        F.lit(0)
    ).otherwise(
        F.when(
            F.col("its_home_dscn_amt.CLCL_ID").isNotNull(),
            F.when(
                F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL").isNull(),
                F.col("CDML_ITS_DISC_AMT")
            ).otherwise(F.col("CDML_ITS_DISC_AMT") + F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL"))
        ).otherwise(F.col("CDML_ITS_DISC_AMT"))
    ).alias("CDML_ITS_DISC_AMT"),
    F.col("CDML_OOP_CALC_BASE").alias("CDML_OOP_CALC_BASE"),
    F.when(F.col("CDML_PAID_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_PAID_AMT"))).alias("CDML_PAID_AMT"),
    F.when(F.col("CDML_PC_IND").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_PC_IND"))).alias("CDML_PC_IND"),
    F.when(F.col("CDML_PF_PRICE").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_PF_PRICE"))).alias("CDML_PF_PRICE"),
    # CDML_PR_PYMT_AMT logic
    F.when(
        F.col("CDML_PR_PYMT_AMT").isNull(),
        F.lit(0)
    ).otherwise(
        F.when(
            F.col("its_home_dscn_amt.CLCL_ID").isNotNull(),
            F.when(
                F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL").isNull(),
                F.col("CDML_PR_PYMT_AMT")
            ).otherwise(F.col("CDML_PR_PYMT_AMT") + F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL"))
        ).otherwise(F.col("CDML_PR_PYMT_AMT"))
    ).alias("CDML_PR_PYMT_AMT"),
    F.when(F.col("CDML_PRE_AUTH_IND").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_PRE_AUTH_IND"))).alias("CDML_PRE_AUTH_IND"),
    F.when(F.col("CDML_PRICE_IND").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_PRICE_IND"))).alias("CDML_PRICE_IND"),
    F.when(F.col("CDML_REF_IND").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_REF_IND"))).alias("CDML_REF_IND"),
    F.when(F.col("CDML_REFSV_SEQ_NO").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_REFSV_SEQ_NO"))).alias("CDML_REFSV_SEQ_NO"),
    F.when(F.col("CDML_RISK_WH_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_RISK_WH_AMT"))).alias("CDML_RISK_WH_AMT"),
    F.when(F.col("CDML_ROOM_TYPE").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_ROOM_TYPE"))).alias("CDML_ROOM_TYPE"),
    F.when(F.col("CDML_SB_PYMT_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_SB_PYMT_AMT"))).alias("CDML_SB_PYMT_AMT"),
    F.when(F.col("CDML_SE_PRICE").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_SE_PRICE"))).alias("CDML_SE_PRICE"),
    F.when(F.col("CDML_SUP_DISC_AMT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_SUP_DISC_AMT"))).alias("CDML_SUP_DISC_AMT"),
    F.when(F.col("CDML_TO_DT").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_TO_DT"))).alias("CDML_TO_DT"),
    F.when(F.col("CDML_UMAUTH_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_UMAUTH_ID"))).alias("CDML_UMAUTH_ID"),
    F.when(F.col("CDML_UMREF_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_UMREF_ID"))).alias("CDML_UMREF_ID"),
    F.when(F.col("CDML_UNITS").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_UNITS"))).alias("CDML_UNITS"),
    F.when(F.col("CDML_UNITS_ALLOW").isNull(), F.lit("")).otherwise(F.trim(F.col("CDML_UNITS_ALLOW"))).alias("CDML_UNITS_ALLOW"),
    F.col("svFinalDispCd").alias("CLM_LN_FINL_DISP_CD"),
    F.when(F.col("DEDE_PFX").isNull(), F.lit("")).otherwise(F.trim(F.col("DEDE_PFX"))).alias("DEDE_PFX"),
    F.when(F.col("IPCD_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("IPCD_ID"))).alias("IPCD_ID"),
    F.when(F.col("LOBD_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("LOBD_ID"))).alias("LOBD_ID"),
    F.when(F.col("LTLT_PFX").isNull(), F.lit("")).otherwise(F.trim(F.col("LTLT_PFX"))).alias("LTLT_PFX"),
    F.when(F.col("PDVC_LOBD_PTR").isNull(), F.lit("")).otherwise(F.trim(F.col("PDVC_LOBD_PTR"))).alias("PDVC_LOBD_PTR"),
    F.when(F.col("PRPR_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("PRPR_ID"))).alias("PRPR_ID"),
    F.when(F.col("PSCD_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("PSCD_ID"))).alias("PSCD_ID"),
    F.when(F.col("RCRC_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("RCRC_ID"))).alias("RCRC_ID"),
    F.when(F.col("SEPC_PRICE_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("SEPC_PRICE_ID"))).alias("SEPC_PRICE_ID"),
    F.when(F.col("SEPY_PFX").isNull(), F.lit("")).otherwise(F.trim(F.col("SEPY_PFX"))).alias("SEPY_PFX"),
    F.when(F.col("SESE_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("SESE_ID"))).alias("SESE_ID"),
    F.when(F.col("SESE_RULE").isNull(), F.lit("")).otherwise(F.trim(F.col("SESE_RULE"))).alias("SESE_RULE"),
    F.col("CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE"),
    F.col("CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    # TPCT_MCTR_SETG
    F.when(F.col("lkupSvcLocTypCd.TPCT_MCTR_SETG").isNull(), F.lit("NA")).otherwise(F.col("lkupSvcLocTypCd.TPCT_MCTR_SETG")).alias("TPCT_MCTR_SETG"),
    # CDML_RESP_CD
    F.when(F.col("DsalwExcdCdml.SRC_CD").isNull(), F.lit("NA")).otherwise(F.col("DsalwExcdCdml.SRC_CD")).alias("CDML_RESP_CD"),
    # EXCD_FOUND
    F.when(F.col("DsalwExcdCdml.EXCD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y")).alias("EXCD_FOUND"),
    # IPCD_TYPE
    F.when(F.col("ipcd_proc_cd.IPCD_ID").isNull(), F.lit("UNK")).otherwise(F.col("ipcd_proc_cd.IPCD_TYPE")).alias("IPCD_TYPE"),
    # VBB_RULE
    F.col("svVbbRuleId").alias("VBB_RULE"),
    # CDSD_VBB_EXCD_ID
    F.when(
        (F.col("cdsd_supp_data.CDSD_VBB_EXCD_ID").isNull()) | (F.length(F.trim(F.col("cdsd_supp_data.CDSD_VBB_EXCD_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("cdsd_supp_data.CDSD_VBB_EXCD_ID"))).alias("CDSD_VBB_EXCD_ID"),
    # CLM_LN_VBB_IN
    F.when(
        (F.col("svVbbRuleId") != F.lit("NA")) | (F.col("svc_lkup.SVC_ID").isNotNull()),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("CLM_LN_VBB_IN"),
    # ITS_SUPLMT_DSCNT_AMT
    F.when(
        (F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL").isNull()) | (F.length(F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL")) == 0),
        F.lit(0)
    ).otherwise(F.col("its_home_dscn_amt.CDMI_SUPP_DISC_AMT_NVL")).alias("ITS_SUPLMT_DSCNT_AMT"),
    # ITS_SRCHRG_AMT
    F.when(
        (F.col("its_home_dscn_amt.CDMI_SURCHG_AMT").isNull()) | (F.length(F.col("its_home_dscn_amt.CDMI_SURCHG_AMT")) == 0),
        F.lit(0)
    ).otherwise(F.col("its_home_dscn_amt.CDMI_SURCHG_AMT")).alias("ITS_SRCHRG_AMT"),
    # NDC
    F.when(
        F.col("CLCL_ID_SUPP").isNull(),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.col("CDSD_NDC_CODE").cast("double").isNull())
            | (F.length(F.trim(F.col("CDSD_NDC_CODE"))) < 10)
            | (F.col("CDSD_NDC_CODE").isNull())
            | (F.length(F.trim(F.col("CDSD_NDC_CODE"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("CDSD_NDC_CODE")))
    ).alias("NDC"),
    # NDC_DRUG_FORM_CD
    F.when(
        F.col("CLCL_ID_SUPP").isNull(),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.col("CDSD_NDC_CODE").cast("double").isNull())
            | (F.length(F.trim(F.col("CDSD_NDC_CODE"))) < 10)
            | (F.col("CDSD_NDC_CODE").isNull())
            | (F.length(F.trim(F.col("CDSD_NDC_CODE"))) == 0),
            F.lit("NA")
        ).otherwise(
            F.when(
                (F.col("CDSD_NDC_MCTR_TYPE").isNull()) | (F.length(F.trim(F.col("CDSD_NDC_MCTR_TYPE"))) == 0),
                F.lit("UNK")
            ).otherwise(F.trim(F.col("CDSD_NDC_MCTR_TYPE")))
        )
    ).alias("NDC_DRUG_FORM_CD"),
    # NDC_UNIT_CT
    F.when(
        (F.col("CDSD_NDC_UNITS").isNull()) | (F.length(F.trim(F.col("CDSD_NDC_UNITS"))) == 0),
        F.lit("")
    ).otherwise(
        F.when(
            F.col("CDSD_NDC_UNITS").cast("double").isNull(),
            F.lit("")
        ).otherwise(F.trim(F.col("CDSD_NDC_UNITS")))
    ).alias("NDC_UNIT_CT"),
    # APCD_ID
    F.col("APCD_ID").alias("APCD_ID"),
    # APSI_STS_IND
    F.col("APSI_STS_IND").alias("APSI_STS_IND")
)

# 2) hf_clm_sts
df_hf_clm_sts = df_main.select(
    F.trim(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(
        (F.col("CDML_CUR_STS").isNull()) | (F.length(F.trim(F.col("CDML_CUR_STS"))) == 0),
        F.lit("00")
    ).otherwise(F.trim(F.col("CDML_CUR_STS"))).alias("CDML_CUR_STS"),
    F.col("svFinalDispCd").alias("CLM_LN_FINL_DISP_CD")
)

# --------------------------------------------------------------------------------
# Write FctsClmLn to .dat (CSeqFileStage) with the name "FctsClmLnMedExtr.ClmLnMed.dat.#RunID#"
# Directory = "verified" → does not contain "landing" or "external" → use adls_path
# ContainsHeader=false, WriteMode=overwrite
# --------------------------------------------------------------------------------
final_fctsclmln_path = f"{adls_path}/verified/FctsClmLnMedExtr.ClmLnMed.dat.{RunID}"
write_files(
    df_FctsClmLn,
    final_fctsclmln_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Write hf_clm_sts hashed-file to parquet
# --------------------------------------------------------------------------------
write_files(
    df_hf_clm_sts,
    "hf_clm_sts.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# 5) FacetsOverrides (ODBCConnector) with 3 output pins, each separate query:
# --------------------------------------------------------------------------------
jdbc_url_facetsOv, jdbc_props_facetsOv = get_db_config(facets_secret_name)

# disall_cdmd_typ
extract_query_FacetsOv_disall_cdmd_typ = f"""
SELECT 
 CAST(Trim(DIS.CLCL_ID) as CHAR(12)) AS CLCL_ID,
 DIS.CDML_SEQ_NO,
 DIS.CDMD_TYPE
FROM {FacetsOwner}.CMC_CDMD_LI_DISALL DIS,
     tempdb..{DriverTable} TMP
WHERE DIS.CLCL_ID = TMP.CLM_ID

UNION

SELECT
 CAST(Trim(CDDD.CLCL_ID) as CHAR(12)) AS CLCL_ID,
 CDDD.CDDL_SEQ_NO as CDML_SEQ_NO,
 CDDD.CDDD_TYPE as CDMD_TYPE
FROM {FacetsOwner}.CMC_CDDD_DNLI_DIS CDDD,
     tempdb..{DriverTable} TMP2
WHERE CDDD.CLCL_ID = TMP2.CLM_ID
"""
df_FacetsOv_disall_cdmd_typ = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facetsOv)
    .options(**jdbc_props_facetsOv)
    .option("query", extract_query_FacetsOv_disall_cdmd_typ)
    .load()
)

# CLHP_for_Subtyp
extract_query_FacetsOv_CLHP_for_Subtyp = f"""
SELECT
 CAST(Trim(CLHP.CLCL_ID) as char(12)) AS CLCL_ID,
 CLHP.CLHP_FAC_TYPE,
 CLHP.CLHP_BILL_CLASS
FROM {FacetsOwner}.CMC_CLHP_HOSP CLHP,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID=CLHP.CLCL_ID
"""
df_FacetsOv_CLHP_for_Subtyp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facetsOv)
    .options(**jdbc_props_facetsOv)
    .option("query", extract_query_FacetsOv_CLHP_for_Subtyp)
    .load()
)

# clor
extract_query_FacetsOv_clor = f"""
SELECT 
 CAST(Trim(OVR.CLCL_ID) as CHAR(12)) AS CLCL_ID
FROM {FacetsOwner}.CMC_CLOR_CL_OVR OVR,
     tempdb..{DriverTable} TMP
WHERE OVR.CLCL_ID = TMP.CLM_ID
  AND OVR.CLOR_OR_ID = 'GD'
"""
df_FacetsOv_clor = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facetsOv)
    .options(**jdbc_props_facetsOv)
    .option("query", extract_query_FacetsOv_clor)
    .load()
)

# --------------------------------------------------------------------------------
# 6) dsalw_excd => DB2Connector (Database=IDS) with a query → pinned to "hf_clm_ln_cdmd_type"
# --------------------------------------------------------------------------------
jdbc_url_ids2, jdbc_props_ids2 = get_db_config(ids_secret_name)
extract_query_dsalw_excd = f"""
SELECT SRC_CD  AS SRC_CD, TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM = 'DISALLOW TYPE CATEGORY'
"""
df_dsalw_excd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids2)
    .options(**jdbc_props_ids2)
    .option("query", extract_query_dsalw_excd)
    .load()
)

# hf_clm_ln_cdmd_type => scenario C => we read or write? 
# The job shows InputPin from df_dsalw_excd, outputPin => "typ_lkup". This means it is written. 
# We'll treat it as scenario C => write to "hf_clm_ln_cdmd_type.parquet".
df_hf_clm_ln_cdmd_type = df_dsalw_excd.select("SRC_CD", "TRGT_CD")
write_files(
    df_hf_clm_ln_cdmd_type,
    "hf_clm_ln_cdmd_type.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Next, the stage does produce "typ_lkup" read by Transformer_127. In Spark, we can read it back for the next transform:
df_typ_lkup = spark.read.parquet("hf_clm_ln_cdmd_type.parquet").alias("typ_lkup")

# --------------------------------------------------------------------------------
# 7) Transformer_127 has two inputs:
#    PrimaryLink => "disall_cdmd_typ" (df_FacetsOv_disall_cdmd_typ)
#    LookupLink => "typ_lkup" (df_typ_lkup) on trim(disall_cdmd_typ.CDMD_TYPE)=typ_lkup.SRC_CD
#    OutputPin => "SavePROVCNTRCTtyp" with constraint: isNull(typ_lkup.SRC_CD)=false AND trim(typ_lkup.TRGT_CD)='PROVCNTRCT'
# --------------------------------------------------------------------------------

df_disall_cdmd_typ = df_FacetsOv_disall_cdmd_typ.alias("disall_cdmd_typ")

df_Transformer_127_joined = df_disall_cdmd_typ.join(
    df_typ_lkup,
    F.trim(df_disall_cdmd_typ["CDMD_TYPE"]) == F.col("typ_lkup.SRC_CD"),
    how="left"
)

# Constraint => isNull(typ_lkup.SRC_CD)=false AND trim(typ_lkup.TRGT_CD)='PROVCNTRCT'
df_SavePROVCNTRCTtyp = df_Transformer_127_joined.filter(
    (F.col("typ_lkup.SRC_CD").isNotNull()) &
    (F.trim(F.col("typ_lkup.TRGT_CD")) == F.lit("PROVCNTRCT"))
).select(
    F.col("disall_cdmd_typ.CLCL_ID").alias("CLCL_ID"),
    F.col("disall_cdmd_typ.CDML_SEQ_NO").alias("CDML_SEQ_NO")
)

# --------------------------------------------------------------------------------
# 8) Lookups stage => CHashedFileStage with 3 input pins => writes 3 files
#    The 3 inputs correspond to:
#      (a) SavePROVCNTRCTtyp => "hf_clm_ln_cdmd_provcntrct"
#      (b) CLHP_for_Subtyp => "Facility_fields"
#      (c) clor => "Pd_prov"
# --------------------------------------------------------------------------------

# (a) SavePROVCNTRCTtyp
write_files(
    df_SavePROVCNTRCTtyp,
    "hf_clm_ln_cdmd_provcntrct.parquet",  # scenario C => .parquet
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# (b) CLHP_for_Subtyp
write_files(
    df_FacetsOv_CLHP_for_Subtyp,
    "Facility_fields.parquet",  # also from CHashedFileStage => scenario C
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# (c) clor
write_files(
    df_FacetsOv_clor,
    "Pd_prov.parquet",  # scenario C
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# 9) clm_nasco_dup_bypass => CHashedFileStage => scenario C => read from parquet
# --------------------------------------------------------------------------------
df_clm_nasco_dup_bypass = spark.read.parquet("hf_clm_nasco_dup_bypass.parquet").alias("nasco_dup_lkup")

# --------------------------------------------------------------------------------
# 10) TMP_DRIVER => ODBCConnector => "SELECT c.CLCL_ID AS CLM_ID, c.CDML_SEQ_NO as CLM_LN_SEQ_NO, t.CLM_STS ...
# --------------------------------------------------------------------------------
jdbc_url_facets3, jdbc_props_facets3 = get_db_config(facets_secret_name)
extract_query_TMP_DRIVER = f"""
SELECT 
 c.CLCL_ID AS CLM_ID,
 c.CDML_SEQ_NO as CLM_LN_SEQ_NO,
 t.CLM_STS
FROM  tempdb..{DriverTable} t,
      {FacetsOwner}.CMC_CDML_CL_LINE c
WHERE c.CLCL_ID = t.CLM_ID
"""
df_TMP_DRIVER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets3)
    .options(**jdbc_props_facets3)
    .option("query", extract_query_TMP_DRIVER)
    .load()
).alias("ClmLn")

# --------------------------------------------------------------------------------
# 11) Trans1 => primary link=ClmLn, lookup link=nasco_dup_lkup
#      Output pins => "Original" (Constraint: isNull(nasco_dup_lkup.CLM_ID)=true)
#                   => "Reversal" (Constraint: trim(ClmLn.CLM_STS)='91')
# --------------------------------------------------------------------------------
df_join_Trans1 = df_TMP_DRIVER.join(
    df_clm_nasco_dup_bypass,
    df_TMP_DRIVER["ClmLn.CLM_ID"] == F.col("nasco_dup_lkup.CLM_ID"),
    how="left"
)

# "Original"
df_Original = df_join_Trans1.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.trim(F.col("ClmLn.CLM_ID")).alias("CLM_ID"),
    F.col("ClmLn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# "Reversal"
df_Reversal = df_join_Trans1.filter(
    F.trim(F.col("ClmLn.CLM_STS")) == F.lit("91")
).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    (F.trim(F.col("ClmLn.CLM_ID")) + F.lit("R")).alias("CLM_ID"),
    F.col("ClmLn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# 12) hf_clm_ln_rev_hold => CHashedFileStage => scenario C => we write the "Reversal" output
# --------------------------------------------------------------------------------
write_files(
    df_Reversal.select("SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "hf_clm_ln_rev_hold.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_clm_ln_rev_hold = spark.read.parquet("hf_clm_ln_rev_hold.parquet")

# --------------------------------------------------------------------------------
# 13) hf_clm_ln_orig_hold => CHashedFileStage => scenario C => we write "Original"
# --------------------------------------------------------------------------------
write_files(
    df_Original.select("SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "hf_clm_ln_orig_hold.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_clm_ln_orig_hold = spark.read.parquet("hf_clm_ln_orig_hold.parquet")

# --------------------------------------------------------------------------------
# 14) Collector => merges (hf_clm_ln_rev_hold, hf_clm_ln_orig_hold) => single output => "lnk_xfm_load"
#     "COLLECTALG=Round-Robin" => in Spark, we simply union them.
# --------------------------------------------------------------------------------
cols_collector = ["LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_ID","CLM_LN_SEQ_NO","SRC_SYS_CD_SK"]
df_collector = (
    df_hf_clm_ln_rev_hold.select(cols_collector)
    .unionByName(df_hf_clm_ln_orig_hold.select(cols_collector))
)

# --------------------------------------------------------------------------------
# 15) Xfm_Load => pass these columns to next stage => "W_BAL_IDS_CLM_LN"
# --------------------------------------------------------------------------------
df_Xfm_Load = df_collector.select(
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# 16) W_BAL_IDS_CLM_LN => CSeqFileStage => write .dat in "load" directory
# --------------------------------------------------------------------------------
final_w_bal_ids_clm_ln_path = f"{adls_path}/load/W_BAL_IDS_CLM_LN.dat"
write_files(
    df_Xfm_Load,
    final_w_bal_ids_clm_ln_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# End of job. No spark.stop().
# --------------------------------------------------------------------------------