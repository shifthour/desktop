# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2004, 2005, 2006, 2007, 2008, 2013 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Adds DRG Code to the data stream
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                  Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                             Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -------------------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Oliver Nielsen     05/07/2004-                     Originally Programmed
# MAGIC SAndrew             08/07/2004-    IDS 2.0    Task tracker issues #1163, #1125, #1279
# MAGIC Brent Leland       12/27/2004                     Trimmed claim ID on reversal transform.
# MAGIC R Tucker           12/30/2004 -                     Added Fclty_Clm_Adms_Src_CD to input/output and removed Adms_Src.
# MAGIC                           01/12/2004 -                    Changed mapping of CLHP_ADM_TYP from direct to conditional, per 
# MAGIC                                                                    CDMA reqirements.
# MAGIC Brent Leland      02/23/2005                      Tirmmed provider IDs
# MAGIC R Tucker           02/24/2005                      Put a NEG on Length of Stay (LOS) reversals, and Hosp Coverage Days 
# MAGIC                                                                     reversals...
# MAGIC                           03/30/2005                      Upper case FCLTY_CLM_ADMS_TYP_CD.
# MAGIC Brent Leland      05/20/2005                      Checked Discharge Hour for null value and defaulted it to zero.
# MAGIC BJ Luce             05/21/2005                      put same discharge hour logic in for reversal rows
# MAGIC Steph Goddard  03/17/2006                      Sequencer changes - added pkey, kept extract separate because of 
# MAGIC                                                                     grouper execution between extract and transform
# MAGIC  BJ Luce            03/20/2006                      add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. 
# MAGIC                                                                     If the claim is on the file, a row is not generated for it in IDS.  
# MAGIC                                                                     However, an R row will be build for it if the status if '91'
# MAGIC Steph Goddard  04/06/2006                      changed logic for admission source to look for nulls in souce code
# MAGIC Steph Goddard  04/17/2006                      changed logic for DRG - only look at generated DRG code for inpatient 
# MAGIC                                                                    claims, not outpatient.  Changed default to NA instead 
# MAGIC                                                                    of 0470 per Michelle Lang
# MAGIC Steph Goddard  05/04/2006                      changed facility claim discharge status move in reversal to Upcase - 
# MAGIC                                                                    other was already upcase.
# MAGIC Steph Goddard  05/08/2006                      Changed default logic for fclty_clm_adms_typ_cd
# MAGIC                                                                    changed default logic for Facility type (facility bill type code) to 'NA' if
# MAGIC                                                                    null or spaces
# MAGIC Sanderw           12/08/2006   Proj 1756    Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen    08/15/2007                      Added Snapshot extract for balancing                                                                  Steph Goddard          8/30/07
# MAGIC SAndrew           2008-02-10 DRG Phase II Added new file created in FctsClmFcltyExtr to supply value of NRMTV_DRG_CD.  
# MAGIC \(9)\(9)\(9)\(9)    Added to HASH.CLEAR file  hf_fclty_clm_nrmtv_drg_grouper                            Steph Goddard         02/21/2008
# MAGIC Bhoomi Dasari    2008-08-06   3567            Changed primary key process from hash file to DB2 table                                    Steph Goddard         08/15/2008
# MAGIC O. Nielsen          2008-07-15 Facets 4.5.1   Logic in Transfor for Bill Type Text was made to parse [2,1] since 
# MAGIC                                                                     Facets started padding the front of the values with a Zero                                   Steph Goddard         08/20/2008
# MAGIC                                                                    Changed both transform derivations to use stage variable Bill
# MAGIC                                                                    Type Text logic.  
# MAGIC                                                                     Removed constraint logic to look for each reversal status.  The
# MAGIC                                                                     hash file contains all claims that need reversals.
# MAGIC SAndrew            2008-02-10  3494 DRG     Added new grouper file that accounts for never events and merged with           Steph Goddard    02/27/2009
# MAGIC                                                                     existing grouper to create one grouper lookup reference file.
# MAGIC SAndrew            2010-09-29   4177 DRG    Changed the format and files coming out of FctsFcltyClmExtr which are              Steph Goddard    10/18/2010
# MAGIC                                                                     sent to Ingenix CDS application.  Removed Facets Call that built
# MAGIC                                                                     hf_clm_cdml_pos and removed hash file from HASH.CLEAR
# MAGIC Hugh Sisson     2013-10-28   TFS-3980      Corrected both DRG  lookup hashed files by adding 0 in Position column            Bhoomi Dasari     10/28/2013
# MAGIC Kalyan Neelam   2014-04-25 TFS 1113      Added HospCovDays <=999 check in svHospCovDays stage variable in            Bhoomi Dasari      4/28/2014   
# MAGIC                                                                    SETUP_CRF Transformer
# MAGIC Hugh Sisson      2015-12-02  5526             Changed layout of file sent to the DRG application                                               Bhoomi Dasari      2/3/2016 
# MAGIC 
# MAGIC Reddy Sanam   2020-08-11  US263442   Changed file names in the source sequential file stages GeneratedPOA,
# MAGIC                                                                  NormativePOA,FctsClmHospExtr
# MAGIC                                                                  Changed "FACETS" to SycSysCd in the transformer "SETUP_CRF"
# MAGIC                                                                  Chnaged substring "FACETS" to SrcSycCD in the sequential file 
# MAGIC                                                                  stage  "B_FCLTY_CLM" 
# MAGIC                                                                  Changed target file name to reflect LhoFcts

# MAGIC **built in FctsClmDriverBuild
# MAGIC ** bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
# MAGIC File created in FctsClmFcltyExtr job
# MAGIC This container is used in:
# MAGIC FctsClmFcltyTrns
# MAGIC NascoClmFcltyExtr
# MAGIC LhoFctsClmFcltyTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC File created from a grouper process by calling /ids/***/scripts/drg/DRG_Lookup.ksh
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','100')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','0')
SrcSysCd = get_widget_value('SrcSysCd','')

# ---------------------------
# Read Hashed File: clm_nasco_dup_bypass  (Scenario C)
# ---------------------------
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/clm_nasco_dup_bypass.parquet")

# ---------------------------
# Read Hashed File: hf_clm_fcts_reversals (Scenario C)
# ---------------------------
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# ---------------------------
# Read CSeqFileStage: FctsClmHospExtr  (from verified directory)
# ---------------------------
schema_FctsClmHospExtr = StructType([
    StructField("CLCL_ID", StringType(), False),
    StructField("MEME_CK", IntegerType(), False),
    StructField("CLHP_FAC_TYPE", StringType(), False),
    StructField("CLHP_BILL_CLASS", StringType(), False),
    StructField("CLHP_FREQUENCY", StringType(), False),
    StructField("CLHP_PRPR_ID_ADM", StringType(), False),
    StructField("CLHP_PRPR_ID_OTH1", StringType(), False),
    StructField("CLHP_PRPR_ID_OTH2", StringType(), False),
    StructField("CLHP_ADM_TYP", StringType(), False),
    StructField("CLHP_ADM_DT", TimestampType(), False),
    StructField("CLHP_DC_STAT", StringType(), False),
    StructField("CLHP_DC_DT", TimestampType(), False),
    StructField("CLHP_STAMENT_FR_DT", TimestampType(), False),
    StructField("CLHP_STAMENT_TO_DT", TimestampType(), False),
    StructField("CLHP_MED_REC_NO", StringType(), False),
    StructField("CLHP_IPCD_METH", StringType(), False),
    StructField("CLHP_EXT_PRICE_AMT", DecimalType(10,2), False),
    StructField("AGRG_ID", StringType(), False),
    StructField("CLHP_INPUT_AGRG_ID", StringType(), False),
    StructField("CLHP_ADM_HOUR_CD", StringType(), False),
    StructField("CLHP_ADM_SOURCE", StringType(), False),
    StructField("CLHP_DC_HOUR_CD", StringType(), False),
    StructField("CLHP_BIRTH_WGT", IntegerType(), False),
    StructField("CLHP_COVD_DAYS", IntegerType(), False),
    StructField("CLHP_LOCK_TOKEN", IntegerType(), False),
    StructField("ATXR_SOURCE_ID", TimestampType(), False),
    StructField("CLCL_LOS", IntegerType(), False),
    StructField("CLCL_RECD_DT", TimestampType(), False),
    StructField("CLCL_PAID_DT", TimestampType(), False),
    StructField("CLCL_CL_SUB_TYPE", StringType(), False)
])
df_FctsClmHospExtr = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_FctsClmHospExtr)
    .csv(f"{adls_path}/verified/LhoFctsClmFcltyExtr.LhoFctsClmFcltyExtr.uniq")
)

# ---------------------------
# Read CSeqFileStage: GeneratedPOA (from verified directory)
# ---------------------------
# This file path has #RunID#, so use runID
genpoa_path = f"{adls_path}/verified/LhoFctsFcltyClmDRGGen_{RunID}_results.dat"
schema_GeneratedPOA = StructType([
    StructField("HE_CLM_ID", StringType(), False),
    StructField("DIAG_CD_1", StringType(), False),
    StructField("DIAG_CD_2", StringType(), False),
    StructField("DIAG_CD_3", StringType(), False),
    StructField("DIAG_CD_4", StringType(), False),
    StructField("DIAG_CD_5", StringType(), False),
    StructField("DIAG_CD_6", StringType(), False),
    StructField("DIAG_CD_7", StringType(), False),
    StructField("DIAG_CD_8", StringType(), False),
    StructField("DIAG_CD_9", StringType(), False),
    StructField("DIAG_CD_10", StringType(), False),
    StructField("DIAG_CD_11", StringType(), False),
    StructField("DIAG_CD_12", StringType(), False),
    StructField("DIAG_CD_13", StringType(), False),
    StructField("DIAG_CD_14", StringType(), False),
    StructField("DIAG_CD_15", StringType(), False),
    StructField("DIAG_CD_16", StringType(), False),
    StructField("DIAG_CD_17", StringType(), False),
    StructField("DIAG_CD_18", StringType(), False),
    StructField("DIAG_CD_19", StringType(), False),
    StructField("DIAG_CD_20", StringType(), False),
    StructField("DIAG_CD_21", StringType(), False),
    StructField("DIAG_CD_22", StringType(), False),
    StructField("DIAG_CD_23", StringType(), False),
    StructField("DIAG_CD_24", StringType(), False),
    StructField("DIAG_CD_25", StringType(), False),
    StructField("DX_TYPE_1", StringType(), False),
    StructField("DX_TYPE_2", StringType(), False),
    StructField("DX_TYPE_3", StringType(), False),
    StructField("DX_TYPE_4", StringType(), False),
    StructField("DX_TYPE_5", StringType(), False),
    StructField("DX_TYPE_6", StringType(), False),
    StructField("DX_TYPE_7", StringType(), False),
    StructField("DX_TYPE_8", StringType(), False),
    StructField("DX_TYPE_9", StringType(), False),
    StructField("DX_TYPE_10", StringType(), False),
    StructField("DX_TYPE_11", StringType(), False),
    StructField("DX_TYPE_12", StringType(), False),
    StructField("DX_TYPE_13", StringType(), False),
    StructField("DX_TYPE_14", StringType(), False),
    StructField("DX_TYPE_15", StringType(), False),
    StructField("DX_TYPE_16", StringType(), False),
    StructField("DX_TYPE_17", StringType(), False),
    StructField("DX_TYPE_18", StringType(), False),
    StructField("DX_TYPE_19", StringType(), False),
    StructField("DX_TYPE_20", StringType(), False),
    StructField("DX_TYPE_21", StringType(), False),
    StructField("DX_TYPE_22", StringType(), False),
    StructField("DX_TYPE_23", StringType(), False),
    StructField("DX_TYPE_24", StringType(), False),
    StructField("DX_TYPE_25", StringType(), False),
    StructField("PROC_CD_1", StringType(), False),
    StructField("PROC_CD_2", StringType(), False),
    StructField("PROC_CD_3", StringType(), False),
    StructField("PROC_CD_4", StringType(), False),
    StructField("PROC_CD_5", StringType(), False),
    StructField("PROC_CD_6", StringType(), False),
    StructField("PROC_CD_7", StringType(), False),
    StructField("PROC_CD_8", StringType(), False),
    StructField("PROC_CD_9", StringType(), False),
    StructField("PROC_CD_10", StringType(), False),
    StructField("PROC_CD_11", StringType(), False),
    StructField("PROC_CD_12", StringType(), False),
    StructField("PROC_CD_13", StringType(), False),
    StructField("PROC_CD_14", StringType(), False),
    StructField("PROC_CD_15", StringType(), False),
    StructField("PROC_CD_16", StringType(), False),
    StructField("PROC_CD_17", StringType(), False),
    StructField("PROC_CD_18", StringType(), False),
    StructField("PROC_CD_19", StringType(), False),
    StructField("PROC_CD_20", StringType(), False),
    StructField("PROC_CD_21", StringType(), False),
    StructField("PROC_CD_22", StringType(), False),
    StructField("PROC_CD_23", StringType(), False),
    StructField("PROC_CD_24", StringType(), False),
    StructField("PROC_CD_25", StringType(), False),
    StructField("PX_TYPE_1", StringType(), False),
    StructField("PX_TYPE_2", StringType(), False),
    StructField("PX_TYPE_3", StringType(), False),
    StructField("PX_TYPE_4", StringType(), False),
    StructField("PX_TYPE_5", StringType(), False),
    StructField("PX_TYPE_6", StringType(), False),
    StructField("PX_TYPE_7", StringType(), False),
    StructField("PX_TYPE_8", StringType(), False),
    StructField("PX_TYPE_9", StringType(), False),
    StructField("PX_TYPE_10", StringType(), False),
    StructField("PX_TYPE_11", StringType(), False),
    StructField("PX_TYPE_12", StringType(), False),
    StructField("PX_TYPE_13", StringType(), False),
    StructField("PX_TYPE_14", StringType(), False),
    StructField("PX_TYPE_15", StringType(), False),
    StructField("PX_TYPE_16", StringType(), False),
    StructField("PX_TYPE_17", StringType(), False),
    StructField("PX_TYPE_18", StringType(), False),
    StructField("PX_TYPE_19", StringType(), False),
    StructField("PX_TYPE_20", StringType(), False),
    StructField("PX_TYPE_21", StringType(), False),
    StructField("PX_TYPE_22", StringType(), False),
    StructField("PX_TYPE_23", StringType(), False),
    StructField("PX_TYPE_24", StringType(), False),
    StructField("PX_TYPE_25", StringType(), False),
    StructField("AGE", StringType(), False),
    StructField("SEX", StringType(), False),
    StructField("DISP", StringType(), False),
    StructField("DRG", StringType(), False),
    StructField("MDC", StringType(), False),
    StructField("VERS", StringType(), False),
    StructField("FILL1", StringType(), False),
    StructField("OP", StringType(), False),
    StructField("RTN_CODE", StringType(), False),
    StructField("NO_DX", StringType(), False),
    StructField("NO_OP", StringType(), False),
    StructField("ADAYS_A", StringType(), False),
    StructField("ADAYS_D", StringType(), False),
    StructField("BWGT", StringType(), False),
    StructField("GTYPE", StringType(), False),
    StructField("CLMD_POA_IND_1", StringType(), False),
    StructField("CLMD_POA_IND_2", StringType(), False),
    StructField("CLMD_POA_IND_3", StringType(), False),
    StructField("CLMD_POA_IND_4", StringType(), False),
    StructField("CLMD_POA_IND_5", StringType(), False),
    StructField("CLMD_POA_IND_6", StringType(), False),
    StructField("CLMD_POA_IND_7", StringType(), False),
    StructField("CLMD_POA_IND_8", StringType(), False),
    StructField("CLMD_POA_IND_9", StringType(), False),
    StructField("CLMD_POA_IND_10", StringType(), False),
    StructField("CLMD_POA_IND_11", StringType(), False),
    StructField("CLMD_POA_IND_12", StringType(), False),
    StructField("CLMD_POA_IND_13", StringType(), False),
    StructField("CLMD_POA_IND_14", StringType(), False),
    StructField("CLMD_POA_IND_15", StringType(), False),
    StructField("CLMD_POA_IND_16", StringType(), False),
    StructField("CLMD_POA_IND_17", StringType(), False),
    StructField("CLMD_POA_IND_18", StringType(), False),
    StructField("CLMD_POA_IND_19", StringType(), False),
    StructField("CLMD_POA_IND_20", StringType(), False),
    StructField("CLMD_POA_IND_21", StringType(), False),
    StructField("CLMD_POA_IND_22", StringType(), False),
    StructField("CLMD_POA_IND_23", StringType(), False),
    StructField("CLMD_POA_IND_24", StringType(), False),
    StructField("CLMD_POA_IND_25", StringType(), False),
    StructField("CODE_CLASS_IND", StringType(), False),
    StructField("PTNT_TYP", StringType(), False),
    StructField("CLS_TYP", StringType(), False),
])
df_GeneratedPOA = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_GeneratedPOA)
    .csv(genpoa_path)
)

# ---------------------------
# Read CSeqFileStage: NormativePOA (from verified directory)
# ---------------------------
normpoa_path = f"{adls_path}/verified/LhoFctsFcltyClmDRGNorm_{RunID}_results.dat"
schema_NormativePOA = StructType([
    StructField("HE_CLM_ID", StringType(), False),
    StructField("DIAG_CD_1", StringType(), False),
    StructField("DIAG_CD_2", StringType(), False),
    StructField("DIAG_CD_3", StringType(), False),
    StructField("DIAG_CD_4", StringType(), False),
    StructField("DIAG_CD_5", StringType(), False),
    StructField("DIAG_CD_6", StringType(), False),
    StructField("DIAG_CD_7", StringType(), False),
    StructField("DIAG_CD_8", StringType(), False),
    StructField("DIAG_CD_9", StringType(), False),
    StructField("DIAG_CD_10", StringType(), False),
    StructField("DIAG_CD_11", StringType(), False),
    StructField("DIAG_CD_12", StringType(), False),
    StructField("DIAG_CD_13", StringType(), False),
    StructField("DIAG_CD_14", StringType(), False),
    StructField("DIAG_CD_15", StringType(), False),
    StructField("DIAG_CD_16", StringType(), False),
    StructField("DIAG_CD_17", StringType(), False),
    StructField("DIAG_CD_18", StringType(), False),
    StructField("DIAG_CD_19", StringType(), False),
    StructField("DIAG_CD_20", StringType(), False),
    StructField("DIAG_CD_21", StringType(), False),
    StructField("DIAG_CD_22", StringType(), False),
    StructField("DIAG_CD_23", StringType(), False),
    StructField("DIAG_CD_24", StringType(), False),
    StructField("DIAG_CD_25", StringType(), False),
    StructField("DX_TYPE_1", StringType(), False),
    StructField("DX_TYPE_2", StringType(), False),
    StructField("DX_TYPE_3", StringType(), False),
    StructField("DX_TYPE_4", StringType(), False),
    StructField("DX_TYPE_5", StringType(), False),
    StructField("DX_TYPE_6", StringType(), False),
    StructField("DX_TYPE_7", StringType(), False),
    StructField("DX_TYPE_8", StringType(), False),
    StructField("DX_TYPE_9", StringType(), False),
    StructField("DX_TYPE_10", StringType(), False),
    StructField("DX_TYPE_11", StringType(), False),
    StructField("DX_TYPE_12", StringType(), False),
    StructField("DX_TYPE_13", StringType(), False),
    StructField("DX_TYPE_14", StringType(), False),
    StructField("DX_TYPE_15", StringType(), False),
    StructField("DX_TYPE_16", StringType(), False),
    StructField("DX_TYPE_17", StringType(), False),
    StructField("DX_TYPE_18", StringType(), False),
    StructField("DX_TYPE_19", StringType(), False),
    StructField("DX_TYPE_20", StringType(), False),
    StructField("DX_TYPE_21", StringType(), False),
    StructField("DX_TYPE_22", StringType(), False),
    StructField("DX_TYPE_23", StringType(), False),
    StructField("DX_TYPE_24", StringType(), False),
    StructField("DX_TYPE_25", StringType(), False),
    StructField("PROC_CD_1", StringType(), False),
    StructField("PROC_CD_2", StringType(), False),
    StructField("PROC_CD_3", StringType(), False),
    StructField("PROC_CD_4", StringType(), False),
    StructField("PROC_CD_5", StringType(), False),
    StructField("PROC_CD_6", StringType(), False),
    StructField("PROC_CD_7", StringType(), False),
    StructField("PROC_CD_8", StringType(), False),
    StructField("PROC_CD_9", StringType(), False),
    StructField("PROC_CD_10", StringType(), False),
    StructField("PROC_CD_11", StringType(), False),
    StructField("PROC_CD_12", StringType(), False),
    StructField("PROC_CD_13", StringType(), False),
    StructField("PROC_CD_14", StringType(), False),
    StructField("PROC_CD_15", StringType(), False),
    StructField("PROC_CD_16", StringType(), False),
    StructField("PROC_CD_17", StringType(), False),
    StructField("PROC_CD_18", StringType(), False),
    StructField("PROC_CD_19", StringType(), False),
    StructField("PROC_CD_20", StringType(), False),
    StructField("PROC_CD_21", StringType(), False),
    StructField("PROC_CD_22", StringType(), False),
    StructField("PROC_CD_23", StringType(), False),
    StructField("PROC_CD_24", StringType(), False),
    StructField("PROC_CD_25", StringType(), False),
    StructField("PX_TYPE_1", StringType(), False),
    StructField("PX_TYPE_2", StringType(), False),
    StructField("PX_TYPE_3", StringType(), False),
    StructField("PX_TYPE_4", StringType(), False),
    StructField("PX_TYPE_5", StringType(), False),
    StructField("PX_TYPE_6", StringType(), False),
    StructField("PX_TYPE_7", StringType(), False),
    StructField("PX_TYPE_8", StringType(), False),
    StructField("PX_TYPE_9", StringType(), False),
    StructField("PX_TYPE_10", StringType(), False),
    StructField("PX_TYPE_11", StringType(), False),
    StructField("PX_TYPE_12", StringType(), False),
    StructField("PX_TYPE_13", StringType(), False),
    StructField("PX_TYPE_14", StringType(), False),
    StructField("PX_TYPE_15", StringType(), False),
    StructField("PX_TYPE_16", StringType(), False),
    StructField("PX_TYPE_17", StringType(), False),
    StructField("PX_TYPE_18", StringType(), False),
    StructField("PX_TYPE_19", StringType(), False),
    StructField("PX_TYPE_20", StringType(), False),
    StructField("PX_TYPE_21", StringType(), False),
    StructField("PX_TYPE_22", StringType(), False),
    StructField("PX_TYPE_23", StringType(), False),
    StructField("PX_TYPE_24", StringType(), False),
    StructField("PX_TYPE_25", StringType(), False),
    StructField("AGE", StringType(), False),
    StructField("SEX", StringType(), False),
    StructField("DISP", StringType(), False),
    StructField("DRG", StringType(), False),
    StructField("MDC", StringType(), False),
    StructField("VERS", StringType(), False),
    StructField("FILL1", StringType(), False),
    StructField("OP", StringType(), False),
    StructField("RTN_CODE", StringType(), False),
    StructField("NO_DX", StringType(), False),
    StructField("NO_OP", StringType(), False),
    StructField("ADAYS_A", StringType(), False),
    StructField("ADAYS_D", StringType(), False),
    StructField("BWGT", StringType(), False),
    StructField("GTYPE", StringType(), False),
    StructField("CLMD_POA_IND_1", StringType(), False),
    StructField("CLMD_POA_IND_2", StringType(), False),
    StructField("CLMD_POA_IND_3", StringType(), False),
    StructField("CLMD_POA_IND_4", StringType(), False),
    StructField("CLMD_POA_IND_5", StringType(), False),
    StructField("CLMD_POA_IND_6", StringType(), False),
    StructField("CLMD_POA_IND_7", StringType(), False),
    StructField("CLMD_POA_IND_8", StringType(), False),
    StructField("CLMD_POA_IND_9", StringType(), False),
    StructField("CLMD_POA_IND_10", StringType(), False),
    StructField("CLMD_POA_IND_11", StringType(), False),
    StructField("CLMD_POA_IND_12", StringType(), False),
    StructField("CLMD_POA_IND_13", StringType(), False),
    StructField("CLMD_POA_IND_14", StringType(), False),
    StructField("CLMD_POA_IND_15", StringType(), False),
    StructField("CLMD_POA_IND_16", StringType(), False),
    StructField("CLMD_POA_IND_17", StringType(), False),
    StructField("CLMD_POA_IND_18", StringType(), False),
    StructField("CLMD_POA_IND_19", StringType(), False),
    StructField("CLMD_POA_IND_20", StringType(), False),
    StructField("CLMD_POA_IND_21", StringType(), False),
    StructField("CLMD_POA_IND_22", StringType(), False),
    StructField("CLMD_POA_IND_23", StringType(), False),
    StructField("CLMD_POA_IND_24", StringType(), False),
    StructField("CLMD_POA_IND_25", StringType(), False),
    StructField("CODE_CLASS_IND", StringType(), False),
    StructField("PTNT_TYP", StringType(), False),
    StructField("CLS_TYP", StringType(), False),
])
df_NormativePOA = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_NormativePOA)
    .csv(normpoa_path)
)

# ---------------------------
# hf_fclty_clm_gen_drg_grouper (Scenario A: GeneratedPOA -> hashed -> next)
#    Deduplicate on key = HE_CLM_ID
# ---------------------------
df_grouper_out = dedup_sort(
    df_GeneratedPOA,
    partition_cols=["HE_CLM_ID"],
    sort_cols=[]
)

# ---------------------------
# hf_fclty_clm_nrmtv_drg_grouper (Scenario A: NormativePOA -> hashed -> next)
#    Deduplicate on key = HE_CLM_ID
# ---------------------------
df_normative_drg_out = dedup_sort(
    df_NormativePOA,
    partition_cols=["HE_CLM_ID"],
    sort_cols=[]
)

# ---------------------------
# SETUP_CRF  (CTransformerStage)
# Primary link: df_FctsClmHospExtr as ClmHospIN
# Additional left joins:
# grouper_out => df_grouper_out
# fcts_reversals => df_hf_clm_fcts_reversals
# nasco_dup_lkup => df_clm_nasco_dup_bypass
# normative_drg => df_normative_drg_out
# ---------------------------
df_setup_crf_pre = (
    df_FctsClmHospExtr.alias("ClmHospIN")
    .join(df_grouper_out.alias("grouper_out"), F.col("ClmHospIN.CLCL_ID") == F.col("grouper_out.HE_CLM_ID"), "left")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), F.col("ClmHospIN.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), F.col("ClmHospIN.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
    .join(df_normative_drg_out.alias("normative_drg"), F.col("ClmHospIN.CLCL_ID") == F.col("normative_drg.HE_CLM_ID"), "left")
)

# Build all Stage Variables in a single select, then we will produce two outputs.
df_sv = df_setup_crf_pre.select(
    F.col("ClmHospIN.*"),
    F.col("grouper_out.*"),
    F.col("fcts_reversals.*"),
    F.col("nasco_dup_lkup.*"),
    F.col("normative_drg.*"),
    # Stage variable expansions:
    # (We replicate DS logic using Spark expressions)
    F.date_format(F.col("ClmHospIN.CLCL_RECD_DT"), "yyyy-MM-dd").alias("svClaimReceivedDate"),
    F.date_format(F.col("ClmHospIN.CLCL_PAID_DT"), "yyyy-MM-dd").alias("svClaimPaidDate"),
    F.date_format(F.col("ClmHospIN.CLHP_DC_DT"), "yyyy-MM-dd").alias("svDischargedDate"),
    F.date_format(F.col("ClmHospIN.CLHP_STAMENT_FR_DT"), "yyyy-MM-dd").alias("svStatementFromDate"),
    F.date_format(F.col("ClmHospIN.CLHP_STAMENT_TO_DT"), "yyyy-MM-dd").alias("svStatementToDate"),
    F.substring(F.col("ClmHospIN.CLHP_FAC_TYPE"),2,1).alias("svFacType")
).withColumn(
    "svBillTypeText",
    F.when(
      F.col("svFacType").isNotNull() &
      (F.length(F.concat_ws("", F.col("svFacType"), F.trim(F.col("ClmHospIN.CLHP_BILL_CLASS")), F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))) > 0),
      F.concat(F.col("svFacType"), F.trim(F.col("ClmHospIN.CLHP_BILL_CLASS")), F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))
    ).otherwise("NA")
).withColumn(
    "svSubType",
    F.when(
      (F.col("ClmHospIN.CLCL_CL_SUB_TYPE").isNull()) | (F.trim(F.col("ClmHospIN.CLCL_CL_SUB_TYPE")) == ""),
      F.lit(" ")
    ).otherwise(F.col("ClmHospIN.CLCL_CL_SUB_TYPE"))
).withColumn(
    "svSubDrg1",
    F.when(
      (F.col("ClmHospIN.CLHP_INPUT_AGRG_ID").substr(F.lit(1),F.lit(1)).rlike("^[0-9]$")),
      F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))) == 4,
             F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))
            ).when(F.length(F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))) == 3,
                   F.concat(F.lit("0"), F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID")))
            ).when(F.length(F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))) == 2,
                   F.lit("00") + F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))
            ).when(F.length(F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))) == 1,
                   F.lit("000") + F.trim(F.col("ClmHospIN.CLHP_INPUT_AGRG_ID"))
            ).otherwise("NA")
    ).otherwise(
      F.when(
        F.col("ClmHospIN.CLHP_INPUT_AGRG_ID").substr(F.lit(1),F.lit(1)) == "D",
        F.concat(F.lit("0"), F.col("ClmHospIN.CLHP_INPUT_AGRG_ID").substr(F.lit(2),F.lit(3)))
      ).otherwise("0000")
    )
).withColumn(
    "svSubDrg2",
    F.when(
      (F.length(F.trim(F.col("svSubDrg1"))) == 0) | (F.trim(F.col("svSubDrg1")) == "0000"),
      F.lit(None)
    ).otherwise(F.rpad(F.right(F.concat(F.lit("000"), F.trim(F.col("svSubDrg1"))),4),4,' '))
).withColumn(
    "ClmSubtyp",
    F.when(
      (F.col("ClmHospIN.CLCL_CL_SUB_TYPE") == "M") | (F.col("ClmHospIN.CLCL_CL_SUB_TYPE") == "D"),
      F.lit("P")
    ).when(
      (F.col("ClmHospIN.CLCL_CL_SUB_TYPE") == "H"),
      F.when(
        (F.col("svFacType") > "6"),
        F.lit("O")
      ).otherwise(
        F.when(
          (F.col("svFacType") < "7") &
          ((F.col("ClmHospIN.CLHP_BILL_CLASS") == "3") | (F.col("ClmHospIN.CLHP_BILL_CLASS") == "4")),
          F.lit("O")
        ).otherwise(F.lit("I"))
      )
    ).otherwise(F.lit("UNK"))
).withColumn(
    "ClmId", F.trim(F.col("ClmHospIN.CLCL_ID"))
).withColumn(
    "svGeneratedDRGCd",
    F.when(
      F.col("ClmSubtyp") == "O",
      F.lit("NA")
    ).when(
      F.length(F.trim(F.col("ClmHospIN.AGRG_ID"))) > 1,
      F.when(
        ((F.col("svClaimReceivedDate") < "2007-10-01") | (F.col("svClaimReceivedDate") > "2007-10-31")) &
        ((F.col("svClaimPaidDate") < "2007-10-01") | (F.col("svClaimPaidDate") > "2007-10-31")),
        F.rpad(F.right(F.concat(F.lit("000"),F.trim(F.col("ClmHospIN.AGRG_ID"))),4),4," ")
      ).otherwise(
        F.when(
          (F.col("svClaimReceivedDate") >= "2007-10-01") & (F.col("svClaimReceivedDate") <= "2007-10-31") &
          (F.col("svClaimPaidDate") >= "2007-10-01") & (F.col("svClaimPaidDate") <= "2007-10-31"),
          F.when(
            F.col("svDischargedDate") == "1753-01-01",
            F.when(
              F.col("svStatementToDate") == "1753-01-01",
              F.lit("UNK")
            ).otherwise(
              F.when(
                (F.col("svStatementToDate") < "2007-10-01") | (F.col("svStatementToDate") > "2007-10-31"),
                F.rpad(F.right(F.concat(F.lit("000"),F.trim(F.col("ClmHospIN.AGRG_ID"))),4),4," ")
              ).otherwise(
                F.when(
                  F.trim(F.col("grouper_out.DRG")).isNull(),
                  F.lit("UNK")
                ).otherwise(F.rpad(F.right(F.concat(F.lit("000"),F.col("grouper_out.DRG")),4),4," "))
              )
            )
          ).otherwise(
            F.when(
              (F.col("svDischargedDate") < "2007-10-01") | (F.col("svDischargedDate") > "2007-10-31"),
              F.rpad(F.right(F.concat(F.lit("000"),F.trim(F.col("ClmHospIN.AGRG_ID"))),4),4," ")
            ).otherwise(
              F.when(
                F.trim(F.col("grouper_out.DRG")).isNull(),
                F.lit("UNK")
              ).otherwise(F.rpad(F.right(F.concat(F.lit("000"),F.col("grouper_out.DRG")),4),4," "))
            )
          )
        ).otherwise(
          F.when(
            F.trim(F.col("grouper_out.DRG")).isNull(),
            F.lit("UNK")
          ).otherwise(F.rpad(F.right(F.concat(F.lit("000"),F.col("grouper_out.DRG")),4),4," "))
        )
      )
    ).otherwise(
      F.when(
        F.trim(F.col("grouper_out.DRG")).isNull(),
        F.lit("UNK")
      ).otherwise(F.rpad(F.right(F.concat(F.lit("000"),F.col("grouper_out.DRG")),4),4," "))
    )
).withColumn(
    "svGeneratedDRGInd",
    F.when(
      F.col("ClmSubtyp") == "O",
      F.lit("N")
    ).otherwise(
      F.when(
        F.length(F.trim(F.col("ClmHospIN.AGRG_ID"))) > 1,
        F.when(
          ((F.col("svClaimReceivedDate") < "2007-10-01") | (F.col("svClaimReceivedDate") > "2007-10-31")) &
          ((F.col("svClaimPaidDate") < "2007-10-01") | (F.col("svClaimPaidDate") > "2007-10-31")),
          F.lit("N")
        ).otherwise(
          F.when(
            (F.col("svClaimReceivedDate") >= "2007-10-01") & (F.col("svClaimReceivedDate") <= "2007-10-31") &
            (F.col("svClaimPaidDate") >= "2007-10-01") & (F.col("svClaimPaidDate") <= "2007-10-31"),
            F.when(
              F.col("svDischargedDate") == "1753-01-01",
              F.when(
                F.col("svStatementToDate") == "1753-01-01",
                F.lit("U")
              ).otherwise(
                F.when(
                  (F.col("svStatementToDate") < "2007-10-01") | (F.col("svStatementToDate") > "2007-10-31"),
                  F.lit("N")
                ).otherwise(
                  F.when(
                    F.trim(F.col("grouper_out.DRG")).isNull(),
                    F.lit("U")
                  ).otherwise(F.lit("Y"))
                )
              )
            ).otherwise(
              F.when(
                (F.col("svDischargedDate") < "2007-10-01") | (F.col("svDischargedDate") > "2007-10-31"),
                F.lit("N")
              ).otherwise(
                F.when(
                  F.trim(F.col("grouper_out.DRG")).isNull(),
                  F.lit("U")
                ).otherwise(F.lit("Y"))
              )
            )
          ).otherwise(F.lit("N"))
        )
      ).otherwise(
        F.when(
          F.trim(F.col("grouper_out.DRG")).isNull(),
          F.lit("U")
        ).otherwise(F.lit("Y"))
      )
    )
).withColumn(
    "svNormativeDRGCd",
    F.when(
      F.col("ClmSubtyp") == "O",
      F.lit("NA")
    ).otherwise(
      F.when(
        F.trim(F.col("normative_drg.DRG")).isNull() | (F.length(F.trim(F.col("normative_drg.DRG"))) == 0),
        F.lit("0999")
      ).otherwise(
        F.rpad(F.right(F.concat(F.lit("000"),F.col("normative_drg.DRG")),4),4," ")
      )
    )
).withColumn(
    "svDrgMethodCode",
    F.when(
      (F.col("svDischargedDate").isin("1753-01-01","NA","UNK")) | (F.length(F.trim(F.col("svDischargedDate"))) == 0),
      F.when(
        (F.col("svStatementToDate").isin("1753-01-01","NA","UNK")) | (F.length(F.trim(F.col("svStatementToDate"))) == 0),
        F.lit("MS")
      ).otherwise(
        F.when(
          (F.col("svStatementToDate") > "1800-01-01") & (F.col("svStatementToDate") < "2007-10-01"),
          F.lit("CMS")
        ).otherwise(
          F.when(
            (F.col("svStatementToDate") >= "2007-10-01"),
            F.lit("MS")
          ).otherwise(F.lit("UNK"))
        )
      )
    ).otherwise(
      F.when(
        (F.col("svDischargedDate") >= "1800-01-01") & (F.col("svDischargedDate") < "2007-10-01"),
        F.lit("CMS")
      ).otherwise(
        F.when(
          (F.col("svDischargedDate") >= "2007-10-01"),
          F.lit("MS")
        ).otherwise(F.lit("UNK"))
      )
    )
).withColumn(
    "svClmAdmsTyp",
    F.when(
      (F.col("svSubType") == "H") &
      (F.col("svFacType").rlike("^[0-9]$")) &
      (F.col("svFacType") < "7"),
      F.when(
        (F.col("ClmHospIN.CLHP_BILL_CLASS").isin("3","4")),
        F.lit("NA")
      ).otherwise(
        F.when(
          F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_TYP"))) == 0,
          F.lit("UNK")
        ).otherwise(F.col("ClmHospIN.CLHP_ADM_TYP"))
      )
    ).otherwise(F.lit("NA"))
).withColumn(
    "svBillCls",
    F.when(
      F.col("svFacType") == "7",
      F.concat(F.lit("7"), F.col("ClmHospIN.CLHP_BILL_CLASS"))
    ).when(
      F.col("ClmHospIN.CLHP_FAC_TYPE") == "8",
      F.concat(F.lit("8"), F.col("ClmHospIN.CLHP_BILL_CLASS"))
    ).otherwise(
      F.concat(F.lit("0"), F.col("ClmHospIN.CLHP_BILL_CLASS"))
    )
).withColumn(
    "svHospBegDate",
    F.date_format(F.col("ClmHospIN.CLHP_STAMENT_FR_DT"), "yyyy-MM-dd")
).withColumn(
    "svHospEndDate",
    F.date_format(F.col("ClmHospIN.CLHP_STAMENT_TO_DT"), "yyyy-MM-dd")
).withColumn(
    "svAdmsDate",
    F.date_format(F.col("ClmHospIN.CLHP_ADM_DT"), "yyyy-MM-dd")
).withColumn(
    "svAdmsSrc",
    F.when(
      (F.trim(F.col("ClmHospIN.CLHP_ADM_TYP")) == "4") & (F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE"))) > 0),
      F.concat(F.lit("4"), F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE")))
    ).otherwise(
      F.when(
        F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_TYP"))) == 0,
        F.lit("NA")
      ).otherwise(
        F.when(
          F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE"))) == 0,
          F.lit("UNK")
        ).otherwise(F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE")))
      )
    )
).withColumn(
    "svHospCovDays",
    F.when(
      F.col("ClmHospIN.CLHP_COVD_DAYS") <= 999,
      F.col("ClmHospIN.CLHP_COVD_DAYS")
    ).otherwise(F.lit(999))
)

# ---------------------------
# Now produce two output links:
# 1) ClmHospOUT:   where nasco_dup_lkup.CLM_ID is null
# 2) reversals:     where fcts_reversals.CLCL_ID is not null
# ---------------------------
df_ClmHospOUT = df_sv.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_reversals = df_sv.filter(F.col("fcts_reversals.CLCL_ID").isNotNull())

# Build the columns exactly in order for ClmHospOUT
df_ClmHospOUT_out = df_ClmHospOUT.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrentDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"),F.lit(";"),F.col("ClmId")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("ClmId"),18," ").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.rpad(F.col("svAdmsSrc"),2," ").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.rpad(F.col("svClmAdmsTyp"),10," ").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.rpad(F.col("svBillCls"),10," ").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.rpad(
      F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))==0,F.lit("UNK"))
       .otherwise(F.col("ClmHospIN.CLHP_FREQUENCY")),
      10," "
    ).alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.rpad(
      F.when(F.col("ClmHospIN.CLHP_DC_STAT").substr(F.lit(1),F.lit(1))==" ",F.lit(" "))
       .otherwise(F.upper(F.col("ClmHospIN.CLHP_DC_STAT"))),
      10," "
    ).alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.rpad(F.col("ClmSubtyp"),10," ").alias("FCLTY_CLM_SUBTYP_CD"),
    F.rpad(F.col("ClmHospIN.CLHP_IPCD_METH"),10," ").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.rpad(
      F.when(F.length(F.col("svFacType"))==0,F.lit("NA"))
       .otherwise(F.col("svFacType")),
      10," "
    ).alias("FCLTY_TYP_CD"),
    F.rpad(F.col("svGeneratedDRGInd"),1," ").alias("IDS_GNRT_DRG_IN"),
    F.rpad(F.col("svAdmsDate"),10," ").alias("ADMS_DT"),
    F.rpad(F.col("svStatementFromDate"),10," ").alias("BILL_STMNT_BEG_DT"),
    F.rpad(F.col("svStatementToDate"),10," ").alias("BILL_STMNT_END_DT"),
    F.rpad(F.col("svDischargedDate"),10," ").alias("DSCHG_DT"),
    F.rpad(F.col("svHospBegDate"),10," ").alias("HOSP_BEG_DT"),
    F.rpad(F.col("svHospEndDate"),10," ").alias("HOSP_END_DT"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")))<1,F.lit(0))
     .otherwise(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")).alias("ADMS_HR"),
    F.when(
      (F.col("ClmHospIN.CLHP_DC_HOUR_CD").isNull())|(F.trim(F.col("ClmHospIN.CLHP_DC_HOUR_CD"))==""),
      F.lit(0)
    ).otherwise(
      F.col("ClmHospIN.CLHP_DC_HOUR_CD").cast("int")
    ).alias("DSCHG_HR"),
    F.col("svHospCovDays").alias("HOSP_COV_DAYS"),
    F.col("ClmHospIN.CLCL_LOS").alias("LOS_DAYS"),
    F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_ADM")).alias("ADM_PHYS_PROV_ID"),
    F.rpad(F.col("svBillTypeText"),3," ").alias("FCLTY_BILL_TYP_TX"),
    F.col("svGeneratedDRGCd").alias("GNRT_DRG_CD"),
    F.col("ClmHospIN.CLHP_MED_REC_NO").alias("MED_RCRD_NO"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1"))).alias("OTHER_PROV_ID_1"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2"))).alias("OTHER_PROV_ID_2"),
    F.when(F.col("svSubDrg2").isNull(),F.lit("    ")).otherwise(F.col("svSubDrg2")).alias("SUBMT_DRG_CD"),
    F.rpad(F.col("svNormativeDRGCd"),4," ").alias("NRMTV_DRG_CD"),
    F.col("svDrgMethodCode").alias("DRG_METHOD_CD")
)

df_reversals_out = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrentDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"),F.lit(";"),F.col("ClmId"),F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.rpad(F.concat(F.col("ClmId"),F.lit("R")),18," ").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.rpad(F.col("svAdmsSrc"),2," ").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.rpad(F.col("svClmAdmsTyp"),10," ").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.rpad(F.col("svBillCls"),10," ").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.rpad(
      F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))<1,F.lit("NA"))
       .otherwise(F.col("ClmHospIN.CLHP_FREQUENCY")),
      10," "
    ).alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.rpad(
      F.when(F.col("ClmHospIN.CLHP_DC_STAT").substr(F.lit(1),F.lit(1))==" ",F.lit(" "))
       .otherwise(F.upper(F.col("ClmHospIN.CLHP_DC_STAT"))),
      10," "
    ).alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.rpad(F.col("ClmSubtyp"),10," ").alias("FCLTY_CLM_SUBTYP_CD"),
    F.rpad(F.col("ClmHospIN.CLHP_IPCD_METH"),10," ").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.rpad(
      F.when(F.length(F.col("svFacType"))==0,F.lit("NA"))
       .otherwise(F.col("svFacType")),
      10," "
    ).alias("FCLTY_TYP_CD"),
    F.rpad(F.col("svGeneratedDRGInd"),1," ").alias("IDS_GNRT_DRG_IN"),
    F.rpad(F.col("svAdmsDate"),10," ").alias("ADMS_DT"),
    F.rpad(F.col("svStatementFromDate"),10," ").alias("BILL_STMNT_BEG_DT"),
    F.rpad(F.col("svStatementToDate"),10," ").alias("BILL_STMNT_END_DT"),
    F.rpad(F.col("svDischargedDate"),10," ").alias("DSCHG_DT"),
    F.rpad(F.col("svHospBegDate"),10," ").alias("HOSP_BEG_DT"),
    F.rpad(F.col("svHospEndDate"),10," ").alias("HOSP_END_DT"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")))<1,F.lit(0))
     .otherwise(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")).alias("ADMS_HR"),
    F.when(
      (F.col("ClmHospIN.CLHP_DC_HOUR_CD").isNull())|(F.trim(F.col("ClmHospIN.CLHP_DC_HOUR_CD"))==""),
      F.lit(0)
    ).otherwise(
      F.col("ClmHospIN.CLHP_DC_HOUR_CD").cast("int")
    ).alias("DSCHG_HR"),
    F.expr("-1 * svHospCovDays").alias("HOSP_COV_DAYS"),
    F.expr("-1 * ClmHospIN.CLCL_LOS").alias("LOS_DAYS"),
    F.col("ClmHospIN.CLHP_PRPR_ID_ADM").alias("ADM_PHYS_PROV_ID"),
    F.rpad(F.col("svBillTypeText"),3," ").alias("FCLTY_BILL_TYP_TX"),
    F.col("svGeneratedDRGCd").alias("GNRT_DRG_CD"),
    F.col("ClmHospIN.CLHP_MED_REC_NO").alias("MED_RCRD_NO"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1"))).alias("OTHER_PROV_ID_1"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2"))).alias("OTHER_PROV_ID_2"),
    F.when(F.col("svSubDrg2").isNull(),F.lit("    ")).otherwise(F.col("svSubDrg2")).alias("SUBMT_DRG_CD"),
    F.rpad(F.col("svNormativeDRGCd"),4," ").alias("NRMTV_DRG_CD"),
    F.col("svDrgMethodCode").alias("DRG_METHOD_CD")
)

# ---------------------------
# Collector (CCollector)
# We union the two flows
# ---------------------------
df_Collector = df_reversals_out.unionByName(df_ClmHospOUT_out)

# ---------------------------
# Snapshot (CTransformerStage) => passes columns to next
# Output link "out" => FcltyClmPK
# Output link "SnapShot" => next Transformer
# ---------------------------
df_Snapshot_out = df_Collector.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("FCLTY_CLM_BILL_CLS_CD"),
    F.col("FCLTY_CLM_BILL_FREQ_CD"),
    F.col("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("FCLTY_CLM_SUBTYP_CD"),
    F.col("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.col("FCLTY_TYP_CD"),
    F.col("IDS_GNRT_DRG_IN"),
    F.col("ADMS_DT"),
    F.col("BILL_STMNT_BEG_DT"),
    F.col("BILL_STMNT_END_DT"),
    F.col("DSCHG_DT"),
    F.col("HOSP_BEG_DT"),
    F.col("HOSP_END_DT"),
    F.col("ADMS_HR"),
    F.col("DSCHG_HR"),
    F.col("HOSP_COV_DAYS"),
    F.col("LOS_DAYS"),
    F.col("ADM_PHYS_PROV_ID"),
    F.col("FCLTY_BILL_TYP_TX"),
    F.col("GNRT_DRG_CD"),
    F.col("MED_RCRD_NO"),
    F.col("OTHER_PROV_ID_1"),
    F.col("OTHER_PROV_ID_2"),
    F.col("SUBMT_DRG_CD"),
    F.col("NRMTV_DRG_CD"),
    F.col("DRG_METHOD_CD")
)

df_Snapshot_for_SnapShotLink = df_Snapshot_out.select(
    F.col("CLM_ID").alias("CLM_ID")  # primary key
)

# ---------------------------
# Next: "Snapshot" => output link "out" => FcltyClmPK
# ---------------------------
df_FcltyClmPK_in = df_Snapshot_out

# Call the shared container
params_for_fcltyclmpk = {
    "CurrRunCycle": CurrRunCycle
}
df_FcltyClmPK_out = FcltyClmPK(df_FcltyClmPK_in, params_for_fcltyclmpk)

# ---------------------------
# "FcltyClmPK" => output link => FctsClmFcltyTrns (CSeqFileStage => write)
# ---------------------------
df_FctsClmFcltyTrns_out = df_FcltyClmPK_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("FCLTY_CLM_BILL_CLS_CD"),
    F.col("FCLTY_CLM_BILL_FREQ_CD"),
    F.col("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("FCLTY_CLM_SUBTYP_CD"),
    F.col("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.col("FCLTY_TYP_CD"),
    F.col("IDS_GNRT_DRG_IN"),
    F.col("ADMS_DT"),
    F.col("BILL_STMNT_BEG_DT"),
    F.col("BILL_STMNT_END_DT"),
    F.col("DSCHG_DT"),
    F.col("HOSP_BEG_DT"),
    F.col("HOSP_END_DT"),
    F.col("ADMS_HR"),
    F.col("DSCHG_HR"),
    F.col("HOSP_COV_DAYS"),
    F.col("LOS_DAYS"),
    F.col("ADM_PHYS_PROV_ID"),
    F.col("FCLTY_BILL_TYP_TX"),
    F.col("GNRT_DRG_CD"),
    F.col("MED_RCRD_NO"),
    F.col("OTHER_PROV_ID_1"),
    F.col("OTHER_PROV_ID_2"),
    F.col("SUBMT_DRG_CD"),
    F.col("NRMTV_DRG_CD"),
    F.col("DRG_METHOD_CD")
)

out_path_fclty = f"{adls_path}/key/LhoFctsClmFcltyExtr.LhoFctsClmFclty.dat.{RunID}"
write_files(
    df_FctsClmFcltyTrns_out,
    out_path_fclty,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------
# The other output link from "Snapshot" => "SnapShot" => next "Transformer" => "RowCount" => B_FCLTY_CLM
# We emulate that by taking df_Snapshot_for_SnapShotLink
# Then the next Transformer uses columns:
#   SRC_SYS_CD_SK (primary key), CLM_ID (primary key)
#   from the instructions, let's pick them from environment or the snapshot
# ---------------------------
df_Transformer_in = df_Snapshot_for_SnapShotLink.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID")  # as primary key
)
df_B_FCLTY_CLM_out = df_Transformer_in

# Write B_FCLTY_CLM
b_fclty_path = f"{adls_path}/load/B_FCLTY_CLM.{SrcSysCd}.dat.{RunID}"
write_files(
    df_B_FCLTY_CLM_out,
    b_fclty_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)