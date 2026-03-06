# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2004, 2005, 2006, 2007, 2008, 2013 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsClmExtr1Seq
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

# MAGIC **built in FctsClmDriverBuild
# MAGIC ** bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
# MAGIC File created in FctsClmFcltyExtr job
# MAGIC This container is used in:
# MAGIC FctsClmFcltyTrns
# MAGIC NascoClmFcltyExtr
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
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

# Read from hashed file clm_nasco_dup_bypass (scenario C -> parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Read from hashed file hf_clm_fcts_reversals (scenario C -> parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read from CSeqFileStage FctsClmHospExtr
schema_FctsClmHospExtr = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("MEME_CK", IntegerType(), nullable=False),
    StructField("CLHP_FAC_TYPE", StringType(), nullable=False),
    StructField("CLHP_BILL_CLASS", StringType(), nullable=False),
    StructField("CLHP_FREQUENCY", StringType(), nullable=False),
    StructField("CLHP_PRPR_ID_ADM", StringType(), nullable=False),
    StructField("CLHP_PRPR_ID_OTH1", StringType(), nullable=False),
    StructField("CLHP_PRPR_ID_OTH2", StringType(), nullable=False),
    StructField("CLHP_ADM_TYP", StringType(), nullable=False),
    StructField("CLHP_ADM_DT", TimestampType(), nullable=False),
    StructField("CLHP_DC_STAT", StringType(), nullable=False),
    StructField("CLHP_DC_DT", TimestampType(), nullable=False),
    StructField("CLHP_STAMENT_FR_DT", TimestampType(), nullable=False),
    StructField("CLHP_STAMENT_TO_DT", TimestampType(), nullable=False),
    StructField("CLHP_MED_REC_NO", StringType(), nullable=False),
    StructField("CLHP_IPCD_METH", StringType(), nullable=False),
    StructField("CLHP_EXT_PRICE_AMT", DecimalType(), nullable=False),
    StructField("AGRG_ID", StringType(), nullable=False),
    StructField("CLHP_INPUT_AGRG_ID", StringType(), nullable=False),
    StructField("CLHP_ADM_HOUR_CD", StringType(), nullable=False),
    StructField("CLHP_ADM_SOURCE", StringType(), nullable=False),
    StructField("CLHP_DC_HOUR_CD", StringType(), nullable=False),
    StructField("CLHP_BIRTH_WGT", IntegerType(), nullable=False),
    StructField("CLHP_COVD_DAYS", IntegerType(), nullable=False),
    StructField("CLHP_LOCK_TOKEN", IntegerType(), nullable=False),
    StructField("ATXR_SOURCE_ID", TimestampType(), nullable=False),
    StructField("CLCL_LOS", IntegerType(), nullable=False),
    StructField("CLCL_RECD_DT", TimestampType(), nullable=False),
    StructField("CLCL_PAID_DT", TimestampType(), nullable=False),
    StructField("CLCL_CL_SUB_TYPE", StringType(), nullable=False)
])
df_FctsClmHospExtr = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_FctsClmHospExtr)
    .csv(f"{adls_path}/verified/FctsClmFcltyExtr.FctsClmFcltyExtr.uniq")
)

# Read from CSeqFileStage GeneratedPOA
schema_GeneratedPOA = StructType([
    StructField("HE_CLM_ID", StringType(), nullable=False),
    StructField("DIAG_CD_1", StringType(), nullable=False),
    StructField("DIAG_CD_2", StringType(), nullable=False),
    StructField("DIAG_CD_3", StringType(), nullable=False),
    StructField("DIAG_CD_4", StringType(), nullable=False),
    StructField("DIAG_CD_5", StringType(), nullable=False),
    StructField("DIAG_CD_6", StringType(), nullable=False),
    StructField("DIAG_CD_7", StringType(), nullable=False),
    StructField("DIAG_CD_8", StringType(), nullable=False),
    StructField("DIAG_CD_9", StringType(), nullable=False),
    StructField("DIAG_CD_10", StringType(), nullable=False),
    StructField("DIAG_CD_11", StringType(), nullable=False),
    StructField("DIAG_CD_12", StringType(), nullable=False),
    StructField("DIAG_CD_13", StringType(), nullable=False),
    StructField("DIAG_CD_14", StringType(), nullable=False),
    StructField("DIAG_CD_15", StringType(), nullable=False),
    StructField("DIAG_CD_16", StringType(), nullable=False),
    StructField("DIAG_CD_17", StringType(), nullable=False),
    StructField("DIAG_CD_18", StringType(), nullable=False),
    StructField("DIAG_CD_19", StringType(), nullable=False),
    StructField("DIAG_CD_20", StringType(), nullable=False),
    StructField("DIAG_CD_21", StringType(), nullable=False),
    StructField("DIAG_CD_22", StringType(), nullable=False),
    StructField("DIAG_CD_23", StringType(), nullable=False),
    StructField("DIAG_CD_24", StringType(), nullable=False),
    StructField("DIAG_CD_25", StringType(), nullable=False),
    StructField("DX_TYPE_1", StringType(), nullable=False),
    StructField("DX_TYPE_2", StringType(), nullable=False),
    StructField("DX_TYPE_3", StringType(), nullable=False),
    StructField("DX_TYPE_4", StringType(), nullable=False),
    StructField("DX_TYPE_5", StringType(), nullable=False),
    StructField("DX_TYPE_6", StringType(), nullable=False),
    StructField("DX_TYPE_7", StringType(), nullable=False),
    StructField("DX_TYPE_8", StringType(), nullable=False),
    StructField("DX_TYPE_9", StringType(), nullable=False),
    StructField("DX_TYPE_10", StringType(), nullable=False),
    StructField("DX_TYPE_11", StringType(), nullable=False),
    StructField("DX_TYPE_12", StringType(), nullable=False),
    StructField("DX_TYPE_13", StringType(), nullable=False),
    StructField("DX_TYPE_14", StringType(), nullable=False),
    StructField("DX_TYPE_15", StringType(), nullable=False),
    StructField("DX_TYPE_16", StringType(), nullable=False),
    StructField("DX_TYPE_17", StringType(), nullable=False),
    StructField("DX_TYPE_18", StringType(), nullable=False),
    StructField("DX_TYPE_19", StringType(), nullable=False),
    StructField("DX_TYPE_20", StringType(), nullable=False),
    StructField("DX_TYPE_21", StringType(), nullable=False),
    StructField("DX_TYPE_22", StringType(), nullable=False),
    StructField("DX_TYPE_23", StringType(), nullable=False),
    StructField("DX_TYPE_24", StringType(), nullable=False),
    StructField("DX_TYPE_25", StringType(), nullable=False),
    StructField("PROC_CD_1", StringType(), nullable=False),
    StructField("PROC_CD_2", StringType(), nullable=False),
    StructField("PROC_CD_3", StringType(), nullable=False),
    StructField("PROC_CD_4", StringType(), nullable=False),
    StructField("PROC_CD_5", StringType(), nullable=False),
    StructField("PROC_CD_6", StringType(), nullable=False),
    StructField("PROC_CD_7", StringType(), nullable=False),
    StructField("PROC_CD_8", StringType(), nullable=False),
    StructField("PROC_CD_9", StringType(), nullable=False),
    StructField("PROC_CD_10", StringType(), nullable=False),
    StructField("PROC_CD_11", StringType(), nullable=False),
    StructField("PROC_CD_12", StringType(), nullable=False),
    StructField("PROC_CD_13", StringType(), nullable=False),
    StructField("PROC_CD_14", StringType(), nullable=False),
    StructField("PROC_CD_15", StringType(), nullable=False),
    StructField("PROC_CD_16", StringType(), nullable=False),
    StructField("PROC_CD_17", StringType(), nullable=False),
    StructField("PROC_CD_18", StringType(), nullable=False),
    StructField("PROC_CD_19", StringType(), nullable=False),
    StructField("PROC_CD_20", StringType(), nullable=False),
    StructField("PROC_CD_21", StringType(), nullable=False),
    StructField("PROC_CD_22", StringType(), nullable=False),
    StructField("PROC_CD_23", StringType(), nullable=False),
    StructField("PROC_CD_24", StringType(), nullable=False),
    StructField("PROC_CD_25", StringType(), nullable=False),
    StructField("PX_TYPE_1", StringType(), nullable=False),
    StructField("PX_TYPE_2", StringType(), nullable=False),
    StructField("PX_TYPE_3", StringType(), nullable=False),
    StructField("PX_TYPE_4", StringType(), nullable=False),
    StructField("PX_TYPE_5", StringType(), nullable=False),
    StructField("PX_TYPE_6", StringType(), nullable=False),
    StructField("PX_TYPE_7", StringType(), nullable=False),
    StructField("PX_TYPE_8", StringType(), nullable=False),
    StructField("PX_TYPE_9", StringType(), nullable=False),
    StructField("PX_TYPE_10", StringType(), nullable=False),
    StructField("PX_TYPE_11", StringType(), nullable=False),
    StructField("PX_TYPE_12", StringType(), nullable=False),
    StructField("PX_TYPE_13", StringType(), nullable=False),
    StructField("PX_TYPE_14", StringType(), nullable=False),
    StructField("PX_TYPE_15", StringType(), nullable=False),
    StructField("PX_TYPE_16", StringType(), nullable=False),
    StructField("PX_TYPE_17", StringType(), nullable=False),
    StructField("PX_TYPE_18", StringType(), nullable=False),
    StructField("PX_TYPE_19", StringType(), nullable=False),
    StructField("PX_TYPE_20", StringType(), nullable=False),
    StructField("PX_TYPE_21", StringType(), nullable=False),
    StructField("PX_TYPE_22", StringType(), nullable=False),
    StructField("PX_TYPE_23", StringType(), nullable=False),
    StructField("PX_TYPE_24", StringType(), nullable=False),
    StructField("PX_TYPE_25", StringType(), nullable=False),
    StructField("AGE", StringType(), nullable=False),
    StructField("SEX", StringType(), nullable=False),
    StructField("DISP", StringType(), nullable=False),
    StructField("DRG", StringType(), nullable=False),
    StructField("MDC", StringType(), nullable=False),
    StructField("VERS", StringType(), nullable=False),
    StructField("FILL1", StringType(), nullable=False),
    StructField("OP", StringType(), nullable=False),
    StructField("RTN_CODE", StringType(), nullable=False),
    StructField("NO_DX", StringType(), nullable=False),
    StructField("NO_OP", StringType(), nullable=False),
    StructField("ADAYS_A", StringType(), nullable=False),
    StructField("ADAYS_D", StringType(), nullable=False),
    StructField("BWGT", StringType(), nullable=False),
    StructField("GTYPE", StringType(), nullable=False),
    StructField("CLMD_POA_IND_1", StringType(), nullable=False),
    StructField("CLMD_POA_IND_2", StringType(), nullable=False),
    StructField("CLMD_POA_IND_3", StringType(), nullable=False),
    StructField("CLMD_POA_IND_4", StringType(), nullable=False),
    StructField("CLMD_POA_IND_5", StringType(), nullable=False),
    StructField("CLMD_POA_IND_6", StringType(), nullable=False),
    StructField("CLMD_POA_IND_7", StringType(), nullable=False),
    StructField("CLMD_POA_IND_8", StringType(), nullable=False),
    StructField("CLMD_POA_IND_9", StringType(), nullable=False),
    StructField("CLMD_POA_IND_10", StringType(), nullable=False),
    StructField("CLMD_POA_IND_11", StringType(), nullable=False),
    StructField("CLMD_POA_IND_12", StringType(), nullable=False),
    StructField("CLMD_POA_IND_13", StringType(), nullable=False),
    StructField("CLMD_POA_IND_14", StringType(), nullable=False),
    StructField("CLMD_POA_IND_15", StringType(), nullable=False),
    StructField("CLMD_POA_IND_16", StringType(), nullable=False),
    StructField("CLMD_POA_IND_17", StringType(), nullable=False),
    StructField("CLMD_POA_IND_18", StringType(), nullable=False),
    StructField("CLMD_POA_IND_19", StringType(), nullable=False),
    StructField("CLMD_POA_IND_20", StringType(), nullable=False),
    StructField("CLMD_POA_IND_21", StringType(), nullable=False),
    StructField("CLMD_POA_IND_22", StringType(), nullable=False),
    StructField("CLMD_POA_IND_23", StringType(), nullable=False),
    StructField("CLMD_POA_IND_24", StringType(), nullable=False),
    StructField("CLMD_POA_IND_25", StringType(), nullable=False),
    StructField("CODE_CLASS_IND", StringType(), nullable=False),
    StructField("PTNT_TYP", StringType(), nullable=False),
    StructField("CLS_TYP", StringType(), nullable=False)
])
df_GeneratedPOA = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_GeneratedPOA)
    .csv(f"{adls_path}/verified/FctsFcltyClmDRGGen_{RunID}_results.dat")
)

# Read from hashed file hf_fclty_clm_gen_drg_grouper (scenario C -> parquet)
df_hf_fclty_clm_gen_drg_grouper = spark.read.parquet(f"{adls_path}/hf_fclty_clm_gen_drg_grouper.parquet")

# Read from CSeqFileStage NormativePOA
schema_NormativePOA = StructType([
    StructField("HE_CLM_ID", StringType(), nullable=False),
    StructField("DIAG_CD_1", StringType(), nullable=False),
    StructField("DIAG_CD_2", StringType(), nullable=False),
    StructField("DIAG_CD_3", StringType(), nullable=False),
    StructField("DIAG_CD_4", StringType(), nullable=False),
    StructField("DIAG_CD_5", StringType(), nullable=False),
    StructField("DIAG_CD_6", StringType(), nullable=False),
    StructField("DIAG_CD_7", StringType(), nullable=False),
    StructField("DIAG_CD_8", StringType(), nullable=False),
    StructField("DIAG_CD_9", StringType(), nullable=False),
    StructField("DIAG_CD_10", StringType(), nullable=False),
    StructField("DIAG_CD_11", StringType(), nullable=False),
    StructField("DIAG_CD_12", StringType(), nullable=False),
    StructField("DIAG_CD_13", StringType(), nullable=False),
    StructField("DIAG_CD_14", StringType(), nullable=False),
    StructField("DIAG_CD_15", StringType(), nullable=False),
    StructField("DIAG_CD_16", StringType(), nullable=False),
    StructField("DIAG_CD_17", StringType(), nullable=False),
    StructField("DIAG_CD_18", StringType(), nullable=False),
    StructField("DIAG_CD_19", StringType(), nullable=False),
    StructField("DIAG_CD_20", StringType(), nullable=False),
    StructField("DIAG_CD_21", StringType(), nullable=False),
    StructField("DIAG_CD_22", StringType(), nullable=False),
    StructField("DIAG_CD_23", StringType(), nullable=False),
    StructField("DIAG_CD_24", StringType(), nullable=False),
    StructField("DIAG_CD_25", StringType(), nullable=False),
    StructField("DX_TYPE_1", StringType(), nullable=False),
    StructField("DX_TYPE_2", StringType(), nullable=False),
    StructField("DX_TYPE_3", StringType(), nullable=False),
    StructField("DX_TYPE_4", StringType(), nullable=False),
    StructField("DX_TYPE_5", StringType(), nullable=False),
    StructField("DX_TYPE_6", StringType(), nullable=False),
    StructField("DX_TYPE_7", StringType(), nullable=False),
    StructField("DX_TYPE_8", StringType(), nullable=False),
    StructField("DX_TYPE_9", StringType(), nullable=False),
    StructField("DX_TYPE_10", StringType(), nullable=False),
    StructField("DX_TYPE_11", StringType(), nullable=False),
    StructField("DX_TYPE_12", StringType(), nullable=False),
    StructField("DX_TYPE_13", StringType(), nullable=False),
    StructField("DX_TYPE_14", StringType(), nullable=False),
    StructField("DX_TYPE_15", StringType(), nullable=False),
    StructField("DX_TYPE_16", StringType(), nullable=False),
    StructField("DX_TYPE_17", StringType(), nullable=False),
    StructField("DX_TYPE_18", StringType(), nullable=False),
    StructField("DX_TYPE_19", StringType(), nullable=False),
    StructField("DX_TYPE_20", StringType(), nullable=False),
    StructField("DX_TYPE_21", StringType(), nullable=False),
    StructField("DX_TYPE_22", StringType(), nullable=False),
    StructField("DX_TYPE_23", StringType(), nullable=False),
    StructField("DX_TYPE_24", StringType(), nullable=False),
    StructField("DX_TYPE_25", StringType(), nullable=False),
    StructField("PROC_CD_1", StringType(), nullable=False),
    StructField("PROC_CD_2", StringType(), nullable=False),
    StructField("PROC_CD_3", StringType(), nullable=False),
    StructField("PROC_CD_4", StringType(), nullable=False),
    StructField("PROC_CD_5", StringType(), nullable=False),
    StructField("PROC_CD_6", StringType(), nullable=False),
    StructField("PROC_CD_7", StringType(), nullable=False),
    StructField("PROC_CD_8", StringType(), nullable=False),
    StructField("PROC_CD_9", StringType(), nullable=False),
    StructField("PROC_CD_10", StringType(), nullable=False),
    StructField("PROC_CD_11", StringType(), nullable=False),
    StructField("PROC_CD_12", StringType(), nullable=False),
    StructField("PROC_CD_13", StringType(), nullable=False),
    StructField("PROC_CD_14", StringType(), nullable=False),
    StructField("PROC_CD_15", StringType(), nullable=False),
    StructField("PROC_CD_16", StringType(), nullable=False),
    StructField("PROC_CD_17", StringType(), nullable=False),
    StructField("PROC_CD_18", StringType(), nullable=False),
    StructField("PROC_CD_19", StringType(), nullable=False),
    StructField("PROC_CD_20", StringType(), nullable=False),
    StructField("PROC_CD_21", StringType(), nullable=False),
    StructField("PROC_CD_22", StringType(), nullable=False),
    StructField("PROC_CD_23", StringType(), nullable=False),
    StructField("PROC_CD_24", StringType(), nullable=False),
    StructField("PROC_CD_25", StringType(), nullable=False),
    StructField("PX_TYPE_1", StringType(), nullable=False),
    StructField("PX_TYPE_2", StringType(), nullable=False),
    StructField("PX_TYPE_3", StringType(), nullable=False),
    StructField("PX_TYPE_4", StringType(), nullable=False),
    StructField("PX_TYPE_5", StringType(), nullable=False),
    StructField("PX_TYPE_6", StringType(), nullable=False),
    StructField("PX_TYPE_7", StringType(), nullable=False),
    StructField("PX_TYPE_8", StringType(), nullable=False),
    StructField("PX_TYPE_9", StringType(), nullable=False),
    StructField("PX_TYPE_10", StringType(), nullable=False),
    StructField("PX_TYPE_11", StringType(), nullable=False),
    StructField("PX_TYPE_12", StringType(), nullable=False),
    StructField("PX_TYPE_13", StringType(), nullable=False),
    StructField("PX_TYPE_14", StringType(), nullable=False),
    StructField("PX_TYPE_15", StringType(), nullable=False),
    StructField("PX_TYPE_16", StringType(), nullable=False),
    StructField("PX_TYPE_17", StringType(), nullable=False),
    StructField("PX_TYPE_18", StringType(), nullable=False),
    StructField("PX_TYPE_19", StringType(), nullable=False),
    StructField("PX_TYPE_20", StringType(), nullable=False),
    StructField("PX_TYPE_21", StringType(), nullable=False),
    StructField("PX_TYPE_22", StringType(), nullable=False),
    StructField("PX_TYPE_23", StringType(), nullable=False),
    StructField("PX_TYPE_24", StringType(), nullable=False),
    StructField("PX_TYPE_25", StringType(), nullable=False),
    StructField("AGE", StringType(), nullable=False),
    StructField("SEX", StringType(), nullable=False),
    StructField("DISP", StringType(), nullable=False),
    StructField("DRG", StringType(), nullable=False),
    StructField("MDC", StringType(), nullable=False),
    StructField("VERS", StringType(), nullable=False),
    StructField("FILL1", StringType(), nullable=False),
    StructField("OP", StringType(), nullable=False),
    StructField("RTN_CODE", StringType(), nullable=False),
    StructField("NO_DX", StringType(), nullable=False),
    StructField("NO_OP", StringType(), nullable=False),
    StructField("ADAYS_A", StringType(), nullable=False),
    StructField("ADAYS_D", StringType(), nullable=False),
    StructField("BWGT", StringType(), nullable=False),
    StructField("GTYPE", StringType(), nullable=False),
    StructField("CLMD_POA_IND_1", StringType(), nullable=False),
    StructField("CLMD_POA_IND_2", StringType(), nullable=False),
    StructField("CLMD_POA_IND_3", StringType(), nullable=False),
    StructField("CLMD_POA_IND_4", StringType(), nullable=False),
    StructField("CLMD_POA_IND_5", StringType(), nullable=False),
    StructField("CLMD_POA_IND_6", StringType(), nullable=False),
    StructField("CLMD_POA_IND_7", StringType(), nullable=False),
    StructField("CLMD_POA_IND_8", StringType(), nullable=False),
    StructField("CLMD_POA_IND_9", StringType(), nullable=False),
    StructField("CLMD_POA_IND_10", StringType(), nullable=False),
    StructField("CLMD_POA_IND_11", StringType(), nullable=False),
    StructField("CLMD_POA_IND_12", StringType(), nullable=False),
    StructField("CLMD_POA_IND_13", StringType(), nullable=False),
    StructField("CLMD_POA_IND_14", StringType(), nullable=False),
    StructField("CLMD_POA_IND_15", StringType(), nullable=False),
    StructField("CLMD_POA_IND_16", StringType(), nullable=False),
    StructField("CLMD_POA_IND_17", StringType(), nullable=False),
    StructField("CLMD_POA_IND_18", StringType(), nullable=False),
    StructField("CLMD_POA_IND_19", StringType(), nullable=False),
    StructField("CLMD_POA_IND_20", StringType(), nullable=False),
    StructField("CLMD_POA_IND_21", StringType(), nullable=False),
    StructField("CLMD_POA_IND_22", StringType(), nullable=False),
    StructField("CLMD_POA_IND_23", StringType(), nullable=False),
    StructField("CLMD_POA_IND_24", StringType(), nullable=False),
    StructField("CLMD_POA_IND_25", StringType(), nullable=False),
    StructField("CODE_CLASS_IND", StringType(), nullable=False),
    StructField("PTNT_TYP", StringType(), nullable=False),
    StructField("CLS_TYP", StringType(), nullable=False)
])
df_NormativePOA = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_NormativePOA)
    .csv(f"{adls_path}/verified/FctsFcltyClmDRGNorm_{RunID}_results.dat")
)

# Read from hashed file hf_fclty_clm_nrmtv_drg_grouper (scenario C -> parquet)
df_hf_fclty_clm_nrmtv_drg_grouper = spark.read.parquet(f"{adls_path}/hf_fclty_clm_nrmtv_drg_grouper.parquet")

# Now perform the SETUP_CRF transformer logic (multiple left joins)
df_setup = (
    df_FctsClmHospExtr.alias("ClmHospIN")
    .join(df_hf_fclty_clm_gen_drg_grouper.alias("grouper_out"),
          F.col("ClmHospIN.CLCL_ID") == F.col("grouper_out.HE_CLM_ID"),
          "left")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"),
          F.col("ClmHospIN.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
          "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
          F.col("ClmHospIN.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
          "left")
    .join(df_hf_fclty_clm_nrmtv_drg_grouper.alias("normative_drg"),
          F.col("ClmHospIN.CLCL_ID") == F.col("normative_drg.HE_CLM_ID"),
          "left")
)

df_setup_vars = df_setup.withColumn(
    "svClaimReceivedDate",
    F.when(F.col("ClmHospIN.CLCL_RECD_DT").isNull(), F.lit("1753-01-01"))
     .otherwise(F.date_format(F.col("ClmHospIN.CLCL_RECD_DT"), "yyyy-MM-dd"))
).withColumn(
    "svClaimPaidDate",
    F.when(F.col("ClmHospIN.CLCL_PAID_DT").isNull(), F.lit("1753-01-01"))
     .otherwise(F.date_format(F.col("ClmHospIN.CLCL_PAID_DT"), "yyyy-MM-dd"))
).withColumn(
    "svDischargedDate",
    F.when(F.col("ClmHospIN.CLHP_DC_DT").isNull(), F.lit("1753-01-01"))
     .otherwise(F.date_format(F.col("ClmHospIN.CLHP_DC_DT"), "yyyy-MM-dd"))
).withColumn(
    "svStatementFromDate",
    F.when(F.col("ClmHospIN.CLHP_STAMENT_FR_DT").isNull(), F.lit("1753-01-01"))
     .otherwise(F.date_format(F.col("ClmHospIN.CLHP_STAMENT_FR_DT"), "yyyy-MM-dd"))
).withColumn(
    "svStatementToDate",
    F.when(F.col("ClmHospIN.CLHP_STAMENT_TO_DT").isNull(), F.lit("1753-01-01"))
     .otherwise(F.date_format(F.col("ClmHospIN.CLHP_STAMENT_TO_DT"), "yyyy-MM-dd"))
).withColumn(
    "svFacType",
    F.expr("substring(ClmHospIN.CLHP_FAC_TYPE, 1, 1)")
).withColumn(
    "svBillTypeText",
    F.when(F.length(F.concat(F.col("svFacType"),
                             F.trim(F.col("ClmHospIN.CLHP_BILL_CLASS")),
                             F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))) > 0,
           F.concat(F.col("svFacType"),
                    F.trim(F.col("ClmHospIN.CLHP_BILL_CLASS")),
                    F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))
          ).otherwise(F.lit("NA"))
).withColumn(
    "svSubType",
    F.when(F.col("ClmHospIN.CLCL_CL_SUB_TYPE").isNull(), F.lit(" "))
     .otherwise(
        F.when(F.length(F.trim(F.col("ClmHospIN.CLCL_CL_SUB_TYPE"))) == 0, F.lit(" "))
         .otherwise(F.col("ClmHospIN.CLCL_CL_SUB_TYPE"))
     )
).withColumn(
    "svSubDrg1",
    F.expr("""
        CASE
          WHEN CAST(substring(ClmHospIN.CLHP_INPUT_AGRG_ID,1,1) AS int) IS NOT NULL THEN
            CASE
              WHEN length(trim(ClmHospIN.CLHP_INPUT_AGRG_ID))=4 THEN trim(ClmHospIN.CLHP_INPUT_AGRG_ID)
              WHEN length(trim(ClmHospIN.CLHP_INPUT_AGRG_ID))=3 THEN '0' || trim(ClmHospIN.CLHP_INPUT_AGRG_ID)
              WHEN length(trim(ClmHospIN.CLHP_INPUT_AGRG_ID))=2 THEN '00' || trim(ClmHospIN.CLHP_INPUT_AGRG_ID)
              WHEN length(trim(ClmHospIN.CLHP_INPUT_AGRG_ID))=1 THEN '000' || trim(ClmHospIN.CLHP_INPUT_AGRG_ID)
              ELSE 'NA'
            END
          ELSE
            CASE
              WHEN substring(ClmHospIN.CLHP_INPUT_AGRG_ID,1,1)='D' THEN '0'|| substring(ClmHospIN.CLHP_INPUT_AGRG_ID,2,3)
              ELSE '0000'
            END
        END
    """)
).withColumn(
    "svSubDrg2",
    F.when(
        (F.length(F.trim(F.col("svSubDrg1")))==0) | (F.trim(F.col("svSubDrg1"))=="0000"),
        F.lit(None)
    ).otherwise(
        F.expr("right('000' || trim(svSubDrg1), 4)")
    )
).withColumn(
    "ClmSubtyp",
    F.expr("""
        CASE
          WHEN ClmHospIN.CLCL_CL_SUB_TYPE IN ('M','D') THEN 'P'
          WHEN ClmHospIN.CLCL_CL_SUB_TYPE='H' THEN
            CASE
              WHEN svFacType > '6' THEN 'O'
              WHEN svFacType < '7' AND (ClmHospIN.CLHP_BILL_CLASS='3' OR ClmHospIN.CLHP_BILL_CLASS='4')
                THEN 'O'
              ELSE 'I'
            END
          ELSE 'UNK'
        END
    """)
).withColumn(
    "ClmId",
    F.trim(F.col("ClmHospIN.CLCL_ID"))
).withColumn(
    "svGeneratedDRGCd",
    F.expr("""
        CASE
          WHEN ClmSubtyp='O' THEN 'NA'
          WHEN length(trim(ClmHospIN.AGRG_ID))>1 THEN
            CASE
              WHEN (svClaimReceivedDate<'2007-10-01' OR svClaimReceivedDate>'2007-10-31')
                   AND (svClaimPaidDate<'2007-10-01' OR svClaimPaidDate>'2007-10-31')
                THEN right('000'|| trim(ClmHospIN.AGRG_ID),4)
              WHEN (svClaimReceivedDate>='2007-10-01' AND svClaimReceivedDate<='2007-10-31')
                   AND (svClaimPaidDate>='2007-10-01' AND svClaimPaidDate<='2007-10-31')
                THEN
                  CASE
                    WHEN svDischargedDate='1753-01-01' THEN
                      CASE
                        WHEN svStatementToDate='1753-01-01' THEN 'UNK'
                        WHEN (svStatementToDate<'2007-10-01' OR svStatementToDate>'2007-10-31')
                          THEN right('000'|| trim(ClmHospIN.AGRG_ID),4)
                        ELSE
                          CASE
                            WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'UNK'
                            ELSE right('000'|| grouper_out.DRG,4)
                          END
                      END
                    WHEN (svDischargedDate<'2007-10-01' OR svDischargedDate>'2007-10-31')
                      THEN right('000'|| trim(ClmHospIN.AGRG_ID),4)
                    ELSE
                      CASE
                        WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'UNK'
                        ELSE right('000'|| grouper_out.DRG,4)
                      END
                  END
              ELSE
                CASE
                  WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'UNK'
                  ELSE right('000'|| grouper_out.DRG,4)
                END
            END
          ELSE
            CASE
              WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'UNK'
              ELSE right('000'|| grouper_out.DRG,4)
            END
        END
    """)
).withColumn(
    "svGeneratedDRGInd",
    F.expr("""
        CASE
          WHEN ClmSubtyp='O' THEN 'N'
          WHEN length(trim(ClmHospIN.AGRG_ID))>1 THEN
            CASE
              WHEN (svClaimReceivedDate<'2007-10-01' OR svClaimReceivedDate>'2007-10-31')
                   AND (svClaimPaidDate<'2007-10-01' OR svClaimPaidDate>'2007-10-31')
                THEN 'N'
              WHEN (svClaimReceivedDate>='2007-10-01' AND svClaimReceivedDate<='2007-10-31')
                   AND (svClaimPaidDate>='2007-10-01' AND svClaimPaidDate<='2007-10-31')
                THEN
                  CASE
                    WHEN svDischargedDate='1753-01-01' THEN
                      CASE
                        WHEN svStatementToDate='1753-01-01' THEN 'U'
                        WHEN (svStatementToDate<'2007-10-01' OR svStatementToDate>'2007-10-31') THEN 'N'
                        ELSE
                          CASE
                            WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'U'
                            ELSE 'Y'
                          END
                      END
                    WHEN (svDischargedDate<'2007-10-01' OR svDischargedDate>'2007-10-31')
                      THEN 'N'
                    ELSE
                      CASE
                        WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'U'
                        ELSE 'Y'
                      END
                  END
              ELSE 'N'
            END
          ELSE
            CASE
              WHEN grouper_out.DRG IS NULL OR length(trim(grouper_out.DRG))=0 THEN 'U'
              ELSE 'Y'
            END
        END
    """)
).withColumn(
    "svNormativeDRGCd",
    F.when(F.col("ClmSubtyp")=="O", F.lit("NA"))
     .otherwise(
         F.when((F.col("normative_drg.DRG").isNull())|(F.length(F.trim(F.col("normative_drg.DRG")))==0),
                F.lit("0999"))
         .otherwise(F.expr("right('000'|| normative_drg.DRG,4)"))
     )
).withColumn(
    "svDrgMethodCode",
    F.expr("""
        CASE
          WHEN svDischargedDate='1753-01-01' OR svDischargedDate='NA' OR svDischargedDate='UNK'
             OR length(trim(svDischargedDate))=0
          THEN
            CASE
              WHEN svStatementToDate='1753-01-01' OR length(trim(svStatementToDate))=0
                   OR svStatementToDate='NA' OR svStatementToDate='UNK'
              THEN 'MS'
              WHEN trim(svStatementToDate)>'1800-01-01' AND trim(svStatementToDate)<'2007-10-01'
                THEN 'CMS'
              WHEN trim(svStatementToDate)>='2007-10-01' THEN 'MS'
              ELSE 'UNK'
            END
          ELSE
            CASE
              WHEN trim(svDischargedDate)>='1800-01-01' AND trim(svDischargedDate)<'2007-10-01'
                THEN 'CMS'
              WHEN trim(svDischargedDate)>='2007-10-01'
                THEN 'MS'
              ELSE 'UNK'
            END
        END
    """)
).withColumn(
    "svClmAdmsTyp",
    F.when(
        (F.col("svSubType")=="H") & (F.expr("svFacType rlike '^[0-9]+$'")) & (F.col("svFacType")<"7"),
        F.when((F.col("ClmHospIN.CLHP_BILL_CLASS")=="3")|(F.col("ClmHospIN.CLHP_BILL_CLASS")=="4"), F.lit("NA"))
         .otherwise(
             F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_TYP")))==0, F.lit("UNK"))
             .otherwise(F.col("ClmHospIN.CLHP_ADM_TYP"))
         )
    ).otherwise(F.lit("NA"))
).withColumn(
    "svBillCls",
    F.when(F.col("svFacType")=="7",
           F.concat(F.lit("7"), F.col("ClmHospIN.CLHP_BILL_CLASS")))
     .when(F.col("ClmHospIN.CLHP_FAC_TYPE")=="8",
           F.concat(F.lit("8"), F.col("ClmHospIN.CLHP_BILL_CLASS")))
     .otherwise(F.concat(F.lit("0"), F.col("ClmHospIN.CLHP_BILL_CLASS")))
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
        (F.trim(F.col("ClmHospIN.CLHP_ADM_TYP"))=="4") & (F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE")))>0),
        F.concat(F.lit("4"), F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE")))
    ).otherwise(
        F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_TYP")))==0, F.lit("NA"))
         .otherwise(
             F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE")))==0, F.lit("UNK"))
             .otherwise(F.trim(F.col("ClmHospIN.CLHP_ADM_SOURCE")))
         )
    )
).withColumn(
    "svHospCovDays",
    F.when(F.col("ClmHospIN.CLHP_COVD_DAYS")<=999, F.col("ClmHospIN.CLHP_COVD_DAYS")).otherwise(F.lit(999))
)

df_ClmHospOUT = df_setup_vars.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_reversals = df_setup_vars.filter(F.col("fcts_reversals.CLCL_ID").isNotNull())

df_ClmHospOUT_sel = df_ClmHospOUT.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrentDate").alias("FIRST_RECYC_DT"),  # from parameter or transformation?
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("ClmId")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("FCLTY_CLM_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("CLM_SK"),
    F.col("svAdmsSrc").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("svClmAdmsTyp").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("svBillCls").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))==0, F.lit("UNK"))
     .otherwise(F.col("ClmHospIN.CLHP_FREQUENCY")).alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.when(F.expr("substring(ClmHospIN.CLHP_DC_STAT,1,1)=' '"),F.lit(" "))
     .otherwise(F.upper(F.col("ClmHospIN.CLHP_DC_STAT"))).alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("ClmSubtyp").alias("FCLTY_CLM_SUBTYP_CD"),
    F.col("ClmHospIN.CLHP_IPCD_METH").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.when(F.length(F.col("svFacType"))==0,F.lit("NA")).otherwise(F.col("svFacType")).alias("FCLTY_TYP_CD"),
    F.col("svGeneratedDRGInd").alias("IDS_GNRT_DRG_IN"),
    F.col("svAdmsDate").alias("ADMS_DT"),
    F.col("svStatementFromDate").alias("BILL_STMNT_BEG_DT"),
    F.col("svStatementToDate").alias("BILL_STMNT_END_DT"),
    F.col("svDischargedDate").alias("DSCHG_DT"),
    F.col("svHospBegDate").alias("HOSP_BEG_DT"),
    F.col("svHospEndDate").alias("HOSP_END_DT"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")))<1,F.lit("0"))
     .otherwise(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")).alias("ADMS_HR"),
    F.when(F.col("ClmHospIN.CLHP_DC_HOUR_CD").isNull() | (F.length(F.trim(F.col("ClmHospIN.CLHP_DC_HOUR_CD")))==0),
           F.lit("0")
     ).otherwise(F.expr("ClmHospIN.CLHP_DC_HOUR_CD")).alias("DSCHG_HR"),
    F.col("svHospCovDays").alias("HOSP_COV_DAYS"),
    F.col("ClmHospIN.CLCL_LOS").alias("LOS_DAYS"),
    F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_ADM")).alias("ADM_PHYS_PROV_ID"),
    F.col("svBillTypeText").alias("FCLTY_BILL_TYP_TX"),
    F.col("svGeneratedDRGCd").alias("GNRT_DRG_CD"),
    F.col("ClmHospIN.CLHP_MED_REC_NO").alias("MED_RCRD_NO"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1"))).alias("OTHER_PROV_ID_1"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2"))).alias("OTHER_PROV_ID_2"),
    F.col("svSubDrg2").alias("SUBMT_DRG_CD"),
    F.col("svNormativeDRGCd").alias("NRMTV_DRG_CD"),
    F.col("svDrgMethodCode").alias("DRG_METHOD_CD")
)

df_reversals_sel = df_reversals.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrentDate").alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("ClmId"),F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("FCLTY_CLM_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.concat(F.col("ClmId"),F.lit("R")).alias("CLM_ID"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("CLM_SK"),
    F.col("svAdmsSrc").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("svClmAdmsTyp").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("svBillCls").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_FREQUENCY")))<1, F.lit("NA"))
     .otherwise(F.col("ClmHospIN.CLHP_FREQUENCY")).alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.when(F.expr("substring(ClmHospIN.CLHP_DC_STAT,1,1)=' '"),F.lit(" "))
     .otherwise(F.upper(F.col("ClmHospIN.CLHP_DC_STAT"))).alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("ClmSubtyp").alias("FCLTY_CLM_SUBTYP_CD"),
    F.col("ClmHospIN.CLHP_IPCD_METH").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.when(F.length(F.col("svFacType"))==0,F.lit("NA")).otherwise(F.col("svFacType")).alias("FCLTY_TYP_CD"),
    F.col("svGeneratedDRGInd").alias("IDS_GNRT_DRG_IN"),
    F.col("svAdmsDate").alias("ADMS_DT"),
    F.col("svStatementFromDate").alias("BILL_STMNT_BEG_DT"),
    F.col("svStatementToDate").alias("BILL_STMNT_END_DT"),
    F.col("svDischargedDate").alias("DSCHG_DT"),
    F.col("svHospBegDate").alias("HOSP_BEG_DT"),
    F.col("svHospEndDate").alias("HOSP_END_DT"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")))<1,F.lit("0"))
     .otherwise(F.col("ClmHospIN.CLHP_ADM_HOUR_CD")).alias("ADMS_HR"),
    F.when(F.col("ClmHospIN.CLHP_DC_HOUR_CD").isNull()|(F.length(F.trim(F.col("ClmHospIN.CLHP_DC_HOUR_CD")))==0),
           F.lit("0")
    ).otherwise(F.expr("ClmHospIN.CLHP_DC_HOUR_CD")).alias("DSCHG_HR"),
    F.expr("-(svHospCovDays)").alias("HOSP_COV_DAYS"),
    F.expr("-(ClmHospIN.CLCL_LOS)").alias("LOS_DAYS"),
    F.col("ClmHospIN.CLHP_PRPR_ID_ADM").alias("ADM_PHYS_PROV_ID"),
    F.col("svBillTypeText").alias("FCLTY_BILL_TYP_TX"),
    F.col("svGeneratedDRGCd").alias("GNRT_DRG_CD"),
    F.col("ClmHospIN.CLHP_MED_REC_NO").alias("MED_RCRD_NO"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH1"))).alias("OTHER_PROV_ID_1"),
    F.when(F.length(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("ClmHospIN.CLHP_PRPR_ID_OTH2"))).alias("OTHER_PROV_ID_2"),
    F.col("svSubDrg2").alias("SUBMT_DRG_CD"),
    F.col("svNormativeDRGCd").alias("NRMTV_DRG_CD"),
    F.col("svDrgMethodCode").alias("DRG_METHOD_CD")
)

df_Collector = df_ClmHospOUT_sel.unionByName(df_reversals_sel)

# Snapshot transformer
df_Snapshot_out = df_Collector
df_Snapshot_snap = df_Snapshot_out.select(
    F.col("CLM_ID").alias("CLM_ID")
)

# Output "out" => goes to container "FcltyClmPK"
params_FcltyClmPK = {
    "CurrRunCycle": CurrRunCycle
}
df_FcltyClmPK = FcltyClmPK(df_Snapshot_out, params_FcltyClmPK)

# Write to FctsClmFcltyTrns (CSeqFileStage) => final columns with rpad for char
df_FcltyClmPK_final = df_FcltyClmPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.rpad(F.col("FCLTY_CLM_ADMS_SRC_CD"),2," ").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.rpad(F.col("FCLTY_CLM_ADMS_TYP_CD"),10," ").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.rpad(F.col("FCLTY_CLM_BILL_CLS_CD"),10," ").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.rpad(F.col("FCLTY_CLM_BILL_FREQ_CD"),10," ").alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.rpad(F.col("FCLTY_CLM_DSCHG_STTUS_CD"),10," ").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.rpad(F.col("FCLTY_CLM_SUBTYP_CD"),10," ").alias("FCLTY_CLM_SUBTYP_CD"),
    F.rpad(F.col("FCLTY_CLM_PROC_BILL_METH_CD"),10," ").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.rpad(F.col("FCLTY_TYP_CD"),10," ").alias("FCLTY_TYP_CD"),
    F.rpad(F.col("IDS_GNRT_DRG_IN"),1," ").alias("IDS_GNRT_DRG_IN"),
    F.rpad(F.col("ADMS_DT"),10," ").alias("ADMS_DT"),
    F.rpad(F.col("BILL_STMNT_BEG_DT"),10," ").alias("BILL_STMNT_BEG_DT"),
    F.rpad(F.col("BILL_STMNT_END_DT"),10," ").alias("BILL_STMNT_END_DT"),
    F.rpad(F.col("DSCHG_DT"),10," ").alias("DSCHG_DT"),
    F.rpad(F.col("HOSP_BEG_DT"),10," ").alias("HOSP_BEG_DT"),
    F.rpad(F.col("HOSP_END_DT"),10," ").alias("HOSP_END_DT"),
    F.col("ADMS_HR"),
    F.col("DSCHG_HR"),
    F.col("HOSP_COV_DAYS"),
    F.col("LOS_DAYS"),
    F.col("ADM_PHYS_PROV_ID"),
    F.rpad(F.col("FCLTY_BILL_TYP_TX"),3," ").alias("FCLTY_BILL_TYP_TX"),
    F.rpad(F.col("GNRT_DRG_CD"),4," ").alias("GNRT_DRG_CD"),
    F.col("MED_RCRD_NO"),
    F.col("OTHER_PROV_ID_1"),
    F.col("OTHER_PROV_ID_2"),
    F.rpad(F.col("SUBMT_DRG_CD"),4," ").alias("SUBMT_DRG_CD"),
    F.rpad(F.col("NRMTV_DRG_CD"),4," ").alias("NRMTV_DRG_CD"),
    F.col("DRG_METHOD_CD")
)

write_files(
    df_FcltyClmPK_final,
    f"{adls_path}/key/FctsClmFcltyExtr.FctsClmFclty.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# The other output from Snapshot => "SnapShot" => goes to "Transformer" => output => "RowCount" => then to B_FCLTY_CLM
df_SnapShot = df_Snapshot_snap

df_Transformer_out = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID")
)

write_files(
    df_Transformer_out,
    f"{adls_path}/load/B_FCLTY_CLM.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)