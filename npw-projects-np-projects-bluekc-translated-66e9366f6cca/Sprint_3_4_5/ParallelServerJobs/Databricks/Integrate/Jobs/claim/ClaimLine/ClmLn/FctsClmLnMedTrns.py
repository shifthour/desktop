# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *******************************************************************************
# MAGIC @ Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC 	Transform extracted Claim Line data from Facets into a common format to 
# MAGIC 	be potentially merged with claim line records from other systems for the 
# MAGIC 	foreign keying process and ultimate load into IDS
# MAGIC 
# MAGIC PRIMARY SOURCE:
# MAGIC 	InFile param	- Sequential file containing extracted claim line 
# MAGIC 			  data to be transformed (../verified)
# MAGIC HASH FILES
# MAGIC          The following hash files are created in other process and are used in other processes besides this job
# MAGIC                 DO NOT CLEAR
# MAGIC                 hf_clm_fcts_reversal - do not clear
# MAGIC                hf_clm_nasco_dup_bypass - do not clear
# MAGIC                hf_clm_ln_dsalw_ln_amts do not clear
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC 	TmpOutFile param	- Sequential file (parameterized name) containing 
# MAGIC 			  transformed claim line data in common record format 
# MAGIC 			  (../common)
# MAGIC JOB PARAMETERS:
# MAGIC 
# MAGIC 	FilePath		- Directory path for sequential data files
# MAGIC 	InFile		- Name of sequential input file
# MAGIC 	TmpOutFile	- Name of sequential output file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 	Developer	Date		Comment
# MAGIC 	-----------------------------	---------------------------	-----------------------------------------------------------
# MAGIC 	Kevin Soderlund	03/09/2004	Originally Programmed
# MAGIC 	Kevin Soderlund	05/01/2004	Coding complete for release 1.0
# MAGIC 	Kevin Soderlund	06/28/2004	Updated with changes for release 1.1
# MAGIC                 Steph Goddard        07/22/2004             Added NullOptCode for provider, proc codes
# MAGIC 	Kevin Soderlund	09/01/2004	Added claim type to metadata
# MAGIC                 Ralph Tucker          09/27/2004             Took out Claim_ID:"-":Claim_Ln_Seq in "GetFkeyCodes Fk check in Stage Variables.  Replaces with: GetFkeyCodes(SrcSysCd, 1, "CLAIM LINE ROOM TYPE", ClmLnRoomTypCd, "X")
# MAGIC                 Ralph Tucker          10/22/2004              Added hashfiles and logic to handle Disallow & Writeoffs from new tables for 2.1 version of IDS.
# MAGIC                 Steph/Brent            12/08/2004              Changed ID to AMT for  ovrMamt
# MAGIC                 Brent Leland            12/14/2004             Moved SQL to load hashfiles to FctsClmExtr job.
# MAGIC                 Sharon Andrew        2/1/2005               IDS 3.0    removed hf_cdml_cob and hf_dcmll_msupp becuase they were used to populate the COB_SAV_AMT AND COB_OTHR_CAR_PD_AMT which were removed from CLM_LN
# MAGIC                                                  3/22/2005             changed service provider rule to 
# MAGIC                                                                                                 If Index('15_93_97_99', trim(FctsClmLn.CDML_CUR_STS), 1) > 0  then  
# MAGIC                                                                                                       If      (GetFkeyProv("FACETS", 0,NullCode(FctsClmLn.PRPR_ID), "X") = 0) then "NA"           
# MAGIC                                                                                                       Else NullCode(FctsClmLn.PRPR_ID)
# MAGIC                                                                                                Else NullCode(FctsClmLn.PRPR_ID)
# MAGIC                 BJ Luce                 3/2/2006               change logic to calculate dsalw_amt, no_resp_amt, pat_resp_amt, prov_wrt_off_amt
# MAGIC                                                                              add new fields to  for clm_ln_svc_loc_typ_cd_sk and non_par_sav_amt
# MAGIC                                                                              add claim line primary key process
# MAGIC                                                                              put output to /key
# MAGIC                  BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC                 Steph Goddard  03/31/2006     changed name of CDML_NON_COV_CHG to CDML_OOP_CALC_BASE and moved to MBR_LIAB_BSS_AMT
# MAGIC                                                                   changed calculation for Non Par Savings Amount to use CDML_CONSIDER_CHG_AMT not CDDL_CHG_AMT
# MAGIC                 Sanderw  12/08/2006   Project 1576  - Reversal logix added for new status codes 89 and  and 99
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen         08/08/2007                                      Added Exract of SnapShot File                                                   devlIDS30                      Steph Goddard           8/30/07
# MAGIC 
# MAGIC Parik                         2008-08-05      3567(Primary Key)  Added the new primary keying process                                       devlIDS                          Steph Goddard           08/12/2008 
# MAGIC  
# MAGIC Rick Henry              2012-05-02        4896                      Added Proc_Cd_Typ_Cd and Proc_Cd_Cat_Cd                        NewDevl                        SAndrew                      2012-05-18
# MAGIC                                                                                         for Proc_Cd_Sk lookup
# MAGIC Kalyan Neelam          2013-01-28    4963 VBB Phase 3  Added 3 new columns on end - VBB_RULE_ID,                         IntegrateNewDevl        Bhoomi Dasari             3/14/2013
# MAGIC                                                                                                                       VBB_EXCD_ID, CLM_LN_VBB_IN
# MAGIC 
# MAGIC Manasa Andru            2014-10-17     TFS - 9580            Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and             IntegrateCurDevl              Kalyan Neelam            2014-10-22
# MAGIC                                                                                                        ITS_SRCHRG_AMT) at the end.
# MAGIC 
# MAGIC Manasa Andru        2014-11-17   TFS - 9580(Post Prod Work)        Added scale of 2 for the 2 new fields                        IntegrateCurDevl             Kalyan Neelam            2014-11-18
# MAGIC                                                                                     (ITS_SUPLMT_DSCNT_AMT and ITS_SRCHRG_AMT) 
# MAGIC Raja Gummadi        2015-05-05         TFS-1242             Added Proc Cd and Proc Cd Typ Cd look ups                            IntegrateNewDevl           Kalyan Neelam            2015-05-05/2015-09-11
# MAGIC 
# MAGIC Manasa Andru       2015-04-22        TFS - 12493         Updated the fields - CLM_LN_DSALW_EXCD and                      IntegrateDev2                 Jag Yelavarthi               2016-05-03
# MAGIC                                                                                   CLM_LN_EOB_EXCD to No Upcase so that the fields would find
# MAGIC                                                                                          a match on the EXCD table.
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,      IntegrateDev1                  Kalyan Neelam          2017-08-29
# MAGIC                                                                                     NDC_UNIT_CT) at the end
# MAGIC Jaideep Mankala     2017-11-20      5828 	   Added new field to identify MED / PDX claim                                 IntegrateDev2                 Kalyan Neelam           2017-11-20
# MAGIC                                                       		when passing to Fkey job
# MAGIC 
# MAGIC Akhila M                 2018-02-22    TFS-21100               CLM_ID Field logic -  removed trim function  in                             IntegrateDev2                  Hugh Sisson               2018-02-27
# MAGIC                                                                                      ApplyBusinessRules Transformer                       
# MAGIC 
# MAGIC Madhavan B          2018-02-06        5792 	     Changed the datatype of the column                                            IntegrateDev1                 Kalyan Neelam           2018-02-08
# MAGIC                                                       		     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Rekha R            2020-10-15  Phase II Governmant Programs Added APC_ID and APC_STTUS_ID                                   IntegrateDev2             Kalyan Neelam           2020-12-10
# MAGIC 
# MAGIC Arpitha V            2023-07-31       US 589700             Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with          IntegrateDevB	Harsha Ravuri	2023-08-31
# MAGIC                                                                                a default value in ApplyBusinessRules stage and mapped it till target

# MAGIC Apply transformations to extracted claim line data based on business rules and format output into a common record format to be able for merging with data from other systems for pimary keying and ultimate load into IDS
# MAGIC This container is used in:
# MAGIC ArgusClmLnExtr
# MAGIC PCSClmLnExtr
# MAGIC           FctsClmLnDntlTrns          FctsClmLnMedTrns
# MAGIC FctsClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC PCTAClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC Added APCD_ID & APSI_STS_IND  columns as part of 310395
# MAGIC Sequential file (parameterized name) containing transformed claim line data in common record format (../key)
# MAGIC Hash file created in the FctsClmLnHashFileExtr are also used in FctsClmLnDntlTrns, FctsClmLnMedExtr and FctsDntlClmLineExtr
# MAGIC 
# MAGIC hf_clm_ln_dsalw_ln_amts
# MAGIC 
# MAGIC Do Not Clear in this process
# MAGIC Sequential file (parameterized name) containing extracted claim line data to be transformed (../verified)
# MAGIC 
# MAGIC Created in FctsClmLnExtr
# MAGIC Hash files created in the FctsClmLnMedExtr are also used in FctsClmLnDntlTrns.
# MAGIC 
# MAGIC Do Not Clear in this process
# MAGIC created in FctsClmLnMedExtr used in FctsClmLnMedTrns 
# MAGIC 
# MAGIC hf_clm_ln_subtype
# MAGIC 
# MAGIC hash files created in FctsClmLnMedExtr and used in both FctsClmLnMedTrns and FctsClmLnDntlTrns, 
# MAGIC 
# MAGIC hf_clm_ln_cdmd_provcntrct
# MAGIC hf_clm_ln_chlp
# MAGIC hf_clm_clor
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# Retrieve job parameters
CurrRunCycle = get_widget_value("CurrRunCycle","1")
RunID = get_widget_value("RunID","20120425")
CurrentDate = get_widget_value("CurrentDate","20120425")
SrcSysCdSk = get_widget_value("SrcSysCdSk","FACETS")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# 1) READ FROM HASHED FILE STAGES (SCENARIO C UNLESS REMOVED BY SCENARIO A)

# hf_clm_fcts_reversals --> TWO OUTPUTS: fcts_reversals, RefSubTyp
df_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_RefSubTyp = spark.read.parquet(f"{adls_path}/hf_clm_ln_subtype.parquet")

# clm_nasco_dup_bypass --> OUTPUT: nasco_dup_lkup
df_nasco_dup_lkup = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Lookups on hf_clm_ln_dsalw_ln_amts --> 9 outputs in DataStage; all come from the same hashed file
df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_ln_amts.parquet")

# Hashed_File_270 --> OUTPUTS: RefPvdCntrct, RefClhp, clor_lkup
df_RefPvdCntrct = spark.read.parquet(f"{adls_path}/hf_clm_ln_cdmd_provcntrct.parquet")
df_RefClhp = spark.read.parquet(f"{adls_path}/hf_clm_ln_clhp.parquet")
df_clor_lkup = spark.read.parquet(f"{adls_path}/hf_clm_ln_clor.parquet")

# 2) READ .DAT FILE: seqVerified => FctsClmLn
# Define the schema for the .dat file (no header, quoteChar = ", assume delimiter=",")
schema_seqVerified = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CDML_SEQ_NO", StringType(), True),
    StructField("CLM_STS", StringType(), True),
    StructField("CDML_AG_PRICE", StringType(), True),
    StructField("CDML_ALLOW", StringType(), True),
    StructField("CDML_AUTHSV_SEQ_NO", StringType(), True),
    StructField("CDML_CAP_IND", StringType(), True),
    StructField("CDML_CHG_AMT", StringType(), True),
    StructField("CDML_CL_NTWK_IND", StringType(), True),
    StructField("CDML_COINS_AMT", StringType(), True),
    StructField("CDML_CONSIDER_CHG", StringType(), True),
    StructField("CDML_COPAY_AMT", StringType(), True),
    StructField("CDML_CUR_STS", StringType(), True),
    StructField("CDML_DED_ACC_NO", StringType(), True),
    StructField("CDML_DED_AMT", StringType(), True),
    StructField("CDML_DISALL_AMT", StringType(), True),
    StructField("CDML_DISALL_EXCD", StringType(), True),
    StructField("CDML_EOB_EXCD", StringType(), True),
    StructField("CDML_FROM_DT", StringType(), True),
    StructField("CDML_IP_PRICE", StringType(), True),
    StructField("CDML_ITS_DISC_AMT", StringType(), True),
    StructField("CDML_OOP_CALC_BASE", StringType(), True),
    StructField("CDML_PAID_AMT", StringType(), True),
    StructField("CDML_PC_IND", StringType(), True),
    StructField("CDML_PF_PRICE", StringType(), True),
    StructField("CDML_PR_PYMT_AMT", StringType(), True),
    StructField("CDML_PRE_AUTH_IND", StringType(), True),
    StructField("CDML_PRICE_IND", StringType(), True),
    StructField("CDML_REF_IND", StringType(), True),
    StructField("CDML_REFSV_SEQ_NO", StringType(), True),
    StructField("CDML_RISK_WH_AMT", StringType(), True),
    StructField("CDML_ROOM_TYPE", StringType(), True),
    StructField("CDML_SB_PYMT_AMT", StringType(), True),
    StructField("CDML_SE_PRICE", StringType(), True),
    StructField("CDML_SUP_DISC_AMT", StringType(), True),
    StructField("CDML_TO_DT", StringType(), True),
    StructField("CDML_UMAUTH_ID", StringType(), True),
    StructField("CDML_UMREF_ID", StringType(), True),
    StructField("CDML_UNITS", StringType(), True),
    StructField("CDML_UNITS_ALLOW", StringType(), True),
    StructField("CLM_LN_FINL_DISP_CD", StringType(), True),
    StructField("DEDE_PFX", StringType(), True),
    StructField("IPCD_ID", StringType(), True),
    StructField("LOBD_ID", StringType(), True),
    StructField("LTLT_PFX", StringType(), True),
    StructField("PDVC_LOBD_PTR", StringType(), True),
    StructField("PRPR_ID", StringType(), True),
    StructField("PSCD_ID", StringType(), True),
    StructField("RCRC_ID", StringType(), True),
    StructField("SEPC_PRICE_ID", StringType(), True),
    StructField("SEPY_PFX", StringType(), True),
    StructField("SESE_ID", StringType(), True),
    StructField("SESE_RULE", StringType(), True),
    StructField("CLCL_CL_SUB_TYPE", StringType(), True),
    StructField("CLCL_NTWK_IND", StringType(), True),
    StructField("TPCT_MCTR_SETG", StringType(), True),
    StructField("CDML_RESP_CD", StringType(), True),
    StructField("EXCD_FOUND", StringType(), True),
    StructField("IPCD_TYPE", StringType(), True),
    StructField("VBBD_RULE", StringType(), True),
    StructField("CDSD_VBB_EXCD_ID", StringType(), True),
    StructField("CLM_LN_VBB_IN", StringType(), True),
    StructField("ITS_SUPLMT_DSCNT", StringType(), True),
    StructField("ITS_SRCHRG_AMT", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("NDC_DRUG_FORM_CD", StringType(), True),
    StructField("NDC_UNIT_CT", StringType(), True),
    StructField("APCD_ID", StringType(), True),
    StructField("APSI_STS_IND", StringType(), True),
])
df_FctsClmLn = (
    spark.read.format("csv")
    .schema(schema_seqVerified)
    .option("header", "false")
    .option("quote", '"')
    .option("sep", ",")
    .load(f"{adls_path}/verified/FctsClmLnMedExtr.ClmLnMed.dat.{RunID}")
)

# 3) READ FROM "PROC_CD" DB2 CONNECTOR => SKIP THE HASHED FILE WRITE (SCENARIO A),
#    DIRECTLY PASS TO NEXT STAGE
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT PROC_CD, PROC_CD_TYP_CD FROM {IDSOwner}.PROC_CD WHERE PROC_CD_CAT_CD = 'MED'"
    )
    .load()
)

# ------------------------------------------------------------------------
# AGGREGATOR: sum_all_for_clm_ln => input link "sum_to_clm_ln" from hf_clm_ln_dsalw_ln_amts
# We replicate aggregator's group-by on CLCL_ID, CDML_SEQ_NO => sum(DSALW_AMT)
# This aggregator output was going into "total_dsalw" hashed file, which is scenario A
# so we skip writing and reading the hashed file, pass directly to "ApplyBusinessRules".
df_sum_to_clm_ln = df_hf_clm_ln_dsalw_ln_amts.alias("sum_to_clm_ln")
df_clm_ln_sum = (
    df_sum_to_clm_ln.groupBy("CLCL_ID","CDML_SEQ_NO")
    .agg(F.sum(F.col("DSALW_AMT")).alias("DSALW_AMT"))
)

# ------------------------------------------------------------------------
# APPLYBUSINESSRULES => CTransformerStage with many input links (left joins)
# We unify them into one chain of left joins. Each reference alias matches the stage link name.

# We'll alias each hashed-file or aggregator read DataFrame for clarity:
df_RefMY = df_hf_clm_ln_dsalw_ln_amts.alias("RefMY")
df_RefMN = df_hf_clm_ln_dsalw_ln_amts.alias("RefMN")
df_RefNY = df_hf_clm_ln_dsalw_ln_amts.alias("RefNY")
df_RefNN = df_hf_clm_ln_dsalw_ln_amts.alias("RefNN")
df_RefOY = df_hf_clm_ln_dsalw_ln_amts.alias("RefOY")
df_RefON = df_hf_clm_ln_dsalw_ln_amts.alias("RefON")
df_RefPY = df_hf_clm_ln_dsalw_ln_amts.alias("RefPY")
df_RefPN = df_hf_clm_ln_dsalw_ln_amts.alias("RefPN")

df_RefPvdCntrct = df_RefPvdCntrct.alias("RefPvdCntrct")
df_RefClhp = df_RefClhp.alias("RefClhp")
df_clor_lkup = df_clor_lkup.alias("clor_lkup")
df_total_dsalw = df_clm_ln_sum.alias("total_dsalw_lkup_for_cap")

# Two references to the same PROC_CD DF to replicate "ProcCdTypCd" and "Proc_cd"
df_ProcCdTypCd = df_PROC_CD.alias("ProcCdTypCd")
df_Proc_cd = df_PROC_CD.alias("Proc_cd")

df_FctsClmLn_alias = df_FctsClmLn.alias("FctsClmLn")

df_enriched = (
    df_FctsClmLn_alias
    # RefMY
    .join(
        df_RefMY,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefMY.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefMY.CDML_SEQ_NO"))
            & (F.lit("M") == F.col("RefMY.EXCD_RESP_CD"))
            & (F.lit("Y") == F.col("RefMY.BYPS_IN"))
        ),
        how="left",
    )
    # RefMN
    .join(
        df_RefMN,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefMN.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefMN.CDML_SEQ_NO"))
            & (F.lit("M") == F.col("RefMN.EXCD_RESP_CD"))
            & (F.lit("N") == F.col("RefMN.BYPS_IN"))
        ),
        how="left",
    )
    # RefNY
    .join(
        df_RefNY,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefNY.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefNY.CDML_SEQ_NO"))
            & (F.lit("N") == F.col("RefNY.EXCD_RESP_CD"))
            & (F.lit("Y") == F.col("RefNY.BYPS_IN"))
        ),
        how="left",
    )
    # RefNN
    .join(
        df_RefNN,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefNN.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefNN.CDML_SEQ_NO"))
            & (F.lit("N") == F.col("RefNN.EXCD_RESP_CD"))
            & (F.lit("N") == F.col("RefNN.BYPS_IN"))
        ),
        how="left",
    )
    # RefOY
    .join(
        df_RefOY,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefOY.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefOY.CDML_SEQ_NO"))
            & (F.lit("O") == F.col("RefOY.EXCD_RESP_CD"))
            & (F.lit("Y") == F.col("RefOY.BYPS_IN"))
        ),
        how="left",
    )
    # RefON
    .join(
        df_RefON,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefON.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefON.CDML_SEQ_NO"))
            & (F.lit("O") == F.col("RefON.EXCD_RESP_CD"))
            & (F.lit("N") == F.col("RefON.BYPS_IN"))
        ),
        how="left",
    )
    # RefPY
    .join(
        df_RefPY,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefPY.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefPY.CDML_SEQ_NO"))
            & (F.lit("P") == F.col("RefPY.EXCD_RESP_CD"))
            & (F.lit("Y") == F.col("RefPY.BYPS_IN"))
        ),
        how="left",
    )
    # RefPN
    .join(
        df_RefPN,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefPN.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefPN.CDML_SEQ_NO"))
            & (F.lit("P") == F.col("RefPN.EXCD_RESP_CD"))
            & (F.lit("N") == F.col("RefPN.BYPS_IN"))
        ),
        how="left",
    )
    # RefPvdCntrct
    .join(
        df_RefPvdCntrct,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("RefPvdCntrct.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("RefPvdCntrct.CDML_SEQ_NO"))
        ),
        how="left",
    )
    # RefClhp
    .join(
        df_RefClhp,
        F.col("FctsClmLn.CLCL_ID") == F.col("RefClhp.CLCL_ID"),
        how="left",
    )
    # clor_lkup
    .join(
        df_clor_lkup,
        F.col("FctsClmLn.CLCL_ID") == F.col("clor_lkup.CLM_ID"),
        how="left",
    )
    # total_dsalw_lkup_for_cap => from aggregator
    .join(
        df_total_dsalw,
        (
            (F.col("FctsClmLn.CLCL_ID") == F.col("total_dsalw_lkup_for_cap.CLCL_ID"))
            & (F.col("FctsClmLn.CDML_SEQ_NO") == F.col("total_dsalw_lkup_for_cap.CDML_SEQ_NO"))
        ),
        how="left",
    )
    # ProcCdTypCd
    .join(
        df_ProcCdTypCd,
        F.col("FctsClmLn.IPCD_ID") == F.col("ProcCdTypCd.PROC_CD"),
        how="left",
    )
    # Proc_cd with condition on Trim(Left(FctsClmLn.IPCD_ID,5))
    .join(
        df_Proc_cd,
        F.trim(F.substring(F.col("FctsClmLn.IPCD_ID"), 1, 5)) == F.col("Proc_cd.PROC_CD"),
        how="left",
    )
)

# Now define the stage variables in PySpark columns, step-by-step.
# We'll do a sequence of withColumn to replicate each logic.

df_enriched = (
    df_enriched
    .withColumn("SrcSysCd", F.lit("FACETS"))
    .withColumn(
        "ClmId",
        F.when(F.trim(F.col("FctsClmLn.CLCL_ID")) != "", F.col("FctsClmLn.CLCL_ID"))
         .otherwise(F.lit("NA"))
    )
    .withColumn(
        "ClmLnId",
        F.when(
            (F.col("FctsClmLn.CDML_SEQ_NO").isNotNull())
            & (F.trim(F.col("FctsClmLn.CDML_SEQ_NO")) != ""),
            F.col("FctsClmLn.CDML_SEQ_NO")
        ).otherwise(F.lit("0"))
    )
    .withColumn(
        "ClmLnRoomTypCd",
        F.when(
            F.isnull(F.substring(F.trim(F.col("FctsClmLn.RCRC_ID")),1,3))) |
            (F.length(F.trim(F.substring(F.col("FctsClmLn.RCRC_ID"),1,3))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.substring(F.col("FctsClmLn.RCRC_ID"),1,3))))
    )
    .withColumn(
        "ClmLnRoomTypCdLkup",
        F.expr('GetFkeyCodes("FACETS", 1, "CLAIM LINE ROOM TYPE", ClmLnRoomTypCd, "X")')
    )
    .withColumn(
        "ServicingProvider",
        F.when(
            F.instr(F.lit("15_93_97_99"), F.trim(F.col("FctsClmLn.CDML_CUR_STS"))) > 0,
            F.when(
                F.expr('GetFkeyProv("FACETS", 0, (CASE WHEN (FctsClmLn.PRPR_ID IS NULL OR length(trim(FctsClmLn.PRPR_ID))=0) THEN "UNK" ELSE upper(trim(FctsClmLn.PRPR_ID)) END), "X")')==F.lit(0),
                F.lit("NA")
            ).otherwise(
                F.when(
                    (F.col("FctsClmLn.PRPR_ID").isNull()) | (F.length(F.trim(F.col("FctsClmLn.PRPR_ID"))) == 0),
                    F.lit("UNK")
                ).otherwise(F.upper(F.trim(F.col("FctsClmLn.PRPR_ID"))))
            )
        ).otherwise(
            F.when(
                (F.col("FctsClmLn.PRPR_ID").isNull()) | (F.length(F.trim(F.col("FctsClmLn.PRPR_ID"))) == 0),
                F.lit("UNK")
            ).otherwise(F.upper(F.trim(F.col("FctsClmLn.PRPR_ID"))))
        )
    )
    .withColumn(
        "svSeseId",
        F.when(
            (F.col("FctsClmLn.SESE_ID").isNull()) | (F.length(F.trim(F.col("FctsClmLn.SESE_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("FctsClmLn.SESE_ID"))))
    )
    .withColumn(
        "svRvnuCd",
        F.when(
            (F.col("FctsClmLn.RCRC_ID").isNull()) | (F.length(F.trim(F.col("FctsClmLn.RCRC_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("FctsClmLn.RCRC_ID"))))
    )
    .withColumn(
        "svNtwkO",
        F.when(F.trim(F.col("FctsClmLn.CDML_CL_NTWK_IND")) == "O", F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svNtwkIP",
        F.when(F.trim(F.col("FctsClmLn.CDML_CL_NTWK_IND")) == "I", F.lit("Y"))
         .otherwise(
             F.when(F.trim(F.col("FctsClmLn.CDML_CL_NTWK_IND")) == "P", F.lit("Y")).otherwise(F.lit("N"))
         )
    )
    .withColumn(
        "svClmSubType",
        F.when(
            (F.col("FctsClmLn.CLCL_CL_SUB_TYPE")=="M") | (F.col("FctsClmLn.CLCL_CL_SUB_TYPE")=="D"),
            F.lit("P")
        ).otherwise(
            F.when(
                F.col("FctsClmLn.CLCL_CL_SUB_TYPE")=="H",
                F.when(F.col("RefClhp.CLHP_FAC_TYPE")>6, F.lit("O"))
                 .otherwise(
                     F.when(
                         (F.col("RefClhp.CLHP_FAC_TYPE")<7)
                         & ((F.col("RefClhp.CLHP_BILL_CLASS")==3) | (F.col("RefClhp.CLHP_BILL_CLASS")==4)),
                         F.lit("O")
                     ).otherwise(F.lit("I"))
                 )
            ).otherwise(F.lit("UNK"))
        )
    )
    .withColumn(
        "svN",
        F.when(
            F.col("RefNY.CLCL_ID").isNotNull(), F.lit("Y")
        ).otherwise(
            F.when(
                (F.col("RefMY.CLCL_ID").isNotNull() | F.col("RefOY.CLCL_ID").isNotNull() | F.col("RefPY.CLCL_ID").isNotNull()), 
                F.lit("O")
            ).otherwise(
                F.when(
                    F.col("RefNN.CLCL_ID").isNotNull(), F.lit("N")
                ).otherwise(F.lit("X"))
            )
        )
    )
    .withColumn(
        "svM",
        F.when(
            F.col("RefMY.CLCL_ID").isNotNull(), F.lit("Y")
        ).otherwise(
            F.when(
                (F.col("RefNY.CLCL_ID").isNotNull() | F.col("RefOY.CLCL_ID").isNotNull() | F.col("RefPY.CLCL_ID").isNotNull()),
                F.lit("O")
            ).otherwise(
                F.when(F.col("RefMN.CLCL_ID").isNotNull(), F.lit("N")).otherwise(F.lit("X"))
            )
        )
    )
    .withColumn(
        "svO",
        F.when(
            F.col("RefOY.CLCL_ID").isNotNull(), F.lit("Y")
        ).otherwise(
            F.when(
                (F.col("RefNY.CLCL_ID").isNotNull() | F.col("RefMY.CLCL_ID").isNotNull() | F.col("RefPY.CLCL_ID").isNotNull()),
                F.lit("O")
            ).otherwise(
                F.when(F.col("RefON.CLCL_ID").isNotNull(), F.lit("N")).otherwise(F.lit("X"))
            )
        )
    )
    .withColumn(
        "svP",
        F.when(
            F.col("RefPY.CLCL_ID").isNotNull(), F.lit("Y")
        ).otherwise(
            F.when(
                (F.col("RefNY.CLCL_ID").isNotNull() | F.col("RefOY.CLCL_ID").isNotNull() | F.col("RefMY.CLCL_ID").isNotNull()),
                F.lit("O")
            ).otherwise(
                F.when(F.col("RefPN.CLCL_ID").isNotNull(), F.lit("N")).otherwise(F.lit("X"))
            )
        )
    )
    .withColumn(
        "svMaster",
        F.when(F.col("svN")==="Y", F.lit("N"))
        .otherwise(
            F.when(F.col("svP")==="Y", F.lit("P"))
             .otherwise(
                F.when(F.col("svM")==="Y", F.lit("M"))
                 .otherwise(
                    F.when(F.col("svO")==="Y", F.lit("O")).otherwise(F.lit("X"))
                 )
             )
        )
    )
    .withColumn("svTotalofAll", F.col("FctsClmLn.CDML_DISALL_AMT"))
    .withColumn(
        "svDefaultToProvWriteOff",
        F.when(
            F.col("clor_lkup.CLM_ID").isNull(), F.lit("N")
        ).otherwise(
            F.when(F.col("FctsClmLn.EXCD_FOUND")==="Y", F.lit("N")).otherwise(F.lit("Y"))
        )
    )
    .withColumn(
        "svNoRespAmt",
        F.when(
            F.col("svDefaultToProvWriteOff")==="Y", F.lit("0.00")
        ).otherwise(
            F.when(
                F.col("svMaster")==="N", F.col("svTotalofAll")
            ).otherwise(
                F.when(
                    F.col("svMaster")==="X",
                    F.when(
                        (F.col("RefNN.DSALW_AMT").isNull())
                        | (F.trim(F.col("RefNN.DSALW_AMT"))=="")
                        | (~F.expr("Num(RefNN.DSALW_AMT)")),
                        F.lit("0")
                    ).otherwise(F.col("RefNN.DSALW_AMT"))
                ).otherwise(F.lit("0.00"))
            )
        )
    )
    .withColumn(
        "svPatRespAmt",
        F.when(F.col("svDefaultToProvWriteOff")==="Y", F.lit("0.00"))
        .otherwise(
            F.when(
                (F.col("svMaster")==="M")
                | ((F.col("svMaster")==="O") & (F.col("svNtwkO")==="Y")),
                F.col("svTotalofAll")
            ).otherwise(
                (
                    F.when(
                        F.col("svMaster")==="X",
                        F.when(
                            (F.col("RefMN.DSALW_AMT").isNull())
                            | (F.trim(F.col("RefMN.DSALW_AMT"))=="")
                            | (~F.expr("Num(RefMN.DSALW_AMT)")),
                            F.lit("0")
                        ).otherwise(F.col("RefMN.DSALW_AMT"))
                    ).otherwise(F.lit("0.0"))
                )
                + (
                    F.when(
                        (F.col("svNtwkO")==="Y")
                        & (F.col("svMaster")==="X"),
                        F.when(
                            (F.col("RefON.DSALW_AMT").isNull())
                            | (F.trim(F.col("RefON.DSALW_AMT"))=="")
                            | (~F.expr("Num(RefON.DSALW_AMT)")),
                            F.lit("0")
                        ).otherwise(F.col("RefON.DSALW_AMT"))
                    ).otherwise(F.lit("0.00"))
                )
            )
        )
    )
    .withColumn(
        "svProvWriteOff",
        F.when(F.col("svDefaultToProvWriteOff")==="Y", F.col("svTotalofAll"))
        .otherwise(
            F.when(
                (F.col("svMaster")==="P")
                | ((F.col("svMaster")==="O") & (F.col("svNtwkIP")==="Y")),
                F.col("svTotalofAll")
            ).otherwise(
                (
                    F.when(
                        F.col("svMaster")==="X",
                        F.when(
                            (F.col("RefPN.DSALW_AMT").isNull())
                            | (F.trim(F.col("RefPN.DSALW_AMT"))=="")
                            | (~F.expr("Num(RefPN.DSALW_AMT)")),
                            F.lit("0")
                        ).otherwise(F.col("RefPN.DSALW_AMT"))
                    ).otherwise(F.lit("0.0"))
                )
                + (
                    F.when(
                        (F.col("svNtwkIP")==="Y")
                        & (F.col("svMaster")==="X"),
                        F.when(
                            (F.col("RefON.DSALW_AMT").isNull())
                            | (F.trim(F.col("RefON.DSALW_AMT"))=="")
                            | (~F.expr("Num(RefON.DSALW_AMT)")),
                            F.lit("0")
                        ).otherwise(F.col("RefON.DSALW_AMT"))
                    ).otherwise(F.lit("0.00"))
                )
            )
        )
    )
    .withColumn(
        "svCapClm",
        F.when(
            (F.col("FctsClmLn.CDML_CAP_IND")==="Y")
            & (F.col("FctsClmLn.CDML_CONSIDER_CHG") != F.col("FctsClmLn.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCapAmt",
        F.when(
            F.col("total_dsalw_lkup_for_cap.CLCL_ID").isNull(),
            F.lit("0")
        ).otherwise(F.col("total_dsalw_lkup_for_cap.DSALW_AMT"))
    )
    .withColumn(
        "svProcCd",
        F.when(
            F.col("ProcCdTypCd.PROC_CD").isNull(),
            F.when(
                F.col("Proc_cd.PROC_CD").isNull(),
                F.lit("NA")
            ).otherwise(F.col("Proc_cd.PROC_CD"))
        ).otherwise(F.col("ProcCdTypCd.PROC_CD"))
    )
)

# Finally select the output link columns => "ClmLnCrfnew"
df_ClmLnCrfnew = df_enriched.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("ClmLnId")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("CLM_LN_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("ClmLnId").alias("CLM_LN_SEQ_NO"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svProcCd").alias("PROC_CD"),
    F.col("ServicingProvider").alias("SVC_PROV_ID"),
    F.when(
        (F.col("FctsClmLn.CDML_DISALL_EXCD").isNull())|(F.trim(F.col("FctsClmLn.CDML_DISALL_EXCD"))==""),
        F.lit("NA")
    ).otherwise(F.trim(F.col("FctsClmLn.CDML_DISALL_EXCD"))).alias("CLM_LN_DSALW_EXCD"),
    F.when(
        (F.col("FctsClmLn.CDML_EOB_EXCD").isNull())|(F.trim(F.col("FctsClmLn.CDML_EOB_EXCD"))==""),
        F.lit("NA")
    ).otherwise(F.trim(F.col("FctsClmLn.CDML_EOB_EXCD"))).alias("CLM_LN_EOB_EXCD"),
    F.when(
        (F.col("FctsClmLn.CLM_LN_FINL_DISP_CD").isNull())|(F.trim(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD")))).alias("CLM_LN_FINL_DISP_CD"),
    F.when(
        (F.col("FctsClmLn.LOBD_ID").isNull())|(F.trim(F.col("FctsClmLn.LOBD_ID"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.LOBD_ID")))).alias("CLM_LN_LOB_CD"),
    F.when(
        (F.col("FctsClmLn.PSCD_ID").isNull())|(F.trim(F.col("FctsClmLn.PSCD_ID"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.PSCD_ID")))).alias("CLM_LN_POS_CD"),
    F.when(
        (F.col("FctsClmLn.CDML_PC_IND").isNull())|(F.trim(F.col("FctsClmLn.CDML_PC_IND"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.CDML_PC_IND")))).alias("CLM_LN_PREAUTH_CD"),
    F.when(
        (F.col("FctsClmLn.CDML_PRE_AUTH_IND").isNull())|(F.trim(F.col("FctsClmLn.CDML_PRE_AUTH_IND"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.CDML_PRE_AUTH_IND")))).alias("CLM_LN_PREAUTH_SRC_CD"),
    F.when(
        (F.col("FctsClmLn.CDML_PRICE_IND").isNull())|(F.trim(F.col("FctsClmLn.CDML_PRICE_IND"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.CDML_PRICE_IND")))).alias("CLM_LN_PRICE_SRC_CD"),
    F.when(
        (F.col("FctsClmLn.CDML_REF_IND").isNull())|(F.trim(F.col("FctsClmLn.CDML_REF_IND"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.CDML_REF_IND")))).alias("CLM_LN_RFRL_CD"),
    F.when(F.length(F.col("svRvnuCd"))==3, F.lit("0").concat(F.col("svRvnuCd"))).otherwise(F.col("svRvnuCd")).alias("CLM_LN_RVNU_CD"),
    F.when(
        (F.col("FctsClmLn.CDML_ROOM_TYPE").isNull())|(F.trim(F.col("FctsClmLn.CDML_ROOM_TYPE"))==""),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("FctsClmLn.CDML_ROOM_TYPE")))).alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.when(
        F.expr("ClmLnRoomTypCdLkup=0"),
        F.lit("NA")
    ).otherwise(F.col("ClmLnRoomTypCd")).alias("CLM_LN_ROOM_TYP_CD"),
    F.col("svSeseId").alias("CLM_LN_TOS_CD"),
    F.when(
        F.substring(F.col("FctsClmLn.SESE_ID"),1,2)=="AN", 
        F.lit("MJ")
    ).otherwise(F.lit("UN")).alias("CLM_LN_UNIT_TYP_CD"),
    F.when(F.trim(F.col("FctsClmLn.CDML_CAP_IND"))=="Y", F.lit("Y")).otherwise(F.lit("N")).alias("CAP_LN_IN"),
    F.when(F.trim(F.col("FctsClmLn.PDVC_LOBD_PTR"))=="1", F.lit("Y")).otherwise(F.lit("N")).alias("PRI_LOB_IN"),
    F.when(
        (F.col("FctsClmLn.CDML_TO_DT").isNull()) | (F.trim(F.col("FctsClmLn.CDML_TO_DT"))==""),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("FctsClmLn.CDML_TO_DT")),1,10)).alias("SVC_END_DT"),
    F.when(
        (F.col("FctsClmLn.CDML_FROM_DT").isNull()) | (F.trim(F.col("FctsClmLn.CDML_FROM_DT"))==""),
        F.lit("UNK")
    ).otherwise(F.substring(F.trim(F.col("FctsClmLn.CDML_FROM_DT")),1,10)).alias("SVC_STRT_DT"),
    F.col("FctsClmLn.CDML_AG_PRICE").alias("AGMNT_PRICE_AMT"),
    F.col("FctsClmLn.CDML_ALLOW").alias("ALW_AMT"),
    F.when(
        F.col("svSeseId") != "*RG", F.col("FctsClmLn.CDML_CHG_AMT")
    ).otherwise(F.lit("0")).alias("CHRG_AMT"),
    F.col("FctsClmLn.CDML_COINS_AMT").alias("COINS_AMT"),
    F.col("FctsClmLn.CDML_CONSIDER_CHG").alias("CNSD_CHRG_AMT"),
    F.col("FctsClmLn.CDML_COPAY_AMT").alias("COPAY_AMT"),
    F.col("FctsClmLn.CDML_DED_AMT").alias("DEDCT_AMT"),
    F.col("FctsClmLn.CDML_DISALL_AMT").alias("DSALW_AMT"),
    F.col("FctsClmLn.CDML_ITS_DISC_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.when(
        F.col("svCapClm")==="Y", F.lit("0.0")
    ).otherwise(F.col("svNoRespAmt")).alias("NO_RESP_AMT"),
    F.col("FctsClmLn.CDML_OOP_CALC_BASE").alias("MBR_LIAB_BSS_AMT"),
    F.when(
        F.col("svCapClm")==="Y", F.lit("0.0")
    ).otherwise(F.col("svPatRespAmt")).alias("PATN_RESP_AMT"),
    F.col("FctsClmLn.CDML_PAID_AMT").alias("PAYBL_AMT"),
    F.col("FctsClmLn.CDML_PR_PYMT_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("FctsClmLn.CDML_SB_PYMT_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("FctsClmLn.CDML_IP_PRICE").alias("PROC_TBL_PRICE_AMT"),
    F.col("FctsClmLn.CDML_PF_PRICE").alias("PROFL_PRICE_AMT"),
    F.when(
        F.col("svCapClm")==="Y", F.col("svCapAmt")
    ).otherwise(F.col("svProvWriteOff")).alias("PROV_WRT_OFF_AMT"),
    F.col("FctsClmLn.CDML_RISK_WH_AMT").alias("RISK_WTHLD_AMT"),
    F.col("FctsClmLn.CDML_SE_PRICE").alias("SVC_PRICE_AMT"),
    F.col("FctsClmLn.CDML_SUP_DISC_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("FctsClmLn.CDML_UNITS_ALLOW").alias("ALW_PRICE_UNIT_CT"),
    F.col("FctsClmLn.CDML_UNITS").alias("UNIT_CT"),
    F.col("FctsClmLn.CDML_DED_ACC_NO").alias("DEDCT_AMT_ACCUM_ID"),
    F.when(
        (F.col("FctsClmLn.CDML_AUTHSV_SEQ_NO").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDML_AUTHSV_SEQ_NO")))==0),
        F.lit(" ")
    ).otherwise(F.col("FctsClmLn.CDML_AUTHSV_SEQ_NO")).alias("PREAUTH_SVC_SEQ_NO"),
    F.when(
        (F.col("FctsClmLn.CDML_REFSV_SEQ_NO").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDML_REFSV_SEQ_NO")))==0),
        F.lit(" ")
    ).otherwise(F.col("FctsClmLn.CDML_REFSV_SEQ_NO")).alias("RFRL_SVC_SEQ_NO"),
    F.when(
        (F.col("FctsClmLn.LTLT_PFX").isNull())|(F.length(F.trim(F.col("FctsClmLn.LTLT_PFX")))<1),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.LTLT_PFX")).alias("LMT_PFX_ID"),
    F.when(
        (F.col("FctsClmLn.CDML_UMAUTH_ID").isNull())|(F.length(F.trim(F.col("FctsClmLn.CDML_UMAUTH_ID")))==0),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.CDML_UMAUTH_ID")).alias("PREAUTH_ID"),
    F.when(
        (F.col("FctsClmLn.DEDE_PFX").isNull())|(F.length(F.trim(F.col("FctsClmLn.DEDE_PFX"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.DEDE_PFX")).alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.when(
        (F.col("FctsClmLn.SEPY_PFX").isNull())|(F.length(F.trim(F.col("FctsClmLn.SEPY_PFX"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.SEPY_PFX")).alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.when(
        (F.col("FctsClmLn.CDML_UMREF_ID").isNull())|(F.length(F.trim(F.col("FctsClmLn.CDML_UMREF_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.CDML_UMREF_ID")).alias("RFRL_ID_TX"),
    F.when(
        (F.col("FctsClmLn.SESE_ID").isNull())|(F.length(F.trim(F.col("FctsClmLn.SESE_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.SESE_ID")).alias("SVC_ID"),
    F.when(
        (F.col("FctsClmLn.SEPC_PRICE_ID").isNull())|(F.length(F.trim(F.col("FctsClmLn.SEPC_PRICE_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("FctsClmLn.SEPC_PRICE_ID")).alias("SVC_PRICE_RULE_ID"),
    F.when(F.length(F.trim(F.col("FctsClmLn.SESE_RULE")))<1, F.lit(None)).otherwise(F.trim(F.col("FctsClmLn.SESE_RULE"))).alias("SVC_RULE_TYP_TX"),
    F.lit(" ").alias("SVC_LOC_TYP_CD"),
    F.when(
        F.col("RefPvdCntrct.CDML_SEQ_NO").isNull(),
        F.lit("0.00")
    ).otherwise(
        F.when(F.col("FctsClmLn.CLCL_NTWK_IND")==="O",
               F.col("FctsClmLn.CDML_CONSIDER_CHG") - F.col("FctsClmLn.CDML_ALLOW"))
         .otherwise(F.lit("0.00"))
    ).alias("NON_PAR_SAV_AMT"),
    F.col("svClmSubType").alias("CLM_SUBTYPE"),
    F.col("FctsClmLn.TPCT_MCTR_SETG").alias("TPCT_MCTR_SETG"),
    F.when(
        F.col("ProcCdTypCd.PROC_CD").isNull(),
        F.when(
            F.col("Proc_cd.PROC_CD").isNull(),
            F.lit("NA")
        ).otherwise(F.col("Proc_cd.PROC_CD_TYP_CD"))
    ).otherwise(F.col("ProcCdTypCd.PROC_CD_TYP_CD")).alias("PROC_CD_TYP_CD"),
    F.lit("MED").alias("PROC_CD_CAT_CD"),
    F.col("FctsClmLn.VBBD_RULE").alias("VBB_RULE_ID"),
    F.col("FctsClmLn.CDSD_VBB_EXCD_ID").alias("VBB_EXCD_ID"),
    F.col("FctsClmLn.CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
    F.col("FctsClmLn.ITS_SUPLMT_DSCNT").alias("ITS_SUPLMT_DSCNT"),
    F.col("FctsClmLn.ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    F.col("FctsClmLn.NDC").alias("NDC"),
    F.col("FctsClmLn.NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
    F.col("FctsClmLn.NDC_UNIT_CT").alias("NDC_UNIT_CT"),
    F.col("FctsClmLn.APCD_ID").alias("APCD_ID"),
    F.col("FctsClmLn.APSI_STS_IND").alias("APSI_STS_IND"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

# ------------------------------------------------------------------------
# BUILD_REVERSAL_CLM => next Transformer with 4 input links (left joins) 
# Primary link = "ClmLnCrfnew" => df_ClmLnCrfnew
# Lookups: fcts_reversals, RefSubTyp, nasco_dup_lkup
df_ClmLnCrfnew_alias = df_ClmLnCrfnew.alias("ClmLnCrfnew")
df_fcts_reversals_alias = df_fcts_reversals.alias("fcts_reversals")
df_RefSubTyp_alias = df_RefSubTyp.alias("RefSubTyp")
df_nasco_dup_lkup_alias = df_nasco_dup_lkup.alias("nasco_dup_lkup")

df_Build_Reversal_Clm = (
    df_ClmLnCrfnew_alias
    .join(
        df_fcts_reversals_alias,
        F.col("ClmLnCrfnew.CLM_ID")==F.col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_RefSubTyp_alias,
        F.trim(F.col("ClmLnCrfnew.CLM_SUBTYPE")) == F.col("RefSubTyp.SRC_DRVD_LKUP_VAL"),
        how="left"
    )
    .join(
        df_nasco_dup_lkup_alias,
        F.col("ClmLnCrfnew.CLM_ID")==F.col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
    .withColumn(
        "svSvcLocTypCd",
        F.when(
            F.col("RefSubTyp.TRGT_CD").isNull(),
            F.lit("NA")
        ).otherwise(
            F.when(F.col("RefSubTyp.TRGT_CD")==="IP", F.lit("I"))
             .otherwise(
                F.when(F.col("RefSubTyp.TRGT_CD")==="OP", F.lit("O"))
                 .otherwise(F.col("ClmLnCrfnew.TPCT_MCTR_SETG"))
             )
        )
    )
)

# Two output links: claimLine (constraint: nasco_dup_lkup.CLM_ID isNull),
# reversal (constraint: fcts_reversals.CLCL_ID isNotNull AND in {89,91}).
# We produce the columns as listed.

common_cols_build_reversal = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROC_CD",
    "SVC_PROV_ID",
    "CLM_LN_DSALW_EXCD",
    "CLM_LN_EOB_EXCD",
    "CLM_LN_FINL_DISP_CD",
    "CLM_LN_LOB_CD",
    "CLM_LN_POS_CD",
    "CLM_LN_PREAUTH_CD",
    "CLM_LN_PREAUTH_SRC_CD",
    "CLM_LN_PRICE_SRC_CD",
    "CLM_LN_RFRL_CD",
    "CLM_LN_RVNU_CD",
    "CLM_LN_ROOM_PRICE_METH_CD",
    "CLM_LN_ROOM_TYP_CD",
    "CLM_LN_TOS_CD",
    "CLM_LN_UNIT_TYP_CD",
    "CAP_LN_IN",
    "PRI_LOB_IN",
    "SVC_END_DT",
    "SVC_STRT_DT",
    "AGMNT_PRICE_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ITS_HOME_DSCNT_AMT",
    "NO_RESP_AMT",
    "MBR_LIAB_BSS_AMT",
    "PATN_RESP_AMT",
    "PAYBL_AMT",
    "PAYBL_TO_PROV_AMT",
    "PAYBL_TO_SUB_AMT",
    "PROC_TBL_PRICE_AMT",
    "PROFL_PRICE_AMT",
    "PROV_WRT_OFF_AMT",
    "RISK_WTHLD_AMT",
    "SVC_PRICE_AMT",
    "SUPLMT_DSCNT_AMT",
    "ALW_PRICE_UNIT_CT",
    "UNIT_CT",
    "DEDCT_AMT_ACCUM_ID",
    "PREAUTH_SVC_SEQ_NO",
    "RFRL_SVC_SEQ_NO",
    "LMT_PFX_ID",
    "PREAUTH_ID",
    "PROD_CMPNT_DEDCT_PFX_ID",
    "PROD_CMPNT_SVC_PAYMT_ID",
    "RFRL_ID_TX",
    "SVC_ID",
    "SVC_PRICE_RULE_ID",
    "SVC_RULE_TYP_TX",
    "SVC_LOC_TYP_CD",
    "NON_PAR_SAV_AMT",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD",
    "VBB_RULE_ID",
    "VBB_EXCD_ID",
    "CLM_LN_VBB_IN",
    "ITS_SUPLMT_DSCNT",
    "ITS_SRCHRG_AMT",
    "NDC",
    "NDC_DRUG_FORM_CD",
    "NDC_UNIT_CT",
    "APCD_ID",
    "APSI_STS_IND",
    "SNOMED_CT_CD",
    "CVX_VCCN_CD"
]

df_claimLine = (
    df_Build_Reversal_Clm
    .filter(
        F.col("nasco_dup_lkup.CLM_ID").isNull()
    )
    .select(
        *[F.col(f"ClmLnCrfnew.{c}").alias(c) for c in common_cols_build_reversal if c not in ["SVC_LOC_TYP_CD"] ],
        F.col("svSvcLocTypCd").alias("SVC_LOC_TYP_CD")
    )
)

df_reversal = (
    df_Build_Reversal_Clm
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & ((F.col("fcts_reversals.CLCL_CUR_STS")==="89") | (F.col("fcts_reversals.CLCL_CUR_STS")==="91"))
    )
    .select(
        *[F.col(f"ClmLnCrfnew.{c}").alias(c) for c in common_cols_build_reversal if c not in ["CLM_ID","PRI_KEY_STRING","SVC_LOC_TYP_CD"] ],
        F.concat(F.trim(F.col("ClmLnCrfnew.SRC_SYS_CD")),F.lit(";"),F.col("ClmLnCrfnew.CLM_ID"),F.lit("R"),F.lit(";"),F.col("ClmLnCrfnew.CLM_LN_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.concat(F.col("ClmLnCrfnew.CLM_ID"),F.lit("R")).alias("CLM_ID"),
        F.col("svSvcLocTypCd").alias("SVC_LOC_TYP_CD")
    )
    .withColumn("AGMNT_PRICE_AMT", -F.col("AGMNT_PRICE_AMT").cast("double"))
    .withColumn("ALW_AMT", -F.col("ALW_AMT").cast("double"))
    .withColumn("CHRG_AMT", -F.col("CHRG_AMT").cast("double"))
    .withColumn("COINS_AMT", -F.col("COINS_AMT").cast("double"))
    .withColumn("CNSD_CHRG_AMT", -F.col("CNSD_CHRG_AMT").cast("double"))
    .withColumn("COPAY_AMT", -F.col("COPAY_AMT").cast("double"))
    .withColumn("DEDCT_AMT", -F.col("DEDCT_AMT").cast("double"))
    .withColumn("DSALW_AMT", -F.col("DSALW_AMT").cast("double"))
    .withColumn("ITS_HOME_DSCNT_AMT", -F.col("ITS_HOME_DSCNT_AMT").cast("double"))
    .withColumn("NO_RESP_AMT", -F.col("NO_RESP_AMT").cast("double"))
    .withColumn("MBR_LIAB_BSS_AMT", -F.col("MBR_LIAB_BSS_AMT").cast("double"))
    .withColumn("PATN_RESP_AMT", -F.col("PATN_RESP_AMT").cast("double"))
    .withColumn("PAYBL_AMT", -F.col("PAYBL_AMT").cast("double"))
    .withColumn("PAYBL_TO_PROV_AMT", -F.col("PAYBL_TO_PROV_AMT").cast("double"))
    .withColumn("PAYBL_TO_SUB_AMT", -F.col("PAYBL_TO_SUB_AMT").cast("double"))
    .withColumn("PROC_TBL_PRICE_AMT", -F.col("PROC_TBL_PRICE_AMT").cast("double"))
    .withColumn("PROFL_PRICE_AMT", -F.col("PROFL_PRICE_AMT").cast("double"))
    .withColumn("PROV_WRT_OFF_AMT", -F.col("PROV_WRT_OFF_AMT").cast("double"))
    .withColumn("RISK_WTHLD_AMT", -F.col("RISK_WTHLD_AMT").cast("double"))
    .withColumn("SVC_PRICE_AMT", -F.col("SVC_PRICE_AMT").cast("double"))
    .withColumn("SUPLMT_DSCNT_AMT", -F.col("SUPLMT_DSCNT_AMT").cast("double"))
    .withColumn("ALW_PRICE_UNIT_CT", -F.col("ALW_PRICE_UNIT_CT").cast("double"))
    .withColumn("UNIT_CT", -F.col("UNIT_CT").cast("double"))
    .withColumn("ITS_SUPLMT_DSCNT", -F.col("ITS_SUPLMT_DSCNT").cast("double"))
    .withColumn("ITS_SRCHRG_AMT", -F.col("ITS_SRCHRG_AMT").cast("double"))
)

# MERGE => CCollector with two inputs => round robin => effectively union
df_merged = df_claimLine.select(df_claimLine.columns).unionByName(df_reversal.select(df_claimLine.columns))

# SNAPSHOT => Another Transformer with two output links: "Pkey" (ClmLnPK) and "Snapshot" (Transformer)
# The job metadata shows the "Pkey" link has nearly all columns, "Snapshot" link has fewer columns
# We'll branch out:

pkey_cols = df_merged.columns + ["MED_PDX_IND","APC_ID","APC_STTUS_ID"]  # from the job snippet
# The job sets MED_PDX_IND='MED', APC_ID=Transform.APCD_ID, APC_STTUS_ID=Transform.APSI_STS_IND
# We'll create them now:
df_SnapShotIn = df_merged.withColumn("MED_PDX_IND", F.lit("MED")) \
                         .withColumn("APC_ID", F.col("APCD_ID")) \
                         .withColumn("APC_STTUS_ID", F.col("APSI_STS_IND"))

# "Pkey" link columns => exactly as in the job
df_Pkey = df_SnapShotIn.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD"),
    F.col("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD"),
    F.col("CAP_LN_IN"),
    F.col("PRI_LOB_IN"),
    F.col("SVC_END_DT"),
    F.col("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID"),
    F.col("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX"),
    F.col("SVC_ID"),
    F.col("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX"),
    F.col("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.col("VBB_RULE_ID"),
    F.col("VBB_EXCD_ID"),
    F.col("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT"),
    F.col("ITS_SRCHRG_AMT"),
    F.col("NDC"),
    F.col("NDC_DRUG_FORM_CD"),
    F.col("NDC_UNIT_CT"),
    F.col("MED_PDX_IND"),
    F.col("APC_ID"),
    F.col("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD"),
)

# "Snapshot" link
df_Snapshot = df_SnapShotIn.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
)

# TRANSFORMER => "Transformer" stage with two stage variables:
#   1) ProcCdSk = GetFkeyProcCd("FACETS", 0, Snapshot.PROC_CD[1, 5], Snapshot.PROC_CD_TYP_CD, Snapshot.PROC_CD_CAT_CD, 'N')
#   2) ClmLnRvnuCdSk = GetFkeyRvnu("FACETS", 0, Snapshot.CLM_LN_RVNU_CD, 'N')
# Then the output columns => RowCount => B_CLM_LN
df_Snapshot_alias = df_Snapshot.alias("Snapshot")
df_Transformer = (
    df_Snapshot_alias
    .withColumn(
        "ProcCdSk",
        F.expr('GetFkeyProcCd("FACETS", 0, substring(Snapshot.PROC_CD,1,5), Snapshot.PROC_CD_TYP_CD, Snapshot.PROC_CD_CAT_CD, "N")')
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        F.expr('GetFkeyRvnu("FACETS", 0, Snapshot.CLM_LN_RVNU_CD, "N")')
    )
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
        F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("ProcCdSk").alias("PROC_CD_SK"),
        F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
        F.col("Snapshot.ALW_AMT").alias("ALW_AMT"),
        F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
        F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT"),
    )
)

# WRITE "B_CLM_LN" => .dat file
write_files(
    df_Transformer,
    f"{adls_path}/load/B_CLM_LN.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# SHARED CONTAINER: ClmLnPK => "Pkey" => "Key"
# This container has 1 input pin, 1 output pin
params_ClmLnPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmLnPKOut = ClmLnPK(df_Pkey, params_ClmLnPK)

# Finally, write to "ClmLn" => "CSeqFileStage" => key/FctsClmLnMedExtr.FctsClmLn.dat.#RunID#
write_files(
    df_ClmLnPKOut,
    f"{adls_path}/key/FctsClmLnMedExtr.FctsClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)