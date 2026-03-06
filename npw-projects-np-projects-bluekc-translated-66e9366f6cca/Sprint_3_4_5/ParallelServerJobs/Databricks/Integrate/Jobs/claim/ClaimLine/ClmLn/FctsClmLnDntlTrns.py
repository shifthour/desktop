# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************
# MAGIC  Copyright 2004 Blue Cross/Blue Shield of Kansas City
# MAGIC  Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC  Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC \(9)Transform extracted Claim Line dental data from Facets into a common format to 
# MAGIC \(9)be potentially merged with claim line records from other systems for the 
# MAGIC \(9)foreign keying process and ultimate load into IDS
# MAGIC 
# MAGIC PRIMARY SOURCE:
# MAGIC \(9)InFile param\(9)- Sequential file containing extracted claim line 
# MAGIC \(9)\(9)\(9)  data to be transformed (../verified)
# MAGIC 
# MAGIC HASH FILES
# MAGIC          The following hash files are created in other processes and are used in other processes besides this job
# MAGIC                 DO NOT CLEAR
# MAGIC                 hf_clm_fcts_reversal - do not clear
# MAGIC                hf_clm_nasco_dup_bypass - do not clear
# MAGIC 
# MAGIC  OUTTPUTS:
# MAGIC 
# MAGIC \(9)TmpOutFile param\(9)- Sequential file (parameterized name) containing 
# MAGIC \(9)\(9)\(9)  transformed claim line data in common record format 
# MAGIC \(9)\(9)\(9)/key
# MAGIC JOB PARAMETERS:
# MAGIC 
# MAGIC \(9)FilePath\(9)\(9)- Directory path for sequential data files
# MAGIC \(9)InFile\(9)\(9)- Name of sequential input file
# MAGIC \(9)TmpOutFile\(9)- Name of sequential output file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC \(9)Developer\(9)Date\(9)\(9)Comment
# MAGIC \(9)-----------------------------\(9)---------------------------\(9)-----------------------------------------------------------
# MAGIC \(9)Kevin Soderlund\(9)05/07/2004\(9)Coding complete for release 1.0
# MAGIC \(9)Kevin Soderlund\(9)06/28/2004\(9)Updated with changes for release 1.1
# MAGIC \(9)Kevin Soderlund\(9)09/01/2004\(9)Added claim type to metadata
# MAGIC                 Brent Leland            11/16/2004             Changed stage variable svOvrMAmt from using claim ID to CDDO_OR_AMT
# MAGIC                 Sharon Andrew        2/01/2005              IDS 3.0  - removed COB_SAV_AMT AND COB_OTHR_CAR_D_AMT 
# MAGIC                                                                                Added hash files to documentation list \(9)hf_cddl_disall ,  hf_cddl_ovr,  hf_cddl_disall_pt,  hf_cddl_ovr_pt
# MAGIC                                                                                 Commented out documentation on \(9)hf_excd_expl_cd and hf_ovr\(9)
# MAGIC                 BJ Luce                   3/2/2006                change logic to calculate dsalw_amt, no_resp_amt, pat_resp_amt, prov_wrt_off_amt
# MAGIC                                                                              add new fields to  for clm_ln_svc_loc_typ_cd_sk and non_par_sav_amt
# MAGIC                                                                              add claim line primary key process
# MAGIC                                                                              put output to /key
# MAGIC                  BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC                 Steph Goddard  03/31/2006                added CDDL_EOB_EXCD to input file and moved to CLM_LN_EOB_EXCD
# MAGIC                                                                              changed SVC_RULE_TYPE_TX to be @NULL if input is blank or null instead of NA
# MAGIC                                                                              changed calculation for Non Par Savings Amount to use CDDL_CONSIDER_CHG instead of CDDL_CHG_AMT
# MAGIC                                                                              changed default for service location type code to OV instead of NA
# MAGIC                 Brent Leland       04/17/2006               Removed clear of hf_clm_ln_dntl_dsalw_ln_amts which no longer exists in job.
# MAGIC                Sanderw                12/08/2006            Project 1576  - Reversal logix added for new status codes 89 and  and 99 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen           8/20/2007        Balancing             Add Balancing Snapshot File                                                        devlIDS30                      Steph Goddard          8/30/07
# MAGIC 
# MAGIC Parik                         2008-08-05      3567(Primary Key)  Added the new primary keying process                                       devlIDS                           Steph Goddard          08/12/2008 
# MAGIC Brent Leland             08-21-2008      TTR-347               Added converion of Dental Service Price Rule                            devlIDS                           Michelle Lang            08-21-2008
# MAGIC                                                                                       for input CPC1 - DENTCPC1 and TRD4 - DENTTRD4
# MAGIC 
# MAGIC Rick Henry              2012-05-02        4896                      Proc_Cd_Typ_Cd and Proc_Cd_Cat_Cd                                     NewDevl                         SAndrew                   2012-05-18
# MAGIC                                                                                         for Proc_Cd_Sk lookup
# MAGIC Kalyan Neelam          2013-02-05  4963 VBB Phase 3  Added 3 new columns on end - VBB_RULE_ID,                         IntegrateNewDevl           Bhoomi Dasari            3/14/2013
# MAGIC                                                                                                                       VBB_EXCD_ID, CLM_LN_VBB_IN
# MAGIC 
# MAGIC Manasa Andru          2014-10-17        TFS - 9580         Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and                IntegrateCurDevl              Kalyan Neelam            2014-10-22
# MAGIC                                                                                                   ITS_SRCHRG_AMT) at the end.
# MAGIC 
# MAGIC Manasa Andru       2014-11-17   TFS - 9580(Post Prod Work)         Added scale of 2 for the 2 new fields                        IntegrateCurDevl              Kalyan Neelam            2014-11-19
# MAGIC                                                                                     (ITS_SUPLMT_DSCNT_AMT and ITS_SRCHRG_AMT) 
# MAGIC 
# MAGIC Manasa Andru       2015-04-22        TFS - 12493         Updated the fields - CLM_LN_DSALW_EXCD and                      IntegrateDev2                 Jag Yelavarthi             2016-05-03
# MAGIC                                                                                   CLM_LN_EOB_EXCD to No Upcase so that the fields would find
# MAGIC                                                                                          a match on the EXCD table.
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,      IntegrateDev1                 Kalyan Neelam            2017-08-29
# MAGIC                                                                                     NDC_UNIT_CT) at the end
# MAGIC 
# MAGIC Jaideep Mankala      2017-11-20      5828 \(9)   Added new field to identify MED / PDX claim                                IntegrateDev2                 Kalyan Neelam            2017-11-20   
# MAGIC                                                       \(9)\(9)when passing to Fkey job
# MAGIC 
# MAGIC Madhavan B          2018-02-06        5792 \(9)     Changed the datatype of the column                                           IntegrateDev1                 Kalyan Neelam            2018-02-08
# MAGIC 
# MAGIC Rekha Radhakrishna 2020-11-10  PBM Phase II        Added two new fields APC_ID and APC_STTUS_ID                   IntegrateDev2                 Kalyan Neelam            2020-12-11
# MAGIC                                                                                     being input to ClmPK
# MAGIC                                                       \(9)\(9)     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Kshema H K        2023-08-01       US 589700            Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a   IntegrateDevB\(9)Harsha Ravuri\(9)2023-08-31
# MAGIC                                                                                  default value in BusinessRules stage and mapped it till target

# MAGIC Sequential file (parameterized name) containing extracted claim line data to be transformed (../verified)
# MAGIC 
# MAGIC Created in FctsDnltClmLineExtr
# MAGIC Apply transformations to extracted claim line data based on business rules and format output into a common record format to be able for merging with data from other systems for foreign keying and ultimate load into IDS
# MAGIC Extract disallow and override amounts
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
# MAGIC Hash files created in the FctsClmLnHashFileExtr are also used in FctsClmLnMedTrns, FctsClmLnMedExtr and FctsDntlClmLineExtr
# MAGIC 
# MAGIC Do Not Clear in this process
# MAGIC hf_clm_ln_dsalw_ln_amts
# MAGIC File is written out as \"FctsClmLnDntlExtr.FctsClmLn.dat.#RunID#\" - and will be picked up in Load4 sequencer with the Claim Line sort
# MAGIC Hash files created in FctsClmDriverBuild
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","20120425")
CurrRunCycle = get_widget_value("CurrRunCycle","1")
CurrentDate = get_widget_value("CurrentDate","20120425")
SrcSysCdSk = get_widget_value("SrcSysCdSk","FCTS")

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet").alias("hf_clm_fcts_reversals")

df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet").alias("clm_nasco_dup_bypass")

schema_seqVerified = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CDDL_SEQ_NO", IntegerType(), nullable=False),
    StructField("EXT_TIMESTAMP", TimestampType(), nullable=False),
    StructField("PRPR_ID", StringType(), nullable=False),
    StructField("LOBD_ID", StringType(), nullable=False),
    StructField("PDVC_LOBD_PTR", StringType(), nullable=False),
    StructField("DPPY_PFX", StringType(), nullable=False),
    StructField("LTLT_PFX", StringType(), nullable=False),
    StructField("CGPY_PFX", StringType(), nullable=False),
    StructField("DPCG_DP_ID_ALT", StringType(), nullable=False),
    StructField("CDDL_ALTDP_EXCD_ID", StringType(), nullable=False),
    StructField("CGCG_ID", StringType(), nullable=False),
    StructField("CDDL_TOOTH_NO", StringType(), nullable=False),
    StructField("CDDL_TOOTH_BEG", StringType(), nullable=False),
    StructField("CDDL_TOOTH_END", StringType(), nullable=False),
    StructField("CDDL_SURF", StringType(), nullable=False),
    StructField("UTUT_CD", StringType(), nullable=False),
    StructField("DEDE_PFX", StringType(), nullable=False),
    StructField("DPPC_PRICE_ID", StringType(), nullable=False),
    StructField("DPDP_ID", StringType(), nullable=False),
    StructField("CGCG_RULE", StringType(), nullable=False),
    StructField("CDDL_FROM_DT", TimestampType(), nullable=False),
    StructField("CDDL_CHG_AMT", DecimalType(), nullable=False),
    StructField("CDDL_CONSIDER_CHG", DecimalType(), nullable=False),
    StructField("CDDL_ALLOW", DecimalType(), nullable=False),
    StructField("CDDL_UNITS", IntegerType(), nullable=False),
    StructField("CDDL_UNITS_ALLOW", IntegerType(), nullable=False),
    StructField("CDDL_DED_AMT", DecimalType(), nullable=False),
    StructField("CDDL_DED_AC_NO", IntegerType(), nullable=False),
    StructField("CDDL_COPAY_AMT", DecimalType(), nullable=False),
    StructField("CDDL_COINS_AMT", DecimalType(), nullable=False),
    StructField("CDDL_RISK_WH_AMT", DecimalType(), nullable=False),
    StructField("CDDL_PAID_AMT", DecimalType(), nullable=False),
    StructField("CDDL_DISALL_AMT", DecimalType(), nullable=False),
    StructField("CDDL_DISALL_EXCD", StringType(), nullable=False),
    StructField("CDDL_AG_PRICE", DecimalType(), nullable=False),
    StructField("CDDL_PF_PRICE", DecimalType(), nullable=False),
    StructField("CDDL_DP_PRICE", DecimalType(), nullable=False),
    StructField("CDDL_IP_PRICE", DecimalType(), nullable=False),
    StructField("CDDL_PRICE_IND", StringType(), nullable=False),
    StructField("CDDL_OOP_CALC_BASE", DecimalType(), nullable=False),
    StructField("CDDL_CL_NTWK_IND", StringType(), nullable=False),
    StructField("CDDL_REF_IND", StringType(), nullable=False),
    StructField("CDDL_CAP_IND", StringType(), nullable=False),
    StructField("CDDL_SB_PYMT_AMT", DecimalType(), nullable=False),
    StructField("CDDL_PR_PYMT_AMT", DecimalType(), nullable=False),
    StructField("CDDL_UMREF_ID", StringType(), nullable=False),
    StructField("CDDL_REFSV_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_FINL_DISP_CD", StringType(), nullable=False),
    StructField("PSCD_ID", StringType(), nullable=False),
    StructField("CLCL_NTWK_IND", StringType(), nullable=False),
    StructField("CDDL_RESP_CD", StringType(), nullable=True),
    StructField("EXCD_FOUND", StringType(), nullable=False),
    StructField("CDDL_EOB_EXCD", StringType(), nullable=False)
])

df_seqVerified = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .option("delimiter", ",")
    .schema(schema_seqVerified)
    .load(f"{adls_path}/verified/FctsClmLnDntlExtr.ClmLnDntl.dat.{RunID}")
    .alias("seqVerified")
)

df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_ln_amts.parquet").alias("hf_clm_ln_dsalw_ln_amts")
df_hf_clm_ln_cdmd_provcntrct = spark.read.parquet(f"{adls_path}/hf_clm_ln_cdmd_provcntrct.parquet").alias("hf_clm_ln_cdmd_provcntrct")
df_hf_clm_ln_clhp = spark.read.parquet(f"{adls_path}/hf_clm_ln_clhp.parquet").alias("hf_clm_ln_clhp")
df_hf_clm_ln_clor = spark.read.parquet(f"{adls_path}/hf_clm_ln_clor.parquet").alias("hf_clm_ln_clor")

df_RefMY = df_hf_clm_ln_dsalw_ln_amts.alias("RefMY")
df_RefMN = df_hf_clm_ln_dsalw_ln_amts.alias("RefMN")
df_RefNY = df_hf_clm_ln_dsalw_ln_amts.alias("RefNY")
df_RefNN = df_hf_clm_ln_dsalw_ln_amts.alias("RefNN")
df_RefOY = df_hf_clm_ln_dsalw_ln_amts.alias("RefOY")
df_RefON = df_hf_clm_ln_dsalw_ln_amts.alias("RefON")
df_RefPY = df_hf_clm_ln_dsalw_ln_amts.alias("RefPY")
df_RefPN = df_hf_clm_ln_dsalw_ln_amts.alias("RefPN")
df_RefPvdCntrct = df_hf_clm_ln_cdmd_provcntrct.alias("RefPvdCntrct")
df_RefClhp = df_hf_clm_ln_clhp.alias("RefClhp")
df_clor_lkup = df_hf_clm_ln_clor.alias("clor_lkup")

df_tBusinessRules = (
    df_seqVerified.alias("FctsClmLn")
    .join(
        df_RefMY,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefMY.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefMY.CDML_SEQ_NO")) &
        (F.lit("M") == F.col("RefMY.EXCD_RESP_CD")) &
        (F.lit("Y") == F.col("RefMY.BYPS_IN")),
        "left"
    )
    .join(
        df_RefMN,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefMN.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefMN.CDML_SEQ_NO")) &
        (F.lit("M") == F.col("RefMN.EXCD_RESP_CD")) &
        (F.lit("N") == F.col("RefMN.BYPS_IN")),
        "left"
    )
    .join(
        df_RefNY,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefNY.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefNY.CDML_SEQ_NO")) &
        (F.lit("N") == F.col("RefNY.EXCD_RESP_CD")) &
        (F.lit("Y") == F.col("RefNY.BYPS_IN")),
        "left"
    )
    .join(
        df_RefNN,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefNN.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefNN.CDML_SEQ_NO")) &
        (F.lit("N") == F.col("RefNN.EXCD_RESP_CD")) &
        (F.lit("N") == F.col("RefNN.BYPS_IN")),
        "left"
    )
    .join(
        df_RefOY,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefOY.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefOY.CDML_SEQ_NO")) &
        (F.lit("O") == F.col("RefOY.EXCD_RESP_CD")) &
        (F.lit("Y") == F.col("RefOY.BYPS_IN")),
        "left"
    )
    .join(
        df_RefON,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefON.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefON.CDML_SEQ_NO")) &
        (F.lit("O") == F.col("RefON.EXCD_RESP_CD")) &
        (F.lit("N") == F.col("RefON.BYPS_IN")),
        "left"
    )
    .join(
        df_RefPY,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefPY.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefPY.CDML_SEQ_NO")) &
        (F.lit("P") == F.col("RefPY.EXCD_RESP_CD")) &
        (F.lit("Y") == F.col("RefPY.BYPS_IN")),
        "left"
    )
    .join(
        df_RefPN,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefPN.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefPN.CDML_SEQ_NO")) &
        (F.lit("P") == F.col("RefPN.EXCD_RESP_CD")) &
        (F.lit("N") == F.col("RefPN.BYPS_IN")),
        "left"
    )
    .join(
        df_RefPvdCntrct,
        (F.col("FctsClmLn.CLCL_ID") == F.col("RefPvdCntrct.CLCL_ID")) &
        (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefPvdCntrct.CDML_SEQ_NO")),
        "left"
    )
    .join(
        df_RefClhp,
        F.col("FctsClmLn.CLCL_ID") == F.col("RefClhp.CLCL_ID"),
        "left"
    )
    .join(
        df_clor_lkup,
        F.col("FctsClmLn.CLCL_ID") == F.col("clor_lkup.CLM_ID"),
        "left"
    )
)

df_tBusinessRules = (
    df_tBusinessRules
    .withColumn(
        "svNtwkO",
        F.when(F.trim(F.col("FctsClmLn.CDDL_CL_NTWK_IND")) == F.lit("O"), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svNtwkIP",
        F.when(F.trim(F.col("FctsClmLn.CDDL_CL_NTWK_IND")) == F.lit("I"), F.lit("Y"))
        .when(F.trim(F.col("FctsClmLn.CDDL_CL_NTWK_IND")) == F.lit("P"), F.lit("Y"))
        .otherwise(F.lit("N"))
    )
    .withColumn(
        "svN",
        F.when(F.col("RefNY.CLCL_ID").isNotNull(), F.lit("Y"))
        .when(
            (F.col("RefMY.CLCL_ID").isNotNull()) | 
            (F.col("RefOY.CLCL_ID").isNotNull()) | 
            (F.col("RefPY.CLCL_ID").isNotNull()),
            F.lit("O")
        )
        .when(F.col("RefNN.CLCL_ID").isNotNull(), F.lit("N"))
        .otherwise(F.lit("X"))
    )
    .withColumn(
        "svM",
        F.when(F.col("RefMY.CLCL_ID").isNotNull(), F.lit("Y"))
        .when(
            (F.col("RefNY.CLCL_ID").isNotNull()) | 
            (F.col("RefOY.CLCL_ID").isNotNull()) | 
            (F.col("RefPY.CLCL_ID").isNotNull()),
            F.lit("O")
        )
        .when(F.col("RefMN.CLCL_ID").isNotNull(), F.lit("N"))
        .otherwise(F.lit("X"))
    )
    .withColumn(
        "svO",
        F.when(F.col("RefOY.CLCL_ID").isNotNull(), F.lit("Y"))
        .when(
            (F.col("RefNY.CLCL_ID").isNotNull()) | 
            (F.col("RefMY.CLCL_ID").isNotNull()) | 
            (F.col("RefPY.CLCL_ID").isNotNull()),
            F.lit("O")
        )
        .when(F.col("RefON.CLCL_ID").isNotNull(), F.lit("N"))
        .otherwise(F.lit("X"))
    )
    .withColumn(
        "svP",
        F.when(F.col("RefPY.CLCL_ID").isNotNull(), F.lit("Y"))
        .when(
            (F.col("RefNY.CLCL_ID").isNotNull()) | 
            (F.col("RefOY.CLCL_ID").isNotNull()) | 
            (F.col("RefMY.CLCL_ID").isNotNull()),
            F.lit("O")
        )
        .when(F.col("RefPN.CLCL_ID").isNotNull(), F.lit("N"))
        .otherwise(F.lit("X"))
    )
    .withColumn(
        "svMaster",
        F.when(F.col("svN") == F.lit("Y"), F.lit("N"))
        .when(F.col("svP") == F.lit("Y"), F.lit("P"))
        .when(F.col("svM") == F.lit("Y"), F.lit("M"))
        .when(F.col("svO") == F.lit("Y"), F.lit("O"))
        .otherwise(F.lit("X"))
    )
    .withColumn(
        "svTotalofAll",
        F.col("FctsClmLn.CDDL_DISALL_AMT")
    )
    .withColumn(
        "svDefaultToProvWriteOff",
        F.when(F.col("clor_lkup.CLM_ID").isNull(), F.lit("N"))
        .otherwise(
            F.when(F.col("FctsClmLn.EXCD_FOUND") == F.lit("Y"), F.lit("N")).otherwise(F.lit("Y"))
        )
    )
    .withColumn(
        "svNoRespAmt",
        F.when(F.col("svDefaultToProvWriteOff") == F.lit("Y"), F.lit("0.00").cast(DecimalType(38,2)))
        .otherwise(
            F.when(F.col("svMaster") == F.lit("N"), F.col("svTotalofAll"))
            .when(
                F.col("svMaster") == F.lit("X"),
                F.when(
                    F.col("RefNN.DSALW_AMT").isNull() | 
                    (F.trim(F.col("RefNN.DSALW_AMT")) == "") | 
                    ~F.col("RefNN.DSALW_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
                    F.lit("0").cast(DecimalType(38,2))
                ).otherwise(F.col("RefNN.DSALW_AMT"))
            )
            .otherwise(F.lit("0.00").cast(DecimalType(38,2)))
        )
    )
    .withColumn(
        "svPatRespAmt",
        F.when(F.col("svDefaultToProvWriteOff") == F.lit("Y"), F.lit("0.00").cast(DecimalType(38,2)))
        .otherwise(
            F.when(
                (F.col("svMaster") == F.lit("M")) | 
                ((F.col("svMaster") == F.lit("O")) & (F.col("svNtwkO") == F.lit("Y"))),
                F.col("svTotalofAll")
            )
            .otherwise(
                (
                    F.when(F.col("svMaster") == F.lit("X"),
                        F.when(
                            F.col("RefMN.DSALW_AMT").isNull() | 
                            (F.trim(F.col("RefMN.DSALW_AMT")) == "") | 
                            ~F.col("RefMN.DSALW_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
                            F.lit("0").cast(DecimalType(38,2))
                        ).otherwise(F.col("RefMN.DSALW_AMT"))
                    )
                    + F.when(
                        (F.col("svNtwkO") == F.lit("Y")) & (F.col("svMaster") == F.lit("X")),
                        F.when(
                            F.col("RefON.DSALW_AMT").isNull() | 
                            (F.trim(F.col("RefON.DSALW_AMT")) == "") | 
                            ~F.col("RefON.DSALW_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
                            F.lit("0").cast(DecimalType(38,2))
                        ).otherwise(F.col("RefON.DSALW_AMT"))
                    ).otherwise(F.lit("0.00").cast(DecimalType(38,2)))
                )
            )
        )
    )
    .withColumn(
        "svProvWriteOff",
        F.when(F.col("svDefaultToProvWriteOff") == F.lit("Y"), F.col("svTotalofAll"))
        .otherwise(
            F.when(
                (F.col("svMaster") == F.lit("P")) | 
                ((F.col("svMaster") == F.lit("O")) & (F.col("svNtwkIP") == F.lit("Y"))),
                F.col("svTotalofAll")
            )
            .otherwise(
                (
                    F.when(F.col("svMaster") == F.lit("X"),
                        F.when(
                            F.col("RefPN.DSALW_AMT").isNull() | 
                            (F.trim(F.col("RefPN.DSALW_AMT")) == "") | 
                            ~F.col("RefPN.DSALW_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
                            F.lit("0").cast(DecimalType(38,2))
                        ).otherwise(F.col("RefPN.DSALW_AMT"))
                    )
                    + F.when(
                        (F.col("svNtwkIP") == F.lit("Y")) & (F.col("svMaster") == F.lit("X")),
                        F.when(
                            F.col("RefON.DSALW_AMT").isNull() | 
                            (F.trim(F.col("RefON.DSALW_AMT")) == "") | 
                            ~F.col("RefON.DSALW_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
                            F.lit("0").cast(DecimalType(38,2))
                        ).otherwise(F.col("RefON.DSALW_AMT"))
                    ).otherwise(F.lit("0.00").cast(DecimalType(38,2)))
                )
            )
        )
    )
    .withColumn(
        "svSvcPrice",
        F.when(
            (F.col("FctsClmLn.DPPC_PRICE_ID").isNull()) | 
            (F.trim(F.col("FctsClmLn.DPPC_PRICE_ID")) == ""),
            F.lit("")
        )
        .otherwise(F.trim(F.col("FctsClmLn.DPPC_PRICE_ID")))
    )
)

df_ClmLnDntRecs = df_tBusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), 
             F.when((F.col("FctsClmLn.CLCL_ID").isNull()) | (F.trim(F.col("FctsClmLn.CLCL_ID")) == ""), F.lit("UNK"))
              .otherwise(F.trim(F.col("FctsClmLn.CLCL_ID"))),
             F.lit(";"), 
             F.when((F.col("FctsClmLn.CDDL_SEQ_NO").isNull()), F.lit("UNK"))
              .otherwise(F.col("FctsClmLn.CDDL_SEQ_NO").cast(StringType()))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.when((F.col("FctsClmLn.CLCL_ID").isNull()) | (F.trim(F.col("FctsClmLn.CLCL_ID")) == ""), F.lit("UNK"))
     .otherwise(F.trim(F.col("FctsClmLn.CLCL_ID"))).alias("CLM_ID"),
    F.when((F.col("FctsClmLn.CDDL_SEQ_NO").isNull()), F.lit("UNK"))
     .otherwise(F.col("FctsClmLn.CDDL_SEQ_NO").cast(StringType())).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("FctsClmLn.DPDP_ID").isNull()) | (F.trim(F.col("FctsClmLn.DPDP_ID")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.DPDP_ID")))).alias("PROC_CD"),
    F.when(
        (F.col("FctsClmLn.PRPR_ID").isNull()) | (F.trim(F.col("FctsClmLn.PRPR_ID")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.PRPR_ID")))).alias("SVC_PROV_ID"),
    F.when(
        (F.col("FctsClmLn.CDDL_DISALL_EXCD").isNull()) | 
         (F.trim(F.col("FctsClmLn.CDDL_DISALL_EXCD")) == ""),
        F.lit("NA")
    )
    .otherwise(F.trim(F.col("FctsClmLn.CDDL_DISALL_EXCD"))).alias("CLM_LN_DSALW_EXCD"),
    F.when(
        (F.col("FctsClmLn.CDDL_EOB_EXCD").isNull()) | 
         (F.trim(F.col("FctsClmLn.CDDL_EOB_EXCD")) == ""),
        F.lit("NA")
    )
    .otherwise(F.trim(F.col("FctsClmLn.CDDL_EOB_EXCD"))).alias("CLM_LN_EOB_EXCD"),
    F.when(
        (F.col("FctsClmLn.CLM_LN_FINL_DISP_CD").isNull()) | 
         (F.trim(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD")))).alias("CLM_LN_FINL_DISP_CD"),
    F.when(
        (F.col("FctsClmLn.LOBD_ID").isNull()) | 
         (F.trim(F.col("FctsClmLn.LOBD_ID")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.LOBD_ID")))).alias("CLM_LN_LOB_CD"),
    F.when(
        (F.col("FctsClmLn.PSCD_ID").isNull()) | 
         (F.trim(F.col("FctsClmLn.PSCD_ID")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.PSCD_ID")))).alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.when(
        (F.col("FctsClmLn.CDDL_PRICE_IND").isNull()) | 
         (F.trim(F.col("FctsClmLn.CDDL_PRICE_IND")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.CDDL_PRICE_IND")))).alias("CLM_LN_PRICE_SRC_CD"),
    F.when(
        (F.col("FctsClmLn.CDDL_REF_IND").isNull()) | 
         (F.trim(F.col("FctsClmLn.CDDL_REF_IND")) == ""),
        F.lit("NA")
    )
    .otherwise(F.upper(F.trim(F.col("FctsClmLn.CDDL_REF_IND")))).alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("UN").alias("CLM_LN_UNIT_TYP_CD"),
    F.when(F.trim(F.col("FctsClmLn.CDDL_CAP_IND")) == F.lit("Y"), F.lit("Y")).otherwise(F.lit("N")).alias("CAP_LN_IN"),
    F.when(F.trim(F.col("FctsClmLn.PDVC_LOBD_PTR")) == F.lit("1"), F.lit("Y")).otherwise(F.lit("N")).alias("PRI_LOB_IN"),
    F.when(
        (F.col("FctsClmLn.CDDL_FROM_DT").isNull()),
        F.lit("UNK")
    )
    .otherwise(F.substring(F.trim(F.col("FctsClmLn.CDDL_FROM_DT")),1,10)).alias("SVC_END_DT"),
    F.when(
        (F.col("FctsClmLn.CDDL_FROM_DT").isNull()),
        F.lit("UNK")
    )
    .otherwise(F.substring(F.trim(F.col("FctsClmLn.CDDL_FROM_DT")),1,10)).alias("SVC_STRT_DT"),
    F.when(
        (F.col("FctsClmLn.CDDL_AG_PRICE").isNull()) |
        ~F.col("FctsClmLn.CDDL_AG_PRICE").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_AG_PRICE")).alias("AGMNT_PRICE_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_ALLOW").isNull()) |
        ~F.col("FctsClmLn.CDDL_ALLOW").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_ALLOW")).alias("ALW_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_CHG_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_CHG_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_CHG_AMT")).alias("CHRG_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_COINS_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_COINS_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_COINS_AMT")).alias("COINS_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_CONSIDER_CHG").isNull()) |
        ~F.col("FctsClmLn.CDDL_CONSIDER_CHG").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_CONSIDER_CHG")).alias("CNSD_CHRG_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_COPAY_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_COPAY_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_COPAY_AMT")).alias("COPAY_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_DED_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_DED_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_DED_AMT")).alias("DEDCT_AMT"),
    F.col("FctsClmLn.CDDL_DISALL_AMT").alias("DSALW_AMT"),
    F.lit(0).alias("ITS_HOME_DSCNT_AMT"),
    F.col("svNoRespAmt").alias("NO_RESP_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_OOP_CALC_BASE").isNull()) |
        ~F.col("FctsClmLn.CDDL_OOP_CALC_BASE").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_OOP_CALC_BASE")).alias("MBR_LIAB_BSS_AMT"),
    F.col("svPatRespAmt").alias("PATN_RESP_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_PAID_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_PAID_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_PAID_AMT")).alias("PAYBL_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_PR_PYMT_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_PR_PYMT_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_PR_PYMT_AMT")).alias("PAYBL_TO_PROV_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_SB_PYMT_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_SB_PYMT_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_SB_PYMT_AMT")).alias("PAYBL_TO_SUB_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_IP_PRICE").isNull()) |
        ~F.col("FctsClmLn.CDDL_IP_PRICE").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_IP_PRICE")).alias("PROC_TBL_PRICE_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_PF_PRICE").isNull()) |
        ~F.col("FctsClmLn.CDDL_PF_PRICE").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_PF_PRICE")).alias("PROFL_PRICE_AMT"),
    F.col("svProvWriteOff").alias("PROV_WRT_OFF_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_RISK_WH_AMT").isNull()) |
        ~F.col("FctsClmLn.CDDL_RISK_WH_AMT").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_RISK_WH_AMT")).alias("RISK_WTHLD_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_DP_PRICE").isNull()) |
        ~F.col("FctsClmLn.CDDL_DP_PRICE").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_DP_PRICE")).alias("SVC_PRICE_AMT"),
    F.lit(0).alias("SUPLMT_DSCNT_AMT"),
    F.when(
        (F.col("FctsClmLn.CDDL_UNITS_ALLOW").isNull()) |
        ~F.col("FctsClmLn.CDDL_UNITS_ALLOW").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_UNITS_ALLOW")).alias("ALW_PRICE_UNIT_CT"),
    F.when(
        (F.col("FctsClmLn.CDDL_UNITS").isNull()) |
        ~F.col("FctsClmLn.CDDL_UNITS").cast(StringType()).rlike("^[0-9.+-]+$"),
        F.lit("0")
    )
    .otherwise(F.col("FctsClmLn.CDDL_UNITS")).alias("UNIT_CT"),
    F.when(
        (F.col("FctsClmLn.CDDL_DED_AC_NO").isNull()) | 
        (F.trim(F.col("FctsClmLn.CDDL_DED_AC_NO")) == ""),
        F.lit(" ")
    )
    .otherwise(F.trim(F.col("FctsClmLn.CDDL_DED_AC_NO")).cast(StringType())).alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("PREAUTH_SVC_SEQ_NO"),
    F.when(
        (F.col("FctsClmLn.CDDL_REFSV_SEQ_NO").isNull()) | 
        (F.trim(F.col("FctsClmLn.CDDL_REFSV_SEQ_NO")) == ""),
        F.lit(" ")
    )
    .otherwise(F.trim(F.col("FctsClmLn.CDDL_REFSV_SEQ_NO")).cast(StringType())).alias("RFRL_SVC_SEQ_NO"),
    F.when(
        (F.col("FctsClmLn.LTLT_PFX").isNull()) | 
        (F.trim(F.col("FctsClmLn.LTLT_PFX")) == ""),
        F.lit("NA")
    )
    .otherwise(F.trim(F.col("FctsClmLn.LTLT_PFX"))).alias("LMT_PFX_ID"),
    F.lit("NA").alias("PREAUTH_ID"),
    F.when(
        (F.col("FctsClmLn.DEDE_PFX").isNull()) | 
        (F.trim(F.col("FctsClmLn.DEDE_PFX")) == ""),
        F.lit("NA")
    )
    .otherwise(F.trim(F.col("FctsClmLn.DEDE_PFX"))).alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.when(
        (F.col("FctsClmLn.DPPY_PFX").isNull()) | 
        (F.trim(F.col("FctsClmLn.DPPY_Pfx")) == ""),
        F.lit("NA")
    )
    .otherwise(F.trim(F.col("FctsClmLn.DPPY_PFX"))).alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.when(
        (F.col("FctsClmLn.CDDL_UMREF_ID").isNull()) | 
        (F.trim(F.col("FctsClmLn.CDDL_UMREF_ID")) == ""),
        F.lit("NA")
    )
    .otherwise(F.trim(F.col("FctsClmLn.CDDL_UMREF_ID"))).alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.when(
        (F.length(F.col("svSvcPrice")) == 0),
        F.lit("NA")
    )
    .when(F.col("svSvcPrice") == F.lit("CPC1"), F.lit("DENTCPC1"))
    .when(F.col("svSvcPrice") == F.lit("TRD4"), F.lit("DENTTRD4"))
    .otherwise(F.col("svSvcPrice")).alias("SVC_PRICE_RULE_ID"),
    F.when(
        (F.col("FctsClmLn.CGCG_RULE").isNull()) | 
        (F.trim(F.col("FctsClmLn.CGCG_RULE")) == ""),
        F.lit(None)
    )
    .otherwise(F.trim(F.col("FctsClmLn.CGCG_RULE"))).alias("SVC_RULE_TYP_TX"),
    F.when(F.length(F.trim(F.col("FctsClmLn.PSCD_ID"))) == 0, F.lit("OV"))
    .otherwise(F.col("FctsClmLn.PSCD_ID")).alias("SVC_LOC_TYP_CD"),
    (
        F.when(F.col("RefPvdCntrct.CDML_SEQ_NO").isNull(), F.lit("0.00"))
        .otherwise(
            F.when(
                F.col("FctsClmLn.CLCL_NTWK_IND") == F.lit("O"), 
                F.col("FctsClmLn.CDDL_CONSIDER_CHG") - F.col("FctsClmLn.CDDL_ALLOW")
            ).otherwise(F.lit("0.00"))
        )
    ).alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

df_BuildReversals = df_ClmLnDntRecs.alias("ClmLnDntRecs") 

df_BuildReversals = (
    df_BuildReversals
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("ClmLnDntRecs.CLM_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("ClmLnDntRecs.CLM_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
)

df_ClmLnDentalRecs = df_BuildReversals.where(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    df_BuildReversals["ClmLnDntRecs.*"]
)

df_reversals = df_BuildReversals.where(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
      (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89")) |
      (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91")) |
      (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("99"))
    )
).select(
    F.col("ClmLnDntRecs.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ClmLnDntRecs.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ClmLnDntRecs.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ClmLnDntRecs.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ClmLnDntRecs.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ClmLnDntRecs.ERR_CT").alias("ERR_CT"),
    F.col("ClmLnDntRecs.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ClmLnDntRecs.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(
        F.col("ClmLnDntRecs.SRC_SYS_CD"), 
        F.lit(";"), 
        F.trim(F.col("ClmLnDntRecs.CLM_ID")), 
        F.lit("R;"),
        F.col("ClmLnDntRecs.CLM_LN_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.col("ClmLnDntRecs.CLM_LN_SK").alias("CLM_LN_SK"),
    F.concat(F.trim(F.col("ClmLnDntRecs.CLM_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("ClmLnDntRecs.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ClmLnDntRecs.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ClmLnDntRecs.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmLnDntRecs.PROC_CD").alias("PROC_CD"),
    F.col("ClmLnDntRecs.SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("ClmLnDntRecs.CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("ClmLnDntRecs.CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("ClmLnDntRecs.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("ClmLnDntRecs.CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("ClmLnDntRecs.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("ClmLnDntRecs.CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("ClmLnDntRecs.CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("ClmLnDntRecs.CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("ClmLnDntRecs.CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("ClmLnDntRecs.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("ClmLnDntRecs.CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("ClmLnDntRecs.CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("ClmLnDntRecs.CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("ClmLnDntRecs.CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.col("ClmLnDntRecs.CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("ClmLnDntRecs.PRI_LOB_IN").alias("PRI_LOB_IN"),
    F.col("ClmLnDntRecs.SVC_END_DT").alias("SVC_END_DT"),
    F.col("ClmLnDntRecs.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.when(F.col("ClmLnDntRecs.AGMNT_PRICE_AMT")>0, -F.col("ClmLnDntRecs.AGMNT_PRICE_AMT")).otherwise(F.col("ClmLnDntRecs.AGMNT_PRICE_AMT")).alias("AGMNT_PRICE_AMT"),
    F.when(F.col("ClmLnDntRecs.ALW_AMT")>0, -F.col("ClmLnDntRecs.ALW_AMT")).otherwise(F.col("ClmLnDntRecs.ALW_AMT")).alias("ALW_AMT"),
    F.when(F.col("ClmLnDntRecs.CHRG_AMT")>0, -F.col("ClmLnDntRecs.CHRG_AMT")).otherwise(F.col("ClmLnDntRecs.CHRG_AMT")).alias("CHRG_AMT"),
    F.when(F.col("ClmLnDntRecs.COINS_AMT")>0, -F.col("ClmLnDntRecs.COINS_AMT")).otherwise(F.col("ClmLnDntRecs.COINS_AMT")).alias("COINS_AMT"),
    F.when(F.col("ClmLnDntRecs.CNSD_CHRG_AMT")>0, -F.col("ClmLnDntRecs.CNSD_CHRG_AMT")).otherwise(F.col("ClmLnDntRecs.CNSD_CHRG_AMT")).alias("CNSD_CHRG_AMT"),
    F.when(F.col("ClmLnDntRecs.COPAY_AMT")>0, -F.col("ClmLnDntRecs.COPAY_AMT")).otherwise(F.col("ClmLnDntRecs.COPAY_AMT")).alias("COPAY_AMT"),
    F.when(F.col("ClmLnDntRecs.DEDCT_AMT")>0, -F.col("ClmLnDntRecs.DEDCT_AMT")).otherwise(F.col("ClmLnDntRecs.DEDCT_AMT")).alias("DEDCT_AMT"),
    F.when(F.col("ClmLnDntRecs.DSALW_AMT")>0, -F.col("ClmLnDntRecs.DSALW_AMT")).otherwise(F.col("ClmLnDntRecs.DSALW_AMT")).alias("DSALW_AMT"),
    F.when(F.col("ClmLnDntRecs.ITS_HOME_DSCNT_AMT")>0, -F.col("ClmLnDntRecs.ITS_HOME_DSCNT_AMT")).otherwise(F.col("ClmLnDntRecs.ITS_HOME_DSCNT_AMT")).alias("ITS_HOME_DSCNT_AMT"),
    F.when(F.col("ClmLnDntRecs.NO_RESP_AMT")>0, -F.col("ClmLnDntRecs.NO_RESP_AMT")).otherwise(F.col("ClmLnDntRecs.NO_RESP_AMT")).alias("NO_RESP_AMT"),
    F.when(F.col("ClmLnDntRecs.MBR_LIAB_BSS_AMT")>0, -F.col("ClmLnDntRecs.MBR_LIAB_BSS_AMT")).otherwise(F.col("ClmLnDntRecs.MBR_LIAB_BSS_AMT")).alias("MBR_LIAB_BSS_AMT"),
    F.when(F.col("ClmLnDntRecs.PATN_RESP_AMT")>0, -F.col("ClmLnDntRecs.PATN_RESP_AMT")).otherwise(F.col("ClmLnDntRecs.PATN_RESP_AMT")).alias("PATN_RESP_AMT"),
    F.when(F.col("ClmLnDntRecs.PAYBL_AMT")>0, -F.col("ClmLnDntRecs.PAYBL_AMT")).otherwise(F.col("ClmLnDntRecs.PAYBL_AMT")).alias("PAYBL_AMT"),
    F.when(F.col("ClmLnDntRecs.PAYBL_TO_PROV_AMT")>0, -F.col("ClmLnDntRecs.PAYBL_TO_PROV_AMT")).otherwise(F.col("ClmLnDntRecs.PAYBL_TO_PROV_AMT")).alias("PAYBL_TO_PROV_AMT"),
    F.when(F.col("ClmLnDntRecs.PAYBL_TO_SUB_AMT")>0, -F.col("ClmLnDntRecs.PAYBL_TO_SUB_AMT")).otherwise(F.col("ClmLnDntRecs.PAYBL_TO_SUB_AMT")).alias("PAYBL_TO_SUB_AMT"),
    F.when(F.col("ClmLnDntRecs.PROC_TBL_PRICE_AMT")>0, -F.col("ClmLnDntRecs.PROC_TBL_PRICE_AMT")).otherwise(F.col("ClmLnDntRecs.PROC_TBL_PRICE_AMT")).alias("PROC_TBL_PRICE_AMT"),
    F.when(F.col("ClmLnDntRecs.PROFL_PRICE_AMT")>0, -F.col("ClmLnDntRecs.PROFL_PRICE_AMT")).otherwise(F.col("ClmLnDntRecs.PROFL_PRICE_AMT")).alias("PROFL_PRICE_AMT"),
    F.when(F.col("ClmLnDntRecs.PROV_WRT_OFF_AMT")>0, -F.col("ClmLnDntRecs.PROV_WRT_OFF_AMT")).otherwise(F.col("ClmLnDntRecs.PROV_WRT_OFF_AMT")).alias("PROV_WRT_OFF_AMT"),
    F.when(F.col("ClmLnDntRecs.RISK_WTHLD_AMT")>0, -F.col("ClmLnDntRecs.RISK_WTHLD_AMT")).otherwise(F.col("ClmLnDntRecs.RISK_WTHLD_AMT")).alias("RISK_WTHLD_AMT"),
    F.when(F.col("ClmLnDntRecs.SVC_PRICE_AMT")>0, -F.col("ClmLnDntRecs.SVC_PRICE_AMT")).otherwise(F.col("ClmLnDntRecs.SVC_PRICE_AMT")).alias("SVC_PRICE_AMT"),
    F.when(F.col("ClmLnDntRecs.SUPLMT_DSCNT_AMT")>0, -F.col("ClmLnDntRecs.SUPLMT_DSCNT_AMT")).otherwise(F.col("ClmLnDntRecs.SUPLMT_DSCNT_AMT")).alias("SUPLMT_DSCNT_AMT"),
    F.col("ClmLnDntRecs.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("ClmLnDntRecs.UNIT_CT").alias("UNIT_CT"),
    F.col("ClmLnDntRecs.DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    F.col("ClmLnDntRecs.PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    F.col("ClmLnDntRecs.RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    F.col("ClmLnDntRecs.LMT_PFX_ID").alias("LMT_PFX_ID"),
    F.col("ClmLnDntRecs.PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("ClmLnDntRecs.PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("ClmLnDntRecs.PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("ClmLnDntRecs.RFRL_ID_TX").alias("RFRL_ID_TX"),
    F.col("ClmLnDntRecs.SVC_ID").alias("SVC_ID"),
    F.col("ClmLnDntRecs.SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    F.col("ClmLnDntRecs.SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    F.col("ClmLnDntRecs.SVC_LOC_TYP_CD").cast(StringType()).alias("SVC_LOC_TYP_CD"),
    F.when(F.col("ClmLnDntRecs.NON_PAR_SAV_AMT") != 0, -F.col("ClmLnDntRecs.NON_PAR_SAV_AMT")).otherwise(F.col("ClmLnDntRecs.NON_PAR_SAV_AMT")).alias("NON_PAR_SAV_AMT"),
    F.lit("DNTL").alias("PROD_CD_TYP_CD"),
    F.lit("DNTL").alias("PROD_CD_CAT_CD"),
    F.col("ClmLnDntRecs.SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("ClmLnDntRecs.CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_Collector = df_ClmLnDentalRecs.unionByName(df_reversals)

df_Transform = df_Collector.alias("Transform")

df_Pkey = df_Transform.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.PROC_CD").alias("PROC_CD"),
    F.col("Transform.SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("Transform.CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("Transform.CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("Transform.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("Transform.CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("Transform.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("Transform.CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("Transform.CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("Transform.CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("Transform.CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("Transform.CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("Transform.CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("Transform.CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("Transform.CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.col("Transform.CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("Transform.PRI_LOB_IN").alias("PRI_LOB_IN"),
    F.col("Transform.SVC_END_DT").alias("SVC_END_DT"),
    F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Transform.AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("Transform.ALW_AMT").alias("ALW_AMT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.COINS_AMT").alias("COINS_AMT"),
    F.col("Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("Transform.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Transform.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Transform.DSALW_AMT").alias("DSALW_AMT"),
    F.col("Transform.ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("Transform.NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("Transform.MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("Transform.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Transform.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("Transform.PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("Transform.PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("Transform.PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    F.col("Transform.PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    F.col("Transform.RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("Transform.SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("Transform.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("Transform.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("Transform.UNIT_CT").alias("UNIT_CT"),
    F.col("Transform.DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    F.col("Transform.PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    F.col("Transform.RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    F.col("Transform.LMT_PFX_ID").alias("LMT_PFX_ID"),
    F.col("Transform.PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("Transform.PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("Transform.PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("Transform.RFRL_ID_TX").alias("RFRL_ID_TX"),
    F.col("Transform.SVC_ID").alias("SVC_ID"),
    F.col("Transform.SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    F.col("Transform.SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    F.col("Transform.SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
    F.col("Transform.NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.col("Transform.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.lit("N").alias("CLM_LN_VBB_IN"),
    F.lit(0).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.lit("MED").alias("MED_PDX_IND"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID"),
    F.col("Transform.SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("Transform.CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_snapshot_2 = df_Transform.select(
    F.col("Transform.CLM_ID").alias("CLCL_ID"),
    F.col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Transform.PROC_CD").alias("PROC_CD"),
    F.col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("Transform.ALW_AMT").alias("ALW_AMT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Transform.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
).alias("Snapshot")

df_EnrichTransformer = (
    df_snapshot_2
    .withColumn(
        "ProcCdSk",
        GetFkeyProcCd("FACETS", F.lit(0),
                      F.substring(F.col("Snapshot.PROC_CD"),1,5),
                      F.col("Snapshot.PROC_CD_TYP_CD"),
                      F.col("Snapshot.PROC_CD_CAT_CD"),
                      F.lit("N"))
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        GetFkeyRvnu("FACETS", F.lit(0), F.col("Snapshot.CLM_LN_RVNU_CD"), F.lit("N"))
    )
)

df_RowCount = df_EnrichTransformer.select(
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
    F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Snapshot.ALW_AMT").alias("ALW_AMT"),
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT")
)

write_files(
    df_RowCount.select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "PROC_CD_SK",
        "CLM_LN_RVNU_CD_SK",
        "ALW_AMT",
        "CHRG_AMT",
        "PAYBL_AMT"
    ),
    f"{adls_path}/load/B_CLM_LN.DENTAL.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

params_ClmLnPK = {
    "CurrRunCycle": get_widget_value("CurrRunCycle","")
}

df_ClmLnPK_output = ClmLnPK(df_Pkey, params_ClmLnPK)

write_files(
    df_ClmLnPK_output.select(
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
        "ITS_SUPLMT_DSCNT_AMT",
        "ITS_SRCHRG_AMT",
        "NDC",
        "NDC_DRUG_FORM_CD",
        "NDC_UNIT_CT",
        "MED_PDX_IND",
        "APC_ID",
        "APC_STTUS_ID",
        "SNOMED_CT_CD",
        "CVX_VCCN_CD"
    ),
    f"{adls_path}/key/FctsClmLnDntlExtr.FctsClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)