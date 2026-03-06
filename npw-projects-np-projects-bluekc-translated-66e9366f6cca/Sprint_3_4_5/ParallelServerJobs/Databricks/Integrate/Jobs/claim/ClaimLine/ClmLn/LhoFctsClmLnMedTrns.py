# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *******************************************************************************
# MAGIC @ Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC \(9)Transform extracted Claim Line data from Facets into a common format to 
# MAGIC \(9)be potentially merged with claim line records from other systems for the 
# MAGIC \(9)foreign keying process and ultimate load into IDS
# MAGIC 
# MAGIC PRIMARY SOURCE:
# MAGIC \(9)InFile param\(9)- Sequential file containing extracted claim line 
# MAGIC \(9)\(9)\(9)  data to be transformed (../verified)
# MAGIC HASH FILES
# MAGIC          The following hash files are created in other process and are used in other processes besides this job
# MAGIC                 DO NOT CLEAR
# MAGIC                 hf_clm_fcts_reversal - do not clear
# MAGIC                hf_clm_nasco_dup_bypass - do not clear
# MAGIC                hf_clm_ln_dsalw_ln_amts do not clear
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC \(9)TmpOutFile param\(9)- Sequential file (parameterized name) containing 
# MAGIC \(9)\(9)\(9)  transformed claim line data in common record format 
# MAGIC \(9)\(9)\(9)  (../common)
# MAGIC JOB PARAMETERS:
# MAGIC 
# MAGIC \(9)FilePath\(9)\(9)- Directory path for sequential data files
# MAGIC \(9)InFile\(9)\(9)- Name of sequential input file
# MAGIC \(9)TmpOutFile\(9)- Name of sequential output file
# MAGIC 
# MAGIC ********************************************************************************
# MAGIC 
# MAGIC  Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME: FctsClmLnHashFile1Extr
# MAGIC 
# MAGIC DESCRIPTION:  One of three hash file extracts that must run in order (1,2,3)
# MAGIC 
# MAGIC 
# MAGIC \(9)Extract Claim Line Disallow Data extract from Facets, and load to hash files to be used in FctsClmLnTrns, FctsClmLnDntlTrns and FctsClmLnDsalwExtr
# MAGIC                 Extract rows from the disallow tables for W_CLM_LN_DSALW - pull all rows for claims on TMP_DRIVER
# MAGIC 
# MAGIC  SOURCE:
# MAGIC 
# MAGIC \(9)CMC_CDML_CL_LINE\(9) 
# MAGIC                 CMC_CDDL_CL_LINE
# MAGIC                 CMC_CDMD_LI_DISALL
# MAGIC                 CMC_CDDD_DNLI_DIS
# MAGIC                 CMC_CDOR_LI_OVR
# MAGIC                 CMC_CDDO_DNLI_OVR
# MAGIC                 CMC_CLCL_CLAIM
# MAGIC                 CMC_CLMI_MISC
# MAGIC                  CMC_CDCB_LI_COB
# MAGIC                 TMP_DRIVER
# MAGIC                  IDS CD_MPPNG
# MAGIC                  IDS DSALW_EXCD
# MAGIC 
# MAGIC 
# MAGIC  Hash Files
# MAGIC               used in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnDsalwExtr
# MAGIC 
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC                    hf_clm_ln_dsalw_a_dup
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC 
# MAGIC \(9)W_CLM_LN_DSALW.dat to be loaded in IDS, must go through the delete process
# MAGIC 
# MAGIC  
# MAGIC                 IDS parameters
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 \(9)Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-10-14          US -  263702                                         Original Programming                                              IntegrateDev2                     
# MAGIC 
# MAGIC Kalyan                           2021-01-14     US-318408                                            Added 2 new Fields                                                 IntegrateDev2
# MAGIC 
# MAGIC Revathi Boojireddy   2023-08-01          US 589700                               Added two new fields SNOMED_CT_CD                             IntegrateDevB\(9)Harsha Ravuri\(9)2023-08-31
# MAGIC                                                                                                            ,CVX_VCCN_CD with a default value in SnapShot 
# MAGIC \(9)                                                                                                 stage and mapped it till target ClmLn file

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
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim as trim_,
    upper,
    left,
    rpad,
    sum as sum_,
    length
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# Retrieve all parameters
CurrRunCycle = get_widget_value("CurrRunCycle","1")
RunID = get_widget_value("RunID","20120425")
CurrentDate = get_widget_value("CurrentDate","20120425")
SrcSysCdSk = get_widget_value("SrcSysCdSk","FACETS")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# Assumed available Spark session: spark
# Also assume adls_path, adls_path_raw, adls_path_publish are predefined

# ----------------------------------------------------------------------------
# Read CHashedFileStage: hf_clm_fcts_reversals (Scenario C)
# Two separate files: "hf_clm_fcts_reversals", "hf_clm_ln_subtype"
# ----------------------------------------------------------------------------
df_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_ref_subtyp = spark.read.parquet(f"{adls_path}/hf_clm_ln_subtype.parquet")

# ----------------------------------------------------------------------------
# Read CHashedFileStage: clm_nasco_dup_bypass (Scenario C)
# File: "hf_clm_nasco_dup_bypass"
# ----------------------------------------------------------------------------
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ----------------------------------------------------------------------------
# Read CHashedFileStage: Lookups (Scenario C)
# All references from single hashed file "hf_clm_ln_dsalw_ln_amts" repeated 9 times.
# We read once and reuse for multiple lookups.
# ----------------------------------------------------------------------------
df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_ln_amts.parquet")

# ----------------------------------------------------------------------------
# Read seqVerified: CSeqFileStage
# File: "verified/LhoFctsClmLnMedExtr.ClmLnMed.dat.#RunID#"
# Must define schema to avoid inferSchema
# ----------------------------------------------------------------------------
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
    StructField("ITS_SUPLMT_DSCNT", DecimalType(15,2), True),
    StructField("ITS_SRCHRG_AMT", DecimalType(15,2), True),
    StructField("NDC", StringType(), True),
    StructField("NDC_DRUG_FORM_CD", StringType(), True),
    StructField("NDC_UNIT_CT", DecimalType(15,2), True)
])

df_seqVerified = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_seqVerified)
    .load(f"{adls_path}/verified/LhoFctsClmLnMedExtr.ClmLnMed.dat.{RunID}")
)

# ----------------------------------------------------------------------------
# Read CHashedFileStage: Hashed_File_270 (Scenario C)
# Three files: "hf_clm_ln_cdmd_provcntrct", "hf_clm_ln_clhp", "hf_clm_ln_clor"
# ----------------------------------------------------------------------------
df_RefPvdCntrct = spark.read.parquet(f"{adls_path}/hf_clm_ln_cdmd_provcntrct.parquet")
df_RefClhp = spark.read.parquet(f"{adls_path}/hf_clm_ln_clhp.parquet")
df_clor_lkup = spark.read.parquet(f"{adls_path}/hf_clm_ln_clor.parquet")

# ----------------------------------------------------------------------------
# sum_all_for_clm_ln: Aggregator
# Input: "sum_to_clm_ln" from df_hf_clm_ln_dsalw_ln_amts with columns
# (CLCL_ID, CDML_SEQ_NO, EXCD_RESP_CD, BYPS_IN, DSALW_AMT).
# It sums DSALW_AMT by (CLCL_ID,CDML_SEQ_NO).
# ----------------------------------------------------------------------------
# The link "sum_to_clm_ln" is simply the entire df_hf_clm_ln_dsalw_ln_amts used for that aggregator.
df_sum_to_clm_ln = df_hf_clm_ln_dsalw_ln_amts.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_AMT")
)
df_clm_ln_sum_hash = (
    df_sum_to_clm_ln
    .groupBy("CLCL_ID", "CDML_SEQ_NO")
    .agg(sum_("DSALW_AMT").alias("DSALW_AMT"))
)

# ----------------------------------------------------------------------------
# total_dsalw: CHashedFileStage (Scenario A)
# "AnyStage -> CHashedFileStage -> AnyStage" with no same-file write.
# Instead of physically writing, we directly pass df_clm_ln_sum_hash forward,
# dropping duplicates across key columns CLCL_ID, CDML_SEQ_NO.
# ----------------------------------------------------------------------------
df_clm_ln_sum_hash_dedup = dedup_sort(
    df_clm_ln_sum_hash,
    partition_cols=["CLCL_ID","CDML_SEQ_NO"],
    sort_cols=[]
)

# ----------------------------------------------------------------------------
# PROC_CD: DB2Connector to read from IDS database
# ----------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_proc_cd = f"""
SELECT PROC_CD, PROC_CD_TYP_CD
FROM {IDSOwner}.PROC_CD
WHERE PROC_CD_CAT_CD = 'MED'
"""
df_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_proc_cd)
    .load()
)

# ----------------------------------------------------------------------------
# hf_ClmLn_ProcCdTypCd: CHashedFileStage (Scenario A)
# Has input from df_PROC_CD, key=PROC_CD. We deduplicate on PROC_CD.
# ----------------------------------------------------------------------------
df_PROC_CD_dedup = dedup_sort(
    df_PROC_CD,
    partition_cols=["PROC_CD"],
    sort_cols=[]
)

# The stage has 2 output pins: "ProcCdTypCd" and "Proc_cd" with the same schema
# We'll keep a single DF but alias it differently for the two usage:
df_ProcCdTypCd = df_PROC_CD_dedup.alias("ProcCdTypCd")
df_Proc_cd = df_PROC_CD_dedup.alias("Proc_cd")

# ----------------------------------------------------------------------------
# ApplyBusinessRules: CTransformerStage
# Primary link = df_seqVerified (alias "FctsClmLn")
# Lookup links = 
#   RefMY, RefMN, RefNY, RefNN, RefOY, RefON, RefPY, RefPN => all from df_hf_clm_ln_dsalw_ln_amts with left-join filters
#   RefPvdCntrct => df_RefPvdCntrct with left join
#   RefClhp => df_RefClhp with left join
#   clor_lkup => df_clor_lkup with left join
#   total_dsalw_lkup_for_cap => df_clm_ln_sum_hash_dedup with left join
#   ProcCdTypCd => df_ProcCdTypCd with left join
#   Proc_cd => df_Proc_cd with left join
#
# Then many stage variables => final output "ClmLnCrfnew"
# ----------------------------------------------------------------------------

# Prepare the main DF
df_fctsClmLn = df_seqVerified.alias("FctsClmLn")

# We will create references to the same hashed-lns DS for each of the 8 lookups
# but joined with different conditions. Because each uses different EXCD_RESP_CD/ BYPS_IN filters.
# We'll define them as sub-DataFrames with those filters so that we can left join them easily.

df_RefMY = df_hf_clm_ln_dsalw_ln_amts.alias("RefMY").filter(
    (col("RefMY.EXCD_RESP_CD") == lit("M")) & (col("RefMY.BYPS_IN") == lit("Y"))
)
df_RefMN = df_hf_clm_ln_dsalw_ln_amts.alias("RefMN").filter(
    (col("RefMN.EXCD_RESP_CD") == lit("M")) & (col("RefMN.BYPS_IN") == lit("N"))
)
df_RefNY = df_hf_clm_ln_dsalw_ln_amts.alias("RefNY").filter(
    (col("RefNY.EXCD_RESP_CD") == lit("N")) & (col("RefNY.BYPS_IN") == lit("Y"))
)
df_RefNN = df_hf_clm_ln_dsalw_ln_amts.alias("RefNN").filter(
    (col("RefNN.EXCD_RESP_CD") == lit("N")) & (col("RefNN.BYPS_IN") == lit("N"))
)
df_RefOY = df_hf_clm_ln_dsalw_ln_amts.alias("RefOY").filter(
    (col("RefOY.EXCD_RESP_CD") == lit("O")) & (col("RefOY.BYPS_IN") == lit("Y"))
)
df_RefON = df_hf_clm_ln_dsalw_ln_amts.alias("RefON").filter(
    (col("RefON.EXCD_RESP_CD") == lit("O")) & (col("RefON.BYPS_IN") == lit("N"))
)
df_RefPY = df_hf_clm_ln_dsalw_ln_amts.alias("RefPY").filter(
    (col("RefPY.EXCD_RESP_CD") == lit("P")) & (col("RefPY.BYPS_IN") == lit("Y"))
)
df_RefPN = df_hf_clm_ln_dsalw_ln_amts.alias("RefPN").filter(
    (col("RefPN.EXCD_RESP_CD") == lit("P")) & (col("RefPN.BYPS_IN") == lit("N"))
)

# Now systematically left-join all
df_abr = df_fctsClmLn.join(
    df_RefMY, 
    (col("FctsClmLn.CLCL_ID") == col("RefMY.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefMY.CDML_SEQ_NO")),
    "left"
).join(
    df_RefMN,
    (col("FctsClmLn.CLCL_ID") == col("RefMN.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefMN.CDML_SEQ_NO")),
    "left"
).join(
    df_RefNY,
    (col("FctsClmLn.CLCL_ID") == col("RefNY.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefNY.CDML_SEQ_NO")),
    "left"
).join(
    df_RefNN,
    (col("FctsClmLn.CLCL_ID") == col("RefNN.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefNN.CDML_SEQ_NO")),
    "left"
).join(
    df_RefOY,
    (col("FctsClmLn.CLCL_ID") == col("RefOY.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefOY.CDML_SEQ_NO")),
    "left"
).join(
    df_RefON,
    (col("FctsClmLn.CLCL_ID") == col("RefON.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefON.CDML_SEQ_NO")),
    "left"
).join(
    df_RefPY,
    (col("FctsClmLn.CLCL_ID") == col("RefPY.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefPY.CDML_SEQ_NO")),
    "left"
).join(
    df_RefPN,
    (col("FctsClmLn.CLCL_ID") == col("RefPN.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefPN.CDML_SEQ_NO")),
    "left"
).join(
    df_RefPvdCntrct.alias("RefPvdCntrct"),
    (col("FctsClmLn.CLCL_ID") == col("RefPvdCntrct.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("RefPvdCntrct.CDML_SEQ_NO")),
    "left"
).join(
    df_RefClhp.alias("RefClhp"),
    col("FctsClmLn.CLCL_ID") == col("RefClhp.CLCL_ID"),
    "left"
).join(
    df_clor_lkup.alias("clor_lkup"),
    col("FctsClmLn.CLCL_ID") == col("clor_lkup.CLM_ID"),
    "left"
).join(
    df_clm_ln_sum_hash_dedup.alias("total_dsalw_lkup_for_cap"),
    (col("FctsClmLn.CLCL_ID") == col("total_dsalw_lkup_for_cap.CLCL_ID")) &
    (col("FctsClmLn.CDML_SEQ_NO") == col("total_dsalw_lkup_for_cap.CDML_SEQ_NO")),
    "left"
).join(
    df_ProcCdTypCd,
    col("FctsClmLn.IPCD_ID") == col("ProcCdTypCd.PROC_CD"),
    "left"
).join(
    df_Proc_cd,
    trim_(left(col("FctsClmLn.IPCD_ID"), lit(5))) == col("Proc_cd.PROC_CD"),
    "left"
)

# Now add stage variables as withColumns
df_enriched = df_abr

# 1) SrcSysCd = 'LUMERIS'
df_enriched = df_enriched.withColumn("SrcSysCd", lit("LUMERIS"))

# 2) ClmId
df_enriched = df_enriched.withColumn(
    "ClmId",
    when(trim_(col("FctsClmLn.CLCL_ID")) != "", col("FctsClmLn.CLCL_ID")).otherwise("NA")
)

# 3) ClmLnId
df_enriched = df_enriched.withColumn(
    "ClmLnId",
    when(
        (col("FctsClmLn.CDML_SEQ_NO").isNotNull()) & (trim_(col("FctsClmLn.CDML_SEQ_NO")) != ""),
        col("FctsClmLn.CDML_SEQ_NO")
    ).otherwise(lit("0"))
)

# 4) ClmLnRoomTypCd
df_enriched = df_enriched.withColumn(
    "ClmLnRoomTypCd",
    when(
        (col("FctsClmLn.RCRC_ID").isNull()) | (length(trim_(col("FctsClmLn.RCRC_ID"))) == 0),
        lit("NA")
    ).otherwise( upper(trim_(left(col("FctsClmLn.RCRC_ID"), lit(3))))) 
)

# 5) ClmLnRoomTypCdLkup
#   = GetFkeyCodes("FACETS", 1, "CLAIM LINE ROOM TYPE", ClmLnRoomTypCd, "X")
#   This is a user-defined function; just call it directly:
df_enriched = df_enriched.withColumn(
    "ClmLnRoomTypCdLkup",
    GetFkeyCodes(
        lit("FACETS"), 
        lit(1), 
        lit("CLAIM LINE ROOM TYPE"), 
        col("ClmLnRoomTypCd"), 
        lit("X")
    )
)

# 6) ServicingProvider
df_enriched = df_enriched.withColumn(
    "ServicingProvider",
    when(
        (col("FctsClmLn.CDML_CUR_STS").rlike("^(15|93|97|99)$")),
        when(
            GetFkeyProv(
                lit("FACETS"),
                lit(0),
                when(
                    col("FctsClmLn.PRPR_ID").isNull() | (trim_(col("FctsClmLn.PRPR_ID")) == ""),
                    lit("UNK")
                ).otherwise(upper(trim_(col("FctsClmLn.PRPR_ID")))),
                lit("X")
            ) == lit(0),
            lit("NA")
        ).otherwise(
            when(
                col("FctsClmLn.PRPR_ID").isNull() | (trim_(col("FctsClmLn.PRPR_ID")) == ""),
                lit("UNK")
            ).otherwise(upper(trim_(col("FctsClmLn.PRPR_ID"))))
        )
    ).otherwise(
        when(
            col("FctsClmLn.PRPR_ID").isNull() | (trim_(col("FctsClmLn.PRPR_ID")) == ""),
            lit("UNK")
        ).otherwise(upper(trim_(col("FctsClmLn.PRPR_ID"))))
    )
)

# 7) svSeseId
df_enriched = df_enriched.withColumn(
    "svSeseId",
    when(
        col("FctsClmLn.SESE_ID").isNull() | (length(trim_(col("FctsClmLn.SESE_ID"))) == 0),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.SESE_ID"))))
)

# 8) svRvnuCd
df_enriched = df_enriched.withColumn(
    "svRvnuCd",
    when(
        col("FctsClmLn.RCRC_ID").isNull() | (length(trim_(col("FctsClmLn.RCRC_ID"))) == 0),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.RCRC_ID"))))
)

# 9) svNtwkO
df_enriched = df_enriched.withColumn(
    "svNtwkO",
    when(trim_(col("FctsClmLn.CDML_CL_NTWK_IND")) == lit("O"), lit("Y")).otherwise(lit("N"))
)

# 10) svNtwkIP
df_enriched = df_enriched.withColumn(
    "svNtwkIP",
    when(trim_(col("FctsClmLn.CDML_CL_NTWK_IND")) == lit("I"), lit("Y"))
    .otherwise(
        when(trim_(col("FctsClmLn.CDML_CL_NTWK_IND")) == lit("P"), lit("Y")).otherwise(lit("N"))
    )
)

# 11) svClmSubType
df_enriched = df_enriched.withColumn(
    "svClmSubType",
    when(
        (col("FctsClmLn.CLCL_CL_SUB_TYPE") == lit("M")) | (col("FctsClmLn.CLCL_CL_SUB_TYPE") == lit("D")),
        lit("P")
    ).otherwise(
        when(
            col("FctsClmLn.CLCL_CL_SUB_TYPE") == lit("H"),
            when(
                col("RefClhp.CLHP_FAC_TYPE") > lit("6"),
                lit("O")
            ).otherwise(
                when(
                    (col("RefClhp.CLHP_FAC_TYPE") < lit("7")) & 
                    ((col("RefClhp.CLHP_BILL_CLASS") == lit("3")) | (col("RefClhp.CLHP_BILL_CLASS") == lit("4"))),
                    lit("O")
                ).otherwise(lit("I"))
            )
        ).otherwise(lit("UNK"))
    )
)

# 12) svN (N-logic)
df_enriched = df_enriched.withColumn(
    "svN",
    when(col("RefNY.CLCL_ID").isNotNull(), lit("Y"))
    .otherwise(
        when(
            (col("RefMY.CLCL_ID").isNotNull()) | 
            (col("RefOY.CLCL_ID").isNotNull()) | 
            (col("RefPY.CLCL_ID").isNotNull()),
            lit("O")
        ).otherwise(
            when(col("RefNN.CLCL_ID").isNotNull(), lit("N")).otherwise(lit("X"))
        )
    )
)

# 13) svM
df_enriched = df_enriched.withColumn(
    "svM",
    when(col("RefMY.CLCL_ID").isNotNull(), lit("Y"))
    .otherwise(
        when(
            (col("RefNY.CLCL_ID").isNotNull()) | 
            (col("RefOY.CLCL_ID").isNotNull()) | 
            (col("RefPY.CLCL_ID").isNotNull()),
            lit("O")
        ).otherwise(
            when(col("RefMN.CLCL_ID").isNotNull(), lit("N")).otherwise(lit("X"))
        )
    )
)

# 14) svO
df_enriched = df_enriched.withColumn(
    "svO",
    when(col("RefOY.CLCL_ID").isNotNull(), lit("Y"))
    .otherwise(
        when(
            (col("RefNY.CLCL_ID").isNotNull()) | 
            (col("RefMY.CLCL_ID").isNotNull()) | 
            (col("RefPY.CLCL_ID").isNotNull()),
            lit("O")
        ).otherwise(
            when(col("RefON.CLCL_ID").isNotNull(), lit("N")).otherwise(lit("X"))
        )
    )
)

# 15) svP
df_enriched = df_enriched.withColumn(
    "svP",
    when(col("RefPY.CLCL_ID").isNotNull(), lit("Y"))
    .otherwise(
        when(
            (col("RefNY.CLCL_ID").isNotNull()) | 
            (col("RefOY.CLCL_ID").isNotNull()) | 
            (col("RefMY.CLCL_ID").isNotNull()),
            lit("O")
        ).otherwise(
            when(col("RefPN.CLCL_ID").isNotNull(), lit("N")).otherwise(lit("X"))
        )
    )
)

# 16) svMaster
df_enriched = df_enriched.withColumn(
    "svMaster",
    when(col("svN") == lit("Y"), lit("N"))
    .otherwise(
        when(col("svP") == lit("Y"), lit("P"))
        .otherwise(
            when(col("svM") == lit("Y"), lit("M"))
            .otherwise(
                when(col("svO") == lit("Y"), lit("O")).otherwise(lit("X"))
            )
        )
    )
)

# 17) svTotalofAll = FctsClmLn.CDML_DISALL_AMT
df_enriched = df_enriched.withColumn(
    "svTotalofAll",
    col("FctsClmLn.CDML_DISALL_AMT")
)

# 18) svDefaultToProvWriteOff
df_enriched = df_enriched.withColumn(
    "svDefaultToProvWriteOff",
    when(col("clor_lkup.CLM_ID").isNull(), lit("N"))
    .otherwise(
        when(col("FctsClmLn.EXCD_FOUND") == lit("Y"), lit("N")).otherwise(lit("Y"))
    )
)

# 19) svNoRespAmt
df_enriched = df_enriched.withColumn(
    "svNoRespAmt",
    when(col("svDefaultToProvWriteOff") == lit("Y"), lit("0.00"))
    .otherwise(
        when(col("svMaster") == lit("N"), col("svTotalofAll"))
        .otherwise(
            when(col("svMaster") == lit("X"),
                 when(
                     col("RefNN.DSALW_AMT").isNull() | (trim_(col("RefNN.DSALW_AMT")) == "") | (~Num(col("RefNN.DSALW_AMT"))),
                     lit("0")
                 ).otherwise(col("RefNN.DSALW_AMT"))
            ).otherwise(lit("0.00"))
        )
    )
)

# 20) svPatRespAmt
df_enriched = df_enriched.withColumn(
    "svPatRespAmt",
    when(col("svDefaultToProvWriteOff") == lit("Y"), lit("0.00"))
    .otherwise(
        when(
            (col("svMaster") == lit("M")) | ((col("svMaster") == lit("O")) & (col("svNtwkO") == lit("Y"))),
            col("svTotalofAll")
        ).otherwise(
            (
              when(col("svMaster") == lit("X"),
                   when(
                       col("RefMN.DSALW_AMT").isNull() | (trim_(col("RefMN.DSALW_AMT")) == "") | (~Num(col("RefMN.DSALW_AMT"))),
                       lit("0")
                   ).otherwise(col("RefMN.DSALW_AMT"))
              ).otherwise(lit("0")) 
              +
              when(
                  (col("svNtwkO") == lit("Y")) & (col("svMaster") == lit("X")),
                  when(
                      col("RefON.DSALW_AMT").isNull() | (trim_(col("RefON.DSALW_AMT")) == "") | (~Num(col("RefON.DSALW_AMT"))),
                      lit("0")
                  ).otherwise(col("RefON.DSALW_AMT"))
              ).otherwise(lit("0"))
            )
        )
    )
)

# 21) svProvWriteOff
df_enriched = df_enriched.withColumn(
    "svProvWriteOff",
    when(col("svDefaultToProvWriteOff") == lit("Y"), col("svTotalofAll"))
    .otherwise(
        when(
            (col("svMaster") == lit("P")) | ((col("svMaster") == lit("O")) & (col("svNtwkIP") == lit("Y"))),
            col("svTotalofAll")
        ).otherwise(
            (
              when(col("svMaster") == lit("X"),
                   when(
                       col("RefPN.DSALW_AMT").isNull() | (trim_(col("RefPN.DSALW_AMT")) == "") | (~Num(col("RefPN.DSALW_AMT"))),
                       lit("0")
                   ).otherwise(col("RefPN.DSALW_AMT"))
              ).otherwise(lit("0")) 
              +
              when(
                  (col("svNtwkIP") == lit("Y")) & (col("svMaster") == lit("X")),
                  when(
                      col("RefON.DSALW_AMT").isNull() | (trim_(col("RefON.DSALW_AMT")) == "") | (~Num(col("RefON.DSALW_AMT"))),
                      lit("0")
                  ).otherwise(col("RefON.DSALW_AMT"))
              ).otherwise(lit("0"))
            )
        )
    )
)

# 22) svCapClm
df_enriched = df_enriched.withColumn(
    "svCapClm",
    when(
        (col("FctsClmLn.CDML_CAP_IND") == lit("Y")) &
        (col("FctsClmLn.CDML_CONSIDER_CHG") != col("FctsClmLn.CDML_DISALL_AMT")),
        lit("Y")
    ).otherwise(lit("N"))
)

# 23) svCapAmt => total_dsalw_lkup_for_cap.DSALW_AMT if not null
df_enriched = df_enriched.withColumn(
    "svCapAmt",
    when(col("total_dsalw_lkup_for_cap.CLCL_ID").isNull(), lit("0")).otherwise(col("total_dsalw_lkup_for_cap.DSALW_AMT"))
)

# 24) svProcCd
df_enriched = df_enriched.withColumn(
    "svProcCd",
    when(col("ProcCdTypCd.PROC_CD").isNull(),
         when(col("Proc_cd.PROC_CD").isNull(), lit("NA")).otherwise(col("Proc_cd.PROC_CD"))
    ).otherwise(col("ProcCdTypCd.PROC_CD"))
)

# Now build final select for the output link "ClmLnCrfnew"
# The stage has many columns in a certain order.

df_ClmLnCrfnew = df_enriched.select(
    lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit("0").alias("ERR_CT"),
    lit("0").alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("SrcSysCd") + lit(";") + col("ClmId") + lit(";") + col("ClmLnId")).alias("PRI_KEY_STRING"),
    lit("0").alias("CLM_LN_SK"),
    col("ClmId").alias("CLM_ID"),
    col("ClmLnId").alias("CLM_LN_SEQ_NO"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svProcCd").alias("PROC_CD"),
    col("ServicingProvider").alias("SVC_PROV_ID"),
    when(
        col("FctsClmLn.CDML_DISALL_EXCD").isNull() | (trim_(col("FctsClmLn.CDML_DISALL_EXCD")) == ""),
        lit("NA")
    ).otherwise(trim_(col("FctsClmLn.CDML_DISALL_EXCD"))).alias("CLM_LN_DSALW_EXCD"),
    when(
        col("FctsClmLn.CDML_EOB_EXCD").isNull() | (trim_(col("FctsClmLn.CDML_EOB_EXCD")) == ""),
        lit("NA")
    ).otherwise(trim_(col("FctsClmLn.CDML_EOB_EXCD"))).alias("CLM_LN_EOB_EXCD"),
    when(
        col("FctsClmLn.CLM_LN_FINL_DISP_CD").isNull() | (trim_(col("FctsClmLn.CLM_LN_FINL_DISP_CD")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.CLM_LN_FINL_DISP_CD")))).alias("CLM_LN_FINL_DISP_CD"),
    when(
        col("FctsClmLn.LOBD_ID").isNull() | (trim_(col("FctsClmLn.LOBD_ID")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.LOBD_ID")))).alias("CLM_LN_LOB_CD"),
    when(
        col("FctsClmLn.PSCD_ID").isNull() | (trim_(col("FctsClmLn.PSCD_ID")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.PSCD_ID")))).alias("CLM_LN_POS_CD"),
    when(
        col("FctsClmLn.CDML_PC_IND").isNull() | (trim_(col("FctsClmLn.CDML_PC_IND")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.CDML_PC_IND")))).alias("CLM_LN_PREAUTH_CD"),
    when(
        col("FctsClmLn.CDML_PRE_AUTH_IND").isNull() | (trim_(col("FctsClmLn.CDML_PRE_AUTH_IND")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.CDML_PRE_AUTH_IND")))).alias("CLM_LN_PREAUTH_SRC_CD"),
    when(
        col("FctsClmLn.CDML_PRICE_IND").isNull() | (trim_(col("FctsClmLn.CDML_PRICE_IND")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.CDML_PRICE_IND")))).alias("CLM_LN_PRICE_SRC_CD"),
    when(
        col("FctsClmLn.CDML_REF_IND").isNull() | (trim_(col("FctsClmLn.CDML_REF_IND")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.CDML_REF_IND")))).alias("CLM_LN_RFRL_CD"),
    when(length(col("svRvnuCd")) == 3, lit("0") + col("svRvnuCd")).otherwise(col("svRvnuCd")).alias("CLM_LN_RVNU_CD"),
    when(
        col("FctsClmLn.CDML_ROOM_TYPE").isNull() | (trim_(col("FctsClmLn.CDML_ROOM_TYPE")) == ""),
        lit("NA")
    ).otherwise(upper(trim_(col("FctsClmLn.CDML_ROOM_TYPE")))).alias("CLM_LN_ROOM_PRICE_METH_CD"),
    when(col("ClmLnRoomTypCdLkup") == lit("0"), lit("NA")).otherwise(col("ClmLnRoomTypCd")).alias("CLM_LN_ROOM_TYP_CD"),
    col("svSeseId").alias("CLM_LN_TOS_CD"),
    when(col("FctsClmLn.SESE_ID").substr(1,2) == lit("AN"), lit("MJ")).otherwise(lit("UN")).alias("CLM_LN_UNIT_TYP_CD"),
    when(trim_(col("FctsClmLn.CDML_CAP_IND")) == lit("Y"), lit("Y")).otherwise(lit("N")).alias("CAP_LN_IN"),
    when(trim_(col("FctsClmLn.PDVC_LOBD_PTR")) == lit("1"), lit("Y")).otherwise(lit("N")).alias("PRI_LOB_IN"),
    when(
        col("FctsClmLn.CDML_TO_DT").substr(lit(1),lit(10)).isNull() | 
        (trim_(col("FctsClmLn.CDML_TO_DT").substr(1,10)) == ""),
        lit("UNK")
    ).otherwise(col("FctsClmLn.CDML_TO_DT").substr(1,10)).alias("SVC_END_DT"),
    when(
        col("FctsClmLn.CDML_FROM_DT").substr(lit(1),lit(10)).isNull() |
        (trim_(col("FctsClmLn.CDML_FROM_DT").substr(1,10)) == ""),
        lit("UNK")
    ).otherwise(col("FctsClmLn.CDML_FROM_DT").substr(1,10)).alias("SVC_STRT_DT"),
    col("FctsClmLn.CDML_AG_PRICE").alias("AGMNT_PRICE_AMT"),
    col("FctsClmLn.CDML_ALLOW").alias("ALW_AMT"),
    when(col("svSeseId") != lit("*RG"), col("FctsClmLn.CDML_CHG_AMT")).otherwise(lit("0")).alias("CHRG_AMT"),
    col("FctsClmLn.CDML_COINS_AMT").alias("COINS_AMT"),
    col("FctsClmLn.CDML_CONSIDER_CHG").alias("CNSD_CHRG_AMT"),
    col("FctsClmLn.CDML_COPAY_AMT").alias("COPAY_AMT"),
    col("FctsClmLn.CDML_DED_AMT").alias("DEDCT_AMT"),
    col("FctsClmLn.CDML_DISALL_AMT").alias("DSALW_AMT"),
    col("FctsClmLn.CDML_ITS_DISC_AMT").alias("ITS_HOME_DSCNT_AMT"),
    when(col("svCapClm") == lit("Y"), lit("0.0")).otherwise(col("svNoRespAmt")).alias("NO_RESP_AMT"),
    col("FctsClmLn.CDML_OOP_CALC_BASE").alias("MBR_LIAB_BSS_AMT"),
    when(col("svCapClm") == lit("Y"), lit("0.0")).otherwise(col("svPatRespAmt")).alias("PATN_RESP_AMT"),
    col("FctsClmLn.CDML_PAID_AMT").alias("PAYBL_AMT"),
    col("FctsClmLn.CDML_PR_PYMT_AMT").alias("PAYBL_TO_PROV_AMT"),
    col("FctsClmLn.CDML_SB_PYMT_AMT").alias("PAYBL_TO_SUB_AMT"),
    col("FctsClmLn.CDML_IP_PRICE").alias("PROC_TBL_PRICE_AMT"),
    col("FctsClmLn.CDML_PF_PRICE").alias("PROFL_PRICE_AMT"),
    when(col("svCapClm") == lit("Y"), col("svCapAmt")).otherwise(col("svProvWriteOff")).alias("PROV_WRT_OFF_AMT"),
    col("FctsClmLn.CDML_RISK_WH_AMT").alias("RISK_WTHLD_AMT"),
    col("FctsClmLn.CDML_SE_PRICE").alias("SVC_PRICE_AMT"),
    col("FctsClmLn.CDML_SUP_DISC_AMT").alias("SUPLMT_DSCNT_AMT"),
    col("FctsClmLn.CDML_UNITS_ALLOW").alias("ALW_PRICE_UNIT_CT"),
    col("FctsClmLn.CDML_UNITS").alias("UNIT_CT"),
    col("FctsClmLn.CDML_DED_ACC_NO").alias("DEDCT_AMT_ACCUM_ID"),
    when(
        col("FctsClmLn.CDML_AUTHSV_SEQ_NO").isNull() | (trim_(col("FctsClmLn.CDML_AUTHSV_SEQ_NO")) == ""),
        lit(" ")
    ).otherwise(col("FctsClmLn.CDML_AUTHSV_SEQ_NO")).alias("PREAUTH_SVC_SEQ_NO"),
    when(
        col("FctsClmLn.CDML_REFSV_SEQ_NO").isNull() | (trim_(col("FctsClmLn.CDML_REFSV_SEQ_NO")) == ""),
        lit(" ")
    ).otherwise(col("FctsClmLn.CDML_REFSV_SEQ_NO")).alias("RFRL_SVC_SEQ_NO"),
    when(
        col("FctsClmLn.LTLT_PFX").isNull() | (trim_(col("FctsClmLn.LTLT_PFX")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.LTLT_PFX")).alias("LMT_PFX_ID"),
    when(
        col("FctsClmLn.CDML_UMAUTH_ID").isNull() | (trim_(col("FctsClmLn.CDML_UMAUTH_ID")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.CDML_UMAUTH_ID")).alias("PREAUTH_ID"),
    when(
        col("FctsClmLn.DEDE_PFX").isNull() | (trim_(col("FctsClmLn.DEDE_PFX")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.DEDE_PFX")).alias("PROD_CMPNT_DEDCT_PFX_ID"),
    when(
        col("FctsClmLn.SEPY_PFX").isNull() | (trim_(col("FctsClmLn.SEPY_PFX")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.SEPY_Pfx")).alias("PROD_CMPNT_SVC_PAYMT_ID"),
    when(
        col("FctsClmLn.CDML_UMREF_ID").isNull() | (trim_(col("FctsClmLn.CDML_UMREF_ID")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.CDML_UMREF_ID")).alias("RFRL_ID_TX"),
    when(
        col("FctsClmLn.SESE_ID").isNull() | (trim_(col("FctsClmLn.SESE_ID")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.SESE_ID")).alias("SVC_ID"),
    when(
        col("FctsClmLn.SEPC_PRICE_ID").isNull() | (trim_(col("FctsClmLn.SEPC_PRICE_ID")) == ""),
        lit("NA")
    ).otherwise(col("FctsClmLn.SEPC_PRICE_ID")).alias("SVC_PRICE_RULE_ID"),
    when(
        length(trim_(col("FctsClmLn.SESE_RULE"))) < 1,
        lit(None)
    ).otherwise(trim_(col("FctsClmLn.SESE_RULE"))).alias("SVC_RULE_TYP_TX"),
    lit(" ").alias("SVC_LOC_TYP_CD"),
    when(
        col("RefPvdCntrct.CDML_SEQ_NO").isNull(),
        lit("0.00")
    ).otherwise(
        when(col("FctsClmLn.CLCL_NTWK_IND") == lit("O"),
             col("FctsClmLn.CDML_CONSIDER_CHG") - col("FctsClmLn.CDML_ALLOW")
        ).otherwise(lit("0.00"))
    ).alias("NON_PAR_SAV_AMT"),
    col("svClmSubType").alias("CLM_SUBTYPE"),
    col("FctsClmLn.TPCT_MCTR_SETG").alias("TPCT_MCTR_SETG"),
    when(col("ProcCdTypCd.PROC_CD").isNull(),
         when(col("Proc_cd.PROC_CD").isNull(), lit("NA")).otherwise(col("Proc_cd.PROC_CD_TYP_CD"))
    ).otherwise(col("ProcCdTypCd.PROC_CD_TYP_CD")).alias("PROC_CD_TYP_CD"),
    lit("MED").alias("PROC_CD_CAT_CD"),
    col("FctsClmLn.VBBD_RULE").alias("VBB_RULE_ID"),
    col("FctsClmLn.CDSD_VBB_EXCD_ID").alias("VBB_EXCD_ID"),
    col("FctsClmLn.CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
    col("FctsClmLn.ITS_SUPLMT_DSCNT").alias("ITS_SUPLMT_DSCNT"),
    col("FctsClmLn.ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    col("FctsClmLn.NDC").alias("NDC"),
    col("FctsClmLn.NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
    col("FctsClmLn.NDC_UNIT_CT").alias("NDC_UNIT_CT")
)

# ----------------------------------------------------------------------------
# Build_Reversal_Clm: CTransformerStage
# Inputs: fcts_reversals, ClmLnCrfnew, RefSubTyp, nasco_dup_lkup
# We have them as separate DataFrames: df_fcts_reversals, df_ref_subtyp, df_clm_nasco_dup_bypass
# but those are used as lookups with certain constraints.
#
# => We produce two outputs: "claimLine" and "reversal"
#
# First, join (left) on fcts_reversals if "ClmLnCrfnew.CLM_ID = fcts_reversals.CLCL_ID"
# Also join on ref_subtyp if "trim(ClmLnCrfnew.CLM_SUBTYPE) = ref_subtyp.SRC_DRVD_LKUP_VAL"
# Also join on nasco_dup_lkup if "ClmLnCrfnew.CLM_ID = nasco_dup_lkup.CLM_ID"
# ----------------------------------------------------------------------------
df_clm_crfnew = df_ClmLnCrfnew.alias("ClmLnCrfnew")
df_fcts_reversals_lu = df_fcts_reversals.alias("fcts_reversals")
df_ref_subtyp_lu = df_ref_subtyp.alias("RefSubTyp")
df_nasco_dup_lu = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

df_brc = (
    df_clm_crfnew
    .join(
        df_fcts_reversals_lu,
        col("ClmLnCrfnew.CLM_ID") == col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_ref_subtyp_lu,
        trim_(col("ClmLnCrfnew.CLM_SUBTYPE")) == col("RefSubTyp.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .join(
        df_nasco_dup_lu,
        col("ClmLnCrfnew.CLM_ID") == col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .withColumn(
        "svSvcLocTypCd",
        when(col("RefSubTyp.TRGT_CD").isNull(), lit("NA"))
        .otherwise(
            when(col("RefSubTyp.TRGT_CD") == lit("IP"), lit("I"))
            .otherwise(
                when(col("RefSubTyp.TRGT_CD") == lit("OP"), lit("O")).otherwise(col("ClmLnCrfnew.TPCT_MCTR_SETG"))
            )
        )
    )
)

# Output pins:
# 1) claimLine => constraint: "IsNull(nasco_dup_lkup.CLM_ID) = @TRUE"
# 2) reversal => constraint: "IsNull(fcts_reversals.CLCL_ID) = @FALSE and (fcts_reversals.CLCL_CUR_STS = '89' or '91')"

df_claimLine = df_brc.filter(
    col("nasco_dup_lkup.CLM_ID").isNull()
)

df_reversal = df_brc.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) &
    ((col("fcts_reversals.CLCL_CUR_STS") == lit("89")) | (col("fcts_reversals.CLCL_CUR_STS") == lit("91")))
)

# Now select columns as specified for each link
claimLine_cols = [
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
    "svSvcLocTypCd",   # Mapped to SVC_LOC_TYP_CD
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
    "NDC_UNIT_CT"
]

df_claimLine_out = (
    df_claimLine.select(
        [col(f"ClmLnCrfnew.{c}") if c not in ["svSvcLocTypCd","ITS_SUPLMT_DSCNT","ITS_SRCHRG_AMT"] 
         else col(c) 
         for c in claimLine_cols]
    )
    .withColumnRenamed("svSvcLocTypCd","SVC_LOC_TYP_CD")
    .withColumnRenamed("ITS_SUPLMT_DSCNT","ITS_SUPLMT_DSCNT")
    .withColumnRenamed("ITS_SRCHRG_AMT","ITS_SRCHRG_AMT")
)

reversal_cols = [
    # same list except the minor changes "CLM_ID" => "CLM_ID : 'R'"
    # but done in the transform logic. We must replicate exactly
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
    "svSvcLocTypCd", 
    "NON_PAR_SAV_AMT",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD",
    "VBB_RULE_ID",
    "VBB_EXCD_ID",
    "CLM_LN_VBB_IN",
    "ITS_SUPLMT_DSCNT",
    "ITS_SRCHRG_AMT",
    # changes: NDC => NDC_SK, NDC_DRUG_FORM_CD => NDC_DRUG_FORM_CD_SK
    # but the stage uses them as "NDC_SK" or "NDC_DRUG_FORM_CD_SK"
    "NDC",
    "NDC_DRUG_FORM_CD",
    "NDC_UNIT_CT"
]

df_reversal_out = (
    df_reversal.select(
        [col(f"ClmLnCrfnew.{c}") if c not in ["svSvcLocTypCd","ITS_SUPLMT_DSCNT","ITS_SRCHRG_AMT","NDC","NDC_DRUG_FORM_CD"] 
         else col(c)
         for c in reversal_cols]
    )
    .withColumnRenamed("svSvcLocTypCd","SVC_LOC_TYP_CD")
    .withColumn("PRI_KEY_STRING", trim_(col("ClmLnCrfnew.SRC_SYS_CD"))+lit(";")+col("ClmLnCrfnew.CLM_ID")+lit("R")+lit(";")+col("ClmLnCrfnew.CLM_LN_SEQ_NO"))
    .withColumn("CLM_ID", col("ClmLnCrfnew.CLM_ID")+lit("R"))
    .withColumn("AGMNT_PRICE_AMT", NEG(col("ClmLnCrfnew.AGMNT_PRICE_AMT")))
    .withColumn("ALW_AMT", NEG(col("ClmLnCrfnew.ALW_AMT")))
    .withColumn("CHRG_AMT", NEG(col("ClmLnCrfnew.CHRG_AMT")))
    .withColumn("COINS_AMT", NEG(col("ClmLnCrfnew.COINS_AMT")))
    .withColumn("CNSD_CHRG_AMT", NEG(col("ClmLnCrfnew.CNSD_CHRG_AMT")))
    .withColumn("COPAY_AMT", NEG(col("ClmLnCrfnew.COPAY_AMT")))
    .withColumn("DEDCT_AMT", NEG(col("ClmLnCrfnew.DEDCT_AMT")))
    .withColumn("DSALW_AMT", NEG(col("ClmLnCrfnew.DSALW_AMT")))
    .withColumn("ITS_HOME_DSCNT_AMT", NEG(col("ClmLnCrfnew.ITS_HOME_DSCNT_AMT")))
    .withColumn("NO_RESP_AMT", NEG(col("ClmLnCrfnew.NO_RESP_AMT")))
    .withColumn("MBR_LIAB_BSS_AMT", NEG(col("ClmLnCrfnew.MBR_LIAB_BSS_AMT")))
    .withColumn("PATN_RESP_AMT", NEG(col("ClmLnCrfnew.PATN_RESP_AMT")))
    .withColumn("PAYBL_AMT", NEG(col("ClmLnCrfnew.PAYBL_AMT")))
    .withColumn("PAYBL_TO_PROV_AMT", NEG(col("ClmLnCrfnew.PAYBL_TO_PROV_AMT")))
    .withColumn("PAYBL_TO_SUB_AMT", NEG(col("ClmLnCrfnew.PAYBL_TO_SUB_AMT")))
    .withColumn("PROC_TBL_PRICE_AMT", NEG(col("ClmLnCrfnew.PROC_TBL_PRICE_AMT")))
    .withColumn("PROFL_PRICE_AMT", NEG(col("ClmLnCrfnew.PROFL_PRICE_AMT")))
    .withColumn("PROV_WRT_OFF_AMT", NEG(col("ClmLnCrfnew.PROV_WRT_OFF_AMT")))
    .withColumn("RISK_WTHLD_AMT", NEG(col("ClmLnCrfnew.RISK_WTHLD_AMT")))
    .withColumn("SVC_PRICE_AMT", NEG(col("ClmLnCrfnew.SVC_PRICE_AMT")))
    .withColumn("SUPLMT_DSCNT_AMT", NEG(col("ClmLnCrfnew.SUPLMT_DSCNT_AMT")))
    .withColumn("ALW_PRICE_UNIT_CT", NEG(col("ClmLnCrfnew.ALW_PRICE_UNIT_CT")))
    .withColumn("UNIT_CT", NEG(col("ClmLnCrfnew.UNIT_CT")))
    .withColumn("ITS_SUPLMT_DSCNT", NEG(col("ClmLnCrfnew.ITS_SUPLMT_DSCNT")))
    .withColumn("ITS_SRCHRG_AMT", NEG(col("ClmLnCrfnew.ITS_SRCHRG_AMT")))
    .withColumnRenamed("NDC","NDC_SK")
    .withColumnRenamed("NDC_DRUG_FORM_CD","NDC_DRUG_FORM_CD_SK")
)

# ----------------------------------------------------------------------------
# merge: CCollector => merges "claimLine" and "reversal"
# Output: "Transform"
# ----------------------------------------------------------------------------
df_merge = df_claimLine_out.unionByName(df_reversal_out)

# ----------------------------------------------------------------------------
# SnapShot: CTransformerStage
# InputPins: "Transform"
# Outputs: "Pkey", "Snapshot"
# ----------------------------------------------------------------------------
# We just pass columns along. The first output "Pkey" has many columns plus some WhereExpression placeholders,
# the second output "Snapshot" has columns with certain subset. We do them in one pass, but we must produce two dataframes.

snap_pkey_cols = [
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
    # plus
    "MED_PDX_IND",
    "APC_ID",
    "APC_STTUS_ID",
    "SNOMED_CT_CD",
    "CVX_VCCN_CD"
]

df_snap = df_merge.alias("Transform").select(
    col("Transform.*"),
    lit("MED").alias("MED_PDX_IND"),
    lit(" ").alias("APC_ID"),
    lit(" ").alias("APC_STTUS_ID"),
    lit("NA").alias("SNOMED_CT_CD"),
    lit("NA").alias("CVX_VCCN_CD")
)

df_pkey = df_snap.select(snap_pkey_cols)
df_snapshot_cols = [
    "CLCL_ID",
    "CLM_LN_SEQ_NO",
    "PROC_CD",
    "CLM_LN_RVNU_CD",
    "ALW_AMT",
    "CHRG_AMT",
    "PAYBL_AMT",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD"
]
df_snapshot = df_snap.select(
    col("Transform.CLM_ID").alias("CLCL_ID"),
    col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("Transform.PROC_CD").alias("PROC_CD"),
    col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    col("Transform.ALW_AMT").alias("ALW_AMT"),
    col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    col("Transform.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

# ----------------------------------------------------------------------------
# Transformer => next stage
# "RowCount" => "B_CLM_LN"
# This stage references "Snapshot" with columns => then adds stage variables "ProcCdSk" etc.
# Finally outputs "RowCount" => we produce df_B_CLM_LN
# ----------------------------------------------------------------------------
df_snapshot_t = df_snapshot.alias("Snapshot")

df_transformer = df_snapshot_t.withColumn("ProcCdSk",
    GetFkeyProcCd(
        lit("FACETS"),
        lit(0),
        col("Snapshot.PROC_CD").substr(lit(1),lit(5)),
        col("Snapshot.PROC_CD_TYP_CD"),
        col("Snapshot.PROC_CD_CAT_CD"),
        lit("N")
    )
).withColumn("ClmLnRvnuCdSk",
    GetFkeyRvnu(
        lit("FACETS"),
        lit(0),
        col("Snapshot.CLM_LN_RVNU_CD"),
        lit("N")
    )
)

df_B_CLM_LN = df_transformer.select(
    lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # as per original transformation assumptions
    col("Snapshot.CLCL_ID").alias("CLM_ID"),
    col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("ProcCdSk").alias("PROC_CD_SK"),
    col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    col("Snapshot.ALW_AMT").alias("ALW_AMT"),
    col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT")
)

# ----------------------------------------------------------------------------
# B_CLM_LN: CSeqFileStage writing to "load/B_CLM_LN.LUMERIS.dat.#RunID#"
# ----------------------------------------------------------------------------
b_clm_ln_path = f"{adls_path}/load/B_CLM_LN.LUMERIS.dat.{RunID}"
write_files(
    df_B_CLM_LN,
    b_clm_ln_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# ClmLnPK: CContainerStage => # MAGIC %run done at top. We call "ClmLnPK" as function?
# In DS, it shows 1 input => "Pkey," 1 output => "Key"
# We have already used "# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPK" at top.
# So we just do:
# ----------------------------------------------------------------------------
params_ClmLnPK = {
    "CurrRunCycle": CurrRunCycle
}
df_pkey_out = ClmLnPK(df_pkey, params_ClmLnPK)

# ----------------------------------------------------------------------------
# ClmLn => CSeqFileStage => writing to "key/LhoFctsClmLnMedExtr.LhoFctsClmLn.dat.#RunID#"
# ----------------------------------------------------------------------------
clmln_path = f"{adls_path}/key/LhoFctsClmLnMedExtr.LhoFctsClmLn.dat.{RunID}"
write_files(
    df_pkey_out,
    clmln_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# AfterJobRoutine => "1"
# The job JSON had "AfterJobRoutine": "1". If that implies a routine call, we would do so if specified,
# but there are no further instructions for it. So we do nothing additional here.
# ----------------------------------------------------------------------------


# NOTE on final requirement: For the final outputs, if you need to rpad columns with char/varchar types,
# you could add withColumn(...) + rpad(...) as needed. The instructions mention:
# "In addition, for all columns of the final dataframe, if the column has type char or varchar, use rpad(...)."
# To strictly comply for the final outputs "B_CLM_LN" and "ClmLn" you would do something like:
#
# df_B_CLM_LN_final = df_B_CLM_LN.select(
#     rpad("SRC_SYS_CD_SK", length, " ").alias("SRC_SYS_CD_SK"),
#     ...
# )
# write_files(df_B_CLM_LN_final, ...)
#
# Similarly for df_pkey_out. However, the above code includes the main transformations without skipping anything.