# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC 2;hf_fcts_alpha_pfx_extr;hf_fcts_plnprofl_ids_cds
# MAGIC 
# MAGIC lookups:
# MAGIC hf_ids_pln_profl_cds
# MAGIC hf_fcts_alpha_pfx_extr
# MAGIC 
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  Ids?????????Seq
# MAGIC 
# MAGIC PROCESSING:   Extract Facets ITPP PLN PRFL data and lookup codes for IDS PLN_PROFL table load.  This job is run daily and it is a complete replace of the IDS table.  
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      \(9)Change Description                                        \(9)Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      \(9)-----------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Terri O'Bryan            2010-07-09       TTR-816 \(9)\(9)Original Programming.                                             \(9)IntegrateNewDevl       
# MAGIC Kalyan Neelam        2011-09-21        TTR 816                       Removed AlphaPfxCdSk lookup to get the SK,                                               Brent Leland              10-05-2011
# MAGIC                                                                                                also removed all the SK fields, changed them to get the SK from the FKey job .
# MAGIC                                                                                                Added Null check condition for the fields where needed.
# MAGIC                                                                                                Added Error processing.    
# MAGIC 
# MAGIC Anoop Nair               2022-03-08         S2S Remediation      Added BCBS and FACETS DSN Connection parameters      IntegrateDev5          Jaideep Mankala     04/26/2022

# MAGIC Records from IDS ALPHA PFX and CD_MPPNG retrieved to obtain codes
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, length, upper, when, coalesce
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RunID = get_widget_value('RunID','20100902A')
RunCycle = get_widget_value('RunCycle','101')
CurrDate = get_widget_value('CurrDate','2011-09-11')

# Read from the dummy table instead of hashed file "hf_pln_profl" (Scenario B read step)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_pln_profl_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, ALPHA_PFX_CD, ALPHA_PFX_LOCAL_PLN_CD, CLM_SUBTYP_CD, EFF_DT_SK, RCPT_INCUR_STRT_DT_SK, CRT_RUN_CYC_EXCTN_SK, PLN_PROFL_SK FROM IDS.dummy_hf_pln_profl"
    )
    .load()
)

# Read from Facets (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_facets = (
    f"SELECT ITPP_PRIMARY_ACCES, ITPP_LOCAL_PLAN_CD, ITPP_CLAIM_TYPE, ITPP_EFF_START_DT, "
    f"ITPP_R_I_START_DT, ITPP_EFF_END_DT, ITPP_R_I_CODE, ITPP_R_I_END_DT, ITPP_CTRL_PLAN_CD, "
    f"ITPP_ORIG_PAR_DT, ITPP_SUBM_PRCS_IND, ITPP_SUBM_EDIT_IND, ITPP_PROV_DATA_IND, ITPP_CL_DEVLP_IND, "
    f"ITPP_PROGRAM_CD, ITPP_XMT_MOD_CD_S, ITPP_LOC_STN_CD_S, ITPP_PRC_STN_CD_S, ITPP_SCCF_PRCS_PLN, "
    f"ITPP_PLAN_PAYER_CD, ITPP_PLAN_PAYER_Q1, ITPP_PLAN_PAYER_Q2, ITPP_PLAN_PAYER_Q3, ITPP_PLAN_PAYER_Q4, "
    f"ITPP_PLAN_PAYER_Q5, ITPP_SCDF_TYPE_CD, ITPP_STATISTIC_CD, ITPP_X1099_GEN_CD, ITPP_EOB_GEN_CD, "
    f"ITPP_XMT_MOD_CD_DR, ITPP_LOC_STN_CD_DR, ITPP_PRC_STN_CD_DR, ITPP_RULE_UPDT_DT, ITPP_CFA_CD, "
    f"ITPP_ADM_EXP_AL_CD, ITPP_AEA_NSTD_AMT, ITPP_MANAG_CARE_CD, ITPP_UPF_PR_EDT_CD, ITPP_ACCESS_FEE_CD, "
    f"ITPP_DELIVERY_METH, ITPP_PROD_TYPE_CD, ITPP_CFA_ACCT_CD, ITPP_ADJ_EDIT_IND, ITPP_NATION_OOA_CD, "
    f"ITPP_RESUB_DF_IND, ITPP_NEWEFF_END_DT, ITPP_CTL_STN_CD_PP, ITPP_LOC_STN_CD_PP, ITPP_ACCT_PLN_NAME, "
    f"ITPP_LAST_CHNG_DT, ITPP_OPERATOR_ID, ITPP_STATUS_CD, ITPP_STATUS_QUAL, ITPP_CTL_CREATE_DT, "
    f"ITPP_CTL_VERIFY_DT, ITPP_CTL_DIST_DT, ITPP_LOC_RECP_DT, ITPP_LOC_VERIFY_DT, ITPP_LOC_DIST_DT, "
    f"ITPP_LAST_PRCS_DTM, ITPP_CL_LST_UPD_DT, ITPP_PP_REC_VER_NO, ITPP_ACCT_TYPE_CD, ITPP_PRCE_DATA_IND, "
    f"ITPP_XMT_MOD_CD_PP, ITPP_CTL_RECP_DT "
    f"FROM {FacetsOwner}.CMC_ITPP_PLAN_PRFL"
)
df_Facets_PlnProfl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

# Read IDS CdMppng (DB2Connector)
df_IDS_CdMppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CD_MPPNG.SRC_CD as SRC_CD, CD_MPPNG.TRGT_DOMAIN_NM as TRGT_DOMAIN_NM, CD_MPPNG.TRGT_CD as TRGT_CD "
        f"FROM {IDSOwner}.cd_mppng CD_MPPNG WHERE CD_MPPNG.SRC_SYS_CD = 'FACETS'"
    )
    .load()
)

# hf_fcts_plnprofl_ids_cds (CHashedFileStage) - Scenario A (intermediate). Deduplicate on key columns:
df_hf_fcts_plnprofl_ids_cds = dedup_sort(
    df_IDS_CdMppng,
    partition_cols=["SRC_CD", "TRGT_DOMAIN_NM"],
    sort_cols=[]
)

# BusinessRules Transformer
df_br_1 = df_Facets_PlnProfl.alias("fcts_plnprofl_extr").join(
    df_hf_fcts_plnprofl_ids_cds.alias("clm_subtyp_lkup"),
    (
        (trim(col("fcts_plnprofl_extr.ITPP_CLAIM_TYPE")) == trim(col("clm_subtyp_lkup.SRC_CD")))
        & (lit("CLAIM SUBTYPE") == trim(col("clm_subtyp_lkup.TRGT_DOMAIN_NM")))
    ),
    "left"
)
df_BusinessRules = df_br_1.join(
    df_hf_fcts_plnprofl_ids_cds.alias("loc_pln_cd_lkup"),
    (
        (trim(col("fcts_plnprofl_extr.ITPP_LOCAL_PLAN_CD")) == trim(col("loc_pln_cd_lkup.SRC_CD")))
        & (lit("ACTIVATING BCBS PLAN") == trim(col("loc_pln_cd_lkup.TRGT_DOMAIN_NM")))
    ),
    "left"
)

df_BusinessRulesVars = (
    df_BusinessRules
    .withColumn(
        "svAlphaPfxCd",
        when(
            col("fcts_plnprofl_extr.ITPP_PRIMARY_ACCES").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PRIMARY_ACCES"))) == 0),
            lit("")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_PRIMARY_ACCES"))))
    )
    .withColumn(
        "svAlphaPfxLocPlnCd",
        when(
            col("loc_pln_cd_lkup.TRGT_CD").isNull()
            | (length(trim(col("loc_pln_cd_lkup.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("loc_pln_cd_lkup.TRGT_CD")))
    )
    .withColumn(
        "svClmSubtypCd",
        when(
            col("clm_subtyp_lkup.TRGT_CD").isNull()
            | (length(trim(col("clm_subtyp_lkup.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("clm_subtyp_lkup.TRGT_CD")))
    )
    .withColumn(
        "svEffDtSk",
        when(
            col("fcts_plnprofl_extr.ITPP_EFF_START_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_EFF_START_DT"))) == 0),
            lit("NA")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_EFF_START_DT").substr(F.lit(1), F.lit(10)))
    )
    .withColumn(
        "svRcptIncurStrtDtSk",
        when(
            col("fcts_plnprofl_extr.ITPP_R_I_START_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_R_I_START_DT"))) == 0),
            lit("NA")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_R_I_START_DT").substr(F.lit(1), F.lit(10)))
    )
    .withColumn("svRunDt", lit(CurrDate))
    .withColumn("SK", lit(0))
)

df_BusinessRules_Transform = df_BusinessRulesVars.filter(
    (length(trim(col("svAlphaPfxCd"))) > 0)
    & (col("svAlphaPfxLocPlnCd") != lit("NA"))
    & (col("svClmSubtypCd") != lit("NA"))
    & (col("svEffDtSk") != lit("NA"))
    & (col("svRcptIncurStrtDtSk") != lit("NA"))
)

df_BusinessRules_Reject = df_BusinessRulesVars.filter(
    (length(trim(col("svAlphaPfxCd"))) == 0)
    | (col("svAlphaPfxLocPlnCd") == lit("NA"))
    | (col("svClmSubtypCd") == lit("NA"))
    | (col("svEffDtSk") == lit("NA"))
    | (col("svRcptIncurStrtDtSk") == lit("NA"))
)

# Write Reject records to Plan_Profile_ErrorRecs.dat
df_Error_Recs = df_BusinessRules_Reject.select(
    col("fcts_plnprofl_extr.ITPP_PRIMARY_ACCES").alias("ITPP_PRIMARY_ACCES"),
    col("fcts_plnprofl_extr.ITPP_LOCAL_PLAN_CD").alias("ITPP_LOCAL_PLAN_CD"),
    col("fcts_plnprofl_extr.ITPP_CLAIM_TYPE").alias("ITPP_CLAIM_TYPE"),
    col("fcts_plnprofl_extr.ITPP_EFF_START_DT").alias("ITPP_EFF_START_DT"),
    col("fcts_plnprofl_extr.ITPP_R_I_START_DT").alias("ITPP_R_I_START_DT")
)
write_files(
    df_Error_Recs,
    f"{adls_path_publish}/external/Plan_Profile_ErrorRecs.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Next, build the output columns for the "Transform" link in BusinessRules
# (Apply rpad on char/varchar columns)
df_Transform_out = df_BusinessRules_Transform.select(
    # JOB_EXCTN_RCRD_ERR_SK
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    # INSRT_UPDT_CD (char(10))
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    # DISCARD_IN (char(1))
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    # PASS_THRU_IN (char(1))
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    # FIRST_RECYC_DT
    col("svRunDt").alias("FIRST_RECYC_DT"),
    # ERR_CT
    F.lit(0).alias("ERR_CT"),
    # RECYCLE_CT
    F.lit(0).alias("RECYCLE_CT"),
    # SRC_SYS_CD
    rpad(lit("FACETS"), 6, " ").alias("SRC_SYS_CD"),
    # PRI_KEY_STRING
    F.concat(
        lit("FACETS"),
        lit(";"),
        col("svAlphaPfxCd"),
        lit(";"),
        col("svAlphaPfxLocPlnCd"),
        lit(";"),
        col("svClmSubtypCd"),
        lit(";"),
        col("svEffDtSk"),
        lit(";"),
        col("svRcptIncurStrtDtSk")
    ).alias("PRI_KEY_STRING"),
    # PLN_PROFL_SK (PrimaryKey)
    col("SK").alias("PLN_PROFL_SK"),
    # ALPHA_PFX_CD
    col("svAlphaPfxCd").alias("ALPHA_PFX_CD"),
    # ALPHA_PFX_LOCAL_PLN_CD
    col("svAlphaPfxLocPlnCd").alias("ALPHA_PFX_LOCAL_PLN_CD"),
    # CLM_SUBTYP_CD
    col("svClmSubtypCd").alias("CLM_SUBTYP_CD"),
    # EFF_DT_SK (char(10))
    rpad(col("svEffDtSk"), 10, " ").alias("EFF_DT_SK"),
    # RCPT_INCUR_STRT_DT_SK (char(10))
    rpad(col("svRcptIncurStrtDtSk"), 10, " ").alias("RCPT_INCUR_STRT_DT_SK"),
    # CRT_RUN_CYC_EXCTN_SK
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_SK
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # ACCT_PLN_NM
    when(
        col("fcts_plnprofl_extr.ITPP_ACCT_PLN_NAME").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_ACCT_PLN_NAME"))) == 0),
        lit("NA")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_ACCT_PLN_NAME")))).alias("ACCT_PLN_NM"),
    # ADJ_EDIT_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_ADJ_EDIT_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_ADJ_EDIT_IND"))) == 0),
            lit("Y")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_ADJ_EDIT_IND")))),
        1,
        " "
    ).alias("ADJ_EDIT_IN"),
    # ADM_EXP_ALLWNC_NONSTD_AMT
    when(
        col("fcts_plnprofl_extr.ITPP_AEA_NSTD_AMT").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_AEA_NSTD_AMT"))) == 0),
        lit(0)
    ).otherwise(col("fcts_plnprofl_extr.ITPP_AEA_NSTD_AMT")).alias("ADM_EXP_ALLWNC_NONSTD_AMT"),
    # CTL_PLN_ACKNMT_RCPT_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CTL_RECP_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CTL_RECP_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_CTL_RECP_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("CTL_PLN_ACKNMT_RCPT_DT_SK"),
    # CTL_PLN_RULE_CRT_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CTL_CREATE_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CTL_CREATE_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_CTL_CREATE_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("CTL_PLN_RULE_CRT_DT_SK"),
    # CTL_PLN_RULE_DSTRB_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CTL_DIST_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CTL_DIST_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_CTL_DIST_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("CTL_PLN_RULE_DSTRB_DT_SK"),
    # CTL_PLN_RULE_VER_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CTL_VERIFY_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CTL_VERIFY_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_CTL_VERIFY_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("CTL_PLN_RULE_VER_DT_SK"),
    # CTL_PLN_STATN_ID
    when(
        col("fcts_plnprofl_extr.ITPP_CTL_STN_CD_PP").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_CTL_STN_CD_PP"))) == 0),
        lit("NA")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_CTL_STN_CD_PP")))).alias("CTL_PLN_STATN_ID"),
    # DF_LOCAL_PLN_STATN_ID
    when(
        col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_DR").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_DR"))) == 0),
        lit("NA")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_DR")))).alias("DF_LOCAL_PLN_STATN_ID"),
    # DF_PRCS_SITE_STATN_ID
    when(
        col("fcts_plnprofl_extr.ITPP_PRC_STN_CD_DR").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_PRC_STN_CD_DR"))) == 0),
        lit("NA")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_PRC_STN_CD_DR")))).alias("DF_PRCS_SITE_STATN_ID"),
    # END_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_EFF_END_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_EFF_END_DT"))) == 0),
            lit("2199-12-31")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_EFF_END_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("END_DT_SK"),
    # HOME_PLN_PROV_CNTCT_ALW_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CL_DEVLP_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CL_DEVLP_IND"))) == 0),
            lit("Y")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_CL_DEVLP_IND")))),
        1,
        " "
    ).alias("HOME_PLN_PROV_CNTCT_ALW_IN"),
    # ITS_SFWR_VRSN_NO
    when(
        col("fcts_plnprofl_extr.ITPP_PP_REC_VER_NO").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_PP_REC_VER_NO"))) == 0),
        lit(0)
    ).otherwise(col("fcts_plnprofl_extr.ITPP_PP_REC_VER_NO")).alias("ITS_SFWR_VRSN_NO"),
    # LOCAL_PLN_RULE_DSTRB_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_LOC_DIST_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_LOC_DIST_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_LOC_DIST_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("LOCAL_PLN_RULE_DSTRB_DT_SK"),
    # LOCAL_PLN_RULE_RCPT_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_LOC_RECP_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_LOC_RECP_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_LOC_RECP_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("LOCAL_PLN_RULE_RCPT_DT_SK"),
    # LOCAL_PLN_RULE_VER_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_LOC_VERIFY_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_LOC_VERIFY_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_LOC_VERIFY_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("LOCAL_PLN_RULE_VER_DT_SK"),
    # LOCAL_PLN_STATN_ID
    upper(trim(col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_PP"))).alias("LOCAL_PLN_STATN_ID"),
    # MULT_PRCS_ID (char(2))
    rpad(
        trim(col("fcts_plnprofl_extr.ITPP_CFA_ACCT_CD")),
        2,
        " "
    ).alias("MULT_PRCS_ID"),
    # NEW_END_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_NEWEFF_END_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_NEWEFF_END_DT"))) == 0),
            lit("2199-12-31")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_NEWEFF_END_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("NEW_END_DT_SK"),
    # ORIG_PARTCPN_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_ORIG_PAR_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_ORIG_PAR_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_ORIG_PAR_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("ORIG_PARTCPN_DT_SK"),
    # PLN_PROFL_ACES_FEE_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_ACCESS_FEE_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_ACCESS_FEE_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_ACCESS_FEE_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_ACES_FEE_CD"),
    # PLN_PROFL_ACCT_ENR_LVL_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_ACCT_TYPE_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_ACCT_TYPE_CD"))) == 0),
            lit("A")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_ACCT_TYPE_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_ACCT_ENR_LVL_CD"),
    # PLN_PROFL_ADM_EXP_ALLWNC_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_ADM_EXP_AL_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_ADM_EXP_AL_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_ADM_EXP_AL_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_ADM_EXP_ALLWNC_CD"),
    # PLN_PROFL_BNF_DLVRY_METH_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_DELIVERY_METH").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_DELIVERY_METH"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_DELIVERY_METH"))),
        1,
        " "
    ).alias("PLN_PROFL_BNF_DLVRY_METH_CD"),
    # PLN_PROFL_CFA_EDIT_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CFA_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CFA_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_CFA_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_CFA_EDIT_CD"),
    # PLN_PROFL_CTL_PLN_CD (char(3))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CTRL_PLAN_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CTRL_PLAN_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_CTRL_PLAN_CD"))),
        3,
        " "
    ).alias("PLN_PROFL_CTL_PLN_CD"),
    # PLN_PROFL_DF_INFO_TYP_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_SCDF_TYPE_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_SCDF_TYPE_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_SCDF_TYPE_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_DF_INFO_TYP_CD"),
    # PLN_PROFL_DF_TRNSMS_MODE_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_DR").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_DR"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_DR"))),
        1,
        " "
    ).alias("PLN_PROFL_DF_TRNSMS_MODE_CD"),
    # PLN_PROFL_EDIT_STTUS_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_STATUS_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_STATUS_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_STATUS_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_EDIT_STTUS_CD"),
    # PLN_PROFL_EDITSTTUS_QLFR_CD (char(2))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_STATUS_QUAL").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_STATUS_QUAL"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_STATUS_QUAL"))),
        2,
        " "
    ).alias("PLN_PROFL_EDITSTTUS_QLFR_CD"),
    # PLN_PROFL_EOB_GNRTN_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_EOB_GEN_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_EOB_GEN_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_EOB_GEN_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_EOB_GNRTN_CD"),
    # PLN_PROFL_MC_INFO_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_MANAG_CARE_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_MANAG_CARE_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_MANAG_CARE_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_MC_INFO_CD"),
    # PLN_PROFL_NTNL_OOA_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_NATION_OOA_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_NATION_OOA_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_NATION_OOA_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_NTNL_OOA_CD"),
    # PLN_PROFL_PAYER_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_PAYER_CD"),
    # PLN_PROFL_PAYER_RSTRCT1_CD (char(2))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q1").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q1"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q1"))),
        2,
        " "
    ).alias("PLN_PROFL_PAYER_RSTRCT1_CD"),
    # PLN_PROFL_PAYER_RSTRCT2_CD (char(2))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q2").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q2"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q2"))),
        2,
        " "
    ).alias("PLN_PROFL_PAYER_RSTRCT2_CD"),
    # PLN_PROFL_PAYER_RSTRCT3_CD (char(2))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q3").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q3"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q3"))),
        2,
        " "
    ).alias("PLN_PROFL_PAYER_RSTRCT3_CD"),
    # PLN_PROFL_PAYER_RSTRCT4_CD (char(2))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q4").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q4"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q4"))),
        2,
        " "
    ).alias("PLN_PROFL_PAYER_RSTRCT4_CD"),
    # PLN_PROFL_PAYER_RSTRCT5_CD (char(2))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q5").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q5"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PLAN_PAYER_Q5"))),
        2,
        " "
    ).alias("PLN_PROFL_PAYER_RSTRCT5_CD"),
    # PLN_PROFL_PRCS_ARGMT_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PROGRAM_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PROGRAM_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PROGRAM_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_PRCS_ARGMT_CD"),
    # PLN_PROFL_PRCS_PLN_CD (char(3))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_SCCF_PRCS_PLN").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_SCCF_PRCS_PLN"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_SCCF_PRCS_PLN"))),
        3,
        " "
    ).alias("PLN_PROFL_PRCS_PLN_CD"),
    # PLN_PROFL_PROD_CAT_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PROD_TYPE_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PROD_TYPE_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_PROD_TYPE_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_PROD_CAT_CD"),
    # PLN_PROFL_RCPT_INCUR_DT_TYP_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_R_I_CODE").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_R_I_CODE"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_R_I_CODE"))),
        1,
        " "
    ).alias("PLN_PROFL_RCPT_INCUR_DT_TYP_CD"),
    # PLN_PROFL_SF_TRNSMS_MODE_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_S").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_S"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_S"))),
        1,
        " "
    ).alias("PLN_PROFL_SF_TRNSMS_MODE_CD"),
    # PLN_PROFL_STAT_RCRD_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_STATISTIC_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_STATISTIC_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_STATISTIC_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_STAT_RCRD_CD"),
    # PLN_PROFL_TRNSMSN_MODE_CD
    when(
        col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_PP").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_PP"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_XMT_MOD_CD_PP")))).alias("PLN_PROFL_TRNSMSN_MODE_CD"),
    # PLN_PROFL_1099_GNRTN_CD (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_X1099_GEN_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_X1099_GEN_CD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("fcts_plnprofl_extr.ITPP_X1099_GEN_CD"))),
        1,
        " "
    ).alias("PLN_PROFL_1099_GNRTN_CD"),
    # PRICE_DATA_APPEND_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PRCE_DATA_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PRCE_DATA_IND"))) == 0),
            lit("Y")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_PRCE_DATA_IND")))),
        1,
        " "
    ).alias("PRICE_DATA_APPEND_IN"),
    # PROV_DATA_APPEND_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_PROV_DATA_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_PROV_DATA_IND"))) == 0),
            lit("N")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_PROV_DATA_IND")))),
        1,
        " "
    ).alias("PROV_DATA_APPEND_IN"),
    # RCPT_INCUR_END_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_R_I_END_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_R_I_END_DT"))) == 0),
            lit("2199-12-31")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_R_I_END_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("RCPT_INCUR_END_DT_SK"),
    # RESUBMT_DF_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_RESUB_DF_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_RESUB_DF_IND"))) == 0),
            lit("Y")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_RESUB_DF_IND")))),
        1,
        " "
    ).alias("RESUBMT_DF_IN"),
    # SF_LOCAL_PLN_STATN_ID
    when(
        col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_S").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_S"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_LOC_STN_CD_S")))).alias("SF_LOCAL_PLN_STATN_ID"),
    # SF_PRCS_SITE_STATN_ID
    when(
        col("fcts_plnprofl_extr.ITPP_PRC_STN_CD_S").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_PRC_STN_CD_S"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_PRC_STN_CD_S")))).alias("SF_PRCS_SITE_STATN_ID"),
    # SRC_SYS_CLM_LAST_UPDT_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_CL_LST_UPD_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_CL_LST_UPD_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_CL_LST_UPD_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("SRC_SYS_CLM_LAST_UPDT_DT_SK"),
    # SRC_SYS_LAST_PRCS_DTM
    when(
        col("fcts_plnprofl_extr.ITPP_LAST_PRCS_DTM").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_LAST_PRCS_DTM"))) == 0),
        lit("1753-01-01-00.00.00.000")
    ).otherwise(col("fcts_plnprofl_extr.ITPP_LAST_PRCS_DTM")).alias("SRC_SYS_LAST_PRCS_DTM"),
    # SRC_SYS_LAST_RULE_UPDT_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_RULE_UPDT_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_RULE_UPDT_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_RULE_UPDT_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("SRC_SYS_LAST_RULE_UPDT_DT_SK"),
    # SRC_SYS_LAST_UPDT_DT_SK (char(10))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_LAST_CHNG_DT").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_LAST_CHNG_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(col("fcts_plnprofl_extr.ITPP_LAST_CHNG_DT").substr(F.lit(1), F.lit(10))),
        10,
        " "
    ).alias("SRC_SYS_LAST_UPDT_DT_SK"),
    # SRC_SYS_LAST_UPDT_USER_ID
    when(
        col("fcts_plnprofl_extr.ITPP_OPERATOR_ID").isNull()
        | (length(trim(col("fcts_plnprofl_extr.ITPP_OPERATOR_ID"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_OPERATOR_ID")))).alias("SRC_SYS_LAST_UPDT_USER_ID"),
    # SUBMSN_EDIT_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_SUBM_EDIT_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_SUBM_EDIT_IND"))) == 0),
            lit("N")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_SUBM_EDIT_IND")))),
        1,
        " "
    ).alias("SUBMSN_EDIT_IN"),
    # SUBMSN_PRCS_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_SUBM_PRCS_IND").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_SUBM_PRCS_IND"))) == 0),
            lit("N")
        ).otherwise(upper(trim(col("fcts_plnprofl_extr.ITPP_SUBM_PRCS_IND")))),
        1,
        " "
    ).alias("SUBMSN_PRCS_IN"),
    # UNFRM_PRICE_FCLTY_EDIT_IN (char(1))
    rpad(
        when(
            col("fcts_plnprofl_extr.ITPP_UPF_PR_EDT_CD").isNull()
            | (length(trim(col("fcts_plnprofl_extr.ITPP_UPF_PR_EDT_CD"))) == 0),
            lit("N")
        ).otherwise(
            when(trim(col("fcts_plnprofl_extr.ITPP_UPF_PR_EDT_CD")) == lit("1"), lit("Y")).otherwise(lit("N"))
        ),
        1,
        " "
    ).alias("UNFRM_PRICE_FCLTY_EDIT_IN"),
    # CLM_SUBTYP_SRC_CD
    trim(col("fcts_plnprofl_extr.ITPP_CLAIM_TYPE")).alias("CLM_SUBTYP_SRC_CD"),
    # ALPHA_PFX_LOCAL_PLN_SRC_CD
    trim(col("fcts_plnprofl_extr.ITPP_LOCAL_PLAN_CD")).alias("ALPHA_PFX_LOCAL_PLN_SRC_CD")
)

# PrimaryKey Transformer: join with hf_pln_profl_lkup again on 6 keys
df_PrimaryKey_join = df_Transform_out.alias("Transform").join(
    df_hf_pln_profl_lkup.alias("lkup"),
    [
        col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("Transform.ALPHA_PFX_CD") == col("lkup.ALPHA_PFX_CD"),
        col("Transform.ALPHA_PFX_LOCAL_PLN_CD") == col("lkup.ALPHA_PFX_LOCAL_PLN_CD"),
        col("Transform.CLM_SUBTYP_CD") == col("lkup.CLM_SUBTYP_CD"),
        col("Transform.EFF_DT_SK") == col("lkup.EFF_DT_SK"),
        col("Transform.RCPT_INCUR_STRT_DT_SK") == col("lkup.RCPT_INCUR_STRT_DT_SK")
    ],
    "left"
)

# We invoke SurrogateKeyGen for the column "PLN_PROFL_SK" due to KeyMgtGetNextValueConcurrent usage
df_enriched = df_PrimaryKey_join.withColumn(
    "PLN_PROFL_SK",
    coalesce(col("lkup.PLN_PROFL_SK").cast("int"), col("Transform.PLN_PROFL_SK").cast("int"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PLN_PROFL_SK",<schema>,<secret_name>)

# svOrigRunCycle => if isnull(lkup.PLN_PROFL_SK) then RunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
df_enriched = df_enriched.withColumn(
    "svOrigRunCycle",
    when(col("lkup.PLN_PROFL_SK").isNull(), col("RunCycle")).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)
df_enriched = df_enriched.withColumn("RunCycle", lit(RunCycle))

# Two outputs from PrimaryKey: "Key" -> IdsPlnProflPkey and "updt" -> hf_pln_profl

df_Key = df_enriched.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("PLN_PROFL_SK").alias("PLN_PROFL_SK"),
    col("Transform.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    col("Transform.ALPHA_PFX_LOCAL_PLN_CD").alias("ALPHA_PFX_LOCAL_PLN_CD"),
    col("Transform.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.RCPT_INCUR_STRT_DT_SK").alias("RCPT_INCUR_STRT_DT_SK"),
    col("svOrigRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.ACCT_PLN_NM").alias("ACCT_PLN_NM"),
    col("Transform.ADJ_EDIT_IN").alias("ADJ_EDIT_IN"),
    col("Transform.ADM_EXP_ALLWNC_NONSTD_AMT").alias("ADM_EXP_ALLWNC_NONSTD_AMT"),
    col("Transform.CTL_PLN_ACKNMT_RCPT_DT_SK").alias("CTL_PLN_ACKNMT_RCPT_DT_SK"),
    col("Transform.CTL_PLN_RULE_CRT_DT_SK").alias("CTL_PLN_RULE_CRT_DT_SK"),
    col("Transform.CTL_PLN_RULE_DSTRB_DT_SK").alias("CTL_PLN_RULE_DSTRB_DT_SK"),
    col("Transform.CTL_PLN_RULE_VER_DT_SK").alias("CTL_PLN_RULE_VER_DT_SK"),
    col("Transform.CTL_PLN_STATN_ID").alias("CTL_PLN_STATN_ID"),
    col("Transform.DF_LOCAL_PLN_STATN_ID").alias("DF_LOCAL_PLN_STATN_ID"),
    col("Transform.DF_PRCS_SITE_STATN_ID").alias("DF_PRCS_SITE_STATN_ID"),
    col("Transform.END_DT_SK").alias("END_DT_SK"),
    col("Transform.HOME_PLN_PROV_CNTCT_ALW_IN").alias("HOME_PLN_PROV_CNTCT_ALW_IN"),
    col("Transform.ITS_SFWR_VRSN_NO").alias("ITS_SFWR_VRSN_NO"),
    col("Transform.LOCAL_PLN_RULE_DSTRB_DT_SK").alias("LOCAL_PLN_RULE_DSTRB_DT_SK"),
    col("Transform.LOCAL_PLN_RULE_RCPT_DT_SK").alias("LOCAL_PLN_RULE_RCPT_DT_SK"),
    col("Transform.LOCAL_PLN_RULE_VER_DT_SK").alias("LOCAL_PLN_RULE_VER_DT_SK"),
    col("Transform.LOCAL_PLN_STATN_ID").alias("LOCAL_PLN_STATN_ID"),
    col("Transform.MULT_PRCS_ID").alias("MULT_PRCS_ID"),
    col("Transform.NEW_END_DT_SK").alias("NEW_END_DT_SK"),
    col("Transform.ORIG_PARTCPN_DT_SK").alias("ORIG_PARTCPN_DT_SK"),
    col("Transform.PLN_PROFL_ACES_FEE_CD").alias("PLN_PROFL_ACES_FEE_CD"),
    col("Transform.PLN_PROFL_ACCT_ENR_LVL_CD").alias("PLN_PROFL_ACCT_ENR_LVL_CD"),
    col("Transform.PLN_PROFL_ADM_EXP_ALLWNC_CD").alias("PLN_PROFL_ADM_EXP_ALLWNC_CD"),
    col("Transform.PLN_PROFL_BNF_DLVRY_METH_CD").alias("PLN_PROFL_BNF_DLVRY_METH_CD"),
    col("Transform.PLN_PROFL_CFA_EDIT_CD").alias("PLN_PROFL_CFA_EDIT_CD"),
    col("Transform.PLN_PROFL_CTL_PLN_CD").alias("PLN_PROFL_CTL_PLN_CD"),
    col("Transform.PLN_PROFL_DF_INFO_TYP_CD").alias("PLN_PROFL_DF_INFO_TYP_CD"),
    col("Transform.PLN_PROFL_DF_TRNSMS_MODE_CD").alias("PLN_PROFL_DF_TRNSMS_MODE_CD"),
    col("Transform.PLN_PROFL_EDIT_STTUS_CD").alias("PLN_PROFL_EDIT_STTUS_CD"),
    col("Transform.PLN_PROFL_EDITSTTUS_QLFR_CD").alias("PLN_PROFL_EDITSTTUS_QLFR_CD"),
    col("Transform.PLN_PROFL_EOB_GNRTN_CD").alias("PLN_PROFL_EOB_GNRTN_CD"),
    col("Transform.PLN_PROFL_MC_INFO_CD").alias("PLN_PROFL_MC_INFO_CD"),
    col("Transform.PLN_PROFL_NTNL_OOA_CD").alias("PLN_PROFL_NTNL_OOA_CD"),
    col("Transform.PLN_PROFL_PAYER_CD").alias("PLN_PROFL_PAYER_CD"),
    col("Transform.PLN_PROFL_PAYER_RSTRCT1_CD").alias("PLN_PROFL_PAYER_RSTRCT1_CD"),
    col("Transform.PLN_PROFL_PAYER_RSTRCT2_CD").alias("PLN_PROFL_PAYER_RSTRCT2_CD"),
    col("Transform.PLN_PROFL_PAYER_RSTRCT3_CD").alias("PLN_PROFL_PAYER_RSTRCT3_CD"),
    col("Transform.PLN_PROFL_PAYER_RSTRCT4_CD").alias("PLN_PROFL_PAYER_RSTRCT4_CD"),
    col("Transform.PLN_PROFL_PAYER_RSTRCT5_CD").alias("PLN_PROFL_PAYER_RSTRCT5_CD"),
    col("Transform.PLN_PROFL_PRCS_ARGMT_CD").alias("PLN_PROFL_PRCS_ARGMT_CD"),
    col("Transform.PLN_PROFL_PRCS_PLN_CD").alias("PLN_PROFL_PRCS_PLN_CD"),
    col("Transform.PLN_PROFL_PROD_CAT_CD").alias("PLN_PROFL_PROD_CAT_CD"),
    col("Transform.PLN_PROFL_RCPT_INCUR_DT_TYP_CD").alias("PLN_PROFL_RCPT_INCUR_DT_TYP_CD"),
    col("Transform.PLN_PROFL_SF_TRNSMS_MODE_CD").alias("PLN_PROFL_SF_TRNSMS_MODE_CD"),
    col("Transform.PLN_PROFL_STAT_RCRD_CD").alias("PLN_PROFL_STAT_RCRD_CD"),
    col("Transform.PLN_PROFL_TRNSMSN_MODE_CD").alias("PLN_PROFL_TRNSMSN_MODE_CD"),
    col("Transform.PLN_PROFL_1099_GNRTN_CD").alias("PLN_PROFL_1099_GNRTN_CD"),
    col("Transform.PRICE_DATA_APPEND_IN").alias("PRICE_DATA_APPEND_IN"),
    col("Transform.PROV_DATA_APPEND_IN").alias("PROV_DATA_APPEND_IN"),
    col("Transform.RCPT_INCUR_END_DT_SK").alias("RCPT_INCUR_END_DT_SK"),
    col("Transform.RESUBMT_DF_IN").alias("RESUBMT_DF_IN"),
    col("Transform.SF_LOCAL_PLN_STATN_ID").alias("SF_LOCAL_PLN_STATN_ID"),
    col("Transform.SF_PRCS_SITE_STATN_ID").alias("SF_PRCS_SITE_STATN_ID"),
    col("Transform.SRC_SYS_CLM_LAST_UPDT_DT_SK").alias("SRC_SYS_CLM_LAST_UPDT_DT_SK"),
    col("Transform.SRC_SYS_LAST_PRCS_DTM").alias("SRC_SYS_LAST_PRCS_DTM"),
    col("Transform.SRC_SYS_LAST_RULE_UPDT_DT_SK").alias("SRC_SYS_LAST_RULE_UPDT_DT_SK"),
    col("Transform.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("Transform.SRC_SYS_LAST_UPDT_USER_ID").alias("SRC_SYS_LAST_UPDT_USER_ID"),
    col("Transform.SUBMSN_EDIT_IN").alias("SUBMSN_EDIT_IN"),
    col("Transform.SUBMSN_PRCS_IN").alias("SUBMSN_PRCS_IN"),
    col("Transform.UNFRM_PRICE_FCLTY_EDIT_IN").alias("UNFRM_PRICE_FCLTY_EDIT_IN"),
    col("Transform.CLM_SUBTYP_SRC_CD").alias("CLM_SUBTYP_SRC_CD"),
    col("Transform.ALPHA_PFX_LOCAL_PLN_SRC_CD").alias("ALPHA_PFX_LOCAL_PLN_SRC_CD")
)

df_updt = df_enriched.select(
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    col("Transform.ALPHA_PFX_LOCAL_PLN_CD").alias("ALPHA_PFX_LOCAL_PLN_CD"),
    col("Transform.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.RCPT_INCUR_STRT_DT_SK").alias("RCPT_INCUR_STRT_DT_SK"),
    lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("PLN_PROFL_SK").alias("PLN_PROFL_SK")
)

# Write df_Key to a sequential file FctsPlnProflExtr.PlnProfl.dat.#RunID# in directory "key"
write_files(
    df_Key,
    f"{adls_path}/key/FctsPlnProflExtr.PlnProfl.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "hf_pln_profl" - Scenario B write step: upsert to IDS.dummy_hf_pln_profl
# Create staging table and then merge
staging_table = f"STAGING.FctsPlnProflExtr_hf_pln_profl_temp"
drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
execute_dml(drop_sql, jdbc_url_ids, jdbc_props_ids)

# Write df_updt into that staging table
(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", staging_table)
    .mode("overwrite")
    .save()
)

# Merge into IDS.dummy_hf_pln_profl
merge_sql = (
    f"MERGE INTO IDS.dummy_hf_pln_profl AS T "
    f"USING {staging_table} AS S "
    f"ON "
    f"(T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.ALPHA_PFX_CD = S.ALPHA_PFX_CD "
    f"AND T.ALPHA_PFX_LOCAL_PLN_CD = S.ALPHA_PFX_LOCAL_PLN_CD "
    f"AND T.CLM_SUBTYP_CD = S.CLM_SUBTYP_CD "
    f"AND T.EFF_DT_SK = S.EFF_DT_SK "
    f"AND T.RCPT_INCUR_STRT_DT_SK = S.RCPT_INCUR_STRT_DT_SK) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    f"T.PLN_PROFL_SK = S.PLN_PROFL_SK "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(SRC_SYS_CD, ALPHA_PFX_CD, ALPHA_PFX_LOCAL_PLN_CD, CLM_SUBTYP_CD, EFF_DT_SK, RCPT_INCUR_STRT_DT_SK, CRT_RUN_CYC_EXCTN_SK, PLN_PROFL_SK) "
    f"VALUES "
    f"(S.SRC_SYS_CD, S.ALPHA_PFX_CD, S.ALPHA_PFX_LOCAL_PLN_CD, S.CLM_SUBTYP_CD, S.EFF_DT_SK, S.RCPT_INCUR_STRT_DT_SK, S.CRT_RUN_CYC_EXCTN_SK, S.PLN_PROFL_SK);"
)
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)