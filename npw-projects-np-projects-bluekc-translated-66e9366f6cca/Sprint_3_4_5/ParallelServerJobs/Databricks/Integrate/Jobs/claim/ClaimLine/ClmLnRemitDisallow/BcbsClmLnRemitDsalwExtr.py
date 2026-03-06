# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_7 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_7 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_5 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_1 09/21/09 12:20:43 Batch  15240_44447 PROMOTE bckcett:31540 testIDSnew u10157 sa
# MAGIC ^1_1 09/21/09 12:16:59 Batch  15240_44229 INIT bckcett:31540 devlIDSnew u10157 sa for 2009-09-21 prod deployment
# MAGIC ^1_4 04/24/09 09:20:30 Batch  15090_33635 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 04/24/09 09:14:25 Batch  15090_33296 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_3 03/10/09 11:34:35 Batch  15045_41695 PROMOTE bckcett devlIDS u10157 sa - Bringing ALL Claim code down from production
# MAGIC ^1_3 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_2 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_2 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_2 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 08/22/08 10:09:03 Batch  14845_36590 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 08/22/08 09:55:32 Batch  14845_35734 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_1 08/19/08 10:43:19 Batch  14842_38609 PROMOTE bckcett testIDS u03651 steph for Sharon 3057
# MAGIC ^1_1 08/19/08 10:38:02 Batch  14842_38285 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC ************************************************************************************
# MAGIC COPYRIGHT 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC ************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  BcbsClmLnRemitDsalwExtr
# MAGIC CALLED BY:        claim / SeqClmLnRemit / BCBSClmLnRemitExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  This job extracts data from BCBS Medical and Dental Claim Detail tables for Claim Line Remit Dsalw
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                    Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                            --------------------------------       -------------------------------   ----------------------------    
# MAGIC Parik                       2008-07-30       3057(Web Remit)   Original Programming                                                                                                  devlIDSnew                     Steph Goddard          08/11/2008
# MAGIC Brent Leland           2008-11-24       3567 Primary Key   Updated with new primary key process                                                                       devlIDS             
# MAGIC SANdrew                2009-03-03        ProdSupport          Added balancing .                                                                                                       devlIDS                           Steph Goddard          03/24/2009
# MAGIC                                                                                       Added hash file lookups with files built in FctsClmLnRemitDriverBuild 
# MAGIC                                                                                      and provides foundation for determing new business rules for reissed checks and adjustment checks.             
# MAGIC SAndrew             2009-06-10      3833 Remit              updated documentation in job to denote many programs use hash file                             devlIDSnew                                                     
# MAGIC                                                    Alternate Chrg          changed balance file to use SrcSysCd parameter and not hard coded 
# MAGIC Prabhu ES          2022-02-26       S2S Remediation      MSSQL connection parameters added                                                                                 IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC BCBS Claim Line Remit Disallow Extract and Primary Key
# MAGIC update primary key table (K_CLM_LN_REMIT_DSALW) with new keys created today
# MAGIC Writing Sequential File to ../key
# MAGIC primary key hash file only contains current run keys and is cleared before writing
# MAGIC Strip fields
# MAGIC Extraction
# MAGIC Reissued checks on orig claims where adjusting-to is now paying.  Amts on reissued chk will be neg.
# MAGIC Reissued checks with on orig claims and no adjusting-to claims are on this pd file.  Amts on orig claim will be positive
# MAGIC Data Collection
# MAGIC Business Logic
# MAGIC Reversals
# MAGIC Hash files loaded in FctsClmRemitDriverBuild
# MAGIC Hash files loaded in FctsClmRemitDriverBuild
# MAGIC Do not delete - BcbsClmLnRemitDsalowExtr also uses
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, expr, rpad, isnull, concat
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnRemitDsalwPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)

# Read from hashed files (scenario C)
df_hf_clm_remit_chk_w_pd_adj = spark.read.parquet(f"{adls_path}/hf_clm_remit_chk_w_pd_adj.parquet").select(
    col("CLCL_ID").alias("CLCL_ID"),
    col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM")
)

df_hf_clm_remit_check_reissue_only = spark.read.parquet(f"{adls_path}/hf_clm_remit_check_reissue_only.parquet").select(
    col("CLCL_ID").alias("CLCL_ID"),
    col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM")
)

df_hf_clm_remit_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_remit_nasco_dup_bypass.parquet").select(
    col("CLM_ID").alias("CLM_ID")
)

df_hf_clm_remit_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_remit_fcts_reversals.parquet").select(
    col("CLCL_ID").alias("CLCL_ID"),
    col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM")
)

# Read from ODBC (BCBS) - BcbsPdMedDntlClmDtl
extract_query_med = """
SELECT CLCL_ID,
       CDML_SEQ_NO,
       DISALLOW_TYPE,
       BYPASS_IN,
       CDML_DISALL_AMT,
       CDML_DISALL_EXCD,
       EXCD_RSPNSB_IN
  FROM tempdb..#DriverTable# DRIVER,
       #$BCBSOwner#.PD_MED_CLM_DTL MED_CLM_DTL
 WHERE DRIVER.CLM_ID = MED_CLM_DTL.CLCL_ID
"""
df_BcbsPdMedDntlClmDtl_Med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_med)
    .load()
)

extract_query_dntl = """
SELECT CLCL_ID,
       CDDL_SEQ_NO,
       DISALLOW_TYPE,
       BYPASS_IN,
       CDDL_DISALL_AMT,
       CDDL_DISALL_EXCD,
       EXCD_RSPNSB_IN
  FROM tempdb..#DriverTable# DRIVER,
       #$BCBSOwner#.PD_DNTL_CLM_DTL DNTL_CLM_DTL
 WHERE DRIVER.CLM_ID = DNTL_CLM_DTL.CLCL_ID
"""
df_BcbsPdMedDntlClmDtl_Dntl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_dntl)
    .load()
)

# StripFields (Med)
df_StripFields_MedOut = df_BcbsPdMedDntlClmDtl_Med.select(
    strip_field(col("CLCL_ID")).alias("CLCL_ID"),
    col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    strip_field(col("DISALLOW_TYPE")).alias("DISALLOW_TYPE"),
    strip_field(col("BYPASS_IN")).alias("BYPASS_IN"),
    col("CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    strip_field(col("CDML_DISALL_EXCD")).alias("CDML_DISALL_EXCD"),
    strip_field(col("EXCD_RSPNSB_IN")).alias("EXCD_RSPNSB_IN")
)

# Transformer_18 (Dntl)
df_Transformer_18_DntlOut = df_BcbsPdMedDntlClmDtl_Dntl.select(
    strip_field(col("CLCL_ID")).alias("CLCL_ID"),
    col("CDDL_SEQ_NO").alias("CDML_SEQ_NO"),
    strip_field(col("DISALLOW_TYPE")).alias("DISALLOW_TYPE"),
    strip_field(col("BYPASS_IN")).alias("BYPASS_IN"),
    col("CDDL_DISALL_AMT").alias("CDML_DISALL_AMT"),
    strip_field(col("CDDL_DISALL_EXCD")).alias("CDML_DISALL_EXCD"),
    strip_field(col("EXCD_RSPNSB_IN")).alias("EXCD_RSPNSB_IN")
)

# LinkMedDntlDtlConcat (CCollector: union of MedOut & DntlOut)
df_LinkMedDntlDtlConcat_Concat = df_StripFields_MedOut.unionByName(df_Transformer_18_DntlOut)

# hf_bcbs_med_dntl_clm_dtl is an intermediate hashed file (Scenario A). Remove it and deduplicate.
df_ConcatOut = dedup_sort(
    df_LinkMedDntlDtlConcat_Concat,
    ["CLCL_ID","CDML_SEQ_NO","DISALLOW_TYPE","BYPASS_IN"],
    []
)

# BusinessLogic (Transformer with multiple left lookups)
df_BusinessLogic_Input = (
    df_ConcatOut.alias("ConcatOut")
    .join(
        df_hf_clm_remit_fcts_reversals.alias("fcts_reversals"),
        col("ConcatOut.CLCL_ID") == col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_nasco_dup_bypass.alias("nasco_dup_lkup"),
        col("ConcatOut.CLCL_ID") == col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_chk_w_pd_adj.alias("chk_w_pd_adj"),
        col("ConcatOut.CLCL_ID") == col("chk_w_pd_adj.CLCL_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_check_reissue_only.alias("reissue_only"),
        col("ConcatOut.CLCL_ID") == col("reissue_only.CLCL_ID"),
        "left"
    )
)

df_BusinessLogic_StgVars = (
    df_BusinessLogic_Input
    .withColumn("PassThru", lit("Y"))
    .withColumn("svClmId", trim(col("ConcatOut.CLCL_ID")))
    .withColumn(
        "svDsalwType",
        when(
            col("ConcatOut.DISALLOW_TYPE").isNull() | (length(trim(col("ConcatOut.DISALLOW_TYPE"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.DISALLOW_TYPE")))
    )
    .withColumn(
        "ThisIsAnAdjustedClaim",
        when(
            (col("fcts_reversals.CLCL_ID").isNotNull()) & (col("fcts_reversals.CLCL_CUR_STS") == lit("91")),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "ThisIsACheckReissueOnly",
        when(
            (col("reissue_only.CLCL_ID").isNotNull()) & (col("chk_w_pd_adj.CLCL_ID").isNull()),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "ThisIsPdAdjWithOrighCheckReissue",
        when(
            (col("reissue_only.CLCL_ID").isNotNull()) & (col("chk_w_pd_adj.CLCL_ID").isNotNull()),
            lit("Y")
        ).otherwise(lit("N"))
    )
)

# Output link: Paid
df_BusinessLogic_Paid = df_BusinessLogic_StgVars.filter(
    col("nasco_dup_lkup.CLM_ID").isNull() & (col("ThisIsAnAdjustedClaim") == lit("N"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    expr("SRC_SYS_CD || ';' || svClmId || ';' || ConcatOut.CDML_SEQ_NO || ';' || ConcatOut.DISALLOW_TYPE || ';' || ConcatOut.BYPASS_IN").alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
    col("svClmId").alias("CLM_ID"),
    col("ConcatOut.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad(col("svDsalwType"), 10, " ").alias("CLM_LN_DSALW_TYP_CD"),
    rpad(trim(col("ConcatOut.BYPASS_IN")), 1, " ").alias("BYPS_IN"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_LN_SK"),
    rpad(
        when(
            col("ConcatOut.CDML_DISALL_EXCD").isNull() | (length(trim(col("ConcatOut.CDML_DISALL_EXCD"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.CDML_DISALL_EXCD"))),
        10, " "
    ).alias("CLM_LN_REMIT_DSALW_EXCD"),
    rpad(
        when(
            col("ConcatOut.EXCD_RSPNSB_IN").isNull() | (length(trim(col("ConcatOut.EXCD_RSPNSB_IN"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.EXCD_RSPNSB_IN"))),
        10, " "
    ).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    rpad(col("svDsalwType"), 10, " ").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    col("ConcatOut.CDML_DISALL_AMT").alias("REMIT_DSALW_AMT")
)

# Output link: ReversalA08
df_BusinessLogic_ReversalA08 = df_BusinessLogic_StgVars.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) & (col("ThisIsAnAdjustedClaim") == lit("Y"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    expr("SRC_SYS_CD || ';' || svClmId || 'R;' || ConcatOut.CDML_SEQ_NO || ';' || ConcatOut.DISALLOW_TYPE || ';' || ConcatOut.BYPASS_IN").alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
    expr("svClmId || 'R'").alias("CLM_ID"),
    col("ConcatOut.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad(col("svDsalwType"), 10, " ").alias("CLM_LN_DSALW_TYP_CD"),
    rpad(trim(col("ConcatOut.BYPASS_IN")), 1, " ").alias("BYPS_IN"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_LN_SK"),
    rpad(
        when(
            col("ConcatOut.CDML_DISALL_EXCD").isNull() | (length(trim(col("ConcatOut.CDML_DISALL_EXCD"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.CDML_DISALL_EXCD"))),
        10, " "
    ).alias("CLM_LN_REMIT_DSALW_EXCD"),
    rpad(
        when(
            col("ConcatOut.EXCD_RSPNSB_IN").isNull() | (length(trim(col("ConcatOut.EXCD_RSPNSB_IN"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.EXCD_RSPNSB_IN"))),
        10, " "
    ).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    rpad(col("svDsalwType"), 10, " ").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    when(
        col("ThisIsACheckReissueOnly") == lit("Y"),
        col("ConcatOut.CDML_DISALL_AMT") * -1
    ).otherwise(col("ConcatOut.CDML_DISALL_AMT")).alias("REMIT_DSALW_AMT")
)

# Output link: ReversalA09
df_BusinessLogic_ReversalA09 = df_BusinessLogic_StgVars.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) & (col("ThisIsAnAdjustedClaim") == lit("Y"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    expr("SRC_SYS_CD || ';' || svClmId || ';' || ConcatOut.CDML_SEQ_NO || ';' || ConcatOut.DISALLOW_TYPE || ';' || ConcatOut.BYPASS_IN").alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
    col("svClmId").alias("CLM_ID"),
    col("ConcatOut.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad(col("svDsalwType"), 10, " ").alias("CLM_LN_DSALW_TYP_CD"),
    rpad(trim(col("ConcatOut.BYPASS_IN")), 1, " ").alias("BYPS_IN"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_LN_SK"),
    rpad(
        when(
            col("ConcatOut.CDML_DISALL_EXCD").isNull() | (length(trim(col("ConcatOut.CDML_DISALL_EXCD"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.CDML_DISALL_EXCD"))),
        10, " "
    ).alias("CLM_LN_REMIT_DSALW_EXCD"),
    rpad(
        when(
            col("ConcatOut.EXCD_RSPNSB_IN").isNull() | (length(trim(col("ConcatOut.EXCD_RSPNSB_IN"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("ConcatOut.EXCD_RSPNSB_IN"))),
        10, " "
    ).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    rpad(col("svDsalwType"), 10, " ").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    when(
        col("ThisIsACheckReissueOnly") == lit("Y"),
        col("ConcatOut.CDML_DISALL_AMT")
    ).when(
        col("ThisIsPdAdjWithOrighCheckReissue") == lit("Y"),
        col("ConcatOut.CDML_DISALL_AMT") * -1
    ).otherwise(col("ConcatOut.CDML_DISALL_AMT") * -1).alias("REMIT_DSALW_AMT")
)

# Link_Collector (union of the three outputs)
df_Link_Collector = df_BusinessLogic_Paid.unionByName(df_BusinessLogic_ReversalA08).unionByName(df_BusinessLogic_ReversalA09)

# SnapShot
df_SnapShot = df_Link_Collector.withColumn(
    "svClmLnDsalwTypCdSk",
    GetFkeyCodes(
        col("SRC_SYS_CD"), 
        lit(102),
        lit("CLAIM LINE DISALLOW TYPE"),
        col("CLM_LN_DSALW_TYP_CD"),
        lit("X")
    )
)

# Two outputs from SnapShot: 
# 1) "Trans" -> ClmLnRemitDsalwPK
df_ClmLnRemitDsalwPK = df_SnapShot.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("ROW_PASS_THRU").alias("ROW_PASS_THRU"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_LN_REMIT_DSALW_SK").alias("CLM_LN_REMIT_DSALW_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    col("BYPS_IN").alias("BYPS_IN"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    col("CLM_LN_REMIT_DSALW_EXCD").alias("CLM_LN_REMIT_DSALW_EXCD"),
    col("CLM_LN_RMT_DSW_EXCD_RESP_CD").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    col("CLM_LN_RMT_DSALW_TYP_CAT_CD").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    col("REMIT_DSALW_AMT").alias("REMIT_DSALW_AMT")
)

# 2) "balance" -> B_CLM_LN_REMIT_DSALW
df_B_CLM_LN_REMIT_DSALW = df_SnapShot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("svClmLnDsalwTypCdSk").alias("CLM_LN_DSALW_TYP_CD_SK"),
    rpad(col("BYPS_IN"), 1, " ").alias("BYPS_IN"),
    col("REMIT_DSALW_AMT").alias("REMIT_DSALW_AMT")
)

# Write to B_CLM_LN_REMIT_DSALW.#SrcSysCd#.dat.#RunID#
file_path_balance = f"{adls_path}/load/B_CLM_LN_REMIT_DSALW.{SrcSysCd}.dat.{RunID}"
write_files(
    df_B_CLM_LN_REMIT_DSALW,
    file_path_balance,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Shared Container: ClmLnRemitDsalwPK
params_ClmLnRemitDsalwPK = {
    "CurrRunCycle": CurrRunCycle,
    "CurrentDate": CurrentDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}
df_ClmLnRemitDsalwPK_Out = ClmLnRemitDsalwPK(df_ClmLnRemitDsalwPK, params_ClmLnRemitDsalwPK)

# Final output: IdsClmLnRemitDsalwExtr
# The job's final SeqFile stage: write BcbsClmLnRemitDsalwExtr.ClmLnRemitDsalw.dat.#RunID# under directory "key"
df_IdsClmLnRemitDsalwExtr = df_ClmLnRemitDsalwPK_Out.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    # "INSRT_UPDT_CD" char(10)
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    # "DISCARD_IN" char(1)
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    # "ROW_PASS_THRU" char(1)
    rpad(col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_LN_REMIT_DSALW_SK").alias("CLM_LN_REMIT_DSALW_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    # "CLM_LN_DSALW_TYP_CD" char(10)
    rpad(col("CLM_LN_DSALW_TYP_CD"), 10, " ").alias("CLM_LN_DSALW_TYP_CD"),
    # "BYPS_IN" char(1)
    rpad(col("BYPS_IN"), 1, " ").alias("BYPS_IN"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    # "CLM_LN_REMIT_DSALW_EXCD" char(10)
    rpad(col("CLM_LN_REMIT_DSALW_EXCD"), 10, " ").alias("CLM_LN_REMIT_DSALW_EXCD"),
    # "CLM_LN_RMT_DSW_EXCD_RESP_CD" char(10)
    rpad(col("CLM_LN_RMT_DSW_EXCD_RESP_CD"), 10, " ").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    # "CLM_LN_RMT_DSALW_TYP_CAT_CD" char(10)
    rpad(col("CLM_LN_RMT_DSALW_TYP_CAT_CD"), 10, " ").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    col("REMIT_DSALW_AMT").alias("REMIT_DSALW_AMT")
)

file_path_ids = f"{adls_path}/key/BcbsClmLnRemitDsalwExtr.ClmLnRemitDsalw.dat.{RunID}"
write_files(
    df_IdsClmLnRemitDsalwExtr,
    file_path_ids,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)