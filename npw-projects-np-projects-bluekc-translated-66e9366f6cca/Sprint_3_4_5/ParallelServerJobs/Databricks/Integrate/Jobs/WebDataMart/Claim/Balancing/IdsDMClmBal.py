# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 04/18/07 14:50:09 Batch  14353_53412 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 06/26/06 13:48:04 Batch  14057_49697 PROMOTE bckcetl ids20 dsadm J. Mahaffey for BJ Luce
# MAGIC ^1_2 06/26/06 13:45:36 Batch  14057_49555 INIT bckcett testIDS30 dsadm J. Mahaffey for BJ Luce
# MAGIC ^1_1 06/21/06 16:08:51 Batch  14052_58136 PROMOTE bckcett testIDS30 u05779 bj
# MAGIC ^1_1 06/21/06 16:08:06 Batch  14052_58089 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_4 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test
# MAGIC ^1_3 04/24/06 11:17:23 Batch  13994_40653 INIT bckcett devlIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_2 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/29/06 06:58:50 Batch  13968_25134 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsDMClmBal
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Using data created during processing, this job balances IDS inputs to Claim Datamart at the Claim level.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsDMClmBalSeq
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Compare the data in W_CLM_BAL (a working table filled with data from the most recent IDS Claims pull) to the Claim Datamart Claim table.  Any rows in the working table but not in the Claim Datamart Claim table are written to W_CLM_BAL_DTL.  Any rows that exist in both tables but do not have matching values are also written to W_CLM_BAL_DTL.  All claims written to W_CLM_BAL_DTL are placed in the IDS Claims hit-list to be reprocessed.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Job  Name                  Database                    Table Name
# MAGIC                  ----------------------------       ----------------------------       ---------------------------------------------------------------------
# MAGIC                                                     Claim_Datamart           W_CLM_BAL
# MAGIC                                                     Claim_Datamart          CLM_DM_CLM
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: 16;hf_webdm_bal1;hf_webdm_bal2;hf_webdm_bal3;hf_webdm_bal4;hf_webdm_bal5;hf_webdm_bal6;hf_webdm_bal7;hf_webdm_bal8;hf_webdm_bal9;hf_webdm_bal10;hf_webdm_bal11;hf_webdm_bal12;hf_webdm_bal13;hf_webdm_bal14;hf_webdm_bal15;hf_webdm_clm_hitlist
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  W_CLM_BAL_DTL
# MAGIC                     ids/update/FctsClmHitList.dat
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset, just restart
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-01-26      Gina Harris                Original Programming.
# MAGIC 2006-04-07      Brent Leland             Truncated Status Dates before writing to char(20) database field.
# MAGIC                                                          Split outputs instead of having them inline.
# MAGIC 2006-06-21      BJ Luce                    remove the not exist sql in the pull from W_CLM_BAL. Process all claims in W_CLM_BAL. Put in constraints.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsBalSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Using data created during processing, this job balances Facets inputs to IDS.  
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsClmRunAllJc
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  ../balancing/FctsIdsClmLnNotify.dat
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Extract jobs for a IDS target table create a list of records extracted from the source.  This sequencer loads that data and compares those records with the rows loaded into the target table.  Any deviation from perfect causes an email to be sent to the warehouse on-call account.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Job  Name                  Database                    Table Name
# MAGIC                  ----------------------------       ----------------------------       ---------------------------------------------------------------------
# MAGIC                                                     IDS_PROD                 W_TASK_EXCL
# MAGIC                                                     Universe                     TASK_EXCL
# MAGIC                                                     Universe                      RUN_CYCLE
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: WriteRunID
# MAGIC                                                                          RunCycExctnSel
# MAGIC                                                                          GetExclusionList
# MAGIC                                                                          GetRunInfo
# MAGIC 
# MAGIC HASH FILES: None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  
# MAGIC                  Job  Name                   Table Data Created
# MAGIC                  ----------------------------        -----------------------------------------------------------------------------------------------------------
# MAGIC                  FctsIdsClmLnBal         IDS_CLM_LN_MISSING.dat.#RunID#
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Load files will have to be recreated. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-12-27      Brent Leland             Original Programming.

# MAGIC IDS - Claim Datamart
# MAGIC Claim Balancing
# MAGIC Write all balancing errors to the hit list to pull the claim again.
# MAGIC Hash files are here to eliminate mutex error with collector.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner', '')
clmmart_secret_name = get_widget_value('clmmart_secret_name', '')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

extract_query = (
    "SELECT w.SRC_SYS_CD, w.CLM_ID, w.CLM_STTUS_CD, w.CLM_TYP_CD, w.CLM_SUBTYP_CD, "
    "w.CLM_PD_DT, w.CLM_REMIT_HIST_CHK_PD_DT, w.CLM_STTUS_DT, w.CLM_PAYBL_AMT, "
    "w.CLM_ACTL_PD_AMT, w.CLM_CHRG_AMT, w.CLM_REMIT_HIST_CHK_NET_PAY_AMT, "
    "w.CLM_REMIT_HIST_CHK_NO, w.MBR_UNIQ_KEY, w.PD_PROV_ID "
    f"FROM {ClmMartOwner}.W_CLM_BAL w"
)
df_W_CLM_BAL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = (
    "SELECT c.SRC_SYS_CD, c.CLM_ID, c.CLM_STTUS_CD, c.CLM_TYP_CD, c.CLM_SUBTYP_CD, "
    "c.CLM_PD_DT, c.CLM_REMIT_HIST_CHK_PD_DT, c.CLM_STTUS_DT, c.CLM_PAYBL_AMT, "
    "c.CLM_ACTL_PD_AMT, c.CLM_CHRG_AMT, c.CLM_REMIT_HIST_CHK_NET_PAY_AMT, "
    "c.CLM_REMIT_HIST_CHK_NO, c.MBR_UNIQ_KEY, c.PROV_PD_PROV_ID "
    f"FROM {ClmMartOwner}.CLM_DM_CLM c"
)
df_CLM_DM_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_join = df_W_CLM_BAL.alias("Claims").join(
    df_CLM_DM_CLM.alias("Lookup"),
    (F.col("Claims.SRC_SYS_CD") == F.col("Lookup.SRC_SYS_CD"))
    & (F.col("Claims.CLM_ID") == F.col("Lookup.CLM_ID")),
    "left"
)

df_MissingClaimID = (
    df_join.filter(F.col("Lookup.CLM_ID").isNull())
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_ID").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_ID").alias("SRC_VAL"),
        F.lit("MISSING").alias("TRGT_VAL")
    )
)
write_files(
    df_MissingClaimID.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal1.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_StatusCd = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_STTUS_CD") != F.col("Lookup.CLM_STTUS_CD"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_STTUS_CD").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_STTUS_CD").alias("SRC_VAL"),
        F.col("Lookup.CLM_STTUS_CD").alias("TRGT_VAL")
    )
)
write_files(
    df_StatusCd.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal2.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_TypeCd = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_TYP_CD") != F.col("Lookup.CLM_TYP_CD"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_TYP_CD").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_TYP_CD").alias("SRC_VAL"),
        F.col("Lookup.CLM_TYP_CD").alias("TRGT_VAL")
    )
)
write_files(
    df_TypeCd.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal3.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_SubtypeCd = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_SUBTYP_CD") != F.col("Lookup.CLM_SUBTYP_CD"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_SUBTYP_CD").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_SUBTYP_CD").alias("SRC_VAL"),
        F.col("Lookup.CLM_SUBTYP_CD").alias("TRGT_VAL")
    )
)
write_files(
    df_SubtypeCd.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal4.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_PaidDate = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_PD_DT") != F.col("Lookup.CLM_PD_DT"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_PD_DT").alias("BAL_CLMN_NM"),
        F.substring(F.col("Claims.CLM_PD_DT"), 1, 20).alias("SRC_VAL"),
        F.substring(F.col("Lookup.CLM_PD_DT"), 1, 20).alias("TRGT_VAL")
    )
)
write_files(
    df_PaidDate.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal5.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_CheckPaidDt = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_REMIT_HIST_CHK_PD_DT") != F.col("Lookup.CLM_REMIT_HIST_CHK_PD_DT"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_REMIT_HIST_CHK_PD_DT").alias("BAL_CLMN_NM"),
        F.substring(F.col("Claims.CLM_REMIT_HIST_CHK_PD_DT"), 1, 20).alias("SRC_VAL"),
        F.substring(F.col("Lookup.CLM_REMIT_HIST_CHK_PD_DT"), 1, 20).alias("TRGT_VAL")
    )
)
write_files(
    df_CheckPaidDt.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal6.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_StatusDt = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_STTUS_DT") != F.col("Lookup.CLM_STTUS_DT"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_STTUS_DT").alias("BAL_CLMN_NM"),
        F.substring(F.col("Claims.CLM_STTUS_DT"), 1, 20).alias("SRC_VAL"),
        F.substring(F.col("Lookup.CLM_STTUS_DT"), 1, 20).alias("TRGT_VAL")
    )
)
write_files(
    df_StatusDt.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal7.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_PayableAmt = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_PAYBL_AMT") != F.col("Lookup.CLM_PAYBL_AMT"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_PAYABL_AMT").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_PAYBL_AMT").alias("SRC_VAL"),
        F.col("Lookup.CLM_PAYBL_AMT").alias("TRGT_VAL")
    )
)
write_files(
    df_PayableAmt.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal8.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ActlPdAmt = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_ACTL_PD_AMT") != F.col("Lookup.CLM_ACTL_PD_AMT"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_ACTL_PD_AMT").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_ACTL_PD_AMT").alias("SRC_VAL"),
        F.col("Lookup.CLM_ACTL_PD_AMT").alias("TRGT_VAL")
    )
)
write_files(
    df_ActlPdAmt.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal9.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ChrgAmt = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_CHRG_AMT") != F.col("Lookup.CLM_CHRG_AMT"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_CHRG_AMT").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_CHRG_AMT").alias("SRC_VAL"),
        F.col("Lookup.CLM_CHRG_AMT").alias("TRGT_VAL")
    )
)
write_files(
    df_ChrgAmt.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal10.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ChkNetPayAmt = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (
            F.col("Claims.CLM_REMIT_HIST_CHK_NET_PAY_AMT")
            != F.col("Lookup.CLM_REMIT_HIST_CHK_NET_PAY_AMT")
        )
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_REMIT_HIST_CHK_NET_PAY_AMT").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_REMIT_HIST_CHK_NET_PAY_AMT").alias("SRC_VAL"),
        F.col("Lookup.CLM_REMIT_HIST_CHK_NET_PAY_AMT").alias("TRGT_VAL")
    )
)
write_files(
    df_ChkNetPayAmt.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal11.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ChkNo = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.CLM_REMIT_HIST_CHK_NO") != F.col("Lookup.CLM_REMIT_HIST_CHK_NO"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("CLM_REMIT_HIST_CHK_NO").alias("BAL_CLMN_NM"),
        F.col("Claims.CLM_REMIT_HIST_CHK_NO").alias("SRC_VAL"),
        F.col("Lookup.CLM_REMIT_HIST_CHK_NO").alias("TRGT_VAL")
    )
)
write_files(
    df_ChkNo.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal12.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_MbrUniqKey = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.MBR_UNIQ_KEY") != F.col("Lookup.MBR_UNIQ_KEY"))
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("MBR_UNIQ_KEY").alias("BAL_CLMN_NM"),
        F.col("Claims.MBR_UNIQ_KEY").alias("SRC_VAL"),
        F.col("Lookup.MBR_UNIQ_KEY").alias("TRGT_VAL")
    )
)
write_files(
    df_MbrUniqKey.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal13.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_PdProvId = (
    df_join.filter(
        F.col("Lookup.CLM_ID").isNotNull()
        & (F.col("Claims.PD_PROV_ID") != F.col("Lookup.PROV_PD_PROV_ID"))
        & (
            (F.col("Claims.PD_PROV_ID") != F.lit("NA"))
            | (
                (F.col("Lookup.PROV_PD_PROV_ID") != F.lit("NA"))
                & F.col("Lookup.PROV_PD_PROV_ID").isNotNull()
            )
        )
    )
    .select(
        F.col("Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Claims.CLM_ID").alias("CLM_ID"),
        F.lit("PROV_PD_PROV_ID").alias("BAL_CLMN_NM"),
        F.col("Claims.PD_PROV_ID").alias("SRC_VAL"),
        F.col("Lookup.PROV_PD_PROV_ID").alias("TRGT_VAL")
    )
)
write_files(
    df_PdProvId.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal14.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_MissingClaimIDout = spark.read.parquet("hf_webdm_bal1.parquet")
df_StatusCdout = spark.read.parquet("hf_webdm_bal2.parquet")
df_TypeCdout = spark.read.parquet("hf_webdm_bal3.parquet")
df_SubtypeCdout = spark.read.parquet("hf_webdm_bal4.parquet")
df_PaidDateout = spark.read.parquet("hf_webdm_bal5.parquet")
df_CheckPaidDtout = spark.read.parquet("hf_webdm_bal6.parquet")
df_StatusDtout = spark.read.parquet("hf_webdm_bal7.parquet")
df_PayableAmtout = spark.read.parquet("hf_webdm_bal8.parquet")
df_ActlPdAmtout = spark.read.parquet("hf_webdm_bal9.parquet")
df_ChrgAmtout = spark.read.parquet("hf_webdm_bal10.parquet")
df_ChkNetPayAmtout = spark.read.parquet("hf_webdm_bal11.parquet")
df_ChkNoout = spark.read.parquet("hf_webdm_bal12.parquet")
df_MbrUniqKeyout = spark.read.parquet("hf_webdm_bal13.parquet")
df_PdProvIdout = spark.read.parquet("hf_webdm_bal14.parquet")

df_collector = (
    df_MissingClaimIDout
    .unionByName(df_StatusCdout)
    .unionByName(df_TypeCdout)
    .unionByName(df_SubtypeCdout)
    .unionByName(df_PaidDateout)
    .unionByName(df_CheckPaidDtout)
    .unionByName(df_StatusDtout)
    .unionByName(df_PayableAmtout)
    .unionByName(df_ActlPdAmtout)
    .unionByName(df_ChrgAmtout)
    .unionByName(df_ChkNetPayAmtout)
    .unionByName(df_ChkNoout)
    .unionByName(df_MbrUniqKeyout)
    .unionByName(df_PdProvIdout)
)
write_files(
    df_collector.select("SRC_SYS_CD", "CLM_ID", "BAL_CLMN_NM", "SRC_VAL", "TRGT_VAL"),
    "hf_webdm_bal15.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Log = spark.read.parquet("hf_webdm_bal15.parquet")

df_DSLink74 = df_Log.select(
    F.col("CLM_ID").alias("CLM_ID")
)
write_files(
    df_DSLink74.select("CLM_ID"),
    "hf_webdm_clm_hitlist.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ReformattedData = df_Log.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("BAL_CLMN_NM").alias("BAL_CLMN_NM"),
    F.col("SRC_VAL").alias("SRC_VAL"),
    F.col("TRGT_VAL").alias("TRGT_VAL")
)

temp_table_name = "STAGING.IdsDMClmBal_W_CLM_BAL_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)
df_ReformattedData.write.jdbc(
    url=jdbc_url,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = (
    f"MERGE INTO {ClmMartOwner}.W_CLM_BAL_DTL AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID AND T.BAL_CLMN_NM = S.BAL_CLMN_NM "
    "WHEN MATCHED THEN UPDATE SET T.SRC_VAL = S.SRC_VAL, T.TRGT_VAL = S.TRGT_VAL "
    "WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, CLM_ID, BAL_CLMN_NM, SRC_VAL, TRGT_VAL) "
    "VALUES (S.SRC_SYS_CD, S.CLM_ID, S.BAL_CLMN_NM, S.SRC_VAL, S.TRGT_VAL);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_HitList = spark.read.parquet("hf_webdm_clm_hitlist.parquet")

write_files(
    df_HitList.select("CLM_ID"),
    f"{adls_path}/ids/update/FctsClmHitList.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)