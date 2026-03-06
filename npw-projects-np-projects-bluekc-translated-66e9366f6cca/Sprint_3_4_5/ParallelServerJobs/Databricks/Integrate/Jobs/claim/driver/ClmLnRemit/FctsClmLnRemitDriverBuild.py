# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_4 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC ^1_1 06/29/09 16:32:48 Batch  15156_59635 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew        Maddy
# MAGIC ^1_1 04/24/09 09:20:30 Batch  15090_33635 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 04/24/09 09:14:25 Batch  15090_33296 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_1 08/22/08 10:09:03 Batch  14845_36590 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 08/22/08 09:55:32 Batch  14845_35734 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_1 08/19/08 10:43:19 Batch  14842_38609 PROMOTE bckcett testIDS u03651 steph for Sharon 3057
# MAGIC ^1_1 08/19/08 10:38:02 Batch  14842_38285 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC **************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsClmLnRemitDriverBuild
# MAGIC CALLED BY: BCBSClmRemitPrereqSeq
# MAGIC 
# MAGIC                           
# MAGIC PROCESSING:
# MAGIC                   Extracts Facets claims based on last activity date.  Claim IDs for claims that are already in the IDS are saved for the delete process that is done prior to loaded the updated claims.
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                      Change Description                                                                                 Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                      -----------------------------------------------------------------------                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andre          08/01/2008     #3057 Prov Web Redsign     program created                                                                                              devlIDnew                       
# MAGIC 
# MAGIC Parik                       2008-07-30       #3057                                   Added nasco dup bypass check criteria                                                          devlIDSnew                  Steph Goddard           08/11/2008
# MAGIC SANdrew                2009-03-10       Prod Support                        Renamed from FctsClmRemitDriverBuild TO FctsClmLnRemitDriverBuild         devlIDS                         Steph Goddard           03/24/2009
# MAGIC                                                                                                       Added criteria checking if re-issued check.  if so builds a hash file used in ClmLnRemitExtr
# MAGIC Prabhu ES              2022-02-26       S2S Remediation                   MSSQL connection parameters added                                                           IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Reversal hash file and status file used in Claim Line Remit and Claim Line Remit Disallow jobs
# MAGIC only claims on the PD tables
# MAGIC used later in ClmLnRemit Extract programs
# MAGIC can be deleted at end of this job
# MAGIC used later in ClmLnRemit Extract programs
# MAGIC test the facets check tables to see if check was issued
# MAGIC used later in both ClmLnRemit Extract programs
# MAGIC nasco_dup_bypass is list of claims that are Nasco Duplicates. Criteria:
# MAGIC Host plan = 740
# MAGIC Group id = 'BLUECARE'
# MAGIC Claim sequence number = '00'
# MAGIC text box 1 in claim attachment = blank
# MAGIC test box 2 in claim attachment not = blank
# MAGIC test box 3 in claim attachment = blank
# MAGIC All claim extract jobs will not build a row for claim if on this hash file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, length, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
CommitPoint = get_widget_value('CommitPoint','10000')
FacetsOwner = get_widget_value('$FacetsOwner','$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(tempdb_secret_name)

# TMP_IDS_CLAIM_REMIT
extract_query = f"SELECT CLM_ID, CLM_STS, CLCL_PAID_DT, CLCL_ID_ADJ_TO, CLCL_ID_ADJ_FROM FROM tempdb..{DriverTable} DRIVER"
df_TMP_IDS_CLAIM_REMIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_tempdb)
    .options(**jdbc_props_tempdb)
    .option("query", extract_query)
    .load()
)

# TrnsSK
df_TrnsSK = df_TMP_IDS_CLAIM_REMIT.filter(col("CLM_STS") == "91").select(
    trim(col("CLM_ID")).alias("CLCL_ID"),
    trim(col("CLM_STS")).alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    trim(col("CLCL_ID_ADJ_TO")).alias("CLCL_ID_ADJ_TO"),
    trim(col("CLCL_ID_ADJ_FROM")).alias("CLCL_ID_ADJ_FROM")
)

# hf_clm_remit_fcts_reversals (Scenario A: intermediate hashed file)
df_hf_clm_remit_fcts_reversals = df_TrnsSK.dropDuplicates(["CLCL_ID"])

# IDS_CLAIM_REMIT_chk_reissue
extract_query = f"""
SELECT 
DRVR.CLM_ID,
DRVR.CLM_STS,
DRVR.CLCL_PAID_DT,
DRVR.CLCL_ID_ADJ_TO,
DRVR.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRVR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CLCK,
{FacetsOwner}.CMC_CKCK_CHECK CKCK,
{FacetsOwner}.CMC_CLCL_CLAIM CLM
WHERE CKCK.CKPY_REF_ID = CLCK.CKPY_REF_ID
AND CKCK.CKCK_REISS_DT <> '1753-01-01'
AND CLCK.CLCL_ID = DRVR.CLM_ID
AND DRVR.CLM_ID = CLM.CLCL_ID
"""
df_IDS_CLAIM_REMIT_chk_reissue = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_tempdb)
    .options(**jdbc_props_tempdb)
    .option("query", extract_query)
    .load()
)

# chk_TrnsSK
df_chk_TrnsSK = df_IDS_CLAIM_REMIT_chk_reissue.select(
    trim(col("CLM_ID")).alias("CLCL_ID"),
    trim(col("CLM_STS")).alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    trim(col("CLCL_ID_ADJ_TO")).alias("CLCL_ID_ADJ_TO"),
    trim(col("CLCL_ID_ADJ_FROM")).alias("CLCL_ID_ADJ_FROM")
)

# hf_clm_remit_check_reissue (Scenario A: intermediate hashed file)
df_hf_clm_remit_check_reissue = df_chk_TrnsSK.dropDuplicates(["CLCL_ID"])

# test_chk_conditions (join: reissued_chks as primary, all_adj_clms as lookup (left))
df_test_chk_conditions_join = df_hf_clm_remit_check_reissue.alias("reissued_chks").join(
    df_hf_clm_remit_fcts_reversals.alias("all_adj_clms"),
    col("reissued_chks.CLCL_ID_ADJ_TO") == col("all_adj_clms.CLCL_ID"),
    how="left"
)

df_reissue_w_pd_adj = df_test_chk_conditions_join.filter(col("all_adj_clms.CLCL_ID").isNotNull()).select(
    col("reissued_chks.CLCL_ID").alias("CLCL_ID"),
    col("reissued_chks.CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    col("reissued_chks.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    col("reissued_chks.CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    col("reissued_chks.CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM")
)

df_reissue_only = df_test_chk_conditions_join.filter(col("all_adj_clms.CLCL_ID").isNull()).select(
    col("reissued_chks.CLCL_ID").alias("CLCL_ID"),
    col("reissued_chks.CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    col("reissued_chks.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    col("reissued_chks.CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    col("reissued_chks.CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM")
)

# hf_clm_remit_chk_w_pd_adj (Scenario C: final hashed file → parquet)
df_reissue_w_pd_adj_write = df_reissue_w_pd_adj.select(
    rpad(col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    rpad(col("CLCL_CUR_STS"), 2, " ").alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    rpad(col("CLCL_ID_ADJ_TO"), 12, " ").alias("CLCL_ID_ADJ_TO"),
    rpad(col("CLCL_ID_ADJ_FROM"), 12, " ").alias("CLCL_ID_ADJ_FROM")
)

write_files(
    df_reissue_w_pd_adj_write,
    f"{adls_path}/hf_clm_remit_chk_w_pd_adj.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# hf_clm_remit_check_reissue_only (Scenario C: final hashed file → parquet)
df_reissue_only_write = df_reissue_only.select(
    rpad(col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    rpad(col("CLCL_CUR_STS"), 2, " ").alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    rpad(col("CLCL_ID_ADJ_TO"), 12, " ").alias("CLCL_ID_ADJ_TO"),
    rpad(col("CLCL_ID_ADJ_FROM"), 12, " ").alias("CLCL_ID_ADJ_FROM")
)

write_files(
    df_reissue_only_write,
    f"{adls_path}/hf_clm_remit_check_reissue_only.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# TMP_ID_CLMS_NASCO_DUPS
extract_query = f"""
SELECT
CLM_ID,
ATUF_TEXT1,
ATUF_TEXT2,
ATUF_TEXT3,
CLM.CLCL_ID_ADJ_TO,
CLM.CLCL_ID_ADJ_FROM,
CLM.CLCL_PAID_DT,
CLM.CLCL_LAST_ACT_DTM
FROM tempdb..{DriverTable} TMP,
{FacetsOwner}.CMC_CLCL_CLAIM CLM,
{FacetsOwner}.CMC_GRGR_GROUP GRP,
{FacetsOwner}.CMC_CLMI_MISC MISC,
{FacetsOwner}.CER_ATXR_ATTACH_U U,
{FacetsOwner}.CER_ATUF_USERFLD_D US
WHERE
TMP.CLM_ID = CLM.CLCL_ID
AND CLM.CLCL_ID = MISC.CLCL_ID
AND MISC.CLMI_HOST_PLAN_CD = '740'
AND CLM.GRGR_CK = GRP.GRGR_CK
AND GRP.GRGR_ID = 'BLUECARD'
AND CLM.ATXR_SOURCE_ID = U.ATXR_SOURCE_ID
AND U.ATSY_ID = 'HTCN'
AND U.ATXR_DEST_ID = US.ATXR_DEST_ID
AND US.ATSY_ID = U.ATSY_ID
"""
df_TMP_ID_CLMS_NASCO_DUPS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_tempdb)
    .options(**jdbc_props_tempdb)
    .option("query", extract_query)
    .load()
)

# Trans
df_Nasco_dup_bypass = df_TMP_ID_CLMS_NASCO_DUPS.filter(
    (substring(col("CLM_ID"), 11, 2) == "00")
    & (length(trim(col("ATUF_TEXT1"))) == 0)
    & (length(trim(col("ATUF_TEXT2"))) > 0)
    & (length(trim(col("ATUF_TEXT3"))) == 0)
).select(
    trim(col("CLM_ID")).alias("CLM_ID")
)

# clm_remit_nasco_dup_hashfiles (Scenario C: final hashed file → parquet)
df_clm_remit_nasco_dup_bypass = df_Nasco_dup_bypass.select(
    rpad(col("CLM_ID"), 12, " ").alias("CLM_ID")
)

write_files(
    df_clm_remit_nasco_dup_bypass,
    f"{adls_path}/hf_clm_remit_nasco_dup_bypass.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)