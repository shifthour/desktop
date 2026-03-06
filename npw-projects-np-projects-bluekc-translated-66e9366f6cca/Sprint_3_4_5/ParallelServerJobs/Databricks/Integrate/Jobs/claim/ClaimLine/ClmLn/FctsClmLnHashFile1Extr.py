# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_5 12/18/06 11:16:18 Batch  14232_40614 INIT bckcetl ids20 dsadm Backup for 12/18 install
# MAGIC ^1_4 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey
# MAGIC ^1_4 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_3 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_3 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
# MAGIC ^1_3 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_2 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/23/06 14:55:53 Batch  13962_53758 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 01/06/06 12:42:54 Batch  13886_45778 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC ********************************************************************************
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
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
# MAGIC \(9)Developer\(9)Date\(9)\(9)Comment
# MAGIC \(9)-----------------------------\(9)---------------------------\(9)-----------------------------------------------------------
# MAGIC \(9)BJ Luce                  3/30/2006                initial program
# MAGIC                 BJ Luce                  4/12/2006                drop AA if ATAU exists for claim line
# MAGIC                                                                                  Added fetch first row to dsalw_excd SQL
# MAGIC                  Prabhu ES             2022-02-26                 S2S Remediation-MSSQL connection parameters added   IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-10

# MAGIC Facets Claim Line Hash File Extract
# MAGIC These hash files are used in FctsClmLnHashFile2Extr and FctsClmLnHashFile3Extr
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Load all rows on CDDD and CDMD to P_CLM_LN_DSALW for the claims being processed.   This table will have to be part of the delete process.
# MAGIC 
# MAGIC The P_CLM_LN_DSALW table will be used for reprocessing disallows rather than repulling from Facets archive.  
# MAGIC 
# MAGIC Loaded in IdsFctsClmLoad4Seq.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# --------------------------------------------------------------------------------
# Retrieve job parameters
# --------------------------------------------------------------------------------
Source = get_widget_value("Source","FACETS")
DriverTable = get_widget_value("DriverTable","")
RunCycle = get_widget_value("RunCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")

# --------------------------------------------------------------------------------
# cddd_cdmd_all (ODBCConnector) --> df_cddd_cdmd_all
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_cddd_cdmd_all = f"""
SELECT CLCL_ID,CDML_SEQ_NO,CDMD_TYPE,CDMD_DISALL_AMT,EXCD_ID,MEME_CK 
FROM {FacetsOwner}.CMC_CDMD_LI_DISALL DIS, tempdb..{DriverTable} TMP 
WHERE DIS.CLCL_ID = TMP.CLM_ID
UNION
SELECT CLCL_ID,CDDL_SEQ_NO,CDDD_TYPE,CDDD_DISALL_AMT,EXCD_ID,MEME_CK 
FROM {FacetsOwner}.CMC_CDDD_DNLI_DIS DIS, tempdb..{DriverTable} TMP 
WHERE DIS.CLCL_ID = TMP.CLM_ID
"""
df_cddd_cdmd_all = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cddd_cdmd_all)
    .load()
)

# --------------------------------------------------------------------------------
# Trans1 (CTransformerStage)
# Input: df_cddd_cdmd_all
# Output columns (p_clm_ln_dsalw)
# --------------------------------------------------------------------------------
df_trans1 = df_cddd_cdmd_all.withColumn(
    "SRC_SYS_CD_SK",
    GetFkeyCodes("IDS", 0, "SOURCE SYSTEM", "FACETS", "X")
).withColumn(
    "CLM_ID",
    F.col("CLCL_ID")
).withColumn(
    "CLM_LN_SEQ_NO",
    F.col("CDML_SEQ_NO")
).withColumn(
    "CLM_LN_DSALW_TYP_CD",
    F.col("CDMD_TYPE")
).withColumn(
    "CLM_LN_DSALW_EXCD",
    F.col("EXCD_ID")
).withColumn(
    "DSALW_AMT",
    F.col("CDMD_DISALL_AMT")
).withColumn(
    "MBR_UNIQ_KEY",
    F.col("MEME_CK")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(RunCycle)
)

# Select in exact order and apply rpad for char/varchar columns if needed.
# (No explicit lengths given here for these columns except those flagged "char" or "varchar" in the job—none were marked in this particular set, so we keep them as-is.)
df_p_clm_ln_dsalw = df_trans1.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "CLM_LN_DSALW_EXCD",
    "DSALW_AMT",
    "MBR_UNIQ_KEY",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# --------------------------------------------------------------------------------
# P_CLM_LN_DSALW (CSeqFileStage) - write the file
# File path -> #$FilePath#/load/P_CLM_LN_DSALW.#Source#.dat => f"{adls_path}/load/P_CLM_LN_DSALW.{Source}.dat"
# --------------------------------------------------------------------------------
write_files(
    df_p_clm_ln_dsalw,
    f"{adls_path}/load/P_CLM_LN_DSALW.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# dsalw_excd (DB2Connector) READ from IDS
# We'll load DSALW_EXCD joined with CD_MPPNG into a single DataFrame, then deduplicate to mimic "fetch first 1 rows only."
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_dsalw_excd = f"""
SELECT 
  EX.EXCD_ID,
  EX.EFF_DT_SK,
  EX.TERM_DT_SK,
  MP.SRC_CD,
  MP.BYPS_IN
FROM {IDSOwner}.DSALW_EXCD EX
JOIN {IDSOwner}.CD_MPPNG MP
  ON EX.EXCD_RESP_CD_SK = MP.CD_MPPNG_SK
"""
df_dsalw_excd_full = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_dsalw_excd)
    .load()
)

df_dsalw_excd = dedup_sort(
    df_dsalw_excd_full,
    partition_cols=["EXCD_ID","EFF_DT_SK","TERM_DT_SK"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# FacetsOverrides (ODBCConnector) - This stage has 3 output pins, each with different queries
# --------------------------------------------------------------------------------
# 1) its_claim -> df_its_claim
query_its_claim = f"""
SELECT MISC.CLCL_ID 
FROM {FacetsOwner}.CMC_CDOR_LI_OVR OVR,
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM,
     tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CLMI_MISC MISC
WHERE OVR.CLCL_ID = TMP.CLM_ID
  AND OVR.CLCL_ID = CLAIM.CLCL_ID
  AND OVR.CDOR_OR_ID in ('AU','AT','DU','DT')
  AND OVR.CLCL_ID = MISC.CLCL_ID
  AND MISC.CLMI_ITS_SCCF_NO > ' '
"""
df_its_claim = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_its_claim)
    .load()
)

# 2) cdor_x_extr -> df_cdor_x_extr
query_cdor_x_extr = f"""
SELECT OVR.CLCL_ID,OVR.CDML_SEQ_NO,OVR.CDOR_OR_ID,OVR.CDOR_OR_AMT,OVR.EXCD_ID,CLAIM.CLCL_PAID_DT, LINE.CDML_DISALL_AMT 
FROM {FacetsOwner}.CMC_CDOR_LI_OVR OVR,
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM,
     tempdb..{DriverTable} TMP, 
     {FacetsOwner}.CMC_CDML_CL_LINE LINE
WHERE  OVR.CLCL_ID = TMP.CLM_ID
  AND  OVR.CLCL_ID = CLAIM.CLCL_ID
  AND  OVR.CDOR_OR_ID = 'AX'
  AND  OVR.CLCL_ID = LINE.CLCL_ID
  AND  OVR.CDML_SEQ_NO  = LINE.CDML_SEQ_NO
  AND  OVR.CDOR_OR_AMT > 0
  AND  LINE.CDML_DISALL_AMT <> 0
UNION
SELECT OVR.CLCL_ID,OVR.CDDL_SEQ_NO,OVR.CDDO_OR_ID,OVR.CDDO_OR_AMT,OVR.EXCD_ID,CLAIM.CLCL_PAID_DT, LINE.CDDL_DISALL_AMT
FROM {FacetsOwner}.CMC_CDDO_DNLI_OVR OVR,
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM,
     tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CDDL_CL_LINE LINE
WHERE  OVR.CLCL_ID = TMP.CLM_ID
  AND  OVR.CLCL_ID = CLAIM.CLCL_ID
  AND  OVR.CDDO_OR_ID = 'DX'
  AND  OVR.CLCL_ID = LINE.CLCL_ID
  AND  OVR.CDDL_SEQ_NO = LINE.CDDL_SEQ_NO
  AND  OVR.CDDO_OR_AMT > 0
  AND  LINE.CDDL_DISALL_AMT <> 0
"""
df_cdor_x_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cdor_x_extr)
    .load()
)

# 3) cdor_lkup -> df_cdor_lkup
query_cdor_lkup = f"""
SELECT OVR.CLCL_ID,OVR.CDML_SEQ_NO,OVR.CDOR_OR_ID,CDOR_OR_AMT,EXCD_ID,CLAIM.CLCL_PAID_DT,
       LINE.CDML_CONSIDER_CHG,LINE.CDML_DISALL_AMT,LINE.CDML_DISALL_EXCD
FROM {FacetsOwner}.CMC_CDOR_LI_OVR OVR,
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM,
     tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CDML_CL_LINE LINE
WHERE  OVR.CLCL_ID = TMP.CLM_ID
  AND  OVR.CLCL_ID = CLAIM.CLCL_ID
  AND  OVR.CDOR_OR_ID in ('AA','AU','AT')
  AND  OVR.CLCL_ID = LINE.CLCL_ID
  AND  OVR.CDML_SEQ_NO  = LINE.CDML_SEQ_NO
  AND  LINE.CDML_DISALL_AMT <> 0
UNION
SELECT OVR.CLCL_ID,OVR.CDDL_SEQ_NO,OVR.CDDO_OR_ID,CDDO_OR_AMT,EXCD_ID,CLAIM.CLCL_PAID_DT,
       LINE.CDDL_CONSIDER_CHG,LINE.CDDL_DISALL_AMT,LINE.CDDL_DISALL_EXCD
FROM {FacetsOwner}.CMC_CDDO_DNLI_OVR OVR,
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM,
     tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CDDL_CL_LINE LINE
WHERE  OVR.CLCL_ID = TMP.CLM_ID
  AND  OVR.CLCL_ID = CLAIM.CLCL_ID
  AND  OVR.CDDO_OR_ID in ('DA','DU','DT')
  AND  OVR.CLCL_ID = LINE.CLCL_ID
  AND  OVR.CDDL_SEQ_NO  = LINE.CDDL_SEQ_NO
  AND  LINE.CDDL_DISALL_AMT <> 0
"""
df_cdor_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cdor_lkup)
    .load()
)

# --------------------------------------------------------------------------------
# Trans3 (CTransformerStage)
# Primary link: df_cdor_lkup (cdor_lkup)
# Lookup link: df_dsalw_excd (DsalwExcdRespCdAAAT), join type left
#   Conditions:
#     cdor_lkup.EXCD_ID = dsalw_excd.EXCD_ID
#     substring(cdor_lkup.CLCL_PAID_DT,1,10) = dsalw_excd.EFF_DT_SK
#     substring(cdor_lkup.CLCL_PAID_DT,1,10) = dsalw_excd.TERM_DT_SK
#
# Output pins:
#   1) CDOR_A_and_T => constraint cdor_lkup.CDOR_OR_ID in ('AA','AT','DA','DT')
#   2) CDOR_U => constraint cdor_lkup.CDOR_OR_ID in ('AU','DU')
# --------------------------------------------------------------------------------

# First, join df_cdor_lkup with df_dsalw_excd:
df_join_3 = df_cdor_lkup.alias("cdor_lkup").join(
    df_dsalw_excd.alias("DsalwExcdRespCdAAAT"),
    (
        (F.col("cdor_lkup.EXCD_ID") == F.col("DsalwExcdRespCdAAAT.EXCD_ID")) &
        (F.substring(F.col("cdor_lkup.CLCL_PAID_DT"),1,10) == F.col("DsalwExcdRespCdAAAT.EFF_DT_SK")) &
        (F.substring(F.col("cdor_lkup.CLCL_PAID_DT"),1,10) == F.col("DsalwExcdRespCdAAAT.TERM_DT_SK"))
    ),
    how="left"
)

# cdor_lkup.* plus columns from dsalw_excd
df_trans3_full = df_join_3.select(
    F.col("cdor_lkup.CLCL_ID").alias("CLCL_ID"),
    F.col("cdor_lkup.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("cdor_lkup.CDOR_OR_ID").alias("CDOR_OR_ID"),
    F.col("cdor_lkup.CDOR_OR_AMT").alias("CDOR_OR_AMT"),
    F.col("cdor_lkup.EXCD_ID").alias("EXCD_ID"),
    F.col("cdor_lkup.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("cdor_lkup.CDML_CONSIDER_CHG").alias("CDML_CONSIDER_CHG"),
    F.col("cdor_lkup.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.col("cdor_lkup.CDML_DISALL_EXCD").alias("CDML_DISALL_EXCD"),
    F.when(F.col("DsalwExcdRespCdAAAT.SRC_CD").isNull(), F.lit("NA")).otherwise(F.col("DsalwExcdRespCdAAAT.SRC_CD")).alias("SRC_CD"),
    F.when(F.col("DsalwExcdRespCdAAAT.BYPS_IN").isNull(), F.lit("N")).otherwise(F.col("DsalwExcdRespCdAAAT.BYPS_IN")).alias("BYPS_IN")
)

# Output 1) CDOR_A_and_T
df_CDOR_A_and_T = df_trans3_full.filter(
    (F.col("CDOR_OR_ID").isin(["AA","AT","DA","DT"]))
).select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    "CDML_SEQ_NO",
    F.rpad(F.col("CDOR_OR_ID"),2," ").alias("CDOR_OR_ID"),
    "CDOR_OR_AMT",
    F.rpad(F.col("EXCD_ID"),3," ").alias("EXCD_ID"),
    "CLCL_PAID_DT",
    "CDML_CONSIDER_CHG",
    "CDML_DISALL_AMT",
    F.rpad(F.col("CDML_DISALL_EXCD"),3," ").alias("CDML_DISALL_EXCD"),
    F.rpad(F.col("SRC_CD"),2," ").alias("SRC_CD"),      # was declared as char/varchar? job says SRC_CD (char? Not explicitly typed). We'll do rpad by 2 from the design? It's not fully clear. 
    F.rpad(F.col("BYPS_IN"),1," ").alias("BYPS_IN")
)

# Output 2) CDOR_U
df_CDOR_U = df_trans3_full.filter(
    (F.col("CDOR_OR_ID").isin(["AU","DU"]))
).select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    "CDML_SEQ_NO",
    F.rpad(F.col("CDOR_OR_ID"),2," ").alias("CDOR_OR_ID"),
    "CDOR_OR_AMT",
    F.rpad(F.col("EXCD_ID"),3," ").alias("EXCD_ID"),
    "CLCL_PAID_DT",
    "CDML_CONSIDER_CHG",
    "CDML_DISALL_AMT",
    F.rpad(F.col("CDML_DISALL_EXCD"),3," ").alias("CDML_DISALL_EXCD"),
    F.rpad(F.col("SRC_CD"),2," ").alias("SRC_CD"),
    F.rpad(F.col("BYPS_IN"),1," ").alias("BYPS_IN")
)

# --------------------------------------------------------------------------------
# cdor_a_t (CHashedFileStage) - scenario A
# Originally: Trans3 -> cdor_a_t -> (Trans2 as t_lkup & a_lkup) + (trns_a_t as a_t_extr_t)
# We remove the physical hashed file and deduplicate on key columns [CLCL_ID, CDML_SEQ_NO, CDOR_OR_ID].
# --------------------------------------------------------------------------------
df_cdor_a_t_dedup = dedup_sort(
    df_CDOR_A_and_T,
    partition_cols=["CLCL_ID","CDML_SEQ_NO","CDOR_OR_ID"],
    sort_cols=[]
)

# We will reuse df_cdor_a_t_dedup for 2 lookups in Trans2 and 1 primary link in trns_a_t.

# --------------------------------------------------------------------------------
# cdor_u (CHashedFileStage) - scenario A
# Trans3 -> cdor_u -> trns_a_t as "au_lkup"
# We remove the physical hashed file and deduplicate on key columns [CLCL_ID, CDML_SEQ_NO, CDOR_OR_ID].
# --------------------------------------------------------------------------------
df_cdor_u_dedup = dedup_sort(
    df_CDOR_U,
    partition_cols=["CLCL_ID","CDML_SEQ_NO","CDOR_OR_ID"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# its_clm_lkup (CHashedFileStage) - scenario A
# FacetsOverrides -> its_clm_lkup -> trns_a_t (its_lkup)
# Key column is [CLCL_ID] marked as primary
# --------------------------------------------------------------------------------
df_its_claim = df_its_claim.select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID")
)
df_its_clm_lkup_dedup = dedup_sort(
    df_its_claim,
    partition_cols=["CLCL_ID"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Trans2 (CTransformerStage)
# Primary link: df_cdor_x_extr (cdor_x_extr)
# Lookup links:
#   1) t_lkup -> from df_cdor_a_t_dedup, left join on
#       cdor_x_extr.CLCL_ID = t_lkup.CLCL_ID,
#       cdor_x_extr.CDML_SEQ_NO = t_lkup.CDML_SEQ_NO,
#       'AT' = t_lkup.CDOR_OR_ID
#   2) a_lkup -> from df_cdor_a_t_dedup, left join on
#       cdor_x_extr.CLCL_ID = a_lkup.CLCL_ID,
#       cdor_x_extr.CDML_SEQ_NO = a_lkup.CDML_SEQ_NO,
#       'AA' = a_lkup.CDOR_OR_ID
#   3) DsalwExcdRespCdX -> df_dsalw_excd, left join on
#       cdor_x_extr.EXCD_ID = DsalwExcdRespCdX.EXCD_ID
#       substring(cdor_x_extr.CLCL_PAID_DT,1,10) = DsalwExcdRespCdX.EFF_DT_SK
#       substring(cdor_x_extr.CLCL_PAID_DT,1,10) = DsalwExcdRespCdX.TERM_DT_SK
#
# Output pin: dsalw_x -> constraint: 
#   IsNull(t_lkup.CDML_SEQ_NO) = @TRUE 
#   AND IsNull(a_lkup.CDML_SEQ_NO) = @TRUE 
#   AND trim(cdor_x_extr.EXCD_ID) <> '319'
# --------------------------------------------------------------------------------

# First, rename columns in df_cdor_a_t_dedup so we can do multiple joins
df_cdor_a_t_for_t = df_cdor_a_t_dedup.withColumnRenamed("CLCL_ID","t_CLCL_ID") \
                                      .withColumnRenamed("CDML_SEQ_NO","t_CDML_SEQ_NO") \
                                      .withColumnRenamed("CDOR_OR_ID","t_CDOR_OR_ID") \
                                      .withColumnRenamed("CDOR_OR_AMT","t_CDOR_OR_AMT") \
                                      .withColumnRenamed("EXCD_ID","t_EXCD_ID") \
                                      .withColumnRenamed("CLCL_PAID_DT","t_CLCL_PAID_DT") \
                                      .withColumnRenamed("CDML_CONSIDER_CHG","t_CDML_CONSIDER_CHG") \
                                      .withColumnRenamed("CDML_DISALL_AMT","t_CDML_DISALL_AMT") \
                                      .withColumnRenamed("CDML_DISALL_EXCD","t_CDML_DISALL_EXCD") \
                                      .withColumnRenamed("SRC_CD","t_SRC_CD") \
                                      .withColumnRenamed("BYPS_IN","t_BYPS_IN")

df_cdor_a_t_for_a = df_cdor_a_t_dedup.withColumnRenamed("CLCL_ID","a_CLCL_ID") \
                                      .withColumnRenamed("CDML_SEQ_NO","a_CDML_SEQ_NO") \
                                      .withColumnRenamed("CDOR_OR_ID","a_CDOR_OR_ID") \
                                      .withColumnRenamed("CDOR_OR_AMT","a_CDOR_OR_AMT") \
                                      .withColumnRenamed("EXCD_ID","a_EXCD_ID") \
                                      .withColumnRenamed("CLCL_PAID_DT","a_CLCL_PAID_DT") \
                                      .withColumnRenamed("CDML_CONSIDER_CHG","a_CDML_CONSIDER_CHG") \
                                      .withColumnRenamed("CDML_DISALL_AMT","a_CDML_DISALL_AMT") \
                                      .withColumnRenamed("CDML_DISALL_EXCD","a_CDML_DISALL_EXCD") \
                                      .withColumnRenamed("SRC_CD","a_SRC_CD") \
                                      .withColumnRenamed("BYPS_IN","a_BYPS_IN")

df_dsalw_excd_for_x = df_dsalw_excd.withColumnRenamed("EXCD_ID","x_EXCD_ID") \
                                   .withColumnRenamed("EFF_DT_SK","x_EFF_DT_SK") \
                                   .withColumnRenamed("TERM_DT_SK","x_TERM_DT_SK") \
                                   .withColumnRenamed("SRC_CD","x_SRC_CD") \
                                   .withColumnRenamed("BYPS_IN","x_BYPS_IN")

df_trans2_join_1 = df_cdor_x_extr.alias("cdor_x_extr") \
    .join(
        df_cdor_a_t_for_t.alias("t_lkup"),
        on=(
            (F.col("cdor_x_extr.CLCL_ID") == F.col("t_lkup.t_CLCL_ID")) &
            (F.col("cdor_x_extr.CDML_SEQ_NO") == F.col("t_lkup.t_CDML_SEQ_NO")) &
            (F.lit("AT") == F.col("t_lkup.t_CDOR_OR_ID"))
        ),
        how="left"
    ) \
    .join(
        df_cdor_a_t_for_a.alias("a_lkup"),
        on=(
            (F.col("cdor_x_extr.CLCL_ID") == F.col("a_lkup.a_CLCL_ID")) &
            (F.col("cdor_x_extr.CDML_SEQ_NO") == F.col("a_lkup.a_CDML_SEQ_NO")) &
            (F.lit("AA") == F.col("a_lkup.a_CDOR_OR_ID"))
        ),
        how="left"
    ) \
    .join(
        df_dsalw_excd_for_x.alias("DsalwExcdRespCdX"),
        on=(
            (F.col("cdor_x_extr.EXCD_ID") == F.col("DsalwExcdRespCdX.x_EXCD_ID")) &
            (F.substring(F.col("cdor_x_extr.CLCL_PAID_DT"),1,10) == F.col("DsalwExcdRespCdX.x_EFF_DT_SK")) &
            (F.substring(F.col("cdor_x_extr.CLCL_PAID_DT"),1,10) == F.col("DsalwExcdRespCdX.x_TERM_DT_SK"))
        ),
        how="left"
    )

# Now apply constraint for dsalw_x:
cond_dsalw_x = (
    (F.col("t_lkup.t_CDML_SEQ_NO").isNull()) &
    (F.col("a_lkup.a_CDML_SEQ_NO").isNull()) &
    (trim(F.col("cdor_x_extr.EXCD_ID")) != F.lit("319"))
)
df_dsalw_x = df_trans2_join_1.filter(cond_dsalw_x).select(
    F.rpad(F.col("cdor_x_extr.CLCL_ID"),12," ").alias("CLCL_ID"),
    F.col("cdor_x_extr.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.rpad(F.col("cdor_x_extr.CDOR_OR_ID"),2," ").alias("DSALW_TYP"),
    F.col("cdor_x_extr.CDOR_OR_AMT").alias("DSALW_AMT"),
    F.rpad(F.col("cdor_x_extr.EXCD_ID"),3," ").alias("DSALW_EXCD"),
    F.when(F.col("DsalwExcdRespCdX.x_EXCD_ID").isNull(), F.lit("NA"))
     .otherwise(F.col("DsalwExcdRespCdX.x_SRC_CD")).alias("EXCD_RESP_CD"),
    F.col("cdor_x_extr.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.when(F.col("DsalwExcdRespCdX.x_SRC_CD").isNull(), F.lit("N"))
     .otherwise(F.col("DsalwExcdRespCdX.x_BYPS_IN")).alias("BYPS_IN")
)

# --------------------------------------------------------------------------------
# hash_x_tmp (CHashedFileStage) - scenario C
# Because there's no subsequent stage reading from it in the JSON (no output pins).
# We'll write it to a parquet file: hf_clm_ln_dsalw_x_tmp.parquet
# --------------------------------------------------------------------------------
write_files(
    df_dsalw_x,
    "hf_clm_ln_dsalw_x_tmp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# trns_a_t (CTransformerStage)
# Primary link: from cdor_a_t => "a_t_extr_t"
#   Actually we removed the hashed file cdor_a_t, so we use df_cdor_a_t_dedup as the direct input
# Lookup link: from cdor_u => "au_lkup" => df_cdor_u_dedup
#   join on (a_t_extr_t.CLCL_ID = au_lkup.CLCL_ID, a_t_extr_t.CDML_SEQ_NO = au_lkup.CDML_SEQ_NO, 'AU' or 'DU' = au_lkup.CDOR_OR_ID)
# Lookup link: from its_clm_lkup => "its_lkup" => df_its_clm_lkup_dedup
#   join on (a_t_extr_t.CLCL_ID = its_lkup.CLCL_ID)
#
# Stage Variables:
#   svAUamt = If IsNull(au_lkup.CDML_SEQ_NO) then 0 else au_lkup.CDOR_OR_AMT
#   svATAUamt = a_t_extr_t.CDML_CONSIDER_CHG - (a_t_extr_t.CDOR_OR_AMT * svAUamt)
#   svATAUamtwoconsider = (a_t_extr_t.CDOR_OR_AMT * svAUamt)
#   svAAamt = a_t_extr_t.CDML_CONSIDER_CHG - a_t_extr_t.CDOR_OR_AMT
#
# Output pins:
#   1) disalw_a_dup => constraint: 
#      (CDOR_OR_ID in ('AA','DA')) and 
#      IsNull(trim(EXCD_ID))=false and 
#      svAAamt > 0 and 
#      trim(EXCD_ID) <> '319'
#   2) dsalw_tu => constraint:
#      (CDOR_OR_ID in ('AT','DT')) and
#      IsNull(trim(EXCD_ID))=false and
#      CDML_DISALL_AMT <> 0 and
#      ((IsNull(its_lkup.CLCL_ID)=true) or (IsNull(its_lkup.CLCL_ID)=false and svATAUamtwoconsider<>0)) and
#      svATAUamt > 0 and
#      trim(EXCD_ID) <> '319'
# --------------------------------------------------------------------------------

# Rename columns from df_cdor_a_t_dedup => "a_t_extr_t" alias
df_a_t_extr_t = df_cdor_a_t_dedup.alias("a_t_extr_t")

df_cdor_u_ren = df_cdor_u_dedup.withColumnRenamed("CLCL_ID","au_CLCL_ID") \
                                .withColumnRenamed("CDML_SEQ_NO","au_CDML_SEQ_NO") \
                                .withColumnRenamed("CDOR_OR_ID","au_CDOR_OR_ID") \
                                .withColumnRenamed("CDOR_OR_AMT","au_CDOR_OR_AMT") \
                                .withColumnRenamed("EXCD_ID","au_EXCD_ID") \
                                .withColumnRenamed("CLCL_PAID_DT","au_CLCL_PAID_DT") \
                                .withColumnRenamed("CDML_CONSIDER_CHG","au_CDML_CONSIDER_CHG") \
                                .withColumnRenamed("CDML_DISALL_AMT","au_CDML_DISALL_AMT") \
                                .withColumnRenamed("CDML_DISALL_EXCD","au_CDML_DISALL_EXCD") \
                                .withColumnRenamed("SRC_CD","au_SRC_CD") \
                                .withColumnRenamed("BYPS_IN","au_BYPS_IN")

df_its_clm_lkup_ren = df_its_clm_lkup_dedup.withColumnRenamed("CLCL_ID","its_CLCL_ID")

df_trns_a_t_join = df_a_t_extr_t \
    .join(
        df_cdor_u_ren.alias("au_lkup"),
        on=(
            (F.col("a_t_extr_t.CLCL_ID") == F.col("au_lkup.au_CLCL_ID")) &
            (F.col("a_t_extr_t.CDML_SEQ_NO") == F.col("au_lkup.au_CDML_SEQ_NO")) &
            ((F.lit("AU") == F.col("au_lkup.au_CDOR_OR_ID")) | (F.lit("DU") == F.col("au_lkup.au_CDOR_OR_ID")))
        ),
        how="left"
    ) \
    .join(
        df_its_clm_lkup_ren.alias("its_lkup"),
        on=(
            (F.col("a_t_extr_t.CLCL_ID") == F.col("its_lkup.its_CLCL_ID"))
        ),
        how="left"
    )

# Stage variables implementation
# svAUamt
df_trns_a_t_vars = df_trns_a_t_join.withColumn(
    "svAUamt",
    F.when(F.col("au_lkup.au_CDML_SEQ_NO").isNull(), F.lit(0)).otherwise(F.col("au_lkup.au_CDOR_OR_AMT"))
).withColumn(
    "svATAUamtwoconsider",
    F.col("a_t_extr_t.CDOR_OR_AMT") * F.col("svAUamt")
).withColumn(
    "svATAUamt",
    F.col("a_t_extr_t.CDML_CONSIDER_CHG") - F.col("svATAUamtwoconsider")
).withColumn(
    "svAAamt",
    F.col("a_t_extr_t.CDML_CONSIDER_CHG") - F.col("a_t_extr_t.CDOR_OR_AMT")
)

# 1) disalw_a_dup
cond_disalw_a_dup = (
    ((F.col("a_t_extr_t.CDOR_OR_ID").isin(["AA","DA"]))) &
    (trim(F.col("a_t_extr_t.EXCD_ID")).isNotNull()) &
    (trim(F.col("a_t_extr_t.EXCD_ID")) != F.lit("319")) &
    (F.col("svAAamt") > 0)
)
df_disalw_a_dup = df_trns_a_t_vars.filter(cond_disalw_a_dup).select(
    F.rpad(F.col("a_t_extr_t.CLCL_ID"),12," ").alias("CLCL_ID"),
    F.col("a_t_extr_t.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.rpad(F.col("a_t_extr_t.CDOR_OR_ID"),2," ").alias("DSALW_TYP"),
    F.col("svAAamt").alias("DSALW_AMT"),
    F.rpad(F.col("a_t_extr_t.EXCD_ID"),3," ").alias("DSALW_EXCD"),
    F.col("a_t_extr_t.SRC_CD").alias("EXCD_RESP_CD"),
    F.col("a_t_extr_t.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.rpad(F.col("a_t_extr_t.BYPS_IN"),1," ").alias("BYPS_IN")
)

# 2) dsalw_tu
cond_dsalw_tu = (
    ((F.col("a_t_extr_t.CDOR_OR_ID").isin(["AT","DT"]))) &
    (trim(F.col("a_t_extr_t.EXCD_ID")).isNotNull()) &
    (trim(F.col("a_t_extr_t.EXCD_ID")) != F.lit("319")) &
    (F.col("a_t_extr_t.CDML_DISALL_AMT") != 0) &
    (
       (F.col("its_lkup.its_CLCL_ID").isNull()) |
       (
         (F.col("its_lkup.its_CLCL_ID").isNotNull()) &
         (F.col("svATAUamtwoconsider") != 0)
       )
    ) &
    (F.col("svATAUamt") > 0)
)
df_dsalw_tu = df_trns_a_t_vars.filter(cond_dsalw_tu).select(
    F.rpad(F.col("a_t_extr_t.CLCL_ID"),12," ").alias("CLCL_ID"),
    F.col("a_t_extr_t.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.rpad(
        F.when(F.col("a_t_extr_t.CDOR_OR_ID") == F.lit("DT"), F.lit("DTDU"))
         .otherwise(F.lit("ATAU")),
        4," "
    ).alias("DSALW_TYP"),
    F.col("svATAUamt").alias("DSALW_AMT"),
    F.rpad(F.col("a_t_extr_t.EXCD_ID"),3," ").alias("DSALW_EXCD"),
    F.col("a_t_extr_t.SRC_CD").alias("EXCD_RESP_CD"),
    F.col("a_t_extr_t.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.rpad(F.col("a_t_extr_t.BYPS_IN"),1," ").alias("BYPS_IN")
)

# --------------------------------------------------------------------------------
# hash_tu_tmp (CHashedFileStage) - scenario A
# trns_a_t -> hash_tu_tmp -> atau_check (a_dup_lkup)
# We remove physical hashed file, deduplicate on [CLCL_ID, CDML_SEQ_NO, DSALW_TYP].
# --------------------------------------------------------------------------------
df_tu_dedup = dedup_sort(
    df_dsalw_tu,
    partition_cols=["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# aa_processing (CHashedFileStage) - scenario A
# trns_a_t -> aa_processing -> atau_check (aa_check)
# Deduplicate on [CLCL_ID, CDML_SEQ_NO, DSALW_TYP].
# --------------------------------------------------------------------------------
df_a_dup_dedup = dedup_sort(
    df_disalw_a_dup,
    partition_cols=["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# atau_check (CTransformerStage)
# Primary link: aa_check => df_a_dup_dedup
# Lookup link: a_dup_lkup => df_tu_dedup, join on
#   aa_check.CLCL_ID = a_dup_lkup.CLCL_ID
#   aa_check.CDML_SEQ_NO = a_dup_lkup.CDML_SEQ_NO
#   'ATAU' = a_dup_lkup.DSALW_TYP
#
# Output pin: disalw_a => constraint IsNull(a_dup_lkup.CLCL_ID) = true
# --------------------------------------------------------------------------------
df_aa_check = df_a_dup_dedup.alias("aa_check")
df_a_dup_lkup = df_tu_dedup.withColumnRenamed("CLCL_ID","lk_CLCL_ID") \
                           .withColumnRenamed("CDML_SEQ_NO","lk_CDML_SEQ_NO") \
                           .withColumnRenamed("DSALW_TYP","lk_DSALW_TYP") \
                           .withColumnRenamed("DSALW_AMT","lk_DSALW_AMT") \
                           .withColumnRenamed("DSALW_EXCD","lk_DSALW_EXCD") \
                           .withColumnRenamed("EXCD_RESP_CD","lk_EXCD_RESP_CD") \
                           .withColumnRenamed("CDML_DISALL_AMT","lk_CDML_DISALL_AMT") \
                           .withColumnRenamed("BYPS_IN","lk_BYPS_IN")

df_atau_check_join = df_aa_check.join(
    df_a_dup_lkup.alias("a_dup_lkup"),
    on=(
       (F.col("aa_check.CLCL_ID") == F.col("a_dup_lkup.lk_CLCL_ID")) &
       (F.col("aa_check.CDML_SEQ_NO") == F.col("a_dup_lkup.lk_CDML_SEQ_NO")) &
       (F.lit("ATAU") == F.col("a_dup_lkup.lk_DSALW_TYP"))
    ),
    how="left"
)

cond_disalw_a = F.col("a_dup_lkup.lk_CLCL_ID").isNull()
df_disalw_a = df_atau_check_join.filter(cond_disalw_a).select(
    F.rpad(F.col("aa_check.CLCL_ID"),12," ").alias("CLCL_ID"),
    F.col("aa_check.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.rpad(F.col("aa_check.DSALW_TYP"),2," ").alias("DSALW_TYP"),
    F.col("aa_check.DSALW_AMT").alias("DSALW_AMT"),
    F.rpad(F.col("aa_check.DSALW_EXCD"),3," ").alias("DSALW_EXCD"),
    F.col("aa_check.EXCD_RESP_CD").alias("EXCD_RESP_CD"),
    F.col("aa_check.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.rpad(F.col("aa_check.BYPS_IN"),1," ").alias("BYPS_IN")
)

# --------------------------------------------------------------------------------
# hash_a_tmp (CHashedFileStage) - scenario C
# atau_check -> hash_a_tmp
# No subsequent stage. We write to parquet: hf_clm_ln_dsalw_a_tmp.parquet
# --------------------------------------------------------------------------------
write_files(
    df_disalw_a,
    "hf_clm_ln_dsalw_a_tmp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)