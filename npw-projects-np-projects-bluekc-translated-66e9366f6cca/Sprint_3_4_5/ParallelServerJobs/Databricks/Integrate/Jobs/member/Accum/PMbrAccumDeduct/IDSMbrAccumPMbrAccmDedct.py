# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2014 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     *
# MAGIC 
# MAGIC Processing:
# MAGIC                     Main Extract reads from the W_ACCUM_DEDCT_MBR_PROD ( created in job IdsWAccumDedctMbrProdExtr)  
# MAGIC                     It joins the member information in the W_ACCUM_DEDCT_MBR_PROD to the P_EOB_DEDCT_XREF, EOB_ACCUM, and DEDCT_CMPNT tables.  
# MAGIC                     Once it has (or if it has)  a join to all of the three tables, then it will know what to use to join to the MBR_ACCUM or the FMLY_ACCUM table.  
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                        \(9)\(9)      Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                   \(9)\(9)      Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -----------------------------------------------------------------------------------------------------------------   \(9)\(9)      -------------------------  -------------------
# MAGIC SAndrew             8/10/2006                        Original development
# MAGIC Ralph Tucker      2008-12-16   3648            Added fields (Carovr_amt, Cob_oop_amt)                                                            
# MAGIC Bhoomi Dasari     2/22/2013    TTR-1454   Changes "Prefetch rows" to 10000 from 50 in DB2 stages                        \(9)\(9)     Kalyan Neelam   2013-03-05
# MAGIC                                                                      and also removing all data elements.
# MAGIC Hugh Sisson        2014-03-26   TFS8263     Hard-coding values for P041 and P040 until permenant fix can be made   
# MAGIC 
# MAGIC Karthik Chintalapani   2016-11-11       5634      Added new columns PLN_YR_EFF_DT              \(9)\(9)      IntegrateDev2         Kalyan Neelam    2016-11-28
# MAGIC                                                                           and  PLN_YR_END_DT
# MAGIC Jaideep Mankala       2016-12-22       5634      Added new lkup fmly_no_accum_lkup to Tx append_mbr_accum       IntegrateDev2       Kalyan Neelam     2016-12-23     
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)to get Plan extract eff and term dates
# MAGIC Jaideep Mankala       2016-12-27               5634      Added new db stage ids_mbr_accum_dates to extract plan eff and       IntegrateDev2    Kalyan Neelam   2016-12-27
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9) term dates
# MAGIC Kalyan Neelam          2016-12-28        5634      Updated logic to load PLN_YR_EFF_DT and PLN_YR_END_DT        IntegrateDev2
# MAGIC                                                                           from CLS_PLN_DTL tables

# MAGIC ** Read from W_ACCUM_DEDCT_MBR_PROD, join to the P_EOB_DEDCT_XREF, EOB_ACCUM, and DEDCT_CMPNT table.
# MAGIC 
# MAGIC ** Needs every DEDCT_VARBL_CD and SEQ_NO record from the P_EOB_DEDCT_XREF for that memeber  that meets the above join criteria.  therefore, the DEDCT_VARBL_CD and SEQ_NO are part of the key fields.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, substring, concat
from pyspark.sql.functions import rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# All parameter widgets
IDSRunCycle = get_widget_value('IDSRunCycle','100')
AccumYear = get_widget_value('AccumYear','2008')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')

A011 = get_widget_value('A011','250')
A025 = get_widget_value('A025','1000')
A030 = get_widget_value('A030','2000')
A056 = get_widget_value('A056','3000')
A277 = get_widget_value('A277','6000')
A371 = get_widget_value('A371','1250')
A372 = get_widget_value('A372','2500')
A373 = get_widget_value('A373','750')

# Acquire JDBC configuration for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ---------------------------------------------------------------------------------------------
# STAGE: ids_mbr_accum (DB2Connector) -> produces two outputs: mbr_accum_data, mbr_tbl
# ---------------------------------------------------------------------------------------------
query_mbr_accum_data = f"""
SELECT
  MBR_ACCUM.MBR_SK,
  MBR_ACCUM.PROD_ACCUM_ID,
  MBR_ACCUM.ACCUM_NO,
  MBR_ACCUM.YR_NO,
  MBR_ACCUM.MBR_UNIQ_KEY,
  MAP2.TRGT_CD,
  MBR_ACCUM.ACCUM_AMT,
  MBR_ACCUM.CAROVR_AMT,
  MBR_ACCUM.COB_OOP_AMT,
  MBR_ACCUM.PLN_YR_EFF_DT,
  MBR_ACCUM.PLN_YR_END_DT
FROM {IDSOwner}.MBR_ACCUM MBR_ACCUM,
     {IDSOwner}.CD_MPPNG  MAP2
WHERE MBR_ACCUM.YR_NO = {AccumYear}
  AND MBR_ACCUM.MBR_ACCUM_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD = 'DEDUCT'
"""

df_mbr_accum_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_mbr_accum_data)
    .load()
)

# Rename TRGT_CD to MBR_ACCUM_TYP_CD and select columns in order
df_mbr_accum_data = df_mbr_accum_data.withColumnRenamed("TRGT_CD", "MBR_ACCUM_TYP_CD").select(
    "MBR_SK",
    "PROD_ACCUM_ID",
    "ACCUM_NO",
    "YR_NO",
    "MBR_UNIQ_KEY",
    "MBR_ACCUM_TYP_CD",
    "ACCUM_AMT",
    "CAROVR_AMT",
    "COB_OOP_AMT",
    "PLN_YR_EFF_DT",
    "PLN_YR_END_DT"
)

# ---------------------------------------------------------------------------------------------
query_mbr_tbl = f"""
SELECT
  MBR.MBR_SK,
  MBR.MBR_UNIQ_KEY,
  MAP1.TRGT_CD
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE MBR.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK
"""

df_mbr_tbl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_mbr_tbl)
    .load()
)

# Rename TRGT_CD to SRC_SYS_CD
df_mbr_tbl = df_mbr_tbl.withColumnRenamed("TRGT_CD", "SRC_SYS_CD").select(
    "MBR_SK",
    "MBR_UNIQ_KEY",
    "SRC_SYS_CD"
)

# ---------------------------------------------------------------------------------------------
# STAGE: hf_mbr_accum_hash (CHashedFileStage) -- scenario A: deduplicate intermediate
# ---------------------------------------------------------------------------------------------
# df_mbr_accum (from mbr_accum_data) deduplicated on keys = [MBR_SK, PROD_ACCUM_ID, ACCUM_NO, YR_NO]
df_mbr_accum = dedup_sort(
    df_mbr_accum_data,
    partition_cols=["MBR_SK", "PROD_ACCUM_ID", "ACCUM_NO", "YR_NO"],
    sort_cols=[]
)

# df_mbr_src_sys (from mbr_tbl) deduplicated on keys = [MBR_SK, MBR_UNIQ_KEY]
df_mbr_src_sys = dedup_sort(
    df_mbr_tbl,
    partition_cols=["MBR_SK", "MBR_UNIQ_KEY"],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------------
# STAGE: MbrsEOB_Accum_Dedct (DB2Connector) -> produces three outputs: med_IN_network, med_OUT_network, all_network
# ---------------------------------------------------------------------------------------------

query_med_in_network = f"""
SELECT DISTINCT 
  W_ACCUM_DEDCT.MBR_SK,
  W_ACCUM_DEDCT.SUB_SK,
  W_ACCUM_DEDCT.MBR_UNIQ_KEY,
  W_ACCUM_DEDCT.GRP_SK,
  P_EOB.DEDCT_VRBL_CD,
  P_EOB.SEQ_NO,
  W_ACCUM_DEDCT.DEDCT_YR,
  P_EOB.DEDCT_VRBL_DESC,
  P_EOB.DEDCT_LMT_NTWK_CD,
  P_EOB.MBR_PROD_ACCUM_ID,
  P_EOB.MBR_ACCUM_NO,
  MIN(DEDCT.MBR_DEDCT_AMT),
  MIN(DEDCT.MBR_DEDCT_CAROVR_AMT),
  W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
  CSPI.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM_DEDCT,
     {IDSOwner}.EOB_ACCUM              EOB,
     {IDSOwner}.P_EOB_DEDCT_XREF       P_EOB,
     {IDSOwner}.DEDCT_CMPNT            DEDCT,
     {IDSOwner}.CD_MPPNG               MAP1,
     {IDSOwner}.CD_MPPNG               MAP2,
     {IDSOwner}.MBR_ENR                ENR,
     {IDSOwner}.CLS_PLN_DTL            CSPI
WHERE W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL = EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.DEDCT_YR = '{AccumYear}'
  AND W_ACCUM_DEDCT.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
  AND ENR.GRP_SK = CSPI.GRP_SK
  AND ENR.CLS_SK = CSPI.CLS_SK
  AND ENR.CLS_PLN_SK = CSPI.CLS_PLN_SK
  AND ENR.EFF_DT_SK <= '{CurrentDate}' AND ENR.TERM_DT_SK >= '{CurrentDate}'
  AND ENR.ELIG_IN = 'Y'
  AND CSPI.EFF_DT_SK <= '{CurrentDate}' AND CSPI.TERM_DT_SK >= '{CurrentDate}'
  AND EOB.EOB_ACCUM_TYP_CD_SK = MAP1.CD_MPPNG_SK
  AND RTRIM(MAP1.TRGT_CD) = 'DEDUCTWCAROVR'
  AND EOB.EOB_ACCUM_PERD_CD_SK = MAP2.CD_MPPNG_SK
  AND RTRIM(MAP2.TRGT_CD) in ('INCURYR','CLNDRYR')
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV = P_EOB.DEDCT_VRBL_CD
  AND P_EOB.DEDCT_LMT_NTWK_CD = 'I'
  AND P_EOB.MBR_PROD_ACCUM_ID <> 'NA'
  AND P_EOB.MBR_ACCUM_NO <> 0
  AND P_EOB.EFF_DT = (
    SELECT MAX(P_EOB2.EFF_DT)
    FROM {IDSOwner}.P_EOB_DEDCT_XREF P_EOB2
    WHERE P_EOB2.DEDCT_VRBL_CD = P_EOB.DEDCT_VRBL_CD
      AND P_EOB2.SEQ_NO = P_EOB.SEQ_NO
      AND P_EOB2.DEDCT_LMT_NTWK_CD = 'I'
      AND P_EOB2.MBR_PROD_ACCUM_ID <> 'NA'
      AND P_EOB2.MBR_ACCUM_NO <> 0
  )
  AND P_EOB.MBR_ACCUM_NO = DEDCT.ACCUM_NO
  AND EOB.EOB_DEDCT_CMPNT_ID = DEDCT.DEDCT_CMPNT_ID
GROUP BY
  W_ACCUM_DEDCT.MBR_SK,
  W_ACCUM_DEDCT.SUB_SK,
  W_ACCUM_DEDCT.MBR_UNIQ_KEY,
  W_ACCUM_DEDCT.GRP_SK,
  P_EOB.DEDCT_VRBL_CD,
  P_EOB.SEQ_NO,
  W_ACCUM_DEDCT.DEDCT_YR,
  P_EOB.DEDCT_VRBL_DESC,
  P_EOB.DEDCT_LMT_NTWK_CD,
  P_EOB.MBR_PROD_ACCUM_ID,
  P_EOB.MBR_ACCUM_NO,
  W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
  CSPI.PLN_BEG_DT_MO_DAY
"""

df_med_in_network_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_med_in_network)
    .load()
)

df_med_in_network_data = df_med_in_network_data.withColumnRenamed("MIN(DEDCT.MBR_DEDCT_AMT)", "MBR_DEDCT_AMT")
df_med_in_network_data = df_med_in_network_data.withColumnRenamed("MIN(DEDCT.MBR_DEDCT_CAROVR_AMT)", "MBR_DEDCT_CAROVR_AMT")

# ---------------------------------------------------------------------------------------------
query_med_out_network = f"""
SELECT DISTINCT
  W_ACCUM_DEDCT.MBR_SK,
  W_ACCUM_DEDCT.SUB_SK,
  W_ACCUM_DEDCT.MBR_UNIQ_KEY,
  W_ACCUM_DEDCT.GRP_SK,
  P_EOB.DEDCT_VRBL_CD,
  P_EOB.SEQ_NO,
  W_ACCUM_DEDCT.DEDCT_YR,
  P_EOB.DEDCT_VRBL_DESC,
  P_EOB.DEDCT_LMT_NTWK_CD,
  P_EOB.MBR_PROD_ACCUM_ID,
  P_EOB.MBR_ACCUM_NO,
  MAX(DEDCT.MBR_DEDCT_AMT),
  MAX(DEDCT.MBR_DEDCT_CAROVR_AMT),
  W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
  CSPI.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM_DEDCT,
     {IDSOwner}.EOB_ACCUM              EOB,
     {IDSOwner}.P_EOB_DEDCT_XREF       P_EOB,
     {IDSOwner}.DEDCT_CMPNT            DEDCT,
     {IDSOwner}.CD_MPPNG               MAP1,
     {IDSOwner}.CD_MPPNG               MAP2,
     {IDSOwner}.MBR_ENR                ENR,
     {IDSOwner}.CLS_PLN_DTL            CSPI
WHERE W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL = EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.DEDCT_YR = '{AccumYear}'
  AND W_ACCUM_DEDCT.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
  AND ENR.GRP_SK = CSPI.GRP_SK
  AND ENR.CLS_SK = CSPI.CLS_SK
  AND ENR.CLS_PLN_SK = CSPI.CLS_PLN_SK
  AND ENR.EFF_DT_SK <= '{CurrentDate}' AND ENR.TERM_DT_SK >= '{CurrentDate}'
  AND ENR.ELIG_IN = 'Y'
  AND CSPI.EFF_DT_SK <= '{CurrentDate}' AND CSPI.TERM_DT_SK >= '{CurrentDate}'
  AND EOB.EOB_ACCUM_TYP_CD_SK = MAP1.CD_MPPNG_SK
  AND RTRIM(MAP1.TRGT_CD) = 'DEDUCTWCAROVR'
  AND EOB.EOB_ACCUM_PERD_CD_SK = MAP2.CD_MPPNG_SK
  AND RTRIM(MAP2.TRGT_CD) in ('INCURYR','CLNDRYR')
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV = P_EOB.DEDCT_VRBL_CD
  AND P_EOB.DEDCT_LMT_NTWK_CD = 'O'
  AND P_EOB.MBR_PROD_ACCUM_ID <> 'NA'
  AND P_EOB.MBR_ACCUM_NO <> 0
  AND P_EOB.EFF_DT = (
    SELECT MAX(P_EOB2.EFF_DT)
    FROM {IDSOwner}.P_EOB_DEDCT_XREF P_EOB2
    WHERE P_EOB2.DEDCT_VRBL_CD = P_EOB.DEDCT_VRBL_CD
      AND P_EOB2.SEQ_NO = P_EOB.SEQ_NO
      AND P_EOB2.DEDCT_LMT_NTWK_CD = 'O'
      AND P_EOB2.MBR_PROD_ACCUM_ID <> 'NA'
      AND P_EOB2.MBR_ACCUM_NO <> 0
  )
  AND P_EOB.MBR_ACCUM_NO = DEDCT.ACCUM_NO
  AND EOB.EOB_DEDCT_CMPNT_ID = DEDCT.DEDCT_CMPNT_ID
GROUP BY
  W_ACCUM_DEDCT.MBR_SK,
  W_ACCUM_DEDCT.SUB_SK,
  W_ACCUM_DEDCT.MBR_UNIQ_KEY,
  W_ACCUM_DEDCT.GRP_SK,
  P_EOB.DEDCT_VRBL_CD,
  P_EOB.SEQ_NO,
  W_ACCUM_DEDCT.DEDCT_YR,
  P_EOB.DEDCT_VRBL_DESC,
  P_EOB.DEDCT_LMT_NTWK_CD,
  P_EOB.MBR_PROD_ACCUM_ID,
  P_EOB.MBR_ACCUM_NO,
  W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
  CSPI.PLN_BEG_DT_MO_DAY
"""

df_med_out_network_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_med_out_network)
    .load()
)

df_med_out_network_data = df_med_out_network_data.withColumnRenamed("MAX(DEDCT.MBR_DEDCT_AMT)", "MBR_DEDCT_AMT")
df_med_out_network_data = df_med_out_network_data.withColumnRenamed("MAX(DEDCT.MBR_DEDCT_CAROVR_AMT)", "MBR_DEDCT_CAROVR_AMT")

# ---------------------------------------------------------------------------------------------
query_all_network = f"""
SELECT DISTINCT
  W_ACCUM_DEDCT.MBR_SK,
  W_ACCUM_DEDCT.SUB_SK,
  W_ACCUM_DEDCT.MBR_UNIQ_KEY,
  W_ACCUM_DEDCT.GRP_SK,
  P_EOB.DEDCT_VRBL_CD,
  P_EOB.SEQ_NO,
  W_ACCUM_DEDCT.DEDCT_YR,
  P_EOB.DEDCT_VRBL_DESC,
  P_EOB.DEDCT_LMT_NTWK_CD,
  P_EOB.MBR_PROD_ACCUM_ID,
  P_EOB.MBR_ACCUM_NO,
  DEDCT.MBR_DEDCT_AMT,
  DEDCT.MBR_DEDCT_CAROVR_AMT,
  W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
  CSPI.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD  W_ACCUM_DEDCT,
     {IDSOwner}.EOB_ACCUM              EOB,
     {IDSOwner}.P_EOB_DEDCT_XREF       P_EOB,
     {IDSOwner}.DEDCT_CMPNT            DEDCT,
     {IDSOwner}.CD_MPPNG               MAP1,
     {IDSOwner}.CD_MPPNG               MAP2,
     {IDSOwner}.MBR_ENR                ENR,
     {IDSOwner}.CLS_PLN_DTL            CSPI
WHERE W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL = EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.DEDCT_YR = '{AccumYear}'
  AND W_ACCUM_DEDCT.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
  AND ENR.GRP_SK = CSPI.GRP_SK
  AND ENR.CLS_SK = CSPI.CLS_SK
  AND ENR.CLS_PLN_SK = CSPI.CLS_PLN_SK
  AND ENR.EFF_DT_SK <= '{CurrentDate}' AND ENR.TERM_DT_SK >= '{CurrentDate}'
  AND ENR.ELIG_IN = 'Y'
  AND CSPI.EFF_DT_SK <= '{CurrentDate}' AND CSPI.TERM_DT_SK >= '{CurrentDate}'
  AND EOB.EOB_ACCUM_TYP_CD_SK = MAP1.CD_MPPNG_SK
  AND RTRIM(MAP1.TRGT_CD) = 'DEDUCTWCAROVR'
  AND EOB.EOB_ACCUM_PERD_CD_SK = MAP2.CD_MPPNG_SK
  AND RTRIM(MAP2.TRGT_CD) in ('INCURYR','CLNDRYR')
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV = P_EOB.DEDCT_VRBL_CD
  AND P_EOB.DEDCT_LMT_NTWK_CD = 'A'
  AND P_EOB.MBR_PROD_ACCUM_ID <> 'NA'
  AND P_EOB.MBR_ACCUM_NO <> 0
  AND P_EOB.EFF_DT = (
    SELECT MAX(P_EOB2.EFF_DT)
    FROM {IDSOwner}.P_EOB_DEDCT_XREF P_EOB2
    WHERE P_EOB2.DEDCT_VRBL_CD = P_EOB.DEDCT_VRBL_CD
      AND P_EOB2.SEQ_NO = P_EOB.SEQ_NO
      AND P_EOB2.DEDCT_LMT_NTWK_CD = 'A'
      AND P_EOB2.MBR_PROD_ACCUM_ID = P_EOB.MBR_PROD_ACCUM_ID
      AND P_EOB2.MBR_ACCUM_NO = P_EOB.MBR_ACCUM_NO
  )
  AND P_EOB.MBR_ACCUM_NO = DEDCT.ACCUM_NO
  AND EOB.EOB_DEDCT_CMPNT_ID = DEDCT.DEDCT_CMPNT_ID
"""

df_all_network = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_all_network)
    .load()
)

# ---------------------------------------------------------------------------------------------
# STAGE: hf_mbr_prcs_accum_mbr_eob_in_ntwrk (CHashedFileStage) - scenario A
# Deduplicate on key columns: [MBR_SK, SUB_SK, MBR_UNIQ_KEY, GRP_SK, DEDCT_VRBL_CD, SEQ_NO, DEDCT_YR]
# Output => in_network
# ---------------------------------------------------------------------------------------------
df_in_network = df_med_in_network_data.select(
    "MBR_SK",
    "SUB_SK",
    "MBR_UNIQ_KEY",
    "GRP_SK",
    "DEDCT_VRBL_CD",
    "SEQ_NO",
    "DEDCT_YR",
    "DEDCT_VRBL_DESC",
    "DEDCT_LMT_NTWK_CD",
    "MBR_PROD_ACCUM_ID",
    "MBR_ACCUM_NO",
    "MBR_DEDCT_AMT",
    "MBR_DEDCT_CAROVR_AMT",
    "PROD_CMPNT_PFX_ID_EBCL",
    "PLN_BEG_DT_MO_DAY"
)

df_in_network = dedup_sort(
    df_in_network,
    partition_cols=["MBR_SK", "SUB_SK", "MBR_UNIQ_KEY", "GRP_SK", "DEDCT_VRBL_CD", "SEQ_NO", "DEDCT_YR"],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------------
# STAGE: hf_mbr_prcs_accum_mbr_eob_out_ntwrk (CHashedFileStage) - scenario A
# Deduplicate on key columns
# Output => out_of_network
# ---------------------------------------------------------------------------------------------
df_out_network = df_med_out_network_data.select(
    "MBR_SK",
    "SUB_SK",
    "MBR_UNIQ_KEY",
    "GRP_SK",
    "DEDCT_VRBL_CD",
    "SEQ_NO",
    "DEDCT_YR",
    "DEDCT_VRBL_DESC",
    "DEDCT_LMT_NTWK_CD",
    "MBR_PROD_ACCUM_ID",
    "MBR_ACCUM_NO",
    "MBR_DEDCT_AMT",
    "MBR_DEDCT_CAROVR_AMT",
    "PROD_CMPNT_PFX_ID_EBCL",
    "PLN_BEG_DT_MO_DAY"
)

df_out_network = dedup_sort(
    df_out_network,
    partition_cols=["MBR_SK", "SUB_SK", "MBR_UNIQ_KEY", "GRP_SK", "DEDCT_VRBL_CD", "SEQ_NO", "DEDCT_YR"],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------------
# STAGE: hf_mbr_prcs_accum_mbr_eob_all (CHashedFileStage) - scenario A
# Deduplicate on key columns
# Output => network_all
# ---------------------------------------------------------------------------------------------
df_network_all = df_all_network.select(
    "MBR_SK",
    "SUB_SK",
    "MBR_UNIQ_KEY",
    "GRP_SK",
    "DEDCT_VRBL_CD",
    "SEQ_NO",
    "DEDCT_YR",
    "DEDCT_VRBL_DESC",
    "DEDCT_LMT_NTWK_CD",
    "MBR_PROD_ACCUM_ID",
    "MBR_ACCUM_NO",
    "MBR_DEDCT_AMT",
    "MBR_DEDCT_CAROVR_AMT",
    "PROD_CMPNT_PFX_ID_EBCL",
    "PLN_BEG_DT_MO_DAY"
)

df_network_all = dedup_sort(
    df_network_all,
    partition_cols=["MBR_SK", "SUB_SK", "MBR_UNIQ_KEY", "GRP_SK", "DEDCT_VRBL_CD", "SEQ_NO", "DEDCT_YR"],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------------
# STAGE: Link_Collector_395 (CCollector)
# Round-Robin collector in DataStage can be approximated by union of data frames
# Input: out_of_network, in_network, network_all
# Output: mbr_eobs
# ---------------------------------------------------------------------------------------------
df_mbr_eobs = df_in_network.union(df_out_network).union(df_network_all)

# ---------------------------------------------------------------------------------------------
# STAGE: hf_mbr_prcs_accum_mbr_eob (CHashedFileStage) - scenario A
# Key columns: [MBR_SK, SUB_SK, MBR_UNIQ_KEY, GRP_SK, DEDCT_VRBL_CD, SEQ_NO, DEDCT_YR]
# Output => MBR_EOB
# ---------------------------------------------------------------------------------------------
df_mbr_eobs = dedup_sort(
    df_mbr_eobs.select(
        "MBR_SK",
        "SUB_SK",
        "MBR_UNIQ_KEY",
        "GRP_SK",
        "DEDCT_VRBL_CD",
        "SEQ_NO",
        "DEDCT_YR",
        "DEDCT_VRBL_DESC",
        "DEDCT_LMT_NTWK_CD",
        "MBR_PROD_ACCUM_ID",
        "MBR_ACCUM_NO",
        "MBR_DEDCT_AMT",
        "MBR_DEDCT_CAROVR_AMT",
        "PROD_CMPNT_PFX_ID_EBCL",
        "PLN_BEG_DT_MO_DAY",
    ),
    partition_cols=["MBR_SK", "SUB_SK", "MBR_UNIQ_KEY", "GRP_SK", "DEDCT_VRBL_CD", "SEQ_NO", "DEDCT_YR"],
    sort_cols=[]
)
df_MBR_EOB = df_mbr_eobs

# ---------------------------------------------------------------------------------------------
# STAGE: append_mbr_accum (CTransformerStage)
# Primary link: MBR_EOB, left lookups: mbr_accum, mbr_src_sys
# Stage Variables: svNextYear, svPlnYrEffDt, svPlnYrEndDt1, svPlnYrEndDt
# Output link: HardCodeFix
# ---------------------------------------------------------------------------------------------
df_enriched = df_MBR_EOB.alias("MBR_EOB").join(
    df_mbr_accum.alias("mbr_accum"),
    (
        (col("MBR_EOB.MBR_SK") == col("mbr_accum.MBR_SK"))
        & (col("MBR_EOB.MBR_PROD_ACCUM_ID") == col("mbr_accum.PROD_ACCUM_ID"))
        & (col("MBR_EOB.MBR_ACCUM_NO") == col("mbr_accum.ACCUM_NO"))
        & (col("MBR_EOB.DEDCT_YR") == col("mbr_accum.YR_NO"))
    ),
    "left"
).join(
    df_mbr_src_sys.alias("mbr_src_sys"),
    (
        (col("MBR_EOB.MBR_SK") == col("mbr_src_sys.MBR_SK"))
        & (col("MBR_EOB.MBR_UNIQ_KEY") == col("mbr_src_sys.MBR_UNIQ_KEY"))
    ),
    "left"
)

# Stage variable svNextYear => cast DEDCT_YR to int, plus 1, cast back to string
df_enriched = df_enriched.withColumn(
    "svNextYear",
    (col("MBR_EOB.DEDCT_YR").cast("int") + lit(1)).cast("string")
)

# svPlnYrEffDt
df_enriched = df_enriched.withColumn(
    "svPlnYrEffDt",
    when(
        length(col("MBR_EOB.PLN_BEG_DT_MO_DAY")) == 3,
        concat(
            col("MBR_EOB.DEDCT_YR"),
            lit("-0"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 1, 1),
            lit("-"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 2, 2)
        )
    ).otherwise(
        concat(
            col("MBR_EOB.DEDCT_YR"),
            lit("-"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 1, 2),
            lit("-"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 3, 2)
        )
    )
)

# svPlnYrEndDt1
df_enriched = df_enriched.withColumn(
    "svPlnYrEndDt1",
    when(
        length(col("MBR_EOB.PLN_BEG_DT_MO_DAY")) == 3,
        concat(
            col("svNextYear"),
            lit("-0"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 1, 1),
            lit("-"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 2, 2)
        )
    ).otherwise(
        concat(
            col("svNextYear"),
            lit("-"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 1, 2),
            lit("-"),
            substring(col("MBR_EOB.PLN_BEG_DT_MO_DAY"), 3, 2)
        )
    )
)

# svPlnYrEndDt => assume FIND.DATE(...) is a user-defined function
df_enriched = df_enriched.withColumn(
    "svPlnYrEndDt",
    FIND_DATE(col("svPlnYrEndDt1"), lit("-1"), lit("D"), lit("X"), lit("CCYY-MM-DD"))
)

# Now produce the columns for HardCodeFix link
df_HardCodeFix = df_enriched.select(
    when(col("mbr_src_sys.SRC_SYS_CD").isNull(), lit("UNK")).otherwise(col("mbr_src_sys.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    when(col("MBR_EOB.MBR_UNIQ_KEY").isNull(), lit(0)).otherwise(col("MBR_EOB.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    col("MBR_EOB.DEDCT_YR").alias("YR_NO"),
    col("MBR_EOB.DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    col("MBR_EOB.SEQ_NO").alias("DEDCT_VRBL_SEQ_NO"),
    col("MBR_EOB.DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    when(col("MBR_EOB.MBR_ACCUM_NO") == lit(53), lit("Y")).otherwise(lit("N")).alias("DRUG_ACCUM_IN"),
    when(col("mbr_accum.ACCUM_AMT").isNull(), lit(0)).otherwise(col("mbr_accum.ACCUM_AMT")).alias("ACCUM_AMT"),
    col("MBR_EOB.MBR_DEDCT_AMT").alias("MBR_DEDCT_AMT"),
    col("MBR_EOB.MBR_ACCUM_NO").alias("ACCUM_NO"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    trim(col("MBR_EOB.DEDCT_VRBL_DESC")).alias("DEDCT_VRBL_DESC"),
    col("MBR_EOB.MBR_PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    when(col("mbr_accum.CAROVR_AMT").isNull(), lit(0)).otherwise(col("mbr_accum.CAROVR_AMT")).alias("CAROVR_AMT"),
    when(col("mbr_accum.COB_OOP_AMT").isNull(), lit(0)).otherwise(col("mbr_accum.COB_OOP_AMT")).alias("COB_OOP_AMT"),
    when(col("MBR_EOB.MBR_DEDCT_CAROVR_AMT").isNull(), lit(0)).otherwise(col("MBR_EOB.MBR_DEDCT_CAROVR_AMT")).alias("MBR_DEDCT_CAROVR_AMT"),
    col("MBR_EOB.PROD_CMPNT_PFX_ID_EBCL").alias("PROD_CMPNT_PFX_ID_EBCL"),
    when(col("mbr_accum.PLN_YR_EFF_DT").isNull(), col("svPlnYrEffDt")).otherwise(col("mbr_accum.PLN_YR_EFF_DT")).alias("PLN_YR_EFF_DT"),
    when(col("mbr_accum.PLN_YR_END_DT").isNull(), col("svPlnYrEndDt")).otherwise(col("mbr_accum.PLN_YR_END_DT")).alias("PLN_YR_END_DT")
)

# ---------------------------------------------------------------------------------------------
# STAGE: HardCoreFix (CTransformerStage)
# Primary link: HardCodeFix
# Output link: LoadFile
# ---------------------------------------------------------------------------------------------
df_HardCoreFix = df_HardCodeFix

# MBR_DEDCT_AMT transformation with nested condition
df_LoadFile = df_HardCoreFix.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("YR_NO").alias("YR_NO"),
    col("DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    col("DEDCT_VRBL_SEQ_NO").alias("DEDCT_VRBL_SEQ_NO"),
    col("DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    col("DRUG_ACCUM_IN").alias("DRUG_ACCUM_IN"),
    col("ACCUM_AMT").alias("ACCUM_AMT"),
    when(
        (col("PROD_CMPNT_PFX_ID_EBCL") == lit("H571")) & (col("DEDCT_VRBL_SEQ_NO") == lit(0)),
        lit(A011)
    ).when(
        (col("PROD_CMPNT_PFX_ID_EBCL") == lit("H571")) & (col("DEDCT_VRBL_SEQ_NO") == lit(1)),
        lit(A056)
    ).when(
        col("PROD_CMPNT_PFX_ID_EBCL") == lit("H571"),
        lit(A277)
    ).when(
        (col("PROD_CMPNT_PFX_ID_EBCL") == lit("H572")) & (col("DEDCT_VRBL_SEQ_NO") == lit(0)),
        lit(A011)
    ).when(
        (col("PROD_CMPNT_PFX_ID_EBCL") == lit("H572")) & (col("DEDCT_VRBL_SEQ_NO") == lit(1)),
        lit(A025)
    ).when(
        col("PROD_CMPNT_PFX_ID_EBCL") == lit("H572"),
        lit(A030)
    ).when(
        (col("PROD_CMPNT_PFX_ID_EBCL") == lit("H542")) & (col("DEDCT_VRBL_SEQ_NO") == lit(0)),
        lit(A373)
    ).when(
        (col("PROD_CMPNT_PFX_ID_EBCL") == lit("H542")) & (col("DEDCT_VRBL_SEQ_NO") == lit(1)),
        lit(A371)
    ).when(
        col("PROD_CMPNT_PFX_ID_EBCL") == lit("H542"),
        lit(A372)
    ).otherwise(col("MBR_DEDCT_AMT")).alias("MBR_DEDCT_AMT"),
    col("ACCUM_NO").alias("ACCUM_NO"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("DEDCT_VRBL_DESC").alias("DEDCT_VRBL_DESC"),
    col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    col("CAROVR_AMT").alias("CAROVR_AMT"),
    col("COB_OOP_AMT").alias("COB_OOP_AMT"),
    col("MBR_DEDCT_CAROVR_AMT").alias("MBR_DEDCT_CAROVR_AMT"),
    col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

# Per instructions, for final output columns that are char or varchar, use rpad with known length or <...> if unknown
# The final file columns in order:
# 1  SRC_SYS_CD (varchar => length unknown => use <...>)
# 2  MBR_UNIQ_KEY (int => skip rpad)
# 3  YR_NO (int => skip)
# 4  DEDCT_VRBL_CD (varchar => length unknown => <...>)
# 5  DEDCT_VRBL_SEQ_NO (int => skip)
# 6  DEDCT_LMT_NTWK_CD (varchar => length unknown => <...>)
# 7  DRUG_ACCUM_IN (char(1))
# 8  ACCUM_AMT (decimal => skip)
# 9  MBR_DEDCT_AMT (decimal => skip)
# 10 ACCUM_NO (int => skip)
# 11 LAST_UPDT_RUN_CYC_NO (int => skip)
# 12 DEDCT_VRBL_DESC (varchar => length unknown => <...>)
# 13 PROD_ACCUM_ID (varchar => length unknown => <...>)
# 14 CAROVR_AMT (decimal => skip)
# 15 COB_OOP_AMT (decimal => skip)
# 16 MBR_DEDCT_CAROVR_AMT (decimal => skip)
# 17 PLN_YR_EFF_DT (char(10))
# 18 PLN_YR_END_DT (char(10))

df_final = df_LoadFile.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("YR_NO"),
    rpad(col("DEDCT_VRBL_CD"), <...>, " ").alias("DEDCT_VRBL_CD"),
    col("DEDCT_VRBL_SEQ_NO"),
    rpad(col("DEDCT_LMT_NTWK_CD"), <...>, " ").alias("DEDCT_LMT_NTWK_CD"),
    rpad(col("DRUG_ACCUM_IN"), 1, " ").alias("DRUG_ACCUM_IN"),
    col("ACCUM_AMT"),
    col("MBR_DEDCT_AMT"),
    col("ACCUM_NO"),
    col("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("DEDCT_VRBL_DESC"), <...>, " ").alias("DEDCT_VRBL_DESC"),
    rpad(col("PROD_ACCUM_ID"), <...>, " ").alias("PROD_ACCUM_ID"),
    col("CAROVR_AMT"),
    col("COB_OOP_AMT"),
    col("MBR_DEDCT_CAROVR_AMT"),
    rpad(col("PLN_YR_EFF_DT"), 10, " ").alias("PLN_YR_EFF_DT"),
    rpad(col("PLN_YR_END_DT"), 10, " ").alias("PLN_YR_END_DT")
)

# ---------------------------------------------------------------------------------------------
# STAGE: P_MBR_ACCUM_DEDCT (CSeqFileStage)
# We write the final df to the path f"{adls_path}/load/P_MBR_ACCUM_DEDCT.dat.{AccumYear}"
# ---------------------------------------------------------------------------------------------
output_file_path = f"{adls_path}/load/P_MBR_ACCUM_DEDCT.dat.{AccumYear}"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)