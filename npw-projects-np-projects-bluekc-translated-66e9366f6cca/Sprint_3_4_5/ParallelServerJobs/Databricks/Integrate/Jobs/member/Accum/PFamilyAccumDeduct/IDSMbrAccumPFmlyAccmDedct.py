# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2014 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsMbrAccumExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Main Extract reads from the W_ACCUM_DEDCT_MBR_PROD ( created in job IdsWAccumDedctMbrProdExtr)  
# MAGIC                     It joins the member information in the W_ACCUM_DEDCT_MBR_PROD to the P_EOB_DEDCT_XREF, EOB_ACCUM, and DEDCT_CMPNT tables.  
# MAGIC                     Once it has (or if it has)  a join to all of the three tables, then it will know what to use to join to the MBR_ACCUM or the FMLY_ACCUM table.  
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                                                             Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                                                                        Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -----------------------------------------------------------------------------------------------------------------                                                         -------------------------  -------------------
# MAGIC SAndrew             8/10/2006                       Original development
# MAGIC Ralph Tucker      2008-12-17   3648           Added two fields (Carovr_amt                        )    devlIDSnew         
# MAGIC Bhoomi Dasari     2/22/2013    TTR-1454  Changes "Prefetch rows" to 10000 from 50 in DB2 stages                                                                         Kalyan Neelam   2013-03-05
# MAGIC                                                                     and also removing all data elements.  
# MAGIC Hugh Sisson        2014-03-26   TFS8263     Hard-coding values for P041 and P040 until permenant fix can be made   
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2                 Kalyan Neelam   2016-11-28
# MAGIC Jaideep Mankala            2016-12-22               5634      Added new lkup fmly_no_accum_lkup to Tx append_mbr_accum       IntegrateDev2             Kalyan Neelam   2016-12-23    
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)to get Plan extract eff and term dates
# MAGIC Jaideep Mankala            2016-12-27               5634      Added new db stage ids_fmly_accum_dates to extract plan eff and       IntegrateDev2           Kalyan Neelam   2016-12-27  
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
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
AccumYear = get_widget_value('AccumYear','2014')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
A011 = get_widget_value('A011','500')
A025 = get_widget_value('A025','2000')
A030 = get_widget_value('A030','4000')
A056 = get_widget_value('A056','6000')
A277 = get_widget_value('A277','12000')
A371 = get_widget_value('A371','2500')
A372 = get_widget_value('A372','5000')
A373 = get_widget_value('A373','1500')
CurrentDate = get_widget_value('CurrentDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_fmly_accum_tbl = f"""
SELECT 
FMLY_ACCUM.SUB_SK,
FMLY_ACCUM.PROD_ACCUM_ID,
FMLY_ACCUM.ACCUM_NO,
FMLY_ACCUM.YR_NO,
FMLY_ACCUM.SUB_UNIQ_KEY,
MAP2.TRGT_CD,
FMLY_ACCUM.ACCUM_AMT,
FMLY_ACCUM.CAROVR_AMT,
FMLY_ACCUM.PLN_YR_EFF_DT,
FMLY_ACCUM.PLN_YR_END_DT
FROM {IDSOwner}.FMLY_ACCUM FMLY_ACCUM,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2
WHERE FMLY_ACCUM.YR_NO = {AccumYear}
  AND FMLY_ACCUM.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK
  AND FMLY_ACCUM.FMLY_ACCUM_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD = 'DEDUCT'
"""

df_fmly_accum_tbl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_fmly_accum_tbl)
    .load()
)

extract_query_sub_tbl = f"""
SELECT
SUB.SUB_SK,
SUB.SUB_UNIQ_KEY,
MAP1.TRGT_CD
FROM {IDSOwner}.SUB SUB,
     {IDSOwner}.CD_MPPNG MAP1
WHERE SUB.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK
"""

df_sub_tbl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_sub_tbl)
    .load()
)

df_fmly_accum_tbl = df_fmly_accum_tbl.select(
    F.col("SUB_SK").cast(IntegerType()).alias("SUB_SK"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("ACCUM_NO").cast(IntegerType()).alias("ACCUM_NO"),
    F.col("YR_NO").cast(IntegerType()).alias("YR_NO"),
    F.col("SUB_UNIQ_KEY").cast(IntegerType()).alias("SUB_UNIQ_KEY"),
    F.col("TRGT_CD").alias("FMLY_ACCUM_TYP_CD"),
    F.col("ACCUM_AMT").cast(DecimalType(38,10)).alias("ACCUM_AMT"),
    F.col("CAROVR_AMT").cast(DecimalType(38,10)).alias("CAROVR_AMT"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_fmly_accum = dedup_sort(
    df_fmly_accum_tbl,
    ["SUB_SK","PROD_ACCUM_ID","ACCUM_NO","YR_NO"],
    []
)

df_sub_tbl = df_sub_tbl.select(
    F.col("SUB_SK").cast(IntegerType()).alias("SUB_SK"),
    F.col("SUB_UNIQ_KEY").cast(IntegerType()).alias("SUB_UNIQ_KEY"),
    F.col("TRGT_CD").alias("SRC_SYS_CD")
)

df_sub_src_sys = dedup_sort(
    df_sub_tbl,
    ["SUB_SK"],
    []
)

extract_query_in_network = f"""
SELECT DISTINCT
W_ACCUM_DEDCT.SUB_SK,
W_ACCUM_DEDCT.GRP_SK,
P_EOB.DEDCT_VRBL_CD,
P_EOB.SEQ_NO,
W_ACCUM_DEDCT.DEDCT_YR,
P_EOB.DEDCT_VRBL_DESC,
P_EOB.DEDCT_LMT_NTWK_CD,
P_EOB.FMLY_PROD_ACCUM_ID,
P_EOB.FMLY_ACCUM_NO,
MIN(DEDCT.FMLY_DEDCT_AMT) AS FMLY_DEDCT_AMT,
MIN(DEDCT.FMLY_DEDCT_CAROVR_AMT) AS FMLY_DEDCT_CAROVR_AMT,
W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
CSPI.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM_DEDCT,
     {IDSOwner}.EOB_ACCUM EOB,
     {IDSOwner}.P_EOB_DEDCT_XREF P_EOB,
     {IDSOwner}.DEDCT_CMPNT DEDCT,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CLS_PLN_DTL CSPI
WHERE W_ACCUM_DEDCT.MBR_RELSHP_CD='SUB'
  AND W_ACCUM_DEDCT.DEDCT_YR='{AccumYear}'
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL=EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.MBR_UNIQ_KEY=ENR.MBR_UNIQ_KEY
  AND ENR.GRP_SK=CSPI.GRP_SK
  AND ENR.CLS_SK=CSPI.CLS_SK
  AND ENR.CLS_PLN_SK=CSPI.CLS_PLN_SK
  AND ENR.EFF_DT_SK<='{CurrentDate}' AND ENR.TERM_DT_SK>='{CurrentDate}'
  AND ENR.ELIG_IN='Y'
  AND CSPI.EFF_DT_SK<='{CurrentDate}' AND CSPI.TERM_DT_SK>='{CurrentDate}'
  AND EOB.EOB_ACCUM_TYP_CD_SK=MAP1.CD_MPPNG_SK
  AND RTRIM(MAP1.TRGT_CD)='DEDUCTWCAROVR'
  AND EOB.EOB_ACCUM_PERD_CD_SK=MAP2.CD_MPPNG_SK
  AND RTRIM(MAP2.TRGT_CD) IN('INCURYR','CLNDRYR')
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV=P_EOB.DEDCT_VRBL_CD
  AND P_EOB.DEDCT_LMT_NTWK_CD='I'
  AND LTRIM(P_EOB.FMLY_PROD_ACCUM_ID) <>'NA'
  AND P_EOB.FMLY_ACCUM_NO<>0
  AND P_EOB.EFF_DT=(SELECT MAX(P_EOB2.EFF_DT)
                    FROM {IDSOwner}.P_EOB_DEDCT_XREF P_EOB2
                    WHERE P_EOB2.DEDCT_VRBL_CD=P_EOB.DEDCT_VRBL_CD
                      AND P_EOB2.SEQ_NO=P_EOB.SEQ_NO
                      AND P_EOB2.DEDCT_VRBL_CD=W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV
                      AND P_EOB2.DEDCT_LMT_NTWK_CD='I'
                      AND P_EOB2.FMLY_PROD_ACCUM_ID<>'NA'
                      AND P_EOB2.FMLY_ACCUM_NO<>0
                      AND P_EOB2.DEDCT_VRBL_DESC=P_EOB.DEDCT_VRBL_DESC
                      AND P_EOB2.FMLY_ACCUM_NO=P_EOB.FMLY_ACCUM_NO)
  AND P_EOB.FMLY_ACCUM_NO=DEDCT.ACCUM_NO
  AND EOB.EOB_DEDCT_CMPNT_ID=DEDCT.DEDCT_CMPNT_ID
GROUP BY
W_ACCUM_DEDCT.SUB_SK,
W_ACCUM_DEDCT.GRP_SK,
P_EOB.DEDCT_VRBL_CD,
P_EOB.SEQ_NO,
W_ACCUM_DEDCT.DEDCT_YR,
P_EOB.DEDCT_VRBL_DESC,
P_EOB.DEDCT_LMT_NTWK_CD,
P_EOB.FMLY_PROD_ACCUM_ID,
P_EOB.FMLY_ACCUM_NO,
W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
CSPI.PLN_BEG_DT_MO_DAY
"""

df_in_network_tmp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_in_network)
    .load()
)

df_in_network_tmp = df_in_network_tmp.select(
    F.col("SUB_SK").cast(IntegerType()).alias("SUB_SK"),
    F.col("GRP_SK").cast(IntegerType()).alias("GRP_SK"),
    F.col("DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    F.col("SEQ_NO").cast(IntegerType()).alias("SEQ_NO"),
    F.col("DEDCT_YR").alias("DEDCT_YR"),
    F.col("DEDCT_VRBL_DESC").alias("DEDCT_VRBL_DESC"),
    F.col("DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    F.col("FMLY_PROD_ACCUM_ID").alias("FMLY_PROD_ACCUM_ID"),
    F.col("FMLY_ACCUM_NO").cast(IntegerType()).alias("FMLY_ACCUM_NO"),
    F.col("FMLY_DEDCT_AMT").cast(DecimalType(38,10)).alias("FMLY_DEDCT_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT").cast(DecimalType(38,10)).alias("FMLY_DEDCT_CAROVR_AMT"),
    F.col("PROD_CMPNT_PFX_ID_EBCL").alias("PROD_CMPNT_PFX_ID_EBCL"),
    F.col("PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY")
)

df_in_network = dedup_sort(
    df_in_network_tmp,
    ["SUB_SK","GRP_SK","DEDCT_VRBL_CD","SEQ_NO","DEDCT_YR"],
    []
)

extract_query_out_network = f"""
SELECT DISTINCT
W_ACCUM_DEDCT.SUB_SK,
W_ACCUM_DEDCT.GRP_SK,
P_EOB.DEDCT_VRBL_CD,
P_EOB.SEQ_NO,
W_ACCUM_DEDCT.DEDCT_YR,
P_EOB.DEDCT_VRBL_DESC,
P_EOB.DEDCT_LMT_NTWK_CD,
P_EOB.FMLY_PROD_ACCUM_ID,
P_EOB.FMLY_ACCUM_NO,
MAX(DEDCT.FMLY_DEDCT_AMT) AS FMLY_DEDCT_AMT,
MAX(DEDCT.FMLY_DEDCT_CAROVR_AMT) AS FMLY_DEDCT_CAROVR_AMT,
W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
CSPI.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM_DEDCT,
     {IDSOwner}.EOB_ACCUM EOB,
     {IDSOwner}.P_EOB_DEDCT_XREF P_EOB,
     {IDSOwner}.DEDCT_CMPNT DEDCT,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CLS_PLN_DTL CSPI
WHERE W_ACCUM_DEDCT.MBR_RELSHP_CD='SUB'
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL=EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.DEDCT_YR='{AccumYear}'
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL=EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.MBR_UNIQ_KEY=ENR.MBR_UNIQ_KEY
  AND ENR.GRP_SK=CSPI.GRP_SK
  AND ENR.CLS_SK=CSPI.CLS_SK
  AND ENR.CLS_PLN_SK=CSPI.CLS_PLN_SK
  AND ENR.EFF_DT_SK<='{CurrentDate}' AND ENR.TERM_DT_SK>='{CurrentDate}'
  AND ENR.ELIG_IN='Y'
  AND CSPI.EFF_DT_SK<='{CurrentDate}' AND CSPI.TERM_DT_SK>='{CurrentDate}'
  AND EOB.EOB_ACCUM_TYP_CD_SK=MAP1.CD_MPPNG_SK
  AND RTRIM(MAP1.TRGT_CD)='DEDUCTWCAROVR'
  AND EOB.EOB_ACCUM_PERD_CD_SK=MAP2.CD_MPPNG_SK
  AND RTRIM(MAP2.TRGT_CD) IN('INCURYR','CLNDRYR')
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV=P_EOB.DEDCT_VRBL_CD
  AND P_EOB.DEDCT_LMT_NTWK_CD='O'
  AND P_EOB.FMLY_PROD_ACCUM_ID<>'NA'
  AND P_EOB.FMLY_ACCUM_NO<>0
  AND P_EOB.EFF_DT=(SELECT MAX(P_EOB2.EFF_DT)
                    FROM {IDSOwner}.P_EOB_DEDCT_XREF P_EOB2
                    WHERE P_EOB2.DEDCT_VRBL_CD=P_EOB.DEDCT_VRBL_CD
                      AND P_EOB2.SEQ_NO=P_EOB.SEQ_NO
                      AND P_EOB2.DEDCT_VRBL_CD=W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV
                      AND P_EOB2.DEDCT_LMT_NTWK_CD='O'
                      AND P_EOB2.FMLY_PROD_ACCUM_ID<>'NA'
                      AND P_EOB2.FMLY_ACCUM_NO<>0
                      AND P_EOB2.DEDCT_VRBL_DESC=P_EOB.DEDCT_VRBL_DESC
                      AND P_EOB2.FMLY_ACCUM_NO=P_EOB.FMLY_ACCUM_NO)
  AND P_EOB.FMLY_ACCUM_NO=DEDCT.ACCUM_NO
  AND EOB.EOB_DEDCT_CMPNT_ID=DEDCT.DEDCT_CMPNT_ID
GROUP BY
W_ACCUM_DEDCT.SUB_SK,
W_ACCUM_DEDCT.GRP_SK,
P_EOB.DEDCT_VRBL_CD,
P_EOB.SEQ_NO,
W_ACCUM_DEDCT.DEDCT_YR,
P_EOB.DEDCT_VRBL_DESC,
P_EOB.DEDCT_LMT_NTWK_CD,
P_EOB.FMLY_PROD_ACCUM_ID,
P_EOB.FMLY_ACCUM_NO,
W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
CSPI.PLN_BEG_DT_MO_DAY
"""

df_out_network_tmp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_out_network)
    .load()
)

df_out_network_tmp = df_out_network_tmp.select(
    F.col("SUB_SK").cast(IntegerType()).alias("SUB_SK"),
    F.col("GRP_SK").cast(IntegerType()).alias("GRP_SK"),
    F.col("DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    F.col("SEQ_NO").cast(IntegerType()).alias("SEQ_NO"),
    F.col("DEDCT_YR").alias("DEDCT_YR"),
    F.col("DEDCT_VRBL_DESC").alias("DEDCT_VRBL_DESC"),
    F.col("DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    F.col("FMLY_PROD_ACCUM_ID").alias("FMLY_PROD_ACCUM_ID"),
    F.col("FMLY_ACCUM_NO").cast(IntegerType()).alias("FMLY_ACCUM_NO"),
    F.col("FMLY_DEDCT_AMT").cast(DecimalType(38,10)).alias("FMLY_DEDCT_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT").cast(DecimalType(38,10)).alias("FMLY_DEDCT_CAROVR_AMT"),
    F.col("PROD_CMPNT_PFX_ID_EBCL").alias("PROD_CMPNT_PFX_ID_EBCL"),
    F.col("PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY")
)

df_out_network = dedup_sort(
    df_out_network_tmp,
    ["SUB_SK","GRP_SK","DEDCT_VRBL_CD","SEQ_NO","DEDCT_YR"],
    []
)

extract_query_all_networks = f"""
SELECT DISTINCT
W_ACCUM_DEDCT.SUB_SK,
W_ACCUM_DEDCT.GRP_SK,
P_EOB.DEDCT_VRBL_CD,
P_EOB.SEQ_NO,
W_ACCUM_DEDCT.DEDCT_YR,
P_EOB.DEDCT_VRBL_DESC,
P_EOB.DEDCT_LMT_NTWK_CD,
P_EOB.FMLY_PROD_ACCUM_ID,
P_EOB.FMLY_ACCUM_NO,
DEDCT.FMLY_DEDCT_AMT AS FMLY_DEDCT_AMT,
DEDCT.FMLY_DEDCT_CAROVR_AMT AS FMLY_DEDCT_CAROVR_AMT,
W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL,
CSPI.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM_DEDCT,
     {IDSOwner}.EOB_ACCUM EOB,
     {IDSOwner}.P_EOB_DEDCT_XREF P_EOB,
     {IDSOwner}.DEDCT_CMPNT DEDCT,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CLS_PLN_DTL CSPI
WHERE W_ACCUM_DEDCT.MBR_RELSHP_CD='SUB'
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL=EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.DEDCT_YR='{AccumYear}'
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_EBCL=EOB.PROD_CMPNT_PFX_ID
  AND W_ACCUM_DEDCT.MBR_UNIQ_KEY=ENR.MBR_UNIQ_KEY
  AND ENR.GRP_SK=CSPI.GRP_SK
  AND ENR.CLS_SK=CSPI.CLS_SK
  AND ENR.CLS_PLN_SK=CSPI.CLS_PLN_SK
  AND ENR.EFF_DT_SK<='{CurrentDate}' AND ENR.TERM_DT_SK>='{CurrentDate}'
  AND ENR.ELIG_IN='Y'
  AND CSPI.EFF_DT_SK<='{CurrentDate}' AND CSPI.TERM_DT_SK>='{CurrentDate}'
  AND EOB.EOB_ACCUM_TYP_CD_SK=MAP1.CD_MPPNG_SK
  AND RTRIM(MAP1.TRGT_CD)='DEDUCTWCAROVR'
  AND EOB.EOB_ACCUM_PERD_CD_SK=MAP2.CD_MPPNG_SK
  AND RTRIM(MAP2.TRGT_CD) IN('INCURYR','CLNDRYR')
  AND W_ACCUM_DEDCT.PROD_CMPNT_PFX_ID_DV=P_EOB.DEDCT_VRBL_CD
  AND P_EOB.DEDCT_LMT_NTWK_CD='A'
  AND P_EOB.FMLY_PROD_ACCUM_ID<>'NA'
  AND P_EOB.FMLY_ACCUM_NO<>0
  AND P_EOB.EFF_DT=(SELECT MAX(P_EOB2.EFF_DT)
                    FROM {IDSOwner}.P_EOB_DEDCT_XREF P_EOB2
                    WHERE P_EOB2.DEDCT_VRBL_CD=P_EOB.DEDCT_VRBL_CD
                      AND P_EOB2.SEQ_NO=P_EOB.SEQ_NO
                      AND P_EOB2.DEDCT_LMT_NTWK_CD='A'
                      AND P_EOB2.FMLY_PROD_ACCUM_ID=P_EOB.FMLY_PROD_ACCUM_ID
                      AND P_EOB2.FMLY_ACCUM_NO=P_EOB.FMLY_ACCUM_NO)
  AND P_EOB.FMLY_ACCUM_NO=DEDCT.ACCUM_NO
  AND EOB.EOB_DEDCT_CMPNT_ID=DEDCT.DEDCT_CMPNT_ID
"""

df_all_networks_tmp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_all_networks)
    .load()
)

df_all_networks_tmp = df_all_networks_tmp.select(
    F.col("SUB_SK").cast(IntegerType()).alias("SUB_SK"),
    F.col("GRP_SK").cast(IntegerType()).alias("GRP_SK"),
    F.col("DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    F.col("SEQ_NO").cast(IntegerType()).alias("SEQ_NO"),
    F.col("DEDCT_YR").alias("DEDCT_YR"),
    F.col("DEDCT_VRBL_DESC").alias("DEDCT_VRBL_DESC"),
    F.col("DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    F.col("FMLY_PROD_ACCUM_ID").alias("FMLY_PROD_ACCUM_ID"),
    F.col("FMLY_ACCUM_NO").cast(IntegerType()).alias("FMLY_ACCUM_NO"),
    F.col("FMLY_DEDCT_AMT").cast(DecimalType(38,10)).alias("FMLY_DEDCT_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT").cast(DecimalType(38,10)).alias("FMLY_DEDCT_CAROVR_AMT"),
    F.col("PROD_CMPNT_PFX_ID_EBCL").alias("PROD_CMPNT_PFX_ID_EBCL"),
    F.col("PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY")
)

df_all_networks = dedup_sort(
    df_all_networks_tmp,
    ["SUB_SK","GRP_SK","DEDCT_VRBL_CD","SEQ_NO","DEDCT_YR"],
    []
)

df_subS_eob_tmp = df_in_network.unionByName(df_out_network).unionByName(df_all_networks)

df_subS_eob = dedup_sort(
    df_subS_eob_tmp,
    ["SUB_SK","GRP_SK","DEDCT_VRBL_CD","SEQ_NO","DEDCT_YR"],
    []
)

df_subS_eob = df_subS_eob.select(
    F.col("SUB_SK"),
    F.col("GRP_SK"),
    F.col("DEDCT_VRBL_CD"),
    F.col("SEQ_NO"),
    F.col("DEDCT_YR"),
    F.col("DEDCT_VRBL_DESC"),
    F.col("DEDCT_LMT_NTWK_CD"),
    F.col("FMLY_PROD_ACCUM_ID"),
    F.col("FMLY_ACCUM_NO"),
    F.col("FMLY_DEDCT_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT"),
    F.col("PROD_CMPNT_PFX_ID_EBCL"),
    F.col("PLN_BEG_DT_MO_DAY")
)

df_SUB_EOB_tmp = dedup_sort(
    df_subS_eob,
    ["SUB_SK","GRP_SK","DEDCT_VRBL_CD","SEQ_NO","DEDCT_YR"],
    []
)

df_SUB_EOB = df_SUB_EOB_tmp.select(
    F.col("SUB_SK"),
    F.col("GRP_SK"),
    F.col("DEDCT_VRBL_CD"),
    F.col("SEQ_NO"),
    F.col("DEDCT_YR"),
    F.col("DEDCT_VRBL_DESC"),
    F.col("DEDCT_LMT_NTWK_CD"),
    F.col("FMLY_PROD_ACCUM_ID"),
    F.col("FMLY_ACCUM_NO"),
    F.col("FMLY_DEDCT_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT"),
    F.col("PROD_CMPNT_PFX_ID_EBCL"),
    F.col("PLN_BEG_DT_MO_DAY")
)

df_enriched = df_SUB_EOB.alias("SUB_EOB") \
    .join(df_fmly_accum.alias("fmly_accum"),
          [
              F.col("SUB_EOB.SUB_SK")==F.col("fmly_accum.SUB_SK"),
              F.col("SUB_EOB.FMLY_PROD_ACCUM_ID")==F.col("fmly_accum.PROD_ACCUM_ID"),
              F.col("SUB_EOB.FMLY_ACCUM_NO")==F.col("fmly_accum.ACCUM_NO"),
              F.col("SUB_EOB.DEDCT_YR").cast(IntegerType())==F.col("fmly_accum.YR_NO")
          ],
          "left"
    ) \
    .join(df_sub_src_sys.alias("sub_src_sys"),
          [
              F.col("SUB_EOB.SUB_SK")==F.col("sub_src_sys.SUB_SK")
          ],
          "left"
    )

df_enriched = df_enriched.withColumn(
    "svNextYear",
    (F.col("SUB_EOB.DEDCT_YR").cast(IntegerType())+F.lit(1)).cast(StringType())
)

df_enriched = df_enriched.withColumn(
    "svPlnYrEffDt",
    F.when(F.length("SUB_EOB.PLN_BEG_DT_MO_DAY")==3,
        F.concat_ws("",
            F.col("SUB_EOB.DEDCT_YR"),
            F.lit("-0"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(1,1),
            F.lit("-"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(2,2)
        )
    ).otherwise(
        F.concat_ws("",
            F.col("SUB_EOB.DEDCT_YR"),
            F.lit("-"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(1,2),
            F.lit("-"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(3,2)
        )
    )
)

df_enriched = df_enriched.withColumn(
    "svPlnYrEndDt1",
    F.when(F.length("SUB_EOB.PLN_BEG_DT_MO_DAY")==3,
        F.concat_ws("",
            F.col("svNextYear"),
            F.lit("-0"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(1,1),
            F.lit("-"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(2,2)
        )
    ).otherwise(
        F.concat_ws("",
            F.col("svNextYear"),
            F.lit("-"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(1,2),
            F.lit("-"),
            F.col("SUB_EOB.PLN_BEG_DT_MO_DAY").substr(3,2)
        )
    )
)

df_enriched = df_enriched.withColumn(
    "svPlnYrEndDt",
    FIND_DATE(
        F.col("svPlnYrEndDt1"),
        F.lit("-1"),
        F.lit("D"),
        F.lit("X"),
        F.lit("CCYY-MM-DD")
    )
)

dfHardCodeFix = df_enriched.select(
    F.when(F.col("sub_src_sys.SUB_UNIQ_KEY").isNull(), F.lit("UNK")).otherwise(F.col("sub_src_sys.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.when(F.col("sub_src_sys.SUB_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("sub_src_sys.SUB_UNIQ_KEY")).alias("SUB_UNIQ_KEY"),
    F.col("SUB_EOB.DEDCT_YR").alias("YR_NO"),
    F.col("SUB_EOB.DEDCT_VRBL_CD").alias("DEDCT_VRBL_CD"),
    F.col("SUB_EOB.SEQ_NO").alias("DEDCT_VRBL_SEQ_NO"),
    F.col("SUB_EOB.DEDCT_LMT_NTWK_CD").alias("DEDCT_LMT_NTWK_CD"),
    F.when(F.col("SUB_EOB.FMLY_ACCUM_NO")==53, F.lit("Y")).otherwise(F.lit("N")).alias("DRUG_ACCUM_IN"),
    F.when(F.col("fmly_accum.ACCUM_AMT").isNull(), F.lit(0)).otherwise(F.col("fmly_accum.ACCUM_AMT")).alias("ACCUM_AMT"),
    F.col("SUB_EOB.FMLY_DEDCT_AMT").alias("FMLY_DEDCT_AMT"),
    F.col("SUB_EOB.FMLY_ACCUM_NO").alias("ACCUM_NO"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    trim(F.col("SUB_EOB.DEDCT_VRBL_DESC")).alias("DEDCT_VRBL_DESC"),
    F.col("SUB_EOB.FMLY_PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.when(F.col("fmly_accum.CAROVR_AMT").isNull(), F.lit(0)).otherwise(F.col("fmly_accum.CAROVR_AMT")).alias("CAROVR_AMT"),
    F.when(F.col("SUB_EOB.FMLY_DEDCT_CAROVR_AMT").isNull(), F.lit(0)).otherwise(F.col("SUB_EOB.FMLY_DEDCT_CAROVR_AMT")).alias("FMLY_DEDCT_CAROVR_AMT"),
    F.col("SUB_EOB.PROD_CMPNT_PFX_ID_EBCL").alias("PROD_CMPNT_PFX_ID_EBCL"),
    F.when(F.col("fmly_accum.PLN_YR_EFF_DT").isNotNull(), F.col("fmly_accum.PLN_YR_EFF_DT")).otherwise(F.col("svPlnYrEffDt")).alias("PLN_YR_EFF_DT"),
    F.when(F.col("fmly_accum.PLN_YR_END_DT").isNotNull(), F.col("fmly_accum.PLN_YR_END_DT")).otherwise(F.col("svPlnYrEndDt")).alias("PLN_YR_END_DT")
)

dfLoadFile = dfHardCodeFix.select(
    F.col("SRC_SYS_CD"),
    F.col("SUB_UNIQ_KEY"),
    F.col("YR_NO"),
    F.col("DEDCT_VRBL_CD"),
    F.col("DEDCT_VRBL_SEQ_NO"),
    F.col("DEDCT_LMT_NTWK_CD"),
    F.col("DRUG_ACCUM_IN"),
    F.col("ACCUM_AMT"),
    F.when(
      F.col("PROD_CMPNT_PFX_ID_EBCL")=="H571",
      F.when(F.col("DEDCT_VRBL_SEQ_NO")==0, A011)
       .when(F.col("DEDCT_VRBL_SEQ_NO")==1, A056)
       .otherwise(A277)
    ).when(
      F.col("PROD_CMPNT_PFX_ID_EBCL")=="H572",
      F.when(F.col("DEDCT_VRBL_SEQ_NO")==0, A011)
       .when(F.col("DEDCT_VRBL_SEQ_NO")==1, A025)
       .otherwise(A030)
    ).when(
      F.col("PROD_CMPNT_PFX_ID_EBCL")=="H542",
      F.when(F.col("DEDCT_VRBL_SEQ_NO")==0, A373)
       .when(F.col("DEDCT_VRBL_SEQ_NO")==1, A371)
       .otherwise(A372)
    ).otherwise(F.col("FMLY_DEDCT_AMT")).cast(DecimalType(38,10)).alias("FMLY_DEDCT_AMT"),
    F.col("ACCUM_NO"),
    F.col("LAST_UPDT_RUN_CYC_NO"),
    F.col("DEDCT_VRBL_DESC"),
    F.col("PROD_ACCUM_ID"),
    F.col("CAROVR_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT"),
    F.col("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT")
)

dfLoadFile = dfLoadFile \
  .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), 25, " ")) \
  .withColumn("DEDCT_VRBL_CD", rpad(F.col("DEDCT_VRBL_CD"), 25, " ")) \
  .withColumn("DEDCT_LMT_NTWK_CD", rpad(F.col("DEDCT_LMT_NTWK_CD"), 25, " ")) \
  .withColumn("DRUG_ACCUM_IN", rpad(F.col("DRUG_ACCUM_IN"), 1, " ")) \
  .withColumn("DEDCT_VRBL_DESC", rpad(F.col("DEDCT_VRBL_DESC"), 25, " ")) \
  .withColumn("PROD_ACCUM_ID", rpad(F.col("PROD_ACCUM_ID"), 25, " ")) \
  .withColumn("PLN_YR_EFF_DT", rpad(F.col("PLN_YR_EFF_DT"), 10, " ")) \
  .withColumn("PLN_YR_END_DT", rpad(F.col("PLN_YR_END_DT"), 10, " "))

write_files(
    dfLoadFile,
    f"{adls_path}/load/P_FMLY_ACCUM_DEDCT.dat.{AccumYear}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)