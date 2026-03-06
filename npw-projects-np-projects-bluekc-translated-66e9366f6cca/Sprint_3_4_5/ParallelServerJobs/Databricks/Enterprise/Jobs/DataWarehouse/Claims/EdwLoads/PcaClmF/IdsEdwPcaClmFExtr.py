# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                                     Project/                                                                                                                                                                                                    Code                             Date
# MAGIC Developer              Date              Altiris #     Change Description                                                                                                Devl Project                                              Reviewer                     Reviewed
# MAGIC ----------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------            ---------------------------------------                           -----------------------             -------------------
# MAGIC Parikshith Chada   11/12/2006    Originally Programmed
# MAGIC Parikshith Chada   12/10/2006    Made changes to the code by adding W_ETL_PCA_CRVR table and 
# MAGIC                                                      added Claim Check information
# MAGIC Parikshith Chada   12/28/2006    Changes to the code of REL_BASE_CLM_SRC_SYS_CD and 
# MAGIC                                                      REL_BASE_CLM_ID fields in Transformer
# MAGIC Bhoomi D               07/01/2007   Added 3 new fields CLM_STTUS_CD, CLM_STTUS_NM, CLM_STTUS_CD_SK
# MAGIC                                                      Changed Related PCA SQL from SK > 1 to SK <> 1 & SK <> 0
# MAGIC Brent Leland          2009-06-09     Fixed transform logic for the SK >1
# MAGIC Hugh Sisson          2009-06-17     Added CLM_PD_DT_SK column     
# MAGIC                                                                           
# MAGIC Bhupinder Kaur     09/19/2013    EDWefficiencies(5114)     To create a loadfile for EDW target table  PCA_CLM_F           EnterpriseWrhsDevl                                   Jag Yelavarthi           2013-12-22
# MAGIC 
# MAGIC 
# MAGIC Jag Yelavarthi       2014-02-07     Corrected Nullability check for REL_BASE_CLM_SRC_SYS_CD column derivation           EnterpriseWrhsDevl                                 Bhoomi Dasari           2/7/2014

# MAGIC JobName: IdsEdwPcaClmFExtr
# MAGIC This file will be used to load into  PCA_CLM_F  table.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Key:
# MAGIC CLM_ID,
# MAGIC REL_BASE_CLM_SK,
# MAGIC CLM_SK
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Key:
# MAGIC REL_BASE_CLM_SK,
# MAGIC CLM_SK
# MAGIC Add Defaults and null handling
# MAGIC Add Defaults and null handling
# MAGIC Add Defaults and null handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# Acquire DB connection for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# -------------------------------------------------------------------------
# Stage: db2_CLM_PCA_Extr
# -------------------------------------------------------------------------
extract_query_db2_CLM_PCA_Extr = """SELECT 
CLM_PCA.CLM_SK,
MAP1.TRGT_CD as CLM_PCA_SRC_SYS_CD,
CLM_PCA.CLM_ID,
CLM.CLS_SK,
CLM.CLS_PLN_SK,
CLM.GRP_SK,
CLM.MBR_SK,
CLM.PROD_SK,
CLM.SUBGRP_SK,
CLM.SUB_SK,
CLM.SVC_STRT_DT_SK,
CLM_PCA.TOT_CNSD_AMT,
CLM_PCA.TOT_PD_AMT,
CLM_PCA.SUB_TOT_PCA_AVLBL_AMT,
CLM_PCA.SUB_TOT_PCA_PD_TO_DT_AMT,
CLM_PCA.CRT_RUN_CYC_EXCTN_SK,
CLM.REL_BASE_CLM_SK,
CLM.PCA_TYP_CD_SK,
MAP2.TRGT_CD as PCA_TYP_CD,
MAP2.TRGT_CD_NM as PCA_TYP_NM,
CLM.SRC_SYS_CD_SK,
MAP3.TRGT_CD as CLM_SRC_SYS_CD,
MAP3.TRGT_CD_NM as CLM_SRC_SYS_CD_NM,
CLM.CLM_STTUS_CD_SK,
MAP4.TRGT_CD as CLM_STTUS_CD,
MAP4.TRGT_CD_NM as CLM_STTUS_NM,
CLM.PD_DT_SK
FROM 
#$IDSOwner#.W_EDW_PCA_ETL_DRVR DRVR,
#$IDSOwner#.CLM_PCA CLM_PCA,
#$IDSOwner#.CLM CLM,
#$IDSOwner#.CD_MPPNG MAP1,
#$IDSOwner#.CD_MPPNG MAP2,
#$IDSOwner#.CD_MPPNG MAP3,
#$IDSOwner#.CD_MPPNG MAP4
WHERE     DRVR.CLM_ID                    =    CLM_PCA.CLM_ID 
AND       DRVR.SRC_SYS_CD_SK            =    CLM_PCA.SRC_SYS_CD_SK
AND       CLM_PCA.SRC_SYS_CD_SK         =    MAP1.CD_MPPNG_SK
AND       MAP1.TRGT_CD                  =    'FACETS'
AND       CLM_PCA.CLM_SK                =    CLM.CLM_SK
AND       CLM.PCA_TYP_CD_SK            =    MAP2.CD_MPPNG_SK
AND       CLM.SRC_SYS_CD_SK            =    MAP3.CD_MPPNG_SK
AND       CLM.CLM_STTUS_CD_SK          =    MAP4.CD_MPPNG_SK
"""
df_db2_CLM_PCA_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_PCA_Extr.replace("#$IDSOwner#", IDSOwner))
    .load()
)

df_lnk_IdsEdwClmPcaExtr_in = df_db2_CLM_PCA_Extr

# -------------------------------------------------------------------------
# Stage: db2_CLM_Extr
# -------------------------------------------------------------------------
extract_query_db2_CLM_Extr = """SELECT 
CLM.CLM_SK,
CLM.SRC_SYS_CD_SK ,
MAP2.TRGT_CD as CLM_SRC_SYS_CD ,
CLM.CLM_ID,
CLM.CLS_SK,
MBR_ENR.CLS_PLN_SK,
CLM.GRP_SK,
CLM.MBR_SK,
MBR_ENR.PROD_SK,
CLM.SUBGRP_SK,
CLM.SUB_SK,
CLM.SVC_STRT_DT_SK,
CLM.CHRG_AMT,
CLM_LN.PAYBL_AMT,
CLM.CRT_RUN_CYC_EXCTN_SK,
CLM.REL_BASE_CLM_SK,
CLM.PCA_TYP_CD_SK,
MAP1.TRGT_CD as CLM_PCA_PCA_TYPE_CD,
MAP1.TRGT_CD_NM as CLM_PCA_PCA_TYPE_NM,
CLM.CLM_STTUS_CD_SK,
MAP4.TRGT_CD as CLM_STTUS_CD,
MAP4.TRGT_CD_NM as CLM_STTUS_NM,
CLM.PD_DT_SK
FROM 
#$IDSOwner#.CLM CLM,
#$IDSOwner#.CLM_LN CLM_LN,
#$IDSOwner#.CD_MPPNG MAP1,
#$IDSOwner#.CD_MPPNG MAP2,
#$IDSOwner#.CD_MPPNG MAP3,
#$IDSOwner#.CD_MPPNG MAP4,
#$IDSOwner#.MBR_ENR MBR_ENR,
#$IDSOwner#.W_EDW_PCA_ETL_DRVR DRVR 
WHERE     DRVR.CLM_ID = CLM.CLM_ID 
AND       DRVR.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
AND       CLM.SRC_SYS_CD_SK = MAP2.CD_MPPNG_SK 
AND       MAP2.TRGT_CD ='FACETS'
AND       CLM.PCA_TYP_CD_SK = MAP1.CD_MPPNG_SK 
AND       MAP1.TRGT_CD IN ('EMPWBNF','RUNOUT')
AND       CLM.CLM_SK = CLM_LN.CLM_SK 
AND       CLM.MBR_SK = MBR_ENR.MBR_SK
AND       MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP3.CD_MPPNG_SK
AND       MAP3.TRGT_CD = 'MED'
AND       CLM.CLM_STTUS_CD_SK = MAP4.CD_MPPNG_SK
AND       CLM.SVC_STRT_DT_SK >= MBR_ENR.EFF_DT_SK
AND       CLM.SVC_STRT_DT_SK <= MBR_ENR.TERM_DT_SK
"""
df_db2_CLM_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_Extr.replace("#$IDSOwner#", IDSOwner))
    .load()
)

df_lnk_IdsEdwClmExtr_in = df_db2_CLM_Extr

# -------------------------------------------------------------------------
# Stage: db2_CLM_PcaRunOut_in
# -------------------------------------------------------------------------
extract_query_db2_CLM_PcaRunOut_in = """SELECT 
CLM.CLM_ID,
CLM.CLS_PLN_SK,
CLM.PROD_SK 
FROM #$IDSOwner#.CD_MPPNG CD_MPPNG,
#$IDSOwner#.CLM CLM
 WHERE CLM.PCA_TYP_CD_SK=CD_MPPNG.CD_MPPNG_SK 
AND CD_MPPNG.TRGT_CD IN ('RUNOUT','PCA');
"""
df_db2_CLM_PcaRunOut_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_PcaRunOut_in.replace("#$IDSOwner#", IDSOwner))
    .load()
)

df_lnk_PcaRunOut_in = df_db2_CLM_PcaRunOut_in

# -------------------------------------------------------------------------
# Stage: db2_CLM_RelBaseClm_in
# -------------------------------------------------------------------------
extract_query_db2_CLM_RelBaseClm_in = """SELECT 

CLM.CLM_SK,
CLM.CLM_ID,
MAP.TRGT_CD as SRC_SYS_CD

FROM #$IDSOwner#.CLM CLM,
     #$IDSOwner#.CD_MPPNG MAP 
WHERE  CLM.CLM_SK in  (SELECT CLM2.REL_BASE_CLM_SK 
                       FROM   #$IDSOwner#.CLM CLM2 
                       WHERE CLM2.REL_BASE_CLM_SK <> 1
                         AND CLM2.REL_BASE_CLM_SK <> 0)
AND    CLM.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
"""
df_db2_CLM_RelBaseClm_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_RelBaseClm_in.replace("#$IDSOwner#", IDSOwner))
    .load()
)

# -------------------------------------------------------------------------
# Stage: CpyRelBaseClm (PxCopy)
# -------------------------------------------------------------------------
df_lkp_Clm_RelBaseClm = df_db2_CLM_RelBaseClm_in.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)
df_lkp_RelBaseClm = df_db2_CLM_RelBaseClm_in.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# -------------------------------------------------------------------------
# Stage: db2_CLM_CHK_in
# -------------------------------------------------------------------------
extract_query_db2_CLM_CHK_in = """SELECT CLM_CHK.CLM_SK,
CLM_CHK.CLM_CHK_PAYE_TYP_CD_SK,
CLM_CHK.CLM_CHK_LOB_CD_SK,
CLM_CHK.CLM_CHK_PAYMT_METH_CD_SK,
CLM_CHK.EXTRNL_CHK_IN,
CLM_CHK.CHK_PD_DT_SK,
CLM_CHK.CHK_ORIG_AMT,
CLM_CHK.CHK_NO,
CLM_CHK.CHK_SEQ_NO,
CLM_CHK.CHK_PAYE_NM,
CLM_CHK.CHK_PAYMT_REF_ID,
CLM_CHK.PCA_CHK_IN,
CD_MPPNG.TRGT_CD as PCA_CLM_CHK_LOB_CD,
CD_MPPNG.TRGT_CD_NM as PCA_CLM_CHK_LOB_NM,
CD_MPPNG2.TRGT_CD as PCA_CLM_CHK_PAYE_TYP_CD,
CD_MPPNG2.TRGT_CD_NM as PCA_CLM_CHK_PAYE_TYP_NM,
CD_MPPNG3.TRGT_CD as PCA_CLM_CHK_PAYMT_METH_CD,
CD_MPPNG3.TRGT_CD_NM as PCA_CLM_CHK_PAYMT_METH_NM
FROM #$IDSOwner#.CLM_CHK CLM_CHK,
#$IDSOwner#.CD_MPPNG CD_MPPNG,
#$IDSOwner#.CD_MPPNG CD_MPPNG2,
#$IDSOwner#.CD_MPPNG CD_MPPNG3 
WHERE CLM_CHK.CLM_CHK_LOB_CD_SK=CD_MPPNG.CD_MPPNG_SK 
AND CLM_CHK.CLM_CHK_PAYE_TYP_CD_SK=CD_MPPNG2.CD_MPPNG_SK 
AND CLM_CHK.CLM_CHK_PAYMT_METH_CD_SK=CD_MPPNG3.CD_MPPNG_SK 
AND CLM_CHK.PCA_CHK_IN='Y';
"""
df_db2_CLM_CHK_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_CHK_in.replace("#$IDSOwner#", IDSOwner))
    .load()
)

# -------------------------------------------------------------------------
# Stage: CpyClmChk (PxCopy)
# -------------------------------------------------------------------------
df_lkp_Clm_Chk_2 = df_db2_CLM_CHK_in.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_CHK_PAYE_TYP_CD_SK").alias("CLM_CHK_PAYE_TYP_CD_SK"),
    F.col("CLM_CHK_LOB_CD_SK").alias("CLM_CHK_LOB_CD_SK"),
    F.col("CLM_CHK_PAYMT_METH_CD_SK").alias("CLM_CHK_PAYMT_METH_CD_SK"),
    F.col("EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
    F.col("CHK_PD_DT_SK").alias("CHK_PD_DT_SK"),
    F.col("CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
    F.col("CHK_NO").alias("CHK_NO"),
    F.col("CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID"),
    F.col("PCA_CHK_IN").alias("PCA_CHK_IN"),
    F.col("PCA_CLM_CHK_LOB_CD").alias("PCA_CLM_CHK_LOB_CD"),
    F.col("PCA_CLM_CHK_LOB_NM").alias("PCA_CLM_CHK_LOB_NM"),
    F.col("PCA_CLM_CHK_PAYE_TYP_CD").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.col("PCA_CLM_CHK_PAYE_TYP_NM").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.col("PCA_CLM_CHK_PAYMT_METH_CD").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.col("PCA_CLM_CHK_PAYMT_METH_NM").alias("PCA_CLM_CHK_PAYMT_METH_NM")
)

df_lkp_Clm_Chk = df_db2_CLM_CHK_in.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_CHK_PAYE_TYP_CD_SK").alias("CLM_CHK_PAYE_TYP_CD_SK"),
    F.col("CLM_CHK_LOB_CD_SK").alias("CLM_CHK_LOB_CD_SK"),
    F.col("CLM_CHK_PAYMT_METH_CD_SK").alias("CLM_CHK_PAYMT_METH_CD_SK"),
    F.col("EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
    F.col("CHK_PD_DT_SK").alias("CHK_PD_DT_SK"),
    F.col("CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
    F.col("CHK_NO").alias("CHK_NO"),
    F.col("CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID"),
    F.col("PCA_CHK_IN").alias("PCA_CHK_IN"),
    F.col("PCA_CLM_CHK_LOB_CD").alias("PCA_CLM_CHK_LOB_CD"),
    F.col("PCA_CLM_CHK_LOB_NM").alias("PCA_CLM_CHK_LOB_NM"),
    F.col("PCA_CLM_CHK_PAYE_TYP_CD").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.col("PCA_CLM_CHK_PAYE_TYP_NM").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.col("PCA_CLM_CHK_PAYMT_METH_CD").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.col("PCA_CLM_CHK_PAYMT_METH_NM").alias("PCA_CLM_CHK_PAYMT_METH_NM")
)

# -------------------------------------------------------------------------
# Stage: lkp_ClmPca (PxLookup)
#   Primary link: df_lnk_IdsEdwClmPcaExtr_in (alias A)
#   Left lookups:
#     - df_lkp_Clm_Chk (alias B) on A.CLM_SK = B.CLM_SK
#     - df_lkp_RelBaseClm (alias C) on A.REL_BASE_CLM_SK = C.CLM_SK
# -------------------------------------------------------------------------
df_lkp_ClmPca = (
    df_lnk_IdsEdwClmPcaExtr_in.alias("A")
    .join(df_lkp_Clm_Chk.alias("B"), F.col("A.CLM_SK") == F.col("B.CLM_SK"), "left")
    .join(df_lkp_RelBaseClm.alias("C"), F.col("A.REL_BASE_CLM_SK") == F.col("C.CLM_SK"), "left")
    .select(
        F.col("A.CLM_SK").alias("CLM_SK"),
        F.col("A.CLM_PCA_SRC_SYS_CD").alias("CLM_PCA_SRC_SYS_CD"),
        F.col("A.CLM_ID").alias("CLM_ID"),
        F.col("A.CLS_SK").alias("CLS_SK"),
        F.col("A.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("A.GRP_SK").alias("GRP_SK"),
        F.col("A.MBR_SK").alias("MBR_SK"),
        F.col("A.PROD_SK").alias("PROD_SK"),
        F.col("A.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("A.SUB_SK").alias("SUB_SK"),
        F.col("A.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("A.TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
        F.col("A.TOT_PD_AMT").alias("TOT_PD_AMT"),
        F.col("A.SUB_TOT_PCA_AVLBL_AMT").alias("SUB_TOT_PCA_AVLBL_AMT"),
        F.col("A.SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT"),
        F.col("A.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("A.REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
        F.col("A.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
        F.col("A.PCA_TYP_CD").alias("PCA_TYP_CD"),
        F.col("A.PCA_TYP_NM").alias("PCA_TYP_NM"),
        F.col("A.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("A.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("A.CLM_SRC_SYS_CD_NM").alias("CLM_SRC_SYS_CD_NM"),
        F.col("A.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
        F.col("A.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        F.col("A.CLM_STTUS_NM").alias("CLM_STTUS_NM"),
        F.col("A.PD_DT_SK").alias("PD_DT_SK"),
        F.col("B.CLM_SK").alias("clmchk_CLM_SK"),
        F.col("B.CLM_CHK_PAYE_TYP_CD_SK").alias("CLM_CHK_PAYE_TYP_CD_SK"),
        F.col("B.CLM_CHK_LOB_CD_SK").alias("CLM_CHK_LOB_CD_SK"),
        F.col("B.CLM_CHK_PAYMT_METH_CD_SK").alias("CLM_CHK_PAYMT_METH_CD_SK"),
        F.col("B.EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
        F.col("B.CHK_PD_DT_SK").alias("CHK_PD_DT_SK"),
        F.col("B.CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
        F.col("B.CHK_NO").alias("CHK_NO"),
        F.col("B.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
        F.col("B.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
        F.col("B.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID"),
        F.col("B.PCA_CHK_IN").alias("PCA_CHK_IN"),
        F.col("B.PCA_CLM_CHK_LOB_CD").alias("PCA_CLM_CHK_LOB_CD"),
        F.col("B.PCA_CLM_CHK_LOB_NM").alias("PCA_CLM_CHK_LOB_NM"),
        F.col("B.PCA_CLM_CHK_PAYE_TYP_CD").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
        F.col("B.PCA_CLM_CHK_PAYE_TYP_NM").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
        F.col("B.PCA_CLM_CHK_PAYMT_METH_CD").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
        F.col("B.PCA_CLM_CHK_PAYMT_METH_NM").alias("PCA_CLM_CHK_PAYMT_METH_NM"),
        F.col("C.CLM_SK").alias("RelBaseClm_CLM_SK"),
        F.col("C.CLM_ID").alias("RelBaseClm_CLM_ID"),
        F.col("C.SRC_SYS_CD").alias("RelBaseClm_SRC_SYS_CD")
    )
)

# -------------------------------------------------------------------------
# Stage: xfrm_Business_rule (CTransformerStage) => output Lnk_ClmPca_out
# -------------------------------------------------------------------------
df_Lnk_ClmPca_out_pre = df_lkp_ClmPca

df_Lnk_ClmPca_out = df_Lnk_ClmPca_out_pre.select(
    F.col("CLM_SK"),
    F.when(F.col("CLM_PCA_SRC_SYS_CD").isNull() | (F.col("CLM_PCA_SRC_SYS_CD") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_PCA_SRC_SYS_CD"))
     .alias("CLM_PCA_SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.when(F.col("CLM_STTUS_CD").isNull() | (F.col("CLM_STTUS_CD") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_STTUS_CD")).alias("CLM_STTUS_CD"),
    F.when(F.col("CLM_STTUS_NM").isNull() | (F.col("CLM_STTUS_NM") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_STTUS_NM")).alias("CLM_STTUS_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_LOB_CD")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_LOB_CD"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_LOB_NM")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_LOB_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYE_TYP_CD")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYE_TYP_NM")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYMT_METH_CD")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYMT_METH_NM")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.when(F.col("PCA_TYP_CD").isNull() | (F.col("PCA_TYP_CD") == ''), F.lit("UNK"))
     .otherwise(F.col("PCA_TYP_CD")).alias("PCA_TYP_CD"),
    F.when(F.col("PCA_TYP_NM").isNull() | (F.col("PCA_TYP_NM") == ''), F.lit("UNK"))
     .otherwise(F.col("PCA_TYP_NM")).alias("PCA_TYP_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("EXTRNL_CHK_IN")).otherwise(F.lit("N"))
     .alias("EXTRNL_CHK_IN"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_PD_DT_SK")).otherwise(F.lit("NA"))
     .alias("CHK_PD_DT_SK"),
    F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_AVLBL_AMT").alias("SUB_TOT_PCA_AVLBL_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_ORIG_AMT")).otherwise(F.lit(0))
     .alias("CHK_ORIG_AMT"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_NO")).otherwise(F.lit(0))
     .alias("CHK_NO"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_SEQ_NO")).otherwise(F.lit(0))
     .alias("CHK_SEQ_NO"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_PAYE_NM")).otherwise(F.lit("NA"))
     .alias("CHK_PAYE_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_PAYMT_REF_ID")).otherwise(F.lit("NA"))
     .alias("CHK_PAYMT_REF_ID"),
    F.when(F.col("RelBaseClm_CLM_SK").isNotNull(),
           F.when(
             F.length(F.trim(F.coalesce(F.col("RelBaseClm_SRC_SYS_CD"), F.lit("")))) == 0,
             F.lit("UNK")
           ).otherwise(F.trim(F.col("RelBaseClm_SRC_SYS_CD")))
    ).otherwise(F.lit("NA")).alias("REL_BASE_CLM_SRC_SYS_CD"),
    F.when(F.col("RelBaseClm_CLM_SK").isNotNull(),
           F.when(
             F.length(F.trim(F.coalesce(F.col("RelBaseClm_CLM_ID"), F.lit("")))) == 0,
             F.lit("UNK")
           ).otherwise(F.trim(F.col("RelBaseClm_CLM_ID")))
    ).otherwise(F.lit("NA")).alias("REL_BASE_CLM_CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CLM_CHK_PAYE_TYP_CD_SK")).otherwise(F.lit(1))
     .alias("CLM_CHK_PAYE_TYP_CD_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CLM_CHK_LOB_CD_SK")).otherwise(F.lit(1))
     .alias("CLM_CHK_LOB_CD_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CLM_CHK_PAYMT_METH_CD_SK")).otherwise(F.lit(1))
     .alias("CLM_CHK_PAYMT_METH_CD_SK"),
    F.col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    F.col("PD_DT_SK").alias("CLM_PD_DT_SK")
)

# -------------------------------------------------------------------------
# Stage: lkp_Clm (PxLookup)
#   Primary link: df_lnk_IdsEdwClmExtr_in (alias M)
#   Left lookups:
#     - df_lnk_PcaRunOut_in (alias N) => join on M.CLM_ID = N.CLM_ID
#     - df_lkp_Clm_RelBaseClm (alias O) => M.REL_BASE_CLM_SK = O.CLM_SK
#     - df_lkp_Clm_Chk_2 (alias P) => M.CLM_SK = P.CLM_SK
# -------------------------------------------------------------------------
df_lkp_Clm = (
    df_lnk_IdsEdwClmExtr_in.alias("M")
    .join(df_lnk_PcaRunOut_in.alias("N"), F.col("M.CLM_ID") == F.col("N.CLM_ID"), "left")
    .join(df_lkp_Clm_RelBaseClm.alias("O"), F.col("M.REL_BASE_CLM_SK") == F.col("O.CLM_SK"), "left")
    .join(df_lkp_Clm_Chk_2.alias("P"), F.col("M.CLM_SK") == F.col("P.CLM_SK"), "left")
    .select(
        F.col("M.CLM_SK").alias("CLM_SK"),
        F.col("M.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("M.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("M.CLM_ID").alias("CLM_ID"),
        F.col("M.CLS_SK").alias("CLS_SK"),
        F.col("M.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("M.GRP_SK").alias("GRP_SK"),
        F.col("M.MBR_SK").alias("MBR_SK"),
        F.col("M.PROD_SK").alias("PROD_SK"),
        F.col("M.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("M.SUB_SK").alias("SUB_SK"),
        F.col("M.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("M.CHRG_AMT").alias("CHRG_AMT"),
        F.col("M.PAYBL_AMT").alias("PAYBL_AMT"),
        F.col("M.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("M.REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
        F.col("M.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
        F.col("M.CLM_PCA_PCA_TYPE_CD").alias("CLM_PCA_PCA_TYPE_CD"),
        F.col("M.CLM_PCA_PCA_TYPE_NM").alias("CLM_PCA_PCA_TYPE_NM"),
        F.col("M.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
        F.col("M.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        F.col("M.CLM_STTUS_NM").alias("CLM_STTUS_NM"),
        F.col("M.PD_DT_SK").alias("PD_DT_SK"),
        F.col("N.CLM_ID").alias("PcaRun_CLM_ID"),
        F.col("N.CLS_PLN_SK").alias("PcaRun_CLS_PLN_SK"),
        F.col("N.PROD_SK").alias("PcaRun_PROD_SK"),
        F.col("O.CLM_SK").alias("RelBaseClm_CLM_SK"),
        F.col("O.CLM_ID").alias("RelBaseClm_CLM_ID"),
        F.col("O.SRC_SYS_CD").alias("RelBaseClm_SRC_SYS_CD"),
        F.col("P.CLM_SK").alias("ClmChk_CLM_SK"),
        F.col("P.CLM_CHK_PAYE_TYP_CD_SK").alias("CLM_CHK_PAYE_TYP_CD_SK"),
        F.col("P.CLM_CHK_LOB_CD_SK").alias("CLM_CHK_LOB_CD_SK"),
        F.col("P.CLM_CHK_PAYMT_METH_CD_SK").alias("CLM_CHK_PAYMT_METH_CD_SK"),
        F.col("P.EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
        F.col("P.CHK_PD_DT_SK").alias("CHK_PD_DT_SK"),
        F.col("P.CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
        F.col("P.CHK_NO").alias("CHK_NO"),
        F.col("P.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
        F.col("P.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
        F.col("P.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID"),
        F.col("P.PCA_CHK_IN").alias("PCA_CHK_IN"),
        F.col("P.PCA_CLM_CHK_LOB_CD").alias("PCA_CLM_CHK_LOB_CD"),
        F.col("P.PCA_CLM_CHK_LOB_NM").alias("PCA_CLM_CHK_LOB_NM"),
        F.col("P.PCA_CLM_CHK_PAYE_TYP_CD").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
        F.col("P.PCA_CLM_CHK_PAYE_TYP_NM").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
        F.col("P.PCA_CLM_CHK_PAYMT_METH_CD").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
        F.col("P.PCA_CLM_CHK_PAYMT_METH_NM").alias("PCA_CLM_CHK_PAYMT_METH_NM")
    )
)

# -------------------------------------------------------------------------
# Stage: xfrm_Business_rules (CTransformerStage) => output Lnk_Clm_out
# -------------------------------------------------------------------------
df_Lnk_Clm_out_pre = df_lkp_Clm

df_Lnk_Clm_out = df_Lnk_Clm_out_pre.select(
    F.col("CLM_SK"),
    F.when(F.col("CLM_SRC_SYS_CD").isNull() | (F.col("CLM_SRC_SYS_CD") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_SRC_SYS_CD"))
     .alias("CLM_PCA_SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.when(F.col("CLM_PCA_PCA_TYPE_CD") == 'EMPWBNF',
           F.col("CLS_PLN_SK"))
     .otherwise(
       F.when(F.col("PcaRun_CLS_PLN_SK").isNull(), F.lit(0)).otherwise(F.col("PcaRun_CLS_PLN_SK"))
     ).alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.when(F.col("CLM_PCA_PCA_TYPE_CD") == 'EMPWBNF',
           F.col("PROD_SK"))
     .otherwise(
       F.when(F.col("PcaRun_PROD_SK").isNull(), F.lit(0)).otherwise(F.col("PcaRun_PROD_SK"))
     ).alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.when(F.col("CLM_STTUS_CD").isNull() | (F.col("CLM_STTUS_CD") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_STTUS_CD")).alias("CLM_STTUS_CD"),
    F.when(F.col("CLM_STTUS_NM").isNull() | (F.col("CLM_STTUS_NM") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_STTUS_NM")).alias("CLM_STTUS_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_LOB_CD")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_LOB_CD"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_LOB_NM")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_LOB_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYE_TYP_CD")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYE_TYP_NM")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYMT_METH_CD")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("PCA_CLM_CHK_PAYMT_METH_NM")).otherwise(F.lit("NA"))
     .alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.when(F.col("CLM_PCA_PCA_TYPE_CD").isNull() | (F.col("CLM_PCA_PCA_TYPE_CD") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_PCA_PCA_TYPE_CD")).alias("PCA_TYP_CD"),
    F.when(F.col("CLM_PCA_PCA_TYPE_NM").isNull() | (F.col("CLM_PCA_PCA_TYPE_NM") == ''), F.lit("UNK"))
     .otherwise(F.col("CLM_PCA_PCA_TYPE_NM")).alias("PCA_TYP_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("EXTRNL_CHK_IN")).otherwise(F.lit("N"))
     .alias("EXTRNL_CHK_IN"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_PD_DT_SK")).otherwise(F.lit("NA"))
     .alias("CHK_PD_DT_SK"),
    F.col("CHRG_AMT").alias("TOT_CNSD_AMT"),
    F.col("PAYBL_AMT").alias("TOT_PD_AMT"),
    F.lit(0).alias("SUB_TOT_PCA_AVLBL_AMT"),
    F.lit(0).alias("SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_ORIG_AMT")).otherwise(F.lit(0))
     .alias("CHK_ORIG_AMT"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_NO")).otherwise(F.lit(0))
     .alias("CHK_NO"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_SEQ_NO")).otherwise(F.lit(0))
     .alias("CHK_SEQ_NO"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_PAYE_NM")).otherwise(F.lit("NA"))
     .alias("CHK_PAYE_NM"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CHK_PAYMT_REF_ID")).otherwise(F.lit("NA"))
     .alias("CHK_PAYMT_REF_ID"),
    F.when(F.col("ClmChk_CLM_SK").isNotNull(),
           F.when(
             F.length(F.trim(F.coalesce(F.col("RelBaseClm_SRC_SYS_CD"), F.lit("")))) == 0,
             F.lit("UNK")
           ).otherwise(F.trim(F.col("RelBaseClm_SRC_SYS_CD")))
    ).otherwise(F.lit("NA")).alias("REL_BASE_CLM_SRC_SYS_CD"),
    F.when(F.col("ClmChk_CLM_SK").isNotNull(),
           F.when(
             F.length(F.trim(F.coalesce(F.col("RelBaseClm_CLM_ID"), F.lit("")))) == 0,
             F.lit("UNK")
           ).otherwise(F.trim(F.col("RelBaseClm_CLM_ID")))
    ).otherwise(F.lit("NA")).alias("REL_BASE_CLM_CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CLM_CHK_PAYE_TYP_CD_SK")).otherwise(F.lit(1))
     .alias("CLM_CHK_PAYE_TYP_CD_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CLM_CHK_LOB_CD_SK")).otherwise(F.lit(1))
     .alias("CLM_CHK_LOB_CD_SK"),
    F.when(F.col("PCA_CHK_IN") == 'Y', F.col("CLM_CHK_PAYMT_METH_CD_SK")).otherwise(F.lit(1))
     .alias("CLM_CHK_PAYMT_METH_CD_SK"),
    F.col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    F.col("PD_DT_SK").alias("CLM_PD_DT_SK")
)

# -------------------------------------------------------------------------
# Stage: Fnl_PcaClmF (PxFunnel) => union of df_Lnk_ClmPca_out and df_Lnk_Clm_out
# -------------------------------------------------------------------------
common_cols_fnl_PcaClmF = [
    "CLM_SK",
    "CLM_PCA_SRC_SYS_CD",
    "CLM_ID",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "MBR_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "CLM_STTUS_CD",
    "CLM_STTUS_NM",
    "PCA_CLM_CHK_LOB_CD",
    "PCA_CLM_CHK_LOB_NM",
    "PCA_CLM_CHK_PAYE_TYP_CD",
    "PCA_CLM_CHK_PAYE_TYP_NM",
    "PCA_CLM_CHK_PAYMT_METH_CD",
    "PCA_CLM_CHK_PAYMT_METH_NM",
    "PCA_TYP_CD",
    "PCA_TYP_NM",
    "EXTRNL_CHK_IN",
    "SVC_STRT_DT_SK",
    "CHK_PD_DT_SK",
    "TOT_CNSD_AMT",
    "TOT_PD_AMT",
    "SUB_TOT_PCA_AVLBL_AMT",
    "SUB_TOT_PCA_PD_TO_DT_AMT",
    "CHK_ORIG_AMT",
    "CHK_NO",
    "CHK_SEQ_NO",
    "CHK_PAYE_NM",
    "CHK_PAYMT_REF_ID",
    "REL_BASE_CLM_SRC_SYS_CD",
    "REL_BASE_CLM_CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_CHK_PAYE_TYP_CD_SK",
    "CLM_STTUS_CD_SK",
    "CLM_CHK_LOB_CD_SK",
    "CLM_CHK_PAYMT_METH_CD_SK",
    "PCA_TYP_CD_SK",
    "CLM_PD_DT_SK"
]
df_lnkFnlDataOut = df_Lnk_ClmPca_out.select(common_cols_fnl_PcaClmF).unionByName(
    df_Lnk_Clm_out.select(common_cols_fnl_PcaClmF)
)

# -------------------------------------------------------------------------
# Stage: clai (CTransformerStage)
#   We output 3 links from df_lnkFnlDataOut:
#     1) lnk_full_data_Out => filter "CLM_SK <> 0 AND CLM_SK <> 1"
#     2) lnk_NA_Out => single row with "WhereExpression" constants
#     3) lnk_UNK_Out => single row with "WhereExpression" constants
# -------------------------------------------------------------------------
df_lnk_full_data_Out = df_lnkFnlDataOut.filter((F.col("CLM_SK") != 0) & (F.col("CLM_SK") != 1))

# lnk_NA_Out => produce one row of literals:
df_lnk_NA_Out = df_lnkFnlDataOut.limit(1).select(
    F.lit(1).alias("CLM_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CLM_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLS_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("SUBGRP_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit("NA").alias("CLM_STTUS_CD"),
    F.lit("NA").alias("CLM_STTUS_NM"),
    F.lit("NA").alias("PCA_CLM_CHK_LOB_CD"),
    F.lit("NA").alias("PCA_CLM_CHK_LOB_NM"),
    F.lit("NA").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.lit("NA").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.lit("NA").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.lit("NA").alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.lit("NA").alias("PCA_TYP_CD"),
    F.lit("NA").alias("PCA_TYP_NM"),
    F.lit("N").alias("PCA_CLM_EXTRNL_CHK_IN"),
    F.lit("1753-01-01").alias("CLM_SVC_STRT_DT_SK"),
    F.lit("1753-01-01").alias("PCA_CLM_CHK_PD_DT_SK"),
    F.lit(0).alias("CLM_PCA_TOT_CNSD_AMT"),
    F.lit(0).alias("CLM_PCA_TOT_PD_AMT"),
    F.lit(0).alias("CLM_PCA_SUB_TOT_PCA_AVLBL_AMT"),
    F.lit(0).alias("CLM_PCA_SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.lit(0).alias("PCA_CLM_CHK_ORIG_AMT"),
    F.lit(0).alias("PCA_CLM_CHK_NO"),
    F.lit(0).alias("PCA_CLM_CHK_SEQ_NO"),
    F.lit("NA").alias("PCA_CLM_CHK_PAYE_NM"),
    F.lit("NA").alias("PCA_CLM_CHK_PAYMT_REF_ID"),
    F.lit("NA").alias("REL_BASE_CLM_SRC_SYS_CD"),
    F.lit("NA").alias("REL_BASE_CLM_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_STTUS_CD_SK"),
    F.lit(1).alias("PCA_CLM_CHK_LOB_CD_SK"),
    F.lit(1).alias("PCA_CLM_CHK_PAYE_TYP_CD_SK"),
    F.lit(1).alias("PCA_CLM_CHK_PAYMT_METH_CD_SK"),
    F.lit(1).alias("PCA_TYP_CD_SK"),
    F.lit("1753-01-01").alias("CLM_PD_DT_SK")
)

# lnk_UNK_Out => produce one row of literals:
df_lnk_UNK_Out = df_lnkFnlDataOut.limit(1).select(
    F.lit(0).alias("CLM_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLS_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("SUBGRP_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit("UNK").alias("CLM_STTUS_CD"),
    F.lit("UNK").alias("CLM_STTUS_NM"),
    F.lit("UNK").alias("PCA_CLM_CHK_LOB_CD"),
    F.lit("UNK").alias("PCA_CLM_CHK_LOB_NM"),
    F.lit("UNK").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.lit("UNK").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.lit("UNK").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.lit("UNK").alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.lit("UNK").alias("PCA_TYP_CD"),
    F.lit("UNK").alias("PCA_TYP_NM"),
    F.lit("N").alias("PCA_CLM_EXTRNL_CHK_IN"),
    F.lit("1753-01-01").alias("CLM_SVC_STRT_DT_SK"),
    F.lit("1753-01-01").alias("PCA_CLM_CHK_PD_DT_SK"),
    F.lit(0).alias("CLM_PCA_TOT_CNSD_AMT"),
    F.lit(0).alias("CLM_PCA_TOT_PD_AMT"),
    F.lit(0).alias("CLM_PCA_SUB_TOT_PCA_AVLBL_AMT"),
    F.lit(0).alias("CLM_PCA_SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.lit(0).alias("PCA_CLM_CHK_ORIG_AMT"),
    F.lit(0).alias("PCA_CLM_CHK_NO"),
    F.lit(0).alias("PCA_CLM_CHK_SEQ_NO"),
    F.lit("UNK").alias("PCA_CLM_CHK_PAYE_NM"),
    F.lit("UNK").alias("PCA_CLM_CHK_PAYMT_REF_ID"),
    F.lit("UNK").alias("REL_BASE_CLM_SRC_SYS_CD"),
    F.lit("UNK").alias("REL_BASE_CLM_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_STTUS_CD_SK"),
    F.lit(0).alias("PCA_CLM_CHK_LOB_CD_SK"),
    F.lit(0).alias("PCA_CLM_CHK_PAYE_TYP_CD_SK"),
    F.lit(0).alias("PCA_CLM_CHK_PAYMT_METH_CD_SK"),
    F.lit(0).alias("PCA_TYP_CD_SK"),
    F.lit("1753-01-01").alias("CLM_PD_DT_SK")
)

# -------------------------------------------------------------------------
# Stage: Fnl_UNK_NA_data (PxFunnel) => union of lnk_NA_Out, lnk_UNK_Out, lnk_full_data_Out
# -------------------------------------------------------------------------
common_cols_final = [
    "CLM_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_RUN_CYC_EXCTN_DT_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "MBR_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "CLM_STTUS_CD",
    "CLM_STTUS_NM",
    "PCA_CLM_CHK_LOB_CD",
    "PCA_CLM_CHK_LOB_NM",
    "PCA_CLM_CHK_PAYE_TYP_CD",
    "PCA_CLM_CHK_PAYE_TYP_NM",
    "PCA_CLM_CHK_PAYMT_METH_CD",
    "PCA_CLM_CHK_PAYMT_METH_NM",
    "PCA_TYP_CD",
    "PCA_TYP_NM",
    "PCA_CLM_EXTRNL_CHK_IN",
    "CLM_SVC_STRT_DT_SK",
    "PCA_CLM_CHK_PD_DT_SK",
    "CLM_PCA_TOT_CNSD_AMT",
    "CLM_PCA_TOT_PD_AMT",
    "CLM_PCA_SUB_TOT_PCA_AVLBL_AMT",
    "CLM_PCA_SUB_TOT_PCA_PD_TO_DT_AMT",
    "PCA_CLM_CHK_ORIG_AMT",
    "PCA_CLM_CHK_NO",
    "PCA_CLM_CHK_SEQ_NO",
    "PCA_CLM_CHK_PAYE_NM",
    "PCA_CLM_CHK_PAYMT_REF_ID",
    "REL_BASE_CLM_SRC_SYS_CD",
    "REL_BASE_CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_STTUS_CD_SK",
    "PCA_CLM_CHK_LOB_CD_SK",
    "PCA_CLM_CHK_PAYE_TYP_CD_SK",
    "PCA_CLM_CHK_PAYMT_METH_CD_SK",
    "PCA_TYP_CD_SK",
    "CLM_PD_DT_SK"
]
df_lnk_NA_Out_aligned = df_lnk_NA_Out.select(common_cols_final)
df_lnk_UNK_Out_aligned = df_lnk_UNK_Out.select(common_cols_final)
df_lnk_full_data_Out_aligned = df_lnk_full_data_Out.select(
    F.col("CLM_SK"),
    F.col("CLM_PCA_SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID"),
    # next columns we map carefully:
    # we do not have separate columns for CRT_RUN_CYC_EXCTN_DT_SK or LAST_RUN_CYC_EXCTN_DT_SK in the df,
    # but the expression says "EDWRunCycleDate". We'll replicate as needed:
    # For funnel, these columns come from the transform. They are not present. Use placeholders or direct?
    # The job indicates "Expression": "EDWRunCycleDate" => we can do a lit(EDWRunCycleDate).
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
    F.col("CLM_STTUS_CD"),
    F.col("CLM_STTUS_NM"),
    F.col("PCA_CLM_CHK_LOB_CD"),
    F.col("PCA_CLM_CHK_LOB_NM"),
    F.col("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.col("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.col("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.col("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.col("PCA_TYP_CD"),
    F.col("PCA_TYP_NM"),
    F.col("EXTRNL_CHK_IN").alias("PCA_CLM_EXTRNL_CHK_IN"),
    F.col("SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("CHK_PD_DT_SK").alias("PCA_CLM_CHK_PD_DT_SK"),
    F.col("TOT_CNSD_AMT").alias("CLM_PCA_TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT").alias("CLM_PCA_TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_AVLBL_AMT").alias("CLM_PCA_SUB_TOT_PCA_AVLBL_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("CLM_PCA_SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.col("CHK_ORIG_AMT").alias("PCA_CLM_CHK_ORIG_AMT"),
    F.col("CHK_NO").alias("PCA_CLM_CHK_NO"),
    F.col("CHK_SEQ_NO").alias("PCA_CLM_CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("PCA_CLM_CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("PCA_CLM_CHK_PAYMT_REF_ID"),
    F.col("REL_BASE_CLM_SRC_SYS_CD"),
    F.col("REL_BASE_CLM_CLM_ID").alias("REL_BASE_CLM_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_STTUS_CD_SK"),
    F.col("CLM_CHK_LOB_CD_SK").alias("PCA_CLM_CHK_LOB_CD_SK"),
    F.col("CLM_CHK_PAYE_TYP_CD_SK").alias("PCA_CLM_CHK_PAYE_TYP_CD_SK"),
    F.col("CLM_CHK_PAYMT_METH_CD_SK").alias("PCA_CLM_CHK_PAYMT_METH_CD_SK"),
    F.col("PCA_TYP_CD_SK"),
    F.col("CLM_PD_DT_SK")
).select(common_cols_final)

df_Fnl_UNK_NA_data = df_lnk_NA_Out_aligned.unionByName(df_lnk_UNK_Out_aligned).unionByName(df_lnk_full_data_Out_aligned)

# -------------------------------------------------------------------------
# In the final writing stage, we must rpad any char/varchar columns with specified length
# using the "Columns" info from seq_PCA_CLM_F_csv_Load.
# The datastage metadata indicates columns with "SqlType":"char" and "Length":...
# We'll apply rpad to those columns if present.
# The final columns from that stage are (with indicated char lengths):
#   "CRT_RUN_CYC_EXCTN_DT_SK" char(10)
#   "LAST_RUN_CYC_EXCTN_DT_SK" char(10)
#   "PCA_CLM_EXTRNL_CHK_IN" char(1)
#   "CLM_SVC_STRT_DT_SK" char(10)
#   "PCA_CLM_CHK_PD_DT_SK" char(10)
#   "CLM_PD_DT_SK" char(10)
# We rpad each of these to its length.
# -------------------------------------------------------------------------
df_final_write_prep = df_Fnl_UNK_NA_data \
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("PCA_CLM_EXTRNL_CHK_IN", F.rpad(F.col("PCA_CLM_EXTRNL_CHK_IN"), 1, " ")) \
    .withColumn("CLM_SVC_STRT_DT_SK", F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ")) \
    .withColumn("PCA_CLM_CHK_PD_DT_SK", F.rpad(F.col("PCA_CLM_CHK_PD_DT_SK"), 10, " ")) \
    .withColumn("CLM_PD_DT_SK", F.rpad(F.col("CLM_PD_DT_SK"), 10, " "))

# Now select columns in correct final order for the output file:
final_cols_for_seq = [
    "CLM_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_RUN_CYC_EXCTN_DT_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "MBR_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "CLM_STTUS_CD",
    "CLM_STTUS_NM",
    "PCA_CLM_CHK_LOB_CD",
    "PCA_CLM_CHK_LOB_NM",
    "PCA_CLM_CHK_PAYE_TYP_CD",
    "PCA_CLM_CHK_PAYE_TYP_NM",
    "PCA_CLM_CHK_PAYMT_METH_CD",
    "PCA_CLM_CHK_PAYMT_METH_NM",
    "PCA_TYP_CD",
    "PCA_TYP_NM",
    "PCA_CLM_EXTRNL_CHK_IN",
    "CLM_SVC_STRT_DT_SK",
    "PCA_CLM_CHK_PD_DT_SK",
    "CLM_PCA_TOT_CNSD_AMT",
    "CLM_PCA_TOT_PD_AMT",
    "CLM_PCA_SUB_TOT_PCA_AVLBL_AMT",
    "CLM_PCA_SUB_TOT_PCA_PD_TO_DT_AMT",
    "PCA_CLM_CHK_ORIG_AMT",
    "PCA_CLM_CHK_NO",
    "PCA_CLM_CHK_SEQ_NO",
    "PCA_CLM_CHK_PAYE_NM",
    "PCA_CLM_CHK_PAYMT_REF_ID",
    "REL_BASE_CLM_SRC_SYS_CD",
    "REL_BASE_CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_STTUS_CD_SK",
    "PCA_CLM_CHK_LOB_CD_SK",
    "PCA_CLM_CHK_PAYE_TYP_CD_SK",
    "PCA_CLM_CHK_PAYMT_METH_CD_SK",
    "PCA_TYP_CD_SK",
    "CLM_PD_DT_SK"
]
df_final = df_final_write_prep.select(final_cols_for_seq)

# -------------------------------------------------------------------------
# Stage: seq_PCA_CLM_F_csv_Load (PxSequentialFile)
#   Write to f"{adls_path}/load/PCA_CLM_F.dat"
#   delimiter=",", quote="^", nullValue=None, mode="overwrite", header=False
# -------------------------------------------------------------------------
write_files(
    df_final,
    f"{adls_path}/load/PCA_CLM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)