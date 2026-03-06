# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsIdsIndvBePopHlthPgmEnrTransExtr
# MAGIC 
# MAGIC Called By: IdsIndvBePopHlthPgmEnrTransCntl
# MAGIC 
# MAGIC                           
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala             06/04/2019      US110616                    Original Program			   IntegrateDev2     	         Abhiram Dasarathy	2019-06-07
# MAGIC 
# MAGIC Venkat Munagala            08/08/2019      US140387                    Mapping changes for 2 columns	                   IntegrateDev2               Jaideep Mankala            08/08/2019
# MAGIC                                                                                                       ENR_DENIED_RSN_CD_SK and
# MAGIC                                                                                                       PGM_CLOSE_RSN_CD_SK 
# MAGIC Raj Kommineni                09/27/2019       US148780               Added logic for 5 columns in transformation                 IntegrateDev2           Jaideep Mankala            10/01/2019
# MAGIC                                                                                                                PGM_CLOSE_DT_SK,
# MAGIC                                                                                                                PGM_ENR_DT_SK,
# MAGIC                                                                                                                PGM_RQST_DT_SK,
# MAGIC                                                                                                                PGM_SCRN_DT_SK and
# MAGIC                                                                                                                PGM_STRT_DT_SK

# MAGIC Job extracts most recent records update from Case Management table and writes records to file for Pkey process
# MAGIC CASE_MGT_STTUS to reference the CASE_MGT_STTUS_RSN_CD_SK for ENR_DENIED_RSN_CD_SK and PGM_CLOSE_RSN_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')
LastRunCyc = get_widget_value('LastRunCyc','')
CurrDate = get_widget_value('CurrDate','')
CurrDateMinOne = get_widget_value('CurrDateMinOne','')

# Database configuration
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# CASE_MGT_STTUS
df_CASE_MGT_STTUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
WITH CASE_STTUS AS 
(
SELECT
    CASE_MGT_STTUS.CASE_MGT_SK,
    CASE_MGT_STTUS.CASE_MGT_STTUS_SEQ_NO,
    CASE_MGT_STTUS.CASE_MGT_STTUS_RSN_CD_SK,
    DENSE_RANK() OVER(PARTITION BY CASE_MGT_SK ORDER BY CASE_MGT_STTUS_SEQ_NO DESC) AS ROWNUM
FROM {IDSOwner}.CASE_MGT_STTUS CASE_MGT_STTUS
)
SELECT 
    CASE_MGT_SK,
    CASE_MGT_STTUS_SEQ_NO,
    CASE_MGT_STTUS_RSN_CD_SK
FROM CASE_STTUS
WHERE ROWNUM =1
""")
    .load()
)

# PREV_DATA
df_PREV_DATA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select distinct 
POP_HLTH_PGM_ENR_ID, 
SRC_SYS_CD_SK, 
SCRN_RQST_PROD_CD_SK, 
SCRN_RQST_SVRTY_CD_SK, 
PGM_RQST_DT_SK, 
PGM_SCRN_DT_SK, 
ROW_TERM_DT_SK,
SCRN_BY_USER_ID, 
SCRN_ASG_TO_USER_ID, 
INDV_BE_POP_HLTH_PGM_ENR_T_SK, 
ROW_EFF_DT_SK,
ENR_PROD_CD_SK,
ENR_SVRTY_CD_SK,
PGM_ENR_CRT_DT_SK,
PGM_ENR_DT_SK,
ENRED_BY_USER_ID,
ENR_ASG_TO_USER_ID,
ENR_POP_HLTH_PGM_ID,
PGM_STRT_DT_SK,
PGM_CLOSE_DT_SK,
ENR_DENIED_RSN_CD_SK,
PGM_CLOSE_RSN_CD_SK
from {IDSOwner}.INDV_BE_POP_HLTH_PGM_ENR_TRANS
WHERE ROW_TERM_DT_SK = '2199-12-31'
""")
    .load()
)

# APP_USER
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select distinct 
USER_SK ,
USER_ID
from {IDSOwner}.APP_USER
""")
    .load()
)

# ENR_SVRTY_CD
df_ENR_SVRTY_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select DISTINCT
'ENRSVRTYCD' as LKUP_VAL,
SRC_CD,
CD_MPPNG_SK
from {IDSOwner}.CD_MPPNG
where 
 SRC_SYS_CD = 'IDS'
AND SRC_CLCTN_CD = 'IDS'  
AND TRGT_CLCTN_CD= 'IDS' 
AND SRC_DOMAIN_NM = 'RISK SEVERITY' 
AND TRGT_DOMAIN_NM = 'ENROLLMENT SEVERITY'
""")
    .load()
)

# PGM_SRC_CD
df_PGM_SRC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select DISTINCT
'PGMSRCCD' as PGM_SRC_CD,
SRC_CD,
CD_MPPNG_SK
from {IDSOwner}.CD_MPPNG
where 
 SRC_SYS_CD = 'IDS'
AND SRC_CLCTN_CD = 'IDS'  
AND TRGT_CLCTN_CD= 'IDS' 
AND SRC_DOMAIN_NM = 'CASE MANAGEMENT ORIGIN' 
AND TRGT_DOMAIN_NM = 'PROGRAM SOURCE'
""")
    .load()
)

# PGM_ORIG_SRC
df_PGM_ORIG_SRC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select DISTINCT
'PGMORIGSRCCD' as PGM_ORIG_SRC_CD,
CD_MPPNG_SK
from {IDSOwner}.CD_MPPNG
where 
 SRC_SYS_CD = 'IDS'
AND SRC_CD = 'BCBSKC'
AND TRGT_DOMAIN_NM = 'PROGRAM ORIGINATING SOURCE'
""")
    .load()
)

# CMPLXTY
df_CMPLXTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select DISTINCT
'CMPLXTY' as CMPLXTY_SRC_CD,
SRC_CD,
CD_MPPNG_SK
from {IDSOwner}.CD_MPPNG
where 
 SRC_SYS_CD = 'IDS'
AND SRC_CLCTN_CD = 'IDS'  
AND TRGT_CLCTN_CD= 'IDS' 
AND SRC_DOMAIN_NM = 'RISK SEVERITY' 
AND TRGT_DOMAIN_NM = 'ENROLLMENT SEVERITY'
""")
    .load()
)

# CASE_MGT
df_CASE_MGT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
Select DISTINCT
CM.CASE_MGT_SK, 
CM.SRC_SYS_CD_SK , 
CM.CASE_MGT_ID , 
CM.CRT_RUN_CYC_EXCTN_SK , 
CM.LAST_UPDT_RUN_CYC_EXCTN_SK , 
CM.DIAG_CD_SK, GRP_SK, 
CM.INPT_USER_SK, 
CM.MBR_SK, 
CM.PRI_CASE_MGR_USER_SK, 
CM.PROC_CD_SK, 
CM.PROD_SK, 
CM.SUBGRP_SK, 
CM.SUB_SK, 
CM.CASE_MGT_CLS_PLN_PRODCAT_CD_SK ,
CM.CASE_MGT_CMPLXTY_LVL_CD_SK ,
CM.CASE_MGT_ORIG_TYP_CD_SK,
CM.CASE_MGT_STTUS_CD_SK , 
CM.CASE_MGT_TYP_CD_SK, 
CM.END_DT_SK , 
CM.INPT_DT_SK , 
CM.PRI_CASE_MGR_NEXT_RVW_DT_SK , 
CM.STRT_DT_SK , 
CM.STTUS_DT_SK, 
CM.MBR_AGE , 
CM.PRI_CNTCT_SEQ_NO , 
CM.STTUS_SEQ_NO , 
CM.SUM_DESC,
'ENRSVRTYCD' as ENR_SVRTY_CD,
'PGMSRCCD' as PGM_SRC_CD,
'PGMORIGSRCCD' as PGM_ORIG_SRC_CD,
'CMPLXTY' as CMPLXTY_SRC_CD,
MBR.INDV_BE_KEY
from {IDSOwner}.CASE_MGT CM , {IDSOwner}.MBR MBR
WHERE
CM.MBR_SK = MBR.MBR_SK
AND CM.LAST_UPDT_RUN_CYC_EXCTN_SK = {LastRunCyc}
""")
    .load()
)

# CD_MPPNG (Used for possible lookup of RPTNG_PROD_CD)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
select distinct CASE_MGT.CASE_MGT_SK , CD_MPPNG_RPTNG_PROD_CD.CD_MPPNG_SK
from {IDSOwner}.CASE_MGT CASE_MGT
INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG_PGM_ID 
  ON CASE_MGT.CASE_MGT_TYP_CD_SK = CD_MPPNG_PGM_ID.CD_MPPNG_SK
INNER JOIN {IDSOwner}.P_SRC_DOMAIN_TRNSLTN P_SRC_DOMAIN_HIST
  ON P_SRC_DOMAIN_HIST.SRC_SYS_CD = 'EDW'
  AND P_SRC_DOMAIN_HIST.DOMAIN_ID = 'CASE_MGT_TYP'
  AND P_SRC_DOMAIN_HIST.SRC_DOMAIN_TX = CD_MPPNG_PGM_ID.TRGT_CD
INNER JOIN {IDSOwner}.POP_HLTH_PGM POP_HLTH_PGM
  ON POP_HLTH_PGM.POP_HLTH_PGM_ID = P_SRC_DOMAIN_HIST.TRGT_DOMAIN_TX
INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG_RPTNG_PROD_CD
  ON CD_MPPNG_RPTNG_PROD_CD.SRC_CD = POP_HLTH_PGM.RPTNG_PROD_CD
  AND CD_MPPNG_RPTNG_PROD_CD.SRC_SYS_CD = 'IDS'
  AND CD_MPPNG_RPTNG_PROD_CD.SRC_DOMAIN_NM = 'REPORTING PRODUCT'
  AND CD_MPPNG_RPTNG_PROD_CD.TRGT_SRC_SYS_CD = 'IDS'
  AND CD_MPPNG_RPTNG_PROD_CD.TRGT_DOMAIN_NM = 'ENROLLMENT PRODUCT'
""")
    .load()
)

# ENR_POP_PGM_ID
df_ENR_POP_PGM_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
select distinct CASE_MGT.CASE_MGT_SK , P_SRC_DOMAIN_HIST.TRGT_DOMAIN_TX
from {IDSOwner}.CASE_MGT CASE_MGT
INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG_PGM_ID 
  ON CASE_MGT.CASE_MGT_TYP_CD_SK = CD_MPPNG_PGM_ID.CD_MPPNG_SK
INNER JOIN {IDSOwner}.P_SRC_DOMAIN_TRNSLTN P_SRC_DOMAIN_HIST
  ON P_SRC_DOMAIN_HIST.SRC_SYS_CD = 'EDW'
  AND P_SRC_DOMAIN_HIST.DOMAIN_ID = 'CASE_MGT_TYP'
  AND P_SRC_DOMAIN_HIST.SRC_DOMAIN_TX = CD_MPPNG_PGM_ID.TRGT_CD
""")
    .load()
)

# db2_CD_MPPNG_in
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM,
COALESCE(SRC_CD,'UNK') SRC_CD
from {IDSOwner}.CD_MPPNG
""")
    .load()
)

# COPY stage
df_copy = df_db2_CD_MPPNG_in

# COPY outputs
df_Case_mgt_stts = df_copy.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_cm_cmplx_lvl_cd = df_copy.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_cm_orig_typ_cd = df_copy.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD").alias("SRC_CD")
)

# CD_LKUP (PxLookup)
df_cd_lkup = (
    df_CASE_MGT.alias("CaseMgmtData")
    .join(
        df_CD_MPPNG.alias("Cd_Mppng_lkup"),
        F.col("CaseMgmtData.CASE_MGT_SK") == F.col("Cd_Mppng_lkup.CASE_MGT_SK"),
        "left"
    )
    .join(
        df_Case_mgt_stts.alias("Case_mgt_stts"),
        F.col("CaseMgmtData.CASE_MGT_STTUS_CD_SK") == F.col("Case_mgt_stts.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cm_cmplx_lvl_cd.alias("cm_cmplx_lvl_cd"),
        F.col("CaseMgmtData.CASE_MGT_CMPLXTY_LVL_CD_SK") == F.col("cm_cmplx_lvl_cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cm_orig_typ_cd.alias("cm_orig_typ_cd"),
        F.col("CaseMgmtData.CASE_MGT_ORIG_TYP_CD_SK") == F.col("cm_orig_typ_cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ENR_POP_PGM_ID.alias("Enr_pop_hlth_pgm_lkup"),
        F.col("CaseMgmtData.CASE_MGT_SK") == F.col("Enr_pop_hlth_pgm_lkup.CASE_MGT_SK"),
        "left"
    )
    .select(
        F.col("CaseMgmtData.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("CaseMgmtData.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CaseMgmtData.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("CaseMgmtData.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CaseMgmtData.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CaseMgmtData.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("CaseMgmtData.GRP_SK").alias("GRP_SK"),
        F.col("CaseMgmtData.MBR_SK").alias("MBR_SK"),
        F.col("CaseMgmtData.PRI_CASE_MGR_USER_SK").alias("PRI_CASE_MGR_USER_SK"),
        F.col("CaseMgmtData.PROC_CD_SK").alias("PROC_CD_SK"),
        F.col("CaseMgmtData.PROD_SK").alias("PROD_SK"),
        F.col("CaseMgmtData.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("CaseMgmtData.SUB_SK").alias("SUB_SK"),
        F.col("CaseMgmtData.CASE_MGT_CLS_PLN_PRODCAT_CD_SK").alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
        F.col("CaseMgmtData.CASE_MGT_CMPLXTY_LVL_CD_SK").alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
        F.col("CaseMgmtData.CASE_MGT_ORIG_TYP_CD_SK").alias("CASE_MGT_ORIG_TYP_CD_SK"),
        F.col("CaseMgmtData.CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
        F.col("CaseMgmtData.CASE_MGT_TYP_CD_SK").alias("CASE_MGT_TYP_CD_SK"),
        F.rpad(F.col("CaseMgmtData.END_DT_SK"),10," ").alias("END_DT_SK"),
        F.rpad(F.col("CaseMgmtData.INPT_DT_SK"),10," ").alias("INPT_DT_SK"),
        F.rpad(F.col("CaseMgmtData.PRI_CASE_MGR_NEXT_RVW_DT_SK"),10," ").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
        F.rpad(F.col("CaseMgmtData.STRT_DT_SK"),10," ").alias("STRT_DT_SK"),
        F.rpad(F.col("CaseMgmtData.STTUS_DT_SK"),10," ").alias("STTUS_DT_SK"),
        F.col("CaseMgmtData.MBR_AGE").alias("MBR_AGE"),
        F.col("CaseMgmtData.PRI_CNTCT_SEQ_NO").alias("PRI_CNTCT_SEQ_NO"),
        F.col("CaseMgmtData.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("CaseMgmtData.SUM_DESC").alias("SUM_DESC"),
        F.col("Cd_Mppng_lkup.CD_MPPNG_SK").alias("ENR_PROD_CD_MPPNG_SK"),
        F.col("Case_mgt_stts.TRGT_CD").alias("CASE_MGT_STTUS_CD"),
        F.col("cm_cmplx_lvl_cd.SRC_CD").alias("CASE_MGT_CMPLXTY_LVL_CD"),
        F.col("CaseMgmtData.ENR_SVRTY_CD").alias("ENR_SVRTY_CD"),
        F.col("cm_orig_typ_cd.TRGT_CD").alias("CASE_MGT_ORIG_TYP_CD"),
        F.col("CaseMgmtData.PGM_SRC_CD").alias("PGM_SRC_CD"),
        F.col("CaseMgmtData.PGM_ORIG_SRC_CD").alias("PGM_ORIG_SRC_CD"),
        F.col("Enr_pop_hlth_pgm_lkup.TRGT_DOMAIN_TX").alias("ENR_POP_HLTH_PGM_ID"),
        F.col("CaseMgmtData.CMPLXTY_SRC_CD").alias("CMPLXTY_SRC_CD"),
        F.col("CaseMgmtData.INDV_BE_KEY").alias("INDV_BE_KEY")
    )
)

# SRC_N_SVRTY_LKUP
df_src_n_svrty_lkup = (
    df_cd_lkup.alias("next_lkup")
    .join(
        df_ENR_SVRTY_CD.alias("lnk_EnrSvrtyCd_out"),
        (
            (F.col("next_lkup.ENR_SVRTY_CD") == F.col("lnk_EnrSvrtyCd_out.LKUP_VAL")) &
            (F.col("next_lkup.CASE_MGT_CMPLXTY_LVL_CD") == F.col("lnk_EnrSvrtyCd_out.SRC_CD")) &
            (F.col("lnkFKeyLkpDataOut.SRC_SYS_CD_SK") == F.col("lnk_EnrSvrtyCd_out.CD_MPPNG_SK"))
        ),
        "left"
    )
    .join(
        df_PGM_SRC_CD.alias("pgm_src_cd"),
        (
            (F.col("next_lkup.PGM_SRC_CD") == F.col("pgm_src_cd.PGM_SRC_CD")) &
            (F.col("next_lkup.CASE_MGT_ORIG_TYP_CD") == F.col("pgm_src_cd.SRC_CD")) &
            (F.col("lnkFKeyLkpDataOut.SRC_SYS_CD_SK") == F.col("pgm_src_cd.CD_MPPNG_SK"))
        ),
        "left"
    )
    .join(
        df_PGM_ORIG_SRC.alias("pgm_orig_src_cd"),
        (
            (F.col("next_lkup.PGM_ORIG_SRC_CD") == F.col("pgm_orig_src_cd.PGM_ORIG_SRC_CD")) &
            (F.col("lnkFKeyLkpDataOut.SRC_SYS_CD_SK") == F.col("pgm_orig_src_cd.CD_MPPNG_SK"))
        ),
        "left"
    )
    .join(
        df_CMPLXTY.alias("Cplx_cd_sk"),
        (
            (F.col("next_lkup.CMPLXTY_SRC_CD") == F.col("Cplx_cd_sk.CMPLXTY_SRC_CD")) &
            (F.col("next_lkup.CASE_MGT_CMPLXTY_LVL_CD") == F.col("Cplx_cd_sk.SRC_CD")) &
            (F.col("lnkFKeyLkpDataOut.SRC_SYS_CD_SK") == F.col("Cplx_cd_sk.CD_MPPNG_SK"))
        ),
        "left"
    )
    .select(
        F.col("next_lkup.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("next_lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("next_lkup.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("next_lkup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("next_lkup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("next_lkup.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("next_lkup.GRP_SK").alias("GRP_SK"),
        F.col("next_lkup.MBR_SK").alias("MBR_SK"),
        F.col("next_lkup.PRI_CASE_MGR_USER_SK").alias("PRI_CASE_MGR_USER_SK"),
        F.col("next_lkup.PROC_CD_SK").alias("PROC_CD_SK"),
        F.col("next_lkup.PROD_SK").alias("PROD_SK"),
        F.col("next_lkup.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("next_lkup.SUB_SK").alias("SUB_SK"),
        F.col("next_lkup.CASE_MGT_CLS_PLN_PRODCAT_CD_SK").alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
        F.col("next_lkup.CASE_MGT_CMPLXTY_LVL_CD_SK").alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
        F.col("next_lkup.CASE_MGT_ORIG_TYP_CD_SK").alias("CASE_MGT_ORIG_TYP_CD_SK"),
        F.col("next_lkup.CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
        F.col("next_lkup.CASE_MGT_TYP_CD_SK").alias("CASE_MGT_TYP_CD_SK"),
        F.rpad(F.col("next_lkup.END_DT_SK"),10," ").alias("END_DT_SK"),
        F.rpad(F.col("next_lkup.INPT_DT_SK"),10," ").alias("INPT_DT_SK"),
        F.rpad(F.col("next_lkup.PRI_CASE_MGR_NEXT_RVW_DT_SK"),10," ").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
        F.rpad(F.col("next_lkup.STRT_DT_SK"),10," ").alias("STRT_DT_SK"),
        F.rpad(F.col("next_lkup.STTUS_DT_SK"),10," ").alias("STTUS_DT_SK"),
        F.col("next_lkup.MBR_AGE").alias("MBR_AGE"),
        F.col("next_lkup.PRI_CNTCT_SEQ_NO").alias("PRI_CNTCT_SEQ_NO"),
        F.col("next_lkup.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("next_lkup.SUM_DESC").alias("SUM_DESC"),
        F.col("next_lkup.ENR_PROD_CD_MPPNG_SK").alias("ENR_PROD_CD_MPPNG_SK"),
        F.col("next_lkup.CASE_MGT_STTUS_CD").alias("CASE_MGT_STTUS_CD"),
        F.col("next_lkup.CASE_MGT_CMPLXTY_LVL_CD").alias("CASE_MGT_CMPLXTY_LVL_CD"),
        F.col("lnk_EnrSvrtyCd_out.CD_MPPNG_SK").alias("ENR_SVRTY_CD_SK"),
        F.col("pgm_src_cd.CD_MPPNG_SK").alias("PGM_SRC_CD_SK"),
        F.col("next_lkup.ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
        F.col("pgm_orig_src_cd.CD_MPPNG_SK").alias("PGM_ORIG_SRC_SYS_CD_SK"),
        F.col("next_lkup.CMPLXTY_SRC_CD").alias("CMPLXTY_SRC_CD"),
        F.col("Cplx_cd_sk.CD_MPPNG_SK").alias("CMPLXTY_SRC_CD_SK"),
        F.col("next_lkup.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("next_lkup.CASE_MGT_ORIG_TYP_CD").alias("CASE_MGT_ORIG_TYP_CD")
    )
)

# PREV_DATA_LKUP
df_prev_data_lkup = (
    df_src_n_svrty_lkup.alias("next_lkup2")
    .join(
        df_PREV_DATA.alias("lnk_Prev_dataLkup_out"),
        (
            (F.col("next_lkup2.CASE_MGT_ID") == F.col("lnk_Prev_dataLkup_out.POP_HLTH_PGM_ENR_ID")) &
            (F.col("next_lkup2.SRC_SYS_CD_SK") == F.col("lnk_Prev_dataLkup_out.SRC_SYS_CD_SK"))
        ),
        "left"
    )
    .join(
        df_APP_USER.alias("lnk_AppUserData_out"),
        F.col("next_lkup2.PRI_CASE_MGR_USER_SK") == F.col("lnk_AppUserData_out.USER_SK"),
        "left"
    )
    .select(
        F.col("next_lkup2.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("next_lkup2.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("next_lkup2.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("next_lkup2.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("next_lkup2.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("next_lkup2.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("next_lkup2.GRP_SK").alias("GRP_SK"),
        F.col("next_lkup2.MBR_SK").alias("MBR_SK"),
        F.col("next_lkup2.PRI_CASE_MGR_USER_SK").alias("PRI_CASE_MGR_USER_SK"),
        F.col("next_lkup2.PROC_CD_SK").alias("PROC_CD_SK"),
        F.col("next_lkup2.PROD_SK").alias("PROD_SK"),
        F.col("next_lkup2.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("next_lkup2.SUB_SK").alias("SUB_SK"),
        F.col("next_lkup2.CASE_MGT_CLS_PLN_PRODCAT_CD_SK").alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
        F.col("next_lkup2.CASE_MGT_CMPLXTY_LVL_CD_SK").alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
        F.col("next_lkup2.CASE_MGT_ORIG_TYP_CD_SK").alias("CASE_MGT_ORIG_TYP_CD_SK"),
        F.col("next_lkup2.CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
        F.col("next_lkup2.CASE_MGT_TYP_CD_SK").alias("CASE_MGT_TYP_CD_SK"),
        F.rpad(F.col("next_lkup2.END_DT_SK"),10," ").alias("END_DT_SK"),
        F.rpad(F.col("next_lkup2.INPT_DT_SK"),10," ").alias("INPT_DT_SK"),
        F.rpad(F.col("next_lkup2.PRI_CASE_MGR_NEXT_RVW_DT_SK"),10," ").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
        F.rpad(F.col("next_lkup2.STRT_DT_SK"),10," ").alias("STRT_DT_SK"),
        F.rpad(F.col("next_lkup2.STTUS_DT_SK"),10," ").alias("STTUS_DT_SK"),
        F.col("next_lkup2.MBR_AGE").alias("MBR_AGE"),
        F.col("next_lkup2.PRI_CNTCT_SEQ_NO").alias("PRI_CNTCT_SEQ_NO"),
        F.col("next_lkup2.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("next_lkup2.SUM_DESC").alias("SUM_DESC"),
        F.col("next_lkup2.ENR_PROD_CD_MPPNG_SK").alias("ENR_PROD_CD_MPPNG_SK"),
        F.col("next_lkup2.CASE_MGT_STTUS_CD").alias("CASE_MGT_STTUS_CD"),
        F.col("next_lkup2.CASE_MGT_CMPLXTY_LVL_CD").alias("CASE_MGT_CMPLXTY_LVL_CD"),
        F.col("lnk_Prev_dataLkup_out.SCRN_RQST_PROD_CD_SK").alias("PREV_SCRN_RQST_PROD_CD_SK"),
        F.col("lnk_Prev_dataLkup_out.SCRN_RQST_SVRTY_CD_SK").alias("PREV_SCRN_RQST_SVRTY_CD_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.PGM_RQST_DT_SK"),10," ").alias("PREV_PGM_RQST_DT_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.PGM_SCRN_DT_SK"),10," ").alias("PREV_PGM_SCRN_DT_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.ROW_TERM_DT_SK"),10," ").alias("PREV_ROW_TERM_DT_SK"),
        F.col("lnk_Prev_dataLkup_out.SCRN_BY_USER_ID").alias("PREV_SCRN_BY_USER_ID"),
        F.col("lnk_Prev_dataLkup_out.SCRN_ASG_TO_USER_ID").alias("PREV_SCRN_ASG_TO_USER_ID"),
        F.col("lnk_Prev_dataLkup_out.INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.ROW_EFF_DT_SK"),10," ").alias("PREV_ROW_EFF_DT_SK"),
        F.rpad(F.col("lnk_AppUserData_out.USER_ID"),10," ").alias("USER_ID"),
        F.col("next_lkup2.CMPLXTY_SRC_CD").alias("CMPLXTY_SRC_CD"),
        F.col("next_lkup2.CMPLXTY_SRC_CD_SK").alias("CMPLXTY_SRC_CD_SK"),
        F.col("next_lkup2.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("next_lkup2.CASE_MGT_ORIG_TYP_CD").alias("CASE_MGT_ORIG_TYP_CD"),
        F.col("lnk_Prev_dataLkup_out.ENR_PROD_CD_SK").alias("PREV_ENR_PROD_CD_SK"),
        F.col("lnk_Prev_dataLkup_out.ENR_SVRTY_CD_SK").alias("PREV_ENR_SVRTY_CD_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.PGM_ENR_CRT_DT_SK"),10," ").alias("PREV_PGM_ENR_CRT_DT_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.PGM_ENR_DT_SK"),10," ").alias("PREV_PGM_ENR_DT_SK"),
        F.col("lnk_Prev_dataLkup_out.ENRED_BY_USER_ID").alias("PREV_ENRED_BY_USER_ID"),
        F.col("lnk_Prev_dataLkup_out.ENR_ASG_TO_USER_ID").alias("PREV_ENR_ASG_TO_USER_ID"),
        F.col("lnk_Prev_dataLkup_out.ENR_POP_HLTH_PGM_ID").alias("PREV_ENR_POP_HLTH_PGM_ID"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.PGM_STRT_DT_SK"),10," ").alias("PREV_PGM_STRT_DT_SK"),
        F.rpad(F.col("lnk_Prev_dataLkup_out.PGM_CLOSE_DT_SK"),10," ").alias("PREV_PGM_CLOSE_DT_SK"),
        F.col("lnk_Prev_dataLkup_out.ENR_DENIED_RSN_CD_SK").alias("PREV_ENR_DENIED_RSN_CD_SK"),
        F.col("lnk_Prev_dataLkup_out.PGM_CLOSE_RSN_CD_SK").alias("PREV_PGM_CLOSE_RSN_CD_SK")
    )
)

# LKP_CMS
df_IhmPgmEnrTransExtr = (
    df_prev_data_lkup.alias("next_lkp_CMS")
    .join(
        df_CASE_MGT_STTUS.alias("Ref_CMS_SK"),
        F.col("next_lkp_CMS.CASE_MGT_SK") == F.col("Ref_CMS_SK.CASE_MGT_SK"),
        "left"
    )
    .select(
        F.col("next_lkp_CMS.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("next_lkp_CMS.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("next_lkp_CMS.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("next_lkp_CMS.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("next_lkp_CMS.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("next_lkp_CMS.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("next_lkp_CMS.GRP_SK").alias("GRP_SK"),
        F.col("next_lkp_CMS.MBR_SK").alias("MBR_SK"),
        F.col("next_lkp_CMS.PRI_CASE_MGR_USER_SK").alias("PRI_CASE_MGR_USER_SK"),
        F.col("next_lkp_CMS.PROC_CD_SK").alias("PROC_CD_SK"),
        F.col("next_lkp_CMS.PROD_SK").alias("PROD_SK"),
        F.col("next_lkp_CMS.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("next_lkp_CMS.SUB_SK").alias("SUB_SK"),
        F.col("next_lkp_CMS.CASE_MGT_CLS_PLN_PRODCAT_CD_SK").alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
        F.col("next_lkp_CMS.CASE_MGT_CMPLXTY_LVL_CD_SK").alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
        F.col("next_lkp_CMS.CASE_MGT_ORIG_TYP_CD_SK").alias("CASE_MGT_ORIG_TYP_CD_SK"),
        F.col("next_lkp_CMS.CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
        F.col("next_lkp_CMS.CASE_MGT_TYP_CD_SK").alias("CASE_MGT_TYP_CD_SK"),
        F.rpad(F.col("next_lkp_CMS.END_DT_SK"),10," ").alias("END_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.INPT_DT_SK"),10," ").alias("INPT_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.PRI_CASE_MGR_NEXT_RVW_DT_SK"),10," ").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.STRT_DT_SK"),10," ").alias("STRT_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.STTUS_DT_SK"),10," ").alias("STTUS_DT_SK"),
        F.col("next_lkp_CMS.MBR_AGE").alias("MBR_AGE"),
        F.col("next_lkp_CMS.PRI_CNTCT_SEQ_NO").alias("PRI_CNTCT_SEQ_NO"),
        F.col("next_lkp_CMS.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("next_lkp_CMS.SUM_DESC").alias("SUM_DESC"),
        F.col("next_lkp_CMS.ENR_PROD_CD_MPPNG_SK").alias("ENR_PROD_CD_MPPNG_SK"),
        F.col("next_lkp_CMS.CASE_MGT_STTUS_CD").alias("CASE_MGT_STTUS_CD"),
        F.col("next_lkp_CMS.CASE_MGT_CMPLXTY_LVL_CD").alias("CASE_MGT_CMPLXTY_LVL_CD"),
        F.col("next_lkp_CMS.ENR_SVRTY_CD_SK").alias("ENR_SVRTY_CD_SK"),
        F.col("next_lkp_CMS.PGM_SRC_CD_SK").alias("PGM_SRC_CD_SK"),
        F.col("next_lkp_CMS.ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
        F.col("next_lkp_CMS.PGM_ORIG_SRC_SYS_CD_SK").alias("PGM_ORIG_SRC_SYS_CD_SK"),
        F.col("next_lkp_CMS.PREV_SCRN_RQST_PROD_CD_SK").alias("PREV_SCRN_RQST_PROD_CD_SK"),
        F.col("next_lkp_CMS.PREV_SCRN_RQST_SVRTY_CD_SK").alias("PREV_SCRN_RQST_SVRTY_CD_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_PGM_RQST_DT_SK"),10," ").alias("PREV_PGM_RQST_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_PGM_SCRN_DT_SK"),10," ").alias("PREV_PGM_SCRN_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_ROW_TERM_DT_SK"),10," ").alias("PREV_ROW_TERM_DT_SK"),
        F.col("next_lkp_CMS.PREV_SCRN_BY_USER_ID").alias("PREV_SCRN_BY_USER_ID"),
        F.col("next_lkp_CMS.PREV_SCRN_ASG_TO_USER_ID").alias("PREV_SCRN_ASG_TO_USER_ID"),
        F.col("next_lkp_CMS.PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_ROW_EFF_DT_SK"),10," ").alias("PREV_ROW_EFF_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.USER_ID"),10," ").alias("USER_ID"),
        F.col("next_lkp_CMS.CMPLXTY_SRC_CD").alias("CMPLXTY_SRC_CD"),
        F.col("next_lkp_CMS.CMPLXTY_SRC_CD_SK").alias("CMPLXTY_SRC_CD_SK"),
        F.col("next_lkp_CMS.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("next_lkp_CMS.CASE_MGT_ORIG_TYP_CD").alias("CASE_MGT_ORIG_TYP_CD"),
        F.col("next_lkp_CMS.PREV_ENR_PROD_CD_SK").alias("PREV_ENR_PROD_CD_SK"),
        F.col("next_lkp_CMS.PREV_ENR_SVRTY_CD_SK").alias("PREV_ENR_SVRTY_CD_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_PGM_ENR_CRT_DT_SK"),10," ").alias("PREV_PGM_ENR_CRT_DT_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_PGM_ENR_DT_SK"),10," ").alias("PREV_PGM_ENR_DT_SK"),
        F.col("next_lkp_CMS.PREV_ENRED_BY_USER_ID").alias("PREV_ENRED_BY_USER_ID"),
        F.col("next_lkp_CMS.PREV_ENR_ASG_TO_USER_ID").alias("PREV_ENR_ASG_TO_USER_ID"),
        F.col("next_lkp_CMS.PREV_ENR_POP_HLTH_PGM_ID").alias("PREV_ENR_POP_HLTH_PGM_ID"),
        F.rpad(F.col("next_lkp_CMS.PREV_PGM_STRT_DT_SK"),10," ").alias("PREV_PGM_STRT_DT_SK"),
        F.col("Ref_CMS_SK.CASE_MGT_STTUS_RSN_CD_SK").alias("CASE_MGT_STTUS_RSN_CD_SK"),
        F.rpad(F.col("next_lkp_CMS.PREV_PGM_CLOSE_DT_SK"),10," ").alias("PREV_PGM_CLOSE_DT_SK"),
        F.col("next_lkp_CMS.PREV_ENR_DENIED_RSN_CD_SK").alias("PREV_ENR_DENIED_RSN_CD_SK"),
        F.col("next_lkp_CMS.PREV_PGM_CLOSE_RSN_CD_SK").alias("PREV_PGM_CLOSE_RSN_CD_SK")
    )
)

# Transformer (Tx)
df_tx = df_IhmPgmEnrTransExtr \
    .withColumn(
        "svPgmclsddt",
        F.when(
            (
                (
                    F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNull() |
                    (
                        F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNotNull() &
                        (F.col("PREV_PGM_CLOSE_RSN_CD_SK") == '1')
                    )
                ) &
                (F.col("CASE_MGT_STTUS_CD") == 'CLSD')
            ),
            F.col("END_DT_SK")
        )
        .when(
            (
                (
                    F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNull() |
                    (
                        F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNotNull() &
                        (F.col("PREV_ENR_DENIED_RSN_CD_SK") == '1')
                    )
                ) &
                (F.col("CASE_MGT_STTUS_CD") == 'DENY')
            ),
            current_date()
        )
        .when(
            (
                F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNull() &
                (
                    (F.col("CASE_MGT_STTUS_CD") != 'CLSD') |
                    (F.col("CASE_MGT_STTUS_CD") != 'DENY')
                )
            ),
            F.lit("2199-12-31")
        )
        .otherwise(F.col("PREV_PGM_CLOSE_DT_SK"))
    )
    .withColumn(
        "svPgmstendt",
        F.when(
            F.col("CASE_MGT_STTUS_CD") == 'PTNTLCASEMGT',
            F.lit("1753-01-01")
        )
        .when(
            (
                (F.col("CASE_MGT_STTUS_CD") != 'PTNTLCASEMGT') &
                F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNotNull() &
                (F.col("PREV_ENR_POP_HLTH_PGM_ID") == F.col("ENR_POP_HLTH_PGM_ID")) &
                (F.col("PREV_PGM_STRT_DT_SK") != '1753-01-01')
            ),
            F.col("PREV_PGM_ENR_DT_SK")
        )
        .otherwise(F.col("STTUS_DT_SK"))
    )

# Output pin: Prev_Data_Update_load
df_Prev_Data_Update_load = df_tx.filter(
    F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").isNotNull() &
    F.col("ENR_PROD_CD_MPPNG_SK").isNotNull() &
    F.col("CASE_MGT_STTUS_CD").isNotNull() &
    F.col("CASE_MGT_CMPLXTY_LVL_CD").isNotNull() &
    F.col("ENR_POP_HLTH_PGM_ID").isNotNull() &
    F.col("PGM_SRC_CD_SK").isNotNull() &
    F.col("PGM_ORIG_SRC_SYS_CD_SK").isNotNull() &
    F.col("CMPLXTY_SRC_CD_SK").isNotNull()
).select(
    F.col("PREV_INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrDateMinOne).alias("ROW_TERM_DT_SK")
)

# Output pin: Final_data_Pkey
df_Final_data_Pkey = df_tx.filter(
    F.col("ENR_PROD_CD_MPPNG_SK").isNotNull() &
    F.col("CASE_MGT_STTUS_CD").isNotNull() &
    F.col("CASE_MGT_CMPLXTY_LVL_CD").isNotNull() &
    F.col("ENR_POP_HLTH_PGM_ID").isNotNull() &
    F.col("PGM_SRC_CD_SK").isNotNull() &
    F.col("PGM_ORIG_SRC_SYS_CD_SK").isNotNull() &
    F.col("CMPLXTY_SRC_CD_SK").isNotNull()
).select(
    F.col("CASE_MGT_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.rpad(F.lit(CurrDate),10," ").alias("ROW_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'DENY',
        F.col("CASE_MGT_STTUS_RSN_CD_SK")
    ).otherwise(F.lit("1")).alias("ENR_DENIED_RSN_CD_SK"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.lit("1")
    ).otherwise(
        F.when(
            (trim(F.col("CASE_MGT_STTUS_CD")) != 'PTNTLCASEMGT') & 
            F.col("PREV_ENR_PROD_CD_SK").isNotNull(),
            F.col("PREV_ENR_PROD_CD_SK")
        ).otherwise(F.col("ENR_PROD_CD_MPPNG_SK"))
    ).alias("ENR_PROD_CD_SK"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.lit("1")
    ).otherwise(
        F.when(
            (trim(F.col("CASE_MGT_STTUS_CD")) != 'PTNTLCASEMGT') &
            F.col("PREV_ENR_SVRTY_CD_SK").isNotNull(),
            F.col("PREV_ENR_SVRTY_CD_SK")
        ).otherwise(F.col("ENR_SVRTY_CD_SK"))
    ).alias("ENR_SVRTY_CD_SK"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'CLSD',
        F.col("CASE_MGT_STTUS_RSN_CD_SK")
    ).otherwise(F.lit("1")).alias("PGM_CLOSE_RSN_CD_SK"),
    F.lit("1").alias("PGM_SCRN_STTUS_CD_SK"),
    F.col("PGM_SRC_CD_SK").alias("PGM_SRC_CD_SK"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.col("ENR_PROD_CD_MPPNG_SK")
    ).otherwise(
        F.when(
            F.col("PREV_SCRN_RQST_PROD_CD_SK").isNotNull(),
            F.col("PREV_SCRN_RQST_PROD_CD_SK")
        ).otherwise(F.col("ENR_PROD_CD_MPPNG_SK"))
    ).alias("SCRN_RQST_PROD_CD_SK"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.col("CMPLXTY_SRC_CD_SK")
    ).otherwise(
        F.when(
            F.col("PREV_SCRN_RQST_SVRTY_CD_SK").isNotNull(),
            F.col("PREV_SCRN_RQST_SVRTY_CD_SK")
        ).otherwise(F.col("CMPLXTY_SRC_CD_SK"))
    ).alias("SCRN_RQST_SVRTY_CD_SK"),
    F.rpad(F.col("svPgmclsddt"),10," ").alias("PGM_CLOSE_DT_SK"),
    F.rpad(
        F.when(
            trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
            F.lit("1753-01-01")
        ).otherwise(
            F.when(
                (trim(F.col("CASE_MGT_STTUS_CD")) != 'PTNTLCASEMGT') &
                F.col("PREV_PGM_ENR_CRT_DT_SK").isNotNull() &
                (trim(F.col("PREV_PGM_ENR_CRT_DT_SK")) != '1753-01-01'),
                F.col("PREV_PGM_ENR_CRT_DT_SK")
            ).otherwise(F.col("STTUS_DT_SK"))
        ),
        10, " "
    ).alias("PGM_ENR_CRT_DT_SK"),
    F.rpad(F.col("svPgmstendt"),10," ").alias("PGM_ENR_DT_SK"),
    F.rpad(F.col("INPT_DT_SK"),10," ").alias("PGM_RQST_DT_SK"),
    F.rpad(F.col("INPT_DT_SK"),10," ").alias("PGM_SCRN_DT_SK"),
    F.rpad(F.col("svPgmstendt"),10," ").alias("PGM_STRT_DT_SK"),
    F.rpad(F.lit("2199-12-31"),10," ").alias("ROW_TERM_DT_SK"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.lit("NA")
    ).otherwise(
        F.when(
            (trim(F.col("CASE_MGT_STTUS_CD")) != 'PTNTLCASEMGT') &
            F.col("PREV_ENRED_BY_USER_ID").isNotNull() &
            (trim(F.col("PREV_ENRED_BY_USER_ID")) != 'NA'),
            F.col("PREV_ENRED_BY_USER_ID")
        ).otherwise(F.col("USER_ID"))
    ).alias("ENRED_BY_USER_ID"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.lit("NA")
    ).otherwise(
        F.when(
            (trim(F.col("CASE_MGT_STTUS_CD")) != 'PTNTLCASEMGT') &
            F.col("PREV_ENR_ASG_TO_USER_ID").isNotNull() &
            (trim(F.col("PREV_ENR_ASG_TO_USER_ID")) != 'NA'),
            F.col("PREV_ENR_ASG_TO_USER_ID")
        ).otherwise(F.col("USER_ID"))
    ).alias("ENR_ASG_TO_USER_ID"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.lit("NA")
    ).otherwise(
        F.when(
            (trim(F.col("CASE_MGT_STTUS_CD")) != 'PTNTLCASEMGT') &
            F.col("PREV_ENR_POP_HLTH_PGM_ID").isNotNull() &
            (trim(F.col("PREV_ENR_POP_HLTH_PGM_ID")) != 'NA'),
            F.col("PREV_ENR_POP_HLTH_PGM_ID")
        ).otherwise(trim(F.col("ENR_POP_HLTH_PGM_ID")))
    ).alias("ENR_POP_HLTH_PGM_ID"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.col("USER_ID")
    ).otherwise(
        F.when(
            F.col("PREV_SCRN_BY_USER_ID").isNotNull(),
            F.col("PREV_SCRN_BY_USER_ID")
        ).otherwise(F.col("USER_ID"))
    ).alias("SCRN_BY_USER_ID"),
    F.when(
        trim(F.col("CASE_MGT_STTUS_CD")) == 'PTNTLCASEMGT',
        F.col("USER_ID")
    ).otherwise(
        F.when(
            F.col("PREV_SCRN_ASG_TO_USER_ID").isNotNull(),
            F.col("PREV_SCRN_ASG_TO_USER_ID")
        ).otherwise(F.col("USER_ID"))
    ).alias("SCRN_ASG_TO_USER_ID"),
    F.col("ENR_POP_HLTH_PGM_ID").alias("SCRN_POP_HLTH_PGM_ID"),
    F.col("PGM_ORIG_SRC_SYS_CD_SK").alias("PGM_ORIG_SRC_SYS_CD_SK")
)

# Output pin: Lkup_Fail_Data
df_Lkup_Fail_Data = df_tx.filter(
    F.col("ENR_PROD_CD_MPPNG_SK").isNull() |
    F.col("CASE_MGT_STTUS_CD").isNull() |
    F.col("CASE_MGT_CMPLXTY_LVL_CD").isNull() |
    F.col("ENR_POP_HLTH_PGM_ID").isNull() |
    F.col("PGM_SRC_CD_SK").isNull() |
    F.col("PGM_ORIG_SRC_SYS_CD_SK").isNull() |
    F.col("CMPLXTY_SRC_CD_SK").isNull()
).select(
    F.col("CASE_MGT_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.rpad(F.lit(CurrDate),10," ").alias("ROW_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ENR_PROD_CD_MPPNG_SK").alias("ENR_PROD_CD_SK"),
    F.col("CASE_MGT_STTUS_CD").alias("CASE_MGT_STTUS_CD"),
    F.col("CASE_MGT_CMPLXTY_LVL_CD").alias("CASE_MGT_CMPLXTY_LVL_CD"),
    F.col("ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
    F.col("PGM_SRC_CD_SK").alias("PGM_SRC_CD_SK"),
    F.col("PGM_ORIG_SRC_SYS_CD_SK").alias("PGM_ORIG_SRC_SYS_CD_SK"),
    F.col("CMPLXTY_SRC_CD_SK").alias("SCRN_RQST_SVRTY_CD_SK")
)

# Update_Old_Records (PxSequentialFile)
file_path_update_old = f"{adls_path}/load/INDV_BE_POP_HLTH_PGM_ENR_TRNS.{SrcSysCd}.UPDATE.{RunID}.dat"
write_files(
    df_Prev_Data_Update_load,
    file_path_update_old,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Pkey_File (PxSequentialFile)
file_path_pkey = f"{adls_path_publish}/external/INDV_BE_POP_HLTH_PGM_ENR_TRNS.{SrcSysCd}.extr.{RunID}.dat"
write_files(
    df_Final_data_Pkey,
    file_path_pkey,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# LKUP_FAIL_RECORDS (PxSequentialFile)
file_path_fail = f"{adls_path_publish}/external/INDV_BE_POP_HLTH_PGM_ENR_TRNS.REJECTS.dat"
write_files(
    df_Lkup_Fail_Data,
    file_path_fail,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)