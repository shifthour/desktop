# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsBlueKCHstdMbrAttributionExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   IDS -> IDS (Hosted members who were active during last month and their claims for a rolling 0-12, 13-24 months are extracted and the most frequently/recently visited provider is attributed to the member)
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     EDW Tables                                     Flat Files
# MAGIC                    CLM                                         PCMH_BCBSKC_HSTD_MBRS_BEKEY_12ClmDetails.dat.#RunID#
# MAGIC                    CLM_LN                                  PCMH_BCBSKC_HSTD_MBRS_BEKEY_24ClmDetails.dat.#RunID#    
# MAGIC                    CLM_EXTRNL_MBRSH                                            
# MAGIC                    P_MBR_BCBSA_SUPLMT            
# MAGIC                    MBR                 
# MAGIC                    MBR_ENR                
# MAGIC                    CD_MPPNG             
# MAGIC                    PROV   
# MAGIC 
# MAGIC HASH FILES:   hf_prov_svc_and_prov_grp_lkup
# MAGIC                         hf_prov_svc_and_prov_grp_12mnth_lkup
# MAGIC                         hf_mbr_attrbtn_proc_cd_lkup
# MAGIC                         hf_mbr_attrbtn_clm_pos_cd_lkup
# MAGIC                         hf_mbr_attrbtn_prov_spec_cd_lkup
# MAGIC                         hf_mbr_attrbtn_provgrp_spec_cd_lkup
# MAGIC                         hf_dedupe_12hstdclms_seq_no
# MAGIC                         hf_dedupe_24hstdclms_seq_no
# MAGIC                       
# MAGIC 
# MAGIC TRANSFORMS:   FORMAT.DATE, LOOK UP, TRIM, CONSTRAINTS
# MAGIC                              
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and load to Sequential File.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential files
# MAGIC                       PCMH_BCBSKC_HSTD_MBRS_BEKEY_12ClmDetails.dat.#RunID#
# MAGIC                       PCMH_BCBSKC_HSTD_MBRS_BEKEY_24ClmDetails.dat.#RunID#
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =======================================================================================================================================================================================================
# MAGIC Developer                          Date                          Project/Altiris #                                                    Change Description                                                                                                      Development Project               Code Reviewer            Date Reviewed
# MAGIC =======================================================================================================================================================================================================
# MAGIC Manasa Andru              2016-05-25          30001(Data Catalyst-Mbr Attribution)                             Original Programming                                                                                                         IntegrateDev2                     Jag Yelavarthi                2016-06-02
# MAGIC 
# MAGIC Manasa Andru              2016-07-21          30001 Mbr Attribution Phase1                                  Added a new field - ALW_AMT                                                                                              IntegrateDev2                    Jag Yelavarthi                 2016-07-25
# MAGIC                                                                                                                                                  to the extract SQL and to the file. 
# MAGIC Sharon Andrew            2016-11-21          Production Abend                                                      Removed BCBSA Source System Code in both IDS_CLM_12 and IDS_CLM_24                Integrate
# MAGIC                                                                                                                                                  Changed     
# MAGIC                                                                                                                                                 PROD_CLM.SRC_SYS_CD_SK = CD6.CD_MPPNG_SK AND ((PROD_CLM.PD_DT_SK <> 'NA' AND CD6.TRGT_CD = 'FACETS') OR (CD6.TRGT_CD = 'BCBSA'))
# MAGIC                                                                                                                                                 to    PROD_CLM.SRC_SYS_CD_SK = CD6.CD_MPPNG_SK AND CD6.TRGT_CD = 'FACETS'        AND PROD_CLM.PD_DT_SK <> 'NA'                                                                                                                                          
# MAGIC 
# MAGIC Ravi Abburi              2017-02-21          30001 Mbr Attribution Phase1                                  Removed unused tables GRP PROD_GRP,CD_MPPNG PROD_PROV_SVC_CD,                    IntegrateDev2                    Kalyan Neelam                2017-03-01
# MAGIC                                                                                                                                               CD_MPPNG PROD_PROV_GRP_CD from the  extract sql                                                                                                   
# MAGIC                                                                                                                                                 and also corrected the filter conditions as 
# MAGIC                                                                                                                                                PROD_MBR.MBR_INDV_BE_KEY = PROD_MBR2.MBR_INDV_BE_KEY
# MAGIC                                                                                                                                               AND  PROD_MBR2.MBR_SK =  MbrEnr.MBR_SK in 12 & 24 Months extracts.
# MAGIC . 
# MAGIC Ravi Abburi              2017-04-12          Break/Fix TFS ticket 19018                                   Added bewlow fiter conditon to match the data types  in 12 & 24 Months extracts                       IntegrateDev2                      Kalyan Neelam               2017-04-14
# MAGIC                                                                                                                                           CAST(P_MBR_BCBSA_SUPLMT.BCBSA_MBR_BRTH_DT as CHAR(10)) =  
# MAGIC                                                                                                                                                                            CLM_EXTRNL_MBRSH.MBR_BRTH_DT_SK 
# MAGIC 
# MAGIC Krishnakanth             2017-11-06          30001 Data Catalyst                                             Removed all Prod_Sh_Nm and kept PCBEXTRNL for the following stages.                                  IntegrateDev2                      Kalyan Neelam               2017-11-08 
# MAGIC   Manivannan                                                                                                                     IDS_CLM_12 and IDS_CLM_24
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru        2019-03-21         60037 Attribution/Capitation                   Modified extract SQL by adding Substr  to the BCBSA_ITS_SUB_ID in the                                                  IntegrateDev2		Jaideep Mankala          04/05/2019
# MAGIC                                                                           Support                                                                 IDS_CLM_12 and IDS_CLM_24 stages/.
# MAGIC 
# MAGIC Manasa Andru       2020-30-26         US # 144892                                               Updated IDS_CLM 12 and 24 stages with new SQL with Rank 2 which consists of                                       IntegrateDev2                       Jaideep Mankala         03/31/2020
# MAGIC                                                                                                                                                all the possible claims from Payment Innovations process. Added sort stages.
# MAGIC 
# MAGIC Harikanth Reddy   2020-12-03          US # 307844                                                      Applied filter critera to the extract sql to update the logic to include                                                           Integrate Dev2                 Jeyaprasanna                  2020-12-09
# MAGIC Kotha Venkat                                                                                                                                  HPN products: 'HP', 'HPEXTRNL'     
# MAGIC 
# MAGIC Manasa Andru        2020-01-08          US - 292445                                                Added svProvNpPaValueValid and svProcCodePosTeleHealthValueValid                                             IntegrateDev2                Jaideep Mankala            01/22/2021
# MAGIC                                                                                                                                                   Stage variables and updated the constraint in the transformer stages.      
# MAGIC 
# MAGIC Manasa Andru        2021-05-26         US - 369019                                                Added filter on BCBSA_ITS_SUB_ID field in the extract SQL in db2 stages to                                              IntegrateDev2                Jaideep Mankala             05/28/2021
# MAGIC                                                                                                                                                exclude the FEP members.                                                   
# MAGIC 
# MAGIC Bhanu S             2021-07-26         US-407875                                                 Replaced SQL in the 2 Extract stages from: AND           
# MAGIC                                                                                           SUBSTRING(P_MBR_BCBSA_SUPLMT.BCBSA_ITS_SUB_ID, 1, 2) NOT IN ('R0','R1','R2','R3','R4','R5','R6','R7','R8','R9')        IntegrateDev2                Goutham K                       07026-2021
# MAGIC                                                                                             TO: AND  P_MBR_BCBSA_SUPLMT.BCBSA_HOME_PLN_CORP_PLN_CD <> 'FEP'  
# MAGIC 
# MAGIC Manasa Andru       2022-01-19          US - 237614                        Updated the Stage variables svProcCodeValueValid and                                                                                                         IntegrateDev2             Goutham K                    02-03-2022
# MAGIC                                                                                                                   svProcCodePosTeleHealthValueValid in BekeyClm12 and BekeyCLM24 
# MAGIC                                                                                                                                    and MbrSkClm12 stages.
# MAGIC                                                                                                        Updated Stage Variable svProvSpecCdValueValid

# MAGIC Service Provider and Provider Group whose Term_Dt > LastDay of prior month
# MAGIC These hashed files are from IdsBlueKCMbrAttributionExtr job and are cleared in this job
# MAGIC New SQL is added to the DB2 Stage and ranking is given in the reverse priority as dupes are removed in hf
# MAGIC Claims of hosted members whose enrollment is active during last month and Claims service start date is within enrolling 12 month window
# MAGIC Claims of hosted members whose enrollment is active during last month and Claims service start date is within enrolling 13-24 month window
# MAGIC Service Provider and Provider Group whose Term_Dt > LastDay 12 months ago
# MAGIC The step to move the output files to the external/processed directory is in the Control job
# MAGIC Claims of hosted members whose enrollment is active during last month and Claims service start date is within enrolling 12 month window
# MAGIC Claims of hosted members whose enrollment is active during last month and Claims service start date is within enrolling 13-24 month window
# MAGIC New SQL is added to the DB2 Stage and ranking is given in the reverse priority as dupes are removed in hf
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, CharType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
FirstDay24MonthsAgo = get_widget_value('FirstDay24MonthsAgo','')
LastDay24MonthsAgo = get_widget_value('LastDay24MonthsAgo','')
FirstDay12MonthsAgo = get_widget_value('FirstDay12MonthsAgo','')
LastDay12MonthsAgo = get_widget_value('LastDay12MonthsAgo','')
FirstDayPreviousMonth = get_widget_value('FirstDayPreviousMonth','')
LastDayPreviousMonth = get_widget_value('LastDayPreviousMonth','')
CurrentDate = get_widget_value('CurrentDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')

# Read hashed-file lookups (Scenario C) as parquet
df_hf_ProvGrp_Spec_Cd = spark.read.parquet(f"{adls_path}/hf_mbr_attrbtn_provgrp_spec_cd_lkup.parquet")
df_hf_Prov_Spec_Cd = spark.read.parquet(f"{adls_path}/hf_mbr_attrbtn_prov_spec_cd_lkup.parquet")
df_hf_Clm_Pos_Cd = spark.read.parquet(f"{adls_path}/hf_mbr_attrbtn_clm_pos_cd_lkup.parquet")
df_hf_Proc_Cd = spark.read.parquet(f"{adls_path}/hf_mbr_attrbtn_proc_cd_lkup.parquet")
df_hf_prov_np_pa_lkup = spark.read.parquet(f"{adls_path}/hf_mbr_attrbtn_prov_np_pa_lkup.parquet")

# Read from IDS_PROV (DB2Connector), deduplicate for hf_Prov (Scenario A)
sql_IDS_PROV = f"""
SELECT
  PROVSVC.PROV_SK,
  PROVSVC.PROV_ID,
  CD1.TRGT_CD,
  PROVSVC.TERM_DT_SK,
  PROVSVC.PROV_NM,
  PROVGRP.PROV_SK,
  PROVGRP.PROV_ID,
  CD2.TRGT_CD,
  PROVGRP.TERM_DT_SK,
  PROVGRP.PROV_NM
FROM
  {IDSOwner}.PROV PROVSVC,
  {IDSOwner}.PROV PROVGRP,
  {IDSOwner}.CD_MPPNG CD1,
  {IDSOwner}.CD_MPPNG CD2
WHERE
  PROVSVC.PROV_SPEC_CD_SK = CD1.CD_MPPNG_SK
  AND PROVGRP.PROV_SPEC_CD_SK = CD2.CD_MPPNG_SK
  AND PROVSVC.REL_GRP_PROV_SK = PROVGRP.PROV_SK
  AND (
       (
         (PROVGRP.PROV_SK = 1 OR PROVGRP.PROV_SK = 0)
         AND (
              PROVGRP.TERM_DT_SK >= '{LastDayPreviousMonth}'
              OR PROVSVC.TERM_DT_SK = '1753-01-01'
              OR PROVSVC.TERM_DT_SK = 'NA'
             )
       )
       OR (PROVGRP.PROV_SK <> 1 AND PROVGRP.PROV_SK <> 0)
      )
"""
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
df_IDS_PROV_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", sql_IDS_PROV)
    .load()
)

df_hf_Prov = (
    df_IDS_PROV_raw.select(
        F.col("PROVSVC.PROV_SK").alias("SVC_PROV_SK"),
        F.col("PROVSVC.PROV_ID").alias("SVC_PROV_ID"),
        F.col("CD1.TRGT_CD").alias("SVC_PROV_SPEC_CD"),
        F.col("PROVSVC.TERM_DT_SK").alias("SVC_TERM_DT_SK"),
        F.col("PROVSVC.PROV_NM").alias("SVC_PROV_NM"),
        F.col("PROVGRP.PROV_SK").alias("PROV_REL_GRP_PROV_SK"),
        F.col("PROVGRP.PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
        F.col("CD2.TRGT_CD").alias("PROV_REL_GRP_PROV_SPEC_CD"),
        F.col("PROVGRP.TERM_DT_SK").alias("PROV_REL_GRP_TERM_DT_SK"),
        F.col("PROVGRP.PROV_NM").alias("PROV_REL_GRP_PROV_NM")
    )
    .dropDuplicates(["SVC_PROV_SK"])
)

# Read from IDS_PROV_Table (DB2Connector), deduplicate for hf_Prov12 (Scenario A)
sql_IDS_PROV_Table = f"""
SELECT
  PROVSVC.PROV_SK,
  PROVSVC.PROV_ID,
  CD1.TRGT_CD,
  PROVSVC.TERM_DT_SK,
  PROVSVC.PROV_NM,
  PROVGRP.PROV_SK,
  PROVGRP.PROV_ID,
  CD2.TRGT_CD,
  PROVGRP.TERM_DT_SK,
  PROVGRP.PROV_NM
FROM
  {IDSOwner}.PROV PROVSVC,
  {IDSOwner}.PROV PROVGRP,
  {IDSOwner}.CD_MPPNG CD1,
  {IDSOwner}.CD_MPPNG CD2
WHERE
  PROVSVC.PROV_SPEC_CD_SK = CD1.CD_MPPNG_SK
  AND PROVGRP.PROV_SPEC_CD_SK = CD2.CD_MPPNG_SK
  AND PROVSVC.REL_GRP_PROV_SK = PROVGRP.PROV_SK
  AND (
       (
         (PROVGRP.PROV_SK = 1 OR PROVGRP.PROV_SK = 0)
         AND (
              PROVGRP.TERM_DT_SK >= '{LastDayPreviousMonth}'
              OR PROVSVC.TERM_DT_SK = '1753-01-01'
              OR PROVSVC.TERM_DT_SK = 'NA'
             )
       )
       OR (PROVGRP.PROV_SK <> 1 AND PROVGRP.PROV_SK <> 0)
      )
  AND (
       (
         PROVSVC.TERM_DT_SK >= '{FirstDay12MonthsAgo}'
         OR PROVSVC.TERM_DT_SK = '1753-01-01'
         OR PROVSVC.TERM_DT_SK = 'NA'
       )
       OR (
         (PROVGRP.PROV_SK <> 1 AND PROVGRP.PROV_SK <> 0)
         AND (
           PROVGRP.TERM_DT_SK >= '{FirstDay12MonthsAgo}'
           OR PROVGRP.TERM_DT_SK = '1753-01-01'
           OR PROVGRP.TERM_DT_SK = 'NA'
         )
       )
       OR (PROVGRP.PROV_SK <> 1 AND PROVGRP.PROV_SK <> 0)
      )
"""
df_IDS_PROV_Table_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", sql_IDS_PROV_Table)
    .load()
)

df_hf_Prov12 = (
    df_IDS_PROV_Table_raw.select(
        F.col("PROVSVC.PROV_SK").alias("SVC_PROV_SK"),
        F.col("PROVSVC.PROV_ID").alias("SVC_PROV_ID"),
        F.col("CD1.TRGT_CD").alias("SVC_PROV_SPEC_CD"),
        F.col("PROVSVC.TERM_DT_SK").alias("SVC_TERM_DT_SK"),
        F.col("PROVSVC.PROV_NM").alias("SVC_PROV_NM"),
        F.col("PROVGRP.PROV_SK").alias("PROV_REL_GRP_PROV_SK"),
        F.col("PROVGRP.PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
        F.col("CD2.TRGT_CD").alias("PROV_REL_GRP_PROV_SPEC_CD"),
        F.col("PROVGRP.TERM_DT_SK").alias("PROV_REL_GRP_TERM_DT_SK"),
        F.col("PROVGRP.PROV_NM").alias("PROV_REL_GRP_PROV_NM")
    )
    .dropDuplicates(["SVC_PROV_SK"])
)

# Read from MbrSK12 => deduplicate => hf_host_new_mbrsk_12_dedupe_data => becomes df_ExtrClaims12
sql_MbrSK12 = f"""
SELECT DISTINCT
    CLM.SRC_SYS_CD_SK,
    CLM_LN.CLM_LN_SK,
    2 as RANK,
    CLM_EXTRNL_MBRSH.CLM_ID,
    CLM_LN.CLM_LN_SEQ_NO,
    CLM_LN.PROC_CD_SK,
    CLM.HOST_IN,
    CLM.SVC_STRT_DT_SK,
    CLM_EXTRNL_MBRSH.SUBMT_SUB_ID,
    CLM.CLM_SUBTYP_CD_SK,
    CLM.PD_DT_SK,
    MBR.MBR_SK AS MBR_MBR_SK,
    MBR.INDV_BE_KEY AS MBR_INDV_BE_KEY,
    MBR.FIRST_NM,
    MBR.LAST_NM,
    MBR.MBR_UNIQ_KEY AS MBR1_MBR_UNIQ_KEY,
    MBR.MBR_SK AS SUPLMNT_MBR_SK,
    MBR.MBR_UNIQ_KEY AS SUPLMNT_MBR_UNIQ_KEY,
    MBR.FIRST_NM,
    MBR.MIDINIT,
    CLM_LN_CD.TRGT_CD AS CLM_LN_TRGT_CD,
    CLM_LN_POS_CD.TRGT_CD AS CLM_LN_POS_TRGT_CD,
    CLM_LN.SVC_PROV_SK,
    CLM.ALW_AMT
FROM
    {IDSOwner}.CLM CLM
    INNER JOIN {IDSOwner}.CLM_LN CLM_LN
      ON CLM.CLM_SK = CLM_LN.CLM_SK
    INNER JOIN {IDSOwner}.CLM_EXTRNL_MBRSH CLM_EXTRNL_MBRSH
      ON CLM_EXTRNL_MBRSH.CLM_SK = CLM.CLM_SK
    INNER JOIN {IDSOwner}.MBR MBR
      ON CLM_EXTRNL_MBRSH.MBR_SK = MBR.MBR_SK
    INNER JOIN {IDSOwner}.MBR MBR2
      ON MBR.INDV_BE_KEY = MBR2.INDV_BE_KEY
    INNER JOIN {IDSOwner}.MBR_ENR MbrEnr
      ON MBR2.MBR_SK = MbrEnr.MBR_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD6
      ON CLM.SRC_SYS_CD_SK = CD6.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD1
      ON CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD2
      ON CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD3
      ON CLM.CLM_TYP_CD_SK = CD3.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD4
      ON CLM.CLM_CAT_CD_SK = CD4.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD5
      ON CLM.CLM_FINL_DISP_CD_SK = CD5.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD7
      ON CLM_LN.CLM_LN_POS_CD_SK = CD7.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.PROD PROD
      ON MbrEnr.PROD_SK = PROD.PROD_SK
    INNER JOIN {IDSOwner}.PROD_SH_NM PROD_SH_NM
      ON PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CLM_LN_CD
      ON CLM_LN_CD.CD_MPPNG_SK = CLM_LN.CLM_LN_POS_CD_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CLM_LN_POS_CD
      ON CLM_LN_POS_CD.CD_MPPNG_SK = CLM_LN.CLM_LN_POS_CD_SK
WHERE
    CD6.TRGT_CD = 'FACETS'
    AND CLM.PD_DT_SK <> 'NA'
    AND CLM.PD_DT_SK <> '1753-01-01'
    AND CD1.TRGT_CD = 'A02'
    AND CD2.TRGT_CD = 'PR'
    AND CD3.TRGT_CD = 'MED'
    AND CD4.TRGT_CD = 'STD'
    AND CD5.TRGT_CD = 'ACPTD'
    AND CLM.SVC_STRT_DT_SK >= '{FirstDay12MonthsAgo}'
    AND CLM.SVC_STRT_DT_SK <= '{LastDay12MonthsAgo}'
    AND MBR.MBR_SK NOT IN (0,1)
    AND MBR.INDV_BE_KEY <> 0
    AND MBR.HOST_MBR_IN = 'Y'
    AND MbrEnr.EFF_DT_SK <= '{LastDayPreviousMonth}'
    AND MbrEnr.TERM_DT_SK >= '{FirstDayPreviousMonth}'
    AND MbrEnr.ELIG_IN = 'Y'
    AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
    AND PROD_SH_NM.PROD_SH_NM IN ('PCBEXTRNL','HPEXTRNL')

UNION ALL

SELECT distinct
    CLM.SRC_SYS_CD_SK,
    CLM_LN.CLM_LN_SK,
    1 as RANK,
    CLM_EXTRNL_MBRSH.CLM_ID,
    CLM_LN.CLM_LN_SEQ_NO,
    CLM_LN.PROC_CD_SK,
    CLM.HOST_IN,
    CLM.SVC_STRT_DT_SK,
    CLM_EXTRNL_MBRSH.SUBMT_SUB_ID,
    CLM.CLM_SUBTYP_CD_SK,
    CLM.PD_DT_SK,
    MBR.MBR_SK as MBR_MBR_SK,
    MBR.INDV_BE_KEY as MBR_INDV_BE_KEY,
    MBR.FIRST_NM,
    MBR.LAST_NM,
    MBR.MBR_UNIQ_KEY as MBR1_MBR_UNIQ_KEY,
    MBR.MBR_SK AS SUPLMNT_MBR_SK,
    MBR.MBR_UNIQ_KEY AS SUPLMNT_MBR_UNIQ_KEY,
    P_MBR_BCBSA_SUPLMT.BCBSA_MBR_FIRST_NM,
    P_MBR_BCBSA_SUPLMT.BCBSA_MBR_MIDINIT,
    CLM_LN_CD.TRGT_CD AS CLM_LN_TRGT_CD,
    CLM_LN_POS_CD.TRGT_CD AS CLM_LN_POS_TRGT_CD,
    CLM_LN.SVC_PROV_SK,
    CLM.ALW_AMT
FROM
    {IDSOwner}.MBR MBR,
    {IDSOwner}.MBR MBR2,
    {IDSOwner}.MBR_ENR MbrEnr,
    {IDSOwner}.P_MBR_BCBSA_SUPLMT P_MBR_BCBSA_SUPLMT,
    {IDSOwner}.CLM_EXTRNL_MBRSH CLM_EXTRNL_MBRSH,
    {IDSOwner}.CLM CLM,
    {IDSOwner}.CLM_LN CLM_LN,
    {IDSOwner}.CD_MPPNG CLM_LN_CD,
    {IDSOwner}.CD_MPPNG CLM_LN_POS_CD,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.PROD_SH_NM PROD_SH_NM,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.CD_MPPNG CD2,
    {IDSOwner}.CD_MPPNG CD3,
    {IDSOwner}.CD_MPPNG CD4,
    {IDSOwner}.CD_MPPNG CD5,
    {IDSOwner}.CD_MPPNG CD6,
    {IDSOwner}.CD_MPPNG CD7
WHERE
    CLM.SRC_SYS_CD_SK = CD6.CD_MPPNG_SK
    AND CD6.TRGT_CD = 'FACETS'
    AND CLM.PD_DT_SK <> 'NA'
    AND CLM.PD_DT_SK <> '1753-01-01'
    AND CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
    AND CD1.TRGT_CD = 'A02'
    AND CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
    AND CD2.TRGT_CD = 'PR'
    AND CLM.CLM_TYP_CD_SK = CD3.CD_MPPNG_SK
    AND CD3.TRGT_CD = 'MED'
    AND CLM.CLM_CAT_CD_SK = CD4.CD_MPPNG_SK
    AND CD4.TRGT_CD = 'STD'
    AND CLM.CLM_FINL_DISP_CD_SK = CD5.CD_MPPNG_SK
    AND CD5.TRGT_CD = 'ACPTD'
    AND CLM.SVC_STRT_DT_SK >= '{FirstDay12MonthsAgo}'
    AND CLM.SVC_STRT_DT_SK <= '{LastDay12MonthsAgo}'
    AND CLM_LN.CLM_LN_POS_CD_SK = CD7.CD_MPPNG_SK
    AND CLM_LN.CLM_SK = CLM.CLM_SK
    AND MBR.MBR_SK = P_MBR_BCBSA_SUPLMT.MBR_SK
    AND MBR.INDV_BE_KEY <> 0
    AND MBR.HOST_MBR_IN = 'Y'
    AND MBR.INDV_BE_KEY = MBR2.INDV_BE_KEY
    AND MBR2.MBR_SK = MbrEnr.MBR_SK
    AND MbrEnr.PROD_SK = PROD.PROD_SK
    AND MbrEnr.EFF_DT_SK <= '{LastDayPreviousMonth}'
    AND MbrEnr.TERM_DT_SK >= '{FirstDayPreviousMonth}'
    AND MbrEnr.ELIG_IN = 'Y'
    AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
    AND PROD_SH_NM.PROD_SH_NM IN ('PCBEXTRNL','HPEXTRNL')
    AND SUBSTR(TRIM(P_MBR_BCBSA_SUPLMT.BCBSA_ITS_SUB_ID), 4) = SUBSTR(TRIM(CLM_EXTRNL_MBRSH.SUBMT_SUB_ID), 4)
    AND CAST(P_MBR_BCBSA_SUPLMT.BCBSA_MBR_BRTH_DT as CHAR(10)) = CLM_EXTRNL_MBRSH.MBR_BRTH_DT_SK
    AND (
         P_MBR_BCBSA_SUPLMT.BCBSA_MBR_FIRST_NM = CLM_EXTRNL_MBRSH.MBR_FIRST_NM
         OR P_MBR_BCBSA_SUPLMT.BCBSA_MBR_FIRST_NM = CLM_EXTRNL_MBRSH.SUB_FIRST_NM
        )
    AND CLM_EXTRNL_MBRSH.CLM_SK = CLM.CLM_SK
    AND CLM_LN_CD.CD_MPPNG_SK = CLM_LN.CLM_LN_POS_CD_SK
    AND CLM_LN_POS_CD.CD_MPPNG_SK = CLM_LN.CLM_LN_POS_CD_SK
    AND P_MBR_BCBSA_SUPLMT.BCBSA_HOME_PLN_CORP_PLN_CD <> 'FEP'
"""
df_MbrSK12_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", sql_MbrSK12)
    .load()
)
df_MbrSK12_sorted = df_MbrSK12_raw.orderBy("SRC_SYS_CD_SK","CLM_LN_SK","RANK")
df_ExtrClaims12 = df_MbrSK12_sorted.dropDuplicates(["SRC_SYS_CD_SK","CLM_LN_SK"])

# Trans1 => left join with df_hf_Prov => produce df_Claims12
df_Trans1 = (
    df_ExtrClaims12.alias("ExtrClaims12")
    .join(
        df_hf_Prov.alias("HfProv"),
        F.col("ExtrClaims12.SVC_PROV_SK") == F.col("HfProv.SVC_PROV_SK"),
        "left"
    )
    .select(
        F.col("ExtrClaims12.CLM_ID").alias("CLM_ID"),
        F.col("ExtrClaims12.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("ExtrClaims12.MBR_MBR_SK").alias("MBR_SK"),
        F.col("ExtrClaims12.MBR1_MBR_UNIQ_KEY").alias("MBR1_MBR_UNIQ_KEY"),
        F.col("ExtrClaims12.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("ExtrClaims12.CLM_LN_POS_TRGT_CD").alias("CLM_LN_POS_TRGT_CD"),
        F.col("ExtrClaims12.HOST_IN").alias("HOST_IN"),
        F.when(F.col("HfProv.SVC_PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("HfProv.SVC_PROV_ID")).alias("SVC_PROV_ID"),
        F.when(F.col("HfProv.SVC_PROV_SK").isNull(), F.lit(0)).otherwise(F.col("HfProv.SVC_PROV_SK")).alias("SVC_PROV_SK"),
        F.when(F.col("HfProv.PROV_REL_GRP_PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("HfProv.PROV_REL_GRP_PROV_ID")).alias("GRP_PROV_ID"),
        F.when(F.col("HfProv.PROV_REL_GRP_PROV_SK").isNull(), F.lit(0)).otherwise(F.col("HfProv.PROV_REL_GRP_PROV_SK")).alias("GRP_PROV_SK"),
        F.when(F.col("HfProv.PROV_REL_GRP_PROV_SPEC_CD").isNull(), F.lit("UNK")).otherwise(F.col("HfProv.PROV_REL_GRP_PROV_SPEC_CD")).alias("PROV_GRP_TRGT_CD"),
        F.when(F.col("HfProv.SVC_PROV_SPEC_CD").isNull(), F.lit("UNK")).otherwise(F.col("HfProv.SVC_PROV_SPEC_CD")).alias("PROV_SVC_TRGT_CD"),
        F.col("ExtrClaims12.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("ExtrClaims12.PROC_CD_SK").alias("PROC_CD_SK"),
        F.col("ExtrClaims12.ALW_AMT").alias("ALW_AMT")
    )
)

# BekeyClm12 => multiple lookups => stage variables => filter => produce df_BekeyClm12_Dedupe
df_BekeyClm12_joined = (
    df_Trans1.alias("Claims12")
    .join(
        df_hf_Proc_Cd.alias("PROC_CD_LOOKUP12").select("PROC_CD","PROC_CD_SK"),
        on=[F.col("Claims12.PROC_CD_SK") == F.col("PROC_CD_LOOKUP12.PROC_CD_SK")],
        how="left"
    )
    .join(
        df_hf_Clm_Pos_Cd.alias("CLM_POS_CD").select("CLM_POS_CD_TYP","ATTRBTN_EFF_DT","ATTRBTN_TERM_DT"),
        (F.trim(F.col("Claims12.CLM_LN_POS_TRGT_CD")) == F.col("CLM_POS_CD.CLM_POS_CD_TYP")),
        "left"
    )
    .join(
        df_hf_ProvGrp_Spec_Cd.alias("ProvGrpSpecCd").select("PROV_SPEC_CD","PROV_SPEC_CD_EFF_DT_SK","PROV_SPEC_CD_TERM_DT_SK"),
        (F.trim(F.col("Claims12.PROV_GRP_TRGT_CD")) == F.col("ProvGrpSpecCd.PROV_SPEC_CD")),
        "left"
    )
    .join(
        df_hf_Prov_Spec_Cd.alias("ProvSpecCd").select("PROV_SPEC_CD","PROV_SPEC_CD_EFF_DT_SK","PROV_SPEC_CD_TERM_DT_SK"),
        (F.trim(F.col("Claims12.PROV_SVC_TRGT_CD")) == F.col("ProvSpecCd.PROV_SPEC_CD")),
        "left"
    )
    .join(
        df_hf_prov_np_pa_lkup.alias("NpPa12").select("CRITR_VAL_FROM_TX","EFF_DT_SK","TERM_DT_SK"),
        (F.col("Claims12.SVC_PROV_ID") == F.col("NpPa12.CRITR_VAL_FROM_TX")),
        "left"
    )
)

df_BekeyClm12_exp = df_BekeyClm12_joined.withColumn(
    "svClaimPOSValueValid",
    F.when(F.col("CLM_POS_CD.CLM_POS_CD_TYP").isNotNull(), "Y").otherwise("N")
).withColumn(
    "svProvSpecCdValueValid",
    F.when(
        (F.col("ProvSpecCd.PROV_SPEC_CD").isNotNull())
        & (F.col("Claims12.SVC_STRT_DT_SK") >= F.col("ProvSpecCd.PROV_SPEC_CD_EFF_DT_SK"))
        & (F.col("Claims12.SVC_STRT_DT_SK") <= F.col("ProvSpecCd.PROV_SPEC_CD_TERM_DT_SK")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svProvGrpSpecCdValueValid",
    F.when(F.col("ProvGrpSpecCd.PROV_SPEC_CD").isNotNull(), "Y").otherwise("N")
).withColumn(
    "svProcCodeValueValid",
    F.when(
        (F.col("PROC_CD_LOOKUP12.PROC_CD").isNotNull())
        & (F.col("CLM_POS_CD.CLM_POS_CD_TYP") != "02")
        & (F.col("CLM_POS_CD.CLM_POS_CD_TYP") != "10")
        & (F.col("PROC_CD_LOOKUP12.PROC_CD") >= "99201")
        & (F.col("PROC_CD_LOOKUP12.PROC_CD") <= "99499"),
        "Y"
    ).otherwise("N")
).withColumn(
    "svProvNpPaValueValid",
    F.when(
        (F.col("NpPa12.CRITR_VAL_FROM_TX").isNotNull())
        & (F.col("Claims12.SVC_STRT_DT_SK") >= F.col("NpPa12.EFF_DT_SK"))
        & (F.col("Claims12.SVC_STRT_DT_SK") <= F.col("NpPa12.TERM_DT_SK")),
        "Y"
    ).otherwise("N")
).withColumn(
    "sv1",
    F.when(
        (F.col("PROC_CD_LOOKUP12.PROC_CD").isNotNull())
        & (F.col("Claims12.SVC_STRT_DT_SK") >= "2021-01-01")
        & (F.col("CLM_POS_CD.CLM_POS_CD_TYP") == "02")
        & (
            ((F.col("PROC_CD_LOOKUP12.PROC_CD") >= "99201") & (F.col("PROC_CD_LOOKUP12.PROC_CD") <= "99205"))
            | ((F.col("PROC_CD_LOOKUP12.PROC_CD") >= "99211") & (F.col("PROC_CD_LOOKUP12.PROC_CD") <= "99215"))
        ),
        "Y"
    ).otherwise("N")
).withColumn(
    "sv2",
    F.when(
        (F.col("PROC_CD_LOOKUP12.PROC_CD").isNotNull())
        & (F.col("Claims12.SVC_STRT_DT_SK") >= "2022-01-01")
        & (F.col("CLM_POS_CD.CLM_POS_CD_TYP") == "10")
        & (
            ((F.col("PROC_CD_LOOKUP12.PROC_CD") >= "99201") & (F.col("PROC_CD_LOOKUP12.PROC_CD") <= "99205"))
            | ((F.col("PROC_CD_LOOKUP12.PROC_CD") >= "99211") & (F.col("PROC_CD_LOOKUP12.PROC_CD") <= "99215"))
        ),
        "Y"
    ).otherwise("N")
).withColumn(
    "svProcCodePosTeleHealthValueValid",
    F.when(
        (F.col("sv1") == "N") & (F.col("sv2") == "N"),
        "N"
    ).otherwise("Y")
)

df_BekeyClm12_final = df_BekeyClm12_exp.filter(
    (
        (F.col("svClaimPOSValueValid") == "Y")
        & (F.col("svProvSpecCdValueValid") == "Y")
        & (F.col("svProvGrpSpecCdValueValid") == "Y")
        & (
            (F.col("svProcCodeValueValid") == "Y")
            | (F.col("svProcCodePosTeleHealthValueValid") == "Y")
        )
    )
    | (
        (F.col("svProvNpPaValueValid") == "Y")
        & (F.col("svClaimPOSValueValid") == "Y")
        & (F.col("svProcCodeValueValid") == "Y")
        & (F.col("Claims12.SVC_STRT_DT_SK") >= "2021-01-01")
        & (
            (F.col("svProcCodeValueValid") == "Y")
            | (F.col("svProcCodePosTeleHealthValueValid") == "Y")
        )
    )
).select(
    F.col("Claims12.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.trim(F.col("Claims12.GRP_PROV_ID")).alias("PROV_REL_GRP_PROV_ID"),
    F.trim(F.col("Claims12.SVC_PROV_ID")).alias("SVC_PROV_ID"),
    F.col("Claims12.CLM_ID").alias("Claim_ID"),
    F.col("Claims12.CLM_LN_SEQ_NO").alias("Claim_Line"),
    F.col("Claims12.MBR_SK").alias("MBR_SK"),
    F.col("Claims12.MBR1_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("Claims12.SVC_STRT_DT_SK").isNull(),
        F.lit(None)
    ).otherwise(F.expr("FORMAT.DATE(trim(Claims12.SVC_STRT_DT_SK), 'DATE', 'DATE', 'CCYYMMDD')")).alias("CLM_LN_SVC_STRT_DT_SK"),
    F.col("Claims12.CLM_LN_POS_TRGT_CD").alias("CLM_LN_POS_CD"),
    F.when(
        F.col("PROC_CD_LOOKUP12.PROC_CD").isNull(),
        F.lit("")
    ).otherwise(F.col("PROC_CD_LOOKUP12.PROC_CD")).alias("PROC_CD"),
    F.trim(F.col("Claims12.HOST_IN")).alias("CLM_ITS_IN"),
    F.col("Claims12.SVC_PROV_SK").alias("CLM_LN_SVC_PROV_SK"),
    F.col("Claims12.GRP_PROV_SK").alias("PROV_REL_GRP_PROV_SK"),
    F.col("Claims12.PROV_GRP_TRGT_CD").alias("PROV_REL_GRP_SPEC_CD"),
    F.col("Claims12.ALW_AMT").alias("ALW_AMT")
)

# hf_dedupe => scenario A => deduplicate on pk => produce dfMonths12 => final write
df_hf_dedupe_12 = df_BekeyClm12_final.dropDuplicates(["MBR_INDV_BE_KEY","PROV_REL_GRP_PROV_ID","SVC_PROV_ID","Claim_ID"])

dfMonths12 = df_hf_dedupe_12.select(
    F.col("MBR_INDV_BE_KEY"),
    F.col("PROV_REL_GRP_PROV_ID"),
    F.col("SVC_PROV_ID"),
    F.rpad(F.col("Claim_ID"), 30, " ").alias("Claim_ID"),
    F.col("Claim_Line"),
    F.col("MBR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("CLM_LN_SVC_STRT_DT_SK"), 10, " ").alias("CLM_LN_SVC_STRT_DT_SK"),
    F.rpad(F.col("CLM_LN_POS_CD"), 2, " ").alias("CLM_LN_POS_CD"),
    F.rpad(F.col("PROC_CD"), 7, " ").alias("PROC_CD"),
    F.rpad(F.col("CLM_ITS_IN"), 1, " ").alias("CLM_ITS_IN"),
    F.col("CLM_LN_SVC_PROV_SK"),
    F.col("PROV_REL_GRP_PROV_SK"),
    F.col("PROV_REL_GRP_SPEC_CD"),
    F.col("ALW_AMT")
)

write_files(
    dfMonths12,
    f"{adls_path_publish}/external/PCMH_BCBSKC_HSTD_MBRS_BEKEY_12ClmDetails.dat.{RunID}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# IDS_CLM_24 => read => sort => deduplicate => produce df_ExtrClaims24
sql_IDS_CLM_24 = f"""
SELECT
    DISTINCT CLM.SRC_SYS_CD_SK,
    CLM_LN.CLM_LN_SK,
    2 as RANK,
    CLM_EXTRNL_MBRSH.CLM_ID,
    CLM_LN.CLM_LN_SEQ_NO,
    CLM_LN.PROC_CD_SK,
    CLM.HOST_IN,
    CLM.SVC_STRT_DT_SK,
    CLM_EXTRNL_MBRSH.SUBMT_SUB_ID,
    CLM.CLM_SUBTYP_CD_SK,
    CLM.PD_DT_SK,
    MBR.MBR_SK AS MBR_MBR_SK,
    MBR.INDV_BE_KEY AS MBR_INDV_BE_KEY,
    MBR.FIRST_NM,
    MBR.LAST_NM,
    MBR.MBR_UNIQ_KEY AS MBR1_MBR_UNIQ_KEY,
    MBR.MBR_SK AS SUPLMNT_MBR_SK,
    MBR.MBR_UNIQ_KEY AS SUPLMNT_MBR_UNIQ_KEY,
    MBR.FIRST_NM,
    MBR.MIDINIT,
    CLM_LN_CD.TRGT_CD AS CLM_LN_TRGT_CD,
    CLM_LN_POS_CD.TRGT_CD AS CLM_LN_POS_TRGT_CD,
    CLM_LN.SVC_PROV_SK,
    CLM.ALW_AMT
FROM
    {IDSOwner}.CLM CLM
    INNER JOIN {IDSOwner}.CLM_LN CLM_LN
      ON CLM.CLM_SK = CLM_LN.CLM_SK
    INNER JOIN {IDSOwner}.CLM_EXTRNL_MBRSH CLM_EXTRNL_MBRSH
      ON CLM_EXTRNL_MBRSH.CLM_SK=CLM.CLM_SK
    INNER JOIN {IDSOwner}.MBR MBR
      ON CLM_EXTRNL_MBRSH.MBR_SK = MBR.MBR_SK
    INNER JOIN {IDSOwner}.MBR MBR2
      ON MBR.INDV_BE_KEY=MBR2.INDV_BE_KEY
    INNER JOIN {IDSOwner}.MBR_ENR MbrEnr
      ON MBR2.MBR_SK=MbrEnr.MBR_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD6
      ON CLM.SRC_SYS_CD_SK=CD6.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD1
      ON CLM.CLM_STTUS_CD_SK=CD1.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD2
      ON CLM.CLM_SUBTYP_CD_SK=CD2.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD3
      ON CLM.CLM_TYP_CD_SK=CD3.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD4
      ON CLM.CLM_CAT_CD_SK=CD4.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD5
      ON CLM.CLM_FINL_DISP_CD_SK=CD5.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CD7
      ON CLM_LN.CLM_LN_POS_CD_SK=CD7.CD_MPPNG_SK
    INNER JOIN {IDSOwner}.PROD PROD
      ON MbrEnr.PROD_SK=PROD.PROD_SK
    INNER JOIN {IDSOwner}.PROD_SH_NM PROD_SH_NM
      ON PROD.PROD_SH_NM_SK=PROD_SH_NM.PROD_SH_NM_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CLM_LN_CD
      ON CLM_LN_CD.CD_MPPNG_SK=CLM_LN.CLM_LN_POS_CD_SK
    INNER JOIN {IDSOwner}.CD_MPPNG CLM_LN_POS_CD
      ON CLM_LN_POS_CD.CD_MPPNG_SK=CLM_LN.CLM_LN_POS_CD_SK
WHERE
    CD6.TRGT_CD = 'FACETS'
    AND CLM.PD_DT_SK <> 'NA'
    AND CLM.PD_DT_SK <> '1753-01-01'
    AND CD1.TRGT_CD = 'A02'
    AND CD2.TRGT_CD = 'PR'
    AND CD3.TRGT_CD = 'MED'
    AND CD4.TRGT_CD = 'STD'
    AND CD5.TRGT_CD = 'ACPTD'
    AND CLM.SVC_STRT_DT_SK >= '{FirstDay24MonthsAgo}'
    AND CLM.SVC_STRT_DT_SK <= '{LastDay24MonthsAgo}'
    AND MBR.MBR_SK NOT IN (0,1)
    AND MBR.INDV_BE_KEY <> 0
    AND MBR.HOST_MBR_IN = 'Y'
    AND MbrEnr.EFF_DT_SK <= '{LastDayPreviousMonth}'
    AND MbrEnr.TERM_DT_SK >= '{FirstDayPreviousMonth}'
    AND MbrEnr.ELIG_IN = 'Y'
    AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
    AND PROD_SH_NM.PROD_SH_NM IN ('PCBEXTRNL','HPEXTRNL')

UNION ALL

SELECT distinct
    CLM.SRC_SYS_CD_SK,
    CLM_LN.CLM_LN_SK,
    1 as RANK,
    CLM_EXTRNL_MBRSH.CLM_ID,
    CLM_LN.CLM_LN_SEQ_NO,
    CLM_LN.PROC_CD_SK,
    CLM.HOST_IN,
    CLM.SVC_STRT_DT_SK,
    CLM_EXTRNL_MBRSH.SUBMT_SUB_ID,
    CLM.CLM_SUBTYP_CD_SK,
    CLM.PD_DT_SK,
    MBR.MBR_SK as MBR_MBR_SK,
    MBR.INDV_BE_KEY as MBR_INDV_BE_KEY,
    MBR.FIRST_NM,
    MBR.LAST_NM,
    MBR.MBR_UNIQ_KEY as MBR1_MBR_UNIQ_KEY,
    P_MBR_BCBSA_SUPLMT.MBR_SK AS SUPLMNT_MBR_SK,
    P_MBR_BCBSA_SUPLMT.MBR_UNIQ_KEY AS SUPLMNT_MBR_UNIQ_KEY,
    P_MBR_BCBSA_SUPLMT.BCBSA_MBR_FIRST_NM,
    P_MBR_BCBSA_SUPLMT.BCBSA_MBR_MIDINIT,
    CLM_LN_CD.TRGT_CD AS CLM_LN_TRGT_CD,
    CLM_LN_POS_CD.TRGT_CD AS CLM_LN_POS_TRGT_CD,
    CLM_LN.SVC_PROV_SK,
    CLM.ALW_AMT
FROM
    {IDSOwner}.MBR MBR,
    {IDSOwner}.MBR MBR2,
    {IDSOwner}.MBR_ENR MbrEnr,
    {IDSOwner}.P_MBR_BCBSA_SUPLMT P_MBR_BCBSA_SUPLMT,
    {IDSOwner}.CLM_EXTRNL_MBRSH CLM_EXTRNL_MBRSH,
    {IDSOwner}.CLM CLM,
    {IDSOwner}.CLM_LN CLM_LN,
    {IDSOwner}.CD_MPPNG CLM_LN_CD,
    {IDSOwner}.CD_MPPNG CLM_LN_POS_CD,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.PROD_SH_NM PROD_SH_NM,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.CD_MPPNG CD2,
    {IDSOwner}.CD_MPPNG CD3,
    {IDSOwner}.CD_MPPNG CD4,
    {IDSOwner}.CD_MPPNG CD5,
    {IDSOwner}.CD_MPPNG CD6,
    {IDSOwner}.CD_MPPNG CD7
WHERE
    CLM.SRC_SYS_CD_SK = CD6.CD_MPPNG_SK
    AND CD6.TRGT_CD = 'FACETS'
    AND CLM.PD_DT_SK <> 'NA'
    AND CLM.PD_DT_SK <> '1753-01-01'
    AND CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
    AND CD1.TRGT_CD = 'A02'
    AND CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
    AND CD2.TRGT_CD = 'PR'
    AND CLM.CLM_TYP_CD_SK = CD3.CD_MPPNG_SK
    AND CD3.TRGT_CD = 'MED'
    AND CLM.CLM_CAT_CD_SK = CD4.CD_MPPNG_SK
    AND CD4.TRGT_CD = 'STD'
    AND CLM.CLM_FINL_DISP_CD_SK = CD5.CD_MPPNG_SK
    AND CD5.TRGT_CD = 'ACPTD'
    AND CLM.SVC_STRT_DT_SK >= '{FirstDay24MonthsAgo}'
    AND CLM.SVC_STRT_DT_SK <= '{LastDay24MonthsAgo}'
    AND CLM_LN.CLM_LN_POS_CD_SK = CD7.CD_MPPNG_SK
    AND CLM_LN.CLM_SK = CLM.CLM_SK
    AND MBR.MBR_SK = P_MBR_BCBSA_SUPLMT.MBR_SK
    AND MBR.INDV_BE_KEY <> 0
    AND MBR.HOST_MBR_IN = 'Y'
    AND MBR.INDV_BE_KEY = MBR2.INDV_BE_KEY
    AND MBR2.MBR_SK = MbrEnr.MBR_SK
    AND MbrEnr.PROD_SK = PROD.PROD_SK
    AND MbrEnr.EFF_DT_SK <= '{LastDayPreviousMonth}'
    AND MbrEnr.TERM_DT_SK >= '{FirstDayPreviousMonth}'
    AND MbrEnr.ELIG_IN = 'Y'
    AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
    AND PROD_SH_NM.PROD_SH_NM IN ('PCBEXTRNL','HPEXTRNL')
    AND SUBSTR(TRIM(P_MBR_BCBSA_SUPLMT.BCBSA_ITS_SUB_ID), 4) = SUBSTR(TRIM(CLM_EXTRNL_MBRSH.SUBMT_SUB_ID), 4)
    AND CAST(P_MBR_BCBSA_SUPLMT.BCBSA_MBR_BRTH_DT as CHAR(10)) = CLM_EXTRNL_MBRSH.MBR_BRTH_DT_SK
    AND (
         P_MBR_BCBSA_SUPLMT.BCBSA_MBR_FIRST_NM = CLM_EXTRNL_MBRSH.MBR_FIRST_NM
         OR P_MBR_BCBSA_SUPLMT.BCBSA_MBR_FIRST_NM = CLM_EXTRNL_MBRSH.SUB_FIRST_NM
        )
    AND CLM_EXTRNL_MBRSH.CLM_SK = CLM.CLM_SK
    AND CLM_LN_CD.CD_MPPNG_SK = CLM_LN.CLM_LN_POS_CD_SK
    AND CLM_LN_POS_CD.CD_MPPNG_SK = CLM_LN.CLM_LN_POS_CD_SK
    AND P_MBR_BCBSA_SUPLMT.BCBSA_HOME_PLN_CORP_PLN_CD <> 'FEP'
;
"""

df_IDS_CLM_24_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", sql_IDS_CLM_24)
    .load()
)
df_IDS_CLM_24_sorted = df_IDS_CLM_24_raw.orderBy("SRC_SYS_CD_SK","CLM_LN_SK","RANK")
df_ExtrClaims24 = df_IDS_CLM_24_sorted.dropDuplicates(["SRC_SYS_CD_SK","CLM_LN_SK"])

# Transformer_416 => left join with df_hf_Prov12 => produce df_Claims24
df_Transformer_416 = (
    df_ExtrClaims24.alias("ExtrClaims24")
    .join(
        df_hf_Prov12.alias("HfProv"),
        F.col("ExtrClaims24.SVC_PROV_SK") == F.col("HfProv.SVC_PROV_SK"),
        "left"
    )
    .select(
        F.col("ExtrClaims24.CLM_ID").alias("CLM_ID"),
        F.col("ExtrClaims24.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("ExtrClaims24.MBR_MBR_SK").alias("MBR_SK"),
        F.col("ExtrClaims24.MBR1_MBR_UNIQ_KEY").alias("MBR1_MBR_UNIQ_KEY"),
        F.col("ExtrClaims24.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("ExtrClaims24.CLM_LN_POS_TRGT_CD").alias("CLM_LN_POS_TRGT_CD"),
        F.col("ExtrClaims24.HOST_IN").alias("HOST_IN"),
        F.when(F.col("HfProv.SVC_PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("HfProv.SVC_PROV_ID")).alias("SVC_PROV_ID"),
        F.when(F.col("HfProv.SVC_PROV_SK").isNull(), F.lit(0)).otherwise(F.col("HfProv.SVC_PROV_SK")).alias("SVC_PROV_SK"),
        F.when(F.col("HfProv.PROV_REL_GRP_PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("HfProv.PROV_REL_GRP_PROV_ID")).alias("GRP_PROV_ID"),
        F.when(F.col("HfProv.PROV_REL_GRP_PROV_SK").isNull(), F.lit(0)).otherwise(F.col("HfProv.PROV_REL_GRP_PROV_SK")).alias("GRP_PROV_SK"),
        F.when(F.col("HfProv.SVC_PROV_SPEC_CD").isNull(), F.lit(0)).otherwise(F.col("HfProv.SVC_PROV_SPEC_CD")).alias("PROV_SPEC_CD"),
        F.when(F.col("HfProv.PROV_REL_GRP_PROV_SPEC_CD").isNull(), F.lit(0)).otherwise(F.col("HfProv.PROV_REL_GRP_PROV_SPEC_CD")).alias("PROV_REL_GRP_PROV_SPEC_CD"),
        F.col("ExtrClaims24.PROC_CD_SK").alias("PROC_CD_SK"),
        F.col("ExtrClaims24.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("ExtrClaims24.ALW_AMT").alias("ALW_AMT")
    )
)

# BekeyClm24 => multiple lookups => stage variables => filter => produce df_BekeyClm24_final
df_BekeyClm24_joined = (
    df_Transformer_416.alias("Claims24")
    .join(
        df_hf_Proc_Cd.alias("PROC_CD_LOOKUP24").select("PROC_CD","PROC_CD_SK"),
        on=[F.col("Claims24.PROC_CD_SK") == F.col("PROC_CD_LOOKUP24.PROC_CD_SK")],
        how="left"
    )
    .join(
        df_hf_Clm_Pos_Cd.alias("CLM_POS_CD24").select("CLM_POS_CD_TYP","ATTRBTN_EFF_DT","ATTRBTN_TERM_DT"),
        F.trim(F.col("Claims24.CLM_LN_POS_TRGT_CD")) == F.col("CLM_POS_CD24.CLM_POS_CD_TYP"),
        "left"
    )
    .join(
        df_hf_ProvGrp_Spec_Cd.alias("ProvGrpSpecCd24").select("PROV_SPEC_CD","PROV_SPEC_CD_EFF_DT_SK","PROV_SPEC_CD_TERM_DT_SK"),
        F.trim(F.col("Claims24.PROV_REL_GRP_PROV_SPEC_CD")) == F.col("ProvGrpSpecCd24.PROV_SPEC_CD"),
        "left"
    )
    .join(
        df_hf_Prov_Spec_Cd.alias("ProvSpecCd24").select("PROV_SPEC_CD","PROV_SPEC_CD_EFF_DT_SK","PROV_SPEC_CD_TERM_DT_SK"),
        F.trim(F.col("Claims24.PROV_SPEC_CD")) == F.col("ProvSpecCd24.PROV_SPEC_CD"),
        "left"
    )
    .join(
        df_hf_prov_np_pa_lkup.alias("NpPa24").select("CRITR_VAL_FROM_TX","EFF_DT_SK","TERM_DT_SK"),
        F.col("Claims24.SVC_PROV_ID") == F.col("NpPa24.CRITR_VAL_FROM_TX"),
        "left"
    )
)

df_BekeyClm24_exp = df_BekeyClm24_joined.withColumn(
    "svClaimPOSValueValid",
    F.when(F.col("CLM_POS_CD24.CLM_POS_CD_TYP").isNotNull(), "Y").otherwise("N")
).withColumn(
    "svProvSpecCdValueValid",
    F.when(
        (F.col("ProvSpecCd24.PROV_SPEC_CD").isNotNull())
        & (F.col("Claims24.SVC_STRT_DT_SK") >= F.col("ProvSpecCd24.PROV_SPEC_CD_EFF_DT_SK"))
        & (F.col("Claims24.SVC_STRT_DT_SK") <= F.col("ProvSpecCd24.PROV_SPEC_CD_TERM_DT_SK")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svProvGrpSpecCdValueValid",
    F.when(F.col("ProvGrpSpecCd24.PROV_SPEC_CD").isNotNull(), "Y").otherwise("N")
).withColumn(
    "svProcCodeValueValid",
    F.when(
        (F.col("PROC_CD_LOOKUP24.PROC_CD").isNotNull())
        & (F.col("CLM_POS_CD24.CLM_POS_CD_TYP") != "02")
        & (F.col("CLM_POS_CD24.CLM_POS_CD_TYP") != "10")
        & (F.col("PROC_CD_LOOKUP24.PROC_CD") >= "99201")
        & (F.col("PROC_CD_LOOKUP24.PROC_CD") <= "99499"),
        "Y"
    ).otherwise("N")
).withColumn(
    "svProvNpPaValueValid",
    F.when(
        (F.col("NpPa24.CRITR_VAL_FROM_TX").isNotNull())
        & (F.col("Claims24.SVC_STRT_DT_SK") >= F.col("NpPa24.EFF_DT_SK"))
        & (F.col("Claims24.SVC_STRT_DT_SK") <= F.col("NpPa24.TERM_DT_SK")),
        "Y"
    ).otherwise("N")
).withColumn(
    "sv1",
    F.when(
        (F.col("PROC_CD_LOOKUP24.PROC_CD").isNotNull())
        & (F.col("Claims24.SVC_STRT_DT_SK") >= "2021-01-01")
        & (F.col("CLM_POS_CD24.CLM_POS_CD_TYP") == "02")
        & (
            ((F.col("PROC_CD_LOOKUP24.PROC_CD") >= "99201") & (F.col("PROC_CD_LOOKUP24.PROC_CD") <= "99205"))
            | ((F.col("PROC_CD_LOOKUP24.PROC_CD") >= "99211") & (F.col("PROC_CD_LOOKUP24.PROC_CD") <= "99215"))
        ),
        "Y"
    ).otherwise("N")
).withColumn(
    "sv2",
    F.when(
        (F.col("PROC_CD_LOOKUP24.PROC_CD").isNotNull())
        & (F.col("Claims24.SVC_STRT_DT_SK") >= "2022-01-01")
        & (F.col("CLM_POS_CD24.CLM_POS_CD_TYP") == "10")
        & (
            ((F.col("PROC_CD_LOOKUP24.PROC_CD") >= "99201") & (F.col("PROC_CD_LOOKUP24.PROC_CD") <= "99205"))
            | ((F.col("PROC_CD_LOOKUP24.PROC_CD") >= "99211") & (F.col("PROC_CD_LOOKUP24.PROC_CD") <= "99215"))
        ),
        "Y"
    ).otherwise("N")
).withColumn(
    "svProcCodePosTeleHealthValueValid",
    F.when(
        (F.col("sv1") == "N") & (F.col("sv2") == "N"),
        "N"
    ).otherwise("Y")
)

df_BekeyClm24_final = df_BekeyClm24_exp.filter(
    (
        (F.col("svClaimPOSValueValid") == "Y")
        & (F.col("svProvSpecCdValueValid") == "Y")
        & (F.col("svProvGrpSpecCdValueValid") == "Y")
        & (
            (F.col("svProcCodeValueValid") == "Y")
            | (F.col("svProcCodePosTeleHealthValueValid") == "Y")
        )
    )
    | (
        (F.col("svProvNpPaValueValid") == "Y")
        & (F.col("svClaimPOSValueValid") == "Y")
        & (F.col("svProcCodeValueValid") == "Y")
        & (F.col("Claims24.SVC_STRT_DT_SK") >= "2021-01-01")
        & (
            (F.col("svProcCodeValueValid") == "Y")
            | (F.col("svProcCodePosTeleHealthValueValid") == "Y")
        )
    )
).select(
    F.col("Claims24.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Claims24.GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    F.trim(F.col("Claims24.SVC_PROV_ID")).alias("SVC_PROV_ID"),
    F.col("Claims24.CLM_ID").alias("Claim_ID"),
    F.col("Claims24.CLM_LN_SEQ_NO").alias("Claim_Line"),
    F.col("Claims24.MBR_SK").alias("MBR_SK"),
    F.col("Claims24.MBR1_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("Claims24.SVC_STRT_DT_SK").isNull(),
        F.lit(None)
    ).otherwise(F.expr("FORMAT.DATE(trim(Claims24.SVC_STRT_DT_SK), 'DATE', 'DATE', 'CCYYMMDD')")).alias("CLM_LN_SVC_STRT_DT_SK"),
    F.col("Claims24.CLM_LN_POS_TRGT_CD").alias("CLM_LN_POS_CD"),
    F.when(
        F.col("PROC_CD_LOOKUP24.PROC_CD").isNull(),
        F.lit("")
    ).otherwise(F.col("PROC_CD_LOOKUP24.PROC_CD")).alias("PROC_CD"),
    F.col("Claims24.HOST_IN").alias("CLM_ITS_IN"),
    F.col("Claims24.SVC_PROV_SK").alias("CLM_LN_SVC_PROV_SK"),
    F.col("Claims24.GRP_PROV_SK").alias("PROV_REL_GRP_PROV_SK"),
    F.col("Claims24.PROV_REL_GRP_PROV_SPEC_CD").alias("PROV_REL_GRP_SPEC_CD"),
    F.col("Claims24.ALW_AMT").alias("ALW_AMT")
)

# hf_dedupe24 => scenario A => deduplicate => produce dfMonths24 => final write
df_hf_dedupe_24 = df_BekeyClm24_final.dropDuplicates(["MBR_INDV_BE_KEY","PROV_REL_GRP_PROV_ID","SVC_PROV_ID","Claim_ID"])

dfMonths24 = df_hf_dedupe_24.select(
    F.col("MBR_INDV_BE_KEY"),
    F.col("PROV_REL_GRP_PROV_ID"),
    F.col("SVC_PROV_ID"),
    F.rpad(F.col("Claim_ID"), 30, " ").alias("Claim_ID"),
    F.col("Claim_Line"),
    F.col("MBR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("CLM_LN_SVC_STRT_DT_SK"), 10, " ").alias("CLM_LN_SVC_STRT_DT_SK"),
    F.rpad(F.col("CLM_LN_POS_CD"), 2, " ").alias("CLM_LN_POS_CD"),
    F.rpad(F.col("PROC_CD"), 7, " ").alias("PROC_CD"),
    F.rpad(F.col("CLM_ITS_IN"), 1, " ").alias("CLM_ITS_IN"),
    F.col("CLM_LN_SVC_PROV_SK"),
    F.col("PROV_REL_GRP_PROV_SK"),
    F.col("PROV_REL_GRP_SPEC_CD"),
    F.col("ALW_AMT")
)

write_files(
    dfMonths24,
    f"{adls_path_publish}/external/PCMH_BCBSKC_HSTD_MBRS_BEKEY_24ClmDetails.dat.{RunID}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)