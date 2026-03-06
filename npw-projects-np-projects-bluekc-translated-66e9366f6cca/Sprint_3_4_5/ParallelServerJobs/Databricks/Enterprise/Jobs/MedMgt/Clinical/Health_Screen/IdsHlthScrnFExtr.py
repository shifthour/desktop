# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwHlthScrnExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                                          Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                    Environment                Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   --------------------              -------------------------  -------------------   
# MAGIC Ralph Tucker     12/12/2007   3036        Originally Programmed                                                                                                                    Steph Goddard   12/14/2007
# MAGIC Hugh Sisson       10/24/2008  TTR-388  Changed definitions on 3 columns                                                                                                  Steph Goddard   10/28/2008
# MAGIC Kalyan Neelam   4/14/2010      4428        Added 8 new fields at the end.                                                                                                     Steph Goddard   04/19/2010
# MAGIC Kalyan Neelam   05/12/2010    4428       Changed logic to pull for 6 new sources, took out src_sys_cd lookup                                           Steph Goddard   05/13/2010
# MAGIC Kalyan Neelam   06/22/2010    4428        Added new source KU Med (UKHOSPAUTHOCPTNLHLTH)                                                      Steph Goddard   06/22/2010
# MAGIC 
# MAGIC SAndrew            12/07/2010   TTR-952    Added lookup to PRunCyc to get Start Date for IDS Create Run Cycle    
# MAGIC Kalyan Neelam   01/05/2011   TTR-959   Added new source LABCORPS                                                                                                    Steph Goddard   01/06/2010
# MAGIC Bhoomi Dasari    2011-10-17     4673        Changed WAIST_CRCMFR_NO                                                                                                   Sandrew            2011-10-21
# MAGIC                                                                  to Decimal(7,2) from Integer(10)                
# MAGIC 
# MAGIC Bhoomi Dasari    2012-08-25    4830        Added "HlthFtns' lookup and added 3 new fields                                                                          SAndrew            2012-09-21
# MAGIC                                                                    BONE_DENSITY_NO,
# MAGIC                                                                    HLTH_SCRN_SRC_SUBTYP_CD_SK,
# MAGIC                                                                    NCTN_TST_RSLT_CD_SK    
# MAGIC Kalyan Neelam  2013-06-11 AHY3.0        Added new columns on end -
# MAGIC                                             Prod Supp     HLTH_SCRN_VNDR_CD, HLTH_SCRN_VNDR_NM,                                                               Bhoomi Dasari    6/14/2013
# MAGIC                                                                  HLTH_SCRN_VNDR_CD_SK
# MAGIC Dan Long          2013-09-25   TFS-3338  Change BMI_NO field to Decimal in HLTH_SCRN,                               EnterpriseNewDevl         Kalyan Neelam    2013-10-01
# MAGIC                                                                  Link_Collector_117, hf_ids_hlth_scrn_extr_all,
# MAGIC                                                                  BusinessLogic, EdwHlthScrnF            
# MAGIC 
# MAGIC Kalyan Neelam  2016-12-21   5414        Added new source HEALTHMINE                                                          EnterpriseDev1               Jag Yelavarthi     2016-12-21

# MAGIC EDW Health Screen Extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# 1) Retrieve Job Parameters
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2010-12-05')
EDWRunCycle = get_widget_value('EDWRunCycle','154')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WelllifeRunCycle = get_widget_value('WelllifeRunCycle','-1')
WelllifeKnowNumbersRunCycle = get_widget_value('WelllifeKnowNumbersRunCycle','-1')
WelllifeSlimmerYouRunCycle = get_widget_value('WelllifeSlimmerYouRunCycle','-1')
HooperHolmesRunCycle = get_widget_value('HooperHolmesRunCycle','-1')
HooperHolmesKnowNumbersRunCycle = get_widget_value('HooperHolmesKnowNumbersRunCycle','-1')
HooperHolmesSlimmerYouRunCycle = get_widget_value('HooperHolmesSlimmerYouRunCycle','-1')
KUMedRunCycle = get_widget_value('KUMedRunCycle','-1')
LabcorpsRunCycle = get_widget_value('LabcorpsRunCycle','-1')
HLTHFTNSRunCycle = get_widget_value('HLTHFTNSRunCycle','-1')
HEALTHMINERunCycle = get_widget_value('HEALTHMINERunCycle','')

# 2) Read from Database (HLTH_SCRN stage) - DB2Connector with database=IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

welllife_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'WELLLIFE'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {WelllifeRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_Welllife = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", welllife_query)
    .load()
)

welllife_kn_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'WELLLIFEKNOWNUMBERS'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {WelllifeKnowNumbersRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_WelllifeKnowNumbers = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", welllife_kn_query)
    .load()
)

welllife_su_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'WELLLIFESLIMMERYOU'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {WelllifeSlimmerYouRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_WelllifeSlimmeryou = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", welllife_su_query)
    .load()
)

hprhlms_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HOOPERHOLMES'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HooperHolmesRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_HprHlms = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", hprhlms_query)
    .load()
)

hprhlms_kn_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HPRHLMSKNWURNBRS'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HooperHolmesKnowNumbersRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_HprHlmsKnowNums = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", hprhlms_kn_query)
    .load()
)

hprhlms_slmru_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HPRHLMSLMRU'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HooperHolmesSlimmerYouRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_HprHlmsSlmru = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", hprhlms_slmru_query)
    .load()
)

kumed_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'UKHOSPAUTHOCPTNLHLTH'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {KUMedRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_KUMed = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", kumed_query)
    .load()
)

labcorps_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'LABCORPS'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LabcorpsRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_Labcorps = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", labcorps_query)
    .load()
)

hlthftns_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HLTHFTNS'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HLTHFTNSRunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_HlthFtns = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", hlthftns_query)
    .load()
)

healthmine_query = f"""
SELECT HLTH_SCRN.HLTH_SCRN_SK,
       HLTH_SCRN.SRC_SYS_CD_SK,
       CD_MPPNG.TRGT_CD,
       HLTH_SCRN.MBR_UNIQ_KEY,
       HLTH_SCRN.SCRN_DT_SK,
       HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
       HLTH_SCRN.GRP_SK,
       HLTH_SCRN.MBR_SK,
       HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
       HLTH_SCRN.CUR_SMOKER_RISK_IN,
       HLTH_SCRN.DBTC_RISK_IN,
       HLTH_SCRN.FRMR_SMOKER_RISK_IN,
       HLTH_SCRN.HEART_DSS_RISK_IN,
       HLTH_SCRN.HI_CHLSTRL_RISK_IN,
       HLTH_SCRN.HI_BP_RISK_IN,
       HLTH_SCRN.EXRCS_LACK_RISK_IN,
       HLTH_SCRN.OVERWT_RISK_IN,
       HLTH_SCRN.STRESS_RISK_IN,
       HLTH_SCRN.STROKE_RISK_IN,
       HLTH_SCRN.BMI_NO,
       HLTH_SCRN.CHLSTRL_RATIO_NO,
       HLTH_SCRN.DIASTOLIC_BP_NO,
       HLTH_SCRN.GLUCOSE_NO,
       HLTH_SCRN.HDL_NO,
       HLTH_SCRN.HT_INCH_NO,
       HLTH_SCRN.LDL_NO,
       HLTH_SCRN.MBR_AGE_NO,
       HLTH_SCRN.PSA_NO,
       HLTH_SCRN.SYSTOLIC_BP_NO,
       HLTH_SCRN.TOT_CHLSTRL_NO,
       HLTH_SCRN.TGL_NO,
       HLTH_SCRN.WAIST_CRCMFR_NO,
       HLTH_SCRN.WT_NO,
       HLTH_SCRN.BODY_FAT_PCT,
       HLTH_SCRN.FSTNG_IN,
       HLTH_SCRN.HA1C_NO,
       HLTH_SCRN.PRGNCY_IN,
       HLTH_SCRN.RFRL_TO_DM_IN,
       HLTH_SCRN.RFRL_TO_PHYS_IN,
       HLTH_SCRN.RESCRN_IN,
       HLTH_SCRN.TSH_NO,
       RUN_CYC.STRT_DT,
       HLTH_SCRN.BONE_DENSITY_NO,
       HLTH_SCRN.HLTH_SCRN_SRC_SUBTYP_CD_SK,
       HLTH_SCRN.NCTN_TST_RSLT_CD_SK,
       HLTH_SCRN.HLTH_SCRN_VNDR_CD_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.P_RUN_CYC RUN_CYC
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HEALTHMINE'
  AND HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HEALTHMINERunCycle}
  AND HLTH_SCRN.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
  AND RUN_CYC.SUBJ_CD = 'HEALTH_SCREEN'
  AND RUN_CYC.TRGT_SYS_CD = 'IDS'
  AND RUN_CYC.SRC_SYS_CD = CD_MPPNG.TRGT_CD
"""
df_HealthMine = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", healthmine_query)
    .load()
)

# 3) Collect (CCollector Stage) - unify all inputs from HLTH_SCRN
#    We rename columns to match the "Extract_All" specification.
def select_extract_all_cols(df_in):
    return df_in.selectExpr(
        "HLTH_SCRN_SK as HLTH_SCRN_SK",
        "SRC_SYS_CD_SK as SRC_SYS_CD_SK",
        "TRGT_CD as SRC_SYS_CD",
        "MBR_UNIQ_KEY as MBR_UNIQ_KEY",
        "SCRN_DT_SK as SCRN_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_SK as GRP_SK",
        "MBR_SK as MBR_SK",
        "HLTH_SCRN_MBR_GNDR_CD_SK as HLTH_SCRN_MBR_GNDR_CD_SK",
        "CUR_SMOKER_RISK_IN as CUR_SMOKER_RISK_IN",
        "DBTC_RISK_IN as DBTC_RISK_IN",
        "FRMR_SMOKER_RISK_IN as FRMR_SMOKER_RISK_IN",
        "HEART_DSS_RISK_IN as HEART_DSS_RISK_IN",
        "HI_CHLSTRL_RISK_IN as HI_CHLSTRL_RISK_IN",
        "HI_BP_RISK_IN as HI_BP_RISK_IN",
        "EXRCS_LACK_RISK_IN as EXRCS_LACK_RISK_IN",
        "OVERWT_RISK_IN as OVERWT_RISK_IN",
        "STRESS_RISK_IN as STRESS_RISK_IN",
        "STROKE_RISK_IN as STROKE_RISK_IN",
        "BMI_NO as BMI_NO",
        "CHLSTRL_RATIO_NO as CHLSTRL_RATIO_NO",
        "DIASTOLIC_BP_NO as DIASTOLIC_BP_NO",
        "GLUCOSE_NO as GLUCOSE_NO",
        "HDL_NO as HDL_NO",
        "HT_INCH_NO as HT_INCH_NO",
        "LDL_NO as LDL_NO",
        "MBR_AGE_NO as MBR_AGE_NO",
        "PSA_NO as PSA_NO",
        "SYSTOLIC_BP_NO as SYSTOLIC_BP_NO",
        "TOT_CHLSTRL_NO as TOT_CHLSTRL_NO",
        "TGL_NO as TGL_NO",
        "WAIST_CRCMFR_NO as WAIST_CRCMFR_NO",
        "WT_NO as WT_NO",
        "BODY_FAT_PCT as BODY_FAT_PCT",
        "FSTNG_IN as FSTNG_IN",
        "HA1C_NO as HA1C_NO",
        "PRGNCY_IN as PRGNCY_IN",
        "RFRL_TO_DM_IN as RFRL_TO_DM_IN",
        "RFRL_TO_PHYS_IN as RFRL_TO_PHYS_IN",
        "RESCRN_IN as RESCRN_IN",
        "TSH_NO as TSH_NO",
        "STRT_DT as CREATE_IDS_REC_PROCESS_DT",
        "BONE_DENSITY_NO as BONE_DENSITY_NO",
        "HLTH_SCRN_SRC_SUBTYP_CD_SK as HLTH_SCRN_SRC_SUBTYP_CD_SK",
        "NCTN_TST_RSLT_CD_SK as NCTN_TST_RSLT_CD_SK",
        "HLTH_SCRN_VNDR_CD_SK as HLTH_SCRN_VNDR_CD_SK"
    )

df_Welllife2 = select_extract_all_cols(df_Welllife)
df_WelllifeKnowNumbers2 = select_extract_all_cols(df_WelllifeKnowNumbers)
df_WelllifeSlimmeryou2 = select_extract_all_cols(df_WelllifeSlimmeryou)
df_HprHlms2 = select_extract_all_cols(df_HprHlms)
df_HprHlmsKnowNums2 = select_extract_all_cols(df_HprHlmsKnowNums)
df_HprHlmsSlmru2 = select_extract_all_cols(df_HprHlmsSlmru)
df_KUMed2 = select_extract_all_cols(df_KUMed)
df_Labcorps2 = select_extract_all_cols(df_Labcorps)
df_HlthFtns2 = select_extract_all_cols(df_HlthFtns)
df_HealthMine2 = select_extract_all_cols(df_HealthMine)

df_Link_Collector_117 = (
    df_Welllife2
    .unionByName(df_WelllifeKnowNumbers2)
    .unionByName(df_WelllifeSlimmeryou2)
    .unionByName(df_HprHlms2)
    .unionByName(df_HprHlmsKnowNums2)
    .unionByName(df_HprHlmsSlmru2)
    .unionByName(df_KUMed2)
    .unionByName(df_Labcorps2)
    .unionByName(df_HlthFtns2)
    .unionByName(df_HealthMine2)
)

# 4) "hf_ids_hlth_scrn_extr_all" is an intermediate hashed file (Scenario A).
#    So we remove it and directly deduplicate df_Link_Collector_117 on key HLTH_SCRN_SK.
df_Link_Collector_117_dedup = dedup_sort(
    df_Link_Collector_117,
    partition_cols=["HLTH_SCRN_SK"],
    sort_cols=[]
)

# 5) Read another hashed file stage "hf_cdma_codes" (Scenario C -> read parquet)
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# 6) BussinessLogic (CTransformerStage) with multiple left lookups
#    Primary input: df_Link_Collector_117_dedup alias "Extract"
df_Joined = (
    df_Link_Collector_117_dedup.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("HLTH_SCRN_MBR_GNDR_CD"),
        F.col("Extract.HLTH_SCRN_MBR_GNDR_CD_SK") == F.col("HLTH_SCRN_MBR_GNDR_CD.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("SubTyplkup"),
        F.col("Extract.HLTH_SCRN_SRC_SUBTYP_CD_SK") == F.col("SubTyplkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("Nctnlkup"),
        F.col("Extract.NCTN_TST_RSLT_CD_SK") == F.col("Nctnlkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("HlthScrnVndrCdlkup"),
        F.col("Extract.HLTH_SCRN_VNDR_CD_SK") == F.col("HlthScrnVndrCdlkup.CD_MPPNG_SK"),
        "left"
    )
)

# 7) Create the output columns for the "LoadFile" link from BussinessLogic
#    Apply all if-else transformations and expansions, then pick final columns in correct order.
df_LoadFile = df_Joined.select(
    F.col("Extract.HLTH_SCRN_SK").alias("HLTH_SCRN_SK"),
    F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),

    # HLTH_SCRN_DT_SK is char(10)
    F.rpad(F.col("Extract.SCRN_DT_SK"), 10, " ").alias("HLTH_SCRN_DT_SK"),

    # CRT_RUN_CYC_EXCTN_DT_SK is char(10), condition
    F.rpad(
        F.when(
            (F.col("Extract.CREATE_IDS_REC_PROCESS_DT").isNull()) |
            (F.length(trim(F.col("Extract.CREATE_IDS_REC_PROCESS_DT"))) < 1),
            F.lit(CurrRunCycleDate)
        ).otherwise(F.col("Extract.CREATE_IDS_REC_PROCESS_DT")),
        10, " "
    ).alias("CRT_RUN_CYC_EXCTN_DT_SK"),

    # LAST_UPDT_RUN_CYC_EXCTN_DT_SK is char(10), always CurrRunCycleDate
    F.rpad(F.lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),

    F.col("Extract.GRP_SK").alias("GRP_SK"),
    F.col("Extract.MBR_SK").alias("MBR_SK"),

    # HLTH_SCRN_MBR_GNDR_CD
    F.when(
        F.col("HLTH_SCRN_MBR_GNDR_CD.TRGT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("HLTH_SCRN_MBR_GNDR_CD.TRGT_CD")).alias("HLTH_SCRN_MBR_GNDR_CD"),

    # HLTH_SCRN_MBR_GNDR_NM
    F.when(
        F.col("HLTH_SCRN_MBR_GNDR_CD.TRGT_CD_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("HLTH_SCRN_MBR_GNDR_CD.TRGT_CD_NM")).alias("HLTH_SCRN_MBR_GNDR_NM"),

    # HLTH_SCRN_CUR_SMOKER_RISK_IN char(1)
    F.rpad(F.col("Extract.CUR_SMOKER_RISK_IN"), 1, " ").alias("HLTH_SCRN_CUR_SMOKER_RISK_IN"),

    # HLTH_SCRN_DBTC_RISK_IN char(1)
    F.rpad(F.col("Extract.DBTC_RISK_IN"), 1, " ").alias("HLTH_SCRN_DBTC_RISK_IN"),

    # HLTH_SCRN_FRMR_SMOKER_RISK_IN char(1)
    F.rpad(F.col("Extract.FRMR_SMOKER_RISK_IN"), 1, " ").alias("HLTH_SCRN_FRMR_SMOKER_RISK_IN"),

    # HLTH_SCRN_HEART_DSS_RISK_IN char(1)
    F.rpad(F.col("Extract.HEART_DSS_RISK_IN"), 1, " ").alias("HLTH_SCRN_HEART_DSS_RISK_IN"),

    # HLTH_SCRN_HI_CHLSTRL_RISK_IN char(1)
    F.rpad(F.col("Extract.HI_CHLSTRL_RISK_IN"), 1, " ").alias("HLTH_SCRN_HI_CHLSTRL_RISK_IN"),

    # HLTH_SCRN_HI_BP_RISK_IN char(1)
    F.rpad(F.col("Extract.HI_BP_RISK_IN"), 1, " ").alias("HLTH_SCRN_HI_BP_RISK_IN"),

    # HLTH_SCRN_EXRCS_LACK_RISK_IN char(1)
    F.rpad(F.col("Extract.EXRCS_LACK_RISK_IN"), 1, " ").alias("HLTH_SCRN_EXRCS_LACK_RISK_IN"),

    # HLTH_SCRN_OVERWT_RISK_IN char(1)
    F.rpad(F.col("Extract.OVERWT_RISK_IN"), 1, " ").alias("HLTH_SCRN_OVERWT_RISK_IN"),

    # HLTH_SCRN_STRESS_RISK_IN char(1)
    F.rpad(F.col("Extract.STRESS_RISK_IN"), 1, " ").alias("HLTH_SCRN_STRESS_RISK_IN"),

    # HLTH_SCRN_STROKE_RISK_IN char(1)
    F.rpad(F.col("Extract.STROKE_RISK_IN"), 1, " ").alias("HLTH_SCRN_STROKE_RISK_IN"),

    F.col("Extract.BMI_NO").alias("HLTH_SCRN_BMI_NO"),
    F.col("Extract.CHLSTRL_RATIO_NO").alias("HLTH_SCRN_CHLSTRL_RATIO_NO"),
    F.col("Extract.DIASTOLIC_BP_NO").alias("HLTH_SCRN_DIASTOLIC_BP_NO"),
    F.col("Extract.GLUCOSE_NO").alias("HLTH_SCRN_GLUCOSE_NO"),
    F.col("Extract.HDL_NO").alias("HLTH_SCRN_HDL_NO"),
    F.col("Extract.HT_INCH_NO").alias("HLTH_SCRN_HT_INCH_NO"),
    F.col("Extract.LDL_NO").alias("HLTH_SCRN_LDL_NO"),
    F.col("Extract.MBR_AGE_NO").alias("HLTH_SCRN_MBR_AGE_NO"),
    F.col("Extract.PSA_NO").alias("HLTH_SCRN_PSA_NO"),
    F.col("Extract.SYSTOLIC_BP_NO").alias("HLTH_SCRN_SYSTOLIC_BP_NO"),
    F.col("Extract.TOT_CHLSTRL_NO").alias("HLTH_SCRN_TOT_CHLSTRL_NO"),
    F.col("Extract.TGL_NO").alias("HLTH_SCRN_TGL_NO"),
    F.col("Extract.WAIST_CRCMFR_NO").alias("HLTH_SCRN_WAIST_CRCMFR_NO"),
    F.col("Extract.WT_NO").alias("HLTH_SCRN_WT_NO"),

    # CRT_RUN_CYC_EXCTN_SK
    F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),

    # LAST_UPDT_RUN_CYC_EXCTN_SK
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),

    # HLTH_SCRN_MBR_GNDR_CD_SK
    F.col("Extract.HLTH_SCRN_MBR_GNDR_CD_SK").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),

    # HLTH_SCRN_BODY_FAT_PCT
    F.col("Extract.BODY_FAT_PCT").alias("HLTH_SCRN_BODY_FAT_PCT"),

    # HLTH_SCRN_FSTNG_IN char(1)
    F.rpad(F.col("Extract.FSTNG_IN"), 1, " ").alias("HLTH_SCRN_FSTNG_IN"),

    # HLTH_SCRN_HA1C_NO
    F.col("Extract.HA1C_NO").alias("HLTH_SCRN_HA1C_NO"),

    # HLTH_SCRN_PRGNCY_IN char(1)
    F.rpad(F.col("Extract.PRGNCY_IN"), 1, " ").alias("HLTH_SCRN_PRGNCY_IN"),

    # HLTH_SCRN_RFRL_TO_DM_IN char(1)
    F.rpad(F.col("Extract.RFRL_TO_DM_IN"), 1, " ").alias("HLTH_SCRN_RFRL_TO_DM_IN"),

    # HLTH_SCRN_RFRL_TO_PHYS_IN char(1)
    F.rpad(F.col("Extract.RFRL_TO_PHYS_IN"), 1, " ").alias("HLTH_SCRN_RFRL_TO_PHYS_IN"),

    # HLTH_SCRN_RESCRN_IN char(1)
    F.rpad(F.col("Extract.RESCRN_IN"), 1, " ").alias("HLTH_SCRN_RESCRN_IN"),

    # HLTH_SCRN_TSH_NO
    F.col("Extract.TSH_NO").alias("HLTH_SCRN_TSH_NO"),

    # NCTN_TST_RSLT_CD
    F.when(
        F.col("Nctnlkup.TRGT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("Nctnlkup.TRGT_CD")).alias("NCTN_TST_RSLT_CD"),

    # NCTN_TST_RSLT_NM
    F.when(
        F.col("Nctnlkup.TRGT_CD_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("Nctnlkup.TRGT_CD_NM")).alias("NCTN_TST_RSLT_NM"),

    # NCTN_TST_RSLT_CD_SK
    F.col("Extract.NCTN_TST_RSLT_CD_SK").alias("NCTN_TST_RSLT_CD_SK"),

    # BONE_DENSITY_NO
    F.col("Extract.BONE_DENSITY_NO").alias("BONE_DENSITY_NO"),

    # HLTH_SCRN_SRC_SUBTYP_CD
    F.when(
        F.col("SubTyplkup.TRGT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("SubTyplkup.TRGT_CD")).alias("HLTH_SCRN_SRC_SUBTYP_CD"),

    # HLTH_SCRN_SRC_SUBTYP_NM
    F.when(
        F.col("SubTyplkup.TRGT_CD_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("SubTyplkup.TRGT_CD_NM")).alias("HLTH_SCRN_SRC_SUBTYP_NM"),

    # HLTH_SCRN_SRC_SUBTYP_CD_SK
    F.col("Extract.HLTH_SCRN_SRC_SUBTYP_CD_SK").alias("HLTH_SCRN_SRC_SUBTYP_CD_SK"),

    # HLTH_SCRN_VNDR_CD
    F.when(
        F.col("HlthScrnVndrCdlkup.TRGT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("HlthScrnVndrCdlkup.TRGT_CD")).alias("HLTH_SCRN_VNDR_CD"),

    # HLTH_SCRN_VNDR_NM
    F.when(
        F.col("HlthScrnVndrCdlkup.TRGT_CD_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("HlthScrnVndrCdlkup.TRGT_CD_NM")).alias("HLTH_SCRN_VNDR_NM"),

    # HLTH_SCRN_VNDR_CD_SK
    F.col("Extract.HLTH_SCRN_VNDR_CD_SK").alias("HLTH_SCRN_VNDR_CD_SK")
)

# 8) EdwHlthScrnlF (CSeqFileStage) writes "HLTH_SCRN_F.dat" with no header, overwrite, delimiter=",", quote='"'
write_files(
    df_LoadFile,
    f"{adls_path}/load/HLTH_SCRN_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)