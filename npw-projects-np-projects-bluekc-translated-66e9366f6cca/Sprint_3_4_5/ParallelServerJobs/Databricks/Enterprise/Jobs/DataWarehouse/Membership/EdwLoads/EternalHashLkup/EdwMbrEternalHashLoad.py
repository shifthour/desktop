# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsMbrCntl
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Load EDW MBR_D information into permanent hash file for lookup by any Enterprise jobs.  This eliminates other processes having to extract membership data for lookups.
# MAGIC                            The hash file must be closed before the delete SQL is run, otherwise no records are deleted.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Brent Leland\(9)2011-05-10\(9)4673  AHY              Original Programming                                                EnterpriseWrhsDevl                 SAndrew                    2011-05-12
# MAGIC                         
# MAGIC Pooja Sunkara        2014-01-27           #5114 - Daptiv#700    Changed the location of this job from                       EnterpriseWrhsDevl                Jag Yelavarthi             2014-01-27    
# MAGIC                                                                                                  /Jobs/member to 
# MAGIC                                                                                                 /Jobs/DataWarehouse/Membership

# MAGIC Load membership information into hash files using different identifiers for quick reference
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunCycle = get_widget_value('RunCycle','100')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""SELECT MBR_SK as MBR_SK,SRC_SYS_CD as SRC_SYS_CD,MBR_UNIQ_KEY as MBR_UNIQ_KEY,CRT_RUN_CYC_EXCTN_DT_SK as CRT_RUN_CYC_EXCTN_DT_SK,LAST_UPDT_RUN_CYC_EXCTN_DT_SK as LAST_UPDT_RUN_CYC_EXCTN_DT_SK,CLS_SK as CLS_SK,GRP_SK as GRP_SK,SUBGRP_SK as SUBGRP_SK,SUB_ALPHA_PFX_SK as SUB_ALPHA_PFX_SK,SUB_MBR_SK as SUB_MBR_SK,SUB_SK as SUB_SK,CLS_DESC as CLS_DESC,CLS_ID as CLS_ID,GRP_ID as GRP_ID,GRP_NM as GRP_NM,MBR_BRTH_DT_SK as MBR_BRTH_DT_SK,MBR_CASEHEAD_ID as MBR_CASEHEAD_ID,MBR_DCSD_DT_SK as MBR_DCSD_DT_SK,MBR_DCSD_IN as MBR_DCSD_IN,MBR_DSBLTY_COV_EFF_DT_SK as MBR_DSBLTY_COV_EFF_DT_SK,MBR_DSBLTY_COV_TERM_DT_SK as MBR_DSBLTY_COV_TERM_DT_SK,MBR_DSBLTY_IN as MBR_DSBLTY_IN,MBR_ETHNIC_CD as MBR_ETHNIC_CD,MBR_ETHNIC_NM as MBR_ETHNIC_NM,MBR_FEP_LOCAL_CNTR_IN as MBR_FEP_LOCAL_CNTR_IN,MBR_FIRST_NM as MBR_FIRST_NM,MBR_MIDINIT as MBR_MIDINIT,MBR_LAST_NM as MBR_LAST_NM,MBR_FULL_NM as MBR_FULL_NM,MBR_GNDR_CD as MBR_GNDR_CD,MBR_GNDR_NM as MBR_GNDR_NM,MBR_ID as MBR_ID,MBR_INDV_BE_KEY as MBR_INDV_BE_KEY,MBR_LANG_CD as MBR_LANG_CD,MBR_LANG_NM as MBR_LANG_NM,MBR_MCAID_NO as MBR_MCAID_NO,MBR_MCARE_NO as MBR_MCARE_NO,MBR_MO_ST_MCAID_CNTY_CD as MBR_MO_ST_MCAID_CNTY_CD,MBR_MO_ST_MCAID_CNTY_NM as MBR_MO_ST_MCAID_CNTY_NM,MBR_OPTRN_VRSN_INDV_BE_KEY_TX as MBR_OPTRN_VRSN_INDV_BE_KEY_TX,MBR_PREX_COND_EFF_DT_SK as MBR_PREX_COND_EFF_DT_SK,MBR_PREX_COND_MO_QTY as MBR_PREX_COND_MO_QTY,MBR_RELSHP_CD as MBR_RELSHP_CD,MBR_RELSHP_NM as MBR_RELSHP_NM,MBR_SCRD_IN as MBR_SCRD_IN,MBR_SSN as MBR_SSN,MBR_STDNT_COV_EFF_DT_SK as MBR_STDNT_COV_EFF_DT_SK,MBR_STDNT_COV_TERM_DT_SK as MBR_STDNT_COV_TERM_DT_SK,MBR_STDNT_IN as MBR_STDNT_IN,MBR_SFX_NO as MBR_SFX_NO,MBR_TERM_DT_SK as MBR_TERM_DT_SK,MBR_UNIQ_KEY_ORIG_EFF_DT_SK as MBR_UNIQ_KEY_ORIG_EFF_DT_SK,MBR_HOME_ADDR_LN_1 as MBR_HOME_ADDR_LN_1,MBR_HOME_ADDR_LN_2 as MBR_HOME_ADDR_LN_2,MBR_HOME_ADDR_LN_3 as MBR_HOME_ADDR_LN_3,MBR_HOME_ADDR_CITY_NM as MBR_HOME_ADDR_CITY_NM,MBR_HOME_ADDR_ST_CD as MBR_HOME_ADDR_ST_CD,MBR_HOME_ADDR_ST_NM as MBR_HOME_ADDR_ST_NM,MBR_HOME_ADDR_ZIP_CD_5 as MBR_HOME_ADDR_ZIP_CD_5,MBR_HOME_ADDR_ZIP_CD_4 as MBR_HOME_ADDR_ZIP_CD_4,MBR_HOME_ADDR_CNTY_NM as MBR_HOME_ADDR_CNTY_NM,MBR_HOME_ADDR_PHN_NO as MBR_HOME_ADDR_PHN_NO,MBR_HOME_ADDR_PHN_NO_EXT as MBR_HOME_ADDR_PHN_NO_EXT,MBR_HOME_ADDR_FAX_NO as MBR_HOME_ADDR_FAX_NO,MBR_HOME_ADDR_FAX_NO_EXT as MBR_HOME_ADDR_FAX_NO_EXT,MBR_HOME_ADDR_EMAIL_ADDR_TX as MBR_HOME_ADDR_EMAIL_ADDR_TX,MBR_MAIL_ADDR_CONF_COMM_IN as MBR_MAIL_ADDR_CONF_COMM_IN,MBR_MAIL_ADDR_LN_1 as MBR_MAIL_ADDR_LN_1,MBR_MAIL_ADDR_LN_2 as MBR_MAIL_ADDR_LN_2,MBR_MAIL_ADDR_LN_3 as MBR_MAIL_ADDR_LN_3,MBR_MAIL_ADDR_CITY_NM as MBR_MAIL_ADDR_CITY_NM,MBR_MAIL_ADDR_ST_CD as MBR_MAIL_ADDR_ST_CD,MBR_MAIL_ADDR_ST_NM as MBR_MAIL_ADDR_ST_NM,MBR_MAIL_ADDR_ZIP_CD_5 as MBR_MAIL_ADDR_ZIP_CD_5,MBR_MAIL_ADDR_ZIP_CD_4 as MBR_MAIL_ADDR_ZIP_CD_4,MBR_MAIL_ADDR_CNTY_NM as MBR_MAIL_ADDR_CNTY_NM,MBR_MAIL_ADDR_PHN_NO as MBR_MAIL_ADDR_PHN_NO,MBR_MAIL_ADDR_PHN_NO_EXT as MBR_MAIL_ADDR_PHN_NO_EXT,MBR_MAIL_ADDR_FAX_NO as MBR_MAIL_ADDR_FAX_NO,MBR_MAIL_ADDR_FAX_NO_EXT as MBR_MAIL_ADDR_FAX_NO_EXT,MBR_MAIL_ADDR_EMAIL_ADDR_TX as MBR_MAIL_ADDR_EMAIL_ADDR_TX,MBR_VAL_BASED_INCNTV_PGM_IN as MBR_VAL_BASED_INCNTV_PGM_IN,SUBGRP_ID as SUBGRP_ID,SUBGRP_NM as SUBGRP_NM,SUB_ALPHA_PFX_CD as SUB_ALPHA_PFX_CD,SUB_CNTGS_CNTY_CD as SUB_CNTGS_CNTY_CD,SUB_CNTR_ST_CD as SUB_CNTR_ST_CD,SUB_FIRST_NM as SUB_FIRST_NM,SUB_MIDINIT as SUB_MIDINIT,SUB_LAST_NM as SUB_LAST_NM,SUB_FULL_NM as SUB_FULL_NM,SUB_HIRE_DT_SK as SUB_HIRE_DT_SK,SUB_HIST_PRCS_DT_SK as SUB_HIST_PRCS_DT_SK,SUB_ID as SUB_ID,SUB_IN_AREA_IN as SUB_IN_AREA_IN,SUB_IN as SUB_IN,SUB_INDV_BE_KEY as SUB_INDV_BE_KEY,SUB_MKTNG_METRO_RURAL_CD as SUB_MKTNG_METRO_RURAL_CD,SUB_RESDNC_CRS_PLN_CD as SUB_RESDNC_CRS_PLN_CD,SUB_RESDNC_CRS_PLN_NM as SUB_RESDNC_CRS_PLN_NM,SUB_RESDNC_SHIELD_PLN_CD as SUB_RESDNC_SHIELD_PLN_CD,SUB_RESDNC_SHIELD_PLN_NM as SUB_RESDNC_SHIELD_PLN_NM,SUB_RETR_DT_SK as SUB_RETR_DT_SK,SUB_SCRD_IN as SUB_SCRD_IN,SUB_SSN as SUB_SSN,SUB_UNIQ_KEY as SUB_UNIQ_KEY,SUB_UNIQ_KEY_ORIG_EFF_DT_SK as SUB_UNIQ_KEY_ORIG_EFF_DT_SK,WEB_PHYS_BNF_IN as WEB_PHYS_BNF_IN,CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,MBR_ETHNIC_CD_SK as MBR_ETHNIC_CD_SK,MBR_GNDR_CD_SK as MBR_GNDR_CD_SK,MBR_HOME_ADDR_ST_CD_SK as MBR_HOME_ADDR_ST_CD_SK,MBR_LANG_CD_SK as MBR_LANG_CD_SK,MBR_MAIL_ADDR_ST_CD_SK as MBR_MAIL_ADDR_ST_CD_SK,MBR_MO_ST_MCAID_CNTY_CD_SK as MBR_MO_ST_MCAID_CNTY_CD_SK,MBR_RELSHP_CD_SK as MBR_RELSHP_CD_SK,MBR_WORK_PHN_NO as MBR_WORK_PHN_NO,MBR_WORK_PHN_NO_EXT as MBR_WORK_PHN_NO_EXT,MBR_CELL_PHN_NO as MBR_CELL_PHN_NO
FROM {EDWOwner}.MBR_D
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {RunCycle}"""

df_EDW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = df_EDW.select(
    col("MBR_SK").alias("MBR_SK"),
    lit(RunCycle).alias("LOAD_RUN_CYCLE"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLS_SK").alias("CLS_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("SUBGRP_SK").alias("SUBGRP_SK"),
    col("SUB_ALPHA_PFX_SK").alias("SUB_ALPHA_PFX_SK"),
    col("SUB_MBR_SK").alias("SUB_MBR_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("CLS_DESC").alias("CLS_DESC"),
    col("CLS_ID").alias("CLS_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    col("MBR_CASEHEAD_ID").alias("MBR_CASEHEAD_ID"),
    col("MBR_DCSD_DT_SK").alias("MBR_DCSD_DT_SK"),
    col("MBR_DCSD_IN").alias("MBR_DCSD_IN"),
    col("MBR_DSBLTY_COV_EFF_DT_SK").alias("MBR_DSBLTY_COV_EFF_DT_SK"),
    col("MBR_DSBLTY_COV_TERM_DT_SK").alias("MBR_DSBLTY_COV_TERM_DT_SK"),
    col("MBR_DSBLTY_IN").alias("MBR_DSBLTY_IN"),
    col("MBR_ETHNIC_CD").alias("MBR_ETHNIC_CD"),
    col("MBR_ETHNIC_NM").alias("MBR_ETHNIC_NM"),
    col("MBR_FEP_LOCAL_CNTR_IN").alias("MBR_FEP_LOCAL_CNTR_IN"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    col("MBR_GNDR_NM").alias("MBR_GNDR_NM"),
    col("MBR_ID").alias("MBR_ID"),
    col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    col("MBR_LANG_CD").alias("MBR_LANG_CD"),
    col("MBR_LANG_NM").alias("MBR_LANG_NM"),
    col("MBR_MCAID_NO").alias("MBR_MCAID_NO"),
    col("MBR_MCARE_NO").alias("MBR_MCARE_NO"),
    col("MBR_MO_ST_MCAID_CNTY_CD").alias("MBR_MO_ST_MCAID_CNTY_CD"),
    col("MBR_MO_ST_MCAID_CNTY_NM").alias("MBR_MO_ST_MCAID_CNTY_NM"),
    col("MBR_OPTRN_VRSN_INDV_BE_KEY_TX").alias("MBR_OPTRN_VRSN_INDV_BE_KEY_TX"),
    col("MBR_PREX_COND_EFF_DT_SK").alias("MBR_PREX_COND_EFF_DT_SK"),
    col("MBR_PREX_COND_MO_QTY").alias("MBR_PREX_COND_MO_QTY"),
    col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    col("MBR_RELSHP_NM").alias("MBR_RELSHP_NM"),
    col("MBR_SCRD_IN").alias("MBR_SCRD_IN"),
    col("MBR_SSN").alias("MBR_SSN"),
    col("MBR_STDNT_COV_EFF_DT_SK").alias("MBR_STDNT_COV_EFF_DT_SK"),
    col("MBR_STDNT_COV_TERM_DT_SK").alias("MBR_STDNT_COV_TERM_DT_SK"),
    col("MBR_STDNT_IN").alias("MBR_STDNT_IN"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("MBR_TERM_DT_SK").alias("MBR_TERM_DT_SK"),
    col("MBR_UNIQ_KEY_ORIG_EFF_DT_SK").alias("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
    col("MBR_HOME_ADDR_LN_1").alias("MBR_HOME_ADDR_LN_1"),
    col("MBR_HOME_ADDR_LN_2").alias("MBR_HOME_ADDR_LN_2"),
    col("MBR_HOME_ADDR_LN_3").alias("MBR_HOME_ADDR_LN_3"),
    col("MBR_HOME_ADDR_CITY_NM").alias("MBR_HOME_ADDR_CITY_NM"),
    col("MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
    col("MBR_HOME_ADDR_ST_NM").alias("MBR_HOME_ADDR_ST_NM"),
    col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    col("MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
    col("MBR_HOME_ADDR_CNTY_NM").alias("MBR_HOME_ADDR_CNTY_NM"),
    col("MBR_HOME_ADDR_PHN_NO").alias("MBR_HOME_ADDR_PHN_NO"),
    col("MBR_HOME_ADDR_PHN_NO_EXT").alias("MBR_HOME_ADDR_PHN_NO_EXT"),
    col("MBR_HOME_ADDR_FAX_NO").alias("MBR_HOME_ADDR_FAX_NO"),
    col("MBR_HOME_ADDR_FAX_NO_EXT").alias("MBR_HOME_ADDR_FAX_NO_EXT"),
    col("MBR_HOME_ADDR_EMAIL_ADDR_TX").alias("MBR_HOME_ADDR_EMAIL_ADDR_TX"),
    col("MBR_MAIL_ADDR_CONF_COMM_IN").alias("MBR_MAIL_ADDR_CONF_COMM_IN"),
    col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    col("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    col("MBR_MAIL_ADDR_ST_NM").alias("MBR_MAIL_ADDR_ST_NM"),
    col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    col("MBR_MAIL_ADDR_CNTY_NM").alias("MBR_MAIL_ADDR_CNTY_NM"),
    col("MBR_MAIL_ADDR_PHN_NO").alias("MBR_MAIL_ADDR_PHN_NO"),
    col("MBR_MAIL_ADDR_PHN_NO_EXT").alias("MBR_MAIL_ADDR_PHN_NO_EXT"),
    col("MBR_MAIL_ADDR_FAX_NO").alias("MBR_MAIL_ADDR_FAX_NO"),
    col("MBR_MAIL_ADDR_FAX_NO_EXT").alias("MBR_MAIL_ADDR_FAX_NO_EXT"),
    col("MBR_MAIL_ADDR_EMAIL_ADDR_TX").alias("MBR_MAIL_ADDR_EMAIL_ADDR_TX"),
    col("MBR_VAL_BASED_INCNTV_PGM_IN").alias("MBR_VAL_BASED_INCNTV_PGM_IN"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("SUBGRP_NM").alias("SUBGRP_NM"),
    col("SUB_ALPHA_PFX_CD").alias("SUB_ALPHA_PFX_CD"),
    col("SUB_CNTGS_CNTY_CD").alias("SUB_CNTGS_CNTY_CD"),
    col("SUB_CNTR_ST_CD").alias("SUB_CNTR_ST_CD"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("SUB_FULL_NM").alias("SUB_FULL_NM"),
    col("SUB_HIRE_DT_SK").alias("SUB_HIRE_DT_SK"),
    col("SUB_HIST_PRCS_DT_SK").alias("SUB_HIST_PRCS_DT_SK"),
    col("SUB_ID").alias("SUB_ID"),
    col("SUB_IN_AREA_IN").alias("SUB_IN_AREA_IN"),
    col("SUB_IN").alias("SUB_IN"),
    col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    col("SUB_MKTNG_METRO_RURAL_CD").alias("SUB_MKTNG_METRO_RURAL_CD"),
    col("SUB_RESDNC_CRS_PLN_CD").alias("SUB_RESDNC_CRS_PLN_CD"),
    col("SUB_RESDNC_CRS_PLN_NM").alias("SUB_RESDNC_CRS_PLN_NM"),
    col("SUB_RESDNC_SHIELD_PLN_CD").alias("SUB_RESDNC_SHIELD_PLN_CD"),
    col("SUB_RESDNC_SHIELD_PLN_NM").alias("SUB_RESDNC_SHIELD_PLN_NM"),
    col("SUB_RETR_DT_SK").alias("SUB_RETR_DT_SK"),
    col("SUB_SCRD_IN").alias("SUB_SCRD_IN"),
    col("SUB_SSN").alias("SUB_SSN"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("SUB_UNIQ_KEY_ORIG_EFF_DT_SK").alias("SUB_UNIQ_KEY_ORIG_EFF_DT_SK"),
    col("WEB_PHYS_BNF_IN").alias("WEB_PHYS_BNF_IN"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_ETHNIC_CD_SK").alias("MBR_ETHNIC_CD_SK"),
    col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    col("MBR_HOME_ADDR_ST_CD_SK").alias("MBR_HOME_ADDR_ST_CD_SK"),
    col("MBR_LANG_CD_SK").alias("MBR_LANG_CD_SK"),
    col("MBR_MAIL_ADDR_ST_CD_SK").alias("MBR_MAIL_ADDR_ST_CD_SK"),
    col("MBR_MO_ST_MCAID_CNTY_CD_SK").alias("MBR_MO_ST_MCAID_CNTY_CD_SK"),
    col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    col("MBR_WORK_PHN_NO").alias("MBR_WORK_PHN_NO"),
    col("MBR_WORK_PHN_NO_EXT").alias("MBR_WORK_PHN_NO_EXT"),
    col("MBR_CELL_PHN_NO").alias("MBR_CELL_PHN_NO")
)

df_enriched = df_Trans1 \
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("MBR_BRTH_DT_SK", rpad(col("MBR_BRTH_DT_SK"), 10, " ")) \
    .withColumn("MBR_DCSD_DT_SK", rpad(col("MBR_DCSD_DT_SK"), 10, " ")) \
    .withColumn("MBR_DCSD_IN", rpad(col("MBR_DCSD_IN"), 1, " ")) \
    .withColumn("MBR_DSBLTY_COV_EFF_DT_SK", rpad(col("MBR_DSBLTY_COV_EFF_DT_SK"), 10, " ")) \
    .withColumn("MBR_DSBLTY_COV_TERM_DT_SK", rpad(col("MBR_DSBLTY_COV_TERM_DT_SK"), 10, " ")) \
    .withColumn("MBR_DSBLTY_IN", rpad(col("MBR_DSBLTY_IN"), 1, " ")) \
    .withColumn("MBR_FEP_LOCAL_CNTR_IN", rpad(col("MBR_FEP_LOCAL_CNTR_IN"), 1, " ")) \
    .withColumn("MBR_MIDINIT", rpad(col("MBR_MIDINIT"), 1, " ")) \
    .withColumn("MBR_SCRD_IN", rpad(col("MBR_SCRD_IN"), 1, " ")) \
    .withColumn("MBR_STDNT_COV_EFF_DT_SK", rpad(col("MBR_STDNT_COV_EFF_DT_SK"), 10, " ")) \
    .withColumn("MBR_STDNT_COV_TERM_DT_SK", rpad(col("MBR_STDNT_COV_TERM_DT_SK"), 10, " ")) \
    .withColumn("MBR_STDNT_IN", rpad(col("MBR_STDNT_IN"), 1, " ")) \
    .withColumn("MBR_TERM_DT_SK", rpad(col("MBR_TERM_DT_SK"), 10, " ")) \
    .withColumn("MBR_UNIQ_KEY_ORIG_EFF_DT_SK", rpad(col("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"), 10, " ")) \
    .withColumn("MBR_HOME_ADDR_ZIP_CD_5", rpad(col("MBR_HOME_ADDR_ZIP_CD_5"), 5, " ")) \
    .withColumn("MBR_HOME_ADDR_ZIP_CD_4", rpad(col("MBR_HOME_ADDR_ZIP_CD_4"), 4, " ")) \
    .withColumn("MBR_HOME_ADDR_PHN_NO_EXT", rpad(col("MBR_HOME_ADDR_PHN_NO_EXT"), 5, " ")) \
    .withColumn("MBR_HOME_ADDR_FAX_NO_EXT", rpad(col("MBR_HOME_ADDR_FAX_NO_EXT"), 5, " ")) \
    .withColumn("MBR_MAIL_ADDR_CONF_COMM_IN", rpad(col("MBR_MAIL_ADDR_CONF_COMM_IN"), 1, " ")) \
    .withColumn("MBR_MAIL_ADDR_ZIP_CD_5", rpad(col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " ")) \
    .withColumn("MBR_MAIL_ADDR_ZIP_CD_4", rpad(col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " ")) \
    .withColumn("MBR_MAIL_ADDR_PHN_NO_EXT", rpad(col("MBR_MAIL_ADDR_PHN_NO_EXT"), 5, " ")) \
    .withColumn("MBR_MAIL_ADDR_FAX_NO_EXT", rpad(col("MBR_MAIL_ADDR_FAX_NO_EXT"), 5, " ")) \
    .withColumn("MBR_VAL_BASED_INCNTV_PGM_IN", rpad(col("MBR_VAL_BASED_INCNTV_PGM_IN"), 1, " ")) \
    .withColumn("SUB_MIDINIT", rpad(col("SUB_MIDINIT"), 1, " ")) \
    .withColumn("SUB_HIRE_DT_SK", rpad(col("SUB_HIRE_DT_SK"), 10, " ")) \
    .withColumn("SUB_HIST_PRCS_DT_SK", rpad(col("SUB_HIST_PRCS_DT_SK"), 10, " ")) \
    .withColumn("SUB_IN_AREA_IN", rpad(col("SUB_IN_AREA_IN"), 1, " ")) \
    .withColumn("SUB_IN", rpad(col("SUB_IN"), 1, " ")) \
    .withColumn("SUB_RETR_DT_SK", rpad(col("SUB_RETR_DT_SK"), 10, " ")) \
    .withColumn("SUB_SCRD_IN", rpad(col("SUB_SCRD_IN"), 1, " ")) \
    .withColumn("SUB_UNIQ_KEY_ORIG_EFF_DT_SK", rpad(col("SUB_UNIQ_KEY_ORIG_EFF_DT_SK"), 10, " ")) \
    .withColumn("WEB_PHYS_BNF_IN", rpad(col("WEB_PHYS_BNF_IN"), 1, " ")) \
    .withColumn("MBR_WORK_PHN_NO_EXT", rpad(col("MBR_WORK_PHN_NO_EXT"), 5, " "))

write_files(
    df_enriched,
    f"{adls_path}/hf_etrnl_mbr_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)