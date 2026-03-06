# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/03/08 10:27:42 Batch  14704_37683 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 04/03/08 10:21:04 Batch  14704_37276 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_2 04/01/08 14:08:56 Batch  14702_50941 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_2 04/01/08 14:04:06 Batch  14702_50657 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 04/18/07 14:50:09 Batch  14353_53412 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_2 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 11/17/06 16:01:03 Batch  14201_57667 PROMOTE bckcett testIDS30 u11141 CDHP system testing - Hugh Sisson
# MAGIC ^1_1 11/17/06 15:57:58 Batch  14201_57500 INIT bckcett devlIDS30 u11141 CDHP system testing - Hugh Sisson
# MAGIC ^1_1 08/10/06 07:54:51 Batch  14102_28496 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_4 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test
# MAGIC ^1_3 05/05/06 12:52:22 Batch  14005_46350 INIT bckcett devlIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_2 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/29/06 06:58:50 Batch  13968_25134 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  This job is called multiple times with a different CommitCnt value.  The CommitCnt value deteremines which batch of records to extract from the W_CLM_INIT table.  Those records are written to the CLM_DM_INIT_CLM table in the Claim Datamart.  The CLM_DM_INIT_CLM table has so many indexes that row updates occur at 5 rows a second.  By looping through the data, restarts after an abort don't have to process records already updated.  DBA suggested multiple updates can occur similtainioiusly so 3 more updates were added.  If no rows exist for CommitCnt SQL does not complain.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description				Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	-----------------------------------------------------------------------	--------------------------------	-------------------------------	----------------------------       
# MAGIC Brent Leland            03/28/2006                                             Originally Programmed
# MAGIC Brent Leland            04/06/2006                                             Added run cycle parameter for new table field 
# MAGIC                                                                                                 LAST_UPDT_RUN_CYC_NO
# MAGIC Ralph Tucker          11/02/2006                                             Added PCA_TYP_CD; Direct Map
# MAGIC Brent Leland            04/01/2008                                             Added three additional parralle update SQL.           devlIDS                                   Steph Goddard            04/01/2008

# MAGIC Update the Claim Mart Claim Init table for a batch of records
# MAGIC Update 4 records similtainioiusly
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
CommitCnt = get_widget_value('CommitCnt','1')
CommitCnt2 = get_widget_value('CommitCnt2','')
CommitCnt3 = get_widget_value('CommitCnt3','')
CommitCnt4 = get_widget_value('CommitCnt4','')
RunCycle = get_widget_value('RunCycle','666')
ClmMartOwner = get_widget_value('ClmMartOwner','')
IDSOwner = get_widget_value('IDSOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# W_CLM_INIT (CODBCStage) - READ
jdbc_url_W_CLM_INIT, jdbc_props_W_CLM_INIT = get_db_config(clmmart_secret_name)
extract_query_W_CLM_INIT = f"""
SELECT
  CMT_CT,
  SRC_SYS_CD,
  CLM_ID,
  CLM_FINL_DISP_CD,
  CLM_STTUS_CD,
  CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD,
  CLM_REMIT_HIST_SUPRES_EOB_IN,
  CLM_REMIT_HIST_SUPRES_REMIT_IN,
  CMPL_MBRSH_IN,
  CLM_RCVD_DT,
  CLM_SRCH_DT,
  CLM_SVC_STRT_DT,
  CLM_SVC_END_DT,
  MBR_BRTH_DT,
  CLM_CHRG_AMT,
  CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CNSD_CHRG_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT,
  ALPHA_PFX_SUB_ID,
  CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  GRP_UNIQ_KEY,
  GRP_ID,
  GRP_NM,
  MBR_UNIQ_KEY,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  PROV_BILL_SVC_ID,
  PROV_PD_PROV_ID,
  PROV_PCP_PROV_ID,
  PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID,
  SUB_UNIQ_KEY,
  LAST_UPDT_RUN_CYC_NO,
  PCA_TYP_CD
FROM {ClmMartOwner}.W_CLM_INIT
WHERE CMT_CT = {CommitCnt}
"""
df_W_CLM_INIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_W_CLM_INIT)
    .options(**jdbc_props_W_CLM_INIT)
    .option("query", extract_query_W_CLM_INIT)
    .load()
)

# Trans1 (CTransformerStage)
df_Trans1 = df_W_CLM_INIT.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_FINL_DISP_CD").alias("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD").alias("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_REMIT_HIST_SUPRES_EOB_IN").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    col("CLM_REMIT_HIST_SUPRES_REMIT_IN").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    col("CMPL_MBRSH_IN").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT").alias("CLM_RCVD_DT"),
    col("CLM_SRCH_DT").alias("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT").alias("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT").alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT").alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT").alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID").alias("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID").alias("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID").alias("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID").alias("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID").alias("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    when(col("PCA_TYP_CD").isNull() | (col("PCA_TYP_CD") == 'NA'), None).otherwise(col("PCA_TYP_CD")).alias("PCA_TYP_CD")
)

# CLM_DM_INIT_CLM (CODBCStage) - MERGE
df_Trans1_write = df_Trans1.select(
    rpad(col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD") if False else col("SRC_SYS_CD"),
    rpad(col("CLM_ID"), 1, " ").alias("CLM_ID") if False else col("CLM_ID"),
    col("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD"),
    rpad(col("CLM_REMIT_HIST_SUPRES_EOB_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    rpad(col("CLM_REMIT_HIST_SUPRES_REMIT_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    rpad(col("CMPL_MBRSH_IN"), 1, " ").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT"),
    col("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY"),
    col("GRP_ID"),
    col("GRP_NM"),
    col("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY"),
    col("PCA_TYP_CD")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM_temp", jdbc_url_W_CLM_INIT, jdbc_props_W_CLM_INIT)

df_Trans1_write.write.format("jdbc")\
    .option("url", jdbc_url_W_CLM_INIT)\
    .options(**jdbc_props_W_CLM_INIT)\
    .option("dbtable", "STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM_temp")\
    .mode("overwrite")\
    .save()

merge_sql_CLM_DM_INIT_CLM = f"""
MERGE {ClmMartOwner}.CLM_DM_INIT_CLM AS T
USING STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_FINL_DISP_CD = S.CLM_FINL_DISP_CD,
    T.CLM_STTUS_CD = S.CLM_STTUS_CD,
    T.CLM_TRNSMSN_STTUS_CD = S.CLM_TRNSMSN_STTUS_CD,
    T.CLM_TYP_CD = S.CLM_TYP_CD,
    T.CLM_REMIT_HIST_SUPRES_EOB_IN = S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    T.CLM_REMIT_HIST_SUPRES_REMIT_IN = S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    T.CMPL_MBRSH_IN = S.CMPL_MBRSH_IN,
    T.CLM_RCVD_DT = S.CLM_RCVD_DT,
    T.CLM_SRCH_DT = S.CLM_SRCH_DT,
    T.CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
    T.CLM_SVC_END_DT = S.CLM_SVC_END_DT,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.CLM_CHRG_AMT = S.CLM_CHRG_AMT,
    T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
    T.CLM_REMIT_HIST_CNSD_CHRG_AMT = S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    T.CLM_REMIT_HIST_NO_RESP_AMT = S.CLM_REMIT_HIST_NO_RESP_AMT,
    T.CLM_REMIT_HIST_PATN_RESP_AMT = S.CLM_REMIT_HIST_PATN_RESP_AMT,
    T.ALPHA_PFX_SUB_ID = S.ALPHA_PFX_SUB_ID,
    T.CLM_SUB_ID = S.CLM_SUB_ID,
    T.CMN_PRCT_PRSCRB_CMN_PRCT_ID = S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_ID = S.GRP_ID,
    T.GRP_NM = S.GRP_NM,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_FIRST_NM = S.MBR_FIRST_NM,
    T.MBR_MIDINIT = S.MBR_MIDINIT,
    T.MBR_LAST_NM = S.MBR_LAST_NM,
    T.PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID,
    T.PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
    T.PROV_PCP_PROV_ID = S.PROV_PCP_PROV_ID,
    T.PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
    T.PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
    T.PROV_SVC_PROV_ID = S.PROV_SVC_PROV_ID,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.PCA_TYP_CD = S.PCA_TYP_CD
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_FINL_DISP_CD,
    CLM_STTUS_CD,
    CLM_TRNSMSN_STTUS_CD,
    CLM_TYP_CD,
    CLM_REMIT_HIST_SUPRES_EOB_IN,
    CLM_REMIT_HIST_SUPRES_REMIT_IN,
    CMPL_MBRSH_IN,
    CLM_RCVD_DT,
    CLM_SRCH_DT,
    CLM_SVC_STRT_DT,
    CLM_SVC_END_DT,
    MBR_BRTH_DT,
    CLM_CHRG_AMT,
    CLM_PAYBL_AMT,
    CLM_REMIT_HIST_CNSD_CHRG_AMT,
    CLM_REMIT_HIST_NO_RESP_AMT,
    CLM_REMIT_HIST_PATN_RESP_AMT,
    ALPHA_PFX_SUB_ID,
    CLM_SUB_ID,
    CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    GRP_UNIQ_KEY,
    GRP_ID,
    GRP_NM,
    MBR_UNIQ_KEY,
    MBR_FIRST_NM,
    MBR_MIDINIT,
    MBR_LAST_NM,
    PROV_BILL_SVC_ID,
    PROV_PD_PROV_ID,
    PROV_PCP_PROV_ID,
    PROV_REL_GRP_PROV_ID,
    PROV_REL_IPA_PROV_ID,
    PROV_SVC_PROV_ID,
    SUB_UNIQ_KEY,
    PCA_TYP_CD
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_FINL_DISP_CD,
    S.CLM_STTUS_CD,
    S.CLM_TRNSMSN_STTUS_CD,
    S.CLM_TYP_CD,
    S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    S.CMPL_MBRSH_IN,
    S.CLM_RCVD_DT,
    S.CLM_SRCH_DT,
    S.CLM_SVC_STRT_DT,
    S.CLM_SVC_END_DT,
    S.MBR_BRTH_DT,
    S.CLM_CHRG_AMT,
    S.CLM_PAYBL_AMT,
    S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    S.CLM_REMIT_HIST_NO_RESP_AMT,
    S.CLM_REMIT_HIST_PATN_RESP_AMT,
    S.ALPHA_PFX_SUB_ID,
    S.CLM_SUB_ID,
    S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    S.GRP_UNIQ_KEY,
    S.GRP_ID,
    S.GRP_NM,
    S.MBR_UNIQ_KEY,
    S.MBR_FIRST_NM,
    S.MBR_MIDINIT,
    S.MBR_LAST_NM,
    S.PROV_BILL_SVC_ID,
    S.PROV_PD_PROV_ID,
    S.PROV_PCP_PROV_ID,
    S.PROV_REL_GRP_PROV_ID,
    S.PROV_REL_IPA_PROV_ID,
    S.PROV_SVC_PROV_ID,
    S.SUB_UNIQ_KEY,
    S.PCA_TYP_CD
  )
;
"""
execute_dml(merge_sql_CLM_DM_INIT_CLM, jdbc_url_W_CLM_INIT, jdbc_props_W_CLM_INIT)

# --------------------------------------------------------------------------------
# W_CLM_INIT2 (CODBCStage) - READ
jdbc_url_W_CLM_INIT2, jdbc_props_W_CLM_INIT2 = get_db_config(clmmart_secret_name)
extract_query_W_CLM_INIT2 = f"""
SELECT
  CMT_CT,
  SRC_SYS_CD,
  CLM_ID,
  CLM_FINL_DISP_CD,
  CLM_STTUS_CD,
  CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD,
  CLM_REMIT_HIST_SUPRES_EOB_IN,
  CLM_REMIT_HIST_SUPRES_REMIT_IN,
  CMPL_MBRSH_IN,
  CLM_RCVD_DT,
  CLM_SRCH_DT,
  CLM_SVC_STRT_DT,
  CLM_SVC_END_DT,
  MBR_BRTH_DT,
  CLM_CHRG_AMT,
  CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CNSD_CHRG_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT,
  ALPHA_PFX_SUB_ID,
  CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  GRP_UNIQ_KEY,
  GRP_ID,
  GRP_NM,
  MBR_UNIQ_KEY,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  PROV_BILL_SVC_ID,
  PROV_PD_PROV_ID,
  PROV_PCP_PROV_ID,
  PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID,
  SUB_UNIQ_KEY,
  LAST_UPDT_RUN_CYC_NO,
  PCA_TYP_CD
FROM {ClmMartOwner}.W_CLM_INIT
WHERE CMT_CT = {CommitCnt2}
"""
df_W_CLM_INIT2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_W_CLM_INIT2)
    .options(**jdbc_props_W_CLM_INIT2)
    .option("query", extract_query_W_CLM_INIT2)
    .load()
)

# Trans2 (CTransformerStage)
df_Trans2 = df_W_CLM_INIT2.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_FINL_DISP_CD").alias("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD").alias("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_REMIT_HIST_SUPRES_EOB_IN").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    col("CLM_REMIT_HIST_SUPRES_REMIT_IN").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    col("CMPL_MBRSH_IN").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT").alias("CLM_RCVD_DT"),
    col("CLM_SRCH_DT").alias("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT").alias("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT").alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT").alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT").alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID").alias("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID").alias("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID").alias("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID").alias("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID").alias("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    when(col("PCA_TYP_CD").isNull() | (col("PCA_TYP_CD") == 'NA'), None).otherwise(col("PCA_TYP_CD")).alias("PCA_TYP_CD")
)

# CLM_DM_INIT_CLM2 (CODBCStage) - MERGE
df_Trans2_write = df_Trans2.select(
    rpad(col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD") if False else col("SRC_SYS_CD"),
    rpad(col("CLM_ID"), 1, " ").alias("CLM_ID") if False else col("CLM_ID"),
    col("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD"),
    rpad(col("CLM_REMIT_HIST_SUPRES_EOB_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    rpad(col("CLM_REMIT_HIST_SUPRES_REMIT_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    rpad(col("CMPL_MBRSH_IN"), 1, " ").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT"),
    col("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY"),
    col("GRP_ID"),
    col("GRP_NM"),
    col("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY"),
    col("PCA_TYP_CD")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM2_temp", jdbc_url_W_CLM_INIT2, jdbc_props_W_CLM_INIT2)

df_Trans2_write.write.format("jdbc")\
    .option("url", jdbc_url_W_CLM_INIT2)\
    .options(**jdbc_props_W_CLM_INIT2)\
    .option("dbtable", "STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM2_temp")\
    .mode("overwrite")\
    .save()

merge_sql_CLM_DM_INIT_CLM2 = f"""
MERGE {ClmMartOwner}.CLM_DM_INIT_CLM AS T
USING STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM2_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_FINL_DISP_CD = S.CLM_FINL_DISP_CD,
    T.CLM_STTUS_CD = S.CLM_STTUS_CD,
    T.CLM_TRNSMSN_STTUS_CD = S.CLM_TRNSMSN_STTUS_CD,
    T.CLM_TYP_CD = S.CLM_TYP_CD,
    T.CLM_REMIT_HIST_SUPRES_EOB_IN = S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    T.CLM_REMIT_HIST_SUPRES_REMIT_IN = S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    T.CMPL_MBRSH_IN = S.CMPL_MBRSH_IN,
    T.CLM_RCVD_DT = S.CLM_RCVD_DT,
    T.CLM_SRCH_DT = S.CLM_SRCH_DT,
    T.CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
    T.CLM_SVC_END_DT = S.CLM_SVC_END_DT,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.CLM_CHRG_AMT = S.CLM_CHRG_AMT,
    T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
    T.CLM_REMIT_HIST_CNSD_CHRG_AMT = S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    T.CLM_REMIT_HIST_NO_RESP_AMT = S.CLM_REMIT_HIST_NO_RESP_AMT,
    T.CLM_REMIT_HIST_PATN_RESP_AMT = S.CLM_REMIT_HIST_PATN_RESP_AMT,
    T.ALPHA_PFX_SUB_ID = S.ALPHA_PFX_SUB_ID,
    T.CLM_SUB_ID = S.CLM_SUB_ID,
    T.CMN_PRCT_PRSCRB_CMN_PRCT_ID = S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_ID = S.GRP_ID,
    T.GRP_NM = S.GRP_NM,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_FIRST_NM = S.MBR_FIRST_NM,
    T.MBR_MIDINIT = S.MBR_MIDINIT,
    T.MBR_LAST_NM = S.MBR_LAST_NM,
    T.PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID,
    T.PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
    T.PROV_PCP_PROV_ID = S.PROV_PCP_PROV_ID,
    T.PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
    T.PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
    T.PROV_SVC_PROV_ID = S.PROV_SVC_PROV_ID,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.PCA_TYP_CD = S.PCA_TYP_CD
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_FINL_DISP_CD,
    CLM_STTUS_CD,
    CLM_TRNSMSN_STTUS_CD,
    CLM_TYP_CD,
    CLM_REMIT_HIST_SUPRES_EOB_IN,
    CLM_REMIT_HIST_SUPRES_REMIT_IN,
    CMPL_MBRSH_IN,
    CLM_RCVD_DT,
    CLM_SRCH_DT,
    CLM_SVC_STRT_DT,
    CLM_SVC_END_DT,
    MBR_BRTH_DT,
    CLM_CHRG_AMT,
    CLM_PAYBL_AMT,
    CLM_REMIT_HIST_CNSD_CHRG_AMT,
    CLM_REMIT_HIST_NO_RESP_AMT,
    CLM_REMIT_HIST_PATN_RESP_AMT,
    ALPHA_PFX_SUB_ID,
    CLM_SUB_ID,
    CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    GRP_UNIQ_KEY,
    GRP_ID,
    GRP_NM,
    MBR_UNIQ_KEY,
    MBR_FIRST_NM,
    MBR_MIDINIT,
    MBR_LAST_NM,
    PROV_BILL_SVC_ID,
    PROV_PD_PROV_ID,
    PROV_PCP_PROV_ID,
    PROV_REL_GRP_PROV_ID,
    PROV_REL_IPA_PROV_ID,
    PROV_SVC_PROV_ID,
    SUB_UNIQ_KEY,
    PCA_TYP_CD
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_FINL_DISP_CD,
    S.CLM_STTUS_CD,
    S.CLM_TRNSMSN_STTUS_CD,
    S.CLM_TYP_CD,
    S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    S.CMPL_MBRSH_IN,
    S.CLM_RCVD_DT,
    S.CLM_SRCH_DT,
    S.CLM_SVC_STRT_DT,
    S.CLM_SVC_END_DT,
    S.MBR_BRTH_DT,
    S.CLM_CHRG_AMT,
    S.CLM_PAYBL_AMT,
    S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    S.CLM_REMIT_HIST_NO_RESP_AMT,
    S.CLM_REMIT_HIST_PATN_RESP_AMT,
    S.ALPHA_PFX_SUB_ID,
    S.CLM_SUB_ID,
    S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    S.GRP_UNIQ_KEY,
    S.GRP_ID,
    S.GRP_NM,
    S.MBR_UNIQ_KEY,
    S.MBR_FIRST_NM,
    S.MBR_MIDINIT,
    S.MBR_LAST_NM,
    S.PROV_BILL_SVC_ID,
    S.PROV_PD_PROV_ID,
    S.PROV_PCP_PROV_ID,
    S.PROV_REL_GRP_PROV_ID,
    S.PROV_REL_IPA_PROV_ID,
    S.PROV_SVC_PROV_ID,
    S.SUB_UNIQ_KEY,
    S.PCA_TYP_CD
  )
;
"""
execute_dml(merge_sql_CLM_DM_INIT_CLM2, jdbc_url_W_CLM_INIT2, jdbc_props_W_CLM_INIT2)

# --------------------------------------------------------------------------------
# W_CLM_INIT3 (CODBCStage) - READ
jdbc_url_W_CLM_INIT3, jdbc_props_W_CLM_INIT3 = get_db_config(clmmart_secret_name)
extract_query_W_CLM_INIT3 = f"""
SELECT
  CMT_CT,
  SRC_SYS_CD,
  CLM_ID,
  CLM_FINL_DISP_CD,
  CLM_STTUS_CD,
  CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD,
  CLM_REMIT_HIST_SUPRES_EOB_IN,
  CLM_REMIT_HIST_SUPRES_REMIT_IN,
  CMPL_MBRSH_IN,
  CLM_RCVD_DT,
  CLM_SRCH_DT,
  CLM_SVC_STRT_DT,
  CLM_SVC_END_DT,
  MBR_BRTH_DT,
  CLM_CHRG_AMT,
  CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CNSD_CHRG_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT,
  ALPHA_PFX_SUB_ID,
  CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  GRP_UNIQ_KEY,
  GRP_ID,
  GRP_NM,
  MBR_UNIQ_KEY,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  PROV_BILL_SVC_ID,
  PROV_PD_PROV_ID,
  PROV_PCP_PROV_ID,
  PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID,
  SUB_UNIQ_KEY,
  LAST_UPDT_RUN_CYC_NO,
  PCA_TYP_CD
FROM {ClmMartOwner}.W_CLM_INIT
WHERE CMT_CT = {CommitCnt3}
"""
df_W_CLM_INIT3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_W_CLM_INIT3)
    .options(**jdbc_props_W_CLM_INIT3)
    .option("query", extract_query_W_CLM_INIT3)
    .load()
)

# Trans3 (CTransformerStage)
df_Trans3 = df_W_CLM_INIT3.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_FINL_DISP_CD").alias("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD").alias("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_REMIT_HIST_SUPRES_EOB_IN").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    col("CLM_REMIT_HIST_SUPRES_REMIT_IN").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    col("CMPL_MBRSH_IN").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT").alias("CLM_RCVD_DT"),
    col("CLM_SRCH_DT").alias("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT").alias("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT").alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT").alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT").alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID").alias("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID").alias("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID").alias("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID").alias("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID").alias("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    when(col("PCA_TYP_CD").isNull() | (col("PCA_TYP_CD") == 'NA'), None).otherwise(col("PCA_TYP_CD")).alias("PCA_TYP_CD")
)

# CLM_DM_INIT_CLM3 (CODBCStage) - MERGE
df_Trans3_write = df_Trans3.select(
    rpad(col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD") if False else col("SRC_SYS_CD"),
    rpad(col("CLM_ID"), 1, " ").alias("CLM_ID") if False else col("CLM_ID"),
    col("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD"),
    rpad(col("CLM_REMIT_HIST_SUPRES_EOB_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    rpad(col("CLM_REMIT_HIST_SUPRES_REMIT_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    rpad(col("CMPL_MBRSH_IN"), 1, " ").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT"),
    col("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY"),
    col("GRP_ID"),
    col("GRP_NM"),
    col("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY"),
    col("PCA_TYP_CD")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM3_temp", jdbc_url_W_CLM_INIT3, jdbc_props_W_CLM_INIT3)

df_Trans3_write.write.format("jdbc")\
    .option("url", jdbc_url_W_CLM_INIT3)\
    .options(**jdbc_props_W_CLM_INIT3)\
    .option("dbtable", "STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM3_temp")\
    .mode("overwrite")\
    .save()

merge_sql_CLM_DM_INIT_CLM3 = f"""
MERGE {ClmMartOwner}.CLM_DM_INIT_CLM AS T
USING STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM3_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_FINL_DISP_CD = S.CLM_FINL_DISP_CD,
    T.CLM_STTUS_CD = S.CLM_STTUS_CD,
    T.CLM_TRNSMSN_STTUS_CD = S.CLM_TRNSMSN_STTUS_CD,
    T.CLM_TYP_CD = S.CLM_TYP_CD,
    T.CLM_REMIT_HIST_SUPRES_EOB_IN = S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    T.CLM_REMIT_HIST_SUPRES_REMIT_IN = S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    T.CMPL_MBRSH_IN = S.CMPL_MBRSH_IN,
    T.CLM_RCVD_DT = S.CLM_RCVD_DT,
    T.CLM_SRCH_DT = S.CLM_SRCH_DT,
    T.CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
    T.CLM_SVC_END_DT = S.CLM_SVC_END_DT,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.CLM_CHRG_AMT = S.CLM_CHRG_AMT,
    T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
    T.CLM_REMIT_HIST_CNSD_CHRG_AMT = S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    T.CLM_REMIT_HIST_NO_RESP_AMT = S.CLM_REMIT_HIST_NO_RESP_AMT,
    T.CLM_REMIT_HIST_PATN_RESP_AMT = S.CLM_REMIT_HIST_PATN_RESP_AMT,
    T.ALPHA_PFX_SUB_ID = S.ALPHA_PFX_SUB_ID,
    T.CLM_SUB_ID = S.CLM_SUB_ID,
    T.CMN_PRCT_PRSCRB_CMN_PRCT_ID = S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_ID = S.GRP_ID,
    T.GRP_NM = S.GRP_NM,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_FIRST_NM = S.MBR_FIRST_NM,
    T.MBR_MIDINIT = S.MBR_MIDINIT,
    T.MBR_LAST_NM = S.MBR_LAST_NM,
    T.PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID,
    T.PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
    T.PROV_PCP_PROV_ID = S.PROV_PCP_PROV_ID,
    T.PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
    T.PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
    T.PROV_SVC_PROV_ID = S.PROV_SVC_PROV_ID,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.PCA_TYP_CD = S.PCA_TYP_CD
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_FINL_DISP_CD,
    CLM_STTUS_CD,
    CLM_TRNSMSN_STTUS_CD,
    CLM_TYP_CD,
    CLM_REMIT_HIST_SUPRES_EOB_IN,
    CLM_REMIT_HIST_SUPRES_REMIT_IN,
    CMPL_MBRSH_IN,
    CLM_RCVD_DT,
    CLM_SRCH_DT,
    CLM_SVC_STRT_DT,
    CLM_SVC_END_DT,
    MBR_BRTH_DT,
    CLM_CHRG_AMT,
    CLM_PAYBL_AMT,
    CLM_REMIT_HIST_CNSD_CHRG_AMT,
    CLM_REMIT_HIST_NO_RESP_AMT,
    CLM_REMIT_HIST_PATN_RESP_AMT,
    ALPHA_PFX_SUB_ID,
    CLM_SUB_ID,
    CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    GRP_UNIQ_KEY,
    GRP_ID,
    GRP_NM,
    MBR_UNIQ_KEY,
    MBR_FIRST_NM,
    MBR_MIDINIT,
    MBR_LAST_NM,
    PROV_BILL_SVC_ID,
    PROV_PD_PROV_ID,
    PROV_PCP_PROV_ID,
    PROV_REL_GRP_PROV_ID,
    PROV_REL_IPA_PROV_ID,
    PROV_SVC_PROV_ID,
    SUB_UNIQ_KEY,
    PCA_TYP_CD
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_FINL_DISP_CD,
    S.CLM_STTUS_CD,
    S.CLM_TRNSMSN_STTUS_CD,
    S.CLM_TYP_CD,
    S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    S.CMPL_MBRSH_IN,
    S.CLM_RCVD_DT,
    S.CLM_SRCH_DT,
    S.CLM_SVC_STRT_DT,
    S.CLM_SVC_END_DT,
    S.MBR_BRTH_DT,
    S.CLM_CHRG_AMT,
    S.CLM_PAYBL_AMT,
    S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    S.CLM_REMIT_HIST_NO_RESP_AMT,
    S.CLM_REMIT_HIST_PATN_RESP_AMT,
    S.ALPHA_PFX_SUB_ID,
    S.CLM_SUB_ID,
    S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    S.GRP_UNIQ_KEY,
    S.GRP_ID,
    S.GRP_NM,
    S.MBR_UNIQ_KEY,
    S.MBR_FIRST_NM,
    S.MBR_MIDINIT,
    S.MBR_LAST_NM,
    S.PROV_BILL_SVC_ID,
    S.PROV_PD_PROV_ID,
    S.PROV_PCP_PROV_ID,
    S.PROV_REL_GRP_PROV_ID,
    S.PROV_REL_IPA_PROV_ID,
    S.PROV_SVC_PROV_ID,
    S.SUB_UNIQ_KEY,
    S.PCA_TYP_CD
  )
;
"""
execute_dml(merge_sql_CLM_DM_INIT_CLM3, jdbc_url_W_CLM_INIT3, jdbc_props_W_CLM_INIT3)

# --------------------------------------------------------------------------------
# W_CLM_INIT4 (CODBCStage) - READ
jdbc_url_W_CLM_INIT4, jdbc_props_W_CLM_INIT4 = get_db_config(clmmart_secret_name)
extract_query_W_CLM_INIT4 = f"""
SELECT
  CMT_CT,
  SRC_SYS_CD,
  CLM_ID,
  CLM_FINL_DISP_CD,
  CLM_STTUS_CD,
  CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD,
  CLM_REMIT_HIST_SUPRES_EOB_IN,
  CLM_REMIT_HIST_SUPRES_REMIT_IN,
  CMPL_MBRSH_IN,
  CLM_RCVD_DT,
  CLM_SRCH_DT,
  CLM_SVC_STRT_DT,
  CLM_SVC_END_DT,
  MBR_BRTH_DT,
  CLM_CHRG_AMT,
  CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CNSD_CHRG_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT,
  ALPHA_PFX_SUB_ID,
  CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  GRP_UNIQ_KEY,
  GRP_ID,
  GRP_NM,
  MBR_UNIQ_KEY,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  PROV_BILL_SVC_ID,
  PROV_PD_PROV_ID,
  PROV_PCP_PROV_ID,
  PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID,
  SUB_UNIQ_KEY,
  LAST_UPDT_RUN_CYC_NO,
  PCA_TYP_CD
FROM {ClmMartOwner}.W_CLM_INIT
WHERE CMT_CT = {CommitCnt4}
"""
df_W_CLM_INIT4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_W_CLM_INIT4)
    .options(**jdbc_props_W_CLM_INIT4)
    .option("query", extract_query_W_CLM_INIT4)
    .load()
)

# Trans4 (CTransformerStage)
df_Trans4 = df_W_CLM_INIT4.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_FINL_DISP_CD").alias("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD").alias("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_REMIT_HIST_SUPRES_EOB_IN").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    col("CLM_REMIT_HIST_SUPRES_REMIT_IN").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    col("CMPL_MBRSH_IN").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT").alias("CLM_RCVD_DT"),
    col("CLM_SRCH_DT").alias("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT").alias("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT").alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT").alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT").alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID").alias("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID").alias("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID").alias("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID").alias("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID").alias("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    when(col("PCA_TYP_CD").isNull() | (col("PCA_TYP_CD") == 'NA'), None).otherwise(col("PCA_TYP_CD")).alias("PCA_TYP_CD")
)

# CLM_DM_INIT_CLM4 (CODBCStage) - MERGE
df_Trans4_write = df_Trans4.select(
    rpad(col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD") if False else col("SRC_SYS_CD"),
    rpad(col("CLM_ID"), 1, " ").alias("CLM_ID") if False else col("CLM_ID"),
    col("CLM_FINL_DISP_CD"),
    col("CLM_STTUS_CD"),
    col("CLM_TRNSMSN_STTUS_CD"),
    col("CLM_TYP_CD"),
    rpad(col("CLM_REMIT_HIST_SUPRES_EOB_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    rpad(col("CLM_REMIT_HIST_SUPRES_REMIT_IN"), 1, " ").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    rpad(col("CMPL_MBRSH_IN"), 1, " ").alias("CMPL_MBRSH_IN"),
    col("CLM_RCVD_DT"),
    col("CLM_SRCH_DT"),
    col("CLM_SVC_STRT_DT"),
    col("CLM_SVC_END_DT"),
    col("MBR_BRTH_DT"),
    col("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT"),
    col("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    col("CLM_REMIT_HIST_NO_RESP_AMT"),
    col("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("ALPHA_PFX_SUB_ID"),
    col("CLM_SUB_ID"),
    col("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY"),
    col("GRP_ID"),
    col("GRP_NM"),
    col("MBR_UNIQ_KEY"),
    col("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("PROV_BILL_SVC_ID"),
    col("PROV_PD_PROV_ID"),
    col("PROV_PCP_PROV_ID"),
    col("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_IPA_PROV_ID"),
    col("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY"),
    col("PCA_TYP_CD")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM4_temp", jdbc_url_W_CLM_INIT4, jdbc_props_W_CLM_INIT4)

df_Trans4_write.write.format("jdbc")\
    .option("url", jdbc_url_W_CLM_INIT4)\
    .options(**jdbc_props_W_CLM_INIT4)\
    .option("dbtable", "STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM4_temp")\
    .mode("overwrite")\
    .save()

merge_sql_CLM_DM_INIT_CLM4 = f"""
MERGE {ClmMartOwner}.CLM_DM_INIT_CLM AS T
USING STAGING.IdsClmMartClmInitUpd_CLM_DM_INIT_CLM4_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_FINL_DISP_CD = S.CLM_FINL_DISP_CD,
    T.CLM_STTUS_CD = S.CLM_STTUS_CD,
    T.CLM_TRNSMSN_STTUS_CD = S.CLM_TRNSMSN_STTUS_CD,
    T.CLM_TYP_CD = S.CLM_TYP_CD,
    T.CLM_REMIT_HIST_SUPRES_EOB_IN = S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    T.CLM_REMIT_HIST_SUPRES_REMIT_IN = S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    T.CMPL_MBRSH_IN = S.CMPL_MBRSH_IN,
    T.CLM_RCVD_DT = S.CLM_RCVD_DT,
    T.CLM_SRCH_DT = S.CLM_SRCH_DT,
    T.CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
    T.CLM_SVC_END_DT = S.CLM_SVC_END_DT,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.CLM_CHRG_AMT = S.CLM_CHRG_AMT,
    T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
    T.CLM_REMIT_HIST_CNSD_CHRG_AMT = S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    T.CLM_REMIT_HIST_NO_RESP_AMT = S.CLM_REMIT_HIST_NO_RESP_AMT,
    T.CLM_REMIT_HIST_PATN_RESP_AMT = S.CLM_REMIT_HIST_PATN_RESP_AMT,
    T.ALPHA_PFX_SUB_ID = S.ALPHA_PFX_SUB_ID,
    T.CLM_SUB_ID = S.CLM_SUB_ID,
    T.CMN_PRCT_PRSCRB_CMN_PRCT_ID = S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_ID = S.GRP_ID,
    T.GRP_NM = S.GRP_NM,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_FIRST_NM = S.MBR_FIRST_NM,
    T.MBR_MIDINIT = S.MBR_MIDINIT,
    T.MBR_LAST_NM = S.MBR_LAST_NM,
    T.PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID,
    T.PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
    T.PROV_PCP_PROV_ID = S.PROV_PCP_PROV_ID,
    T.PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
    T.PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
    T.PROV_SVC_PROV_ID = S.PROV_SVC_PROV_ID,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.PCA_TYP_CD = S.PCA_TYP_CD
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_FINL_DISP_CD,
    CLM_STTUS_CD,
    CLM_TRNSMSN_STTUS_CD,
    CLM_TYP_CD,
    CLM_REMIT_HIST_SUPRES_EOB_IN,
    CLM_REMIT_HIST_SUPRES_REMIT_IN,
    CMPL_MBRSH_IN,
    CLM_RCVD_DT,
    CLM_SRCH_DT,
    CLM_SVC_STRT_DT,
    CLM_SVC_END_DT,
    MBR_BRTH_DT,
    CLM_CHRG_AMT,
    CLM_PAYBL_AMT,
    CLM_REMIT_HIST_CNSD_CHRG_AMT,
    CLM_REMIT_HIST_NO_RESP_AMT,
    CLM_REMIT_HIST_PATN_RESP_AMT,
    ALPHA_PFX_SUB_ID,
    CLM_SUB_ID,
    CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    GRP_UNIQ_KEY,
    GRP_ID,
    GRP_NM,
    MBR_UNIQ_KEY,
    MBR_FIRST_NM,
    MBR_MIDINIT,
    MBR_LAST_NM,
    PROV_BILL_SVC_ID,
    PROV_PD_PROV_ID,
    PROV_PCP_PROV_ID,
    PROV_REL_GRP_PROV_ID,
    PROV_REL_IPA_PROV_ID,
    PROV_SVC_PROV_ID,
    SUB_UNIQ_KEY,
    PCA_TYP_CD
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_FINL_DISP_CD,
    S.CLM_STTUS_CD,
    S.CLM_TRNSMSN_STTUS_CD,
    S.CLM_TYP_CD,
    S.CLM_REMIT_HIST_SUPRES_EOB_IN,
    S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
    S.CMPL_MBRSH_IN,
    S.CLM_RCVD_DT,
    S.CLM_SRCH_DT,
    S.CLM_SVC_STRT_DT,
    S.CLM_SVC_END_DT,
    S.MBR_BRTH_DT,
    S.CLM_CHRG_AMT,
    S.CLM_PAYBL_AMT,
    S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
    S.CLM_REMIT_HIST_NO_RESP_AMT,
    S.CLM_REMIT_HIST_PATN_RESP_AMT,
    S.ALPHA_PFX_SUB_ID,
    S.CLM_SUB_ID,
    S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
    S.GRP_UNIQ_KEY,
    S.GRP_ID,
    S.GRP_NM,
    S.MBR_UNIQ_KEY,
    S.MBR_FIRST_NM,
    S.MBR_MIDINIT,
    S.MBR_LAST_NM,
    S.PROV_BILL_SVC_ID,
    S.PROV_PD_PROV_ID,
    S.PROV_PCP_PROV_ID,
    S.PROV_REL_GRP_PROV_ID,
    S.PROV_REL_IPA_PROV_ID,
    S.PROV_SVC_PROV_ID,
    S.SUB_UNIQ_KEY,
    S.PCA_TYP_CD
  )
;
"""
execute_dml(merge_sql_CLM_DM_INIT_CLM4, jdbc_url_W_CLM_INIT4, jdbc_props_W_CLM_INIT4)