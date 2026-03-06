# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids  and update edw.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                		  Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------         		 --------------------------------       -------------------------------   ----------------------------       
# MAGIC SAndrew                     2009-11-15      3833 Remit Update    new program                                                     		  devlIDS                             Steph Goddard         12/10/2009
# MAGIC Kalyan Neelam         2010-02-09        4278                     Extract fields - MCAID_STTUS_ID,                  
# MAGIC                                                                                        PATN_PD_AMT from CLM table                               		 IntegrateCurDevl              Steph Goddard          03/08/2010
# MAGIC Raja Gummadi          2012-07-03        4896                    Added new column at the end                                   		 IntegrateNewDevl         Brent Leland              07-10-2012
# MAGIC                                                                                        CLM_SUBMT_ICD_VRSN_CD_SK
# MAGIC Shanmugam A 	 2017-03-02         5321                    SQL in stage 'IDS_W_CLM_LN_REMIT_DSALW_UPDT' 	IntegrateDev2
# MAGIC 						will be aliased to match with stage meta data
# MAGIC 	         
# MAGIC Mohan Karnati           2019-06-20        ADO-73034        Adding CLM_TXNMY_CD filed to  IDS_CLM_on_DRVR and                 IntegrateDev1	               Kalyan Neelam          2019-07-01
# MAGIC                                                                                                    passing it till CLM_IDSRemitDsalwUpdt_dat stage 
# MAGIC 
# MAGIC 
# MAGIC Giri  Mallavaram    2020-04-16         PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2       Kalyan Neelam     2020-04-16

# MAGIC IDS Claim Remit Update
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
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT \n\nSRC_SYS_CD_SK,\nCLM_ID,\nSUM ( DIFF_REMIT_DSALW_AMT) AS DIFF_REMIT_DSALW_AMT\n\nFROM {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT \n\nGROUP BY \n\nSRC_SYS_CD_SK,\nCLM_ID"
df_IDS_W_CLM_LN_REMIT_DSALW_UPDT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT \nDISTINCT \nCLM.SRC_SYS_CD_SK,\nCLM.CLM_ID,\nCLM.CLM_SK,\nCLM.CRT_RUN_CYC_EXCTN_SK,\nCLM.LAST_UPDT_RUN_CYC_EXCTN_SK,\nCLM.ADJ_FROM_CLM_SK,\nCLM.ADJ_TO_CLM_SK,\nCLM.ALPHA_PFX_SK,\nCLM.CLM_EOB_EXCD_SK,CLM.CLS_SK,\nCLM.CLS_PLN_SK,CLM.EXPRNC_CAT_SK,\nCLM.FNCL_LOB_SK,\nCLM.GRP_SK,CLM.MBR_SK,\nCLM.NTWK_SK,\nCLM.PROD_SK,CLM.SUBGRP_SK,CLM.SUB_SK,\nCLM.CLM_ACDNT_CD_SK,\nCLM.CLM_ACDNT_ST_CD_SK,\nCLM.CLM_ACTV_BCBS_PLN_CD_SK,\nCLM.CLM_AGMNT_SRC_CD_SK,\nCLM.CLM_BTCH_ACTN_CD_SK,\nCLM.CLM_CAP_CD_SK,\nCLM.CLM_CAT_CD_SK,CLM.CLM_CHK_CYC_OVRD_CD_SK,\nCLM.CLM_COB_CD_SK,CLM.CLM_FINL_DISP_CD_SK,\nCLM.CLM_INPT_METH_CD_SK,CLM.\nCLM_INPT_SRC_CD_SK,\nCLM.CLM_INTER_PLN_PGM_CD_SK,\nCLM.CLM_NTWK_STTUS_CD_SK,CLM.\nCLM_NONPAR_PROV_PFX_CD_SK,\nCLM.CLM_OTHR_BNF_CD_SK,CLM.CLM_PAYE_CD_SK,\nCLM.CLM_PRCS_CTL_AGNT_PFX_CD_SK,\nCLM.CLM_SVC_DEFN_PFX_CD_SK,\nCLM.CLM_SVC_PROV_SPEC_CD_SK,\nCLM.CLM_SVC_PROV_TYP_CD_SK,\nCLM.CLM_STTUS_CD_SK,\nCLM.CLM_SUBMTTING_BCBS_PLN_CD_SK,\nCLM.CLM_SUB_BCBS_PLN_CD_SK,CLM.CLM_SUBTYP_CD_SK,\nCLM.CLM_TYP_CD_SK,CLM.ATCHMT_IN,\nCLM.CLNCL_EDIT_IN,CLM.COBRA_CLM_IN,CLM.FIRST_PASS_IN,CLM.HOST_IN,\nCLM.LTR_IN,CLM.MCARE_ASG_IN,CLM.NOTE_IN,CLM.PCA_AUDIT_IN,CLM.PCP_SUBMT_IN,CLM.PROD_OOA_IN,CLM.ACDNT_DT_SK,CLM.INPT_DT_SK,\nCLM.MBR_PLN_ELIG_DT_SK,CLM.NEXT_RVW_DT_SK,CLM.PD_DT_SK,\nCLM.PAYMT_DRAG_CYC_DT_SK,CLM.PRCS_DT_SK,CLM.RCVD_DT_SK,\nCLM.SVC_STRT_DT_SK,CLM.SVC_END_DT_SK,CLM.SMLR_ILNS_DT_SK,\nCLM.STTUS_DT_SK,CLM.WORK_UNABLE_BEG_DT_SK,CLM.WORK_UNABLE_END_DT_SK,CLM.ACDNT_AMT,CLM.ACTL_PD_AMT,CLM.ALW_AMT,CLM.DSALW_AMT,\nCLM.COINS_AMT,CLM.CNSD_CHRG_AMT,CLM.COPAY_AMT,CLM.CHRG_AMT,CLM.DEDCT_AMT,CLM.PAYBL_AMT,CLM.CLM_CT,CLM.MBR_AGE,\nCLM.ADJ_FROM_CLM_ID,CLM.ADJ_TO_CLM_ID,CLM.DOC_TX_ID,CLM.MCAID_RESUB_NO,CLM.MCARE_ID,CLM.MBR_SFX_NO,CLM.PATN_ACCT_NO,\nCLM.PAYMT_REF_ID,CLM.PROV_AGMNT_ID,\nCLM.RFRNG_PROV_TX,CLM.SUB_ID,CLM.PCA_TYP_CD_SK,\nCLM.REL_PCA_CLM_SK,CLM.REL_BASE_CLM_SK,\nCLM.REMIT_SUPRSION_AMT,\nCLM.MCAID_STTUS_ID, \nCLM.PATN_PD_AMT,\nCLM.CLM_SUBMT_ICD_VRSN_CD_SK,\nCLM.CLM_TXNMY_CD,\nBILL_PAYMT_EXCL_IN\n\nFROM       {IDSOwner}.CLM  CLM, \n                 {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT DRVR \nWHERE               DRVR.SRC_SYS_CD_SK     =   CLM.SRC_SYS_CD_SK\nAND      DRVR.CLM_ID                      =   CLM.CLM_ID"
df_IDS_CLM_on_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_clmln_remit_dsalw_updt_clm = df_IDS_CLM_on_DRVR.dropDuplicates(["SRC_SYS_CD_SK","CLM_ID"])

df_join_for_copy_of_transformer_401 = df_IDS_W_CLM_LN_REMIT_DSALW_UPDT.alias("DRVR").join(
    df_hf_clmln_remit_dsalw_updt_clm.alias("facets_clm_recs"),
    on=[
        F.col("DRVR.SRC_SYS_CD_SK") == F.col("facets_clm_recs.SRC_SYS_CD_SK"),
        F.col("DRVR.CLM_ID") == F.col("facets_clm_recs.CLM_ID")
    ],
    how="left"
)

df_with_svDsalwAmt = df_join_for_copy_of_transformer_401.withColumn(
    "svDsalwAmt",
    F.when(F.col("facets_clm_recs.CLM_SK").isNull(), F.lit(0.0))
     .otherwise(F.col("facets_clm_recs.DSALW_AMT") + F.col("DRVR.DIFF_REMIT_DSALW_AMT"))
)

df_update_clm_no_select = df_with_svDsalwAmt.filter(F.col("facets_clm_recs.CLM_SK").isNotNull())

df_update_clm = df_update_clm_no_select.select(
    F.col("facets_clm_recs.CLM_SK").alias("CLM_SK"),
    F.col("facets_clm_recs.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("facets_clm_recs.CLM_ID").alias("CLM_ID"),
    F.col("facets_clm_recs.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("facets_clm_recs.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("facets_clm_recs.ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
    F.col("facets_clm_recs.ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
    F.col("facets_clm_recs.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.col("facets_clm_recs.CLM_EOB_EXCD_SK").alias("CLM_EOB_EXCD_SK"),
    F.col("facets_clm_recs.CLS_SK").alias("CLS_SK"),
    F.col("facets_clm_recs.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("facets_clm_recs.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("facets_clm_recs.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("facets_clm_recs.GRP_SK").alias("GRP_SK"),
    F.col("facets_clm_recs.MBR_SK").alias("MBR_SK"),
    F.col("facets_clm_recs.NTWK_SK").alias("NTWK_SK"),
    F.col("facets_clm_recs.PROD_SK").alias("PROD_SK"),
    F.col("facets_clm_recs.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("facets_clm_recs.SUB_SK").alias("SUB_SK"),
    F.col("facets_clm_recs.CLM_ACDNT_CD_SK").alias("CLM_ACDNT_CD_SK"),
    F.col("facets_clm_recs.CLM_ACDNT_ST_CD_SK").alias("CLM_ACDNT_ST_CD_SK"),
    F.col("facets_clm_recs.CLM_ACTV_BCBS_PLN_CD_SK").alias("CLM_ACTV_BCBS_PLN_CD_SK"),
    F.col("facets_clm_recs.CLM_AGMNT_SRC_CD_SK").alias("CLM_AGMNT_SRC_CD_SK"),
    F.col("facets_clm_recs.CLM_BTCH_ACTN_CD_SK").alias("CLM_BTCH_ACTN_CD_SK"),
    F.col("facets_clm_recs.CLM_CAP_CD_SK").alias("CLM_CAP_CD_SK"),
    F.col("facets_clm_recs.CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    F.col("facets_clm_recs.CLM_CHK_CYC_OVRD_CD_SK").alias("CLM_CHK_CYC_OVRD_CD_SK"),
    F.col("facets_clm_recs.CLM_COB_CD_SK").alias("CLM_COB_CD_SK"),
    F.col("facets_clm_recs.CLM_FINL_DISP_CD_SK").alias("CLM_FINL_DISP_CD_SK"),
    F.col("facets_clm_recs.CLM_INPT_METH_CD_SK").alias("CLM_INPT_METH_CD_SK"),
    F.col("facets_clm_recs.CLM_INPT_SRC_CD_SK").alias("CLM_INPT_SRC_CD_SK"),
    F.col("facets_clm_recs.CLM_INTER_PLN_PGM_CD_SK").alias("CLM_INTER_PLN_PGM_CD_SK"),
    F.col("facets_clm_recs.CLM_NTWK_STTUS_CD_SK").alias("CLM_NTWK_STTUS_CD_SK"),
    F.col("facets_clm_recs.CLM_NONPAR_PROV_PFX_CD_SK").alias("CLM_NONPAR_PROV_PFX_CD_SK"),
    F.col("facets_clm_recs.CLM_OTHR_BNF_CD_SK").alias("CLM_OTHR_BNF_CD_SK"),
    F.col("facets_clm_recs.CLM_PAYE_CD_SK").alias("CLM_PAYE_CD_SK"),
    F.col("facets_clm_recs.CLM_PRCS_CTL_AGNT_PFX_CD_SK").alias("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
    F.col("facets_clm_recs.CLM_SVC_DEFN_PFX_CD_SK").alias("CLM_SVC_DEFN_PFX_CD_SK"),
    F.col("facets_clm_recs.CLM_SVC_PROV_SPEC_CD_SK").alias("CLM_SVC_PROV_SPEC_CD_SK"),
    F.col("facets_clm_recs.CLM_SVC_PROV_TYP_CD_SK").alias("CLM_SVC_PROV_TYP_CD_SK"),
    F.col("facets_clm_recs.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("facets_clm_recs.CLM_SUBMTTING_BCBS_PLN_CD_SK").alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    F.col("facets_clm_recs.CLM_SUB_BCBS_PLN_CD_SK").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    F.col("facets_clm_recs.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
    F.col("facets_clm_recs.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    F.col("facets_clm_recs.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("facets_clm_recs.CLNCL_EDIT_IN").alias("CLNCL_EDIT_IN"),
    F.col("facets_clm_recs.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("facets_clm_recs.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("facets_clm_recs.HOST_IN").alias("HOST_IN"),
    F.col("facets_clm_recs.LTR_IN").alias("LTR_IN"),
    F.col("facets_clm_recs.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("facets_clm_recs.NOTE_IN").alias("NOTE_IN"),
    F.col("facets_clm_recs.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("facets_clm_recs.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("facets_clm_recs.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("facets_clm_recs.ACDNT_DT_SK").alias("ACDNT_DT_SK"),
    F.col("facets_clm_recs.INPT_DT_SK").alias("INPT_DT_SK"),
    F.col("facets_clm_recs.MBR_PLN_ELIG_DT_SK").alias("MBR_PLN_ELIG_DT_SK"),
    F.col("facets_clm_recs.NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
    F.col("facets_clm_recs.PD_DT_SK").alias("PD_DT_SK"),
    F.col("facets_clm_recs.PAYMT_DRAG_CYC_DT_SK").alias("PAYMT_DRAG_CYC_DT_SK"),
    F.col("facets_clm_recs.PRCS_DT_SK").alias("PRCS_DT_SK"),
    F.col("facets_clm_recs.RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("facets_clm_recs.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("facets_clm_recs.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    F.col("facets_clm_recs.SMLR_ILNS_DT_SK").alias("SMLR_ILNS_DT_SK"),
    F.col("facets_clm_recs.STTUS_DT_SK").alias("STTUS_DT_SK"),
    F.col("facets_clm_recs.WORK_UNABLE_BEG_DT_SK").alias("WORK_UNABLE_BEG_DT_SK"),
    F.col("facets_clm_recs.WORK_UNABLE_END_DT_SK").alias("WORK_UNABLE_END_DT_SK"),
    F.col("facets_clm_recs.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("facets_clm_recs.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("facets_clm_recs.ALW_AMT").alias("ALW_AMT"),
    F.col("svDsalwAmt").alias("DSALW_AMT"),
    F.col("facets_clm_recs.COINS_AMT").alias("COINS_AMT"),
    F.col("facets_clm_recs.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("facets_clm_recs.COPAY_AMT").alias("COPAY_AMT"),
    F.col("facets_clm_recs.CHRG_AMT").alias("CHRG_AMT"),
    F.col("facets_clm_recs.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("facets_clm_recs.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("facets_clm_recs.CLM_CT").alias("CLM_CT"),
    F.col("facets_clm_recs.MBR_AGE").alias("MBR_AGE"),
    F.col("facets_clm_recs.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("facets_clm_recs.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("facets_clm_recs.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("facets_clm_recs.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("facets_clm_recs.MCARE_ID").alias("MCARE_ID"),
    F.col("facets_clm_recs.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("facets_clm_recs.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("facets_clm_recs.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("facets_clm_recs.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("facets_clm_recs.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("facets_clm_recs.SUB_ID").alias("SUB_ID"),
    F.col("facets_clm_recs.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    F.col("facets_clm_recs.REL_PCA_CLM_SK").alias("REL_PCA_CLM_SK"),
    F.col("facets_clm_recs.REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
    F.col("facets_clm_recs.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("facets_clm_recs.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("facets_clm_recs.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("facets_clm_recs.CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD_SK"),
    F.col("facets_clm_recs.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.col("facets_clm_recs.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
)

df_update_clm_rpadded = df_update_clm.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 256, " ")
).withColumn(
    "ATCHMT_IN", F.rpad(F.col("ATCHMT_IN"), 1, " ")
).withColumn(
    "CLNCL_EDIT_IN", F.rpad(F.col("CLNCL_EDIT_IN"), 1, " ")
).withColumn(
    "COBRA_CLM_IN", F.rpad(F.col("COBRA_CLM_IN"), 1, " ")
).withColumn(
    "FIRST_PASS_IN", F.rpad(F.col("FIRST_PASS_IN"), 1, " ")
).withColumn(
    "HOST_IN", F.rpad(F.col("HOST_IN"), 1, " ")
).withColumn(
    "LTR_IN", F.rpad(F.col("LTR_IN"), 1, " ")
).withColumn(
    "MCARE_ASG_IN", F.rpad(F.col("MCARE_ASG_IN"), 1, " ")
).withColumn(
    "NOTE_IN", F.rpad(F.col("NOTE_IN"), 1, " ")
).withColumn(
    "PCA_AUDIT_IN", F.rpad(F.col("PCA_AUDIT_IN"), 1, " ")
).withColumn(
    "PCP_SUBMT_IN", F.rpad(F.col("PCP_SUBMT_IN"), 1, " ")
).withColumn(
    "PROD_OOA_IN", F.rpad(F.col("PROD_OOA_IN"), 1, " ")
).withColumn(
    "ACDNT_DT_SK", F.rpad(F.col("ACDNT_DT_SK"), 10, " ")
).withColumn(
    "INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " ")
).withColumn(
    "MBR_PLN_ELIG_DT_SK", F.rpad(F.col("MBR_PLN_ELIG_DT_SK"), 10, " ")
).withColumn(
    "NEXT_RVW_DT_SK", F.rpad(F.col("NEXT_RVW_DT_SK"), 10, " ")
).withColumn(
    "PD_DT_SK", F.rpad(F.col("PD_DT_SK"), 10, " ")
).withColumn(
    "PAYMT_DRAG_CYC_DT_SK", F.rpad(F.col("PAYMT_DRAG_CYC_DT_SK"), 10, " ")
).withColumn(
    "PRCS_DT_SK", F.rpad(F.col("PRCS_DT_SK"), 10, " ")
).withColumn(
    "RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " ")
).withColumn(
    "SVC_STRT_DT_SK", F.rpad(F.col("SVC_STRT_DT_SK"), 10, " ")
).withColumn(
    "SVC_END_DT_SK", F.rpad(F.col("SVC_END_DT_SK"), 10, " ")
).withColumn(
    "SMLR_ILNS_DT_SK", F.rpad(F.col("SMLR_ILNS_DT_SK"), 10, " ")
).withColumn(
    "STTUS_DT_SK", F.rpad(F.col("STTUS_DT_SK"), 10, " ")
).withColumn(
    "WORK_UNABLE_BEG_DT_SK", F.rpad(F.col("WORK_UNABLE_BEG_DT_SK"), 10, " ")
).withColumn(
    "WORK_UNABLE_END_DT_SK", F.rpad(F.col("WORK_UNABLE_END_DT_SK"), 10, " ")
).withColumn(
    "ADJ_FROM_CLM_ID", F.rpad(F.col("ADJ_FROM_CLM_ID"), 256, " ")
).withColumn(
    "ADJ_TO_CLM_ID", F.rpad(F.col("ADJ_TO_CLM_ID"), 256, " ")
).withColumn(
    "DOC_TX_ID", F.rpad(F.col("DOC_TX_ID"), 18, " ")
).withColumn(
    "MCAID_RESUB_NO", F.rpad(F.col("MCAID_RESUB_NO"), 15, " ")
).withColumn(
    "MCARE_ID", F.rpad(F.col("MCARE_ID"), 12, " ")
).withColumn(
    "MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " ")
).withColumn(
    "PATN_ACCT_NO", F.rpad(F.col("PATN_ACCT_NO"), 256, " ")
).withColumn(
    "PAYMT_REF_ID", F.rpad(F.col("PAYMT_REF_ID"), 16, " ")
).withColumn(
    "PROV_AGMNT_ID", F.rpad(F.col("PROV_AGMNT_ID"), 12, " ")
).withColumn(
    "RFRNG_PROV_TX", F.rpad(F.col("RFRNG_PROV_TX"), 256, " ")
).withColumn(
    "SUB_ID", F.rpad(F.col("SUB_ID"), 14, " ")
).withColumn(
    "MCAID_STTUS_ID", F.rpad(F.col("MCAID_STTUS_ID"), 256, " ")
).withColumn(
    "CLM_TXNMY_CD", F.rpad(F.col("CLM_TXNMY_CD"), 256, " ")
).withColumn(
    "BILL_PAYMT_EXCL_IN", F.rpad(F.col("BILL_PAYMT_EXCL_IN"), 1, " ")
).withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 256, " ")  # ensure final char/varchar columns remain padded
)

df_update_clm_final = df_update_clm_rpadded.select(
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ADJ_FROM_CLM_SK",
    "ADJ_TO_CLM_SK",
    "ALPHA_PFX_SK",
    "CLM_EOB_EXCD_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "EXPRNC_CAT_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "MBR_SK",
    "NTWK_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "CLM_ACDNT_CD_SK",
    "CLM_ACDNT_ST_CD_SK",
    "CLM_ACTV_BCBS_PLN_CD_SK",
    "CLM_AGMNT_SRC_CD_SK",
    "CLM_BTCH_ACTN_CD_SK",
    "CLM_CAP_CD_SK",
    "CLM_CAT_CD_SK",
    "CLM_CHK_CYC_OVRD_CD_SK",
    "CLM_COB_CD_SK",
    "CLM_FINL_DISP_CD_SK",
    "CLM_INPT_METH_CD_SK",
    "CLM_INPT_SRC_CD_SK",
    "CLM_INTER_PLN_PGM_CD_SK",
    "CLM_NTWK_STTUS_CD_SK",
    "CLM_NONPAR_PROV_PFX_CD_SK",
    "CLM_OTHR_BNF_CD_SK",
    "CLM_PAYE_CD_SK",
    "CLM_PRCS_CTL_AGNT_PFX_CD_SK",
    "CLM_SVC_DEFN_PFX_CD_SK",
    "CLM_SVC_PROV_SPEC_CD_SK",
    "CLM_SVC_PROV_TYP_CD_SK",
    "CLM_STTUS_CD_SK",
    "CLM_SUBMTTING_BCBS_PLN_CD_SK",
    "CLM_SUB_BCBS_PLN_CD_SK",
    "CLM_SUBTYP_CD_SK",
    "CLM_TYP_CD_SK",
    "ATCHMT_IN",
    "CLNCL_EDIT_IN",
    "COBRA_CLM_IN",
    "FIRST_PASS_IN",
    "HOST_IN",
    "LTR_IN",
    "MCARE_ASG_IN",
    "NOTE_IN",
    "PCA_AUDIT_IN",
    "PCP_SUBMT_IN",
    "PROD_OOA_IN",
    "ACDNT_DT_SK",
    "INPT_DT_SK",
    "MBR_PLN_ELIG_DT_SK",
    "NEXT_RVW_DT_SK",
    "PD_DT_SK",
    "PAYMT_DRAG_CYC_DT_SK",
    "PRCS_DT_SK",
    "RCVD_DT_SK",
    "SVC_STRT_DT_SK",
    "SVC_END_DT_SK",
    "SMLR_ILNS_DT_SK",
    "STTUS_DT_SK",
    "WORK_UNABLE_BEG_DT_SK",
    "WORK_UNABLE_END_DT_SK",
    "ACDNT_AMT",
    "ACTL_PD_AMT",
    "ALW_AMT",
    "DSALW_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "CHRG_AMT",
    "DEDCT_AMT",
    "PAYBL_AMT",
    "CLM_CT",
    "MBR_AGE",
    "ADJ_FROM_CLM_ID",
    "ADJ_TO_CLM_ID",
    "DOC_TX_ID",
    "MCAID_RESUB_NO",
    "MCARE_ID",
    "MBR_SFX_NO",
    "PATN_ACCT_NO",
    "PAYMT_REF_ID",
    "PROV_AGMNT_ID",
    "RFRNG_PROV_TX",
    "SUB_ID",
    "PCA_TYP_CD_SK",
    "REL_PCA_CLM_SK",
    "REL_BASE_CLM_SK",
    "REMIT_SUPRSION_AMT",
    "MCAID_STTUS_ID",
    "PATN_PD_AMT",
    "CLM_SUBMT_ICD_VRSN_CD_SK",
    "CLM_TXNMY_CD",
    "BILL_PAYMT_EXCL_IN"
)

write_files(
    df_update_clm_final,
    f"{adls_path}/load/CLM.IDSRemitDsalwUpdt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)